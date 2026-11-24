//! k16 fork: Snowflake COPY loads and greenfield ledger seeding, attached to
//! `backfill::copy_backfill` as its `snowflake` child module so the upstream
//! file carries only hooks. See FORK.md before moving code back.

use super::*;

use crate::emit::ch_emitter::EmitterConfig;
use crate::emit::route::RouteSnapshot;
use crate::snowflake::runtime::SnowflakeRuntime;
use crate::snowflake::state::GenerationPhase;
use crate::snowflake::types::{SnowflakeRow, TableSchema};
use sha2::{Digest, Sha256};

/// Record relations a completed greenfield bootstrap loaded at `s_lsn` as
/// finished initial loads. Returns how many new ledger entries were written.
pub async fn record_bootstrap_loaded(
    spill_dir: &Path,
    system_id: u64,
    rels: impl IntoIterator<Item = RelName>,
    s_lsn: u64,
) -> std::io::Result<usize> {
    Ledger::record_bootstrap_loaded(spill_dir, system_id, rels, s_lsn).await
}

impl Ledger {
    /// Mark relations a greenfield bootstrap already loaded at `s_lsn` as
    /// done, so boot's opt-in seed does not load them a second time. An
    /// existing entry is newer intent and stays untouched.
    pub(crate) async fn record_bootstrap_loaded(
        spill_dir: &Path,
        system_id: u64,
        rels: impl IntoIterator<Item = RelName>,
        s_lsn: u64,
    ) -> std::io::Result<usize> {
        let mut ledger = Self::load(spill_dir, system_id).await?;
        let mut added = 0;
        for rel in rels {
            ledger.entries.entry(rel).or_insert_with(|| {
                added += 1;
                LedgerRec {
                    s_lsn: Pos::new(s_lsn),
                    mode: InitialLoadMode::BaseBackup,
                    phase: Phase::Done,
                    toast_seeded: false,
                }
            });
        }
        if added > 0 {
            ledger.persist().await?;
        }
        Ok(added)
    }
}

impl CopyBackfiller {
    pub(super) async fn staged_pass_snowflake(
        &self,
        mode: InitialLoadMode,
        reqs: &[BackupRequest],
        dest: Arc<EmitterConfig>,
        runtime: Arc<SnowflakeRuntime>,
    ) -> anyhow::Result<PassOutcome> {
        let config = self.config_rx.as_ref().map(|rx| rx.borrow().clone());
        let plan = backfill_staging::prepare_snowflake(
            &runtime,
            &dest,
            &self.mapping,
            reqs,
            mode,
            config.as_ref(),
            self.config_rx.as_ref(),
        )
        .await?;
        let mut outcome = PassOutcome::default();
        if !plan.operations.is_empty() {
            let active_reqs: Vec<BackupRequest> = reqs
                .iter()
                .filter(|r| plan.operations.contains_key(&r.desc.rel_name))
                .cloned()
                .collect();
            let mut pass_emitter = (*dest).clone();
            pass_emitter.snowflake_snapshots = Arc::new(plan.operations.clone());
            // Snowflake generations make a rerun idempotent, so the pass
            // never resumes from a backup checkpoint
            let scratch_dir = self.spill_dir.join("backup_backfill");
            BackupCheckpoint::discard(&scratch_dir).await?;
            let checkpoint = BackupCheckpoint::new(
                mode,
                &active_reqs,
                &plan.mapping.snapshot(),
                &pass_emitter,
                config.as_deref(),
            );
            let ctx = PassContext {
                pg: self.source_pg(),
                dest: DestEmitter::new(Arc::new(pass_emitter), None),
                mapping: plan.mapping,
                published: self.mapping.clone(),
                stats: self.stats.clone(),
                catalog: self.catalog.clone(),
                log: self.log.clone(),
                scratch_dir,
                config_rx: self.config_rx.clone(),
                history_rx: self.history_rx.clone(),
                budget: self.budget.clone(),
                oracle: self.oracle.clone(),
                source_major: self.source_major,
                checkpoint,
            };
            outcome = crate::backfill::backup_backfill::run_pass(&ctx, mode, &active_reqs).await?;
            BackupCheckpoint::discard(&ctx.scratch_dir).await?;
        }
        // Pending visibility rows are durable before publication. Their xid
        // manifest must be durable too, so a crash after a view swap cannot
        // leave undecided rows with no settlement path.
        self.pending_rows
            .lock()
            .await
            .reconcile_snowflake(&runtime)
            .await
            .map_err(anyhow::Error::msg)
            .context("Snowflake pending visibility reconciliation")?;
        let selected = plan
            .rels
            .iter()
            .map(|r| r.desc.rel_name.clone())
            .collect::<Vec<_>>();
        let _mapping_guard = self
            .mapping
            .guard_matches(&plan.source_mapping, &selected)
            .await
            .context("Snowflake snapshot mapping changed before publication")?;
        if let (Some(expected), Some(rx)) = (&config, &self.config_rx) {
            let current = rx.borrow();
            anyhow::ensure!(
                plan.rels
                    .iter()
                    .all(|r| backfill_staging::route_config_matches(expected, &current, &r.desc)),
                "Snowflake snapshot routing config changed before publication"
            );
        }
        crate::backfill::backfill_bootstrap::publish_snapshot_rels(&runtime, &plan.rels).await?;
        let mut ledger = self.pending_rows.lock().await;
        if !ledger.is_empty() {
            let mut session = StagingSession::connect(self.dest.clone()).await?;
            crate::backfill::visibility_pending::settle(&mut ledger, &mut session, &self.stats)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        drop(ledger);
        for rel in &plan.rels {
            self.mark_done_entry(&rel.desc.rel_name).await;
        }
        Ok(outcome)
    }

    pub(super) async fn copy_once_snowflake(
        &self,
        client: &tokio_postgres::Client,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
        runtime: Arc<SnowflakeRuntime>,
    ) -> anyhow::Result<CopyOutcome> {
        let mapping = self.mapping.snapshot();
        let table = mapping.get(&desc.rel_name).with_context(|| {
            format!(
                "Snowflake COPY relation {} is no longer mapped",
                desc.rel_name
            )
        })?;
        let config = self.config_rx.as_ref().map(|rx| rx.borrow().clone());
        let route = RouteSnapshot::freeze(
            table.clone(),
            config
                .as_ref()
                .map_or_else(Arc::default, |rc| rc.column_rules.clone()),
            self.dest
                .current()
                .row_policy()
                .for_rel(config.as_deref(), &desc.rel_name),
        );
        let preparation_guard = self
            .mapping
            .guard_matches(&mapping, std::slice::from_ref(&desc.rel_name))
            .await
            .context("Snowflake COPY mapping changed before preparation")?;
        if let (Some(expected), Some(rx)) = (&config, &self.config_rx) {
            let current = rx.borrow();
            anyhow::ensure!(
                backfill_staging::route_config_matches(expected, &current, desc),
                "Snowflake COPY routing config changed before preparation"
            );
        }
        runtime.defer_publication(desc)?;
        let schema = runtime.schema_for(desc, &route).await?;
        let logical_id = snowflake_copy_operation_id(&runtime.source_identity, desc, s_lsn.get());
        let generation = runtime
            .begin_snapshot_attempt(desc, s_lsn.get(), &logical_id)
            .await?;
        drop(preparation_guard);
        if generation.phase == GenerationPhase::Replayed {
            return Ok(CopyOutcome {
                rows: 0,
                skipped_empty: false,
                p_hi: snapshot_wal_lsn(client).await?,
            });
        }
        anyhow::ensure!(
            generation.phase == GenerationPhase::Prepared,
            "Snowflake snapshot attempt is not prepared"
        );
        let operation_id = generation.operation_id;
        let (incarnation, _) = runtime.lineage(desc)?;

        let qtable = format!(
            "{}.{}",
            quote_ident(&desc.rel_name.namespace),
            quote_ident(&desc.rel_name.name)
        );
        let nonempty: bool = client
            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM ONLY {qtable})"), &[])
            .await
            .context("Snowflake COPY emptiness probe")?
            .get(0);
        let (rows, batch_ids) = if nonempty {
            let (tx, rx) = mpsc::channel::<Vec<BackfillTuple>>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
            let copy = async {
                let result = crate::ops::stages::COPY
                    .measure(copy_rows_into(
                        client,
                        desc,
                        s_lsn.get(),
                        &tx,
                        &self.stats,
                        BlockRange {
                            start: 0,
                            end: None,
                        },
                    ))
                    .await;
                drop(tx);
                result
            };
            let drain = drain_snowflake_copy(
                rx,
                runtime.clone(),
                schema,
                desc.clone(),
                self.oracle.clone(),
                operation_id.clone(),
                generation.generation_id,
                incarnation,
            );
            tokio::try_join!(copy, drain)?
        } else {
            (0, Vec::new())
        };
        let _mapping_guard = self
            .mapping
            .guard_matches(&mapping, std::slice::from_ref(&desc.rel_name))
            .await
            .context("Snowflake COPY mapping changed before publication")?;
        if let (Some(expected), Some(rx)) = (&config, &self.config_rx) {
            let current = rx.borrow();
            anyhow::ensure!(
                backfill_staging::route_config_matches(expected, &current, desc),
                "Snowflake COPY routing config changed before publication"
            );
        }
        runtime
            .mark_snapshot_loaded(desc, &operation_id, &batch_ids, rows == 0)
            .await?;
        runtime.publish_snapshot(desc, &operation_id).await?;
        let p_hi = snapshot_wal_lsn(client).await?;
        Ok(CopyOutcome {
            rows,
            skipped_empty: rows == 0,
            p_hi,
        })
    }
}

fn snowflake_copy_operation_id(source_identity: &str, desc: &RelDescriptor, s_lsn: u64) -> String {
    snowflake_snapshot_logical_id("copy", source_identity, desc, s_lsn)
}

pub(crate) fn snowflake_snapshot_logical_id(
    kind: &str,
    source_identity: &str,
    desc: &RelDescriptor,
    s_lsn: u64,
) -> String {
    let mut digest = Sha256::new();
    for part in [
        b"snowflake-snapshot-v1".as_slice(),
        kind.as_bytes(),
        source_identity.as_bytes(),
        &desc.oid.to_be_bytes(),
        &desc.rfn.spc_node.to_be_bytes(),
        &desc.rfn.db_node.to_be_bytes(),
        &desc.rfn.rel_node.to_be_bytes(),
        &s_lsn.to_be_bytes(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    format!("{kind}-{}", hex::encode(digest.finalize()))
}

#[allow(clippy::too_many_arguments)]
async fn drain_snowflake_copy(
    mut rx: mpsc::Receiver<Vec<BackfillTuple>>,
    runtime: Arc<SnowflakeRuntime>,
    schema: TableSchema,
    desc: Arc<RelDescriptor>,
    oracle: Option<Arc<Oracle>>,
    operation_id: String,
    generation_id: u64,
    incarnation: u64,
) -> anyhow::Result<Vec<String>> {
    // Snapshot batches land as Parquet through COPY INTO, which favors larger
    // files than live streaming, and several stay in flight so verified
    // batches of the generation share one grouped MERGE instead of each
    // paying a full round trip before the next seals
    let row_limit = runtime.config.batch_rows.max(SNAPSHOT_BATCH_ROWS);
    let byte_limit = runtime.config.batch_bytes.max(SNAPSHOT_BATCH_BYTES);
    let in_flight = runtime.config.channels_per_table.clamp(1, 4);
    let mut deliveries = tokio::task::JoinSet::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let mut ids = Vec::new();
    let send = |batch: Vec<SnowflakeRow>,
                deliveries: &mut tokio::task::JoinSet<anyhow::Result<String>>| {
        let runtime = runtime.clone();
        let schema = schema.clone();
        let operation_id = operation_id.clone();
        deliveries
            .spawn(async move { runtime.deliver_snapshot(schema, batch, &operation_id).await });
    };
    while let Some(slab) = rx.recv().await {
        for tuple in slab {
            let mut committed = tuple.into_committed_insert();
            if let Some(oracle) = &oracle {
                crate::snowflake::types::render_source_text(oracle, &mut committed, &desc).await?;
            }
            batch_bytes = batch_bytes.saturating_add(committed.decoded.approx_bytes());
            batch.push(
                SnowflakeRow::from_committed_with_lineage(
                    &schema,
                    &committed,
                    0,
                    false,
                    &runtime.source_identity,
                    incarnation,
                    generation_id,
                )
                .map_err(anyhow::Error::msg)?,
            );
            if batch.len() >= row_limit || batch_bytes >= byte_limit {
                while deliveries.len() >= in_flight {
                    ids.push(joined(deliveries.join_next().await)?);
                }
                send(std::mem::take(&mut batch), &mut deliveries);
                batch_bytes = 0;
            }
        }
    }
    if !batch.is_empty() {
        send(batch, &mut deliveries);
    }
    while let Some(done) = deliveries.join_next().await {
        ids.push(joined(Some(done))?);
    }
    Ok(ids)
}

/// Snapshot batch floors; live batches keep `[snowflake] batch_rows/bytes`
const SNAPSHOT_BATCH_ROWS: usize = 200_000;
const SNAPSHOT_BATCH_BYTES: usize = 32 << 20;

fn joined(
    done: Option<Result<anyhow::Result<String>, tokio::task::JoinError>>,
) -> anyhow::Result<String> {
    match done {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(anyhow::anyhow!("snapshot delivery task failed: {e}")),
        None => Err(anyhow::anyhow!("snapshot delivery task disappeared")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowflake::test_support::{attr, desc};

    #[test]
    fn snowflake_copy_operation_is_stable_and_source_qualified() {
        let d = desc("orders", vec![attr(1, "id", INT8OID)], None);
        let id = snowflake_copy_operation_id("source-a", &d, 42);
        assert_eq!(id, snowflake_copy_operation_id("source-a", &d, 42));
        assert_ne!(id, snowflake_copy_operation_id("source-b", &d, 42));
        assert_ne!(id, snowflake_copy_operation_id("source-a", &d, 43));
        let mut reincarnated = d;
        reincarnated.rfn.rel_node += 1;
        assert_ne!(
            id,
            snowflake_copy_operation_id("source-a", &reincarnated, 42)
        );
    }
}
