//! k16 fork: Snowflake backup-walk snapshot generations, attached to
//! `backfill::backfill_staging` as its `fork` child module. See FORK.md
//! before moving code back into upstream-owned files.

use super::*;

use crate::config::ResolvedConfig;
use crate::destination::snowflake::runtime::SnowflakeRuntime;
use crate::destination::snowflake::state::GenerationPhase;
use crate::emit::ch_emitter::EmitterConfig;
use crate::emit::route::RouteSnapshot;
use crate::mapping::MappingSnapshot;
use crate::runtime_config::InitialLoadMode;
use crate::schema::RelDescriptor;
use foldhash::HashSetExt;
use futures::{StreamExt, TryStreamExt};

/// One backup-walk snapshot generation. `Replayed` means a prior successful
/// attempt was recovered and its relation must not be reread into this pass.
#[derive(Clone)]
pub struct SnowflakeSnapshotRel {
    pub desc: Arc<RelDescriptor>,
    pub operation_id: String,
    pub phase: GenerationPhase,
}

/// Frozen source-shaped routes and journaled generation targets for a pass.
pub struct SnowflakeSnapshotPlan {
    pub mapping: MappingHandle,
    pub source_mapping: MappingSnapshot,
    pub rels: Vec<SnowflakeSnapshotRel>,
    pub operations: HashMap<RelName, String>,
}

pub(crate) fn route_config_matches(
    expected: &ResolvedConfig,
    current: &ResolvedConfig,
    desc: &RelDescriptor,
) -> bool {
    let rel = &desc.rel_name;
    expected.tables.get(rel) == current.tables.get(rel)
        && expected.table_opt_ins.get(rel) == current.table_opt_ins.get(rel)
        && expected.rules.settings(rel) == current.rules.settings(rel)
        && desc.attributes.iter().filter(|a| !a.dropped).all(|a| {
            expected.column_rules.settings(rel, &a.name)
                == current.column_rules.settings(rel, &a.name)
                && expected.column_rules.accepted_type(rel, &a.name)
                    == current.column_rules.accepted_type(rel, &a.name)
        })
}

pub async fn prepare_snowflake(
    runtime: &SnowflakeRuntime,
    emitter: &EmitterConfig,
    live: &MappingHandle,
    reqs: &[BackupRequest],
    mode: InitialLoadMode,
    config: Option<&Arc<ResolvedConfig>>,
    config_rx: Option<&tokio::sync::watch::Receiver<Arc<ResolvedConfig>>>,
) -> Result<SnowflakeSnapshotPlan> {
    prepare_snowflake_with_kind(
        runtime,
        emitter,
        live,
        reqs,
        mode.as_str(),
        config.map(Arc::as_ref),
        config.zip(config_rx),
    )
    .await
}

pub async fn prepare_snowflake_with_kind(
    runtime: &SnowflakeRuntime,
    emitter: &EmitterConfig,
    live: &MappingHandle,
    reqs: &[BackupRequest],
    kind: &str,
    config: Option<&ResolvedConfig>,
    current_config: Option<(
        &Arc<ResolvedConfig>,
        &tokio::sync::watch::Receiver<Arc<ResolvedConfig>>,
    )>,
) -> Result<SnowflakeSnapshotPlan> {
    let mut relation_oids = HashSet::with_capacity(reqs.len());
    anyhow::ensure!(
        reqs.iter().all(|req| relation_oids.insert(req.desc.oid)),
        "Snowflake snapshot pass contains duplicate relations"
    );
    let live_map = live.snapshot();
    // begin_snapshot_attempt can finish a previously loaded generation, so
    // the route must remain current during setup as well as final publication.
    let selected = reqs
        .iter()
        .map(|r| r.desc.rel_name.clone())
        .collect::<Vec<_>>();
    let _mapping_guard = live
        .guard_matches(&live_map, &selected)
        .await
        .context("Snowflake snapshot mapping changed before preparation")?;
    if let Some((expected, rx)) = current_config {
        let current = rx.borrow();
        anyhow::ensure!(
            reqs.iter()
                .all(|r| route_config_matches(expected, &current, &r.desc)),
            "Snowflake snapshot routing config changed before preparation"
        );
    }
    let mut walk_map = HashMap::with_capacity(reqs.len());
    let mut rels = Vec::with_capacity(reqs.len());
    let mut operations = HashMap::with_capacity(reqs.len());
    let prepared = futures::stream::iter(reqs.to_vec())
        .map(|req| {
            let live_map = &live_map;
            async move {
                let name = &req.desc.rel_name;
                let Some(mapping) = live_map.get(name) else {
                    tracing::warn!(target: "walshadow::backfill_staging", qname = %name,
                "Snowflake snapshot relation unmapped at pass start");
                    return Ok::<_, anyhow::Error>(None);
                };
                let route = RouteSnapshot::freeze(
                    mapping.clone(),
                    config.map_or_else(Arc::default, |c| c.column_rules.clone()),
                    emitter.row_policy().for_rel(config, name),
                );
                runtime.defer_publication(&req.desc)?;
                runtime.schema_for(&req.desc, &route).await?;
                let logical_id = crate::backfill::copy_backfill::snowflake_snapshot_logical_id(
                    kind,
                    &runtime.source_identity,
                    &req.desc,
                    req.s_lsn,
                );
                let generation = runtime
                    .begin_snapshot_attempt(&req.desc, req.s_lsn, &logical_id)
                    .await?;
                if generation.phase != GenerationPhase::Replayed {
                    anyhow::ensure!(
                        generation.phase == GenerationPhase::Prepared,
                        "Snowflake snapshot generation is not prepared for {name}"
                    );
                }
                Ok(Some((req, mapping.clone(), generation)))
            }
        })
        .buffered(runtime.config.metadata_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    for (req, mapping, generation) in prepared.into_iter().flatten() {
        if generation.phase != GenerationPhase::Replayed {
            operations.insert(req.desc.rel_name.clone(), generation.operation_id.clone());
            walk_map.insert(req.desc.rel_name.clone(), (*mapping).clone());
        }
        rels.push(SnowflakeSnapshotRel {
            desc: req.desc.clone(),
            operation_id: generation.operation_id,
            phase: generation.phase,
        });
    }
    tracing::info!(target: "walshadow::backfill_staging", tables = rels.len(),
        "Snowflake snapshot generations prepared");
    Ok(SnowflakeSnapshotPlan {
        mapping: crate::mapping::mapping_handle(walk_map),
        source_mapping: live_map,
        rels,
        operations,
    })
}

impl StagingSession {
    pub fn snowflake_runtime(&self) -> Option<&Arc<SnowflakeRuntime>> {
        self.dest.base().snowflake.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_config_fence_ignores_unrelated_tables_but_rejects_own_opt_out() {
        let rel = RelName::new("public", "orders");
        let other = RelName::new("public", "customers");
        let desc = RelDescriptor {
            ancestors: Vec::new(),
            rfn: Default::default(),
            oid: 1,
            toast_oid: 0,
            namespace_oid: 1,
            rel_name: rel.clone(),
            kind: 'r',
            persistence: 'p',
            replident: crate::schema::ReplIdent::Nothing,
            attributes: vec![],
        };
        let mut expected = ResolvedConfig::default();
        expected.tables.insert(
            rel.clone(),
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("db", "orders"),
                columns: vec![],
            },
        );
        let mut current = expected.clone();
        current.tables.insert(
            other,
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("db", "customers"),
                columns: vec![],
            },
        );
        assert!(route_config_matches(&expected, &current, &desc));
        current.tables.remove(&rel);
        assert!(!route_config_matches(&expected, &current, &desc));
    }
}
