//! k16 fork: Snowflake pending-visibility capture and settlement, attached to
//! `backfill::visibility_pending` as its `snowflake` child module so the
//! upstream file carries only hooks. See FORK.md before moving code back.

use super::*;

use crate::emit::ch_emitter::EmitterConfig;
use crate::snowflake::runtime::SnowflakeRuntime;
use crate::snowflake::types::SnowflakeRow;

/// [`ship`] hook: the live emitter when the destination is Snowflake
pub(super) fn snowflake_emitter(dest: &crate::config::DestEmitter) -> Option<Arc<EmitterConfig>> {
    let emitter = dest.current();
    emitter.snowflake.is_some().then_some(emitter)
}

/// Capture source-shaped, fully decoded rows before any visibility decision.
/// The RocksDB record survives a failed or repeated snapshot pass.
pub(super) async fn ship_snowflake(
    pending: PendingSpool,
    live: &MappingSnapshot,
    runtime: &Arc<crate::snowflake::runtime::SnowflakeRuntime>,
    stats: Arc<EmitterStats>,
    resolver: ToastResolver,
    oracle: Option<Arc<Oracle>>,
    policy: crate::emit::route::RowPolicy,
) -> Result<Vec<PendingManifest>, String> {
    use sha2::{Digest, Sha256};
    let PendingSpool {
        spool,
        catalog,
        tally,
    } = pending;
    if spool.records() == 0 {
        spool.discard().await;
        return Ok(Vec::new());
    }
    let mut manifests = Vec::new();
    let mut routes = HashMap::new();
    for desc in catalog.descriptors() {
        let Some(counted) = tally.get(&desc.rel_name) else {
            continue;
        };
        let Some(mapping) = live.get(&desc.rel_name) else {
            return Err(format!(
                "Snowflake pending relation {} has no route",
                desc.rel_name
            ));
        };
        let route = RouteSnapshot::freeze(mapping.clone(), Arc::default(), policy.clone());
        let schema = runtime
            .schema_for(desc, &route)
            .await
            .map_err(|e| e.to_string())?;
        routes.insert(desc.rel_name.clone(), (route, schema));
        manifests.push(PendingManifest {
            rel: PendingRel {
                rel: desc.rel_name.clone(),
                target: mapping.target.clone(),
                tees: Vec::new(),
                leaf: None,
            },
            relation_oid: desc.oid,
            start_lsn: counted.start_lsn,
            xids: counted.xids.iter().copied().collect(),
            rows: counted.rows,
        });
    }
    let mut reader = spool
        .into_reader()
        .await
        .map_err(|e| format!("pending visibility: spool seal: {e}"))?;
    let mut captured = 0u64;
    while let Some(mut tuple) = reader
        .next()
        .await
        .map_err(|e| format!("pending visibility: spool replay: {e}"))?
    {
        let desc = catalog
            .get(tuple.rfn.db_node, tuple.rfn.rel_node)
            .ok_or_else(|| "Snowflake pending relation disappeared from catalog".to_string())?;
        let (route, schema) = routes.get(&desc.rel_name).ok_or_else(|| {
            format!(
                "Snowflake pending relation {} has no captured route",
                desc.rel_name
            )
        })?;
        let base = metadata_base(&desc);
        let xmin = match tuple.columns.get(base).and_then(Option::as_ref) {
            Some(ColumnValue::Oid(x)) => *x,
            _ => return Err("Snowflake pending xmin metadata missing".into()),
        };
        let xmax = match tuple.columns.get(base + 1).and_then(Option::as_ref) {
            Some(ColumnValue::Oid(x)) => *x,
            _ => return Err("Snowflake pending xmax metadata missing".into()),
        };
        tuple.columns.truncate(base);
        if tuple.has_mapped_external(&route.mapping) {
            if !resolver.stores_chunks() {
                return Err("Snowflake pending row has unresolved external TOAST".into());
            }
            let _permit =
                bootstrap::resolve_or_fill_toast(&mut tuple, &desc, &route.mapping, &resolver)
                    .await?;
        }
        let (incarnation, _) = runtime.lineage(&desc).map_err(|e| e.to_string())?;
        let captured_generation = runtime
            .pending_capture_generation(desc.oid)
            .map_err(|e| e.to_string())?;
        let id_input = serde_json::to_vec(&(
            runtime.source_identity.as_str(),
            desc.oid,
            captured_generation,
            tuple.rfn.rel_node,
            tuple.blkno,
            tuple.offnum,
            xmin,
            xmax,
            tuple.source_lsn,
        ))
        .map_err(|e| e.to_string())?;
        let id = hex::encode(Sha256::digest(id_input));
        let ordinal = u32::from(tuple.offnum);
        let mut committed = tuple.into_committed_insert();
        if let Some(oracle) = &oracle {
            crate::snowflake::types::render_source_text(oracle, &mut committed, &desc)
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut row = SnowflakeRow::from_committed_with_lineage(
            schema,
            &committed,
            ordinal,
            false,
            &runtime.source_identity,
            incarnation,
            captured_generation,
        )?;
        row.event_id = id.clone();
        runtime
            .capture_pending(
                &desc.rel_name.namespace,
                &desc.rel_name.name,
                schema.clone(),
                row,
                id,
                xmin,
                xmax,
                captured_generation,
            )
            .map_err(|e| e.to_string())?;
        captured += 1;
    }
    stats.pending_rows.fetch_add(captured, Ordering::Relaxed);
    stats
        .pending_tables
        .fetch_add(manifests.len() as u64, Ordering::Relaxed);
    Ok(manifests)
}

impl PendingLedger {
    /// Rebuild any carry records durably captured before a crash interrupted
    /// the manifest handoff. Never erase transaction decisions already learned.
    pub async fn reconcile_snowflake(
        &mut self,
        runtime: &crate::snowflake::runtime::SnowflakeRuntime,
    ) -> Result<(), String> {
        for manifest in runtime.pending_manifests().map_err(|e| e.to_string())? {
            if let Some(entry) = self
                .entries
                .iter_mut()
                .find(|entry| entry.relation_oid == manifest.relation_oid)
            {
                if entry.namespace != manifest.source_namespace
                    || entry.relname != manifest.source_relname
                    || entry.target.database != manifest.database
                    || entry.target.table != manifest.table
                {
                    return Err("Snowflake pending relation metadata conflicts with ledger".into());
                }
                entry.start_lsn = entry.start_lsn.min(Pos::new(manifest.start_lsn));
                for xid in manifest.xids {
                    if !entry.outstanding.contains(&xid)
                        && !entry.committed.contains(&xid)
                        && !entry.aborted.contains(&xid)
                    {
                        entry.outstanding.push(xid);
                    }
                }
            } else {
                self.entries.push(PendingEntry {
                    relation_oid: manifest.relation_oid,
                    namespace: manifest.source_namespace,
                    relname: manifest.source_relname,
                    target: TableTarget::new(&manifest.database, &manifest.table),
                    tees: Vec::new(),
                    leaf: None,
                    start_lsn: Pos::new(manifest.start_lsn),
                    outstanding: manifest.xids,
                    committed: Vec::new(),
                    aborted: Vec::new(),
                    held: false,
                    fresh_committed: Vec::new(),
                    fresh_aborted: Vec::new(),
                });
            }
        }
        self.persist()
            .await
            .map_err(|e| format!("Snowflake pending manifest persist: {e}"))
    }
}

/// [`settle`] for Snowflake: reconcile durable captures, persist decisions,
/// then settle each relation remotely and retire the finished ones
pub(super) async fn settle_snowflake(
    ledger: &mut PendingLedger,
    runtime: &SnowflakeRuntime,
    stats: &EmitterStats,
) -> Result<(), String> {
    ledger.reconcile_snowflake(runtime).await?;
    // Persist learned xid outcomes before any remote effect. A crash after
    // delivery can then replay settlement from the durable decisions.
    ledger
        .persist()
        .await
        .map_err(|e| format!("Snowflake pending decision persist: {e}"))?;
    let mut done = Vec::new();
    for (i, entry) in ledger.entries.iter_mut().enumerate() {
        if entry.relation_oid == 0 {
            return Err("Snowflake pending ledger lacks relation OID".into());
        }
        runtime
            .settle_pending_relation(entry.relation_oid, &entry.committed, &entry.aborted)
            .await
            .map_err(|e| format!("Snowflake pending settlement: {e}"))?;
        let records = runtime
            .state
            .pending_for_relation(entry.relation_oid)
            .map_err(|e| format!("Snowflake pending state read: {e}"))?;
        if records
            .iter()
            .all(|r| r.phase == crate::snowflake::state::PendingPhase::Retired)
        {
            done.push(i);
        }
        entry.fresh_committed.clear();
        entry.fresh_aborted.clear();
    }
    for i in done.into_iter().rev() {
        ledger.entries.remove(i);
    }
    stats
        .pending_outstanding_xids
        .store(ledger.outstanding().len() as u64, Ordering::Relaxed);
    ledger
        .persist()
        .await
        .map_err(|e| format!("Snowflake pending ledger persist: {e}"))
}
