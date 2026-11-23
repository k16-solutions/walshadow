//! k16 fork: Snowflake greenfield snapshot generations (prepare before the
//! backup walk, publish after it), attached to `backfill::backfill_bootstrap`
//! as its `fork` child module. See FORK.md.

use super::*;

use crate::backfill::backfill_staging::{self, SnowflakeSnapshotPlan, SnowflakeSnapshotRel};
use crate::backfill::backfill_types::BackupRequest;
use crate::config::ResolvedConfig;
use crate::destination::snowflake::runtime::SnowflakeRuntime;
use crate::destination::snowflake::state::GenerationPhase;
use crate::emit::ch_emitter::EmitterConfig;
use crate::mapping::MappingHandle;
use foldhash::HashSet;

impl BootstrapConfig {
    pub fn with_snapshot_lsn(mut self, lsn: u64) -> Self {
        self.snapshot_lsn = Some(lsn);
        self
    }
}

pub async fn prepare_greenfield_snapshots(
    runtime: &SnowflakeRuntime,
    emitter: &EmitterConfig,
    mapping: &MappingHandle,
    catalog: &CatalogMap,
    skip_initial: &HashSet<RelName>,
    config: &ResolvedConfig,
    snapshot_lsn: u64,
) -> Result<SnowflakeSnapshotPlan> {
    let routes = mapping.snapshot();
    let reqs: Vec<BackupRequest> = catalog
        .descriptors()
        .filter(|desc| {
            !catalog.is_toast(desc.rfn.db_node, desc.rfn.rel_node)
                && routes.contains_key(&desc.rel_name)
                && !skip_initial.contains(&desc.rel_name)
        })
        .map(|desc| BackupRequest {
            desc: desc.clone(),
            s_lsn: snapshot_lsn,
        })
        .collect();
    backfill_staging::prepare_snowflake_with_kind(
        runtime,
        emitter,
        mapping,
        &reqs,
        "greenfield",
        Some(config),
        None,
    )
    .await
}

pub async fn publish_greenfield_snapshots(
    runtime: &SnowflakeRuntime,
    plan: &SnowflakeSnapshotPlan,
    live: &MappingHandle,
) -> Result<()> {
    let selected = plan
        .rels
        .iter()
        .map(|r| r.desc.rel_name.clone())
        .collect::<Vec<_>>();
    let _mapping_guard = live
        .guard_matches(&plan.source_mapping, &selected)
        .await
        .context("Snowflake greenfield mapping changed before publication")?;
    publish_snapshot_rels(runtime, &plan.rels).await
}

/// Seal and publish every unpublished relation of a snapshot plan. Each
/// relation publishes under its own table lock and journal; the stage is
/// metadata round trips, so bound it without serializing it.
pub async fn publish_snapshot_rels(
    runtime: &SnowflakeRuntime,
    rels: &[SnowflakeSnapshotRel],
) -> Result<()> {
    use futures::{StreamExt, TryStreamExt};
    // Owned items: borrowed stream items trip higher-ranked Send inference
    // when a caller spawns this future
    let pending = rels
        .iter()
        .filter(|rel| rel.phase != GenerationPhase::Replayed)
        .cloned()
        .collect::<Vec<_>>();
    futures::stream::iter(pending)
        .map(|rel| async move {
            let ids = runtime.registered_snapshot_batch_ids(&rel.operation_id)?;
            runtime
                .mark_snapshot_loaded(&rel.desc, &rel.operation_id, &ids, ids.is_empty())
                .await?;
            runtime
                .publish_snapshot(&rel.desc, &rel.operation_id)
                .await
                .with_context(|| format!("publish Snowflake snapshot {}", rel.desc.rel_name))
        })
        .buffer_unordered(runtime.config.metadata_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_floor_is_opt_in() {
        let cfg = BootstrapConfig::new(PathBuf::from("/shadow"));
        assert_eq!(cfg.snapshot_lsn, None);
        assert_eq!(cfg.with_snapshot_lsn(0x100).snapshot_lsn, Some(0x100));
    }
}
