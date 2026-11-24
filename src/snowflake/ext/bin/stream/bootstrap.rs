//! k16 fork: Snowflake greenfield bootstrap (snapshot floor, generations,
//! publication) and `initial_load = "none"` skips, attached to the stream
//! binary's `bootstrap` module as its `snowflake` child. See FORK.md.

use super::*;

use walshadow::backfill_staging::SnowflakeSnapshotPlan;
use walshadow::bootstrap_marker::ExtractedCheckpoint;
use walshadow::config::ResolvedConfig;
use walshadow::mapping::MappingSnapshot;
use walshadow::schema::RelName;
use walshadow::snowflake::runtime::SnowflakeRuntime;

/// Snowflake greenfield state carried from snapshot preparation to
/// publication
pub(super) struct SnowflakeGreenfield {
    pub(super) plan: SnowflakeSnapshotPlan,
    pub(super) runtime: Arc<SnowflakeRuntime>,
    /// WAL floor pinned on the bootstrap marker
    pub(super) floor: u64,
    pub(super) emitter: Arc<EmitterConfig>,
    pub(super) mapping: MappingHandle,
}

/// Pin the greenfield WAL floor and prepare one snapshot generation per
/// relation before any page row ships. `None` unless the target is Snowflake
pub(super) async fn prepare_snowflake_greenfield(
    ch_target: &mut Option<(
        EmitterConfig,
        MappingHandle,
        Arc<ResolvedConfig>,
        HashSet<RelName>,
    )>,
    marker: &mut BootstrapMarker,
    resume: &mut Option<ExtractedCheckpoint>,
    routes: &mut MappingSnapshot,
    shadow_data_dir: &Path,
    sampled_floor: u64,
    drain_catalog: &walshadow::backup_page_walk::CatalogMap,
) -> Result<Option<SnowflakeGreenfield>> {
    let Some((emitter, mapping, resolved, skip_initial)) = ch_target.as_mut() else {
        return Ok(None);
    };
    let Some(runtime) = emitter.snowflake.clone() else {
        return Ok(None);
    };
    let floor = marker
        .pin_snapshot_lsn(shadow_data_dir, sampled_floor)
        .await
        .context("pin Snowflake greenfield WAL floor")?;
    if resume.take().is_some() {
        bootstrap_marker::restart_extraction(shadow_data_dir)
            .await
            .context("restart Snowflake greenfield extraction")?;
    }
    let plan = walshadow::backfill_bootstrap::prepare_greenfield_snapshots(
        &runtime,
        emitter,
        mapping,
        drain_catalog,
        skip_initial,
        resolved,
        floor,
    )
    .await
    .context("prepare Snowflake greenfield generations")?;
    for rel in &plan.rels {
        if rel.phase == walshadow::snowflake::state::GenerationPhase::Replayed {
            skip_initial.insert(rel.desc.rel_name.clone());
        }
    }
    *routes = plan.mapping.snapshot();
    emitter.snowflake_snapshots = Arc::new(plan.operations.clone());
    Ok(Some(SnowflakeGreenfield {
        plan,
        runtime,
        floor,
        emitter: Arc::new(emitter.clone()),
        mapping: mapping.clone(),
    }))
}

/// Publish the prepared generations, record them as completed initial loads
/// and settle any pending-visibility rows they captured
pub(super) async fn publish_snowflake_greenfield(
    args: &Args,
    system_id: u64,
    greenfield: &SnowflakeGreenfield,
    bootstrap_stats: &EmitterStats,
) -> Result<()> {
    let SnowflakeGreenfield {
        plan,
        runtime,
        floor,
        emitter,
        mapping,
    } = greenfield;
    walshadow::backfill_bootstrap::publish_greenfield_snapshots(runtime, plan, mapping)
        .await
        .context("publish Snowflake greenfield generations")?;
    // Published generations are the tables' initial load; without a done
    // ledger entry boot's `initial_load` opt-in seed would copy them again
    let recorded = walshadow::copy_backfill::record_bootstrap_loaded(
        &args.spill_dir,
        system_id,
        plan.rels.iter().map(|rel| rel.desc.rel_name.clone()),
        *floor,
    )
    .await
    .context("record Snowflake greenfield loads in backfill ledger")?;
    tracing::info!(target: "walshadow::bootstrap", recorded,
        "greenfield loads recorded as completed initial loads");
    let mut ledger = walshadow::visibility_pending::PendingLedger::load(&args.spill_dir, system_id)
        .await
        .context("load Snowflake greenfield pending ledger")?;
    ledger
        .reconcile_snowflake(runtime)
        .await
        .map_err(anyhow::Error::msg)
        .context("reconcile Snowflake greenfield pending rows")?;
    if !ledger.is_empty() {
        let mut session = walshadow::backfill_staging::StagingSession::connect(
            walshadow::config::DestEmitter::new(emitter.clone(), None),
        )
        .await
        .context("open Snowflake pending settlement")?;
        walshadow::visibility_pending::settle(&mut ledger, &mut session, bootstrap_stats)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    Ok(())
}

/// `initial_load = "none"` (table override, else namespace) opts a relation
/// out of the greenfield snapshot: create it and stream CDC, but don't
/// page-walk its existing rows
pub(super) fn bootstrap_skips_initial(
    emitter: &EmitterConfig,
    resolved: &walshadow::config::ResolvedConfig,
    desc: &RelDescriptor,
) -> bool {
    // A partition loads as its root's `initial_load` says
    let relation = &routing(desc, Some(&resolved.rules))
        .owner(desc)
        .unwrap_or(desc)
        .rel_name;
    let table_mode = emitter
        .table_opt_ins
        .get(relation)
        .and_then(|row| row.initial_load.as_deref())
        .or_else(|| {
            emitter
                .table_initial_loads
                .get(relation)
                .map(String::as_str)
        });
    match table_mode {
        Some(mode) => mode.parse::<InitialLoadMode>() == Ok(InitialLoadMode::None),
        None => {
            resolved
                .namespaces
                .get(relation.namespace.as_ref())
                .and_then(|ns| ns.initial_load)
                == Some(InitialLoadMode::None)
        }
    }
}

/// [`bootstrap_build_mapping`] for Snowflake: create every relation's table
/// with bounded concurrency, deferring publication for snapshot loads
#[allow(clippy::too_many_arguments)]
pub(super) async fn snowflake_build_mapping(
    emitter_cfg: &EmitterConfig,
    catalog: &walshadow::backup_page_walk::CatalogMap,
    runtime: &Arc<SnowflakeRuntime>,
    dest: Arc<walshadow::config::DestEmitter>,
    ddl_cfg: walshadow::ch_ddl::DdlConfig,
    mapping: MappingHandle,
    config_rx: tokio::sync::watch::Receiver<Arc<ResolvedConfig>>,
    resolved: Arc<ResolvedConfig>,
) -> Result<(MappingHandle, Arc<ResolvedConfig>)> {
    use futures::{StreamExt, TryStreamExt};
    // Relations have independent storage and durable journals. Bound remote
    // setup without serializing thousands of SQL round trips on one lane.
    futures::stream::iter(catalog.descriptors())
        .map(|desc| {
            let dest = dest.clone();
            let ddl_cfg = ddl_cfg.clone();
            let config_rx = config_rx.clone();
            let mapping = mapping.clone();
            let resolved = resolved.clone();
            async move {
                let mut applicator = walshadow::ch_ddl::DdlApplicator::new(
                    dest,
                    ddl_cfg,
                    mapping.clone(),
                    config_rx,
                )
                .await?;
                if !bootstrap_skips_initial(emitter_cfg, &resolved, desc)
                    && mapping.with(|m| m.contains_key(&desc.rel_name))
                {
                    runtime.defer_publication(desc)?;
                }
                applicator
                    .add(desc)
                    .await
                    .with_context(|| format!("bootstrap: ensure Snowflake table {}", desc.rel_name))
            }
        })
        .buffer_unordered(runtime.config.metadata_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    tracing::info!(target: "walshadow::bootstrap",
        tables = mapping.with(|m| m.len()),
        "Snowflake bootstrap table setup complete");
    Ok((mapping, resolved))
}
