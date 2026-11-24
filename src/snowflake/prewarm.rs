//! Snowflake storage prewarm for boot's opt-in seed.

use std::sync::Arc;

use ahash::HashSet;
use tokio::sync::Mutex;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::emit::ch_ddl::DdlApplicator;
use crate::emit::ch_emitter::EmitterConfig;
use crate::runtime_config::TableRow;
use crate::schema::RelName;

/// Create the Snowflake storage of every exact `replicate = true` opt-in
/// concurrently before boot's serial opt-in seed, which then finds each
/// table ready. Validation and publication deferral match the seed's own
/// order, so a pending initial load stays hidden. Best effort: a failure
/// here resurfaces, with context, from the seed.
pub async fn prewarm_opt_ins<'a>(
    emitter_cfg: &EmitterConfig,
    applicator: &mut DdlApplicator,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    rows: impl Iterator<Item = (&'a RelName, &'a TableRow)>,
) {
    use futures::StreamExt;
    let Some(runtime) = emitter_cfg.snowflake.clone() else {
        return;
    };
    let started = std::time::Instant::now();
    let mut seen = HashSet::default();
    let mut descs = Vec::new();
    for (rel, row) in rows {
        if row.replicate != Some(true) || !seen.insert(rel.clone()) {
            continue;
        }
        let Ok(Some(desc)) = catalog.lock().await.descriptor_by_name(rel).await else {
            continue;
        };
        let Ok(Some(_)) = applicator.snowflake_opt_in_mapping(
            &desc,
            row.target_database.as_deref(),
            row.target_table.as_deref(),
        ) else {
            continue;
        };
        if row
            .initial_load
            .as_deref()
            .is_some_and(|mode| mode != "none")
            && applicator.defer_snowflake_publication(&desc).is_err()
        {
            continue;
        }
        descs.push(desc);
    }
    let total = descs.len();
    let failed = futures::stream::iter(descs)
        .map(|desc| {
            let runtime = runtime.clone();
            async move { runtime.ensure_table(&desc).await.is_err() }
        })
        .buffer_unordered(runtime.config.metadata_concurrency)
        .filter(|failed| std::future::ready(*failed))
        .count()
        .await;
    tracing::info!(
        target: "walshadow::config",
        tables = total,
        failed,
        elapsed_secs = started.elapsed().as_secs_f64(),
        "Snowflake opt-in storage prewarmed",
    );
}
