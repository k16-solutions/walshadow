//! k16 fork: per-tuple TOAST resolution for Snowflake's pending-visibility
//! capture, attached to `emit::pipeline::bootstrap` as its `fork` child
//! module. Upstream resolves bootstrap TOAST in batches (`resolve_batch`);
//! the Snowflake capture still resolves one tuple at a time. See FORK.md.

use super::*;

use crate::toast::{check_value_caps, pointer_extsize};

/// Resolve mapped TOAST pointers or fill in disabled mode. Value cap
/// checked before any fetch; the returned leaf permit is shrunk to the
/// retained decoded bytes and rides the routed row to insert ack
pub(crate) async fn resolve_or_fill_toast(
    tuple: &mut BackfillTuple,
    rel: &RelDescriptor,
    mapping: &TableMapping,
    resolver: &ToastResolver,
) -> Result<Option<crate::budget::MemoryPermit>, String> {
    let mut sites = mapped_pointers(tuple, mapping);
    if sites.is_empty() {
        return Ok(None);
    }
    let need = check_value_caps(
        sites.iter().map(|site| site.p),
        resolver.inline_value_max(),
        resolver.overflow(),
    )
    .map_err(|e| format!("bootstrap: {e}"))?;
    fill_oversize(tuple, &mut sites, resolver);
    let mut leaf = crate::budget::acquire_opt(resolver.budget(), need).await;
    let mut retained = 0usize;
    for site in &sites {
        let type_oid = rel.attributes.get(site.idx).map_or(0, |a| a.type_oid);
        let fetched = resolver
            .fetch_value(
                tuple.rfn.db_node,
                site.p.va_toastrelid,
                site.p.va_valueid,
                tuple.source_lsn,
                pointer_extsize(&site.p),
            )
            .await
            .map_err(|e| format!("bootstrap: toast store fetch: {e}"))?;
        let target = &mapping.columns[site.col].target_name;
        let (column, bytes) = apply_fetched(fetched, &site.p, type_oid, rel, target, resolver)?;
        retained += bytes;
        tuple.columns[site.idx] = Some(column);
    }
    if let Some(p) = leaf.as_mut() {
        p.shrink(retained as u64);
    }
    Ok(leaf)
}
