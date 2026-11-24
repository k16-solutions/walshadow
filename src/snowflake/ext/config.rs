//! k16 fork: prepared Snowflake opt-in mappings and Snowflake mapping moves,
//! attached to `config` as its `snowflake` child module. See FORK.md.

use super::*;

impl ConfigResolver {
    /// Install a destination-validated opt-in mapping. Snowflake callers
    /// prepare this from source identity and neutral types before remote DDL,
    /// then publish it through the same resolver layer as CH opt-ins.
    pub async fn materialize_prepared_opt_in(&self, desc: &RelDescriptor, mapping: TableMapping) {
        let mut inner = self.inner.lock().await;
        let rel = desc.rel_name.clone();
        inner.opt_in.mappings.insert(rel.clone(), mapping);
        inner.opt_in.derived.remove(&rel);
        inner.opt_in.excluded.remove(&rel);
        inner.opt_in.pending_decl.remove(&rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_in_total.fetch_add(1, Ordering::Relaxed);
        self.republish(&inner).await;
    }

    /// Move a Snowflake relation's source name at a fenced DDL barrier.
    /// Suppress the old configured name so a later republish cannot restore
    /// a stale route to the renamed warehouse view.
    pub async fn rename_snowflake_mapping(
        &self,
        old: &RelName,
        new: &RelName,
        mapping: TableMapping,
    ) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.mappings.remove(old);
        inner.opt_in.derived.remove(old);
        inner.opt_in.pending_decl.remove(old);
        inner.opt_in.excluded.insert(old.clone());
        inner.opt_in.excluded.remove(new);
        inner.opt_in.derived.insert(new.clone(), mapping);
        self.republish(&inner).await;
    }
}
