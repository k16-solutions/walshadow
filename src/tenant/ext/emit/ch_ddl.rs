//! k16 fork: tenant destination scopes and live boot knobs, attached to
//! `emit::ch_ddl` as its `tenant` child module. See FORK.md.

use super::*;

/// Boot knobs for a refresh. `replicate_all` reads the live snapshot instead:
/// a tenant attaches with nothing in scope and widens once primed
pub(super) fn live_boot(config: &DdlConfig, snap: &ResolvedConfig) -> DdlBoot {
    DdlBoot {
        replicate_all: snap.replicate_all,
        ..config.boot()
    }
}

impl DdlApplicator {
    /// Refuse ClickHouse routes outside `scopes`, the databases this tenant's
    /// config reserves for it
    pub fn reserve_destinations(&mut self, scopes: Arc<crate::tenant::ChScopes>) {
        self.reserved = Some(scopes);
    }
}
