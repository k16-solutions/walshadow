//! k16 fork: the standby endpoint for bulk reads (`[source] replica_host` /
//! `replica_user`), attached to `config` as its `standby` child module. See
//! FORK.md.

use super::*;

impl SourceConn {
    /// Standby endpoint for bulk reads, `None` when `[source]` names none
    pub fn replica_pg_config(&self) -> Option<PgConfig> {
        if self.replica_host.is_none() && self.replica_user.is_none() {
            return None;
        }
        let mut cfg = self.to_pg_config();
        if let Some(host) = &self.replica_host {
            cfg.host = host.clone();
        }
        if let Some(user) = &self.replica_user {
            cfg.user = user.clone();
        }
        Some(cfg)
    }
}
