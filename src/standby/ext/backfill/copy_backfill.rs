//! k16 fork: COPY initial loads read from a standby (`[source] replica_host`
//! / `replica_user`), attached to `backfill::copy_backfill` as its `standby`
//! child module. See FORK.md.

use super::*;

use crate::pg::is_recovery_conflict;

/// A standby this far behind `S` is not one to load from
pub(super) const REPLICA_REPLAY_WAIT: Duration = Duration::from_secs(600);
/// Loads a standby's recovery conflicts cancel, re-run in place
const RECOVERY_CONFLICT_ATTEMPTS: u32 = 5;

impl CopyBackfiller {
    /// Endpoint COPY reads from: the live `[source]` replica when one is set,
    /// else the boot replica, else [`Self::source_pg`]
    pub(super) fn copy_pg(&self) -> PgConfig {
        let live = self.config_rx.as_ref().map(|rx| rx.borrow().source.clone());
        match live {
            Some(conn) if !conn.host.is_empty() => conn
                .replica_pg_config()
                .or_else(|| self.replica_pg.clone())
                .unwrap_or_else(|| conn.to_pg_config()),
            _ => self.replica_pg.clone().unwrap_or_else(|| self.pg.clone()),
        }
    }

    /// COPY reads this standby instead of the primary: `[source]
    /// replica_host` / `replica_user` for this backfiller's database
    pub fn with_replica(mut self, replica: Option<PgConfig>) -> Self {
        self.replica_pg = replica;
        self
    }

    /// One COPY load, re-run in place when a standby's replay cancels it
    pub(super) async fn copy_retrying_conflicts(
        &self,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
    ) -> anyhow::Result<CopyOutcome> {
        let mut attempt = 1;
        loop {
            match self.copy_once(desc, s_lsn).await {
                Err(e) if attempt < RECOVERY_CONFLICT_ATTEMPTS && is_recovery_conflict(&e) => {
                    tracing::warn!(
                        target: "walshadow::backfill",
                        qname = %desc.rel_name,
                        attempt,
                        error = %format!("{e:#}"),
                        "standby cancelled COPY for replay; re-running the load",
                    );
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                res => break res,
            }
        }
    }
}
