//! k16 fork: the `[bootstrap] copy_concurrency` cap on COPY initial loads,
//! attached to `backfill::copy_backfill` as its `tenant` child module. A
//! tenant activating hundreds of tables would otherwise open one source
//! session per table at once. See FORK.md.

use super::*;

/// COPY initial loads in flight when `[bootstrap] copy_concurrency` is unset
pub(super) const DEFAULT_COPY_CONCURRENCY: usize = 8;

impl CopyBackfiller {
    /// One COPY load under a `[bootstrap] copy_concurrency` slot
    pub(super) async fn copy_in_slot(
        &self,
        desc: &Arc<RelDescriptor>,
        s_lsn: Pos<Snapshot>,
    ) -> anyhow::Result<CopyOutcome> {
        match self.copy_slots.acquire().await {
            Ok(_slot) => self.copy_retrying_conflicts(desc, s_lsn).await,
            Err(e) => Err(anyhow::anyhow!("COPY slots closed: {e}")),
        }
    }
}
