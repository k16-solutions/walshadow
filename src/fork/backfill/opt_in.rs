//! k16 fork: opt-in backfills deferred until a batch's mappings publish,
//! attached to `backfill::opt_in` as its `fork` child module. See FORK.md.

use super::*;

/// Backfills opt-ins requested, started only once every mapping of a batch
/// of opt-ins is published: a running backfill guards the mapping while it
/// prepares and publishes, so starting each one as its opt-in lands would
/// make every later opt-in wait on the earlier loads
#[derive(Default)]
pub struct DeferredBackfills {
    pub(super) starts: Vec<(Arc<RelDescriptor>, InitialLoadMode, u64)>,
    pub(super) opt_outs: Vec<RelName>,
}

impl DeferredBackfills {
    pub fn len(&self) -> usize {
        self.starts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.starts.is_empty() && self.opt_outs.is_empty()
    }

    pub async fn start(self, b: &Arc<dyn Backfiller>) {
        for rel in &self.opt_outs {
            b.note_opt_out(rel).await;
        }
        for (desc, mode, lsn) in self.starts {
            b.clone().note_opt_in(desc, mode, lsn).await;
        }
    }
}
