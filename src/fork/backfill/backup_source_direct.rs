//! k16 fork: stream the backup window's WAL from the primary while a
//! standby serves the data files, attached to `backfill::backup_source_direct`
//! as its `fork` child module. See FORK.md.

use super::*;

use std::path::Path;

use walrus::pg::wal::segment::SegmentName;

use crate::record::WAL_SEG_SIZE;
use crate::source::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use crate::source::timeline::history_filename;

/// Backup-window WAL streamed live into the landed `pg_wal`, the
/// `pg_basebackup -X stream` shape, for a BASE_BACKUP run with `WAL false`.
/// A standby recycles the window's segments long before a multi-hour copy
/// ends; the primary's slot keeps them, and reading them as they are written
/// costs the primary no disk reads
#[derive(Clone)]
pub struct WalLeg {
    /// Server holding the window's WAL, normally the slot's primary
    pub source: PgConfig,
}

impl DirectSource {
    pub fn with_wal_leg(mut self, leg: WalLeg) -> Self {
        self.wal_leg = Some(leg);
        self
    }
}

/// Stream `[start_lsn, end)` from `source` into segment files under `pg_wal`,
/// `end` arriving on `stop` once the backup finishes. Slotless: the caller's
/// slot already retains the window, and a slotless status never moves it.
/// Returns the position written through
pub(super) async fn stream_window(
    source: PgConfig,
    timeline: u32,
    start_lsn: u64,
    pg_wal: PathBuf,
    mut stop: watch::Receiver<Option<u64>>,
) -> Result<u64> {
    tokio::fs::create_dir_all(&pg_wal)
        .await
        .with_context(|| format!("create {}", pg_wal.display()))?;
    let mut feed = SourceFeed::connect(&source)
        .await
        .context("connect WAL source")?;
    // The tar would have carried it; recovery on a promoted branch needs it
    if timeline > 1
        && let Some(history) = feed.timeline_history(timeline).await?
    {
        let path = pg_wal.join(history_filename(timeline));
        tokio::fs::write(&path, history)
            .await
            .with_context(|| format!("write {}", path.display()))?;
    }
    let begin = start_lsn - start_lsn % WAL_SEG_SIZE;
    feed.start_physical_replication(None, begin, timeline)
        .await
        .with_context(|| format!("START_REPLICATION at {}", format_pg_lsn(begin)))?;
    let mut seg = SegmentName {
        timeline,
        log_id: (begin >> 32) as u32,
        seg_no: ((begin & 0xFFFF_FFFF) / WAL_SEG_SIZE) as u32,
    };
    let mut page = vec![0u8; WAL_SEG_SIZE as usize];
    let mut next = begin;
    let mut frame = Vec::new();
    loop {
        if stop.borrow_and_update().is_some_and(|end| next >= end) {
            break;
        }
        let event = tokio::select! {
            biased;
            res = stop.changed() => {
                ensure!(res.is_ok(), "backup ended without a stop position");
                continue;
            }
            res = feed.next_event(StandbyStatus::collapsed(next), &mut frame) => res?,
        };
        let chunk = match event {
            SourceEvent::Wal(chunk) => chunk,
            SourceEvent::TimelineEnd => {
                bail!("source ended timeline {timeline} inside the backup window; it was promoted")
            }
            SourceEvent::Shutdown => bail!("source shut down inside the backup window"),
        };
        ensure!(
            chunk.start_lsn == next,
            "WAL frame at {} where {} was expected",
            format_pg_lsn(chunk.start_lsn),
            format_pg_lsn(next),
        );
        let mut data = chunk.data;
        while !data.is_empty() {
            let off = (next - seg.start_lsn(WAL_SEG_SIZE)) as usize;
            let n = data.len().min(page.len() - off);
            page[off..off + n].copy_from_slice(&data[..n]);
            data = &data[n..];
            next += n as u64;
            if off + n == page.len() {
                write_segment(&pg_wal, &seg, &page).await?;
                page.fill(0);
                seg = seg.next(WAL_SEG_SIZE);
            }
        }
    }
    // Zero tail past the last record, as a walreceiver's open segment has
    if next > seg.start_lsn(WAL_SEG_SIZE) {
        write_segment(&pg_wal, &seg, &page).await?;
    }
    Ok(next)
}

async fn write_segment(pg_wal: &Path, seg: &SegmentName, page: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let name = seg.format();
    let tmp = pg_wal.join(format!("{name}.tmp"));
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(page).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&tmp, pg_wal.join(&name))
        .await
        .with_context(|| format!("rename {name} into pg_wal"))?;
    Ok(())
}
