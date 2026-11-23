//! k16 fork: Snowflake greenfield snapshot floor on the bootstrap marker,
//! attached to `backfill::bootstrap_marker` as its `fork` child module. See
//! FORK.md before moving code back into upstream-owned files.

use super::*;

impl BootstrapMarker {
    /// Pin the Snowflake generation floor before any page row can ship.
    pub async fn pin_snapshot_lsn(&mut self, dir: &Path, sampled: u64) -> Result<u64> {
        let floor = self.snapshot_lsn.unwrap_or(sampled);
        anyhow::ensure!(floor != 0, "Snowflake bootstrap snapshot floor is zero");
        if self.snapshot_lsn.is_none() {
            self.snapshot_lsn = Some(floor);
            self.write(dir).await?;
        }
        Ok(floor)
    }
}

/// A Snowflake retry starts a fresh physical generation, so it must reread
/// every page rather than use the ClickHouse extracted-checkpoint shortcut.
pub async fn restart_extraction(data_dir: &Path) -> Result<()> {
    discard_partial(data_dir).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snowflake_snapshot_floor_survives_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shadow");
        let mut marker = begin_attempt(&dir, None, Some("base_original".into()))
            .await
            .unwrap();
        assert_eq!(marker.pin_snapshot_lsn(&dir, 0x100).await.unwrap(), 0x100);
        let mut retry = begin_attempt(&dir, Some(marker), Some("base_original".into()))
            .await
            .unwrap();
        assert_eq!(retry.pin_snapshot_lsn(&dir, 0x200).await.unwrap(), 0x100);
        assert_eq!(
            BootstrapMarker::read(&dir).unwrap().unwrap().snapshot_lsn,
            Some(0x100)
        );
    }

    #[test]
    fn legacy_marker_has_no_snapshot_floor() {
        let marker: BootstrapMarker =
            toml::from_str("attempts = 1\nbackup_name = 'base_original'\n").unwrap();
        assert_eq!(marker.snapshot_lsn, None);
    }
}
