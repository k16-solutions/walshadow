//! Direct base-backup source. Wraps wal-rus's
//! `pg::replication::base_backup::run_base_backup` to drive walshadow's
//! [`BackupSource`] trait. Tablespace symlinks ride inside the data-dir
//! archive in PG protocol order, surfacing as `FileKind::Symlink`.
//!
//! Single-pass: seed the page-walk
//! [`CatalogMap`](crate::backfill::backup_page_walk::CatalogMap) from source PG
//! before this runs so all routing decisions are known when bytes land.
//! See [architecture/bootstrap.md](../../architecture/bootstrap.md).
//!
//! A standby can serve the data files while the backup window's WAL streams
//! from the primary into `pg_wal` beside them ([`WalLeg`]).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use walrus::pg::backup::{format_pg_lsn, tarball};
use walrus::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, run_base_backup,
};
use walrus::pg::replication::conn::{PgConfig, ReplicationConn};

use crate::backfill::backup_source::{
    BackupSink, BackupSource, EndInfo, PumpStats, PumpTarget, StartInfo, pump_tar_to_sink,
};

/// Replication-protocol BASE_BACKUP issuer
pub struct DirectSource {
    pub source: PgConfig,
    pub opts: BaseBackupOpts,
    pub wal_leg: Option<WalLeg>,
}

impl DirectSource {
    pub fn new(source: PgConfig, opts: BaseBackupOpts) -> Self {
        Self {
            source,
            opts,
            wal_leg: None,
        }
    }
}

#[async_trait]
impl BackupSource for DirectSource {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<dyn BackupSink>,
        stats: Arc<PumpStats>,
    ) -> Result<(StartInfo, EndInfo)> {
        let DirectSource {
            source,
            opts,
            wal_leg,
        } = *self;
        ensure!(
            wal_leg.is_none() || !opts.wal,
            "DirectSource: a WAL leg replaces the archive's WAL, request WAL false"
        );

        let conn = ReplicationConn::connect(&source)
            .await
            .context("DirectSource: connect to source PG for BASE_BACKUP")?;

        // depth=8 matches wal-rus internal sizing; producer back-pressures
        // on slow archive drain
        let (tx, mut rx) = mpsc::channel::<Result<BackupEvent>>(8);
        let pump = tokio::spawn(async move {
            run_base_backup(conn, opts, tx).await;
        });

        let mut start: Option<StartInfo> = None;
        let mut end: Option<EndInfo> = None;
        let pg_wal = data_dir.join("pg_wal");
        let target = PumpTarget::new(data_dir, sink.clone(), stats);
        let (stop_tx, stop_rx) = watch::channel(None);
        let mut leg: Option<tokio_util::task::AbortOnDropHandle<Result<u64>>> = None;

        while let Some(ev) = rx.recv().await {
            let ev = ev.context("DirectSource: BASE_BACKUP event channel")?;
            match ev {
                BackupEvent::Start(s) => {
                    let s = StartInfo {
                        start_lsn: s.start_lsn,
                        timeline: s.timeline,
                        tablespaces: s.tablespaces,
                    };
                    sink.start(&s).await?;
                    if let Some(wal_leg) = &wal_leg {
                        leg = Some(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                            standby::stream_window(
                                wal_leg.source.clone(),
                                s.timeline,
                                s.start_lsn,
                                pg_wal.clone(),
                                stop_rx.clone(),
                            ),
                        )));
                    }
                    start = Some(s);
                }
                BackupEvent::Archive { meta, body } => {
                    tracing::debug!(
                        target = "walshadow::backup_source_direct",
                        name = %meta.name,
                        oid = meta.oid,
                        "archive open",
                    );
                    // A leg that ends before the stop signal has failed; don't
                    // copy the rest of the cluster to learn that at the end
                    match leg.as_mut() {
                        Some(handle) => tokio::select! {
                            res = drive_archive(body, &target) => res?,
                            res = handle => {
                                let err = match res {
                                    Ok(Ok(_)) => anyhow::anyhow!("returned before the backup finished"),
                                    Ok(Err(e)) => e,
                                    Err(e) => e.into(),
                                };
                                return Err(err.context("DirectSource: backup-window WAL leg ended early"));
                            }
                        },
                        None => drive_archive(body, &target).await?,
                    }
                }
                BackupEvent::Finish(e) => {
                    let e = EndInfo {
                        end_lsn: e.end_lsn,
                        timeline: e.timeline,
                    };
                    if let Some(handle) = leg.take() {
                        stop_tx.send_replace(Some(e.end_lsn));
                        let through = handle
                            .await
                            .context("DirectSource: WAL leg join")?
                            .context("DirectSource: backup-window WAL leg")?;
                        tracing::info!(
                            target: "walshadow::backup_source_direct",
                            end_lsn = %format_pg_lsn(e.end_lsn),
                            through = %format_pg_lsn(through),
                            "backup-window WAL streamed into pg_wal",
                        );
                    }
                    sink.finish(&e).await?;
                    end = Some(e);
                }
            }
        }

        if let Err(e) = pump.await {
            bail!("DirectSource: BASE_BACKUP pump task panicked: {e:#}");
        }

        let start = start.context("DirectSource: no StartInfo emitted")?;
        let end = end.context("DirectSource: no EndInfo emitted")?;
        Ok((start, end))
    }
}

/// Drain one archive body through `pump_tar_to_sink`. wal-rus's
/// `ChannelReader` is `AsyncRead`, so the tar reader takes it directly, no
/// SyncIoBridge / spawn_blocking.
async fn drive_archive(
    body: mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    target: &PumpTarget,
) -> Result<()> {
    let reader = ChannelReader::new(body);
    let mut archive = tarball::Reader::new(reader);
    pump_tar_to_sink(&mut archive, target)
        .await
        .context("DirectSource: tar unpack")?;
    Ok(())
}

// k16 fork: the standby WAL leg (see FORK.md)
#[path = "../standby/ext/backfill/backup_source_direct.rs"]
mod standby;
pub use standby::WalLeg;
