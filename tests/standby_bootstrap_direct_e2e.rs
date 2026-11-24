//! k16 fork: `DirectSource` with a WAL leg, the standby layout. Split out
//! of upstream's `bootstrap_direct_e2e.rs` so syncs don't collide on it;
//! see FORK.md.
//!
//! Skipped silently when `initdb` is not on `$PATH`.

#[path = "common/tools.rs"]
mod tools;
use tools::pg_available;

#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::io::Write;
use std::time::Duration;

use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::backfill_bootstrap::{
    BootstrapConfig, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source_direct::{DirectSource, WalLeg};
use walshadow::shadow::{Shadow, ShadowConfig};

/// Same row budget as the object-store sibling: one heap page worth so
/// the page walker has guaranteed bytes without a multi-page sweep.
const N_ROWS: i32 = 64;

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = ports::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow bootstrap direct source overrides").unwrap();
    writeln!(f, "wal_level = replica").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// `WAL false` backup with the window streamed beside it, the standby
/// layout: the landed `pg_wal` must hold the source's own bytes from the
/// start segment through `end_lsn`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_source_wal_leg_streams_backup_window_into_pg_wal() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_source_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "CREATE TABLE t (id int PRIMARY KEY, payload text NOT NULL);\n\
             INSERT INTO t SELECT g, repeat('x', 200) FROM generate_series(1, {N_ROWS}) g;\n"
        ))
        .expect("apply workload");

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    let conninfo = format!(
        "host={socket_host} port={} user=postgres dbname=postgres",
        source.config().port,
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .expect("source tokio_postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let catalog_map = seed_in_snapshot(&client).await.expect("seed catalog");

    let pgcfg = PgConfig {
        host: socket_host,
        port: source.config().port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-bootstrap-wal-leg".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    };
    let opts = BaseBackupOpts {
        label: "bootstrap-wal-leg".into(),
        fast_checkpoint: true,
        no_verify_checksums: false,
        max_rate_kib: None,
        wal: false,
    };
    let direct = DirectSource::new(pgcfg.clone(), opts).with_wal_leg(WalLeg { source: pgcfg });

    // Writes racing the copy put records inside the window
    let writer = tokio::spawn(async move {
        for i in 0..50 {
            client
                .execute(
                    "UPDATE t SET payload = $1 WHERE id = 1",
                    &[&format!("u{i}")],
                )
                .await
                .unwrap();
        }
        client
    });
    let shadow_data = tmp.path().join("shadow-data");
    let cfg = BootstrapConfig::new(shadow_data.clone());
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, Box::new(direct), catalog_map, false);
    let mut observer = RecordingObserver::default();
    let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
    drain_res.expect("drain task error");
    let outcome = pump_res
        .expect("pump task panicked")
        .expect("pump task returned error");
    writer.await.unwrap();
    let (start, end) = (outcome.start.start_lsn, outcome.end.end_lsn);
    assert!(end >= start);

    const SEG: u64 = 16 << 20;
    let tli = outcome.start.timeline;
    let mut lsn = start - start % SEG;
    while lsn < end {
        let name = format!(
            "{tli:08X}{:08X}{:08X}",
            lsn >> 32,
            (lsn & 0xFFFF_FFFF) / SEG
        );
        let landed = fs::read(shadow_data.join("pg_wal").join(&name))
            .unwrap_or_else(|e| panic!("segment {name} missing from landed pg_wal: {e}"));
        let original = fs::read(source.config().data_dir.join("pg_wal").join(&name)).unwrap();
        assert_eq!(landed.len() as u64, SEG, "segment {name} not full size");
        let upto = (end - lsn).min(SEG) as usize;
        assert!(
            landed[..upto] == original[..upto],
            "segment {name} differs from the source through end_lsn",
        );
        lsn += SEG;
    }
}

#[derive(Default)]
struct RecordingObserver {
    tuples: Vec<walshadow::heap_decoder::CommittedTuple>,
    xact_end_calls: u32,
}

impl walshadow::decoder_sink::TupleObserver for RecordingObserver {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a walshadow::heap_decoder::CommittedTuple,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), walshadow::decoder_sink::DecoderSinkError>>
                + Send
                + 'a,
        >,
    > {
        self.tuples.push(committed.clone());
        Box::pin(async { Ok(()) })
    }
    fn on_xact_end<'a>(
        &'a mut self,
        commit_lsn: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<u64, walshadow::decoder_sink::DecoderSinkError>>
                + Send
                + 'a,
        >,
    > {
        self.xact_end_calls += 1;
        Box::pin(async move { Ok(commit_lsn) })
    }
}
