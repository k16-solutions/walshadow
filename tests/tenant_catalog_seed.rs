//! k16 fork: `seed_all_databases` against a live PG with several databases.
//! Split out of upstream's `catalog_seed.rs` so syncs don't collide on its
//! tail; see FORK.md.
//!
//! Skipped silently when `initdb` is not on `$PATH`.

#[path = "common/tools.rs"]
mod tools;
use tools::pg_available;

#[path = "common/ports.rs"]
mod ports;

use std::time::Duration;

use tokio_postgres::NoTls;
use walshadow::catalog_tracker::CatalogTracker;
use walshadow::pg::socket_conninfo;
use walshadow::shadow::{Shadow, ShadowConfig};

fn make_cluster(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

async fn connect(sh: &Shadow) -> tokio_postgres::Client {
    let cfg = sh.config();
    let conninfo = socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// A catalog another database rotated before attach must be seeded too:
/// shadow replays every database's catalogs, and bootstrap landing keeps
/// only files the tracker calls catalog
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_all_databases_covers_rotated_catalogs_of_other_databases() {
    use walrus::pg::replication::conn::PgConfig;
    use walrus::pg::replication::tls::{SslMode, TlsParams};

    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_cluster(&tmp, ports::PG_SOURCE_PORT);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };
    sh.psql_one("CREATE DATABASE tenant_b").expect("create db");
    let tenant_b: u32 = sh
        .psql_one("SELECT oid::int8 FROM pg_database WHERE datname = 'tenant_b'")
        .unwrap()
        .parse()
        .unwrap();

    let cfg = sh.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-test".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    };
    let mut b_cfg = pgcfg.clone();
    b_cfg.database = "tenant_b".into();
    let b = walshadow::source_feed::open_sql_client(&b_cfg)
        .await
        .expect("connect tenant_b");
    // pg_description is unmapped: its rotation lands only in pg_class
    b.batch_execute("VACUUM FULL pg_description")
        .await
        .expect("vacuum full");
    let rotated: u32 = b
        .query_one(
            "SELECT pg_relation_filenode('pg_description'::regclass)::int8",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0) as u32;
    assert!(rotated >= 16384, "rotation must escape the < 16384 rule");

    let primary = connect(&sh).await;
    let mut only_followed = CatalogTracker::new();
    only_followed
        .seed_from_source(&primary)
        .await
        .expect("seed");
    assert!(
        !only_followed.is_catalog(tenant_b, rotated),
        "followed-database seed alone misses the other database's rotation",
    );

    let mut tracker = CatalogTracker::new();
    walshadow::source_feed::seed_all_databases(&mut tracker, &primary, &pgcfg)
        .await
        .expect("seed all");
    assert!(
        tracker.is_catalog(tenant_b, rotated),
        "rotated pg_description {rotated} of tenant_b must be catalog",
    );
}
