// k16 fork: tests for `config`'s the standby endpoint.
// `include!`d inside upstream's `config::tests` so they share its fixtures
// and imports; rustfmt skips included files, so run it on this one by hand.
// See FORK.md.

#[test]
fn source_conn_replica_overrides_host_and_user() {
    let root: toml::Table = toml::from_str(
        "[source]\nhost = \"primary\"\nuser = \"repl\"\npassword = \"pw\"\n\
         dbname = \"app\"\nreplica_user = \"repl|replica\"\n",
    )
    .unwrap();
    let conn = SourceConn::from_table(&root).unwrap();
    let replica = conn.replica_pg_config().expect("replica configured");
    assert_eq!(
        (replica.host.as_str(), replica.user.as_str()),
        ("primary", "repl|replica")
    );
    assert_eq!(replica.password.as_deref(), Some("pw"));
    assert_eq!(replica.database, "app");
    assert_eq!(conn.to_pg_config().user, "repl");

    let moved: toml::Table =
        toml::from_str("[source]\nhost = \"primary\"\nreplica_host = \"standby\"\n").unwrap();
    let replica = SourceConn::from_table(&moved).unwrap().replica_pg_config();
    assert_eq!(replica.map(|r| r.host), Some("standby".into()));

    let bare: toml::Table =
        toml::from_str("[source]\nhost = \"primary\"\nreplica_user = \"\"\n").unwrap();
    assert!(
        SourceConn::from_table(&bare)
            .unwrap()
            .replica_pg_config()
            .is_none()
    );
}
