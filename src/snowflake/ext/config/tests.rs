// k16 fork: tests for `config`'s Snowflake mapping moves and destination selection.
// `include!`d inside upstream's `config::tests` so they share its fixtures
// and imports; rustfmt skips included files, so run it on this one by hand.
// See FORK.md.

#[tokio::test]
async fn snowflake_rename_moves_mapping_and_suppresses_old_name_on_republish() {
    let base = base_with("retain");
    let mapping = dummy_handles();
    let (resolver, _rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        None,
        toml::Table::new(),
        mapping.clone(),
        ResolverBoot::default(),
    );
    let old = RelName::new("public", "before");
    let new = RelName::new("public", "after");
    resolver
        .register_derived_mapping(
            &crate::schema::int4_rel(16400, "before", 'r', &[(1, "id")]),
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("default", "before"),
                columns: Vec::new(),
            },
        )
        .await;
    resolver
        .rename_snowflake_mapping(
            &old,
            &new,
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("default", "after"),
                columns: Vec::new(),
            },
        )
        .await;
    assert!(!mapping.with(|m| m.contains_key(&old)));
    assert!(mapping.with(|m| m.contains_key(&new)));
    resolver
        .apply_config_event(ConfigEvent::GlobalCleared)
        .await;
    assert!(!mapping.with(|m| m.contains_key(&old)));
    assert!(mapping.with(|m| m.contains_key(&new)));
}

#[tokio::test]
async fn reload_rejects_destination_change_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let base = EmitterConfig::default();
    let (resolver, rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        Some(path.clone()),
        toml::Table::new(),
        dummy_handles(),
        ResolverBoot::default(),
    );
    tokio::fs::write(&path, include_str!("../../../../config/snowflake.toml"))
        .await
        .unwrap();
    assert!(
        resolver
            .reload()
            .await
            .unwrap_err()
            .to_string()
            .contains("destination requires")
    );
    assert!(!rx.has_changed().unwrap());
}
