// k16 fork: tests for `config`'s tenant activation and reserved destinations.
// `include!`d inside upstream's `config::tests` so they share its fixtures
// and imports; rustfmt skips included files, so run it on this one by hand.
// See FORK.md.

/// A tenant's start scope carries its initial load onto a table rule that
/// names none, and leaves one that does alone
#[test]
fn activation_fills_a_missing_initial_load_only() {
    let mut base = EmitterConfig::from_toml_str(
        "[ch]\n[table.public.early]\nreplicate = true\ntarget_table = \"early\"\n\
         [table.public.kept]\nreplicate = true\ninitial_load = \"none\"\n",
    )
    .unwrap();
    let row = |load: &str| TableRow {
        replicate: Some(true),
        initial_load: Some(load.into()),
        ..TableRow::default()
    };
    let early = RelName::new("public", "early");
    let kept = RelName::new("public", "kept");
    Activation {
        priming: false,
        opt_ins: [(early.clone(), row("copy")), (kept.clone(), row("copy"))]
            .into_iter()
            .collect(),
    }
    .apply(&mut base);
    let load = |rel: &RelName| base.table_opt_ins[rel].initial_load.clone();
    assert_eq!(load(&early).as_deref(), Some("copy"));
    assert_eq!(load(&kept).as_deref(), Some("none"));
    assert_eq!(
        base.table_opt_ins[&early].target_table.as_deref(),
        Some("early")
    );
}

#[tokio::test]
async fn tenant_runtime_retarget_keeps_both_published_maps_in_reserved_scope() {
    let base = base_with("retain");
    let mapping = dummy_handles();
    let (resolver, rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        None,
        toml::Table::new(),
        mapping.clone(),
        ResolverBoot::default(),
    );
    resolver
        .reserve_destinations(Arc::new(crate::tenant::clickhouse_scopes(&base)))
        .unwrap();
    let desc = rel_desc("public", "events");
    resolver.materialize_opt_in(&desc, None, None).await;
    let prior = rx.borrow().clone();
    resolver
        .apply_config_event(ConfigEvent::TableUpserted {
            rel: desc.rel_name.clone(),
            row: TableRow {
                target_database: Some("another_tenant".into()),
                ..Default::default()
            },
        })
        .await;
    assert!(Arc::ptr_eq(&prior, &rx.borrow()));
    assert_eq!(resolver.rejections(), 1);
    assert_eq!(
        mapping.snapshot()[&desc.rel_name].target.database,
        "default"
    );
    resolver
        .apply_config_event(ConfigEvent::TableUpserted {
            rel: desc.rel_name.clone(),
            row: TableRow {
                target_table: Some("renamed".into()),
                ..Default::default()
            },
        })
        .await;
    assert_eq!(rx.borrow().tables[&desc.rel_name].target.table, "renamed");
    assert_eq!(resolver.rejections(), 0);
}

#[tokio::test]
async fn tenant_seed_and_tees_cannot_publish_unreserved_routes() {
    let base = base_with("retain");
    let (resolver, rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        None,
        toml::Table::new(),
        dummy_handles(),
        ResolverBoot::default(),
    );
    let desc = rel_desc("public", "events");
    resolver
        .materialize_opt_in(&desc, Some("another_tenant".into()), None)
        .await;
    assert!(
        resolver
            .reserve_destinations(Arc::new(crate::tenant::clickhouse_scopes(&base)))
            .is_err()
    );
    assert_eq!(
        rx.borrow().tables[&desc.rel_name].target.database,
        "another_tenant"
    );

    let tee_base = EmitterConfig::from_toml_str(
        "[ch]\n[table.public.events]\nreplicate = true\n\
         tee = [{ database = 'another_tenant', table = 'audit' }]\n",
    )
    .unwrap();
    let (resolver, rx) = ConfigResolver::new(
        &tee_base,
        CliOverrides::default(),
        None,
        toml::Table::new(),
        dummy_handles(),
        ResolverBoot::default(),
    );
    resolver
        .reserve_destinations(Arc::new(crate::tenant::clickhouse_scopes(&base)))
        .unwrap();
    let mapping = TableMapping {
        target: TableTarget::new("default", "events"),
        columns: Vec::new(),
        tees: vec![crate::mapping::Tee {
            database: Some("another_tenant".into()),
            table: "audit".into(),
            instance: None,
            engine: None,
            order_by: Vec::new(),
            primary_key: Vec::new(),
        }],
    };
    resolver.register_derived_mapping(&desc, mapping).await;
    assert!(rx.borrow().tables.is_empty());
    assert_eq!(resolver.rejections(), 1);
}

#[tokio::test]
async fn tenant_reload_cannot_repoint_an_empty_pipeline_outside_reserved_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let base = base_with("retain");
    let (resolver, rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        Some(path.clone()),
        toml::Table::new(),
        dummy_handles(),
        ResolverBoot::default(),
    );
    resolver
        .reserve_destinations(Arc::new(crate::tenant::clickhouse_scopes(&base)))
        .unwrap();
    let prior = rx.borrow().clone();
    tokio::fs::write(&path, "[ch]\nhost = 'another_tenant'\n")
        .await
        .unwrap();
    let err = resolver.reload().await.unwrap_err().to_string();
    assert!(
        err.contains("outside this tenant's reserved destinations"),
        "{err}"
    );
    assert!(Arc::ptr_eq(&prior, &rx.borrow()));
    assert!(rx.borrow().tables.is_empty());
}

#[tokio::test]
async fn tenant_reload_rejects_shared_destinations_before_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let config = |database: &str| {
        format!(
            "[source]\ndbname = 'postgres'\n[tenants]\n\
         [tenant.a]\ndbname = 'app'\n[tenant.a.ch]\nhost = 'ch'\ndatabase = 'a'\n\
         [tenant.b]\ndbname = 'other'\n[tenant.b.ch]\nhost = 'ch'\ndatabase = '{database}'\n",
        )
    };
    let root: toml::Table = toml::from_str(&config("b")).unwrap();
    let tenants = crate::tenant::TenantsConfig::from_table(&root)
        .unwrap()
        .unwrap();
    let base =
        EmitterConfig::from_table(&tenants.get("a").unwrap().effective_table(&root)).unwrap();
    let (resolver, rx) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        Some(path.clone()),
        toml::Table::new(),
        dummy_handles(),
        ResolverBoot::default(),
    );
    resolver.bind_tenant("a");
    tokio::fs::write(&path, config("b")).await.unwrap();
    resolver.reload().await.unwrap();
    let prior = rx.borrow().clone();
    tokio::fs::write(&path, config("a")).await.unwrap();
    let err = resolver.reload().await.unwrap_err().to_string();
    assert!(err.contains("both reach ClickHouse database"), "{err}");
    assert!(Arc::ptr_eq(&prior, &rx.borrow()));
}
