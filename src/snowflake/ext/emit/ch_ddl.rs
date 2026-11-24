//! k16 fork: Snowflake DDL routing, attached to `emit::ch_ddl` as its
//! `snowflake` child module so the upstream file carries only hooks. See
//! FORK.md before moving code back into upstream-owned files.

use super::*;

use crate::snowflake::runtime::SnowflakeRuntime;

impl DdlApplicator {
    /// Resolve and validate a Snowflake opt-in route before any remote DDL.
    /// ClickHouse callers keep the resolver's existing mapping derivation.
    pub fn snowflake_opt_in_mapping(
        &mut self,
        desc: &RelDescriptor,
        db_override: Option<&str>,
        table_override: Option<&str>,
    ) -> Result<Option<TableMapping>, EmitterError> {
        self.refresh_config();
        let Some(runtime) = self.snowflake.as_ref() else {
            return Ok(None);
        };
        let expected = snowflake_target(desc, runtime);
        let settings = self.config.rules.settings(&desc.rel_name);
        for candidate in [db_override, settings.target_database.as_deref()] {
            if candidate.is_some_and(|value| value != expected.database.as_str()) {
                return Err(EmitterError::Config(format!(
                    "Snowflake database override for {} is unsupported",
                    desc.rel_name
                )));
            }
        }
        for candidate in [table_override, settings.target_table.as_deref()] {
            if candidate.is_some_and(|value| value != expected.table.as_str()) {
                return Err(EmitterError::Config(format!(
                    "Snowflake table override for {} is unsupported",
                    desc.rel_name
                )));
            }
        }
        let mapping = TableMapping {
            target: expected,
            columns: snowflake_columns(desc, &self.config.column_rules)?,
            tees: Vec::new(),
        };
        validate_snowflake_route(desc, &mapping, runtime)?;
        Ok(Some(mapping))
    }

    pub fn defer_snowflake_publication(&self, desc: &RelDescriptor) -> Result<(), EmitterError> {
        if let Some(runtime) = &self.snowflake {
            runtime
                .defer_publication(desc)
                .map_err(|e| EmitterError::Config(e.to_string()))?;
        }
        Ok(())
    }

    pub(super) async fn apply_snowflake(
        &mut self,
        event: &SchemaEvent,
        snowflake: &crate::snowflake::runtime::SnowflakeRuntime,
        commit_lsn: u64,
        cfg: &DdlConfig,
    ) -> Result<(), EmitterError> {
        match event {
            SchemaEvent::Added { desc } => self.apply_added(desc, cfg).await,
            SchemaEvent::Changed { old, new, diff } => {
                let renamed_table = old.rel_name != new.rel_name;
                let source_name = if renamed_table {
                    &old.rel_name
                } else {
                    &new.rel_name
                };
                let Some(mut mapped) = self.mapping_for(source_name) else {
                    self.stats.skipped += 1;
                    return Ok(());
                };
                if !diff.type_changes.is_empty() {
                    return Err(EmitterError::Config(format!(
                        "Snowflake schema change for {} includes unsupported type changes",
                        new.rel_name
                    )));
                }
                if renamed_table {
                    if self.mapping_for(&new.rel_name).is_some() {
                        return Err(EmitterError::Config(format!(
                            "Snowflake rename target {} is already mapped",
                            new.rel_name
                        )));
                    }
                    mapped = TableMapping {
                        target: snowflake_target(new, snowflake),
                        columns: snowflake_columns(new, &cfg.column_rules)?,
                        tees: Vec::new(),
                    };
                } else {
                    fold_diff_into_mapping(&mut mapped, new, diff, &[], &cfg.column_rules);
                }
                validate_snowflake_route(new, &mapped, snowflake)?;
                snowflake
                    .apply_schema_at(event, commit_lsn)
                    .await
                    .map_err(|e| EmitterError::Config(e.to_string()))?;
                if renamed_table {
                    if let Some(resolver) = &self.resolver {
                        resolver
                            .rename_snowflake_mapping(&old.rel_name, &new.rel_name, mapped)
                            .await;
                    } else {
                        self.mapping
                            .mutate(|m| {
                                let map = Arc::make_mut(m);
                                map.remove(&old.rel_name);
                                map.insert(new.rel_name.clone(), Arc::new(mapped));
                            })
                            .await;
                    }
                } else {
                    self.fold_mapping_diff(new, diff, &[], cfg).await;
                }
                self.stats.alters_applied += 1;
                Ok(())
            }
            SchemaEvent::Dropped { rel_name, .. } => {
                if self.mapping_for(rel_name).is_none() {
                    self.stats.skipped += 1;
                    return Ok(());
                }
                match cfg.drop_strategy_for(&rel_name.namespace) {
                    DropTableStrategy::Drop => {
                        snowflake
                            .apply_schema_at(event, commit_lsn)
                            .await
                            .map_err(|e| EmitterError::Config(e.to_string()))?;
                        self.forget_mapping(rel_name).await;
                        self.stats.drops_applied += 1;
                    }
                    _ => self.stats.skipped += 1,
                }
                Ok(())
            }
        }
    }

    /// A source table rename changes two route keys in one WAL interval.
    pub async fn predict_route_effects(
        &mut self,
        event: &SchemaEvent,
        mapping: &MappingSnapshot,
        config: Option<&ResolvedConfig>,
    ) -> Result<Vec<(RelName, Option<TableMapping>)>, EmitterError> {
        if let (Some(runtime), SchemaEvent::Changed { old, new, diff }) = (&self.snowflake, event)
            && old.rel_name != new.rel_name
            && mapping.contains_key(&old.rel_name)
        {
            if mapping.contains_key(&new.rel_name) {
                return Err(EmitterError::Config(format!(
                    "Snowflake rename target {} is already mapped",
                    new.rel_name
                )));
            }
            if !diff.type_changes.is_empty() {
                return Err(EmitterError::Config(format!(
                    "Snowflake schema change for {} includes unsupported type changes",
                    new.rel_name
                )));
            }
            let cfg = self.plan_config(config);
            let next = TableMapping {
                target: snowflake_target(new, runtime),
                columns: snowflake_columns(new, &cfg.column_rules)?,
                tees: Vec::new(),
            };
            validate_snowflake_route(new, &next, runtime)?;
            return Ok(vec![
                (old.rel_name.clone(), None),
                (new.rel_name.clone(), Some(next)),
            ]);
        }
        if let Some(runtime) = self.snowflake.clone() {
            // Planning surfaces Snowflake's refusals; `predict_route_mapping`
            // is infallible upstream, so it can only drop them
            let excluded = match event {
                SchemaEvent::Added { desc } | SchemaEvent::Changed { new: desc, .. }
                    if !mapping.contains_key(&desc.rel_name) =>
                {
                    self.is_excluded(desc).await
                }
                _ => false,
            };
            let cfg = self.plan_config(config);
            return Ok(
                predict_snowflake_route_effect(&cfg, mapping, event, excluded, &runtime)?
                    .into_iter()
                    .collect(),
            );
        }
        Ok(self
            .predict_route_mapping(event, mapping, config)
            .await
            .into_iter()
            .collect())
    }

    /// ClickHouse connection; Snowflake mode has none
    pub(super) fn ch(&mut self) -> Result<&mut ChConn, EmitterError> {
        self.client.as_mut().ok_or_else(|| {
            EmitterError::Config("ClickHouse DDL attempted in Snowflake mode".into())
        })
    }

    /// Mapped relation: validate the route and create the Snowflake table
    pub(super) async fn snowflake_ensure_mapped(
        &mut self,
        desc: &RelDescriptor,
        m: &TableMapping,
        snowflake: &SnowflakeRuntime,
    ) -> Result<(), EmitterError> {
        validate_snowflake_route(desc, m, snowflake)?;
        snowflake
            .ensure_table(desc)
            .await
            .map_err(|e| EmitterError::Config(e.to_string()))?;
        self.stats.creates_applied += 1;
        Ok(())
    }

    /// Unmapped relation: auto-create under the Snowflake target naming
    pub(super) async fn snowflake_add_unmapped(
        &mut self,
        desc: &RelDescriptor,
        cfg: &DdlConfig,
        snowflake: &SnowflakeRuntime,
    ) -> Result<(), EmitterError> {
        if !cfg.auto_creates(desc) {
            self.stats.skipped += 1;
            return Ok(());
        }
        let settings = cfg.rule_for(desc);
        let target = cfg.create_target(&settings, &desc.rel_name);
        let columns = snowflake_columns(desc, &cfg.column_rules)?;
        let expected = snowflake_target(desc, snowflake);
        if (settings.target_database.is_some() || settings.target_table.is_some())
            && target != expected
        {
            return Err(EmitterError::Config(format!(
                "Snowflake table target override for {} is unsupported",
                desc.rel_name
            )));
        }
        snowflake
            .ensure_table(desc)
            .await
            .map_err(|e| EmitterError::Config(e.to_string()))?;
        self.stats.creates_applied += 1;
        let mapping = TableMapping {
            target: expected,
            columns,
            tees: Vec::new(),
        };
        self.register_mapping(desc, mapping).await;
        Ok(())
    }

    /// [`Self::ensure_ch_table`] for Snowflake: validate, then create
    pub(super) async fn snowflake_ensure_table(
        &mut self,
        desc: &RelDescriptor,
        snowflake: &SnowflakeRuntime,
    ) -> Result<(), EmitterError> {
        snowflake_columns(desc, &self.config.column_rules)?;
        if let Some(mapping) = self.mapping_for(&desc.rel_name) {
            validate_snowflake_route(desc, &mapping, snowflake)?;
        }
        snowflake
            .ensure_table(desc)
            .await
            .map_err(|e| EmitterError::Config(e.to_string()))?;
        self.stats.creates_applied += 1;
        Ok(())
    }
}

fn snowflake_columns(
    desc: &RelDescriptor,
    rules: &ColumnRules,
) -> Result<Vec<ColumnMapping>, EmitterError> {
    let mut names = HashSet::default();
    let mut columns = Vec::new();
    for attr in desc.attributes.iter().filter(|a| !a.dropped) {
        let rule = rules.settings(&desc.rel_name, &attr.name);
        let target_name = rule.target_name.unwrap_or_else(|| attr.name.clone());
        if target_name != attr.name || rule.target_type.is_some() {
            return Err(EmitterError::Config(format!(
                "Snowflake column remapping/type overrides are not supported for {}.{}",
                desc.rel_name, attr.name
            )));
        }
        if target_name.is_empty() || !names.insert(target_name.to_ascii_uppercase()) {
            return Err(EmitterError::Config(format!(
                "invalid or duplicate Snowflake column name {} on {}",
                target_name, desc.rel_name
            )));
        }
        columns.push(ColumnMapping {
            src_attnum: attr.attnum,
            target_name,
            target_type: crate::snowflake::types::type_for(attr.type_oid, attr.typmod).sql(),
            type_pinned: false,
        });
    }
    Ok(columns)
}

fn snowflake_target(
    desc: &RelDescriptor,
    runtime: &crate::snowflake::runtime::SnowflakeRuntime,
) -> TableTarget {
    let namespace = runtime
        .config
        .schema_mapping
        .get(desc.rel_name.namespace.as_ref())
        .map(String::as_str)
        .unwrap_or(&desc.rel_name.namespace);
    TableTarget::new(namespace, &desc.rel_name.name)
}

fn validate_snowflake_route(
    desc: &RelDescriptor,
    mapping: &TableMapping,
    runtime: &crate::snowflake::runtime::SnowflakeRuntime,
) -> Result<(), EmitterError> {
    validate_complete_snowflake_mapping(desc, mapping)?;
    let expected = snowflake_target(desc, runtime);
    if mapping.target != expected {
        return Err(EmitterError::Config(format!(
            "Snowflake route for {} targets {} instead of {}",
            desc.rel_name, mapping.target, expected
        )));
    }
    for attr in desc.attributes.iter().filter(|a| !a.dropped) {
        if !mapping
            .columns
            .iter()
            .any(|c| c.src_attnum == attr.attnum && c.target_name == attr.name)
        {
            return Err(EmitterError::Config(format!(
                "Snowflake column remapping is not supported for {}.{}",
                desc.rel_name, attr.name
            )));
        }
    }
    Ok(())
}

pub(super) fn predict_snowflake_route_effect(
    cfg: &DdlConfig,
    mapping: &MappingSnapshot,
    event: &SchemaEvent,
    excluded: bool,
    runtime: &crate::snowflake::runtime::SnowflakeRuntime,
) -> Result<Option<(RelName, Option<TableMapping>)>, EmitterError> {
    if let SchemaEvent::Added { desc } = event {
        if mapping.contains_key(&desc.rel_name) || excluded || !cfg.auto_creates(desc) {
            return Ok(None);
        }
        let target = snowflake_target(desc, runtime);
        let columns = snowflake_columns(desc, &cfg.column_rules)?;
        return Ok(Some((
            desc.rel_name.clone(),
            Some(TableMapping {
                target,
                columns,
                tees: Vec::new(),
            }),
        )));
    }
    if let SchemaEvent::Changed { new, diff, .. } = event
        && !diff.type_changes.is_empty()
    {
        return Err(EmitterError::Config(format!(
            "Snowflake schema change for {} includes unsupported type changes",
            new.rel_name
        )));
    }
    let effect = predict_route_effect(cfg, mapping, event, excluded);
    if let SchemaEvent::Changed { new, .. } = event
        && let Some((_, Some(mapped))) = &effect
    {
        validate_snowflake_route(new, mapped, runtime)?;
    }
    Ok(effect)
}

fn validate_complete_snowflake_mapping(
    desc: &RelDescriptor,
    mapping: &TableMapping,
) -> Result<(), EmitterError> {
    let wanted: HashSet<i16> = desc
        .attributes
        .iter()
        .filter(|a| !a.dropped)
        .map(|a| a.attnum)
        .collect();
    let got: HashSet<i16> = mapping.columns.iter().map(|c| c.src_attnum).collect();
    if wanted != got || got.len() != mapping.columns.len() {
        return Err(EmitterError::Config(format!(
            "Snowflake mapping for {} would omit or duplicate a source column",
            desc.rel_name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{INT4OID, TEXTOID};
    use crate::snowflake::test_support::{attr, desc};

    #[test]
    fn snowflake_mapping_rejects_missing_and_duplicate_columns() {
        let descriptor = desc(
            "records",
            vec![attr(1, "id", INT4OID), attr(2, "value", TEXTOID)],
            Some(vec![1]),
        );
        let columns = snowflake_columns(&descriptor, &ColumnRules::default()).unwrap();
        assert_eq!(columns.len(), 2);
        let mut mapping = TableMapping {
            tees: Vec::new(),
            target: TableTarget::new("DB", "records"),
            columns,
        };
        validate_complete_snowflake_mapping(&descriptor, &mapping).unwrap();
        mapping.columns.pop();
        assert!(validate_complete_snowflake_mapping(&descriptor, &mapping).is_err());

        let duplicate = desc(
            "records",
            vec![attr(1, "ID", INT4OID), attr(2, "id", TEXTOID)],
            Some(vec![1]),
        );
        assert!(snowflake_columns(&duplicate, &ColumnRules::default()).is_err());
    }
}
