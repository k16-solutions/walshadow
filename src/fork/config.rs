//! k16 fork: tenant activation, reserved ClickHouse destinations, standby
//! endpoints and Snowflake mapping moves, attached to `config` as its `fork`
//! child module. See FORK.md.

use super::*;

/// While priming, nothing is in scope whatever the config says; once primed,
/// the tables in scope at the tenant's start carry their initial load as
/// opt-ins, persisted by the tenant so a restart resumes unfinished loads
#[derive(Debug, Default, Clone)]
pub struct Activation {
    pub priming: bool,
    pub opt_ins: HashMap<RelName, TableRow>,
}

impl Activation {
    pub(super) fn apply(&self, base: &mut EmitterConfig) {
        if self.priming {
            // Table rules scope relations too (`[table.*] replicate = true`),
            // not only opt-ins: clear them all until the start scope publishes
            base.replicate_all = false;
            base.table_opt_ins.clear();
            base.table_initial_loads.clear();
            base.table_entries.clear();
        }
        for (rel, row) in &self.opt_ins {
            let entry = base
                .table_opt_ins
                .entry(rel.clone())
                .or_insert_with(|| row.clone());
            // A table rule naming no initial load takes the start scope's,
            // so `[tenant.<id>] initial_load` covers explicit tables too
            if entry.initial_load.is_none() {
                entry.initial_load.clone_from(&row.initial_load);
            }
        }
    }
}

impl SourceConn {
    /// Standby endpoint for bulk reads, `None` when `[source]` names none
    pub fn replica_pg_config(&self) -> Option<PgConfig> {
        if self.replica_host.is_none() && self.replica_user.is_none() {
            return None;
        }
        let mut cfg = self.to_pg_config();
        if let Some(host) = &self.replica_host {
            cfg.host = host.clone();
        }
        if let Some(user) = &self.replica_user {
            cfg.user = user.clone();
        }
        Some(cfg)
    }
}

impl ConfigResolver {
    /// Replace the activation layer; the next [`reload`](Self::reload)
    /// publishes it
    pub fn set_activation(&self, activation: Activation) {
        *self.activation.lock().unwrap() = activation;
    }

    /// What `base` would resolve to under this resolver's overlay and CLI
    /// layers, without publishing: lets a tenant decide its start scope from
    /// the full config before any of it takes effect
    pub async fn preview(&self, base: &EmitterConfig) -> ResolvedConfig {
        let inner = self.inner.lock().await;
        let prev = self.tx.borrow().column_rules.clone();
        Self::resolve(base, &inner.overlay, &self.cli, &inner.opt_in, &prev).0
    }

    /// Serve tenant `id`: reloads see only its effective config
    pub fn bind_tenant(&self, id: &str) {
        let _ = self.tenant.set(id.to_string());
    }

    /// Bind before any mapping is published; seeded runtime rows obey the
    /// same destination reservations as later WAL updates
    pub fn reserve_destinations(
        &self,
        scopes: Arc<crate::tenants::ChScopes>,
    ) -> Result<(), EmitterError> {
        let _ = self.reserved.set(scopes);
        self.check_destinations(&self.tx.borrow())
    }

    pub(super) fn check_destinations(&self, resolved: &ResolvedConfig) -> Result<(), EmitterError> {
        let Some(scopes) = self.reserved.get() else {
            return Ok(());
        };
        // TOAST mirrors use each connection's default database even before
        // any user relation is mapped
        for conn in std::iter::once(&resolved.conn).chain(resolved.instances.values()) {
            if !scopes.contains(&crate::tenants::ch_scope(conn, &conn.database)) {
                return Err(EmitterError::Config(format!(
                    "{}:{} database {} is outside this tenant's reserved destinations; \
                     detach and re-attach after reserving it in the tenant config",
                    conn.host, conn.port, conn.database,
                )));
            }
        }
        for mapping in resolved.tables.values() {
            for target in mapping.targets() {
                let conn = match target.instance.as_deref() {
                    Some(name) => resolved.instances.get(name).ok_or_else(|| {
                        EmitterError::Config(format!("unknown ClickHouse instance `{name}`"))
                    })?,
                    None => &resolved.conn,
                };
                if !scopes.contains(&crate::tenants::ch_scope(conn, &target.database)) {
                    return Err(EmitterError::Config(format!(
                        "{} on {}:{} is outside this tenant's reserved destinations; \
                         detach and re-attach after reserving it in the tenant config",
                        target.sql(),
                        conn.host,
                        conn.port,
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn tenant(&self) -> Option<&str> {
        self.tenant.get().map(String::as_str)
    }

    /// Install a destination-validated opt-in mapping. Snowflake callers
    /// prepare this from source identity and neutral types before remote DDL,
    /// then publish it through the same resolver layer as CH opt-ins.
    pub async fn materialize_prepared_opt_in(&self, desc: &RelDescriptor, mapping: TableMapping) {
        let mut inner = self.inner.lock().await;
        let rel = desc.rel_name.clone();
        inner.opt_in.mappings.insert(rel.clone(), mapping);
        inner.opt_in.derived.remove(&rel);
        inner.opt_in.excluded.remove(&rel);
        inner.opt_in.pending_decl.remove(&rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_in_total.fetch_add(1, Ordering::Relaxed);
        self.republish(&inner).await;
    }

    /// Move a Snowflake relation's source name at a fenced DDL barrier.
    /// Suppress the old configured name so a later republish cannot restore
    /// a stale route to the renamed warehouse view.
    pub async fn rename_snowflake_mapping(
        &self,
        old: &RelName,
        new: &RelName,
        mapping: TableMapping,
    ) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.mappings.remove(old);
        inner.opt_in.derived.remove(old);
        inner.opt_in.pending_decl.remove(old);
        inner.opt_in.excluded.insert(old.clone());
        inner.opt_in.excluded.remove(new);
        inner.opt_in.derived.insert(new.clone(), mapping);
        self.republish(&inner).await;
    }
}
