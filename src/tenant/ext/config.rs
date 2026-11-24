//! k16 fork: tenant activation and reserved ClickHouse destinations,
//! attached to `config` as its `tenant` child module. See FORK.md.

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
        scopes: Arc<crate::tenant::ChScopes>,
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
            if !scopes.contains(&crate::tenant::ch_scope(conn, &conn.database)) {
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
                if !scopes.contains(&crate::tenant::ch_scope(conn, &target.database)) {
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
}
