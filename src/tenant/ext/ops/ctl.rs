//! k16 fork: the `ctl tenant` verbs, tenant-scoped reads and writes and the
//! tenant listing, attached to `ops::ctl` as its `tenant` child module. See
//! FORK.md.

use super::*;

/// Read verbs carry `tenant = "<id>"`; config writes nest under
/// `[tenant.<id>]` so they land in that tenant's view
pub(super) fn scope_to_tenant(mut command: Command, id: &str) -> Command {
    match command.verb.as_str() {
        "apply" | "unset" if !command.reads_stdin => {
            let mut tenant = Table::new();
            tenant.insert(id.into(), Value::Table(std::mem::take(&mut command.body)));
            command.body.insert("tenant".into(), Value::Table(tenant));
        }
        "tables" | "columns" | "schemas" => {
            command.body.insert("tenant".into(), id.into());
        }
        _ => {}
    }
    command
}

#[derive(Debug, Subcommand)]
pub(super) enum TenantCommand {
    /// Every tenant with its database, phase and lag
    List,
    /// One tenant's declaration, passwords masked
    Show { id: String },
    /// Declare a tenant; it attaches, loads its tables and then streams.
    /// The spec holds the tenant's sections (`[snowflake]`, `[stream]`,
    /// `[table.*]`, …) as in a single-database config
    Add {
        id: String,
        #[arg(long)]
        dbname: String,
        /// Spec file; `-` reads stdin
        #[arg(long)]
        spec: Option<PathBuf>,
        #[arg(long, value_enum)]
        initial_load: Option<InitialLoad>,
    },
    /// Replace a tenant's spec. Credentials, role, warehouse and table rules
    /// apply live; a new database or destination identity re-attaches it
    Update {
        id: String,
        #[arg(long)]
        dbname: Option<String>,
        #[arg(long)]
        spec: Option<PathBuf>,
    },
    /// Stop and forget a tenant; its destination data stays
    Remove { id: String },
    /// Stop a tenant without forgetting it; it holds no WAL while detached
    Detach { id: String },
    /// Re-attach a detached tenant, reloading every table
    Attach { id: String },
}

impl TenantCommand {
    pub(super) fn into_command(self) -> Result<Command> {
        let id_body = |id: String| pair("id", id.into());
        match self {
            Self::List => Ok(sugar("tenants", Table::new())),
            Self::Show { id } => Ok(sugar("tenant-show", id_body(id))),
            Self::Remove { id } => Ok(sugar("tenant-remove", id_body(id))),
            Self::Detach { id } => {
                let mut body = id_body(id);
                body.insert("state".into(), "detached".into());
                Ok(sugar("tenant-state", body))
            }
            Self::Attach { id } => {
                let mut body = id_body(id);
                body.insert("state".into(), "active".into());
                Ok(sugar("tenant-state", body))
            }
            Self::Add {
                id,
                dbname,
                spec,
                initial_load,
            } => {
                let mut body = read_spec(spec.as_deref())?;
                body.insert("dbname".into(), dbname.into());
                if let Some(mode) = initial_load {
                    body.insert("initial_load".into(), mode.as_str().into());
                }
                Ok(sugar("tenant-put", tenant_block(&id, body)))
            }
            Self::Update { id, dbname, spec } => {
                let mut body = read_spec(spec.as_deref())?;
                match dbname {
                    Some(db) => {
                        body.insert("dbname".into(), db.into());
                    }
                    None if !body.contains_key("dbname") => anyhow::bail!(
                        "update replaces the whole spec: pass --dbname or put dbname in the spec"
                    ),
                    None => {}
                }
                Ok(sugar("tenant-put", tenant_block(&id, body)))
            }
        }
    }
}

pub(super) fn render_tenants(root: &Table) -> Option<String> {
    let rows = root.get("tenants")?.as_array()?;
    let cells: Vec<[String; 6]> = rows
        .iter()
        .filter_map(Value::as_table)
        .map(|t| {
            let num = |k: &str| {
                t.get(k)
                    .and_then(Value::as_integer)
                    .map_or(String::new(), |n| n.to_string())
            };
            [
                str_of(t, "id"),
                str_of(t, "dbname"),
                str_of(t, "desired"),
                str_of(t, "phase"),
                num("lag_bytes"),
                str_of(t, "ack_lsn"),
            ]
        })
        .collect();
    let header = [
        "TENANT",
        "DATABASE",
        "DESIRED",
        "PHASE",
        "LAG_BYTES",
        "ACK_LSN",
    ];
    let mut widths = header.map(str::len);
    for row in &cells {
        for (w, c) in widths.iter_mut().zip(row) {
            *w = (*w).max(c.len());
        }
    }
    let line = |cols: [&str; 6]| {
        cols.iter()
            .zip(widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut out = vec![line(header)];
    for row in &cells {
        out.push(line(row.each_ref().map(String::as_str)));
    }
    Some(out.join("\n"))
}
