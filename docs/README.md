# walshadow documentation

Start with path matching your environment:

- [Quickstart](quickstart.md) runs PostgreSQL, ClickHouse, and walshadow
  locally with Docker Compose
- [Getting started](getting-started.md) connects existing PostgreSQL and
  ClickHouse databases

Use remaining guides as needed:

- [Snowflake destination](snowflake.md), configuration, recovery, and qualification
- [Tenants](tenants.md), many client databases through one daemon and slot
- [Configuration](configuration.md), connection settings and live control
- [Table selection](table-selection.md), replication scope and initial load
- [Several databases](multi-database.md), replicating several databases of one cluster as tenants
- [Destination tables](destination-tables.md), generated ClickHouse schema and
  query patterns
- [Schema changes](schema-changes.md), supported DDL behavior
- [Operations](operations.md), monitoring, restarts, and WAL retention
- [Helm chart](../charts/walshadow/README.md), running walshadow on Kubernetes
- [Planned source switchover](failover.md), controlled PostgreSQL primary moves
- [Limitations](limitations.md), unsupported behavior and safeguards
- [Development](development.md), builds, integration prerequisites, fixtures, and coverage
- [Branching and releases](branching.md), merge rules, upstream syncs, release tags, and GitHub rules
- [Upstream sync plan](upstream-sync.md), the biweekly merge from ClickHouse/walshadow and its automation

Read [architecture](../architecture/README.md) for system design and
[plans](../plans/INDEX.md) for unfinished work
