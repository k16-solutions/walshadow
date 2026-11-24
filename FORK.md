# Keeping fork code out of upstream's files

This repository is k16-solutions' fork of
[ClickHouse/walshadow](https://github.com/ClickHouse/walshadow). We merge
upstream's `main` regularly (see [docs/branching.md](docs/branching.md)). Every
line we change in a file upstream also changes is a merge conflict waiting to
happen, so fork code lives in files upstream never touches. Upstream's files
carry only short hooks that call into that code.

## The fork's features

Each feature the fork adds has one directory under `src/` holding all of its
code:

| Feature | Directory | What it holds |
|---------|-----------|---------------|
| Snowflake destination | `src/snowflake/` | Delivery, staging, durable state, recovery, destination selection (`snowflake::config`); the Snowflake branches of the tail, DDL, COPY loads, pending visibility, snapshot generations and greenfield bootstrap |
| Tenants | `src/tenant/` | The tenant registry, the WAL router (`tenant::router`), the `ctl tenant` handlers (`tenant::ctl`), the stream binary's tenant half; tenant activation, reserved destinations and `tenant=` metrics |
| Standby reads | `src/standby/` | Bulk loads read from a standby (`[source] replica_host`): the backup window's WAL leg and standby COPY reads |

Two pieces in `src/tenant/` apply to every opt-in, not only to tenants:
deferred opt-in backfills and the `[bootstrap] copy_concurrency` cap. They came
with tenant activation, which opts in hundreds of tables at once, so they live
with it.

## Who owns which file

Fork-owned files are ours. Edit them freely; upstream has no copy of them.

| Path | What it holds |
|------|---------------|
| `src/snowflake/**`, `src/tenant/**`, `src/standby/**` | The fork's features (see [above](#the-forks-features)) |
| `tests/snowflake_*.rs`, `tests/tenant_*.rs`, `tests/standby_*.rs` | Fork integration tests |
| `bench/src/snowflake.rs`, `bench/src/qualification.rs`, `bench/src/bin/snowflake_qualify.rs` | Snowflake benchmarks |
| `charts/walshadow/**`, `.github/workflows/chart.yml` | Helm chart |
| `config/snowflake.toml`, `docs/snowflake.md`, `docs/tenants.md`, `docs/branching.md`, `plans/snowflake*.md`, `FORK.md` | Fork config examples and docs |

Every other file is upstream-owned. Change it only to add a hook, and keep the
hook as small as you can.

## Inside a feature directory

```
src/<feature>/                 crate::<feature>: the feature's own modules
src/<feature>/ext/<path>.rs    extends the upstream module at src/<path>.rs
src/<feature>/bin/<bin>/*.rs   the feature's own modules of a binary
```

- **`src/<feature>/` itself** is an ordinary module, declared in `src/lib.rs`
  under the `k16 fork` comment. Code that needs no private access to an
  upstream module goes here.
- **`ext/`** holds code that does need that access (see
  [below](#ext-extending-an-upstream-module)). Its files are not part of
  `crate::<feature>`.
- **`bin/`** holds a binary's modules that belong to the feature. The binary's
  `main.rs` declares each one with `#[path]`, as `src/bin/stream/main.rs` does
  with `src/tenant/bin/stream/tenant.rs`.

A feature with no modules of its own, like `standby`, has only `ext/`.

To add a feature, create `src/<feature>/`, add `pub mod <feature>;` to the
`k16 fork` block in `src/lib.rs` if it has modules of its own, and list it in
the tables above.

## `ext/`: extending an upstream module

A file under `src/<feature>/ext/` mirrors the upstream module it extends. For
example, `src/snowflake/ext/emit/ch_ddl.rs` extends `src/emit/ch_ddl.rs`. The
upstream file declares it as a child module named after the feature, in one
place, at the end of the file:

```rust
// k16 fork: Snowflake DDL and tenant destination scopes (see FORK.md)
#[path = "../snowflake/ext/emit/ch_ddl.rs"]
mod snowflake;
#[path = "../tenant/ext/emit/ch_ddl.rs"]
mod tenant;
```

`#[path]` is relative to the declaring file's directory. When two features
extend the same module, each gets its own child, declared together. Because the
fork file is a child module, it can use the parent's private items. It starts
with `use super::*;` and can hold:

- **Inherent methods**: `impl DdlApplicator { ... }` works from the child and can
  read private fields. A method the parent calls needs `pub(super)`; a method
  that was already `pub` stays `pub`. `pub(super)` also reaches the other
  features' children of the same module: `tenant::copy_in_slot` calls the
  standby retry in `copy_backfill`'s `standby` child.
- **Free functions and types**: the parent calls them as
  `snowflake::name(...)`. When code outside the module used the old path,
  re-export the item with `pub use snowflake::Name;` next to the `mod`
  line.
- **Unit tests**: put them in the child's own `#[cfg(test)] mod tests`. Shared
  relation fixtures are in `crate::snowflake::test_support`, because upstream
  test modules keep theirs private. If a test needs an upstream test module's
  private fixtures, `include!` it from inside that module, as `src/config.rs`
  does with `src/<feature>/ext/config/tests.rs`. rustfmt skips included files,
  so run `rustfmt --edition 2024` on those by hand.

## Writing hooks

- **One call per branch point.** Write
  `if let Some(tail) = snowflake::snowflake_tail(...) { return Ok(tail); }`,
  not the whole Snowflake branch inline.
- **Run the hook's condition inside the fork file.** A hook that checks
  `emitter.snowflake` from the fork file runs on every path, so the fork file
  gets line coverage even in CI without Snowflake. CI fails any file with 0%
  coverage.
- **Don't add positional parameters to upstream functions.** A new `bool`
  argument means editing every upstream call site and test, now and after every
  sync. For example, `neutral_values` in `emit::pipeline::bootstrap::drain`
  touches 18 test calls. Add a new fork function instead, or carry the setting
  in a struct the function already takes.
- **Don't change upstream signatures.** Making `MappingHandle::publish` and
  `mutate` `async` added `.await` to upstream code and tests that upstream keeps
  editing.
- **Don't fix upstream bugs locally.** Fix them upstream and merge, or
  cherry-pick upstream's exact commit. Our own Rust 1.100 fix, `f05f956`, did
  the same job as upstream's `6ab7b59` in different words. That one duplicate
  makes 13 test files conflict on the next sync.
- **New struct fields on upstream types** are a hook too. Put them at the end of
  the struct. Where it reads cleanly, group several in one fork-owned struct.

## Integration tests

Add fork tests in new `tests/<feature>_<topic>.rs` files, not at the end of an
upstream test file. Upstream appends tests there too, so both sides edit the
same lines. Copy the few fixtures you need (`make_cluster`, `StopOnDrop`, …).
Duplicated fixtures cost less than a conflict on every sync.

## Resolving a conflict on a hook

Hooks are short on purpose, so most conflicts on them are "upstream changed the
lines around our call". Take upstream's version of the surrounding code and
re-add the hook. When upstream appends something at the end of a file and
that clashes with the `mod <feature>;` declarations, keep both.

## Known hot spots

Moving code doesn't fix these. Each needs a design change:

- **`src/bin/stream/session.rs`**: tenants replaced upstream's per-database
  setup with `tenant::SessionShared` and code in
  `src/tenant/bin/stream/tenant.rs`. Every upstream change to that setup must
  be ported there by hand. It is the largest conflict in every sync. Fix it by
  building tenants on upstream's setup functions instead of copies of them, or
  by upstreaming the tenant layer.
- **Snowflake branches inside upstream logic** (`reorder.rs`, `opt_in.rs`,
  `copy_backfill.rs`, `emit/pipeline/decode.rs`): a few lines each, but they
  sit inside code upstream rewrites often.
- **`emit/pipeline/bootstrap.rs`**: the `neutral_values` argument, see above.
