# Keeping fork code out of upstream's files

This repository is k16-solutions' fork of
[ClickHouse/walshadow](https://github.com/ClickHouse/walshadow). We merge
upstream's `main` regularly (see [docs/branching.md](docs/branching.md)). Every
line we change in a file upstream also changes is a merge conflict waiting to
happen, so fork code lives in files upstream never touches. Upstream's files
carry only short hooks that call into that code.

## Who owns which file

Fork-owned files are ours. Edit them freely; upstream has no copy of them.

| Path | What it holds |
|------|---------------|
| `src/fork/**` | Fork code that extends an upstream module (see [below](#srcfork-extending-an-upstream-module)) |
| `src/destination/**` | Snowflake destination |
| `src/tenants.rs`, `src/source/tenant_router.rs`, `src/ops/tenant_ctl.rs`, `src/bin/stream/tenant.rs` | Tenants |
| `tests/fork_*.rs`, `tests/multi_tenant_e2e.rs`, `tests/snowflake_live.rs`, `tests/destination_config.rs` | Fork tests |
| `bench/src/snowflake.rs`, `bench/src/qualification.rs`, `bench/src/bin/snowflake_qualify.rs` | Snowflake benchmarks |
| `charts/walshadow/**`, `.github/workflows/chart.yml` | Helm chart |
| `config/snowflake.toml`, `docs/snowflake.md`, `docs/tenants.md`, `docs/branching.md`, `docs/upstream-sync.md`, `plans/snowflake*.md`, `FORK.md`, `AGENTS.md` | Fork config examples and docs. `AGENTS.md` (and `.claude/CLAUDE.md`, which imports it) gives coding agents these rules |
| `scripts/fork/**`, `.github/workflows/upstream-sync.yml`, `.claude/**` | Upstream sync tooling and agent instructions. Upstream's `.gitignore` ignores `.claude/`, so add new files there with `git add -f` |

Every other file is upstream-owned. Change it only to add a hook, and keep the
hook as small as you can.

## `src/fork/`: extending an upstream module

A file under `src/fork/` mirrors the upstream module it extends. For example,
`src/fork/emit/ch_ddl.rs` extends `src/emit/ch_ddl.rs`. The upstream file
declares it as a child module in one place, at the end of the file:

```rust
// k16 fork: Snowflake DDL and tenant scopes live in src/fork/ (see FORK.md)
#[path = "../fork/emit/ch_ddl.rs"]
mod fork;
```

`#[path]` is relative to the declaring file's directory. Because the fork file
is a child module, it can use the parent's private items. It starts with
`use super::*;` and can hold:

- **Inherent methods**: `impl DdlApplicator { ... }` works from the child and can
  read private fields. A method the parent calls needs `pub(super)`; a method
  that was already `pub` stays `pub`.
- **Free functions and types**: the parent calls them as `fork::name(...)`. When
  code outside the module used the old path, re-export the item with
  `pub use fork::Name;` next to the `mod fork;` line.
- **Unit tests**: put them in the child's own `#[cfg(test)] mod tests`. Shared
  relation fixtures are in `crate::fork::test_support`, because upstream test
  modules keep theirs private. If a test needs an upstream test module's private
  fixtures, `include!` it from inside that module, as `src/config.rs` does with
  `src/fork/config/tests.rs`. rustfmt skips included files, so run
  `rustfmt --edition 2024` on those by hand.

`src/fork/mod.rs` is the crate-level `crate::fork` module for fork-wide
helpers. Code that needs no private access belongs in a fork-owned module such
as `src/destination/snowflake/`, not under `src/fork/`.

The stream binary works the same way, through `src/fork/bin/stream/`.

## Writing hooks

- **One call per branch point.** Write
  `if let Some(tail) = fork::snowflake_tail(...) { return Ok(tail); }`, not
  the whole Snowflake branch inline.
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

Add fork tests in new `tests/fork_<topic>.rs` files, not at the end of an
upstream test file. Upstream appends tests there too, so both sides edit the
same lines. Copy the few fixtures you need (`make_cluster`, `StopOnDrop`, …).
Duplicated fixtures cost less than a conflict on every sync.

## Resolving a conflict on a hook

Hooks are short on purpose, so most conflicts on them are "upstream changed the
lines around our call". Take upstream's version of the surrounding code and
re-add the hook. When upstream appends something at the end of a file and
that clashes with the `mod fork;` declaration, keep both.

## Known hot spots

Moving code doesn't fix these. Each needs a design change:

- **`src/bin/stream/session.rs`**: tenants replaced upstream's per-database
  setup with `tenant::SessionShared` and code in `tenant.rs`. Every upstream
  change to that setup must be ported into `tenant.rs` by hand. It is the
  largest conflict in every sync. Fix it by building tenants on upstream's
  setup functions instead of copies of them, or by upstreaming the tenant
  layer.
- **Snowflake branches inside upstream logic** (`reorder.rs`, `opt_in.rs`,
  `copy_backfill.rs`, `emit/pipeline/decode.rs`): a few lines each, but they
  sit inside code upstream rewrites often.
- **`emit/pipeline/bootstrap.rs`**: the `neutral_values` argument, see above.
