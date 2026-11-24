# Agent instructions

This repository is k16-solutions' fork of
[ClickHouse/walshadow](https://github.com/ClickHouse/walshadow). Upstream is
merged in every two weeks ([docs/upstream-sync.md](docs/upstream-sync.md)).
Every line you change in a file upstream also changes is a merge conflict
someone resolves by hand later. So the main rule here is: **put fork code in
fork-owned files, and keep edits to upstream-owned files to one-line hooks.**
[FORK.md](FORK.md) has the details. Read it before changing anything under
`src/`.

## Which files you may change freely

Fork-owned. Upstream has no copy of these:

- `src/fork/**`: fork code that extends an upstream module (see below)
- `src/destination/**`: the Snowflake destination
- `src/tenants.rs`, `src/source/tenant_router.rs`, `src/ops/tenant_ctl.rs`,
  `src/bin/stream/tenant.rs`: tenants
- `tests/fork_*.rs`, `tests/multi_tenant_e2e.rs`, `tests/snowflake_live.rs`,
  `tests/destination_config.rs`
- `bench/src/snowflake.rs`, `bench/src/qualification.rs`,
  `bench/src/bin/snowflake_qualify.rs`
- `charts/**`, `.github/workflows/chart.yml`,
  `.github/workflows/upstream-sync.yml`, `scripts/fork/**`, `.claude/**`
- `FORK.md`, `AGENTS.md`, `docs/branching.md`, `docs/upstream-sync.md`,
  `docs/snowflake.md`, `docs/tenants.md`, `config/snowflake.toml`,
  `plans/snowflake*.md`

Everything else is upstream-owned: the rest of `src/`, `tests/`, `benches/`,
`pgext/`, `docker/`, `architecture/`, `Cargo.toml`, `Cargo.lock`,
`.github/workflows/ci.yml`, `docker.yml` and `release.yml`, and the remaining
`docs/`.

## Changing behavior in an upstream-owned file

1. Write the code in a fork-owned file:
   - If it needs the upstream module's private items, put it in
     `src/fork/<same path>.rs`. Upstream's `src/emit/ch_ddl.rs` is extended by
     `src/fork/emit/ch_ddl.rs`. If the upstream file doesn't declare
     `mod fork;` yet, add the declaration at the end of the file:
     ```rust
     // k16 fork: <what lives there> (see FORK.md)
     #[path = "../fork/emit/ch_ddl.rs"]
     mod fork;
     ```
     The fork file starts with `use super::*;`.
   - Otherwise put it in a fork-owned module, such as `src/destination/...`.
2. In the upstream file, add only the call: one `if let ... = fork::x(...)`,
   one `pub use fork::Name;`, one struct field.
3. Put new tests in the fork file's `#[cfg(test)] mod tests` or in a new
   `tests/fork_<topic>.rs`. Never append to an upstream test file. Shared
   relation fixtures are in `crate::fork::test_support`.

## Never do these in upstream-owned files

- Add a parameter to an upstream function, or change its signature or make it
  `async`. That rewrites every upstream caller and test, now and after every
  sync. Add a fork function instead.
- Fix an upstream bug or a lint locally. Fix it upstream, or cherry-pick
  upstream's exact commit. Our own Rust 1.100 fix conflicted with upstream's
  in 13 test files.
- Reformat, rename, reorder imports, or reword messages and comments.
- Copy an upstream function into fork code to change it. Call it, or add a
  hook to it.
- Edit `.gitignore`, `deny.toml` or `Cargo.lock` by hand. The lock changes
  through `cargo`. Fork dependencies go at the end of `[dependencies]` in
  `Cargo.toml`.

If you can't avoid a bigger change to an upstream-owned file, say so in the PR
description and explain why.

## Before you finish

- `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo test -p walshadow --lib --bins` pass. CI also runs rustdoc with
  warnings denied and the e2e suites.
- If you change dependencies, run `cargo deny --locked check`. PR CI doesn't
  run it; only the upstream sync does.
- `git diff --stat origin/main` touches upstream-owned files only with hook
  lines.
- When the change is large, `scripts/fork/trial-merge.sh HEAD upstream/main`
  shows what it costs the next sync. Compare it with the same command on
  `origin/main`.
- `.claude/` is in upstream's `.gitignore`. Add new files there with
  `git add -f`.

## Branches and PRs

Branch from `origin/main`, and squash-merge feature PRs. Upstream sync PRs
(`merge/upstream-YYYY-MM-DD`) are merged with a merge commit, never squashed
([docs/branching.md](docs/branching.md)). Never push `v*` tags: upstream's
tags are in every clone, and ours trigger releases.
