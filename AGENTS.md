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

- `src/snowflake/**`: the Snowflake destination
- `src/tenant/**`: tenants
- `src/standby/**`: bulk loads read from a standby
- `tests/snowflake_*.rs`, `tests/tenant_*.rs`, `tests/standby_*.rs`
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

1. Pick the feature the change belongs to (`snowflake`, `tenant` or
   `standby`; FORK.md lists what each holds) and write the code in its
   directory:
   - If it needs the upstream module's private items, put it in
     `src/<feature>/ext/<same path>.rs`. Upstream's `src/emit/ch_ddl.rs` is
     extended by `src/snowflake/ext/emit/ch_ddl.rs` and
     `src/tenant/ext/emit/ch_ddl.rs`. If the upstream file doesn't declare
     that feature's child yet, add the declaration at the end of the file,
     next to any other feature's:
     ```rust
     // k16 fork: <what lives there> (see FORK.md)
     #[path = "../snowflake/ext/emit/ch_ddl.rs"]
     mod snowflake;
     ```
     The fork file starts with `use super::*;`.
   - Otherwise put it in the feature's own module, `src/<feature>/`.
2. In the upstream file, add only the call: one
   `if let ... = snowflake::x(...)`, one `pub use snowflake::Name;`, one
   struct field.
3. Put new tests in the fork file's `#[cfg(test)] mod tests` or in a new
   `tests/<feature>_<topic>.rs`. Never append to an upstream test file. Shared
   relation fixtures are in `crate::snowflake::test_support`.

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
- If you change dependencies, run `cargo deny --locked check`. From merge 1
  on, CI's lint job runs it too and fails on any advisory.
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
