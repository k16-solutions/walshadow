---
paths:
  - "src/**/*.rs"
  - "tests/**/*.rs"
  - "benches/**/*.rs"
  - "pgext/**"
  - "docker/**"
  - "Cargo.toml"
  - ".github/workflows/ci.yml"
  - ".github/workflows/docker.yml"
  - ".github/workflows/release.yml"
---

# Editing a file that may be upstream-owned

Check the file against the fork-owned list in AGENTS.md first. `src/fork/**`,
`src/destination/**`, the tenant files and `tests/fork_*.rs` are ours; edit
them freely.

For any other file under these paths, upstream ClickHouse/walshadow edits it
too, and each line you change is a conflict in the next sync:

- Put the logic in `src/fork/<same path>.rs`, a child module the upstream file
  declares once at its end with `#[path = "..."] mod fork;`, or in a fork-owned
  module. Leave a one-line call here.
- Keep upstream's function signatures, formatting, imports, messages and
  comments as they are.
- Put tests in the fork file or in `tests/fork_<topic>.rs`, not in this file.
- State in the PR description any change here beyond a hook, and why it
  couldn't be a hook.
