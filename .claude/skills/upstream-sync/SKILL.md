---
name: upstream-sync
description: Merge ClickHouse/walshadow's main into this fork on a merge/upstream-YYYY-MM-DD branch and open the sync PR. Use for the biweekly upstream sync, whether a person runs /upstream-sync or the scheduled workflow does.
---

# Upstream sync

You are merging upstream (`ClickHouse/walshadow`) into the k16-solutions fork.
Read [docs/upstream-sync.md](../../../docs/upstream-sync.md) for the plan and
[FORK.md](../../../FORK.md) for which files the fork owns. Follow these steps in
order. Stop and report instead of guessing when a step says so.

## 1. Set up

```bash
git remote get-url upstream || git remote add upstream https://github.com/ClickHouse/walshadow.git
git config remote.upstream.tagOpt --no-tags   # upstream's v* tags must never reach origin
git config rerere.enabled true                # reuse earlier conflict resolutions
git fetch origin --prune && git fetch upstream --prune
scripts/fork/trial-merge.sh origin/main upstream/main
```

If the trial merge is clean and fewer than 5 upstream commits are pending, stop
and report "nothing worth syncing". If a `merge/upstream-*` PR is already open,
stop and report its number. Don't open a second one.

## 2. Pick the merge target

Merge everything pending unless one of these upstream commits is in range.
Each needs a port, not just a conflict resolution:

- a commit that deletes or renames a type, module or function that
  `src/destination/**`, `src/fork/**` or `src/bin/stream/tenant.rs` uses
  (`git log -p origin/main..upstream/main` and grep for the removed names)
- a change to how `src/bin/stream/session.rs` builds per-database pipelines
  (the fork builds those in `tenant.rs`)

When one is in range, merge only up to the commit before it (`git merge
<sha>~1`), and say in the PR that the next sync starts with that commit.

## 3. Merge

```bash
git switch -c merge/upstream-$(date -u +%F) origin/main
git merge --no-ff <target>
```

Resolve conflicts with these rules:

- **Fork-owned files** (FORK.md's table): keep ours, then adapt them to
  upstream's API changes.
- **Upstream-owned files**: take upstream's version, then re-add the fork's
  hook lines. A hook is a `fork::...` call, a `mod fork;` declaration, a
  `pub use fork::...` line or a fork-added struct field. Never keep an older
  copy of upstream's own code.
- **Code the fork moved elsewhere**: when upstream edits a block the fork
  deleted (per-database setup in `session.rs`, which moved to `tenant.rs`),
  keep it deleted and port upstream's change to the fork's copy.
- **Duplicate fixes**: when both sides fixed the same thing, take upstream's.
- **`Cargo.lock`**: `git checkout --theirs Cargo.lock`, then build so cargo
  re-adds the fork's dependencies.
- **`.github/rulesets/main.json`**: keep it deleted.
- **`Cargo.toml` `version`**: take upstream's.
- **`.github/workflows/ci.yml`**: keep the fork's coverage ceiling step
  (`MAX_MISSED_LINES`) and take everything else from upstream.

## 4. Build and test

The CI lint job runs these. Run them and fix what fails:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --lib --bins
cargo test -p walshadow --lib --bins
cargo deny --locked check      # when cargo-deny is installed
```

Fix compile errors in fork-owned files first. When an upstream test fails to
build only because of a fork API change (an extra argument, an `async` fn, a
new struct field), make the smallest edit to that test.

Stop and report instead of committing when either of these holds:

- a test fails and the cause is not obvious from the diff
- the fix would change behavior beyond porting upstream's change

## 5. Commit and open the PR

Commit the merge with a message that lists the upstream range, the conflicts
and how each was resolved. Push and open a PR into `main`:

- Title: `Merge upstream through <short-sha> (<upstream version or date>)`
- Body sections:
  - **Upstream range**: first and last commit, and the count
  - **Conflicts and resolutions**: one bullet per file
  - **Ports**: upstream changes re-applied to fork code
  - **Follow-ups**: anything deferred, such as cargo-deny advisories,
    coverage, or commits left for the next sync
  - **Testing**
- Say in the body: **merge with "Create a merge commit", never squash.**

Do not merge the PR, enable auto-merge, push tags or push to `main`.

## 6. Afterwards

Compare the required check names with CI's job names (docs/branching.md,
"Keep required checks in step with CI"). If a job was renamed, note it in the
PR for a maintainer; the ruleset is configured live in GitHub, not in the repo.
