# Upstream sync plan

We merge [ClickHouse/walshadow](https://github.com/ClickHouse/walshadow)'s
`main` into ours every two weeks, on the 1st and the 15th. This page covers why
we use that cadence, how a sync runs, and how Claude automates most of it.
[docs/branching.md](branching.md#syncing-upstream) has the git mechanics.
[FORK.md](../FORK.md) lists which files the fork owns and how to keep fork code
out of upstream's files.

## Why every two weeks

Upstream is busy: about 15 commits a week, from four or five ClickHouse
developers, and it is pre-1.0, so internals still change. The cost of a sync
grows faster than the number of commits it brings in:

| Sync | Upstream commits | Files fixed by hand | Lines changed by hand |
|---|---|---|---|
| 2026-09-22 | 14 | 14 | +88 / −272 |
| 2026-09-28 | 12 | 14 | +131 / −319 |
| 2026-10-20 | 46 | 55 | +1,793 / −2,340 |
| 2026-11-09 | 40 | 34 | +877 / −881 |

Two weeks keeps a sync near 30 commits, small enough for one person to finish
in a day or two. Weekly would be smaller still, but most weeks would then spend
review time on a handful of commits. When upstream lands a large refactor, sync
right after it rather than waiting for the next date.

## The schedule

| When | What |
|---|---|
| 1st and 15th, 13:00 UTC | [upstream-sync.yml](../.github/workflows/upstream-sync.yml) runs. The `trial merge` job writes a conflict report to the run summary. The `sync` job has Claude merge, build and open a `merge/upstream-YYYY-MM-DD` PR |
| Within 3 working days | The sync owner reviews, finishes anything Claude flagged, gets CI green and merges with **Create a merge commit** |
| Any time | `scripts/fork/trial-merge.sh` shows what a merge would conflict on, without touching your checkout |

The sync owner rotates each cycle. Skip a cycle only on purpose: the next one
is then twice as big.

## How a sync runs

The steps are the same whether Claude or a person runs them. They live in
[.claude/skills/upstream-sync/SKILL.md](../.claude/skills/upstream-sync/SKILL.md),
so the scheduled job, `/upstream-sync` in a local Claude Code session and a
person reading the file all follow one procedure.

1. **Trial merge.** `scripts/fork/trial-merge.sh origin/main upstream/main`
   lists each conflicted file and its conflicted lines.
2. **Pick the target.** Usually all of upstream's `main`. When upstream
   deletes or reshapes something fork code builds on, merge in two steps: up to
   the commit before it, then the rest. The second step is then only the port.
   For example, `51da81f` replaces `ColumnValue` with raw PostgreSQL bytes, and
   the Snowflake destination converts from `ColumnValue`.
3. **Merge on `merge/upstream-YYYY-MM-DD`** with `git rerere` on, so a redone
   merge reuses earlier resolutions.
4. **Resolve** with the rules in the skill. In short: take upstream's code,
   re-add the fork's hooks, port upstream's changes into code the fork moved,
   and take upstream's fix when both sides fixed the same thing.
5. **Build and test** with CI's lint commands, unit tests and `cargo deny`.
6. **Open the PR** with the upstream range, each conflict and its resolution,
   any ports and any follow-ups. **Merge commit only, never squash.**

### Reviewing a sync PR

- Every required check is green, and the required check names still match
  CI's jobs ([branching.md](branching.md#keep-required-checks-in-step-with-ci)).
- The line-coverage step passes under `MAX_MISSED_LINES`. Lower the ceiling
  when fork tests land.
- `cargo deny` passes, or each new advisory has a follow-up.
- The e2e suites pass. `snowflake_live` needs Snowflake credentials, so run it
  by hand when the sync touched the Snowflake path.
- Every port the PR lists has a reviewer who knows that code: tenants
  (`tenant.rs`, `session.rs`) or Snowflake (`src/destination/`).

## Automating it with Claude

### Recommended: GitHub Actions and `claude-code-action`

[upstream-sync.yml](../.github/workflows/upstream-sync.yml) runs on the
schedule above, or by hand from the Actions tab. The `trial merge` job needs no
secrets. The `sync` job installs the Rust toolchain and C dependencies, then
runs [anthropics/claude-code-action](https://github.com/anthropics/claude-code-action)
with the prompt `/upstream-sync`, which loads the skill. Claude can run only
`git`, `cargo`, `gh pr`, `gh api` and `scripts/fork/*`, plus file reads and
edits. It never merges or pushes to `main`.

One-time setup:

1. Add the Claude API key as the `ANTHROPIC_API_KEY` repository secret.
2. Create a GitHub App for the sync, with **Contents** and **Pull requests**
   set to read and write, and install it on this repository. Store its ID in
   the `UPSTREAM_SYNC_APP_ID` variable and its private key in the
   `UPSTREAM_SYNC_APP_PRIVATE_KEY` secret. Pushes and PRs made with the default
   `GITHUB_TOKEN` don't start other workflows, so without the App, CI would
   never run on the sync PR.
3. Trigger it once by hand (**Run workflow**) to check the setup. Scheduled
   runs start only after this workflow is on `main`.

Each run costs Claude API usage for one long agent session: a build and the
conflict resolution, at most 200 turns.

### Alternatives

- **Claude Code scheduled tasks ("routines").** These run a prompt on a cron
  in Anthropic's cloud, against a clone of the repo. The VM has Rust, Docker and
  PostgreSQL 16, with about 4 vCPU and 16 GB. No workflow file or App is
  needed. Runs push to `claude/`-prefixed branches unless the prompt names
  another branch, and commits carry the connected GitHub user. The feature is
  in research preview, and builds on this crate are slow at that size. It is
  good for the trial-merge report, less so for a full merge.
- **On demand.** Run `/upstream-sync` in a local Claude Code session. Use this
  for staged merges and for anything the scheduled run stopped on.

## What stays with people

- Approving and merging every sync PR.
- Decisions on `cargo deny` advisories: upgrade, replace or ignore with a
  reason.
- Ports that change fork behavior. The skill stops and reports these rather
  than guessing.
- Upstreaming fork changes that aren't k16-specific. Every accepted upstream
  PR is code we stop porting. The tenant layer is the biggest candidate.
