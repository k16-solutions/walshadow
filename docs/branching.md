# Branching and releases

This repository is k16-solutions' fork of
[ClickHouse/walshadow](https://github.com/ClickHouse/walshadow). Development is
trunk-based: `main` is the only long-lived branch. Every change reaches `main`
through a pull request, and releases are `v*` tags on `main`.

```
feature/data-123-x ────────── PR, squash ──────────▶ main ── tag v0.1.7-k16.1 ──▶ release + images
merge/upstream-YYYY-MM-DD ─── PR, merge commit ────▶ main
```

## Branches

| Branch | Purpose | Merge into `main` with |
|--------|---------|------------------------|
| `main` | Integration and release branch. Protected, see [GitHub rules](#github-rules) | n/a |
| `feature/<ticket>-<slug>`, `fix/<slug>`, `docs/<slug>` | Short-lived work, branched from `main` | Squash |
| `merge/upstream-YYYY-MM-DD` | Brings in upstream's `main` | Merge commit, never squash |
| `hotfix/<version>` | Patch a release without shipping unreleased `main` | Merge commit |

Use the Linear key in feature branch names when there is one, for example
`feature/data-471-chart-skeleton`. The names are a convention; GitHub does not
enforce them.

## Day-to-day flow

1. Branch from an up-to-date `origin/main`.
2. Open a pull request into `main`. CI runs on every push to it.
3. Get one approval, a green run of every required check, and every review
   thread resolved. Pushing after an approval dismisses it.
4. Squash-merge. The PR title becomes the commit subject on `main`, so put the
   ticket in it, for example `[DATA-471] Add a Helm chart`.

GitHub deletes the branch after the merge. Auto-merge is on, so you can enable it
on a PR and it merges once the approval and checks are in. The branch does not
have to be up to date with `main` to merge. Use **Update branch** when you want
CI to run against the latest `main`.

## Syncing upstream

Upstream's `main` is merged into ours every two weeks. The schedule, the
automation and the review checklist are in [upstream-sync.md](upstream-sync.md).
`scripts/fork/trial-merge.sh` shows what a merge would conflict on without
touching your checkout. The git steps:

```bash
git remote add upstream git@github.com:ClickHouse/walshadow.git   # once
git config remote.upstream.tagOpt --no-tags                       # once, see Releases
git fetch origin && git fetch upstream
git switch -c merge/upstream-$(date +%F) origin/main
git config rerere.enabled true                                    # once; reuses resolutions
git merge upstream/main
# resolve conflicts (rules: .claude/skills/upstream-sync/SKILL.md), build, run the tests
git push -u origin HEAD
```

Open the PR into `main` and merge it with **Create a merge commit**. Never
squash it. A merge commit keeps upstream's commits as ancestors of `main`, so
the next sync only brings in what is new. A squash lands upstream's changes as
one new commit. Git then doesn't know they are merged, and the next sync
conflicts with them again. The ruleset allows squash for feature PRs, so it
cannot stop this. Whoever merges, or enables auto-merge, picks the method.

Conflicts to expect:

- **`.github/rulesets/main.json`**: upstream keeps its own ruleset file there.
  The fork deleted it because our rules live in GitHub (see
  [GitHub rules](#github-rules)). When upstream changes it, git reports a
  modify/delete conflict. Keep it deleted: `git rm .github/rulesets/main.json`.
- **`version` in `Cargo.toml`**: take upstream's version. See [Releases](#releases).
- **CI job names**: a renamed job breaks a required check. See
  [Keep required checks in step with CI](#keep-required-checks-in-step-with-ci).

## Releases

A `v*` tag on `main` is a release. Pushing one runs:

- [release.yml](../.github/workflows/release.yml): builds the binaries and the
  pgext module for linux x86_64 and aarch64, and attaches the archives and
  `SHA256SUMS` to a GitHub Release.
- [docker.yml](../.github/workflows/docker.yml): builds one image per
  PostgreSQL major, tagged `<version>-pgN`.

Every push to `main` also builds `sha-<short>` artifacts and `pgN` images, so
`main` must stay releasable.

Rules for tags:

- **The tag must equal `version` in `Cargo.toml`, plus a `v`.** release.yml fails
  otherwise. Bump `Cargo.toml` in a PR, merge it, then tag that merge commit.
- **Don't reuse a version upstream has tagged.** Upstream tags `v0.1.x` too, and
  our `Cargo.toml` version comes from upstream. Add `-k16.N` to the upstream
  version the release is based on: `v0.1.7-k16.1`, then `v0.1.7-k16.2`. Start
  again at `-k16.1` after a sync brings in a new upstream version.
- **Push the one tag by name**: `git push origin v0.1.7-k16.1`. Never run
  `git push --tags`. A clone that has fetched upstream has upstream's tags. Pushing
  them would publish upstream's versions from this repository, and the tag rules
  stop anyone from deleting them.
- **Tags are permanent.** If a release is wrong, fix it on `main` and tag the next
  version. Don't move or delete the tag.

### Hotfix

Usually a hotfix is an ordinary fix PR into `main`, followed by the next tag. If
`main` holds unreleased changes that must not ship yet:

1. Branch from the release tag: `git switch -c hotfix/v0.1.7-k16.2 v0.1.7-k16.1`.
2. Make the fix, bump `Cargo.toml`, and get it reviewed in a PR into `main`. Tag
   the fix commit on the hotfix branch and push the tag.
3. Merge the PR with a merge commit, so the tagged commit is part of `main`'s
   history.

## GitHub rules

The rules are GitHub repository rulesets, set through the API (Settings → Rules →
Rulesets). No file in this repository applies them. When you change a ruleset,
update this page in the same PR as the reason for the change.

**`main: PR review + required CI`** applies to `main`:

- Every change goes through a pull request. Nobody can push directly, delete
  `main`, or force-push it.
- One approval. Pushing new commits dismisses earlier approvals. Every review
  thread must be resolved.
- Merge commit or squash. Rebase merging is off.
- These checks must pass. Each is pinned to the GitHub Actions app, so nothing
  else can report it:
  - `build tests (instrumented)`
  - `e2e (pg16)`, `e2e (pg17)`, `e2e (pg18)`, `e2e (pg19)`
  - `line coverage (merged 16/17/18/19)`
  - `lint`
- Branches don't have to be up to date with `main` before merging. CI takes about
  40 minutes, and requiring it would force a re-run after every merge.
- Nobody can bypass these rules, admins included. Make sure someone other than the
  author can review.

**`release tags: immutable v*`** applies to `v*` tags. They can't be deleted or
moved. Removing a tag means an admin disables this ruleset first.

Repository settings: rebase merging off, merged branches deleted, auto-merge
allowed, **Update branch** offered on PRs.

## Keep required checks in step with CI

Required checks are matched by job name. A required check that never reports
blocks every PR at "Expected — Waiting for status to be reported". Upstream does
rename CI jobs: `build + e2e (pgN)` became `e2e (pgN)`.

After an upstream sync or any change to job names in
[ci.yml](../.github/workflows/ci.yml), compare what CI reports with what the
ruleset requires:

```bash
gh pr checks <pr-number>
gh api repos/k16-solutions/walshadow/rulesets/27742631 \
  --jq '.rules[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context'
```

If they differ, update the ruleset before merging.

- Only jobs from the CI workflow are required. Docker and Release run on PRs only
  when their paths change, so requiring them would block every other PR.
- Give each job a name no other workflow uses. Required checks match on the name
  alone, so a second workflow's `lint` job would also count as `lint`.

## Why trunk-based, not `develop`

scaffold-data uses `develop` → `rc/*` → `release/*` → `main` (its
`docs/release_flow.md`). This repository uses a single trunk because:

- **It is a fork.** Upstream works on `main`. With one trunk, each sync is one
  merge into one branch.
- **The workflows come from upstream.** ci.yml, docker.yml and release.yml run on
  `main` and `v*` tags. A `develop` branch would mean editing them, and those
  edits would conflict on every sync.
- **Releases are tested where they are deployed.** scaffold-data deploys a pinned
  walshadow image tag and chart version (`terraform/modules/walshadow`). A
  release is validated there, so this repository doesn't need `rc/*` branches.

The rest matches scaffold-data:

- rulesets rather than classic branch protection
- one approval
- every CI job required
- merge commits for branch-to-branch merges, squash for features
