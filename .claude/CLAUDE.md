@../AGENTS.md

## Claude Code

- `/upstream-sync` runs the upstream merge procedure
  (`.claude/skills/upstream-sync/SKILL.md`). The scheduled workflow runs the
  same skill.
- When you edit a Rust file outside the fork-owned paths in AGENTS.md, the
  rule in `.claude/rules/upstream-owned-files.md` applies.
