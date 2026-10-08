@AGENTS.md

## Claude-specific notes

- Treat this file as an import shim plus small Claude-specific notes.
- Keep this file short; repo policy lives in `AGENTS.md`.
- In the Claude desktop app, turn on the PR monitor (Auto-fix) with
  `mcp__ccd_pr__set_monitor` as soon as you open a PR or mark one ready. It
  wakes the session on CI failures, merge conflicts and review comments; act
  on those events without waiting to be asked. The switch is per session and
  per PR, so every session that owns a PR turns it on for that PR.
