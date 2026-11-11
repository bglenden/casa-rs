@AGENTS.md

## Claude-specific notes

- Treat this file as an import shim plus small Claude-specific notes.
- Keep this file short; repo policy lives in `AGENTS.md`.
- In the Claude desktop app, turn on the PR monitor (Auto-fix) with
  `mcp__ccd_pr__set_monitor` as soon as you open a PR or mark one ready. It
  wakes the session on CI failures, merge conflicts and review comments; act
  on those events without waiting to be asked. The switch is per session and
  per PR, so every session that owns a PR turns it on for that PR. In a cloud
  session, `subscribe_pr_activity` does the same.
- Keep working on the issue while the PR monitor runs. Follow AGENTS.md's
  "While Checks Run" for continuation and waiting turns. Check which events
  the monitor actually wakes this session for; a terminal watcher alone
  does not guarantee a resumed agent turn. The monitor's "do not poll CI"
  forbids repeated foreground checks, not one bounded or background wait.
- `AGENTS.md`'s local review is, in Claude Code, a fresh `Agent` (not a
  fork) with model `fable` (the latest Fable) and effort `xhigh`; run it in
  the background and keep working. Its external review of Claude-led work
  is the latest Astra at `xhigh` or a human, which the owner arranges from
  the brief you post.
