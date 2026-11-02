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
- A passing check or a merge does not wake the session. When you wait for
  one, also start a single background wait (`gh pr checks N --watch`, or a
  Monitor until-loop where `gh` is unavailable). The PR monitor's "do not
  poll CI" forbids repeated foreground checks, not that one background wait.
- `AGENTS.md`'s local review is, in Claude Code, a fresh `Agent` (not a
  fork) with model `fable` and effort `xhigh`; run it in the background and
  keep working. Its external review of Claude-led work is Astra at `xhigh`
  or a human, which the owner arranges from the brief you post.
