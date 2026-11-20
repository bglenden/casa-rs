@AGENTS.md

## Claude-specific notes

- Treat this file as an import shim plus small Claude-specific notes.
- Keep this file short; repo policy lives in `AGENTS.md`.
- For an exceptional hosted run or a pending required review, use the PR
  monitor (`mcp__ccd_pr__set_monitor` in desktop or `subscribe_pr_activity`
  in cloud) if it covers the result this session needs. Follow AGENTS.md's
  "Continuation and Blockers" through the queue. Confirm which events wake
  the session before relying on the monitor to resume delivery.
- `AGENTS.md`'s local review is, in Claude Code, a fresh `Agent` (not a
  fork) with model `fable` (the latest Fable) and effort `xhigh`; run it in
  the background and keep working. Its external review of Claude-led work
  is the latest Astra at `xhigh` or a human, which the owner arranges from
  the brief you post.
