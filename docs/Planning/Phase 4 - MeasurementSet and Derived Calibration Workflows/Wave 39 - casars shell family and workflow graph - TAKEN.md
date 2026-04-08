# Wave 39 - casars shell family and workflow graph

## Summary

Start the framework-level shell-family migration in `casars` so complex apps
stop feeling like raw CLI schemas in a terminal wrapper.

This wave strengthens the new shell model in two places:

- `BrowserShell` now owns the left pane for browser apps instead of dropping
  back to legacy table-inspector rows after a session opens.
- `WorkflowShell` now uses a reusable workflow-graph layer that tracks ordered
  but revisitable stages, versioned products, stale downstream artifacts, and
  session history.

## Scope

- Keep the existing shell split:
  - `InspectShell`
  - `BrowserShell`
  - `WorkflowShell`
- Remove the legacy browser-session shortcut that bypassed the shared shell
  layout for `tablebrowser`.
- Add a pure workflow helper module in `casars` for stage status, product
  revisions, recommendation logic, and downstream staleness.
- Rework `calibrate` so Overview and Products expose the shell-level workflow
  model rather than only mirroring the latest managed CLI report.
- Add future-fit tests for linear calibration, imaging, self-cal iteration,
  and VLBI-style prior-calibration chains at the workflow-graph level.

## Landed Behavior

- `tablebrowser` now shows the shared `BrowserShell` left-pane structure
  (`Context`, `Views`, `Tools`) while the right side owns
  `Overview` / `Structure` / `Content` / `Inspector`.
- Browser view selection from the left pane is mouse-clickable and keyboard-
  navigable without relying on the removed browser-specific tab strip.
- `calibrate` now tracks product revisions with explicit status:
  - `active`
  - `stale`
  - `superseded`
- Upstream reruns mark downstream workflow products stale when their recorded
  dependency revisions are older than the new upstream revision.
- Workflow Overview now shows:
  - dataset
  - recommended next stage
  - selected stage
  - active/stale product counts
  - latest run summary
- Workflow Products now show:
  - revision
  - family
  - stage
  - status
  - dependency revision lineage
  - run sequence

## Notes

- This wave does not yet move shell metadata fully into registry/protocol-level
  descriptors; stage metadata is still defined in the current app layer.
- `msexplore` still uses the shell contract only at the layout level. A later
  wave should replace the remaining schema-derived feel with a cleaner inspect
  view model.
- The workflow graph is generic enough to model linear calibration, iterative
  self-calibration loops, imaging artifact pipelines, and VLBI prior-cal/apply
  chains, but only `calibrate` is wired into it in this wave.
