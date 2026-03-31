<!-- SPDX-License-Identifier: LGPL-3.0-or-later -->
# Kitty Graphics Protocol Details

Notes and lessons learned from direct Kitty/Ghostty graphics protocol
investigation, saved outside the phase-planning area so future graphics work can
find them quickly.

## Movie Frame Reuse: The Important Placement Rule

When reusing terminal-resident images for movie playback, placement replacement
semantics are keyed by the pair:

- `(image_id, placement_id)`

Reusing the same `placement_id` with a different `image_id` does **not**
reliably replace the previously visible image by itself.

This matters for cached movie playback:

- `upload-each-frame` works because each frame is uploaded and displayed
  immediately.
- Naive `preload-then-place` can appear to freeze after one full cycle if it
  only keeps issuing `a=p` for new `image_id` values against one reused
  `placement_id`.

## Safe Lifecycle For Cached Playback

The safer cached playback lifecycle is:

1. preload frames once with `a=t`
2. before showing a different cached frame in the same logical slot, explicitly
   remove the previous visible placement
3. place the next cached frame with `a=p`

In practice, that means one of these:

- delete-previous placement before the next place
- clear the whole slot before the next place

The risky lifecycle is:

- reusing the same `placement_id` across different `image_id` values with no
  explicit delete or clear

## Practical Consequence For `casars`

The earlier conclusion "cached preload-then-place is broken in Kitty/Ghostty"
was too strong. The more accurate conclusion is:

- our original cached placement lifecycle was wrong
- cached reuse is still viable, but it must follow explicit delete/clear
  semantics instead of assuming `placement_id` alone implies replacement

So for future `imexplore` graphics optimization:

- keep `upload-each-frame` as the correctness baseline
- if reviving cached movie playback, port the explicit
  delete-before-place/clear-slot lifecycle
- do not reintroduce the older replace-only cached placement path

## Related Production Follow-Up

See backlog item `12.13` in:

- [docs/Planning/Phase 5 - Lattices Coordinates Images/BACKLOG.md](/Users/brianglendenning/.codex/worktrees/a4fd/casa-rs/docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/BACKLOG.md)

That backlog item is the production follow-up for using these protocol details
to revisit high-performance plane-movie playback.
