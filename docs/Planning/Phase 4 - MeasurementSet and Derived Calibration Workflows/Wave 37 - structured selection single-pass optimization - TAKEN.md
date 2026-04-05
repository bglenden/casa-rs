# Wave 37 - structured selection single-pass optimization

## Summary

Trim the remaining planner-side overhead in the benchmarked `applycal` path by
making structured MeasurementSet selection scan the MAIN rows once, instead of
performing one full-row pass per requested scalar column.

## Scope

- Keep the existing structured-selection semantics unchanged for `field`, `spw`,
  `data_desc`, antenna, baseline, time, scan, state, observation, and array
  filters.
- Replace the repeated per-column full-row scans with one pass that extracts
  only the requested scalar fields.
- Preserve the existing regression coverage for selection edge cases,
  especially the “requested SPW has no DDID mapping” behavior.
- Re-run the existing `applycal` benchmark to confirm whether the planner
  change materially moves the end-to-end ratio.

## Notes

- The change is a modest improvement, not a parity-closing one: it trims the
  benchmarked `planning_selection` phase slightly, but MAIN-table save remains
  the dominant cost center in the Rust path.
- This wave is the practical stopping point for local planner tweaks; the
  remaining performance work needs deeper persistence changes.
