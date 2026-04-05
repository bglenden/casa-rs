# Wave 36 - calibrate managed launcher output

## Summary

Add the first structured launcher-facing output path for `calibrate` so
`casars` can render a useful Overview tab for calibration workflows instead of
only raw stdout/stderr.

## Scope

- Add a managed-output envelope in `casa-calibration` that covers the public
  workflow reports already emitted by the library and CLI.
- Advertise that managed output in the `calibrate` schema so `casars` can
  inject the right arguments and pick the calibration renderer.
- Teach `casars` to parse the calibration managed output and render compact
  Overview lines for apply, summary, plan, stats, solve, and fluxscale
  results.
- Add focused launcher-side coverage for the new structured calibration
  Overview path without moving calibration logic into the UI layer.

## Notes

- The first cut intentionally keeps the app thin: the structured payload is
  emitted by `casa-calibration`, and `casars` only parses and renders it.
- This wave is evaluation-oriented UI work, not a replacement for future
  richer calibration-specific forms or inspection views.
