# Wave 38 - calibrate inspection and diagnostic plots

## Summary

Add the first real plotting surface to `calibrate` so the launcher can show
useful calibration-table inspection plots and apply/solve diagnostic plots
instead of advertising a dead Plots tab.

## Scope

- Add a library-first calibration plotting module in `casa-calibration`.
- Reuse the existing `casa-ms` scatter-plot payload/render/export path
  instead of creating a parallel plotting stack.
- Expose a focused preset catalog in `casars` for the most useful initial
  calibration plots.
- Verify the new plot payloads on both synthetic fixtures and real CASA-
  generated calibration tables.

## Shipped Presets

- Gain amplitude vs time
- Gain phase vs time
- Bandpass amplitude vs frequency
- Bandpass phase vs frequency
- Corrected-data amplitude vs time
- Corrected-data phase vs time
- Corrected-data amplitude vs frequency
- Corrected-data phase vs frequency

## Notes

- The preset choice follows the practical first-look plots emphasized by CASA
  plotting/docs (`plotms`, `plotcal`, `plotbandpass`) and the local
  radio-astronomy reference corpus: time-domain gain inspection, frequency-
  domain bandpass inspection, and corrected-data diagnostics after apply/solve.
- `calibrate` plots stay thin at the UI layer. `casa-calibration` builds the
  plot payloads, and `casars` only catalogs, renders, and exports them.
- Corrected-data plots require `CORRECTED_DATA`; they intentionally do not
  fall back silently to raw `DATA`.
- Copy-CLI support stays disabled for these presets until there is a stable
  CLI plot contract for `calibrate`.
