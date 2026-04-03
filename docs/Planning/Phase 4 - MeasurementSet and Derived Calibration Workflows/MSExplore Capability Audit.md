# MSExplore Capability Audit

## Scope

This audit records the current `msexplore` capability surface against the local
CASA `plotms` task signature and the current `casa-rs` implementation.

Primary local source of truth for CASA `plotms` parameters:
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/lib/python3.14/site-packages/casaplotms/private/task_plotms.py:36`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/lib/python3.14/site-packages/casaplotms/plotms.py:565`

This document is intentionally merge-oriented rather than aspirational: it
tracks what is shipped now, what would block an honest merge, and what remains
explicitly deferred.

## Shipped

The current `msexplore` user surface covers these common MeasurementSet
`plotms` capability families:

- Plot presets and axes:
  - time, uvdist, u, v, w, channel, frequency, velocity
  - amplitude, phase, real, imaginary
  - weight, sigma, weight spectrum, sigma spectrum, flag, flagrow
  - azimuth, elevation, hour angle, parallactic angle
- Metadata-oriented presets through the shared MeasurementSet summary/plot
  substrate:
  - UV coverage
  - antenna layout
  - scan timeline
  - spectral-window coverage
- Selection controls:
  - `field`, `spw`, `timerange`, `uvrange`, `antenna`, `scan`,
    `correlation`, `array`, `observation`, `intent`, `feed`, `msselect`
- Averaging controls:
  - `avgchannel`, `avgtime`, `avgscan`, `avgfield`, `avgbaseline`,
    `avgantenna`, `avgspw`, `scalar`
- Spectral transforms:
  - `freqframe`, `restfreq`, `veldef`
- Layout and presentation:
  - single plots, iterated grids, stacked paired pages, generic multi-plot
    pages, same-cell overplots
  - `gridrows`, `gridcols`, `iteraxis`, `xselfscale`, `yselfscale`,
    `xsharedaxis`, `ysharedaxis`
  - `showlegend`, `legendposition`, `showmajorgrid`, `showminorgrid`,
    `headeritems`, `exprange=current|all` with the current shared-axis guard
- Flag editing:
  - staged rectangular preview/apply on single plots and iterated grids
  - `flag`, `unflag`, `extcorr`, `extchannel`, panel targeting, plot-index
    targeting where supported
- Export:
  - `txt`, `png`, `pdf`
- Performance guard:
  - hard `max_plot_points` cap, default `10_000_000`

## Merge-Blocking Gaps

None identified in the current exposed CLI/TUI surface.

The modeled-but-unimplemented transform fields are **not** exposed in the
current `msexplore` CLI/TUI schema, so they are deferred rather than
merge-blocking:
- `phasecenter`
- `xframe`
- `xinterp`
- `yframe`
- `yinterp`

## Deferred

These CASA `plotms` capabilities remain intentionally out of scope for this PR:

- Additional transform controls beyond `freqframe` / `restfreq` / `veldef`:
  - `phasecenter`
  - explicit x/y frame overrides
  - explicit interpolation controls
- Symbol customization:
  - `customsymbol`, `symbolshape`, `symbolsize`, `symbolcolor`,
    `symbolfill`, `symboloutline`
  - flagged-symbol customization
- Connector controls:
  - `xconnector`, `timeconnector`
- Additional axis placement controls:
  - `yaxislocation`
- Plot range customization beyond the current `exprange` support:
  - full `plotrange` parity
- Overlay families:
  - `showatm`, `showtsky`, `showimage`, `colorizeoverlay`
- Calibration-table plotting / `caltable` support
- Callibrary / `callib` support
- Free-form long-tail axis families that are modeled in CASA `plotms` but not
  yet part of the current MeasurementSet-first product surface
- Page-aware staged flag editing for generic multi-plot pages and stacked page
  payloads

## Notes

- The current branch intentionally removes `listobs` and `msinfo` as separate
  app/bin surfaces. Their reusable summary and metadata plot logic remains as an
  internal MeasurementSet substrate beneath `msexplore`.
- CASA parity remains mandatory for user-visible `msexplore` behavior changed in
  this branch. The local parity runner is `scripts/run-msexplore-casa-parity.sh`.
