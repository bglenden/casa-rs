# CASA Tutorial Capability Matrix

Truth class: current descriptive
Last reality check: 2026-05-05
Verification: just docs-check

Wave issue: #137
Child issue: #116

This matrix maps the extracted CASA task/tool surface from current CASA Guide
pages to casa-rs ownership and planned implementation waves. It is a planning
and oracle map; it does not add public APIs.

Status legend:

- `available`: substantial current casa-rs capability exists.
- `partial`: a real subset exists, but tutorial parameters or output products
  need expansion.
- `missing`: no tutorial-ready implementation exists.
- `external`: external CASA/pipeline/viewer behavior is not an implementation
  target.

## Current Supported Surfaces Used By The Matrix

- `casa-tables`: persistent tables, storage, TaQL, table browsing.
- `casa-ms`: MeasurementSet summaries, selectors, plotting, derived MS support.
- `casa-calibration`: summary/stats/apply/gain/bandpass/fluxscale workflows.
- `casa-imaging` and `casars-imager`: MFS/cube imaging task surface and CASA
  `tclean` parity harnesses for existing deterministic cases.
- `casa-images`, `casa-lattices`, `casa-coordinates`: image/lattice/coordinate
  storage and analysis substrate.
- `casa-vla` and `casars-importvla`: VLA archive import surface.
- `casars-python`: `casars.data` object layer and calibration/importvla task
  projections; broader imager/MS/image-creation bindings are not yet complete.

## Capability Matrix

| CASA task/tool | Current casa-rs status | Library owner | App / Python exposure | Known tutorial gaps | Correctness oracle | Performance oracle | Owning issue / wave |
|---|---|---|---|---|---|---|---|
| `listobs` | partial | `casa-ms` | `msexplore` / future Python MS surface | selector grammar and TAQL extraction, reusable selector seam | CASA `listobs` on tutorial MS | timing for summary plus selected-row resolution | #75, #117, #121 |
| `plotms` | partial | `casa-ms` | `msexplore`, calibration plots | long-tail axes, corrected-data diagnostics, polarization/P-band plots | CASA `plotms` or exported plot data where available | `scripts/bench-msexplore-vs-casa.sh` plus tutorial plot timings | #73, #117, #121 |
| `plotcal` | partial | `casa-calibration` | `calibrate` diagnostic plots | CASA task-name compatibility is not a target; VLA pages need gain/bandpass/fluxscale diagnostic plot presets and export parity | CASA `plotcal` plots or exported caltable plot data where available | calibration plot timing on VLA tutorial caltables | #122, #128 |
| `tclean` MFS | partial/available | `casa-imaging`, `casars-imager` | `casars-imager`; Python projection includes automask, `savemodel=modelcolumn`, single-plane `startmodel`, and bounded `outlierfile` controls | tutorial parameter mapping and true multi-image outlier orchestration | CASA `tclean` image products plus MODEL_DATA comparisons for `savemodel=modelcolumn` and startmodel seeded model parity; #220 validates outlierfile parsing and rejects unsupported orchestration | imager parity scripts and tutorial timings | #117, #127, #167, #196, #219, #220 |
| `tclean` cube / spectral | partial | `casa-imaging`, `casars-imager` | `casars-imager`; Python projection includes automask controls | cube seam coverage, spectral transform integration, large-cube controls, mosaic cube gridding for Antennae Band 7 line products | CASA `tclean` cube outputs | cube parity scripts and tutorial timings | #74, #76, #119, #123, #161, #167 |
| legacy `clean` | external/compatibility reference | `casa-imaging`, `casars-imager` if comparison needed | no CASA alias planned | VLA imaging page references legacy output for comparison; implement equivalent capability through idiomatic imaging APIs, not a `clean` task clone | CASA legacy `clean` products only as regression inputs/comparison artifacts | compare legacy tutorial timing only if a breadth issue requires it | #128 |
| `split` | partial | `casa-ms`, `casa-tables` | no dedicated task surface yet | corrected-data split, selected column/subset output, metadata preservation | CASA `split` output MS, 2x2 MS interop as needed | tutorial split timings | #118, #121 |
| `mstransform` | missing/limited | `casa-ms` | none | spectral regrid/channel selection tutorial subset | CASA `mstransform` output MS | transform timing on VLA tutorials | #123 |
| `uvcontsub` | partial/available | `casa-calibration`, `casa-ms` | `calibrate uvcontsub`, Python calibration wrapper | line-free channel fitting, output MS semantics, field/data-column selection | CASA `uvcontsub` output MS and downstream image products | TW Hydra uvcontsub timing and cube parity note | #119, #123 |
| `imcontsub` | missing | `casa-images` | none | image-domain continuum subtraction for VLA HI breadth | CASA image products | breadth-wave timings | #128 |
| `gaincal` | partial/available | `casa-calibration` | `calibrate`, Python calibration tasks | selfcal loops, selection tails, solution diagnostics | CASA caltables and apply results | calibration benchmark scripts | #118, #122 |
| `applycal` | partial/available | `casa-calibration`, `casa-tables` | `calibrate`, Python calibration tasks | apply-path performance, tutorial selection tails, corrected split handoff | CASA corrected MS columns | `scripts/bench-calibrate-vs-casa.sh` | #94, #118, #122 |
| `bandpass` | partial/available | `casa-calibration` | `calibrate` | VLA tutorial parameter coverage, BPOLY tails | CASA bandpass caltables | calibration benchmark scripts | #122, #128 |
| `fluxscale` | partial/available | `casa-calibration` | `calibrate` | VLA tutorial models/fields, selection tails | CASA fluxscale tables/results | calibration benchmark scripts | #122 |
| `setjy` | missing/partial model setup | `casa-calibration`, `casa-ms` | no first-class task stage | calibrator model setup, component/image model writes | CASA model columns/caltable downstream parity | tutorial calibration timings | #122 |
| `gencal` | partial/available | `casa-calibration` | `calibrate gencal`, Python calibration wrapper | automatic antenna-position lookup, remaining caltypes beyond antpos/gceff/opac | CASA generated caltables | tutorial prior-cal timings | #121 |
| `flagdata` | partial | `casa-ms` | `flagdata` CLI, Rust library | manual, clip-zero, quack, tfcrop-family, rflag-family, extend, and summary exist; needs full same-input VLA flagging guide comparison once tutorial MS artifact is restaged | CASA FLAG column deltas | flagging timing on tutorial MS | #121, #128, #174 |
| `flagmanager` | partial | `casa-ms` | `flagmanager` CLI, Rust library | save/list/restore/delete/rename flag-version operations exist; needs CASA flag-version product comparison on the VLA flagging guide MS | CASA flag version products | VLA/ALMA breadth timing | #127, #128, #174 |
| `statwt` | missing | `casa-ms` | none | weight recomputation for data-combination tutorials | CASA WEIGHT/SIGMA columns | tutorial timing | #127, #128 |
| `concat` | missing/partial | `casa-ms`, `casa-tables` | none | MS concatenation metadata and weights | CASA `concat` output MS | data-combination timings | #127, #128 |
| `hanningsmooth` | missing | `casa-ms` | none | P-band tutorial subset | CASA smoothed MS | breadth timing | #128 |
| `clearcal` / `delmod` | missing/partial | `casa-ms`, `casa-calibration` | none | model/corrected column lifecycle | CASA MS column state | calibration/imaging timings | #128 |
| `ft` | missing/partial | `casa-imaging`, `casa-ms` | none | model prediction for source subtraction | CASA MODEL_DATA comparison | source-subtraction timing | #128 |
| component list tool `cl.addcomponent` | missing | future model/component owner | none | source subtraction and simulation component-list support | CASA component list / predicted MS | source-subtraction/simulation timings | #128, #129 |
| `imhead` | partial/available | `casa-images`, `casa-coordinates` | `imexplore imhead`, `casars.tasks.image_analysis.imhead` | edit modes and long-tail metadata formatting | CASA `imhead` output and image keywords | image-analysis timing | #120, #125 |
| `imstat` | partial/available | `casa-images`, `casa-lattices` | `imexplore imstat`, `casars.tasks.image_analysis.imstat` | full region grammar and long-tail CASA parameters | CASA `imstat` numeric output | image-analysis timing | #120, #125 |
| `immoments` | partial/available | `casa-images`, `casa-lattices`, `casa-coordinates` | `immoments`, `casars.tasks.image_analysis.immoments` | broader moment set and output-coordinate tails | CASA moment images | image-analysis timing | #120, #123 |
| `exportfits` | partial/available | `casa-images`, `casa-coordinates` | `exportfits`, `casars.tasks.image_analysis.exportfits` | full FITS-header fidelity and binary-table needs | CASA FITS headers and WCS | export timing | #120 |
| `importfits` | partial | `casa-images`, `casa-coordinates` | future task/Python projection | simulation model-image ingestion | CASA imported image metadata | simulation setup timing | #124 |
| `imregrid` | missing/partial | `casa-images`, `casa-coordinates` | none | data-combination and feathering image alignment | CASA regridded image | breadth timing | #127 |
| `immath` | partial via expression work | `casa-images` | future task/Python projection | expression syntax and image output parity | CASA image products | breadth timing | #120, #127 |
| `imsubimage` / `imcollapse` | missing/partial | `casa-images`, `casa-lattices` | future task/Python projection | region/channel slicing and collapsed outputs | CASA output images | breadth timing | #127 |
| `imfit` | missing | `casa-images` | none | Gaussian/source fitting for polarization/source-subtraction tutorials | CASA fit records | breadth timing | #127, #128 |
| `impv` | missing | `casa-images`, `casa-coordinates` | none | position-velocity extraction and metadata | CASA PV image output | VLA IRC timing | #123 |
| image tool `ia.open` | partial via object layer | `casa-images`, `casars-python` | `casars.data.Image` | CASA tool-method parity is not direct API target | CASA tool output where needed | image object timing | #120, #127 |
| viewer / CARTA flows | external | none | none | do not implement viewer; support exported products | product-open/read checks only | none | classified external |
| `simobserve` | missing | simulation owner, likely `casa-ms` plus image/coordinate support | future simulation task/Python surface | model prediction, antenna configs, time/frequency sampling, MS writes | CASA `simobserve` output MS | simulation generation timing | #124 |
| `simanalyze` | missing as workflow | composition of simulation, imaging, image analysis | future simulation task/Python surface | workflow orchestration without cloning CASA pipeline semantics | CASA `simanalyze` products | simulation analysis timing | #125 |
| `simalma` | missing | simulation owner | future task | ALMA/ACA array combinations | CASA `simalma` products | breadth timing | #129 |
| simulator tool `sm.open` / `sm.predict` | missing | simulation owner, `casa-ms` | future task/object projection | synthetic MS lifecycle and prediction | CASA simulator-tool output MS | simulation timing | #124, #129 |
| simulator tool `sm.setnoise` / `sm.setgain` / `sm.corrupt` | partial | simulation owner, calibration/noise models | `simobserve` task/Python corruption controls | deterministic simple noise, gain/phase, bandpass, polarization leakage, and primary-beam pointing offset; not full calibration-table corruption | CASA simulator noise+gain reference plus native common-corruption output | corrupted vs uncorrupted timing | #126 |
| `rmtables` | available through filesystem/task orchestration | app/test support | none as public task | cleanup convenience only | file existence behavior | none | no feature issue |

## Unsupported Parameter Families To Track Explicitly

Do not mark broad task names as done until these tutorial-visible parameter
families are either implemented or deliberately split:

- MS selectors: field, spw/channel, timerange, uvrange, intent, correlation,
  scan/antenna, datacolumn.
- Calibration selectors: field/spw/intent/uvrange/correlation support across
  solve/apply/diagnostics.
- Imaging controls: cube spectral mode, automasking, pbcor, startmodel,
  outlier fields, mosaics, polarization, usepointing, large-cube runtime
  controls.
- Image analysis selectors: region, mask, channel/stokes slicing, spectral
  coordinate metadata, output coordinate systems.
- Simulation controls: model image scaling, antenna/configuration files,
  observatory metadata, pointing, integration/time sampling, spectral setup,
  random seeds, thermal noise, gain/phase/bandpass/pointing/polarization
  corruptions.

## Oracle Harness Plan

Correctness:

- Use local CASA 6.7.5-9 as the primary oracle when available.
- Record each tutorial's declared CASA version in dataset/test manifests.
- Compare persisted products with CASA/casacore C++ where bytes or metadata
  matter: MS tables, caltables, images, FITS products.
- Heavy parity tests must skip cleanly when CASA/casacore or tutorial datasets
  are unavailable.

Performance:

- Record timings per vertical for CLI/app path and Python path where exposed.
- Use existing benchmark scripts when available:
  - `scripts/bench-calibrate-vs-casa.sh`
  - `scripts/bench-msexplore-vs-casa.sh`
- Add tutorial-specific timing scripts only in the wave that implements the
  relevant functionality.
- Severe regressions block a wave; non-severe gaps become shaped follow-ups
  unless the wave's acceptance says match/exceed is required.
