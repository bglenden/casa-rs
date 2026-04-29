# Wave 3 Issue 118 - ALMA First Look TW Hydra Self-Calibration Parity

Truth class: current descriptive
Last reality check: 2026-04-29
Verification: just verify; full TW Hydra self-cal loop commands and evidence below

Wave issue: #140
Child issue: #118

This note records the CASA-to-casa-rs mapping for the ALMA First Look / TW Hydra
self-calibration loop. It builds on the #117 MFS foundation and keeps task
ownership in the existing `casars-imager` and `calibrate` executables, with the
Python wrapper staying a thin JSON-protocol client.

## Tutorial Mapping

| CASA tutorial operation | casa-rs owner | Wave 3 #118 mapping |
|---|---|---|
| `tclean(..., savemodel="modelcolumn")` | `casars-imager` | `--savemodel modelcolumn` predicts the final clean model into MAIN.MODEL_DATA. |
| `gaincal(..., calmode="p", gaintype="G"|"T")` | `calibrate solve-gain` | `--model-column --mode-gain p --gain-type g|t --minsnr 3.0` solves from MODEL_DATA and flags low-SNR solutions. |
| `gaincal(..., calmode="ap", gaintype="T", solnorm=True)` | `calibrate solve-gain` | `--mode-gain ap --gain-type t --solnorm --minsnr 3.0` solves final amplitude+phase gains. |
| `applycal(..., interp=["linear"], calwt=False)` | `calibrate apply` | Applies the current caltable chain without calibration weight mutation. |
| `split(..., datacolumn="corrected")` | `calibrate export-corrected` | Copies selected CORRECTED_DATA into DATA in a selected output MS. |

## Implementation Notes

Corrected-data export now honors the same structured MS selection controls as
the rest of the calibration surface. For the tutorial this means the Rust split
step materializes the `field=5, spw=0` self-cal dataset instead of copying the
full calibrated MS.

The gain solver now stores CASA-style diagnostics in `PARAMERR`, `SNR`, and
`WEIGHT`. `PARAMERR` is derived from the final Hessian and reduced chi-square,
`SNR = abs(CPARAM) / PARAMERR`, and `--minsnr` flags solutions whose SNR is not
greater than the threshold. The CLI/task/Python default is `3.0`, matching the
CASA tutorial path; lower-level Rust tests use `min_snr=0.0` where they need to
exercise pure solver behavior without threshold flagging.

MODEL_DATA solves weight each sample by visibility weight times model strength,
`weight * |MODEL_DATA|^2`. On the isolated first phase solve using CASA-written
MODEL_DATA, this reduced the common-solution phase RMS to
`0.02961474863587555 rad`.

`savemodel=modelcolumn` now uses the same FFT-backed standard-gridder prediction
path as the major-cycle residual refresh instead of direct component summation,
and apply/export write back only the changed tiled columns where the MS layout
permits it.

Linear time interpolation of complex calibration tables now follows CASA's
`ROCTMainColumns::fparamArray("") -> CTTimeInterp1 -> RIorAPArray` path:
`CPARAM` is converted to tracked amplitude/phase, interpolated as floats, and
converted back to complex gains. Applying CASA's own `phase_2.cal` with
casa-rs now matches CASA on unflagged `CORRECTED_DATA` with median relative
difference `8.1e-8`, p95 `2.7e-7`, and max `1.4e-6`; the remaining high-tail
samples in the all-sample comparison are flagged in both CASA and casa-rs.

Scalar `T` phase solves now keep parallel-hand correlations as separate
frequency/time-collapsed residual contributions until graph accumulation.
CASA uses one scalar parameter for `T Jones`, but it does not average RR and LL
into one complex pseudo-sample before phase normalization. On CASA's
`twhya_selfcal_2.ms` input this reduced the isolated `phase_3.cal` p95 phase
difference from roughly `0.09-0.15 rad` to `0.0459 rad`.

The phase-only iterative solver now follows CASA's `VisCalSolver2` diagonal
Levenberg-Marquardt path for complex gains: residuals are `predicted -
observed`, the per-antenna update is `-grad / (2*hess)`, CASA's parabolic
step-size search is applied, and phase-only normalization is deferred until the
post-solve conditioning step. Temporary CASA C++ traces showed the prepared
scalar-`T` solve edges after model collapse, AP enforcement, flags, and weights
matched to `~1e-5`, and the traced CASA gradient/Hessian for the first solve
interval is reproduced from the casa-rs edge sums to `~1e-5`. A second trace
around CASA `SolvableVisJones::applyRefAnt` showed the remaining delta is not
from reference-antenna application. Against a fresh current CASA rerun of only
`phase_3.cal`, the isolated `T` comparison is now median `0.0019 rad`, p95
`0.0149 rad`, p99 `0.0245 rad`, and max `0.1004 rad`; the comparison plot is
`target/wdad-wave3-118/evidence/issue118_t_phase_lm_comparison.png`.

CASA computes `PARAMERR` and `SNR` before global refant and phase-only
post-solve normalization. The Rust phase-only solver now preserves the
pre-normalization gains for the Hessian and chi-square error path while writing
phase-only `CPARAM`, matching CASA's diagnostic ordering. On the phase-2 G
solve from an identical CASA-model input this reduced flag mismatches from 14
cells to 1 borderline cell (`CASA SNR=2.998`, Rust SNR=3.138`) without changing
common-solution phases.

## Full Loop Evidence

CASA evidence is under:

```text
target/wdad-wave3-118/casa_fresh
```

The current casa-rs evidence is under:

```text
target/wdad-wave3-118/rust_full_selfcal_matched
target/wdad-wave3-118/evidence/current_final_image_zoom_panel.png
```

The Rust loop used release binaries and a fresh copy of the tutorial MS:

```bash
cargo build --release -p casars-imager --bin casars-imager -p casa-calibration --bin calibrate
python target/wdad-wave3-118/run_rust_selfcal_matched.py
```

The final restored-image zoom panel is:

```text
target/wdad-wave3-118/evidence/current_final_image_zoom_panel.png
```

Final restored image comparison:

| Metric | CASA | casa-rs |
|---|---:|---:|
| peak | `0.388116` | `0.388948` |
| RMS | `0.013049` | `0.013104` |
| difference RMS |  | `0.000406` |
| max absolute difference |  | `0.003478` |
| correlation |  | `0.999524` |

Current restored image progression:

| Stage | CASA peak | casa-rs peak | diff RMS | correlation |
|---|---:|---:|---:|---:|
| first image | `0.307186` | `0.307009` | `0.000100` | `0.999962` |
| second image | `0.338573` | `0.338329` | `0.000074` | `0.999979` |
| third image | `0.366629` | `0.366685` | `0.000206` | `0.999866` |
| fourth image | `0.385059` | `0.384625` | `0.000359` | `0.999620` |
| final image | `0.388116` | `0.388948` | `0.000406` | `0.999524` |

The restored image loop now stays close through the final image. The remaining
differences are small compared with the prior phase-2/T divergence and are
consistent with the remaining marginal SNR/flag decisions plus small clean
iteration differences.

A matched solution-rank comparison on the final CASA/casa-rs runs still shows
broad tails:

| Table | Metric | Median | 95th percentile | 98th percentile | 99th percentile | Max |
|---|---|---:|---:|---:|---:|---:|
| `phase.cal` | abs phase diff rad | `0.0054` | `0.0189` |  | `0.0331` | `0.0386` |
| `phase_2.cal` | abs phase diff rad | `0.0024` | `0.0133` |  | `0.0237` | `0.0265` |
| `phase_3.cal` | abs phase diff rad, full-chain run | `0.0043` | `0.0231` |  | `0.0632` | `0.3068` |
| `phase_3.cal` | abs phase diff rad, isolated CASA-like LM solve | `0.0019` | `0.0165` |  | `0.0244` | `0.0942` |
| `amp.cal` | abs phase diff rad | `0.0061` | `0.0235` |  | `0.0395` | `0.0849` |
| `amp.cal` | fractional amplitude diff | `0.0085` | `0.0475` |  | `0.0664` | `0.1171` |

Flag parity is exact for `phase.cal` and `amp.cal`, differs by 1 cell for
`phase_2.cal`, and differs by 10 cells for `phase_3.cal`; the `phase_3.cal`
differences are marginal SNR decisions around the threshold.

## Timing Evidence

Fresh full-loop timings were collected on 2026-04-28 with CASA run through:

```text
/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python
```

CASA evidence is under:

```text
target/wdad-wave3-118/casa_fresh
```

Matched casa-rs evidence is under:

```text
target/wdad-wave3-118/rust_full_selfcal_matched
```

Current matched full-loop totals:

| Runtime | Total seconds | Imaging seconds | Calibration/apply/export seconds |
|---|---:|---:|---:|
| CASA | `175.962` | `166.449` | `9.513` |
| casa-rs | `179.484` | `142.781` | `36.702` |

Current matched casa-rs full-loop timings:

| Step | Seconds |
|---|---:|
| first image | `28.004` |
| phase inf G solve | `3.101` |
| apply phase inf | `5.979` |
| split selfcal | `6.138` |
| second image | `31.462` |
| phase 170s G solve | `2.669` |
| apply phase 170s | `3.051` |
| split selfcal 2 | `1.661` |
| third image | `25.487` |
| phase 30s T solve | `2.632` |
| apply phase 30s | `2.995` |
| split selfcal 3 | `1.689` |
| fourth image | `27.342` |
| amp inf T solnorm solve | `2.580` |
| apply amp | `2.577` |
| split final | `1.631` |
| final image | `30.486` |

The current full loop is within roughly 2% of the recorded CASA runtime. Rust
imaging is faster on the high-iteration stages, while calibration/apply/export
remains slower than CASA but no longer dominates the end-to-end result.

## Verification

Focused checks:

```bash
cargo test -p casa-calibration --test gain_solve -- --nocapture
cargo test -p casa-calibration --lib task_contract -- --nocapture
cargo test -p casa-calibration --lib parse_args_accepts_solve_gain_command -- --nocapture
cargo test -p casa-calibration --test apply_execute export_corrected_data -- --nocapture
cargo test -p casars-imager --lib end_to_end_smoke_writes_casa_products -- --nocapture
cd crates/casars-python && uv run --extra test python -m pytest python/tests/test_calibrate.py -q
```

Full branch gate:

```bash
just verify
```
