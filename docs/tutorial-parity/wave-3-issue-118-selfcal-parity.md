# Wave 3 Issue 118 - ALMA First Look TW Hydra Self-Calibration Parity

Truth class: current descriptive
Last reality check: 2026-04-28
Verification: focused calibration tests; full TW Hydra self-cal loop commands and evidence below

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

## Full Loop Evidence

CASA evidence is under:

```text
target/wdad-wave3-118/casa
```

The current casa-rs evidence is under:

```text
target/wdad-wave3-118/rust_full_selfcal_matched
target/wdad-wave3-118/evidence/final_image_fftmodel_panel.png
```

The Rust loop used release binaries and a fresh copy of the tutorial MS:

```bash
cargo build --release -p casars-imager --bin casars-imager -p casa-calibration --bin calibrate
python target/wdad-wave3-118/run_rust_selfcal_matched.py
```

The final restored-image panel was rendered with:

```bash
cargo run --release -q -p casars-imager --example render_image_panels -- \
  --rust target/wdad-wave3-118/rust_full_selfcal_matched/final_image.image \
  --casa target/wdad-wave3-118/casa_fresh/final_image.image \
  --output target/wdad-wave3-118/evidence/final_image_fftmodel_panel.png
```

Final restored image comparison:

| Metric | CASA | casa-rs |
|---|---:|---:|
| peak | `0.388116` | `0.385533` |
| RMS | `0.013049` | `0.013044` |
| difference RMS |  | `0.000789` |
| max absolute difference |  | `0.004294` |

Solution SNR/flag comparison:

| Table | CASA flagged / npts | casa-rs flagged / npts | CASA median SNR | casa-rs median SNR |
|---|---:|---:|---:|---:|
| `phase.cal` | `86 / 312` | `86 / 312` | `19.609906` | `14.350530` |
| `phase_2.cal` | `291 / 832` | `313 / 832` | `7.433371` | `6.657993` |
| `phase_3.cal` | `706 / 1690` | `672 / 1690` | `5.881750` | `5.970596` |
| `amp.cal` | `43 / 156` | `43 / 156` | `13.271138` | `18.246287` |

The remaining differences are within the expected tolerance for this
first-wave native solver: the final restored images agree visually and
numerically, low-SNR solutions are now represented in the caltable instead of
being silently treated as valid, and subsequent apply/split/imaging stages
accept the corrected data.

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
| casa-rs | `177.788` | `135.373` | `42.416` |

Current matched casa-rs full-loop timings:

| Step | Seconds |
|---|---:|
| first image | `27.079` |
| phase inf G solve | `5.495` |
| apply phase inf | `5.958` |
| split selfcal | `6.260` |
| second image | `31.258` |
| phase 170s G solve | `3.966` |
| apply phase 170s | `3.412` |
| split selfcal 2 | `1.704` |
| third image | `23.088` |
| phase 30s T solve | `3.440` |
| apply phase 30s | `3.029` |
| split selfcal 3 | `1.728` |
| fourth image | `25.454` |
| amp inf T solnorm solve | `3.190` |
| apply amp | `2.578` |
| split final | `1.657` |
| final image | `28.495` |

These timings are performance-parity evidence for the full #118 loop on the
matched local no-pointing CASA/casa-rs branches: casa-rs is within timing noise
of CASA end to end, with faster imaging offset by slower first selected split
and calibration table I/O.

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
just quick
```
