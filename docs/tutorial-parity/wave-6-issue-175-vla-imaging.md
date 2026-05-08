# Wave 6 Issue 175 VLA Imaging

Verification: `cargo test -p casa-imaging mtmfs --lib`; `cargo test -p casars-imager pblimit --lib`; `cargo test -p casars-imager single_field_primary_beam_product --lib`; `cargo test -p casars-imager outlier_file_request_accepts_vla_imaging_multiscale_modelcolumn_slice --lib`; `cargo test -p casa-images image_analysis_task_dispatch_roundtrips_real_image_products --lib`; `PYTHONPATH=crates/casars-python/python python -m pytest crates/casars-python/python/tests/test_image_analysis.py -q`; `cargo build --release -p casars-imager --bin casars-imager -p casa-images --bin immath -p casa-images --bin imexplore`; reduced CASA/Rust MT-MFS multiscale W-projection smoke on `SNR_G55_10s.calib.ms`.

## Scope

Issue #175 covers the VLA CASA Imaging guide from the CASA 6.5.4 scripted
source. The implemented and scripted non-ASP surface is:

- `tclean` dirty and regular MFS products with `niter=0`, `niter=1000`,
  `niter=10000`, and `nmajor=4`.
- Natural, uniform, and Briggs weighting examples.
- Multiscale MFS and multiscale MFS with W-projection.
- MT-MFS (`nterms=2`) and MT-MFS with W-projection.
- Multiscale outlier-field imaging with `savemodel='modelcolumn'`.
- Regular primary-beam correction through image/PB division.
- Wideband PB-corrected MT-MFS products from `casars-imager --pbcor`.
- `imhead` summary/list plus tutorial `mode='put', hdkey='bunit'`.
- `immath` scalar image expression for the brightness-temperature example.

The experimental ASP examples are explicitly split to backlog issue #223.

## Inputs

| Artifact | Local policy | Evidence |
|---|---|---|
| CASA guide source | `target/VLA_CASA_Imaging-CASA6.5.4.raw.wiki` | oldid 36701, last modified 2024-06-10 |
| Official noninteractive script | `target/VLACASAImaging-CASA6.5.4_testscript.tgz` | SHA-256 `8c15e776ca6f8f6bd4a6a3c67044ed8f258c77571550c34b39f5427bf758f4a2` |
| Calibrated MS archive | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/vla/imaging/SNR_G55_10s.calib.tar.gz` | size `1250616054` bytes, SHA-256 `b79a63d1142674c89c4c3ae702a28625867728a420a3c156e0ec44078200bf6a` |

The registry key is `vla/imaging/calibrated-ms`.

## Implementation

`casars-imager` now accepts the guide's negative `pblimit` policy, writes
single-field `.pb` products for the VLA imaging paths, and writes PB-corrected
regular and MT-MFS products when requested. MT-MFS now supports the tutorial
combination of multiscale terms and W-projection, and `savemodel=modelcolumn`
handles the multi-DDID VLA input by preparing per-DDID selections before merging
the imaging inputs.

`casa-images` now supports the guide's scalar `immath` expression and the
tutorial `imhead(mode='put', hdkey='bunit')` operation. The Python image-analysis
wrapper can pass the same `imhead` mode/key/value arguments through `imexplore`.

The reproducible runner is:

```sh
CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data \
  scripts/run-wave6-issue175-vla-imaging.sh /Volumes/GLENDENNING/casa-rs/issue175-runs/smoke
```

The default `smoke` case runs a reduced but same-parameter CASA/Rust comparison
for MT-MFS, multiscale, W-projection, `spw=0`, `imsize=128`, and `niter=1`.
The full non-ASP Rust tutorial command sequence is available with
`CASA_RS_WAVE6_ISSUE175_CASES=official-rust`.

## Evidence

Reduced same-parameter smoke, `SNR_G55_10s.calib.ms`, `spw=0`, `imsize=128`,
`niter=1`, `deconvolver='mtmfs'`, `nterms=2`, `scales=[0,6,10,30,60]`,
`gridder='wproject'`:

| Product | Correlation | RMS diff | Max abs diff | Rust max | CASA max |
|---|---:|---:|---:|---:|---:|
| `.image.tt0` | `0.9982898032493348` | `7.243814850152627e-05` | `0.0006394009105861187` | `0.00735859852284193` | `0.007904737256467342` |
| `.residual.tt0` | `0.9982806318387037` | `7.098060187728562e-05` | `0.0006373929791152477` | `0.007056406233459711` | `0.007693799212574959` |
| `.model.tt0` | `0.9999999999999678` | `5.17070475325454e-07` | `6.618502084165812e-05` | `0.0007245508022606373` | `0.0007907358231022954` |

Runtime for that reduced smoke:

| Engine | Wall time |
|---|---:|
| CASA C++ | `11.103140165796503 s` |
| casa-rs | `37.98 s` |

The reduced smoke is intentionally small enough for local review. The full
1280-pixel tutorial sequence is scripted but not used as the default loop
because it writes large products and takes substantially longer.
