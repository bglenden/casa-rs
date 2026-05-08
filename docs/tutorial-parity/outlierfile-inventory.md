# CASA `outlierfile` Inventory

Truth class: source-backed implementation note
Last reality check: 2026-05-07
Verification:
- `cargo test -p casars-imager outlier_file -- --nocapture`
- `cargo test -p casars-imager --features slow-tests outlierfile_ -- --nocapture`

## CASA Source Seams

- `casatasks/src/private/imagerhelpers/_gclean.py` documents `outlierfile`
  as a text file with one set of `parameter=value` pairs per outlier field.
- `casatasks/src/private/imagerhelpers/input_parameters.py::parseOutlierFile`
  parses the file, starts a new outlier definition at each `imagename`, and
  maps recognized fields into image, grid, deconvolution, and normalization
  parameter dictionaries.
- `makeImagingParamLists` clones the main image/grid/deconvolution/norm
  parameter sets for each outlier and then overlays the outlier-file values,
  which makes this a multi-image imaging path.

## Parsed Fields

`casars-imager` now parses and reports the CASA new-format outlier fields that
CASA documents for `tclean`:

| Field | CASA role | casa-rs status |
|---|---|---|
| `imagename` | starts and names an outlier image definition | required for execution |
| `imsize` | outlier image shape | executed for positive square sizes |
| `cell` | outlier cell size | executed for positive square arcsec cells |
| `phasecenter` | outlier image phase center | executed for supported J2000 text/radian directions |
| `startmodel` | outlier start model image | parsed; executed through the single-plane startmodel loader when present |
| `usemask` | outlier mask mode | `user` is accepted for outlier masks; `auto-multithresh` remains rejected |
| `mask` | outlier clean mask | CASA pixel circle regions such as `circle[[40pix,40pix],10pix]` are executed for the supported MFS/Hogbom slice; other region/image mask forms reject |
| `specmode` | outlier spectral mode | executed for `mfs`/`cont`; other modes reject |
| `nchan` | outlier channel count | `1` is accepted for MFS compatibility; other values reject |
| `start` | outlier spectral start | parsed; non-empty values reject |
| `width` | outlier spectral width | parsed; non-empty values reject |
| `nterms` | outlier Taylor-term count | `1` is accepted; other values reject |
| `reffreq` | outlier reference frequency | parsed; non-empty values reject |
| `gridder` | outlier gridder override | `standard`/`gridft`/`ft` are accepted; other values reject |
| `deconvolver` | outlier deconvolver override | `hogbom` is accepted; other values reject |
| `wprojplanes` | outlier w-projection plane count | `1` is accepted; other values reject |

Unknown fields are retained in the rejection diagnostic as ignored fields,
matching CASA's policy of not treating unrecognized pairs as supported imaging
controls.

## Execution Boundary

`casars-imager --outlierfile` now executes the main image and each supported
outlier definition as a CASA-style MFS/Hogbom image set. The dirty path clones
the main configuration, overlays the outlier image parameters, applies CASA's
standard-gridder outlier semantics, and runs the same MeasurementSet preparation
and image-writing path for each image definition.

For `niter>0`, the supported slice uses a joint multi-image Hogbom controller:
each image has its own residual/model/PSF plane, minor-cycle components are
chosen across the image set, and each major-cycle refresh subtracts the summed
models from the prepared visibilities using the per-image phase shifts captured
by the preparation trace. This avoids the known-wrong independent single-image
CLEAN behavior for cleaned outlier fields.

The source-backed parity gate now uses CASA's own
`refim_twopoints_twochan.ms` multifield fixture and the same upstream
`test_task_tclean.py` parameters: main `imsize=100`, `cell='8.0arcsec'`,
main `phasecenter='J2000 19:59:28.500 +40.44.01.50'`, outlier
`imsize=[80,80]`, outlier `phasecenter='J2000 19:58:40.895 +40.55.58.543'`,
`usemask=user`, and `mask=circle[[40pix,40pix],10pix]`. The latest local
evidence was:

```text
outlierfile dirty parity: rust_elapsed=1.410s casa_elapsed=6.742s main_rms=6.209766e-8 main_max_abs=7.152557e-7 main_corr=1.000000e0 outlier_rms=1.086336e-2 outlier_max_abs=1.319971e-1 outlier_corr=9.996859e-1
outlierfile clean parity: rust_elapsed=1.670s casa_elapsed=13.435s rust_minor=13 rust_major=2 main_image_50_50=1.072964e0/1.075265e0 outlier_image_40_40=5.588160e0/5.587516e0 main_residual_30_18=3.899402e-2/3.849955e-2 main_model_rms=2.384186e-9 main_model_max_abs=2.384186e-7 outlier_model_rms=1.394999e-5 outlier_model_max_abs=1.071334e-3
```

Instrumentation of the niter>0 model split showed that CASA single-field
`tclean` with the outlier phase center produces the same dirty residual and PSF
as the outlierfile path. Rust and CASA differed in the dirty outlier residual at
the standard gridding seam, and the first cleaned outlier source has near-tied
pixels whose residuals differ by less than 0.1%. The joint Hogbom controller now
keeps CASA's y-major scan order for those near ties, so the model split remains
stable against that gridding roundoff.
