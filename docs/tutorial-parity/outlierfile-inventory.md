# CASA `outlierfile` Inventory

Truth class: source-backed implementation note
Last reality check: 2026-08-31
Verification:
- `just imaging-t31-multidomain-geometry <testdata_root> <casa_prefix>`

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

The application parser accepts the CASA new-format outlier fields that
CASA documents for `tclean`:

| Field | CASA role | casa-rs status |
|---|---|---|
| `imagename` | starts and names an outlier image definition | required for execution |
| `imsize` | outlier image shape | executed for positive square sizes |
| `cell` | outlier cell size | executed for positive square arcsec cells |
| `phasecenter` | outlier image phase center | executed for supported J2000 text/radian directions |
| `startmodel` | outlier start model image | parsed; non-empty values reject |
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

`casars-imager --outlierfile` compiles the main image and supported outliers as
one canonical image-domain collection. `casa-ms` supplies one ordered selected
observation stream with explicit GridFT projections for every domain;
reconstruction predicts the summed domain model once, subtracts it once, and
forms each domain's PSF and residual through the same bounded operator route.
The runtime retains one shared major-cycle lineage and one atomic model
generation rather than launching independent imaging runs.

For `niter>0`, each compiled field receives CASA's shared minor-cycle control
record and runs a deterministic field-local Högbom controller in canonical
domain order. The iteration budget is compared only after every field exits its
minor cycle, so the aggregate may exceed `niter`. All accepted field terms are
still combined into one model delta before the shared visibility-domain
residual refresh.

The source-backed parity gate now uses CASA's own
`refim_twopoints_twochan.ms` multifield fixture and the same upstream
`test_task_tclean.py` parameters: main `imsize=100`, `cell='8.0arcsec'`,
main `phasecenter='J2000 19:59:28.500 +40.44.01.50'`, outlier
`imsize=[80,80]`, outlier `phasecenter='J2000 19:58:40.895 +40.55.58.543'`,
`usemask=user`, and `mask=circle[[40pix,40pix],10pix]`. The T31 frozen-CASA
gate requires exact WCS and at most `1e-3` normalized RMS for every main and
outlier PSF, residual, model, and restored image. The accepted evidence is:

```text
dirty main:    psf=6.488577e-7 residual=7.482402e-8 model=0 image=7.482402e-8
dirty outlier: psf=6.315367e-7 residual=5.605816e-7 model=0 image=5.605816e-7
clean main:    psf=6.488577e-7 residual=8.047645e-7 model=8.472027e-7 image=6.778549e-7
clean outlier: psf=6.315367e-7 residual=1.682477e-6 model=8.814884e-7 image=6.758271e-7
clean controller: charged=13 actual=14 (main actual=3, outlier actual=11)
```

The GridFT projection boundary is distinct from direct `fixvis` reprojection:
it applies CASA's negated-UV convention, rotates from source to target, restores
the external UVW convention, and stores the opposite phase scalar required by
the Rust adjoint exponent. Prediction consumes the conjugate of that same
compiled phase. This shared projection removed the former outlier PSF-shape
error without a mode-specific operator or spectral-sign exception.
