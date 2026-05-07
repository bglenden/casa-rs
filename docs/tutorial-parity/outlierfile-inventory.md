# CASA `outlierfile` Inventory

Truth class: source-backed implementation note
Last reality check: 2026-05-07
Verification: `cargo test -p casars-imager outlier_file -- --nocapture`

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
| `imagename` | starts and names an outlier image definition | parsed |
| `imsize` | outlier image shape | parsed |
| `cell` | outlier cell size | parsed |
| `phasecenter` | outlier image phase center | parsed |
| `startmodel` | outlier start model image | parsed |
| `mask` | outlier clean mask | parsed |
| `specmode` | outlier spectral mode | parsed |
| `nchan` | outlier channel count | parsed |
| `start` | outlier spectral start | parsed |
| `width` | outlier spectral width | parsed |
| `nterms` | outlier Taylor-term count | parsed |
| `reffreq` | outlier reference frequency | parsed |
| `gridder` | outlier gridder override | parsed |
| `deconvolver` | outlier deconvolver override | parsed |
| `wprojplanes` | outlier w-projection plane count | parsed |

Unknown fields are retained in the rejection diagnostic as ignored fields,
matching CASA's policy of not treating unrecognized pairs as supported imaging
controls.

## Execution Boundary

`outlierfile` execution is rejected for now. CASA's semantics require multiple
image definitions in one imaging run, shared major-cycle orchestration, and
per-outlier image/grid/deconvolution/norm overrides. The current casa-rs imager
frontend intentionally owns one image definition per run, so accepting an
outlier file without multi-image orchestration would silently produce the wrong
CASA behavior.
