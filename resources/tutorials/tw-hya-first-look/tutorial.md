# First Look at Imaging: TW Hya

This learner copy is yours to edit. The portable tutorial template remains immutable.

## Acquire the calibrated observation

Review the source, integrity, disk, and extraction facts before downloading. CASA-RS does not acquire data merely because this notebook was opened.

My notes:

## Inspect the observation

After the calibrated MeasurementSet is ready, inspect its fields, spectral windows, scans, and UV coverage from the dataset explorer.

## Load imaging parameters

Open the parameter block below to load the sparse tutorial overrides directly into the normal imager task tab. Running the task remains a separate user action.

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666666 kind=task -->
```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
vis = "data/twhya_calibrated.ms"
imagename = "products/twhya-continuum"
field = "5"
specmode = "mfs"
imsize = 250
cell = "0.1arcsec"
weighting = "briggs"
robust = 0.5
```
<!-- /casa-rs-cell -->

Record what you learn from the resulting image here.
