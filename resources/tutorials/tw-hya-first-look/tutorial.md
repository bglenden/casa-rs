# First Look at Imaging: TW Hya

This learner copy is yours to edit. The portable tutorial template remains immutable.

This CASA-RS tutorial follows the primary path of the official
[First Look at Imaging CASA 6.6.6](https://casaguides.nrao.edu/index.php/First_Look_at_Imaging)
guide using the same calibrated TW Hya MeasurementSet and the same scientific
selections. It replaces CASA's interactive `tclean` mask viewer with explicit,
reproducible task cells. The optional calibration/flagging comparison in the
official guide needs two additional datasets and is not part of this first
learner pack.

## What you will do

1. Acquire and verify the calibrated observation.
2. Read the MeasurementSet summary and identify the target and calibrators.
3. Inspect UV coverage and amplitude versus UV distance.
4. Make dirty and cleaned images of the phase calibrator.
5. Split out and average the TW Hya target data.
6. Make a reproducible continuum image and apply primary-beam correction.

Every parameter block opens the normal CASA-RS task tab. Loading parameters
does not run the task; review the highlighted tutorial overrides and choose
Run yourself. Task runs and products are recorded in this notebook.

## 1. Acquire the calibrated observation

The source archive is about 416 MB and expands to roughly 1 GB. In the tutorial
status card above, choose **Review**. Confirm that the approval sheet shows:

- the NRAO `FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar` source;
- destination `data/twhya_calibrated.ms`;
- expected SHA-256 ending in `a97b2`;
- enough free space for the bounded extraction plan.

Choose **Approve and Download** only after reviewing those facts. CASA-RS
downloads into this project, verifies the digest, checks the MeasurementSet,
and publishes it atomically. Wait for the dataset state to become **Ready**.

My acquisition notes:

## 2. Get oriented with the observation

Open **Datasets** in the left dock and double-click `twhya_calibrated.ms`.
The **Summary** view is CASA-RS's `listobs` equivalent. Check the following
against the summary:

- the observation has 68,335 data records from 2012-11-19;
- field 3 is the phase calibrator J1037-295;
- field 5 is the science target TW Hya;
- spectral window 0 has 384 channels across 234.375 MHz near 372.65 GHz;
- the correlations are XX and YY.

The scan intents explain why the fields are present: bandpass, amplitude, and
phase calibrators surround repeated target scans. This is the information you
will use when selecting fields for plotting and imaging.

My summary notes:

## 3. Inspect the UV coverage

In the same MeasurementSet explorer:

1. Select **Plots**.
2. Choose **UV Coverage**.
3. Open **Selections** and leave **Color By** set to **Field**.
4. Choose **Generate**.
5. When the plot is ready, use **Save to Notebook → New plot**.

The plotted `u,v` samples should be mirrored about the origin. Each track is a
projected antenna baseline; the collection of tracks is the Fourier-plane
sampling that determines the synthesized beam. Dense, well-distributed
coverage generally produces a better-behaved point-spread function.

What do the gaps and longest baselines suggest about the angular resolution?

## 4. Compare amplitude with UV distance

Still in **Plots**:

1. Choose **Amplitude vs UV Distance**.
2. In **Selections**, set **Iterate by** to **Field**.
3. Average channels and time enough to make the field-to-field trend clear.
   For this one-SPW dataset, averaging all 384 channels is appropriate.
4. Choose **Generate**, step through the field panels, and save the result to
   the notebook.

The calibrators and target need not have the same shape. In particular, Ceres
(field 2) changes amplitude with UV distance because it is resolved. A point
source would remain approximately flat apart from noise and calibration error.

My visibility notes:

## 5. Make a dirty image of the phase calibrator

The official guide first calls `tclean` with no iterations. The result is a
dirty image: the sky convolved with the synthesized beam, before deconvolution.
This cell makes that intent explicit with `niter = 0` and `dirty_only = true`.

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666601 kind=task -->
```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
vis = "data/twhya_calibrated.ms"
imagename = "products/phase-cal-dirty"
field = "3"
phasecenter = "3"
specmode = "mfs"
gridder = "standard"
deconvolver = "hogbom"
imsize = 250
cell = "0.1arcsec"
weighting = "briggs"
robust = 0.5
niter = 0
threshold = "0.0mJy"
dirty_only = true
write_pb = true
```
<!-- /casa-rs-cell -->

After the run, open the `.image`, `.residual`, and `.psf` products from the run
card. With zero iterations, the image and residual should represent the same
undeconvolved sky estimate, while the PSF shows the synthesized beam response.

My dirty-image notes:

## 6. Clean the phase calibrator

CASA's guide next uses an interactive mask and several major cycles. CASA-RS
does not yet provide that interactive mask viewer, so this learner pack uses
the repository's parity-tested noninteractive equivalent: the same Briggs
weighting, a central box mask, a 15 mJy stopping threshold, and a generous
iteration limit.

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666602 kind=task -->
```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
vis = "data/twhya_calibrated.ms"
imagename = "products/phase-cal-clean"
field = "3"
phasecenter = "3"
specmode = "mfs"
gridder = "standard"
deconvolver = "hogbom"
imsize = 250
cell = "0.1arcsec"
weighting = "briggs"
robust = 0.5
niter = 10000
threshold = "15mJy"
mask_box = "100,100,150,150"
write_pb = true
```
<!-- /casa-rs-cell -->

Compare the cleaned image and residual with the dirty result. The model should
contain the compact calibrator emission, and the strongest sidelobe structure
should be reduced in the residual.

Optional guide experiment: reload this cell and try `robust = -1.0`, then image
Ceres with `field = "2"`. Compare beam size, noise, and how Ceres resolves with
baseline length. Use a different `imagename` for every experiment.

My phase-calibrator notes:

## 7. Split and average the science target

The official guide selects TW Hya (field 5) and averages each group of eight
channels before continuum imaging. This reduces the data volume without
discarding useful continuum information.

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666603 kind=task -->
```toml
[casars]
format = 1
surface = "split"
kind = "task"
contract = 1

[parameters]
vis = "data/twhya_calibrated.ms"
outputvis = "data/twhya_smoothed.ms"
field = "5"
width = 8
datacolumn = "DATA"
keepflags = true
```
<!-- /casa-rs-cell -->

When the split succeeds, open `twhya_smoothed.ms` in **Datasets** and inspect
its Summary. Because only TW Hya was retained, the output field is now field 0.
Confirm that the channel count and row count reflect the requested selection
and averaging.

My split-data notes:

## 8. Make the TW Hya continuum image

This cell is the reproducible CASA-RS counterpart of the guide's automatic
science-target clean. Multifrequency synthesis combines the selected channels
into one continuum image. The 0.1 arcsec cell puts several pixels across the
roughly half-arcsecond synthesized beam, and Briggs `robust = 0.5` balances
resolution against sensitivity.

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666666 kind=task -->
```toml
[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
vis = "data/twhya_smoothed.ms"
imagename = "products/twhya-continuum"
field = "0"
phasecenter = "0"
specmode = "mfs"
gridder = "standard"
deconvolver = "hogbom"
imsize = 250
cell = "0.1arcsec"
weighting = "briggs"
robust = 0.5
niter = 10000
threshold = "15mJy"
mask_box = "100,100,150,150"
write_pb = true
```
<!-- /casa-rs-cell -->

Open the resulting image, model, residual, PSF, PB, and sum-of-weights products.
TW Hya should be bright and resolved relative to the beam. Check that the
residual outside the central mask looks noise-like and that the run stopped at
the threshold or before the iteration limit.

My continuum-image notes:

## 9. Apply primary-beam correction

The `.image` product is not corrected for the sensitivity falloff of an ALMA
12-m antenna away from the pointing center. Apply the `.pb` response to make a
science-ready intensity image:

<!-- casa-rs-cell:v1 id=019f6666-6666-7666-8666-666666666605 kind=task -->
```toml
[casars]
format = 1
surface = "impbcor"
kind = "task"
contract = 1

[parameters]
imagename = "products/twhya-continuum.image"
pbimage = "products/twhya-continuum.pb"
outfile = "products/twhya-continuum.pbcor.image"
mode = "divide"
cutoff = -1.0
overwrite = true
```
<!-- /casa-rs-cell -->

Compare the corrected and uncorrected images. The source flux is corrected for
the primary beam, but the correction also amplifies noise toward the edge.
Use the uncorrected image for uniform-noise source finding and the corrected
image for flux or intensity measurements.

## 10. Record what you learned

- Which fields are calibrators, and which field is TW Hya?
- How did the UV coverage relate to the synthesized beam?
- What changed between the dirty and cleaned phase-calibrator images?
- Why was the target split and channel-averaged before imaging?
- Why are both corrected and uncorrected continuum images useful?

My conclusions:
