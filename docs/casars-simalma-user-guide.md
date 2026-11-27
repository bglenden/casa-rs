# ALMA and ACA simulation

Truth class: current descriptive
Last reality check: 2026-11-15
Verification: `cargo test -p casa-parity --test api simalma_casa::` and `cargo test -p casa-simulation --test api simalma_parameters::`

`casars.tasks.simalma` runs native ALMA/ACA observations, images each array,
concatenates the interferometric data and combines them with total power.
The [parameter reference](reference/task-parameters.md) gives CASA's exact
signature and defaults. Computation runs in Rust through the same simulator,
concat, single-dish gridder, synthesis imager and image-analysis applications.

```python
from casars.tasks import simalma

simalma(project="m51", skymodel="M51ha.fits", dryrun=False,
        antennalist=["alma.cycle6.3.cfg", "aca.cycle6.cfg"],
        totaltime="1800s", tpnant=2, tptime="7200s", pwv=0.6,
        imsize=[128, 128], mapsize="1arcmin", graphics="none")
```

The default `dryrun=True` checks the model/configurations and reports the
planned exposures without publishing simulated data. `overwrite=False`
protects an existing project. With overwrite enabled, input images, pointing
files and antenna configurations inside that project are refused before the
old directory can be removed.

## Observing times and total power

A vector `totaltime` supplies one exposure per configuration. A single time
belongs to the highest-resolution configuration; lower-resolution 12 m
configurations receive half that time, ACA 7 m twice, and an explicitly
listed TP configuration four times. Array resolution strings use the same
configuration resolver as simobserve. Configuration names without a cycle
number work without CASA's Python-3 comparison failure.

`tpnant` selects the TP antenna count. Each observes for `tptime`; multiple
antennas produce `.sd.ms.Ant0`, `.sd.ms.Ant1`, etc., with independent noise
streams. `pwv=0` disables thermal noise; positive values select the native
ATM noise model. CASA truncates TP exposure to integer minutes; a requested
TP exposure below one minute is refused instead of starting a zero-time run.

For a rectangular mosaic, TP covers one additional 12 m primary beam.
For explicit pointings, it chooses the smaller of a rectangle covering all
pointings plus two beams, or a two-beam patch around each pointing.
Its square sampling is `0.96 * lambda / (3D)`. Integration is shortened when
needed to visit every pointing within the requested time.

TP imaging uses the SF kernel with support 6 and nine cells per beam. Its
own CASA intensity factor is `gridded_major^2 / (0.96 * PB12)^2`, rounded to
six decimal places. This differs deliberately from simanalyze's formula.
A private native concatenation feeds multiple TP antennas to the single-MS
gridder, preserving weights, antenna metadata and pointings.

## Imaging and combination

Each configuration is imaged and analyzed with Briggs weighting. The final
interferometric concatenation weights 12 m arrays by 1 and 7 m arrays by
0.34. TP is regridded onto the synthesis grid and multiplied by its primary
beam before feathering; the result is divided by that beam. The project
contains the array and concatenated images, differences and fidelities,
TP image, `.sd.image.pbscaled`, `.feather.image0` and `.feather.image`.

`graphics="file"`, `"screen"` and `"both"` prepare complete observation,
image/analysis and combination figures through the Python renderer;
`"none"` omits them. A 12 m/7 m/two-TP-antenna run writes 14 PNGs.
The final TP antenna supplies the shared TP figure names, as in CASA.
Numerical products are preserved while making the summary figure. CASA's
`.last` files are not written.

## Independent evidence

`tools/simulation/simalma_oracle.py` runs CASA 6.7.6.14 and records the compact
fixture or the full upstream `regression/simalma_12m_ACA_combination` recipe.
`tools/simulation/simalma_regression.py` compares every produced image and MS.
The deterministic full recipe has 54 image products and five MS products;
its worst image NRMS is 6.18e-6. Clean DATA is below 1e-6 NRMS.

The original noisy recipe has 52 image products and nine MS products.
CASA and casa-rs use different random streams. Independent noise RMS comparisons check generation, and SIGMA is checked
as MS metadata. The oracle then supplies the
same native noise samples to CASA's DATA and CORRECTED_DATA for imaging.
The resulting independent CASA image calculations match within 1.70e-5
NRMS. This common stochastic input is explicit; it is not a reference
computed by Rust.

Other oracle controls are the already documented physical-epoch alignment,
physical SF beam and full-precision fidelity floor, restored legacy Python
callables, and sorted sky-model discovery. The compact project name avoids
CASA's known position-angle expression bug. Exact masks and all defined
pixels are checked; undefined masked non-finite samples are serialized as
JSON nulls. The integrated counting law covers the whole task at two image
sizes, including TP concatenation, gridding and weighted feathering.

Three serial release runs of the complete noisy upstream recipe, with no
compiler running, have medians of 19.95 s native and
31.91 s CASA (1.60 times faster). This includes all
observations, per-antenna TP, imaging, analysis and weighted feathering. The
timing runs use each implementation's own noise stream and exclude the
comparison oracle's common-noise transfer.

The graphics regression compares all 14 simalma figures and seven additional
multi-array, TP, seed and feather simanalyze figures with actual CASA
Matplotlib artists: image pixels, UV/ephemeris curves, labels, image/display
ranges, titles, axes, pointing circles and statistical annotations. Native plot arrays share one binary payload across
child tasks. Complete file/screen/both/none policies and live memory at two
image sizes and spectral channel counts are tested. Plotting uses the existing native document typography.
