# Native simulation imaging and analysis

Truth class: current descriptive
Last reality check: 2026-11-15
Verification: `cargo test -p casa-parity --test api simanalyze_multi_casa::`

`casars.tasks.simanalyze` composes native Rust applications to image a
simulation project. It discovers the MS inputs and sky model, concatenates
interferometric arrays, grids total power, initializes synthesis imaging
from a supplied model, feathers, and computes the convolved sky,
difference, fidelity and image statistics.

```python
from casars.tasks import simanalyze

simanalyze(project="m51c", image=True,
           vis="$project.aca.i.ms,$project.aca.tp.sd.ms",
           modelimage="$project.sd.image", imsize=[512, 512],
           cell="0.2arcsec", analyze=True, graphics="none")
```

The [parameter reference](reference/task-parameters.md) gives CASA's full
signature and defaults. The single-array ALMA disk route is described in
[the tutorial evidence](tutorial-parity/ppdisk-alma.md). Multi-array and total-power graphics use the same native plot documents
as the single-array route. `graphics="file"`, `"screen"`, `"both"` and
`"none"` select the host's output policy. TP has an analysis figure and
no interferometric PSF. Feathered figures retain the original synthesis
PSF and CASA's original figure-name prefix.

## Input selection and calculations

`vis="default"` discovers project MSs in sorted order and prefers their
noisy counterparts. A previous `<project>.concat.ms` is excluded when the
original arrays are present. Explicit `vis` accepts comma-separated paths;
project-relative files take priority. `$project` names the project prefix,
including when the native task is given an absolute project directory.
Sky-model discovery prefers the first selected array's model and otherwise
takes sorted candidates; `skymodel` selects one explicitly.

Several interferometric MSs are concatenated with CASA's
`(dish_diameter / first_diameter)^2` weight factors. This automatic rule
requires one spectral window, a constant dish diameter within each MS, and
matching constant integration times. Other combinations must be prepared
explicitly. The [native concat core](casars-concat-user-guide.md) owns its
input-format limits. A repeated simulation run replaces its previous
concatenation instead of appending the same observations again.

Total power uses the [native single-dish application](casars-tsdimaging-user-guide.md):
SF support 6, nine cells per antenna primary beam, beam-area intensity
scaling and cubic regridding to the requested final cell/shape. The requested
cell must not exceed that SF cell. Multiple total-power MSs are refused
instead of silently discarding all but CASA's last one.

`modelimage` initializes tclean after total-power publication, so the same
invocation can create and use `<project>.sd.image`. A Jy/beam seed is copied
privately with the Jy/pixel label that CASA's simulation wrapper supplies;
its numerical seed values and the caller's original image are preserved.
Other incompatible units are refused before synthesis publication.

An explicit `featherimage`, or the new total-power image when no seed model
was supplied, is combined with the synthesis image. This follows CASA's
unweighted simanalyze feather convention. Primary-beam-weighted combination
belongs to simalma. Existing imager selection and gridding limits still
apply; heterogeneous 12 m/7 m combination uses its mosaic response.

Numerical outputs are CASA images, with masks, WCS and beams. `dryrun`
prepares without publishing scientific products. `overwrite=False` refuses
existing outputs. Concat, TP and feather publications use private staging;
input/output collisions are refused. `logfile` appends a native run report.

## Independent comparisons

`tools/simulation/simanalyze_multi_oracle.py` records CASA 6.7.6.14 products
for multi-array mosaics, standalone total power, a TP seed model and
feathering. The regression checks shapes, masks, coordinates, labels and
defined pixels at 0.001 normalized RMS. Samples excluded by the image mask
have no scientific value; their stored Fourier intermediates can differ.

The oracle restores CASA's missing legacy `gentools` factory and a feather
callable shadowed by its simutil import. Its numerical controls are the
already documented full-precision fidelity floor and the accepted SF beam
correction: CASA's own C++ `grdsf` table is passed to CASA's beam estimator.
No native output or Rust computation supplies that independent reference.
`--legacy-sf` retains the approximate CASA beam for measuring the raw
departure. On the small TP case it changes scaled intensity by about 0.11%.

`tools/simulation/simanalyze_regression.py` reproduces the upstream
`regression/sim_multi_arrays_and_TP` calculation recipe from the public
`image/m51ha.model`: 12 m, total power and 7 m observations, then two seeded
simanalyze calls. It compares regenerated CASA products and records release
timing. The 35 s physical epoch control at the regression's 2012 date
accounts for CASA's documented TAI-labelled-as-UTC simulation bug.

The integrated counting law runs concatenation, total power, seed models
and feathering at two image sizes and measures live heap against admission.

The full regenerated m51 run compares 36 native CASA images, including
PSFs and residuals, with exact masks and matching coordinate references and increments. Its maximum defined-
pixel NRMS is 9.18e-6; restored images and differences are below 3.5e-7,
and the TP image is 1.40e-6. DATA NRMS is 8.66e-7 (12 m), 2.52e-7
(7 m) and 6.34e-8 (TP), all below the required 1e-6. The B1950 model
uses its observation epoch for TP sampling and keeps its native tangent
basis during synthesis imaging.

Serial release measurements of the regression's two analysis calls (three
repeats, no compiler running) have medians of 6.31 s native and 12.85 s
CASA, about 2.03 times faster. This measures analysis of existing MSs,
including TP imaging, seed preparation, synthesis and image analysis.
