# Total-power imaging with tsdimaging

`casars.tasks.tsdimaging` grids total-power spectra into a CASA image and
weight image. Its 33 parameter names, order and defaults match CASA
6.7.6.14. The simulation core currently requires explicit image geometry
and native spectral channels:

```python
from casars.tasks import tsdimaging

tsdimaging(
    infiles="observation.sd.ms",
    outfile="total_power",
    imsize=[128, 128],
    cell="3arcsec",
    phasecenter="J2000 270deg -23deg",
    gridfunction="SF",
    convsupport=6,
)
```

The products are `total_power.image` and `total_power.weight`. Existing
products require `overwrite=True`. The image carries its brightness unit,
rest frequency, coordinates, weight-median mask and raster/kernel beam.
The task returns `None`, as CASA does. It runs natively without a CASA
installation.

Intent selection excludes flagged STATE rows. Only autocorrelations are
gridded; `antenna` can select an antenna by ID or name, with an optional
`&&&` suffix. Comma-separated spectral-window IDs/names select compatible
windows. The task prefers corrected, then float, then observed data, with
stored natural weights and the CASA two-hand combination. Four correlation
products are refused. All five kernels are available: BOX, SF, GAUSS,
GJINC and the telescope PB.

Default channels come from the selected intent/window. The default brightness
label uses the first declared unit in corrected, float, then observed data,
reading legacy UNIT before QuantumUnits. Explicit brightness labels are `K`
or `Jy/beam`.

POINTING directions are converted at each integration's epoch and antenna
position into the image frame. Empty POINTING falls back to FIELD; no
restoring beam is invented when raster sampling is unavailable. Image and
weight grids are held whole, with bounded input batches and resource
admission.

Automatic geometry, multiple input sets, pointing interpolation, moving
sources, spectral averaging/regridding, alternate pointing columns,
clipminmax and selection forms outside this core are explicit refusals
naming #938. The theoretical beam follows the previously accepted
[kernel corrections](CASA%20(C%2B%2B)%20bugs.md); CASA's model sometimes
differs from the kernel its gridder uses.

CASA 6.7.6.14 comparisons cover ten small kernel cases and the named
pointing boundaries. Image/weight errors are at most 3.2e-7 normalized RMS.
The regenerated multi-array regression TP case agrees to 2.34e-6 (image)
and 1.77e-6 (weight), with exact masks and matching WCS. The upstream
native 1024-channel FLOAT_DATA test102 has identical pixels, weights and
masks, with matching units and WCS. Its GBT PB beam is 718.6745 arcsec
natively versus CASA's 661.8582 arcsec Airy-model estimate, the existing PB
kernel-model departure. CASA reads both native products.

The public-data reproducer is
`tools/singledish/tsdimaging_task_parity.py --data CASATESTDATA --work WORK
--native-bin TSDIMAGING --native-measures MEASURES`, run under CASA's Python.
Larger public datasets stay external; the small native regression tests
retain their CASA-generated inputs and independent reference products.
