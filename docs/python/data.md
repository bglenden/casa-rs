# Data API

`casars.data` exposes persistent file-backed objects only.

The module also publishes its canonical object-surface contract directly from
Rust:

- `protocol_info()` returns the stable object protocol descriptor
- `schema_bundle()` returns the canonical schema bundle for `Image`
  and the `casars.tools` classes

## Images

### Create and edit a native CASA image

Use `casars.tools.image` to create a native image and edit its pixels and
coordinates through the CASA Python interface:

```python
import numpy as np
from casars.tools import image

ia = image()
ia.fromshape("example.im", [4, 3])
ia.putchunk(np.array([[1.0, 2.0], [3.0, 4.0]]), blc=[1, 0])
cs = ia.coordsys()
cs.setreferencepixel([1.5, 1.0])
ia.setcoordsys(cs.torecord())
ia.setbrightnessunit("Jy/pixel")
ia.done()
```

A coordinate tool returned by `ia.coordsys()` has its own state. Inspect the
direction axes with `cs.findcoordinate("direction")["pixel"]` or identify the
coordinate for a particular axis with `cs.findaxis(world=False, axis=0)`.
`cs.names()` and `cs.units()` follow world-axis order. Use `cs.copy()` to edit
a separate coordinate system, then pass its record to `ia.setcoordsys()` to
persist the edits to the image.

Use `cs.projection()` to inspect the direction projection and
`cs.referencecode()` to list stored frames in coordinate order.
`cs.getconversiontype("direction")` reports the current conversion frame.
Use `cs.setnames(values)` to replace the world-axis labels in order, or
pass `type` to replace one coordinate's labels. `cs.setstokes(values)`
sets the polarization labels in pixel order.

To build a coordinate system without an image, call
`tools.coordsys().newcoordsys(direction=True, spectral=True, stokes=["I", "Q"])`.
Use `addcoordinate` to append axes and `fromrecord` to restore a coordinate
record. Pass `cs.torecord()` as the `csys` argument to `ia.fromshape` or
`ia.fromarray`. The coordinate record must describe each image axis.

Use `cs.setepoch(epoch)` to store an epoch measure record. `cs.epoch()`
returns the epoch in days with its reference code and any reference offset.

Use `cs.setobserver(name)` and `cs.settelescope(name)` to update observation
metadata. An unknown telescope name is stored and the setter returns
`False`. The record retains the last known telescope position.

Use `cs.toworldmany(pixels)["numeric"]` and
`cs.topixelmany(world)["numeric"]` to convert a floating-point matrix with
one point per column. The rows follow the coordinate system's axis order.
World values use the units reported by `cs.units()`.
NumPy `float32`, `float64`, `longdouble`, `longlong`, and `ulonglong`
matrices are converted to double precision, as in CASA.

Use `ia.open("example.im")` to reopen the product. Use
`outfile=""` with `fromshape` or `fromarray` for a temporary image. Use `ia.setmiscinfo(record)` to replace an image’s miscellaneous metadata, and
`ia.sethistory(origin, messages)` to append provenance. For an image with stored
restoring beams, `ia.restoringbeam()` reads the beam quantities and
`ia.restoringbeam(mbret="matrix")` reads a per-plane beam grid.

Use `ia.open("example.fits")` to read pixels and their validity mask directly
from a FITS file. Metadata edits on this attachment last until `ia.done()`.
Use `ia.fromfits("example.im", "example.fits")` to import a FITS image as a
writable native CASA image for pixel edits and `ia.subimage(...)`. `whichhdu`
selects the image HDU and `whichrep` selects the coordinate representation.
`zeroblanks=True` writes zero at blank pixels
while preserving their invalid mask values. `ia.newimagefromfits(...)` returns
a separate image tool. The remaining
CASA image and coordinate operations are tracked in
[#1027](https://github.com/bglenden/casa-rs/issues/1027).

### Extract a subimage

Use `ia.subimage(outfile="cutout.im", region=region)` to copy a selection
to a native CASA image. The method returns a separate image tool. Leave
`outfile` empty for a temporary image, or set `wantreturn=False` to write the
file and return `None`. The copy shifts its reference pixels, keeps the
applicable restoring beams and input history, and combines the region with
the input's validity mask and any supplied `mask` expression.

Use `dropdeg=True` to drop singleton axes. List the singleton axes you want
to keep in `keepaxes`. Region inputs include CRTF text and files, saved region
references, and unit-stride boxes and polygon records.
An existing entry inside the input image cannot be replaced, protecting
the source's history and mask tables.

### Access an existing image through the Data API

Use `Image.open(path, writable=False)` to open an existing CASA-compatible image. The v1 surface is intentionally narrow:

- metadata access through `shape`, `pixel_type`, `units`, `image_info`, `misc_info`, `mask_names`, and `default_mask_name`
- pixel I/O through `get_slice`, `put_slice`, `get_plane`, and `get_mask_slice`

Read operations return NumPy arrays for numeric payloads. Writes accept NumPy-compatible inputs and materialize them into the image as owned copies.

### V1 write policy

Image writes are part of the supported v1 API, but only in a narrow form:

- supported: pixel-slice updates on existing persistent images via `put_slice`
- supported: reads from the persisted default mask via `get_mask_slice`
- not part of v1: image creation from Python
- not part of v1: coordinate-system authoring or editing from Python
- not part of v1: general metadata authoring beyond the persisted fields exposed for read access

In other words, `casars.data.Image` is a stable read/write pixel-access surface for existing images, not a full Python image-construction or image-metadata editing API.

### Stitch independently CLEANed pointings

Run `tclean` for each field with the same spectral selection, reference
frequency and restoring beam. Pass each restored `.image` and its `.pb` to
`linearmosaic`. Use the uncorrected `.image` from each clean. A separate
`tclean(niter=0)` can supply the output grid if none of the pointing images
covers the region you want.

```python
from casars.data import linearmosaic
from casars.tasks import tclean

common = dict(
    vis="two-pointings.ms", datacolumn="data", gridder="standard",
    specmode="mfs", reffreq="1.5GHz", cell="8arcsec", interactive=False,
    pblimit=0.01,
)
for field in (0, 1):
    tclean(**common, field=str(field), imagename=f"point{field}",
           imsize=288, niter=20, restoringbeam=["80arcsec", "80arcsec", "0deg"])

tclean(**common, field="0", imagename="mosaic-grid", imsize=96, niter=0,
       phasecenter="J2000 19h59m28.5 +40d50m01.5")

linearmosaic(
    [(f"point{field}.image", f"point{field}.pb") for field in (0, 1)],
    template="mosaic-grid.image",
    output="stitched.image",
    output_weight="stitched.weight",
)
```

Choose the reference frequency, angular cell size, restoring beam and
output coordinates for your observation. Only the template's grid is used.
`stitched.weight` gives the beam-squared coverage on that grid. The
flux-correct image is zero where coverage is at most `1e-6` of peak coverage.
Both outputs are CASA images, and both paths must be new.
When a projected footprint edge falls exactly halfway between output pixels,
coordinate rounding can move the edge by a pixel compared with CASA.

## Tables

Use the CASA-style `casars.tools.table` tool for in-process table access:

```python
from casars.tools import table

tb = table()
tb.open("observation.ms")
print(tb.nrows(), tb.colnames())
first_baseline = tb.getcell("ANTENNA1", 0)
tb.close()
```

`getcol` returns fixed-shape arrays with rows on the last axis.
Use `getvarcol` for variable-shape cells. To write, open with
`nomodify=False`; the write methods are `putcell`, `putcol`,
and `putcolkeywords`, among others.
