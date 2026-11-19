# Data API

Use `casars.tools` to work with image and table files in process.
`casars.data` provides data helpers and publishes the native object protocol.

- `protocol_info()` returns the stable object protocol descriptor.
- `schema_bundle()` returns the canonical schema bundle for the
  `casars.tools` classes.

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

Use `ia.open("example.im")` to reopen the product. Use
`outfile=""` with `fromshape` or `fromarray` for a temporary image. Use `ia.setmiscinfo(record)` to replace an image’s miscellaneous metadata, and
`ia.sethistory(origin, messages)` to append provenance. For an image with stored
restoring beams, `ia.restoringbeam()` reads the beam quantities and
`ia.restoringbeam(mbret="matrix")` reads a per-plane beam grid.

For a native image, `ia.haslock()` reports its read and write locks in that
order. Use `ia.lock(writelock=True, nattempts=1)` to try for a write lock
without waiting. `ia.unlock()` flushes pending changes and releases the
lock. `putchunk` normally releases the lock after writing. Pass
`locking=False` to keep it while making several updates.

Use `ia.open("example.fits")` to read pixels and their validity mask directly
from a FITS file. Metadata edits on this attachment last until `ia.done()`.
Use `ia.fromfits("example.im", "example.fits")` to import a FITS image as a
writable native CASA image. `whichhdu` selects the image HDU and `whichrep` selects the
coordinate representation. `zeroblanks=True` writes zero at blank pixels
while preserving their invalid mask values. `ia.newimagefromfits(...)` returns
a separate image tool.

`ia.torecord()` captures the pixels, coordinates and image info, including
restoring beams. `ia.fromrecord(record)` restores a temporary image. Pass an
output path to write a native CASA image. As in CASA, restoring a record uses
single-precision pixels and starts a new history.

Use `ia.fromimage(outfile="copy.im", infile="example.im")` to copy an image
with its coordinates, active pixel mask, metadata and history. Use
`newimagefromimage` to keep the current attachment and receive a separate tool.
An absolute CASA `LCSlicer` region selects a pixel box and its stride.
Use `ia.getregion(region=...)` to read that same selection. Pass
`getmask=True` to read validity, `mask` to apply a lattice expression,
or `axes` to average the valid pixels along selected axes.

`ia.statistics()` returns statistics over a real native CASA or
temporary image. Pass `axes` for per-plane results, `region` or `mask` to
select pixels, and `robust=True` for the median, quartiles, and median absolute
deviation. The tool supports classical, biweight, Chauvenet, fit-to-half and
hinges-and-fences statistics. FITS statistics, disk storage, logfile output,
and flux for per-plane beams are tracked in
[#1627](https://github.com/bglenden/casa-rs/issues/1627).

The remaining CASA image and coordinate operations are tracked in
[#1027](https://github.com/bglenden/casa-rs/issues/1027).

### Migrate an existing image reader or writer

The former `casars.data.Image` and `casars.Image` exports have been retired.
Create an `image()` tool, call `open(path)`, and call `done()` when finished.

| Previous operation | CASA image tool |
| --- | --- |
| `shape`, `pixel_type`, `units` | `shape()`, `pixeltype()`, `brightnessunit()` |
| `coordinate_system`, `misc_info` | `coordsys().torecord()`, `miscinfo()` |
| `image_info` | `torecord()["imageinfo"]`, or `restoringbeam()` for beam quantities |
| `mask_names`, `default_mask_name` | `maskhandler("get")`, `maskhandler("default")` |
| `get_slice(start, shape, stride=...)` | `getchunk(blc=start, trc=last, inc=stride)` |
| `get_mask_slice(...)` | `getchunk(..., getmask=True)` |
| `put_slice(pixels, start)` | `putchunk(pixels, blc=start)` |

`blc` and `trc` are inclusive zero-based pixel coordinates.
For each axis, calculate `last = start + (shape - 1) * stride`.
To read a plane, give that axis the same start and end, and squeeze that
axis from the resulting array. Use `casars.imexplore.data` for a plane with
its native NumPy dtype, mask and WCS.

Like CASA, `getchunk` returns real pixels as `float64` and complex pixels as
`complex128`. Use the storage type from `pixeltype()` if your caller needs
the original width. An unmasked chunk returns an all-true mask. Use
`maskhandler("default")` if your caller distinguishes a missing default mask.
CASA returns `[""]` for a missing default mask and `["T"]` when listing an
unmasked image.

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
