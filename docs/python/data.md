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

Use `ia.addnoise(seeds=[1, 2])` to add reproducible normal noise. Pass
`type` and `pars` for another CASA noise distribution, or `zero=True`
to replace the selected pixels with noise. The region selects the lattice
section and the image's validity mask is retained.

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

Use `cs.referencevalue()` and `cs.increment()` to read the world-axis
reference values and spacing. Pass `type` to select a coordinate, or combine
`n`, `q`, `s`, and `m` in `format` to request numbers, quantities, display
strings, and CASA measures.
Use `cs.setreferencevalue(values)` and `cs.setincrement(values)` to edit
the coordinate record. Numeric values use the declared world-axis units.
Quantity strings can specify their own units, and `type` selects one
coordinate.
Use `cs.setreferencelocation(pixel, world, mask)` to set pixel and world
references together. A full-length boolean mask can limit the edit to
specific axes. Stokes references stay fixed.
Use `cs.setunits(values)` to change the declared units and rescale the
coordinate values. On a selected linear or tabular coordinate,
`overwrite=True` relabels units while keeping the numeric values.
Use `cs.setrestfrequency(values, which=0, append=False)` to choose the active
spectral rest frequency. Floating-point inputs use the coordinate's unit.
Pass quantity strings or a quantity record to supply units explicitly.
With `append=True`, `which` indexes the combined old and new list.
Use `cs.frequencytovelocity(values)` and `cs.velocitytofrequency(values)`
to convert relative to the active rest frequency. The default convention is
radio Doppler and the default velocity unit is `km/s`. Pass `doppler`,
`velunit`, or `frequnit` to choose other conventions or units.
Use `cs.frequencytofrequency(values, velocity="10km/s")` to shift a vector
by a velocity. A dimensionless `velocity` is a fraction of the speed of light.

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

Use `cs.toworld(pixel)` and `cs.topixel(world)` for a single absolute
coordinate. `toworld` supports the same numeric, quantity, string, and
measure format letters as `referencevalue`.
Use `cs.toworldmany(pixels)["numeric"]` and
`cs.topixelmany(world)["numeric"]` to convert a floating-point matrix with
one point per column. The rows follow the coordinate system's axis order.
World values use the units reported by `cs.units()`.
NumPy `float32`, `float64`, `longdouble`, `longlong`, and `ulonglong`
matrices are converted to double precision, as in CASA.

Use `cs.torel(value, isworld=1)` for world offsets from the reference and
`cs.toabs` to recover absolute positions. Use `isworld=0` for pixels, or
pass the tagged record from a coordinate conversion. The corresponding
`torelmany` and `toabsmany` methods take matrices with one coordinate per
column.

Use `ia.toworld(pixels, format="nqsm")` to convert one image position.
Each format letter requests numeric values, quantities, strings, or
measures. Pass `dovelocity=False` to omit spectral velocities from the
measure record. Pass one of these representations to `ia.topixel(world)`
to obtain the zero-based pixel coordinates in its `numeric` field.
`ia.coordmeasures(pixel)` returns world measures and the pixel's intensity
and validity. Pass `dframe` or `sframe` to request specific direction or
spectral frames.

Use `ia.open("example.im")` to reopen the product. Use
`outfile=""` with `fromshape` or `fromarray` for a temporary image. Use `ia.setmiscinfo(record)` to replace an image’s miscellaneous metadata, and
`ia.sethistory(origin, messages)` to append provenance. For an image with stored
restoring beams, `ia.restoringbeam()` reads the beam quantities and
`ia.restoringbeam(mbret="matrix")` reads a per-plane beam grid.
Use `ia.beamforconvolvedsize(source, convolved)` to calculate the Gaussian
beam needed to reach a target resolution. Use `ia.deconvolvefrombeam(source,
beam)` to recover a fitted source size and see whether the source is resolved.
Pass the major axis, minor axis and position angle as angular quantity strings.
Use `ia.rotatebeam("15deg")` to rotate all stored restoring beams. On a
directly opened FITS image, the rotation applies only to the open tool.
Each open tool keeps its own view of the restoring beams. Reopen a native
image to read beam changes made through another tool.
Use `ia.summary(list=False)` to read the shape, coordinate axes, mask names
and restoring beams and return the two report messages. The default
`list=True` sends the report to the logger.
Use `ia.makecomplex("output.im", "imaginary.im")` to combine the attached
real image with a conformant real image of matching precision. A supplied
region selects the same section from both inputs. The output copies the real
image's metadata and uses the intersection of the input masks.
Use `ia.isconform("other.im")` to check whether a float image has the same
shape and compatible coordinates as a native float or complex image.
Use `ia.convertflux(value, major, minor)` to convert a Gaussian's integrated
flux to peak brightness. Pass `topeak=False` for the reverse conversion and
select the channel and polarization for an image with per-plane beams.
Use `ia.deconvolvecomponentlist(cl.torecord())` to remove the restoring beam
from Gaussian components on an attached float or complex image. Point
components are returned intact. For a per-plane beam grid, select the
channel and polarization on each nondegenerate axis.
Use `ia.modify(cl.torecord(), subtract=False)` to add a component model to a
float image. The default subtracts the model. A region limits the pixel
section. CASA validates an explicit mask and applies the edit to the stored
pixels in that section.
Use `ia.imageconcat(infiles=["a.im", "b.im"], axis=0)` for a temporary
concatenation, or give `outfile` for a native image. The default
concatenation axis is spectral. `reorder=True` sorts inputs along the
selected coordinate. Virtual modes are tracked in #1749.
Use `ia.replacemaskedpixels(pixels=0)` to replace invalid pixels in an attached
float or complex image. Use `update=True` to validate the selected native
pixel mask after replacement.
Use `ia.set(pixels=3.5)` to fill the attached float image. `pixels` can also
be a scalar lattice expression. Use `region` to restrict the edit and set
`pixelmask=0` to invalidate the selected pixels or `pixelmask=1` to validate
them in the default mask.

Use `ia.calc(pixels=...)` to evaluate a numeric expression into a writable
float or complex image. An existing default mask limits writes to its valid
pixels. The operation preserves the destination coordinates and metadata.
Use `ia.imagecalc(outfile=..., pixels=...)` to create a native product with
the expression's validity mask. An empty `outfile` returns a read-only image
whose pixel reads evaluate the expression against its source images. Use
`prec="double"` for double precision or `imagemd` to copy metadata from a
separate image.
Use `ia.calcmask(...)` to save a Boolean expression as a named native image
mask. Expressions can combine real, complex, and FITS images and read named
masks using `mask("image.im:maskname")` where CASA accepts that source type.

Use `ia.fitsheader()` to inspect the FITS header for a float image without
exporting the pixels. Pass `exclude="HISTORY"` to omit the log from the returned
dictionary. Use `retstr=True` to return the complete string of 80-byte cards.
Use `ia.tofits("example.fits")` to write FITS from an attached float image.
It accepts region and mask selection, scaled integer pixels with `bitpix=16`,
spectral velocity or wavelength axes, and export axis ordering controls.
Pass `history=False` to omit the image log from the output.

For a native image, `ia.haslock()` reports its read and write locks in that
order. Use `ia.lock(writelock=True, nattempts=1)` to try for a write lock
without waiting. `ia.unlock()` flushes pending changes and releases the
lock. `putchunk` normally releases the lock after writing. Pass
`locking=False` to keep it while making several updates.

Use `ia.putregion(pixels, pixelmask, region)` to edit pixels and validity in
the same region. The default `usemask=True` skips masked pixels after applying
the supplied mask. Pass `replicate=True` to fill omitted axes and use the
region's stride.

Use `ia.open("example.fits")` to read pixels and their validity mask directly
from a FITS file. Metadata edits apply for the life of the attachment.
Use `ia.subimage(...)` on that attachment for a temporary or native
section with its WCS and validity mask. With `wantreturn=False`
and an output path, the tool publishes the section and returns a detached
image tool.
Use `ia.rename("new-name.fits")` to move an attached FITS file. The tool
reopens the moved file and reads its stored metadata again.

Use `ia.insert("source.im")` to place a source image by aligning reference
pixels, or supply `locate` to set the lower corner. A region record may
select a strided section. CRTF text uses the attached destination's
coordinates before applying the selection to the source. An existing writable
destination mask receives the source validity over the inserted section.

Use `ia.fromfits("example.im", "example.fits")` to import a FITS image as a
writable native CASA image for pixel edits. `whichhdu`
selects the image HDU and `whichrep` selects the coordinate representation.
`zeroblanks=True` writes zero at blank pixels
while preserving their invalid mask values. `ia.newimagefromfits(...)` returns
a separate image tool.

`ia.torecord()` captures the pixels, coordinates and image info, including
restoring beams. `ia.fromrecord(record)` restores a temporary image. Pass an
output path to write a native CASA image. As in CASA, restoring a record uses
single-precision pixels and starts a new history.

Use `ia.fromimage(outfile="copy.im", infile="example.im")` to copy an image
with its coordinates, active pixel mask, metadata and history. Use
`newimagefromimage` to keep the current attachment and receive a separate tool.
An absolute CASA `LCSlicer` region selects a pixel box and its stride. The
copy retains the selected frequencies and restoring beams. Additional region
forms are tracked in #1625.

Use `ia.getregion(region=...)` to read that same selection. Pass
`getmask=True` to read validity, `mask` to apply a lattice expression,
or `axes` to average the valid pixels along selected axes.
Use `ia.boundingbox(region=...)` to inspect the inclusive pixel corners,
stride, sampled shape and world coordinates of the corners.
Use `ia.crop(axes=...)` to trim fully masked edge planes along the selected
axes. The method applies the region and optional mask expression before
finding the crop bounds. Pass `region=[]` when using `box`, `chans`, or
`stokes` separately.
Use `ia.decimate(axis=..., factor=..., method="copy")` to keep every nth
plane. With `method="mean"`, it averages valid samples in each complete
group and drops the trailing partial group.

`ia.statistics()` returns statistics over a real native CASA image, an
attached FITS image, or a temporary image. Pass `axes` for per-plane results,
`region` or `mask` to select pixels, and `robust=True` for the median,
quartiles, and median absolute deviation. The tool supports classical,
biweight, Chauvenet, fit-to-half and hinges-and-fences statistics. Supply
`logfile` for a report with labeled planes. `append=False` replaces that
file. `list=True` adds a summary, and `verbose=True` sends the table to the
logger. Disk storage and flux for per-plane beams are tracked in
[#1627](https://github.com/bglenden/casa-rs/issues/1627).

The remaining CASA image and coordinate operations are tracked in
[#1027](https://github.com/bglenden/casa-rs/issues/1027).

Use `ia.maketestimage()` to load `demo/Images/imagetestimage.fits` as a
temporary image. Pass `outfile` to write a native image. The tool searches
the working directory, `CASA_RS_MEASURESPATH`, and the CASA installation's
`data` directory when `CASAPATH` or `AIPSPATH` identifies it.

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

Use `ia.adddegaxes(stokes="I", spectral=True)` to make a copy with new
singleton axes. Pass `outfile` to save the copy as a native image. The copy
preserves the source pixels, existing coordinates, named masks and history.
Set `silent=True` to skip requested coordinate types that already exist.
Sources with image attribute groups are tracked in
[#1625](https://github.com/bglenden/casa-rs/issues/1625).

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
