# Data API

`casars.data` exposes persistent file-backed objects only.

The module also publishes its canonical object-surface contract directly from
Rust:

- `protocol_info()` returns the stable object protocol descriptor
- `schema_bundle()` returns the canonical schema bundle for `Image`, `Table`
  and the `casars.tools` classes

## Images

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

## Tables

Use `Table.open(path, writable=False)` to open an existing table. The v1 table surface focuses on cell/column access instead of browser-style row dictionaries:

- table metadata through `row_count`, `column_names`, `keywords`, and `column_keywords(name)`
- cell access through `get_cell` and `set_cell`
- column access through `get_column` and `put_column`
- column keyword updates through `set_column_keywords`

Returned values follow the documented type mapping:

- scalar numeric, bool, and complex values become Python scalars
- numeric scalar columns become NumPy arrays
- fixed-shape numeric array cells and columns become NumPy arrays
- variable-shape array columns fall back to Python lists of per-row NumPy arrays
- record values become nested Python dictionaries
