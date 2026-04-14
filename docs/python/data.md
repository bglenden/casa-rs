# Data API

`casars.data` exposes persistent file-backed objects only.

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
