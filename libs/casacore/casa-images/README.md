# casa-images

Read, write, and evaluate casacore-compatible astronomical images.

## Main types

- `PagedImage` — A disk-backed image with coordinates, masks, and metadata.
- `ImageInterface` — Typed access to image pixels and metadata.
- `GaussianBeam` — Restoring beam axes and position angle in radians.
- `ImageExprBuilder` — Construction of lazy image expressions.

Convert a Gaussian restoring beam to a solid angle in steradians.

```rust
use casa_images::GaussianBeam;

let beam = GaussianBeam::new(2e-5, 1e-5, 0.0);
assert!(beam.area() > 0.0);
```
