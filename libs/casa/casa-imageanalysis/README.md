# casa-imageanalysis

Analyze and regrid astronomical images through CASA-compatible operations.

## Main types

- `RegridTemplate` — Target coordinates and shape for an image regrid.
- `Regridded` — Regridded pixels and associated output metadata.
- `ComponentListImage` — Image pixels derived from an analytic component list.
- `Convolved` — Convolution output pixels and associated beam information.

Build a template with a specified target image shape.

```rust
use casa_coordinates::CoordinateSystem;
use casa_imageanalysis::RegridTemplate;

let template = RegridTemplate { coordinates: CoordinateSystem::new(), shape: vec![64, 64] };
assert_eq!(template.shape, [64, 64]);
```
