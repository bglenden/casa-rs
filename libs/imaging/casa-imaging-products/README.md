# casa-imaging-products

Form and normalize astronomical images from imaging accumulators.

## Main types

- `RestoringBeam` — The Gaussian beam used in image restoration.
- `ContinuumGenerationDemand` — Which continuum members must be generated.
- `ProductsError` — Invalid product geometry, normalization, or publication.

Use a restoring beam with its position angle measured east of north.

```rust
use casa_imaging_products::RestoringBeam;

let beam = RestoringBeam::new(2e-5, 1e-5, 0.1).unwrap();
assert!(beam.major_fwhm_rad() > beam.minor_fwhm_rad());
```
