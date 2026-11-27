# casa-imaging-deconvolution

Reconstruct images from residuals and measurement-operator products.

## Main types

- `Clark` — Clark minor-cycle reconstruction.
- `ClarkState` — Residual, model, and peak-search state for Clark iterations.
- `RestoringBeam` — Validated major and minor widths and orientation in radians.
- `CleanStop` — The termination condition for reconstruction.

Construct a restoring beam whose widths are specified in radians.

```rust
use casa_imaging_deconvolution::RestoringBeam;

let beam = RestoringBeam::new(2e-5, 1e-5, 0.0).unwrap();
assert_eq!(beam.minor_fwhm_rad(), 1e-5);
```
