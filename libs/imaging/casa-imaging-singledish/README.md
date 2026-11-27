# casa-imaging-singledish

Grid total-power spectra into CASA-style single-dish images.

## Main types

- `TotalPowerGrid` — An accumulator for spectral samples on an image grid.
- `TotalPowerImage` — The gridded image and corresponding weight product.
- `GridFunction` — The spatial kernel used to accumulate spectra.
- `SpectrumBlock` — A batch of input spectra and sampling metadata.

Mask weights below half the median positive weight.

```rust
use casa_imaging_singledish::weight_mask_threshold;

assert_eq!(weight_mask_threshold(&[0.0, 2.0, 4.0], 0.5), Some(1.5));
```
