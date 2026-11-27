# casa-imaging-metal

Run radio-interferometric gridding and prediction on Apple Metal.

## Main types

- `MetalBackend` — The operator backend used when a compatible device is available.
- `MetalNoiseRow` — A row of inputs for device-side cell-noise processing.
- `MetalNoiseTerm` — A noise contribution evaluated on the device.

Budget staging-ring bytes for one polarization product.

```rust
use casa_imaging_metal::ring_bytes;

assert!(ring_bytes(1) > 0);
```
