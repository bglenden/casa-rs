# casa-imaging-operator

Apply the radio-interferometric measurement operator on visibility batches.

## Main types

- `GridBackend` — Gridding and prediction operations across implementations.
- `CpuBackend` — The native CPU implementation of the operator.
- `GridGeometry` — The mapping between sky pixels and padded Fourier-grid cells.
- `PaddingFactor` — The CASA gridding padding factor.

Validate the requested padding factor for a Fourier grid.

```rust
use casa_imaging_operator::PaddingFactor;

assert!(PaddingFactor::new(0.5).is_err());
assert_eq!(PaddingFactor::new(1.2).unwrap().get(), 1.2);
```
