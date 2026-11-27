# casa-fft

Create reusable two-dimensional FFTW transforms for CASA processing.

## Main types

- `Fft2` — Complex forward and inverse transforms over contiguous planes.
- `RealFft2` — Real transforms with Hermitian half-spectrum storage.
- `FftError` — Invalid shapes, storage layouts, or FFTW planning failures.

Prepare a single-thread complex transform for a 4 by 4 plane.

```rust
use casa_fft::Fft2;

let _plan = Fft2::<f32>::with_threads([4, 4], 1).unwrap();
```
