# casa-numerics

Fit numerical models and evaluate radio-astronomy beam patterns.

## Main types

- `LeastSquaresDesign` — A reusable QR factorization of a real design matrix.
- `PrimaryBeam` — Frequency-aware primary beam models.
- `BeamPattern` — The voltage or power response used by a primary beam.

Fit a constant to two observations with equal weight.

```rust
use casa_numerics::solve_least_squares;

let [mean] = solve_least_squares(&[([1.0], 2.0), ([1.0], 4.0)]).unwrap();
assert!((mean - 3.0).abs() < 1e-12);
```
