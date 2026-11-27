# casa-imaging-model

Compile imaging requests into backend-independent problems.

## Main types

- `CompiledProblem` — The resolved logical imaging problem.
- `ProductRequirements` — The image products requested from execution.
- `Projection` — The science-level sky projection.
- `FixedRestoringBeam` — A user-selected restoring beam specification.

Choose a convenient FFT-friendly width for an image plane.

```rust
use casa_imaging_model::optimum_image_size;

assert!(optimum_image_size(513) >= 513);
```
