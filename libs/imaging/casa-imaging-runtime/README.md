# casa-imaging-runtime

Admit resources and execute major-cycle passes for native imaging.

## Main types

- `CubeState` — Execution storage for image-cube members.
- `ReturningAllocator` — Reusable allocation storage under an explicit budget.
- `pass::Cancel` — A shared request to stop an imaging pass.
- `PlaneCycleRow` — Per-plane details from cycle execution.

Signal cancellation from the owner of a running imaging operation.

```rust
use casa_imaging_runtime::pass::Cancel;

let cancel = Cancel::new();
cancel.cancel();
assert!(cancel.is_cancelled());
```
