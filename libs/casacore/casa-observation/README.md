# casa-observation

Specify bounded visibility access shared by MeasurementSet storage formats.

## Main types

- `AccessRequest` — A named data group, axis selection, and payload requirements.
- `VisibilityBatch` — An admitted collection of visibility samples and metadata.
- `VisibilityCursor` — Bound traversal of an observation.
- `Roles` — The selected payload and auxiliary fields a traversal reads.

Estimate payload memory for one sample and two channels.

```rust
use casa_observation::Roles;

assert_eq!(Roles::PAYLOAD.sample_bytes(2, 1, true, 0), 50);
```
