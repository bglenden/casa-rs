# casa-simulation

Simulate radio interferometric and total-power MeasurementSets.

## Main types

- `Simulator` — A stateful simulator for array, observation, and prediction setup.
- `ArrayConfig` — Antenna coordinates and array properties.
- `CoordinateSystem` — The reference frame for input antenna positions.
- `SimulatorOptions` — Admission and session configuration for simulation.

Declare antenna coordinates given as ITRF positions in metres.

```rust
use casa_simulation::CoordinateSystem;

let frame = CoordinateSystem::Global;
assert!(matches!(frame, CoordinateSystem::Global));
```
