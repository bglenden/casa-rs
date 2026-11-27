# casa-vla

Read VLA export archives and convert them to MeasurementSets.

## Main types

- `ImportVlaOptions` — Selection and naming policy for VLA archive import.
- `BandName` — Receiver bands and their CASA task tokens.
- `AntennaNameScheme` — Antenna naming policy for imported observations.
- `VlaError` — Invalid archives, record formats, and publication errors.

Get the CASA selection token for the VLA L band.

```rust
use casa_vla::BandName;

assert_eq!(BandName::L.as_task_token(), "L");
```
