# casa-atmosphere

Predict absorption, atmospheric brightness, and path delay for radio observations.

## Main types

- `AtmosphereProfile` — Layered atmosphere with pressure, temperature, and humidity.
- `SpectralGrid` — Spectral windows and channel frequencies in Hz.
- `AbsorptionProfile` — Absorption coefficients and path terms by layer and channel.
- `AtmosphereSession` — Reusable profile and spectral grid state.

Create a spectral grid with one 100 GHz channel.

```rust
use casa_atmosphere::SpectralGrid;

let _grid = SpectralGrid::single(100e9).unwrap();
```
