# casa-coordinates

Map astronomical image pixels to physical world coordinates.

## Main types

- `CoordinateSystem` — An ordered system of direction, spectral, and ancillary axes.
- `DirectionCoordinate` — A celestial projection with reference direction and pixel.
- `SpectralCoordinate` — A frequency coordinate with frame and rest frequency.
- `Projection` — A spherical map projection such as SIN or TAN.

Define a J2000 SIN direction coordinate with angular increments in radians.

```rust
use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType};
use casa_core::measures::direction::DirectionRef;

let _direction = DirectionCoordinate::new(
    DirectionRef::J2000, Projection::new(ProjectionType::SIN),
    [0.0, 0.5], [-1e-4, 1e-4], [128.0, 128.0],
);
```
