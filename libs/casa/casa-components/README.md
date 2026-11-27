# casa-components

Represent and sample analytic radio-astronomy sky models.

## Main types

- `SkyComponent` — A source shape, direction, flux, and spectral behavior.
- `ComponentList` — A collection of components for prediction or on-disk exchange.
- `ComponentShape` — A point or extended brightness distribution.
- `Flux` — Polarized component flux in the specified basis and unit.

Start an empty sky component list.

```rust
use casa_components::ComponentList;

let model = ComponentList::new();
assert!(model.is_empty());
```
