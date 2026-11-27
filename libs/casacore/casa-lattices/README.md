# casa-lattices

Traverse and compute over typed multidimensional casacore lattices.

## Main types

- `Lattice` — Read access to a typed multidimensional pixel array.
- `ArrayLattice` — An in-memory lattice backed by an ndarray.
- `LatticeIterExt` — Chunked iteration with caller-selected traversal.
- `LatticeStatistics` — Statistics over pixels and lattice masks.

Wrap a small one-dimensional array as a lattice.

```rust
use casa_lattices::ArrayLattice;
use ndarray::arr1;

let _lattice = ArrayLattice::new(arr1(&[1.0_f32, 2.0, 3.0]).into_dyn());
```
