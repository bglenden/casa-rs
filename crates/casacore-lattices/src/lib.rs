// SPDX-License-Identifier: LGPL-3.0-or-later
//! N-dimensional lattice abstractions for casacore-compatible data.
//!
//! This crate provides the `Lattice<T>` and `LatticeMut<T>` traits for
//! typed, N-dimensional array access, along with concrete implementations:
//!
//! - [`ArrayLattice<T>`] — in-memory lattice wrapping an `ArrayD<T>`.
//! - [`PagedArray<T>`] — disk-backed lattice using tiled table storage.
//! - [`TempLattice<T>`] — automatic memory/disk switching lattice.
//! - [`LatticeStatistics<T>`] — cached statistics over lattices, with explicit
//!   Rust execution-policy control via [`ExecutionPolicy`].
//!
//! # Iteration
//!
//! Navigators ([`LatticeStepper`], [`TiledLineStepper`], [`TileStepper`])
//! define low-level stepping strategies. [`TraversalSpec`] provides a
//! higher-level traversal API, while [`LatticeIter`] and [`LatticeIterMut`]
//! remain available for explicit navigator-based iteration. The
//! [`LatticeIterExt`] trait provides convenience methods
//! (`traverse`, `iter_lines`, `iter_tiles`, `iter_chunks`).
//!
//! # Regions and masks
//!
//! The [`LCRegion`] trait defines spatial subsets of a lattice. Concrete
//! implementations include [`LCBox`], [`LCEllipsoid`], and set-algebra
//! combinators ([`LCComplement`], [`LCIntersection`], [`LCUnion`],
//! [`LCDifference`]). [`SubLattice`] and [`SubLatticeMut`] provide
//! region-restricted views.
//!
//! # Relationship to C++ casacore
//!
//! The C++ casacore `Lattices` module provides `Lattice<T>`,
//! `ArrayLattice<T>`, `PagedArray<T>`, `TempLattice<T>`, lattice
//! iterators, and region/mask types. This Rust crate mirrors that
//! hierarchy using idiomatic Rust traits and generics rather than
//! C++ virtual inheritance.
//!
//! # Element types
//!
//! Only the 12 casacore-native types may be used as lattice elements,
//! enforced by the [`LatticeElement`] trait. These correspond to the
//! C++ template instantiations of `Lattice<T>`.

mod array_lattice;
pub mod array_math;
mod element;
mod error;
#[doc(hidden)]
pub mod execution;
mod iterator;
mod lattice;
mod lattice_stepper;
mod lc_box;
mod lc_ellipsoid;
mod lc_operations;
mod navigator;
mod paged_array;
mod region;
pub mod statistics;
mod sub_lattice;
mod temp_lattice;
mod tile_stepper;
mod tiled_line_stepper;
mod tiled_shape;
mod traversal;
pub(crate) mod value_bridge;

pub use array_lattice::ArrayLattice;
pub use array_math::{
    array_fractile, array_madfm, array_median, near, near_abs, near_f32, near_tol,
};
pub use element::LatticeElement;
pub use error::LatticeError;
pub use iterator::{LatticeChunk, LatticeIter, LatticeIterExt, LatticeIterMut};
pub use lattice::{Lattice, LatticeMut};
pub use lattice_stepper::LatticeStepper;
pub use lc_box::LCBox;
pub use lc_ellipsoid::LCEllipsoid;
pub use lc_operations::{LCComplement, LCDifference, LCIntersection, LCUnion};
pub use navigator::LatticeNavigator;
pub use paged_array::PagedArray;
pub use region::LCRegion;
pub use statistics::{ExecutionPolicy, LatticeStatistics, Statistic, StatsElement};
pub use sub_lattice::{SubLattice, SubLatticeMut};
pub use temp_lattice::TempLattice;
pub use tile_stepper::TileStepper;
pub use tiled_line_stepper::TiledLineStepper;
pub use tiled_shape::TiledShape;
pub use traversal::{
    TraversalCacheHint, TraversalCacheMode, TraversalCacheScope, TraversalChunk, TraversalCursor,
    TraversalCursorIter, TraversalIter, TraversalSpec, recommended_tile_cache_size,
};
