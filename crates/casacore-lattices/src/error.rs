// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for lattice operations.

/// Errors that can occur during lattice operations.
///
/// Corresponds conceptually to errors thrown by the C++ `Lattice<T>` hierarchy:
/// shape mismatches, index out of bounds, and tile configuration problems.
#[derive(Debug, thiserror::Error)]
pub enum LatticeError {
    /// An index was out of bounds for the lattice shape.
    ///
    /// C++ casacore throws `AipsError` with a message like
    /// "ArrayError: index out of range".
    #[error("index {index:?} out of bounds for lattice shape {shape:?}")]
    IndexOutOfBounds {
        /// The attempted index.
        index: Vec<usize>,
        /// The lattice shape.
        shape: Vec<usize>,
    },

    /// A slice specification is incompatible with the lattice shape.
    #[error(
        "slice start {start:?} + shape {slice_shape:?} (stride {stride:?}) \
         exceeds lattice shape {lattice_shape:?}"
    )]
    SliceOutOfBounds {
        /// Slice starting position.
        start: Vec<usize>,
        /// Requested slice shape.
        slice_shape: Vec<usize>,
        /// Slice stride.
        stride: Vec<usize>,
        /// The actual lattice shape.
        lattice_shape: Vec<usize>,
    },

    /// The number of dimensions in an operation does not match the lattice.
    #[error("dimensionality mismatch: expected {expected}, got {got}")]
    NdimMismatch {
        /// Expected number of dimensions.
        expected: usize,
        /// Actual number of dimensions.
        got: usize,
    },

    /// Shape mismatch between source and destination in a copy or put operation.
    #[error("shape mismatch: expected {expected:?}, got {got:?}")]
    ShapeMismatch {
        /// Expected shape.
        expected: Vec<usize>,
        /// Actual shape.
        got: Vec<usize>,
    },

    /// Attempt to write to a read-only lattice.
    #[error("lattice is not writable")]
    NotWritable,

    /// An explicit tile shape is incompatible with the lattice shape.
    ///
    /// C++ casacore requires tile dimensions to not exceed the lattice shape.
    #[error("tile shape {tile_shape:?} exceeds lattice shape {lattice_shape:?} on axis {axis}")]
    TileMismatch {
        /// The tile shape that was specified.
        tile_shape: Vec<usize>,
        /// The lattice shape.
        lattice_shape: Vec<usize>,
        /// The first axis where the tile exceeds the lattice.
        axis: usize,
    },

    /// A table-level error (propagated from `casacore-tables`).
    #[error("table error: {0}")]
    Table(String),
}
