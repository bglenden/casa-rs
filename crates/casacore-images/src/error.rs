// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for image operations.
//!
//! [`ImageError`] is the unified error type returned by all operations in this
//! crate. It wraps errors from the table, lattice, and coordinate layers and
//! adds image-specific variants for shape mismatches, missing masks, and
//! invalid metadata.

use casacore_coordinates::CoordinateError;
use casacore_lattices::LatticeError;
use casacore_tables::TableError;

/// Errors that can occur during image operations.
///
/// This enum covers table I/O failures, lattice access problems, coordinate
/// system errors, and image-specific issues such as shape mismatches and
/// missing pixel masks.
///
/// Corresponds conceptually to the `AipsError` exceptions thrown by the C++
/// casacore `Images` module.
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    /// A table-layer error occurred (opening, saving, cell access, etc.).
    #[error("table error: {0}")]
    Table(String),

    /// A lattice-layer error occurred (indexing, slicing, tiling, etc.).
    #[error("lattice error: {0}")]
    Lattice(String),

    /// A coordinate-system error occurred (axis mismatch, conversion failure, etc.).
    #[error("coordinate error: {0}")]
    Coordinate(String),

    /// The shape of data supplied does not match the expected image shape.
    #[error("shape mismatch: expected {expected:?}, got {got:?}")]
    ShapeMismatch {
        /// The expected shape.
        expected: Vec<usize>,
        /// The actual shape.
        got: Vec<usize>,
    },

    /// An operation requires a persistent (disk-backed) image, but the image
    /// is memory-only.
    #[error("operation requires a persistent image")]
    NotPersistent,

    /// Image metadata (keywords, records) could not be interpreted.
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),

    /// A named pixel mask was requested but does not exist.
    #[error("mask not found: {0}")]
    MaskNotFound(String),

    /// A general I/O error occurred.
    #[error("I/O error: {0}")]
    Io(String),

    /// A mutation was attempted on a read-only image view or expression.
    #[error("{0} is read-only")]
    ReadOnly(&'static str),
}

impl From<TableError> for ImageError {
    fn from(e: TableError) -> Self {
        Self::Table(e.to_string())
    }
}

impl From<LatticeError> for ImageError {
    fn from(e: LatticeError) -> Self {
        Self::Lattice(e.to_string())
    }
}

impl From<CoordinateError> for ImageError {
    fn from(e: CoordinateError) -> Self {
        Self::Coordinate(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_error_display() {
        let e = ImageError::Table("column not found".into());
        assert_eq!(e.to_string(), "table error: column not found");
    }

    #[test]
    fn shape_mismatch_display() {
        let e = ImageError::ShapeMismatch {
            expected: vec![10, 20],
            got: vec![10, 30],
        };
        assert!(e.to_string().contains("expected [10, 20]"));
        assert!(e.to_string().contains("got [10, 30]"));
    }

    #[test]
    fn not_persistent_display() {
        let e = ImageError::NotPersistent;
        assert_eq!(e.to_string(), "operation requires a persistent image");
    }

    #[test]
    fn from_coordinate_error() {
        let ce = CoordinateError::DimensionMismatch {
            expected: 2,
            got: 3,
        };
        let ie: ImageError = ce.into();
        assert!(matches!(ie, ImageError::Coordinate(_)));
    }

    #[test]
    fn read_only_display() {
        let e = ImageError::ReadOnly("ImageExpr");
        assert_eq!(e.to_string(), "ImageExpr is read-only");
    }
}
