// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the casacore ASCII table format and write the CASA table tool's text dump.
//!
//! The input accepts native flat arrays (for example `D2,3`). The output uses
//! brackets (`D[2,3]`), as CASA's `toasciifmt` does. They are distinct formats.

mod read;
mod write;

pub use read::{AsciiInput, read_ascii};
pub use write::write_ascii;

/// An error converting a casacore ASCII table.
#[derive(Debug, thiserror::Error)]
pub enum AsciiError {
    /// A header or argument does not conform to the ASCII table format.
    #[error("{0}")]
    Format(String),
    /// An input could not be read or an output could not be written.
    #[error("{path}: {source}")]
    Io {
        /// File being read or written.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A table schema or cell could not be represented.
    #[error(transparent)]
    Table(#[from] crate::TableError),
    /// The source declares an invalid table schema.
    #[error(transparent)]
    Schema(#[from] crate::SchemaError),
}
