// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS interchange for interferometric visibilities.

mod export_calibration;
mod export_layout;
mod export_metadata;
mod groups;
mod import_columns;
mod metadata;
mod uvfits_export;
mod uvfits_import;
mod uvfits_ms;

pub use uvfits_export::{UvFitsExportOptions, export_uvfits};
pub use uvfits_import::{UvFitsImportOptions, import_uvfits};

/// Shared implementation of native FITS visibility imports.
#[doc(hidden)]
pub mod reader_support {
    pub use super::groups::{
        binary_extension, binary_extension_name, fits_correlation, open_file, optional_key,
    };
    pub use super::import_columns::{ImportColumns, MainRow, MainSamples, Spectra};
    pub use super::invalid;
    pub use super::metadata::{fixed_text_column, history};
    pub use super::uvfits_ms::{
        column_keyword, configure_references, fits_date, insert, write_polarization,
    };
    pub use crate::column_def::column_schema;
    pub use crate::ms::apply_column_metadata;
    pub use crate::subtable_rows::{
        b, complex_array, f, f32_array, f64_array, i, row_from_defs, s, string_array, subtable_mut,
    };
}

use crate::MsError;

#[doc(hidden)]
pub fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
