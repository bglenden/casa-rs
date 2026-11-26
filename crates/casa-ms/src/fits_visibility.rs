// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS interchange for interferometric visibilities.

mod export_calibration;
mod export_layout;
mod export_metadata;
mod groups;
mod metadata;
mod uvfits_export;
mod uvfits_import;
mod uvfits_ms;

pub use uvfits_export::{UvFitsExportOptions, export_uvfits};
pub use uvfits_import::{UvFitsImportOptions, import_uvfits};

use crate::MsError;

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
