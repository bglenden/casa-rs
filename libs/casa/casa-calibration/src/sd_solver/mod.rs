// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single-dish calibration domain operations and table generation.

pub mod edges;
pub mod gain;
mod map_geometry;
pub mod position_switch;
mod spectrum;
pub(crate) mod tool;

use std::path::Path;

use casa_tables::{TableError, TableWriteLock, publication::lock_table_tree_for_replacement};

use crate::output::CalibrationOutputMode;

/// Whether a calibration table subtype is a single-dish sky spectrum table.
pub fn is_sky_subtype(subtype: &str) -> bool {
    matches!(subtype, "SDSKY_PS" | "SDSKY_RASTER" | "SDSKY_OTF")
}

/// Acquire write lock(s) needed before updating or replacing a single-dish caltable.
pub fn lock_solve_output(
    output: &Path,
    mode: CalibrationOutputMode,
) -> Result<Vec<TableWriteLock>, TableError> {
    if !output.join("table.dat").is_file() {
        return Ok(Vec::new());
    }
    if mode == CalibrationOutputMode::Replace {
        lock_table_tree_for_replacement(output)
    } else {
        TableWriteLock::acquire_to_reread(output, 1).map(|lock| vec![lock])
    }
}

pub use tool::SingleDishSolveControls;
