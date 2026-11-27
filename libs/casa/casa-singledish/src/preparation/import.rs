// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish imported single-dish MeasurementSets for the detached tool methods.

use std::path::Path;

use casa_calibration::output::CalibrationOutput;

/// Import an ATNF ASAP scan table into a CASA MeasurementSet.
///
/// A readable existing casacore table may be replaced. The input remains
/// independent of the output and a failed conversion leaves the prior
/// product available.
///
/// # Errors
/// Returns an error when either path is unsuitable, the input cannot be
/// decoded, or the complete result cannot be published.
pub fn import_asap(input: &Path, output: &Path, parallel: bool) -> Result<(), String> {
    publish_import(
        input,
        output,
        parallel,
        crate::single_dish_import::read_asap,
    )
    .inspect_err(|error| tracing::error!(%error, "ASAP scan-table import failed"))
}

/// Import a packed Nobeyama NOSTAR scan file into a CASA MeasurementSet.
///
/// The staged product includes the instrument's NRO_ARRAY mapping.
///
/// # Errors
/// Returns an error for an unreadable NOSTAR scan or if the destination
/// cannot be prepared, written or published safely.
pub fn import_nro(input: &Path, output: &Path, parallel: bool) -> Result<(), String> {
    publish_import(input, output, parallel, crate::single_dish_import::read_nro)
        .inspect_err(|error| tracing::error!(%error, "NRO scan import failed"))
}

fn publish_import(
    input: &Path,
    output: &Path,
    parallel: bool,
    decode: fn(&Path, bool) -> casa_ms::MsResult<crate::single_dish_import::ImportedScanTable>,
) -> Result<(), String> {
    if input.as_os_str().is_empty() || output.as_os_str().is_empty() {
        return Err("Both infile and outfile are required.".into());
    }
    if casa_calibration::paths::overlap(input, output).map_err(|error| error.to_string())? {
        return Err("The input scan table and output MeasurementSet overlap.".into());
    }
    if output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .is_some_and(|parent| !parent.is_dir())
    {
        return Err("The output's parent directory does not exist.".into());
    }
    let target = CalibrationOutput::prepare_import(output).map_err(|error| error.to_string())?;
    let mut result = decode(input, parallel).map_err(|error| error.to_string())?;
    for input_table in &result.input_tables {
        if casa_calibration::paths::overlap(input_table, target.destination())
            .map_err(|error| error.to_string())?
        {
            return Err(format!(
                "The output overlaps input table {}.",
                input_table.display()
            ));
        }
    }
    result
        .write_stage(&target.staging)
        .map_err(|error| error.to_string())?;
    target.commit().map_err(|error| error.to_string())
}
