// SPDX-License-Identifier: LGPL-3.0-or-later
//! The spectral axes stored in prior calibration products.

use std::collections::BTreeSet;
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};
use ndarray::{ArrayD, IxDyn};

use super::GencalError;

/// SolvableVisCal::initSVC collapses all spectral vectors for uniform
/// parameters: mean channel frequency and summed widths/resolutions.
/// Keep this inside the writer's staging directory, before publication.
pub(crate) fn write_spectral_axis(
    path: &Path,
    present_windows: &BTreeSet<i32>,
    collapse: bool,
) -> Result<(), GencalError> {
    write_prior_spectral_axis(path, present_windows, collapse, false)
}

pub(super) fn write_prior_spectral_axis(
    path: &Path,
    present_windows: &BTreeSet<i32>,
    collapse: bool,
    first_channel: bool,
) -> Result<(), GencalError> {
    let mut table = Table::open(TableOptions::new(path))
        .map_err(|error| GencalError::InvalidRequest(error.to_string()))?;
    for row in 0..table.row_count() {
        let absent = !present_windows.contains(&(row as i32));
        table
            .cell_accessor_mut(row, "FLAG_ROW")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Bool(absent))))
            .map_err(|e| GencalError::InvalidRequest(e.to_string()))?;
        for column in ["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"] {
            if !collapse && column != "CHAN_WIDTH" {
                continue;
            }
            let ArrayValue::Float64(values) = table
                .cell_accessor(row, column)
                .and_then(|cell| cell.array())
                .map_err(|error| GencalError::InvalidRequest(error.to_string()))?
            else {
                return Err(GencalError::InvalidRequest(format!(
                    "{column} is not a double array"
                )));
            };
            if values.is_empty() {
                return Err(GencalError::InvalidRequest(format!(
                    "SPECTRAL_WINDOW row {row} has empty {column}"
                )));
            }
            let value = if first_channel {
                *values
                    .iter()
                    .next()
                    .expect("checked for empty channel vectors")
            } else if column == "CHAN_FREQ" {
                values.iter().sum::<f64>() / values.len() as f64
            } else {
                values.iter().sum::<f64>()
            };
            if column == "CHAN_WIDTH" {
                table
                    .cell_accessor_mut(row, "TOTAL_BANDWIDTH")
                    .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Float64(value.abs()))))
                    .map_err(|error| GencalError::InvalidRequest(error.to_string()))?;
            }
            if !collapse {
                continue;
            }
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| {
                    cell.set(Value::Array(ArrayValue::Float64(ArrayD::from_elem(
                        IxDyn(&[1]),
                        value,
                    ))))
                })
                .map_err(|error| GencalError::InvalidRequest(error.to_string()))?;
        }
        if collapse {
            table
                .cell_accessor_mut(row, "NUM_CHAN")
                .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Int32(1))))
                .map_err(|error| GencalError::InvalidRequest(error.to_string()))?;
        }
    }
    table
        .save(TableOptions::new(path))
        .map_err(|source| GencalError::Save {
            path: path.display().to_string(),
            source: Box::new(source),
        })
}

/// A synthesized observation namespace can number entities differently.
/// Update only when the old table and the staged CASA subtables give its
/// ordinal IDs the same physical meanings, including the collapsed SPWs.
pub(super) fn validate_prescribed_namespace(
    prior: &Path,
    staged: &Path,
) -> Result<(), GencalError> {
    use crate::rerefant::{names_agree, same_antenna};
    use casa_ms::observation_io::NamespaceRows;

    let invalid = GencalError::InvalidRequest;
    let compare_error = |error: casa_observation::ObservationError| invalid(error.to_string());
    let previous = NamespaceRows::read_antennas(prior).map_err(compare_error)?;
    let replacement = NamespaceRows::read_antennas(staged).map_err(compare_error)?;
    if previous.len() != replacement.len()
        || !previous
            .iter()
            .zip(&replacement)
            .all(|(old, new)| same_antenna(old, new, 0.0) && names_agree(old, new))
    {
        return Err(invalid(
            "existing calibration table has incompatible ANTENNA identity; prior product is unchanged".into(),
        ));
    }

    let previous = NamespaceRows::read_spectral_windows(prior).map_err(compare_error)?;
    let replacement = NamespaceRows::read_spectral_windows(staged).map_err(compare_error)?;
    if previous.len() != replacement.len()
        || !previous
            .iter()
            .zip(&replacement)
            .enumerate()
            .all(|(id, (old, new))| {
                let original_name = match old.name.as_str() {
                    "" | "none" => "spw",
                    name => name,
                };
                // The MSv2 converter adds the source SPW row to the XRADIO name.
                let same_name = old.name == new.name || new.name == format!("{original_name}_{id}");
                same_name
                    && old.frame == new.frame
                    && old.frequencies_hz == new.frequencies_hz
                    && old.widths_hz == new.widths_hz
            })
    {
        return Err(invalid(
            "existing calibration table has incompatible SPECTRAL_WINDOW identity; prior product is unchanged".into(),
        ));
    }
    Ok(())
}
