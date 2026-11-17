// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the estimator's columns once, without constructing complete MAIN rows.

use std::path::Path;

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::ArrayValue;

use super::{FluxScaleError, GainSampleRow};

pub(super) fn read(table: &Table, root: &Path) -> Result<Vec<GainSampleRow>, FluxScaleError> {
    let failed = |error: casa_tables::TableError| FluxScaleError::UnsupportedInput {
        path: root.display().to_string(),
        reason: error.to_string(),
    };
    let invalid = |column: &str| FluxScaleError::UnsupportedInput {
        path: root.display().to_string(),
        reason: format!("missing or wrong-typed {column}"),
    };
    let columns = table
        .required_scalar_columns_owned(&[
            "FIELD_ID",
            "SPECTRAL_WINDOW_ID",
            "ANTENNA1",
            "ANTENNA2",
            "TIME",
            "INTERVAL",
            "SCAN_NUMBER",
        ])
        .map_err(failed)?;
    let integers = |name: &str| match columns.get(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values.as_slice()),
        _ => Err(invalid(name)),
    };
    let doubles = |name: &str| match columns.get(name) {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values.as_slice()),
        _ => Err(invalid(name)),
    };
    let fields = integers("FIELD_ID")?;
    let windows = integers("SPECTRAL_WINDOW_ID")?;
    let antennas = integers("ANTENNA1")?;
    let reference_antennas = integers("ANTENNA2")?;
    let times = doubles("TIME")?;
    let intervals = doubles("INTERVAL")?;
    let scans = integers("SCAN_NUMBER")?;
    let indices = (0..table.row_count()).collect::<Vec<_>>();
    let gains = table
        .column_accessor("CPARAM")
        .and_then(|column| column.array_cells_owned_uncached(&indices))
        .map_err(failed)?;
    let flags = table
        .column_accessor("FLAG")
        .and_then(|column| column.array_cells_owned_uncached(&indices))
        .map_err(failed)?;
    gains
        .into_iter()
        .zip(flags)
        .enumerate()
        .map(|(row_index, (gains, flags))| {
            let Some(ArrayValue::Complex32(gains)) = gains else {
                return Err(invalid("CPARAM"));
            };
            let Some(ArrayValue::Bool(flags)) = flags else {
                return Err(invalid("FLAG"));
            };
            if gains.shape() != flags.shape()
                || gains.is_empty()
                || gains.ndim() > 2
                || (gains.ndim() == 2 && gains.shape()[1] != 1)
            {
                return Err(FluxScaleError::UnsupportedInput {
                    path: root.display().to_string(),
                    reason: format!(
                        "row {row_index}: fluxscale needs matching single-channel gain/flag arrays"
                    ),
                });
            }
            Ok(GainSampleRow {
                row_index,
                field_id: fields[row_index],
                spw_id: windows[row_index],
                antenna_id: antennas[row_index],
                antenna2_id: reference_antennas[row_index],
                time_seconds: times[row_index],
                interval_seconds: intervals[row_index],
                scan_number: scans[row_index],
                gains,
                flags,
            })
        })
        .collect()
}
