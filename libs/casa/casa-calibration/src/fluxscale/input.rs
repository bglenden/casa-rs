// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the estimator's columns once, without constructing complete MAIN rows.

use std::path::Path;

use casa_core::ArrayValue;
use casa_tables::{RequiredScalarColumnValues, SelectedArray2DCells, Table};

use super::{FluxScaleError, GainCell, GainSampleRow};

const SCALARS: [&str; 7] = [
    "FIELD_ID",
    "SPECTRAL_WINDOW_ID",
    "ANTENNA1",
    "ANTENNA2",
    "TIME",
    "INTERVAL",
    "SCAN_NUMBER",
];

pub(super) fn read(table: &Table, root: &Path) -> Result<Vec<GainSampleRow>, FluxScaleError> {
    let failed = |error: casa_tables::TableError| FluxScaleError::UnsupportedInput {
        path: root.display().to_string(),
        reason: error.to_string(),
    };
    let invalid = |column: &str| FluxScaleError::UnsupportedInput {
        path: root.display().to_string(),
        reason: format!("missing or wrong-typed {column}"),
    };
    let indices = (0..table.row_count()).collect::<Vec<_>>();
    // The selected-row SSM reader coalesces scalar columns per bucket; the
    // all-row API otherwise decodes each column in a separate file pass.
    // Keep the established all-row route for other storage managers.
    let coalesced = SCALARS.iter().all(|column| {
        table
            .data_manager_info()
            .iter()
            .any(|dm| dm.dm_type == "StandardStMan" && dm.columns.iter().any(|name| name == column))
    });
    let columns = if coalesced {
        table.required_scalar_columns_owned_for_rows(&SCALARS, &indices)
    } else {
        table.required_scalar_columns_owned(&SCALARS)
    }
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
    if let Some((gains, flags, width)) = packed(table, &indices).map_err(failed)? {
        return Ok((0..indices.len())
            .map(|row_index| {
                let range = row_index * width..(row_index + 1) * width;
                GainSampleRow {
                    row_index,
                    field_id: fields[row_index],
                    spw_id: windows[row_index],
                    antenna_id: antennas[row_index],
                    antenna2_id: reference_antennas[row_index],
                    time_seconds: times[row_index],
                    interval_seconds: intervals[row_index],
                    scan_number: scans[row_index],
                    gains: GainCell::from_slice(&gains[range.clone()], true),
                    flags: GainCell::from_slice(&flags[range], true),
                }
            })
            .collect());
    }
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
                gains: gains.into(),
                flags: flags.into(),
            })
        })
        .collect()
}

type PackedGains = (Vec<casa_core::Complex32>, Vec<bool>, usize);

fn packed(
    table: &Table,
    indices: &[usize],
) -> Result<Option<PackedGains>, casa_tables::TableError> {
    let Some(&first) = indices.first() else {
        return Ok(None);
    };
    let gain_column = table.column_accessor("CPARAM")?;
    let mut shapes = gain_column.array_shapes_uncached(&[first])?;
    let Some(shape) = shapes.pop().flatten() else {
        return Ok(None);
    };
    let [width, 1] = shape.as_slice() else {
        return Ok(None);
    };
    if *width == 0 {
        return Ok(None);
    }
    let shape = [*width, 1];
    let Some(SelectedArray2DCells::Complex32(gains)) =
        gain_column.array_cells_fixed_2d_typed_uncached(indices, shape)?
    else {
        return Ok(None);
    };
    let Some(SelectedArray2DCells::Bool(flags)) = table
        .column_accessor("FLAG")?
        .array_cells_fixed_2d_typed_uncached(indices, shape)?
    else {
        return Ok(None);
    };
    Ok(Some((gains.into_values(), flags.into_values(), shape[0])))
}
