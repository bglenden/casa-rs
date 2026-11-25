// SPDX-License-Identifier: LGPL-3.0-or-later
//! In-place Hanning smoothing as exposed by the MeasurementSet tool.

use casa_tables::{Table, TableWriteLock};
use casa_types::ArrayValue;

use crate::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, maximum_visibility_cell_elements,
};
use crate::{MeasurementSet, MsError, MsResult};

use super::hanning::{flag, smooth_tool_cell, tool_weight};

/// Visibility columns requested by `ms.hanningsmooth()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HanningDataColumn {
    /// Smooth the observed DATA or FLOAT_DATA.
    Observed,
    /// Smooth CORRECTED_DATA.
    Corrected,
    /// Smooth observed and already-present corrected data.
    Both,
}

/// Smooth existing visibility, flags and weights at the specified MAIN rows.
///
/// A present weight spectrum supplies channel-dependent weights and keeps
/// scalar WEIGHT. Otherwise WEIGHT is multiplied by 8/3. SIGMA is unchanged.
/// Rows with fewer than three channels are left untouched.
///
/// # Errors
/// A required column cannot be read or written, cell shapes disagree, or the
/// supplied lock does not protect the physical MAIN table.
pub fn smooth_ms_hanning(
    ms: &mut MeasurementSet,
    rows: &[usize],
    mode: HanningDataColumn,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let table = ms.main_table();
    let has = |name| {
        table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let observed = if has("FLOAT_DATA") {
        "FLOAT_DATA"
    } else {
        "DATA"
    };
    let available = [observed, "CORRECTED_DATA"];
    let visibilities = match mode {
        HanningDataColumn::Observed => &available[..1],
        HanningDataColumn::Corrected => &available[1..],
        HanningDataColumn::Both if has("CORRECTED_DATA") => &available,
        HanningDataColumn::Both => &available[..1],
    };
    let flag_shapes = table.column_accessor("FLAG")?.array_shapes_uncached(rows)?;
    let spectral = if has("WEIGHT_SPECTRUM") {
        Some(
            table
                .column_accessor("WEIGHT_SPECTRUM")?
                .array_shapes_uncached(rows)?,
        )
    } else {
        None
    };
    let mut partitions = [Vec::new(), Vec::new()];
    for (index, (&row, shape)) in rows.iter().zip(flag_shapes).enumerate() {
        if let Some([_, nchan]) = shape.as_deref() {
            if *nchan >= 3 {
                let defined = spectral
                    .as_ref()
                    .is_some_and(|values| values[index].is_some());
                partitions[usize::from(defined)].push(row);
            }
        } else {
            return Err(invalid(
                "FLAG must have a defined [correlation, channel] shape",
            ));
        }
    }
    for (index, rows) in partitions.into_iter().enumerate() {
        if !rows.is_empty() {
            smooth_partition(ms, rows, visibilities, index != 0, lock)?;
        }
    }
    Ok(())
}

fn smooth_partition(
    ms: &mut MeasurementSet,
    rows: Vec<usize>,
    visibilities: &[&str],
    has_spectrum: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let weight = if has_spectrum {
        "WEIGHT_SPECTRUM"
    } else {
        "WEIGHT"
    };
    let elements = maximum_visibility_cell_elements(ms).map_err(invalid)?;
    let column = |name: &str, bytes: usize| -> MsResult<_> {
        Ok(MeasurementSetWriteColumnPlan {
            name: name.to_owned(),
            bytes_per_row: elements
                .checked_mul(bytes)
                .ok_or_else(|| invalid("Hanning row size overflows"))?,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        })
    };
    let columns = visibilities
        .iter()
        .map(|name| column(name, 8))
        .chain([column("FLAG", 1), column(weight, 4)])
        .collect::<MsResult<_>>()?;
    let resources = MeasurementSetWriteResources::from_system_memory(1).map_err(invalid)?;
    let plan = MeasurementSetWritePlan::selected_row_mutation(rows, columns, resources)
        .map_err(invalid)?;
    MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        plan,
        Some(lock),
        invalid,
        |session, ms| {
            loop {
                let rows = session.next_mutation_rows().map_err(invalid)?.to_vec();
                if rows.is_empty() {
                    break;
                }
                let batch =
                    smooth_batch(ms.main_table(), rows, visibilities, weight, has_spectrum)?;
                session.write_mutation_batch(ms, batch).map_err(invalid)?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

fn smooth_batch(
    table: &Table,
    rows: Vec<usize>,
    visibilities: &[&str],
    weight: &str,
    has_spectrum: bool,
) -> MsResult<MeasurementSetMutationBatch> {
    let mut flags = read_cells(table, "FLAG", &rows)?;
    let mut weights = read_cells(table, weight, &rows)?;
    let mut columns = Vec::with_capacity(visibilities.len() + 2);
    for name in visibilities {
        let mut data = read_cells(table, name, &rows)?;
        for (data, flag) in data.iter_mut().zip(&flags) {
            smooth_tool_cell(data, flag.shape())?;
        }
        columns.push(array_column(name, data));
    }
    for (flags, weights) in flags.iter_mut().zip(&mut weights) {
        if !matches!(flags, ArrayValue::Bool(_)) {
            return Err(invalid("FLAG must be Bool"));
        }
        tool_weight(weights, flags.shape(), has_spectrum)?;
        flag(flags);
    }
    columns.push(array_column("FLAG", flags));
    columns.push(array_column(weight, weights));
    Ok(MeasurementSetMutationBatch {
        row_indices: rows,
        columns,
    })
}

fn read_cells(table: &Table, name: &str, rows: &[usize]) -> MsResult<Vec<ArrayValue>> {
    table
        .column_accessor(name)?
        .array_cells_owned_uncached(rows)?
        .into_iter()
        .map(|value| value.ok_or_else(|| invalid(format!("Hanning input {name} is undefined"))))
        .collect()
}

fn array_column(name: &str, values: Vec<ArrayValue>) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values: MeasurementSetMutationColumnValues::Arrays(values),
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
