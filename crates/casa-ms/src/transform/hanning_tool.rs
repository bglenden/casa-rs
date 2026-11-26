// SPDX-License-Identifier: LGPL-3.0-or-later
//! In-place Hanning smoothing as exposed by the MeasurementSet tool.

use casa_core::ArrayValue;
use casa_tables::{Table, TableWriteLock};

use crate::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, VisibilityWriteDestination, maximum_visibility_cell_elements,
};
use crate::{MeasurementSet, MsError, MsResult};

use super::hanning::{flag, smooth_tool_array, tool_weight};

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

/// Physical MAIN columns used by the Hanning transform.
pub struct HanningColumns<'a> {
    /// Observed and/or corrected arrays in processing order.
    pub visibilities: &'a [&'a str],
    /// Boolean flags with the same shape as each visibility array.
    pub flag: &'a str,
    /// Per-correlation weights used in the absence of a defined spectrum.
    pub weight: &'a str,
    /// Per-channel weights, when the attachment's first logical cell has a spectrum.
    pub spectrum: Option<&'a str>,
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
    columns: HanningColumns<'_>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if !ms.path().is_some_and(|path| lock.matches_table_path(path)) {
        return Err(invalid(
            "the Hanning write lock does not protect physical MAIN",
        ));
    }
    let table = ms.main_table();
    let flag_shapes = table
        .column_accessor(columns.flag)?
        .array_shapes_uncached(rows)?;
    let mut selected = Vec::with_capacity(rows.len());
    for (&row, shape) in rows.iter().zip(flag_shapes) {
        if let Some([_, nchan]) = shape.as_deref() {
            if *nchan >= 3 {
                selected.push(row);
            }
        } else {
            return Err(invalid(
                "FLAG must have a defined [correlation, channel] shape",
            ));
        }
    }
    if !selected.is_empty() {
        smooth_rows(ms, selected, &columns, lock)?;
    }
    Ok(())
}

fn smooth_rows(
    ms: &mut MeasurementSet,
    rows: Vec<usize>,
    inputs: &HanningColumns<'_>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let weight = inputs.spectrum.unwrap_or(inputs.weight);
    let has_spectrum = inputs.spectrum.is_some();
    let elements = maximum_visibility_cell_elements(ms).map_err(invalid)?;
    let destinations = inputs
        .visibilities
        .iter()
        .map(|name| VisibilityWriteDestination::new(ms.main_table(), name))
        .collect::<MsResult<Vec<_>>>()?;
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
    let columns = destinations
        .iter()
        .flat_map(|destination| destination.plans(elements, false))
        .chain([column(inputs.flag, 1)?, column(weight, 4)?])
        .collect();
    let resources = MeasurementSetWriteResources::from_system_memory(1).map_err(invalid)?;
    let plan = MeasurementSetWritePlan::selected_row_mutation(rows, columns, resources)
        .map_err(invalid)?;
    if has_spectrum
        && plan.selected_rows().len() <= plan.batch_rows()
        && smooth_packed(
            ms.main_table_mut(),
            plan.selected_rows(),
            inputs,
            weight,
            lock,
        )?
    {
        return Ok(());
    }
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
                let batch = smooth_batch(ms.main_table(), rows, inputs, &destinations)?;
                session.write_mutation_batch(ms, batch).map_err(invalid)?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

fn smooth_packed(
    table: &mut Table,
    rows: &[usize],
    inputs: &HanningColumns<'_>,
    weight: &str,
    lock: &mut TableWriteLock,
) -> MsResult<bool> {
    let (&first, &last) = rows.first().zip(rows.last()).expect("nonempty packed rows");
    if !rows.iter().copied().eq(first..=last) {
        return Ok(false);
    }
    let read = |name| table.column_accessor(name)?.packed_2d_cells_uncached(rows);
    let (Some(mut flags), Some(mut weights)) = (read(inputs.flag)?, read(weight)?) else {
        return Ok(false);
    };
    if !matches!(flags, ArrayValue::Bool(_)) {
        return Err(invalid("FLAG must be Bool"));
    }
    let mut visibilities = Vec::with_capacity(inputs.visibilities.len());
    for &name in inputs.visibilities {
        let Some(mut data) = read(name)? else {
            return Ok(false);
        };
        smooth_tool_array(&mut data, flags.shape())?;
        visibilities.push((name, data));
    }
    tool_weight(&mut weights, flags.shape(), true)?;
    flag(&mut flags);
    let columns = visibilities
        .iter()
        .map(|(name, values)| (*name, values))
        .chain([(inputs.flag, &flags), (weight, &weights)])
        .collect::<Vec<_>>();
    Ok(table
        .prepare_write()
        .try_save_packed_array_rows_holding(first..last + 1, &columns, lock)?)
}

fn smooth_batch(
    table: &Table,
    rows: Vec<usize>,
    inputs: &HanningColumns<'_>,
    destinations: &[VisibilityWriteDestination<'_>],
) -> MsResult<MeasurementSetMutationBatch> {
    let weight = inputs.spectrum.unwrap_or(inputs.weight);
    let mut flags = read_cells(table, inputs.flag, &rows)?;
    let mut weights = read_cells(table, weight, &rows)?;
    let mut columns = Vec::with_capacity(destinations.len() + 2);
    for destination in destinations {
        let mut data = read_cells(table, destination.name(), &rows)?;
        for (data, flag) in data.iter_mut().zip(&flags) {
            smooth_tool_array(data, flag.shape())?;
        }
        columns.extend(destination.batch(table, &rows, data, None)?);
    }
    for (flags, weights) in flags.iter_mut().zip(&mut weights) {
        if !matches!(flags, ArrayValue::Bool(_)) {
            return Err(invalid("FLAG must be Bool"));
        }
        tool_weight(weights, flags.shape(), inputs.spectrum.is_some())?;
        flag(flags);
    }
    columns.push(array_column(inputs.flag, flags));
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
