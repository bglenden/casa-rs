// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validated row-axis writes through the MeasurementSet mutation session.

use std::borrow::Cow;
use std::collections::HashMap;

use casa_tables::{RequiredScalarColumnValues, Table, TableWriteLock};
use casa_types::{ArrayValue, RecordValue, ScalarValue};
use ndarray::{ArrayD, ArrayViewD, Axis, IxDyn};

use super::write_record::{Column, WriteColumn, write_columns};
use super::{BaselineGrid, VisibilityPolarization, input, rows_by_time};
use crate::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWriteError, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, TARGET_BATCH_BYTES,
};
use crate::{MeasurementSet, MsError, MsResult};

/// The baseline-axis layout established by `getdata` for a visibility write.
#[derive(Clone, Copy, Default)]
pub struct VisibilityWriteOptions<'a> {
    /// The number of gap slots between adjacent antenna groups.
    pub baseline_gap: Option<i32>,
    /// The declared baselines when a positioned chunk represents a subset.
    pub baseline_pairs: Option<&'a [(i32, i32)]>,
    /// The recorded correlations selected by `selectpolarization`.
    pub polarization: Option<&'a VisibilityPolarization>,
    /// A positioned cursor supplies its own row ordering.
    pub iterator: bool,
}

struct WriteGrid {
    shape: [usize; 2],
    positions: Vec<[usize; 2]>,
}

#[derive(Clone, Copy)]
struct RecordedLayout<'a> {
    shape: [usize; 2],
    projection: Option<&'a [usize]>,
}

impl RecordedLayout<'_> {
    fn projected_shape(&self) -> [usize; 2] {
        let [correlations, channels] = self.shape;
        [
            self.projection.map_or(correlations, <[usize]>::len),
            channels,
        ]
    }
}

impl WriteGrid {
    fn new(
        ms: &MeasurementSet,
        rows: &[usize],
        options: VisibilityWriteOptions<'_>,
    ) -> MsResult<Self> {
        let gap = options.baseline_gap.ok_or_else(|| {
            input("call getdata with ifraxis=True before writing a baseline-axis record")
        })?;
        let columns = ms
            .main_table()
            .required_scalar_columns_owned_for_rows(&["TIME", "ANTENNA1", "ANTENNA2"], rows)?;
        let grid = BaselineGrid::new(&columns, options.baseline_pairs, gap)?;
        Ok(Self {
            shape: [grid.baseline_count(), grid.time_count()],
            positions: grid.write_positions()?,
        })
    }
}

/// Persist rows from a visibility record on a row or baseline/time axis.
/// The caller owns the writable MAIN lock for the selected physical rows.
///
/// # Errors
/// A recognized item's dtype, rank, row axis or cell shape does not match MAIN,
/// or the existing storage manager cannot persist the requested cells.
pub fn put_tool_visibility_rows(
    ms: &mut MeasurementSet,
    lock: &mut TableWriteLock,
    rows: &[usize],
    items: RecordValue,
    options: VisibilityWriteOptions<'_>,
) -> MsResult<bool> {
    if rows.is_empty() {
        return Ok(false);
    }
    let columns = write_columns(ms.main_table(), items)?;
    if columns.is_empty() {
        return Ok(false);
    }
    let ordered_rows = if !options.iterator && options.polarization.is_some() {
        rows_by_time(ms.main_table(), rows)?
    } else {
        Cow::Borrowed(rows)
    };
    let rows = ordered_rows.as_ref();
    let layouts = if columns.iter().any(|field| field.column.cell_rank() > 0) {
        recorded_cell_layouts(ms, rows, options.polarization)?
    } else {
        Vec::new()
    };
    let grid = columns
        .iter()
        .any(|field| field.values.ndim() == field.column.cell_rank() + 2)
        .then(|| WriteGrid::new(ms, rows, options))
        .transpose()?;
    let plans = columns
        .iter()
        .map(|field| row_column_plan(field, rows.len(), &layouts, grid.as_ref()))
        .collect::<MsResult<Vec<_>>>()?;
    let bytes_per_row = plans.iter().try_fold(0usize, |total, field| {
        total
            .checked_add(field.bytes_per_row)
            .ok_or_else(|| input("putdata row size overflow"))
    })?;
    let mut resources = MeasurementSetWriteResources::from_system_memory(1).map_err(write_error)?;
    resources.available_bytes = resources
        .available_bytes
        .min(TARGET_BATCH_BYTES.max(bytes_per_row));
    let plan = MeasurementSetWritePlan::selected_row_mutation(rows.to_vec(), plans, resources)
        .map_err(write_error)?;
    let batch_rows = plan.batch_rows();
    MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        plan,
        Some(lock),
        write_error,
        |writer, ms| {
            for (batch, rows) in rows.chunks(batch_rows).enumerate() {
                let first = batch * batch_rows;
                let values = columns
                    .iter()
                    .map(|field| {
                        row_column_batch(
                            ms.main_table(),
                            field,
                            rows,
                            first,
                            &layouts,
                            grid.as_ref(),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                writer
                    .write_mutation_batch(
                        ms,
                        MeasurementSetMutationBatch {
                            row_indices: rows.to_vec(),
                            columns: values,
                        },
                    )
                    .map_err(write_error)?;
            }
            Ok(())
        },
    )?;
    Ok(true)
}

fn write_error(error: MeasurementSetWriteError) -> MsError {
    input(error.to_string())
}

fn recorded_cell_layouts<'a>(
    ms: &MeasurementSet,
    rows: &[usize],
    requested: Option<&'a VisibilityPolarization>,
) -> MsResult<Vec<RecordedLayout<'a>>> {
    let names = ["DATA_DESC_ID"];
    let ids = ms
        .main_table()
        .required_scalar_columns_owned_for_rows(&names, rows)?;
    let Some(RequiredScalarColumnValues::Int32(ids)) = ids.get(names[0]) else {
        return Err(input("DATA_DESC_ID must contain an integer"));
    };
    let descriptions = ms.data_description()?;
    let windows = ms.spectral_window()?;
    let polarizations = ms.polarization()?;
    let mut layouts = HashMap::new();
    ids.iter()
        .map(|&id| {
            if let Some(&layout) = layouts.get(&id) {
                return Ok(layout);
            }
            let description = nonnegative(id, "DATA_DESC_ID")?;
            let window = nonnegative(
                descriptions.spectral_window_id(description)?,
                "SPECTRAL_WINDOW_ID",
            )?;
            let polarization = nonnegative(
                descriptions.polarization_id(description)?,
                "POLARIZATION_ID",
            )?;
            let shape = [
                nonnegative(polarizations.num_corr(polarization)?, "NUM_CORR")?,
                nonnegative(windows.num_chan(window)?, "NUM_CHAN")?,
            ];
            let projection = requested
                .map(|selected| selected.write_indices(id))
                .transpose()?;
            if projection.is_some_and(|indices| {
                indices.is_empty() || indices.iter().any(|&index| index >= shape[0])
            }) {
                return Err(input(
                    "selected polarization is outside the recorded correlations",
                ));
            }
            let layout = RecordedLayout { shape, projection };
            layouts.insert(id, layout);
            Ok(layout)
        })
        .collect()
}

fn nonnegative(value: i32, name: &str) -> MsResult<usize> {
    usize::try_from(value).map_err(|_| input(format!("negative {name}")))
}

fn row_column_plan(
    field: &WriteColumn,
    row_count: usize,
    layouts: &[RecordedLayout<'_>],
    grid: Option<&WriteGrid>,
) -> MsResult<MeasurementSetWriteColumnPlan> {
    let column = field.column;
    let name = column.name();
    let rank = column.cell_rank();
    let shape = field.values.shape();
    let axes = shape.get(rank..).unwrap_or_default();
    if axes != [row_count] && !grid.is_some_and(|grid| axes == grid.shape) {
        return Err(input(format!(
            "putdata {name} shape {shape:?} requires {row_count} selected rows"
        )));
    }
    if layouts
        .iter()
        .any(|layout| layout.projected_shape()[..rank] != shape[..rank])
    {
        return Err(input(format!(
            "putdata {name} shape {shape:?} differs from the recorded cell shape"
        )));
    }
    let max_correlations = layouts.iter().map(|layout| layout.shape[0]).max();
    let mut physical_shape = [1, 1];
    physical_shape[..rank].copy_from_slice(&shape[..rank]);
    if rank > 0
        && let Some(correlations) = max_correlations
    {
        physical_shape[0] = correlations;
    }
    let bytes_per_row =
        physical_shape[..rank]
            .iter()
            .try_fold(column.cell_bytes(), |bytes, size| {
                bytes
                    .checked_mul(*size)
                    .ok_or_else(|| input(format!("putdata {name} cell size overflow")))
            })?;
    Ok(MeasurementSetWriteColumnPlan {
        name: name.into(),
        bytes_per_row,
        mode: MeasurementSetColumnWriteMode::Replace,
        storage_manager: MeasurementSetColumnStorage::Persisted,
        tile_shape: None,
        create_source_column: None,
    })
}

fn row_column_batch(
    table: &Table,
    field: &WriteColumn,
    rows: &[usize],
    first: usize,
    layouts: &[RecordedLayout<'_>],
    grid: Option<&WriteGrid>,
) -> MsResult<MeasurementSetMutationColumnBatch> {
    let name = field.column.name();
    let rank = field.column.cell_rank();
    let end = first + rows.len();
    macro_rules! cells {
        ($values:ident, $variant:ident) => {{
            let rows = (first..end)
                .map(|row| ArrayValue::$variant(input_cell($values, rank, row, grid).to_owned()));
            MeasurementSetMutationColumnValues::Arrays(rows.collect())
        }};
    }
    let mut values = match &field.values {
        ArrayValue::Bool(values) if field.column == Column::FlagRow => {
            MeasurementSetMutationColumnValues::Scalars(
                (first..end)
                    .map(|row| ScalarValue::Bool(input_cell(values, rank, row, grid)[IxDyn(&[])]))
                    .collect(),
            )
        }
        ArrayValue::Bool(values) => cells!(values, Bool),
        ArrayValue::Float32(values) => cells!(values, Float32),
        ArrayValue::Complex32(values) => cells!(values, Complex32),
        _ => unreachable!("putdata arrays have been normalized to storage precision"),
    };
    if let MeasurementSetMutationColumnValues::Arrays(new) = &mut values {
        patch_recorded_correlations(table, name, rows, &layouts[first..end], new)?;
    }
    Ok(MeasurementSetMutationColumnBatch {
        name: name.into(),
        values,
    })
}

fn patch_recorded_correlations(
    table: &Table,
    name: &str,
    rows: &[usize],
    layouts: &[RecordedLayout<'_>],
    values: &mut [ArrayValue],
) -> MsResult<()> {
    if layouts.iter().all(|layout| layout.projection.is_none()) {
        return Ok(());
    }
    let old = table
        .column_accessor(name)?
        .array_cells_owned_uncached(rows)?;
    for ((mut original, layout), value) in old.into_iter().zip(layouts).zip(values) {
        let Some(indices) = layout.projection else {
            continue;
        };
        let previous = original.as_mut().ok_or_else(|| {
            input(format!(
                "putdata cannot update selected correlations of undefined {name}"
            ))
        })?;
        if previous.shape().first().copied() != Some(layout.shape[0])
            || previous.shape().get(1..) != value.shape().get(1..)
        {
            return Err(input(format!(
                "putdata recorded {name} cell has an incompatible shape"
            )));
        }
        macro_rules! patch {
            ($old:ident, $new:ident) => {
                for (input, &output) in indices.iter().enumerate() {
                    $old.index_axis_mut(Axis(0), output)
                        .assign(&$new.index_axis(Axis(0), input));
                }
            };
        }
        match (previous, &*value) {
            (ArrayValue::Bool(old), ArrayValue::Bool(new)) => patch!(old, new),
            (ArrayValue::Float32(old), ArrayValue::Float32(new)) => patch!(old, new),
            (ArrayValue::Complex32(old), ArrayValue::Complex32(new)) => patch!(old, new),
            _ => {
                return Err(input(format!(
                    "putdata recorded {name} cell has an incompatible type"
                )));
            }
        }
        *value = original.expect("original cell was available");
    }
    Ok(())
}

fn input_cell<'a, T>(
    values: &'a ArrayD<T>,
    rank: usize,
    row: usize,
    grid: Option<&WriteGrid>,
) -> ArrayViewD<'a, T> {
    if values.ndim() == rank + 1 {
        values.index_axis(Axis(rank), row)
    } else {
        let [baseline, time] = grid.expect("validated baseline-axis record").positions[row];
        values
            .index_axis(Axis(rank + 1), time)
            .index_axis_move(Axis(rank), baseline)
    }
}
