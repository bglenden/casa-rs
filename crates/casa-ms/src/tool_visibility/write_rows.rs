// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validated row-axis writes through the MeasurementSet mutation session.

use std::collections::HashMap;

use casa_tables::{
    ArrayShapeContract, ColumnType, RequiredScalarColumnValues, Table, TableWriteLock,
};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue};
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

mod legacy;
pub use legacy::LegacyVisibilityWriteGrid;

/// The baseline-axis layout established by `getdata` for a visibility write.
#[derive(Clone, Copy, Default)]
pub struct VisibilityWriteOptions<'a> {
    /// The number of gap slots between adjacent antenna groups.
    pub baseline_gap: Option<i32>,
    /// Whether the baseline record was read with a stride that omits rows.
    pub sampled_baselines: bool,
    /// The most recent visibility read used a baseline axis. A grid with
    /// one integration then omits its trailing time dimension.
    pub recent_baseline_read: bool,
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
    collapsed: bool,
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
    fn new(main: &Table, rows: &[usize], options: VisibilityWriteOptions<'_>) -> MsResult<Self> {
        if options.sampled_baselines {
            return Err(input(
                "putdata cannot write a downsampled baseline-axis record",
            ));
        }
        let gap = options.baseline_gap.ok_or_else(|| {
            input("call getdata with ifraxis=True before writing a baseline-axis record")
        })?;
        let columns =
            main.required_scalar_columns_owned_for_rows(&["TIME", "ANTENNA1", "ANTENNA2"], rows)?;
        let grid = BaselineGrid::new(&columns, options.baseline_pairs, gap)?;
        Ok(Self {
            shape: [grid.baseline_count(), grid.time_count()],
            positions: grid.write_positions(&columns)?,
            collapsed: options.recent_baseline_read && grid.time_count() == 1,
        })
    }
}

/// Normalized visibility arrays and their mapping from the public MS view.
pub struct VisibilityWriteRecord<'a> {
    rows: Vec<usize>,
    columns: Vec<WriteColumn>,
    grid: Option<WriteGrid>,
    polarization: Option<&'a VisibilityPolarization>,
}

/// Validated destination rows for one physical MAIN table.
pub struct VisibilityWritePart<'a> {
    rows: Vec<usize>,
    positions: Vec<usize>,
    names: Vec<String>,
    layouts: Vec<RecordedLayout<'a>>,
    plan: MeasurementSetWritePlan,
}

impl<'a> VisibilityWriteRecord<'a> {
    /// Whether this field can be written by `putdata`.
    pub fn accepts_column(name: &str) -> bool {
        Column::from_name(name).is_some()
    }

    /// Interpret the input record using the same ordering and grid as `getdata`.
    ///
    /// # Errors
    /// A recognized item cannot be converted or its row or baseline axis is invalid.
    pub fn new(
        main: &Table,
        mut rows: Vec<usize>,
        items: RecordValue,
        options: VisibilityWriteOptions<'a>,
    ) -> MsResult<Self> {
        let columns = write_columns(main, items)?;
        let full_grid = columns
            .iter()
            .any(|field| field.values.ndim() == field.column.cell_rank() + 2);
        if !options.iterator && options.polarization.is_some() && !columns.is_empty() {
            let ordered = rows_by_time(main, &rows)?;
            if let std::borrow::Cow::Owned(ordered) = ordered {
                rows = ordered;
            }
        }
        let grid = (full_grid || (options.recent_baseline_read && !columns.is_empty()))
            .then(|| WriteGrid::new(main, &rows, options))
            .transpose()?;
        for field in &columns {
            let shape = field.values.shape();
            let axes = shape.get(field.column.cell_rank()..).unwrap_or_default();
            let covers_selection = match axes {
                [count] => {
                    *count
                        >= grid
                            .as_ref()
                            .filter(|grid| grid.collapsed)
                            .map_or(rows.len(), |grid| grid.shape[0])
                }
                [baseline, time] => grid
                    .as_ref()
                    .is_some_and(|grid| *baseline >= grid.shape[0] && *time >= grid.shape[1]),
                _ => false,
            };
            if !covers_selection {
                return Err(input(format!(
                    "putdata {} shape {shape:?} requires {} selected rows",
                    field.column.name(),
                    rows.len()
                )));
            }
        }
        Ok(Self {
            rows,
            columns,
            grid,
            polarization: options.polarization,
        })
    }

    /// Logical rows in the order represented by the record's row axis.
    pub fn rows(&self) -> &[usize] {
        &self.rows
    }

    /// Recognized logical columns, in the normalized record order.
    pub fn column_names(&self) -> impl ExactSizeIterator<Item = &'static str> + '_ {
        self.columns.iter().map(|field| field.column.name())
    }

    /// Validate one physical destination before writing any of the record.
    /// Each pair gives a physical row and its position in the logical record.
    ///
    /// # Errors
    /// A row, column binding, dtype, physical cell shape, or write budget is invalid.
    pub fn prepare_part(
        &self,
        ms: &MeasurementSet,
        targets: Vec<(usize, usize)>,
        names: Vec<String>,
    ) -> MsResult<VisibilityWritePart<'a>> {
        if names.len() != self.columns.len()
            || targets.iter().any(|&(_, source)| source >= self.rows.len())
        {
            return Err(input("invalid visibility write projection"));
        }
        let (rows, positions): (Vec<_>, Vec<_>) = targets.into_iter().unzip();
        if rows.is_empty() || rows.iter().any(|&row| row >= ms.row_count()) {
            return Err(input("putdata row is outside the physical MeasurementSet"));
        }
        if names.iter().collect::<std::collections::HashSet<_>>().len() != names.len() {
            return Err(input(
                "putdata record resolves more than once to the same physical column",
            ));
        }
        let layouts = if self
            .columns
            .iter()
            .any(|field| field.column.cell_rank() > 0)
        {
            recorded_cell_layouts(ms, &rows, self.polarization)?
        } else {
            Vec::new()
        };
        let plans = self
            .columns
            .iter()
            .zip(&names)
            .map(|(field, name)| row_column_plan(ms.main_table(), field, name, &layouts))
            .collect::<MsResult<Vec<_>>>()?;
        let bytes_per_row = plans.iter().try_fold(0usize, |total, field| {
            total
                .checked_add(field.bytes_per_row)
                .ok_or_else(|| input("putdata row size overflow"))
        })?;
        let mut resources =
            MeasurementSetWriteResources::from_system_memory(1).map_err(write_error)?;
        resources.available_bytes = resources
            .available_bytes
            .min(TARGET_BATCH_BYTES.max(bytes_per_row));
        let plan = MeasurementSetWritePlan::selected_row_mutation(rows.clone(), plans, resources)
            .map_err(write_error)?;
        if layouts.iter().any(|layout| layout.projection.is_some()) {
            // Test every original cell needed to preserve recorded hands before
            // publishing any member or batch of the public write.
            for (field, name) in self.columns.iter().zip(&names) {
                let rank = field.column.cell_rank();
                if rank == 0 {
                    continue;
                }
                for (rows, layouts) in rows
                    .chunks(plan.batch_rows())
                    .zip(layouts.chunks(plan.batch_rows()))
                {
                    if layouts.iter().any(|layout| layout.projection.is_some()) {
                        read_projected_originals(
                            ms.main_table(),
                            name,
                            rows,
                            layouts,
                            rank,
                            field.values.primitive_type(),
                        )?;
                    }
                }
            }
        }
        Ok(VisibilityWritePart {
            rows,
            positions,
            names,
            layouts,
            plan,
        })
    }

    /// Persist a validated part while the caller holds that MAIN table's lock.
    ///
    /// # Errors
    /// The storage manager cannot read or persist one of the selected cells.
    pub fn write_part(
        &self,
        ms: &mut MeasurementSet,
        lock: &mut TableWriteLock,
        part: VisibilityWritePart<'a>,
    ) -> MsResult<()> {
        let VisibilityWritePart {
            rows,
            positions,
            names,
            layouts,
            plan,
        } = part;
        let batch_rows = plan.batch_rows();
        MeasurementSetWriteSession::mutate_selected_rows(
            ms,
            plan,
            Some(lock),
            write_error,
            |writer, ms| {
                for (batch, rows) in rows.chunks(batch_rows).enumerate() {
                    let first = batch * batch_rows;
                    let range = first..first + rows.len();
                    let values = self
                        .columns
                        .iter()
                        .zip(&names)
                        .map(|(field, name)| {
                            let recorded = if field.column.cell_rank() == 0 {
                                &[]
                            } else {
                                &layouts[range.clone()]
                            };
                            row_column_batch(
                                ms.main_table(),
                                field,
                                name,
                                rows,
                                &positions[range.clone()],
                                recorded,
                                self.grid.as_ref(),
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
        Ok(())
    }
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
    table: &Table,
    field: &WriteColumn,
    name: &str,
    layouts: &[RecordedLayout<'_>],
) -> MsResult<MeasurementSetWriteColumnPlan> {
    let column = field.column;
    let rank = column.cell_rank();
    let shape = field.values.shape();
    let descriptor = table
        .schema()
        .and_then(|schema| schema.column(name))
        .ok_or_else(|| input(format!("putdata physical column {name} is unavailable")))?;
    let valid_shape = match descriptor.column_type() {
        ColumnType::Scalar => rank == 0,
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            rank != 0 && layouts.iter().all(|layout| layout.shape[..rank] == **shape)
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
            rank != 0 && ndim.is_none_or(|ndim| ndim == rank)
        }
        _ => false,
    };
    if descriptor.data_type() != Some(field.values.primitive_type()) || !valid_shape {
        return Err(input(format!(
            "putdata physical column {name} has an incompatible type or shape"
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
    name: &str,
    rows: &[usize],
    positions: &[usize],
    layouts: &[RecordedLayout<'_>],
    grid: Option<&WriteGrid>,
) -> MsResult<MeasurementSetMutationColumnBatch> {
    let rank = field.column.cell_rank();
    macro_rules! cells {
        ($values:ident, $variant:ident) => {{
            let rows = positions
                .iter()
                .map(|&row| ArrayValue::$variant(input_cell($values, rank, row, grid).to_owned()));
            MeasurementSetMutationColumnValues::Arrays(rows.collect())
        }};
    }
    let mut values = match &field.values {
        ArrayValue::Bool(values) if field.column == Column::FlagRow => {
            MeasurementSetMutationColumnValues::Scalars(
                positions
                    .iter()
                    .map(|&row| ScalarValue::Bool(input_cell(values, rank, row, grid)[IxDyn(&[])]))
                    .collect(),
            )
        }
        ArrayValue::Bool(values) => cells!(values, Bool),
        ArrayValue::Float32(values) => cells!(values, Float32),
        ArrayValue::Complex32(values) => cells!(values, Complex32),
        _ => unreachable!("putdata arrays have been normalized to storage precision"),
    };
    if let MeasurementSetMutationColumnValues::Arrays(new) = &mut values {
        patch_recorded_correlations(table, name, rows, layouts, field, new)?;
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
    field: &WriteColumn,
    values: &mut [ArrayValue],
) -> MsResult<()> {
    if layouts.iter().all(|layout| layout.projection.is_none()) {
        return Ok(());
    }
    let old = read_projected_originals(
        table,
        name,
        rows,
        layouts,
        field.column.cell_rank(),
        field.values.primitive_type(),
    )?;
    for ((mut original, layout), value) in old.into_iter().zip(layouts).zip(values) {
        let Some(indices) = layout.projection else {
            continue;
        };
        let previous = original.as_mut().expect("projected original was validated");
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

fn read_projected_originals(
    table: &Table,
    name: &str,
    rows: &[usize],
    layouts: &[RecordedLayout<'_>],
    rank: usize,
    primitive: PrimitiveType,
) -> MsResult<Vec<Option<ArrayValue>>> {
    let originals = table
        .column_accessor(name)?
        .array_cells_owned_uncached(rows)?;
    for (original, layout) in originals.iter().zip(layouts) {
        if layout.projection.is_none() {
            continue;
        }
        let original = original.as_ref().ok_or_else(|| {
            input(format!(
                "putdata cannot update selected correlations of undefined {name}"
            ))
        })?;
        if original.shape() != &layout.shape[..rank] || original.primitive_type() != primitive {
            return Err(input(format!(
                "putdata recorded {name} cell has an incompatible type or shape"
            )));
        }
    }
    Ok(originals)
}

fn input_cell<'a, T>(
    values: &'a ArrayD<T>,
    rank: usize,
    row: usize,
    grid: Option<&WriteGrid>,
) -> ArrayViewD<'a, T> {
    if values.ndim() == rank + 1 {
        let position = grid
            .filter(|grid| grid.collapsed)
            .map_or(row, |grid| grid.positions[row][0]);
        values.index_axis(Axis(rank), position)
    } else {
        let [baseline, time] = grid.expect("validated baseline-axis record").positions[row];
        values
            .index_axis(Axis(rank + 1), time)
            .index_axis_move(Axis(rank), baseline)
    }
}
