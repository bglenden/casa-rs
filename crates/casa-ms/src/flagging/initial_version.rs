// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded on-disk flag snapshots.

use super::version_storage::flag_version_schema;
use super::{
    FLAG_COLUMN, FLAG_ROW_COLUMN, FlaggingError, append_flag_version_entry,
    ensure_flagversions_dir, flag_version_table_path, ms_path,
};
use crate::MeasurementSet;
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetScalarColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession,
};
use casa_tables::{
    ColumnBinding, ColumnOverrides, DataManagerKind, StreamedScalarType,
    StreamedTiledShapeValueType, Table, TableColumn, TableError, TableOptions,
};
use casa_types::{ArrayValue, ScalarValue};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// Snapshot flags in physical row order for a new, private staging MS.
/// `shapes` is the same FLAG layout used when writing the source table.
#[doc(hidden)]
pub fn save_initial_flag_version(
    ms: &MeasurementSet,
    shapes: Vec<MeasurementSetArrayShapePlan>,
) -> Result<(), FlaggingError> {
    let path = ms_path(ms)?;
    ensure_flagversions_dir(path)?;
    let output = flag_version_table_path(path, "Original");
    write_flag_snapshot(ms, &output, Some(shapes), true)?;
    append_flag_version_entry(path, "Original", "Original flags at import into CASA")
}

pub(super) fn write_flag_snapshot(
    ms: &MeasurementSet,
    output: &Path,
    shapes: Option<Vec<MeasurementSetArrayShapePlan>>,
    row_flags: bool,
) -> Result<(), FlaggingError> {
    let path = ms_path(ms)?;
    let error = |reason| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason,
    };
    let source = ms.main_table();
    let shapes = match shapes {
        Some(shapes) => shapes,
        None => snapshot_shapes(source).map_err(error)?,
    };
    let options = TableOptions::new(output).with_data_manager(DataManagerKind::StandardStMan);
    let schema = flag_version_schema(row_flags).map_err(|source| error(source.to_string()))?;
    Table::with_schema(schema)
        .save(options.clone())
        .map_err(|source| error(source.to_string()))?;
    let row_count = ms.row_count();
    if row_count == 0 {
        return Ok(());
    }
    let tile_rows = shapes
        .iter()
        .filter_map(|shape| shape.tile_shape.last().copied())
        .max()
        .unwrap_or(1);
    let resources = MeasurementSetWriteResources::from_system_memory(2)
        .map_err(|source| error(source.to_string()))?;
    let plan = MeasurementSetWritePlan::variable_array_creation(
        row_count,
        vec![MeasurementSetArrayColumnPlan {
            name: FLAG_COLUMN.into(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        }],
        row_flags
            .then(|| MeasurementSetScalarColumnPlan {
                name: FLAG_ROW_COLUMN.into(),
                value_type: StreamedScalarType::Bool,
            })
            .into_iter()
            .collect(),
        resources,
    )
    .map_err(|source| error(source.to_string()))?;
    // Keep the owned row views within one row band of the largest tile.
    let batch_rows = plan.batch_rows().min(tile_rows);
    let mut writer = MeasurementSetWriteSession::start(output, plan)
        .map_err(|source| error(source.to_string()))?;
    let flags = source
        .column_accessor(FLAG_COLUMN)
        .map_err(|source| error(source.to_string()))?;
    let row_flags = row_flags
        .then(|| source.column_accessor(FLAG_ROW_COLUMN))
        .transpose()
        .map_err(|source| error(source.to_string()))?;
    for start in (0..row_count).step_by(batch_rows) {
        let rows: Vec<_> = (start..(start + batch_rows).min(row_count)).collect();
        write_snapshot_flags(&flags, &rows, &mut writer).map_err(error)?;
        if let Some(row_flags) = &row_flags {
            let scalars = row_flags
                .scalar_cells_owned_for_rows(&rows)
                .map_err(|source| error(source.to_string()))?;
            for (row, scalar) in rows.into_iter().zip(scalars) {
                let Some(ScalarValue::Bool(flag)) = scalar else {
                    return Err(error(format!("missing Boolean FLAG_ROW in row {row}")));
                };
                writer
                    .push_scalar_row(FLAG_ROW_COLUMN, Some(ScalarValue::Bool(flag)))
                    .map_err(|source| error(source.to_string()))?;
            }
        }
    }
    let mut table = Table::open_metadata_for_construction(options.clone(), row_count)
        .map_err(|source| error(source.to_string()))?;
    let mut overrides = ColumnOverrides::for_row_count(row_count);
    overrides.insert_deferred(FLAG_COLUMN);
    let bindings = HashMap::from([(
        FLAG_COLUMN.into(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    )]);
    writer
        .save_table_and_finish(&mut table, options, &bindings, &overrides)
        .map_err(|source| error(source.to_string()))?;
    Ok(())
}

fn write_snapshot_flags(
    flags: &TableColumn<'_>,
    rows: &[usize],
    writer: &mut MeasurementSetWriteSession,
) -> Result<(), String> {
    if let Some(ArrayValue::Bool(block)) = flags
        .packed_2d_cells_uncached(rows)
        .map_err(|error| error.to_string())?
    {
        // Uniform cells are packed in physical row order without one
        // allocation per cell. The writer consumes each contiguous row.
        let shape = &block.shape()[..2];
        let values = block
            .as_slice_memory_order()
            .expect("packed column is contiguous");
        for row in values.chunks_exact(shape.iter().product()) {
            writer
                .push_bool_row(FLAG_COLUMN, shape, row)
                .map_err(|error| error.to_string())?;
        }
        return Ok(());
    }

    let arrays = flags
        .array_cells_owned_uncached(rows)
        .map_err(|error| error.to_string())?;
    for (&row, array) in rows.iter().zip(arrays) {
        let Some(ArrayValue::Bool(array)) = array else {
            return Err(format!("missing Boolean FLAG in row {row}"));
        };
        let casa_order = array.t();
        let values = match casa_order.to_slice() {
            Some(values) => Cow::Borrowed(values),
            None => Cow::Owned(casa_order.iter().copied().collect()),
        };
        writer
            .push_bool_row(FLAG_COLUMN, array.shape(), &values)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn snapshot_shapes(table: &Table) -> Result<Vec<MeasurementSetArrayShapePlan>, String> {
    let mut histogram = BTreeMap::new();
    for start in (0..table.row_count()).step_by(256) {
        let rows: Vec<_> = (start..(start + 256).min(table.row_count())).collect();
        let shapes = read_flag_shapes(table, &rows).map_err(|source| source.to_string())?;
        for (row, shape) in rows.into_iter().zip(shapes) {
            let Some([correlations, channels]) = shape.as_deref() else {
                return Err(format!("missing two-dimensional FLAG in row {row}"));
            };
            *histogram.entry((*correlations, *channels)).or_insert(0) += 1;
        }
    }
    Ok(histogram
        .into_iter()
        .map(|((correlations, channels), rows)| {
            MeasurementSetArrayShapePlan::visibility(correlations, channels, rows, "")
        })
        .collect())
}

pub(super) fn read_flag_shapes(
    table: &Table,
    rows: &[usize],
) -> Result<Vec<Option<Vec<usize>>>, TableError> {
    let column = table.column_accessor(FLAG_COLUMN)?;
    column.array_shapes_uncached(rows).or_else(|_| {
        Ok(column
            .array_cells_owned_uncached(rows)?
            .into_iter()
            .map(|cell| cell.map(|array| array.shape().to_vec()))
            .collect())
    })
}
