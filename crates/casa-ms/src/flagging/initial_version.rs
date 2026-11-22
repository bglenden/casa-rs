// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded initial flag snapshot for a newly staged MeasurementSet.

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
    StreamedTiledShapeValueType, Table, TableOptions,
};
use casa_types::{ArrayValue, ScalarValue};
use std::collections::HashMap;

/// Snapshot flags in physical row order for a new, private staging MS.
/// `shapes` is the same FLAG layout used when writing the source table.
#[doc(hidden)]
pub fn save_initial_flag_version(
    ms: &MeasurementSet,
    shapes: Vec<MeasurementSetArrayShapePlan>,
) -> Result<(), FlaggingError> {
    let path = ms_path(ms)?;
    let error = |reason| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason,
    };
    ensure_flagversions_dir(path)?;
    let output = flag_version_table_path(path, "Original");
    let options = TableOptions::new(&output).with_data_manager(DataManagerKind::StandardStMan);
    let schema = flag_version_schema(true).map_err(|source| error(source.to_string()))?;
    Table::with_schema(schema)
        .save(options.clone())
        .map_err(|source| error(source.to_string()))?;
    let row_count = ms.row_count();
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
        vec![MeasurementSetScalarColumnPlan {
            name: FLAG_ROW_COLUMN.into(),
            value_type: StreamedScalarType::Bool,
        }],
        resources,
    )
    .map_err(|source| error(source.to_string()))?;
    // Keep the owned row views within one row band of the largest tile.
    let batch_rows = plan.batch_rows().min(tile_rows);
    let mut writer = MeasurementSetWriteSession::start(&output, plan)
        .map_err(|source| error(source.to_string()))?;
    let source = ms.main_table();
    let flags = source
        .column_accessor(FLAG_COLUMN)
        .map_err(|source| error(source.to_string()))?;
    let row_flags = source
        .column_accessor(FLAG_ROW_COLUMN)
        .map_err(|source| error(source.to_string()))?;
    for start in (0..row_count).step_by(batch_rows) {
        let rows: Vec<_> = (start..(start + batch_rows).min(row_count)).collect();
        let arrays = flags
            .array_cells_owned_uncached(&rows)
            .map_err(|source| error(source.to_string()))?;
        let scalars = row_flags
            .scalar_cells_owned_for_rows(&rows)
            .map_err(|source| error(source.to_string()))?;
        for ((row, array), scalar) in rows.into_iter().zip(arrays).zip(scalars) {
            let Some(ArrayValue::Bool(array)) = array else {
                return Err(error(format!("missing Boolean FLAG in row {row}")));
            };
            let Some(ScalarValue::Bool(flag)) = scalar else {
                return Err(error(format!("missing Boolean FLAG_ROW in row {row}")));
            };
            let values: Vec<_> = array.t().iter().copied().collect();
            writer
                .push_bool_row(FLAG_COLUMN, array.shape(), &values)
                .and_then(|()| {
                    writer.push_scalar_row(FLAG_ROW_COLUMN, Some(ScalarValue::Bool(flag)))
                })
                .map_err(|source| error(source.to_string()))?;
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
    append_flag_version_entry(path, "Original", "Original flags at import into CASA")
}
