// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column-oriented full products avoid constructing and cloning MAIN records.

use std::collections::HashMap;
use std::path::Path;

use casa_tables::{ColumnOverrides, ColumnType, DataManagerKind, Table, TableOptions};
use casa_types::{ArrayValue, Value};

use super::{FluxScaleError, FluxScaleEstimate, GainSampleRow};

pub(super) fn ordered(rows: &[GainSampleRow]) -> bool {
    rows.windows(2).all(|pair| {
        let (a, b) = (&pair[0], &pair[1]);
        a.spw_id
            .cmp(&b.spw_id)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.antenna_id.cmp(&b.antenna_id))
            .then(a.antenna2_id.cmp(&b.antenna2_id))
            .is_le()
    })
}

pub(super) fn save(
    input: &Table,
    source: &Path,
    stage: &Path,
    rows: &[GainSampleRow],
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
) -> Result<bool, FluxScaleError> {
    let Some(schema) = input.schema() else {
        return Ok(false);
    };
    if schema
        .columns()
        .iter()
        .any(|column| matches!(column.column_type(), ColumnType::Record))
    {
        return Ok(false);
    }
    let failed =
        |error: casa_tables::TableError| crate::output_merge::invalid(stage, error.to_string());
    let scalar_names = schema
        .columns()
        .iter()
        .filter(|column| matches!(column.column_type(), ColumnType::Scalar))
        .map(|column| column.name())
        .collect::<Vec<_>>();
    let mut scalars = input.scalar_columns_owned(&scalar_names).map_err(failed)?;
    let indices = (0..rows.len()).collect::<Vec<_>>();
    let mut overrides = ColumnOverrides::for_row_count(rows.len());
    for column in schema.columns() {
        let name = column.name();
        let values = if matches!(column.column_type(), ColumnType::Scalar) {
            scalars
                .remove(name)
                .expect("requested scalar column")
                .into_iter()
                .map(|value| value.map(Value::Scalar))
                .collect()
        } else if name == "CPARAM" {
            rows.iter()
                .map(|row| {
                    let gains = estimates.get(&(row.field_id, row.spw_id)).map_or_else(
                        || row.gains.clone(),
                        |estimate| row.gains.mapv(|gain| gain * estimate.correction_factor),
                    );
                    Some(Value::Array(ArrayValue::Complex32(gains)))
                })
                .collect()
        } else if name == "FLAG" {
            rows.iter()
                .map(|row| Some(Value::Array(ArrayValue::Bool(row.flags.clone()))))
                .collect()
        } else {
            input
                .column_accessor(name)
                .and_then(|column| column.array_cells_owned_uncached(&indices))
                .map_err(failed)?
                .into_iter()
                .map(|value| value.map(Value::Array))
                .collect()
        };
        overrides.insert_values(name, values);
    }
    let mut output = Table::open_metadata_for_construction(TableOptions::new(source), rows.len())
        .map_err(failed)?;
    for name in crate::constants::STANDARD_SUBTABLE_KEYWORDS {
        output.keywords_mut().upsert(
            *name,
            Value::table_ref(casa_tables::strip_directory(&stage.join(name), stage)),
        );
    }
    output
        .prepare_write()
        .save_with_column_overrides(
            TableOptions::new(stage).with_data_manager(DataManagerKind::StandardStMan),
            &HashMap::new(),
            &overrides,
        )
        .map_err(failed)?;
    Ok(true)
}
