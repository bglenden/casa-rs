// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column-oriented full products avoid constructing and cloning MAIN records.

use std::collections::HashMap;
use std::path::Path;

use casa_core::{ArrayValue, Value};
use casa_tables::{ColumnOverrides, ColumnType, DataManagerKind, Table, TableOptions};

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
    if copy_and_scale(input, source, stage, rows, estimates)? {
        return Ok(true);
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
                        || row.gains.to_array(),
                        |estimate| row.gains.mapv(|gain| gain * estimate.correction_factor),
                    );
                    Some(Value::Array(ArrayValue::Complex32(gains)))
                })
                .collect()
        } else if name == "FLAG" {
            rows.iter()
                .map(|row| Some(Value::Array(ArrayValue::Bool(row.flags.to_array()))))
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

/// Preserve unmodified MAIN payloads in their original storage layout.
/// The existing selected-row writer appends replacement CPARAM records; no
/// unchanged array columns need to be decoded or reconstructed.
fn copy_and_scale(
    input: &Table,
    source: &Path,
    stage: &Path,
    rows: &[GainSampleRow],
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
) -> Result<bool, FluxScaleError> {
    let failed =
        |error: casa_tables::TableError| crate::output_merge::invalid(stage, error.to_string());
    if !input
        .data_manager_info()
        .iter()
        .all(|dm| dm.dm_type == "StandardStMan")
        || input.single_rewrite_options().map_err(failed)?.is_none()
    {
        return Ok(false);
    }
    for entry in std::fs::read_dir(source)
        .map_err(|e| crate::output_merge::invalid(source, e.to_string()))?
    {
        let entry = entry.map_err(|e| crate::output_merge::invalid(source, e.to_string()))?;
        if entry.file_name() == "table.lock" {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(|e| crate::output_merge::invalid(source, e.to_string()))?;
        if kind.is_symlink() {
            return Err(crate::output_merge::invalid(
                source,
                "cannot copy a symbolic-link table file",
            )
            .into());
        }
        if kind.is_file() {
            let destination = stage.join(entry.file_name());
            let mut source = std::fs::File::open(entry.path())
                .map_err(|e| crate::output_merge::invalid(source, e.to_string()))?;
            // Create owned, writable output files even when the source is
            // read-only. io::copy retains the kernel file-copy fast path.
            let mut destination = std::fs::File::create(destination)
                .map_err(|e| crate::output_merge::invalid(stage, e.to_string()))?;
            std::io::copy(&mut source, &mut destination)
                .map_err(|e| crate::output_merge::invalid(stage, e.to_string()))?;
        }
    }
    let mut output = Table::open(TableOptions::new(stage)).map_err(failed)?;
    // CASA may publish appended rows only in the source's sync data. That
    // lock file is not copied; rebuild columns if table.dat trails it.
    if output.row_count() != rows.len() {
        return Ok(false);
    }
    let mut changed = Vec::new();
    output.reserve_array_cell_updates("CPARAM", rows.len());
    {
        let mut writer = output
            .row_accessor_mut()
            .prepare(&["CPARAM"])
            .map_err(failed)?;
        for row in rows {
            if let Some(estimate) = estimates.get(&(row.field_id, row.spw_id)) {
                writer.seek(row.row_index).map_err(failed)?;
                writer
                    .set_value_at(
                        0,
                        Value::Array(ArrayValue::Complex32(
                            row.gains.mapv(|gain| gain * estimate.correction_factor),
                        )),
                    )
                    .map_err(failed)?;
                changed.push(row.row_index);
            }
        }
        writer.flush().map_err(failed)?;
    }
    if !changed.is_empty() {
        output
            .prepare_write()
            .save_selected_rows(&["CPARAM"], &changed)
            .map_err(failed)?;
    }
    for name in crate::constants::STANDARD_SUBTABLE_KEYWORDS {
        output.keywords_mut().upsert(
            *name,
            Value::table_ref(casa_tables::strip_directory(&stage.join(name), stage)),
        );
    }
    output
        .save_metadata_only(TableOptions::new(stage))
        .map_err(failed)?;
    Ok(true)
}
