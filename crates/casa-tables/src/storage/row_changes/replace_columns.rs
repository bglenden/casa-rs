// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use super::{
    ColumnDescContents, CompositeStorage, DataManagerEntry, ScalarValue, StagedEdit, StorageError,
    Value, incremental_stman, standard_stman,
};

use crate::storage::{TABLE_CONTROL_FILE, data_type::CasacoreDataType, table_control};

pub(crate) fn replace_private_int32_column(
    table: &Path,
    column: &str,
    values: &[i32],
) -> Result<bool, StorageError> {
    let table_control::TableDatResult::Plain(descriptor) =
        table_control::read_table_dat_dispatch(&table.join(TABLE_CONTROL_FILE))?
    else {
        return Ok(false);
    };
    if descriptor.nrrow != values.len() as u64 {
        return Ok(false);
    }
    let columns = descriptor
        .table_desc
        .columns
        .iter()
        .zip(&descriptor.column_set.columns);
    let Some((description, binding)) = columns.clone().find(|(desc, _)| desc.col_name == column)
    else {
        return Ok(false);
    };
    if description.is_array || description.data_type != CasacoreDataType::TpInt {
        return Ok(false);
    }
    let Some(manager) = descriptor.column_set.data_managers.iter().find(|manager| {
        manager.seq_nr == binding.dm_seq_nr && manager.type_name == "StandardStMan"
    }) else {
        return Ok(false);
    };
    let Some(index) = columns
        .filter(|(_, binding)| binding.dm_seq_nr == manager.seq_nr)
        .position(|(description, _)| description.col_name == column)
    else {
        return Ok(false);
    };
    let file = table.join(format!("table.f{}", manager.seq_nr));
    crate::publication::private_table_file_metadata(&file)?;
    standard_stman::replace_int32_columns(
        &file,
        &file,
        manager,
        &[(index, values)],
        descriptor.nrrow,
    )
}

impl StagedEdit<'_> {
    pub(super) fn stage_replaced_scalars(
        &self,
        manager: &mut DataManagerEntry,
        columns: &[ColumnDescContents],
        replacements: &BTreeMap<String, Vec<Option<Value>>>,
        rows: u64,
    ) -> Result<bool, StorageError> {
        if !matches!(
            manager.type_name.as_str(),
            "StandardStMan" | "IncrementalStMan"
        ) || columns
            .iter()
            .any(|column| column.is_array || column.is_record())
        {
            return Ok(false);
        }
        let name = format!("table.f{}", manager.seq_nr);
        let old = self.table.join(&name);
        let staged = self.staged.path().join(&name);
        if manager.type_name == "IncrementalStMan"
            && incremental_stman::stage_int32_replacement(
                &old,
                &staged,
                columns,
                replacements,
                rows,
            )?
        {
            return Ok(true);
        }
        if manager.type_name == "StandardStMan"
            && standard_stman::stage_int32_replacement(
                &old,
                &staged,
                manager,
                columns,
                replacements,
                rows,
            )?
        {
            return Ok(true);
        }
        let unchanged = columns
            .iter()
            .map(|column| column.col_name.as_str())
            .filter(|name| !replacements.contains_key(*name))
            .collect::<HashSet<_>>();
        let mut preserved = if unchanged.is_empty() {
            Default::default()
        } else {
            CompositeStorage
                .load_named_scalar_columns_with_row_hint(self.table, &unchanged, Some(rows))?
                .columns
        };
        let values = columns
            .iter()
            .map(|column| {
                let Some(replacement) = replacements.get(&column.col_name) else {
                    return preserved.remove(&column.col_name).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "missing scalar column {}",
                            column.col_name
                        ))
                    });
                };
                replacement
                    .iter()
                    .map(|cell| match cell {
                        Some(Value::Scalar(value)) => Ok(Some(value.clone())),
                        None => Ok(None),
                        Some(_) => Err(StorageError::FormatMismatch(format!(
                            "replacement for {} is not scalar",
                            column.col_name
                        ))),
                    })
                    .collect()
            })
            .collect::<Result<Vec<Vec<Option<ScalarValue>>>, StorageError>>()?;
        let columns_values = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        manager.data = if manager.type_name == "IncrementalStMan" {
            let (endian, size, cache) = incremental_stman::ism_layout(&old)?;
            incremental_stman::write_ism_scalar_columns_with_layout(
                &staged,
                columns,
                &columns_values,
                (endian, Some(size), cache),
            )?
        } else {
            let manager_name = standard_stman::dm_name_from_blob(&manager.data).unwrap_or_default();
            standard_stman::rewrite_ssm_columns(
                &old,
                &staged,
                columns,
                rows as usize,
                &manager_name,
                |column, row| Ok(columns_values[column][row].clone().map(Value::Scalar)),
            )?
        };
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use casa_types::{PrimitiveType, RecordField, RecordValue};

    use crate::{
        ColumnSchema, DataManagerKind, EndianFormat, RequiredScalarColumnValues, Table,
        TableOptions, TableSchema,
    };

    use super::*;

    #[test]
    fn private_replacement_preserves_neighboring_cells_and_the_descriptor() {
        let scratch = tempfile::tempdir().unwrap();
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("keep", PrimitiveType::Int32),
            ColumnSchema::scalar("replace", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let retained = (0..70).collect::<Vec<_>>();
        for &row in &retained {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("keep", Value::Scalar(ScalarValue::Int32(row))),
                    RecordField::new("replace", Value::Scalar(ScalarValue::Int32(42))),
                ]))
                .unwrap();
        }
        let replacement = (100..170).map(|value| -value).collect::<Vec<_>>();
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            let path = scratch.path().join(format!("{endian:?}"));
            let options = TableOptions::new(&path)
                .with_data_manager(DataManagerKind::StandardStMan)
                .with_endian_format(endian);
            table.save(options.clone()).unwrap();
            let descriptor = path.join(TABLE_CONTROL_FILE);
            let before = fs::read(&descriptor).unwrap();
            assert!(replace_private_int32_column(&path, "replace", &replacement).unwrap());
            assert_eq!(fs::read(descriptor).unwrap(), before);
            let values = Table::open(options)
                .unwrap()
                .required_scalar_columns_owned(&["keep", "replace"])
                .unwrap();
            assert!(
                matches!(&values["keep"], RequiredScalarColumnValues::Int32(values) if values == &retained)
            );
            assert!(
                matches!(&values["replace"], RequiredScalarColumnValues::Int32(values) if values == &replacement)
            );
        }
    }
}
