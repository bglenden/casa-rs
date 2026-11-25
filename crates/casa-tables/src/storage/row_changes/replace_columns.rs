// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, HashSet};

use super::{
    ColumnDescContents, CompositeStorage, DataManagerEntry, ScalarValue, StagedEdit, StorageError,
    Value, incremental_stman, standard_stman,
};

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
