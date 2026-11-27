// SPDX-License-Identifier: LGPL-3.0-or-later
//! Route bounded column projections to the physical backing tables.

use super::table_control::ConcatTableDatContents;
use super::*;

impl CompositeStorage {
    pub(super) fn load_concat_metadata(
        &self,
        path: &Path,
        concat: &ConcatTableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        let Some((first, parts)) = concat.table_paths.split_first() else {
            return self.load_concat_table(path, concat);
        };
        let first_path = add_directory_for_link(first, path);
        let mut output = self.load_metadata_only(&first_path)?;
        rebase_table_keywords(&mut output.keywords, &first_path, path);
        for keywords in output.column_keywords.values_mut() {
            rebase_table_keywords(keywords, &first_path, path);
        }
        for part in parts {
            let part = self.load_metadata_only(&add_directory_for_link(part, path))?;
            merge_member_schema(&mut output.schema, part.schema)?;
            output.row_count += part.row_count;
        }
        output.table_info = load_table_info(path);
        output.virtual_columns.clear();
        output.virtual_bindings.clear();
        output.dm_info.clear();
        output.retained_read_metadata = None;
        Ok(output)
    }

    pub(super) fn load_concat_named_scalars(
        &self,
        path: &Path,
        concat: &ConcatTableDatContents,
        names: &HashSet<&str>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let mut result = ScalarColumnSnapshot {
            row_count: 0,
            columns: HashMap::new(),
        };
        for member in &concat.table_paths {
            let path = add_directory_for_link(member, path);
            let part = self.load_named_scalar_columns_with_row_hint(&path, names, None)?;
            result.row_count += part.row_count;
            for (column, values) in part.columns {
                result.columns.entry(column).or_default().extend(values);
            }
        }
        Ok(result)
    }

    pub(super) fn load_concat_scalar_column(
        &self,
        path: &Path,
        concat: &ConcatTableDatContents,
        column: &str,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        let mut values = Vec::new();
        for member in &concat.table_paths {
            let path = add_directory_for_link(member, path);
            values.extend(self.load_scalar_column_with_row_hint(&path, column, None)?);
        }
        Ok(values)
    }

    pub(super) fn load_concat_rows<T>(
        &self,
        path: &Path,
        concat: &ConcatTableDatContents,
        selected: &[usize],
        mut read: impl FnMut(&Path, &[usize]) -> Result<Vec<Option<T>>, StorageError>,
    ) -> Result<Vec<Option<T>>, StorageError> {
        // Group the caller's slots by physical table while retaining their
        // original order and duplicates in the returned block.
        let mut order = selected.iter().copied().enumerate().collect::<Vec<_>>();
        order.sort_unstable_by_key(|&(_, row)| row);
        let mut pending = order.as_slice();
        let mut values = (0..selected.len()).map(|_| None).collect::<Vec<_>>();
        let mut offset = 0;
        for member in &concat.table_paths {
            if pending.is_empty() {
                break;
            }
            let path = add_directory_for_link(member, path);
            let row_end = offset + self.load_metadata_only(&path)?.row_count;
            let count = pending.partition_point(|&(_, row)| row < row_end);
            if count > 0 {
                let local = pending[..count]
                    .iter()
                    .map(|&(_, row)| row - offset)
                    .collect::<Vec<_>>();
                let loaded = read(&path, &local)?;
                for ((slot, _), value) in pending[..count].iter().copied().zip(loaded) {
                    values[slot] = value;
                }
                pending = &pending[count..];
            }
            offset = row_end;
        }
        Ok(values)
    }

    pub(crate) fn load_array_shapes_for_projection(
        &self,
        table_path: &Path,
        column: &str,
        rows: &[usize],
    ) -> Result<Option<Vec<Option<Vec<usize>>>>, StorageError> {
        let layout = read_table_dat_dispatch(&table_path.join(TABLE_CONTROL_FILE))?;
        if matches!(layout, TableDatResult::Plain(_)) {
            return Ok(None);
        }
        self.load_array_shapes_in_layout(table_path, layout, column, rows)
            .map(Some)
    }

    fn load_array_shapes_in_layout(
        &self,
        table_path: &Path,
        layout: TableDatResult,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, StorageError> {
        match layout {
            TableDatResult::Plain(table_dat) => {
                let metadata = RetainedTableReadMetadata::open(table_path, table_dat)?;
                self.plain_array_cell_shapes_uncached(table_path, &metadata, column, rows)
            }
            TableDatResult::Concat(concat) => {
                self.load_concat_rows(table_path, &concat, rows, |path, selected| {
                    let layout = read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))?;
                    self.load_array_shapes_in_layout(path, layout, column, selected)
                })
            }
            TableDatResult::Ref(reference) => {
                if !reference.column_names.iter().any(|name| name == column) {
                    return Ok(vec![None; rows.len()]);
                }
                let parent = add_directory_for_link(&reference.parent_relative_path, table_path);
                let layout = read_table_dat_dispatch(&parent.join(TABLE_CONTROL_FILE))?;
                self.load_array_shapes_in_layout(
                    &parent,
                    layout,
                    reference.parent_column(column),
                    &reference.parent_rows(rows)?,
                )
            }
        }
    }
}

pub(super) fn merge_member_schema(
    combined: &mut Option<crate::TableSchema>,
    member: Option<crate::TableSchema>,
) -> Result<(), StorageError> {
    if let (Some(combined), Some(member)) = (combined, member)
        && combined.merge_concatenation(&member)
    {
        return Ok(());
    }
    Err(StorageError::Schema(
        "incompatible concatenated table columns".into(),
    ))
}
