// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rebase extra table-valued keywords when materializing a sorted copy.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions, add_directory, strip_directory};
use casa_types::{RecordValue, Value};

use crate::{MsError, MsResult, SubtableId};

pub(super) fn copy_extra_links(input: &Table, output: &Path) -> MsResult<()> {
    let input_path = input
        .path()
        .ok_or_else(|| MsError::InvalidInput("the sorted source has no table path".into()))?;
    let mut copies = Copies::default();
    let keywords = input.keywords();
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        if let Some(Value::TableRef(reference)) = keywords.get(id.name()) {
            let original = add_directory(reference, input_path);
            copies.by_output.insert(output.join(id.name()), original);
        }
    }
    let mut top = keywords.clone();
    top.remove("SORTED_TABLE");
    top.remove("SORT_COLUMNS");
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        top.remove(id.name());
    }
    let changed = copies.rewrite(&mut top, input_path, output)?;
    let columns = copies.rewrite_columns(input, input_path, output)?;
    if changed || !columns.is_empty() {
        let mut stored = Table::open(TableOptions::new(output))?;
        for field in top.fields() {
            if matches!(field.value, Value::TableRef(_)) {
                stored
                    .keywords_mut()
                    .upsert(&field.name, field.value.clone());
            }
        }
        for (name, keywords) in columns {
            stored.set_column_keywords(name, keywords);
        }
        stored.save_metadata_only(TableOptions::new(output))?;
    }
    Ok(())
}

#[derive(Default)]
struct Copies {
    by_output: BTreeMap<PathBuf, PathBuf>,
    copying: Vec<PathBuf>,
}

impl Copies {
    fn rewrite(&mut self, record: &mut RecordValue, input: &Path, output: &Path) -> MsResult<bool> {
        let mut changed = false;
        for field in record.fields_mut() {
            let Value::TableRef(reference) = &mut field.value else {
                continue;
            };
            let declared = add_directory(reference, input);
            let source = declared
                .canonicalize()
                .map_err(|error| invalid(&declared, error))?;
            let name = declared
                .file_name()
                .ok_or_else(|| invalid(&declared, "linked table has no name"))?;
            let destination = output.join(name);
            if let Some(previous) = self.by_output.get(&destination) {
                if previous
                    .canonicalize()
                    .as_ref()
                    .is_ok_and(|path| *path == source)
                {
                    *reference = strip_directory(&destination, output);
                    changed = true;
                    continue;
                }
                return Err(invalid(
                    &destination,
                    "different table links have the same name",
                ));
            }
            self.copy(&source, &destination)?;
            *reference = strip_directory(&destination, output);
            changed = true;
        }
        Ok(changed)
    }

    fn rewrite_columns(
        &mut self,
        table: &Table,
        input: &Path,
        output: &Path,
    ) -> MsResult<Vec<(String, RecordValue)>> {
        let mut updates = Vec::new();
        for column in table
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
        {
            let name = column.name();
            if let Some(keywords) = table.column_keywords(name) {
                let mut keywords = keywords.clone();
                if self.rewrite(&mut keywords, input, output)? {
                    updates.push((name.to_owned(), keywords));
                }
            }
        }
        Ok(updates)
    }

    fn copy(&mut self, source: &Path, output: &Path) -> MsResult<()> {
        if self.copying.iter().any(|path| path == source) {
            return Err(invalid(source, "cyclic table link"));
        }
        let opts = TableOptions::new(source);
        #[cfg(unix)]
        let mut table = Table::open_with_lock(
            opts,
            casa_tables::LockOptions::new(casa_tables::LockMode::AutoLocking).read_only(true),
        )?;
        #[cfg(not(unix))]
        let mut table = Table::open(opts)?;
        self.copying.push(source.to_path_buf());
        self.rewrite(table.keywords_mut(), source, output)?;
        for (name, keywords) in self.rewrite_columns(&table, source, output)? {
            table.set_column_keywords(name, keywords);
        }
        table.deep_copy(TableOptions::new(output))?;
        self.copying.pop();
        self.by_output
            .insert(output.to_path_buf(), source.to_path_buf());
        Ok(())
    }
}

fn invalid(path: &Path, error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(format!("copy linked table {}: {error}", path.display()))
}
