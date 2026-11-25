// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_tables::publication::{copy_locked_table_tree, lock_table_tree_for_replacement};
use casa_tables::{Table, TableOptions, TablePartNames, add_directory, strip_directory};
use casa_types::{RecordValue, Value};

use super::invalid;
use crate::MsResult;

pub(super) fn validate_joins(tables: &[Table], names: &[String]) -> MsResult<()> {
    for name in names {
        let mut subtables = Vec::with_capacity(tables.len());
        for table in tables {
            let Some(Value::TableRef(link)) = table.keywords().get(name) else {
                return Err(invalid(format!("MultiMS subtable {name} is missing")));
            };
            let path = add_directory(link, table.path().expect("persisted member"));
            subtables.push(Table::open(TableOptions::new(path))?);
        }
        let _ = Table::concat(subtables)?;
    }
    Ok(())
}

pub(super) fn copy_from_first(
    first: &Table,
    target: &Path,
    omit: &[String],
    held: &[casa_tables::TableWriteLock],
    save_keywords: bool,
) -> MsResult<()> {
    let source = first.path().expect("persisted member");
    let mut output = Table::open(TableOptions::new(target))?;
    let mut copies = SubtableCopies {
        root: reference_root(source)?,
        source,
        target,
        omit,
        held,
        by_target: BTreeMap::new(),
    };
    copies.transfer(first.keywords(), output.keywords_mut())?;
    for column in first
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
    {
        let name = column.name();
        if let Some(keys) = first.column_keywords(name) {
            let mut target_keys = output.column_keywords(name).cloned().unwrap_or_default();
            copies.transfer(keys, &mut target_keys)?;
            output.set_column_keywords(name, target_keys);
        }
    }
    if save_keywords {
        output.save_metadata_only(TableOptions::new(target))?;
    }
    Ok(())
}

struct SubtableCopies<'a> {
    root: PathBuf,
    source: &'a Path,
    target: &'a Path,
    omit: &'a [String],
    held: &'a [casa_tables::TableWriteLock],
    by_target: BTreeMap<PathBuf, PathBuf>,
}

impl SubtableCopies<'_> {
    fn transfer(&mut self, source: &RecordValue, target: &mut RecordValue) -> MsResult<()> {
        for field in source.fields() {
            let Value::TableRef(link) = &field.value else {
                continue;
            };
            if self.omit.iter().any(|name| name == &field.name) {
                continue;
            }
            let declared = add_directory(link, self.source);
            let source = declared.canonicalize().map_err(invalid)?;
            if reference_root(&source)? == self.root {
                target.remove(&field.name);
                continue;
            }
            let name = declared
                .file_name()
                .ok_or_else(|| invalid("subtable has no file name"))?;
            let output = self.target.join(name);
            self.copy(source, &output)?;
            target.upsert(
                &field.name,
                Value::TableRef(strip_directory(&output, self.target)),
            );
        }
        Ok(())
    }

    fn copy(&mut self, source: PathBuf, target: &Path) -> MsResult<()> {
        if let Some(previous) = self.by_target.get(target) {
            return if previous == &source {
                Ok(())
            } else {
                Err(invalid(format!(
                    "different subtables are named {}",
                    target.display()
                )))
            };
        }
        // The destination member is a private copy. Only replace table
        // entries here, and remove a symbolic link without touching its target.
        match target.symlink_metadata() {
            Ok(metadata) if metadata.is_symlink() => fs::remove_file(target).map_err(invalid)?,
            Ok(metadata) if metadata.is_dir() && target.join("table.dat").is_file() => {
                fs::remove_dir_all(target).map_err(invalid)?;
            }
            Ok(_) => {
                return Err(invalid(format!(
                    "subtable conflicts with {}",
                    target.display()
                )));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(invalid(error)),
        }
        let _locks = if self
            .held
            .iter()
            .any(|lock| lock.matches_table_path(&source))
        {
            Vec::new()
        } else {
            lock_table_tree_for_replacement(&source)?
        };
        if matches!(
            Table::immediate_part_names(&source, &source)?,
            TablePartNames::Plain(_)
        ) {
            copy_locked_table_tree(&source, target).map_err(invalid)?;
        } else {
            Table::open(TableOptions::new(&source))?.deep_copy(TableOptions::new(target))?;
        }
        self.by_target.insert(target.to_path_buf(), source);
        Ok(())
    }
}

pub(super) fn reference_root(path: &Path) -> MsResult<PathBuf> {
    let mut path = path.to_path_buf();
    for _ in 0..64 {
        let TablePartNames::Reference(parent) = Table::immediate_part_names(&path, &path)? else {
            return Ok(path);
        };
        path = parent.canonicalize().map_err(invalid)?;
    }
    Err(invalid("subtable reference chain is too deep"))
}
