// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, Value};
use casa_tables::publication::{copy_locked_table_tree, lock_table_tree_for_replacement};
use casa_tables::{
    Table, TableOptions, TablePartNames, TableWriteLock, add_directory, strip_directory,
};

use super::invalid;
use casa_ms::MsResult;

pub(super) fn validate_joins(tables: &[Table], names: &[String]) -> MsResult<()> {
    for name in names {
        let mut subtables = Vec::new();
        for table in tables {
            let path = table.path().expect("persisted member");
            if let Some(Value::TableRef(link)) = table.keywords().get(name) {
                if matches!(
                    Table::immediate_part_names(path, path)?,
                    TablePartNames::Plain(_)
                ) {
                    subtables.push(Table::open(TableOptions::new(add_directory(link, path)))?);
                } else {
                    let mut sources = Vec::new();
                    linked_parts(path, name, 0, &mut sources)?;
                    for source in sources {
                        subtables.push(Table::open(TableOptions::new(source))?);
                    }
                }
            } else {
                return Err(missing_subtable(name));
            }
        }
        let _ = Table::concat(subtables)?;
    }
    Ok(())
}

pub(super) fn copy_from_first(
    first: &Table,
    target: &Path,
    omit: &[String],
    held: &[TableWriteLock],
    save_keywords: bool,
    stage: &Path,
) -> MsResult<()> {
    if save_keywords && !reference_root(target)?.starts_with(stage) {
        return Err(invalid(
            "cannot update a reference base outside the MultiMS stage",
        ));
    }
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
    let mut joins = BTreeSet::new();
    joined_names(&copies.root, 0, &mut joins)?;
    copies.transfer(first.keywords(), output.keywords_mut(), &joins)?;
    for name in joins.iter().filter(|name| !omit.contains(name)) {
        copies.copy_joined(name, output.keywords_mut())?;
    }
    for column in first
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
    {
        let name = column.name();
        if let Some(keys) = first.column_keywords(name) {
            let mut target_keys = output.column_keywords(name).cloned().unwrap_or_default();
            copies.transfer(keys, &mut target_keys, &BTreeSet::new())?;
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
    held: &'a [TableWriteLock],
    by_target: BTreeMap<PathBuf, PathBuf>,
}

impl SubtableCopies<'_> {
    fn transfer(
        &mut self,
        source: &RecordValue,
        target: &mut RecordValue,
        joined: &BTreeSet<String>,
    ) -> MsResult<()> {
        for field in source.fields() {
            let Value::TableRef(link) = &field.value else {
                continue;
            };
            if self.omit.contains(&field.name) || joined.contains(&field.name) {
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

    fn copy_joined(&self, name: &str, record: &mut RecordValue) -> MsResult<()> {
        let mut paths = Vec::new();
        linked_parts(self.source, name, 0, &mut paths)?;
        let paths = paths
            .into_iter()
            .map(|path| path.canonicalize().map_err(invalid))
            .collect::<MsResult<Vec<_>>>()?;
        let mut locks = Vec::new();
        for path in paths.iter().collect::<BTreeSet<_>>() {
            locks.extend(self.lock_if_needed(path)?);
        }
        let tables = paths
            .into_iter()
            .map(|path| Table::open(TableOptions::new(path)))
            .collect::<Result<Vec<_>, _>>()?;
        // CASA assigns a separate table directory to the copied join.
        // Keep the target's previous physical subtable beside it.
        let output = tempfile::Builder::new()
            .prefix("tab")
            .tempdir_in(self.target)
            .map_err(invalid)?;
        Table::concat(tables)?.deep_copy(TableOptions::new(output.path()))?;
        let output = output.keep();
        record.upsert(name, Value::TableRef(strip_directory(&output, self.target)));
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
        let _locks = self.lock_if_needed(&source)?;
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

    fn lock_if_needed(&self, source: &Path) -> MsResult<Vec<TableWriteLock>> {
        if self.held.iter().any(|lock| lock.matches_table_path(source)) {
            Ok(Vec::new())
        } else {
            Ok(lock_table_tree_for_replacement(source)?)
        }
    }
}

fn missing_subtable(name: &str) -> casa_ms::MsError {
    invalid(format!("MultiMS subtable {name} is missing"))
}

fn joined_names(path: &Path, depth: usize, names: &mut BTreeSet<String>) -> MsResult<()> {
    if depth >= 64 {
        return Err(invalid("MultiMS subtable chain is too deep"));
    }
    match Table::immediate_part_names(path, path)? {
        TablePartNames::Plain(_) => {}
        TablePartNames::Reference(parent) => joined_names(&parent, depth + 1, names)?,
        TablePartNames::Concatenation {
            parts,
            subtable_names,
        } => {
            names.extend(subtable_names);
            if let Some(first) = parts.first() {
                joined_names(first, depth + 1, names)?;
            }
        }
    }
    Ok(())
}

fn linked_parts(path: &Path, name: &str, depth: usize, leaves: &mut Vec<PathBuf>) -> MsResult<()> {
    if depth >= 64 {
        return Err(invalid("MultiMS subtable chain is too deep"));
    }
    match Table::immediate_part_names(path, path)? {
        TablePartNames::Plain(_) => {
            let table = Table::open(TableOptions::new(path))?;
            let Some(Value::TableRef(link)) = table.keywords().get(name) else {
                return Err(missing_subtable(name));
            };
            leaves.push(add_directory(link, path));
        }
        TablePartNames::Reference(parent) => linked_parts(&parent, name, depth + 1, leaves)?,
        TablePartNames::Concatenation {
            parts,
            subtable_names,
        } => {
            if subtable_names.iter().any(|joined| joined == name) {
                for part in parts {
                    linked_parts(&part, name, depth + 1, leaves)?;
                }
            } else if let Some(first) = parts.first() {
                linked_parts(first, name, depth + 1, leaves)?;
            } else {
                return Err(missing_subtable(name));
            }
        }
    }
    Ok(())
}

fn reference_root(path: &Path) -> MsResult<PathBuf> {
    let mut path = path.to_path_buf();
    for _ in 0..64 {
        let TablePartNames::Reference(parent) = Table::immediate_part_names(&path, &path)? else {
            return Ok(path);
        };
        path = parent.canonicalize().map_err(invalid)?;
    }
    Err(invalid("subtable reference chain is too deep"))
}
