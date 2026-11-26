// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve a subtable through casacore's on-disk MAIN descriptors.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use casa_core::{ScalarValue, Value};
use casa_tables::{Table, TableOptions, TablePartNames};

use super::{MeasurementSet, MsError, MsResult, SubtableId, subtable_path_from_main};

impl MeasurementSet {
    /// The physical tables making up a subtable linked from an open MAIN.
    /// Reference tables inherit the parent's links. A concatenation uses
    /// the first member's link unless its descriptor names the subtable for
    /// concatenation, in which case members contribute in descriptor order.
    ///
    /// # Errors
    /// A required subtable or a contributing parent is missing or unreadable.
    pub fn linked_subtable_paths(main: &Table, id: SubtableId) -> MsResult<Vec<PathBuf>> {
        collect(main, &[id], false, 0)?
            .remove(&id)
            .ok_or_else(|| MsError::MissingSubtable(id.name().to_string()))
    }

    /// Resolve several subtable links in one traversal of the MAIN descriptor.
    /// Tables missing from any contributing member are omitted.
    ///
    /// # Errors
    /// A contributing MAIN descriptor is unreadable or its chain is too deep.
    pub fn linked_subtables(
        main: &Table,
        ids: &[SubtableId],
    ) -> MsResult<HashMap<SubtableId, Vec<PathBuf>>> {
        collect(main, ids, false, 0)
    }

    pub(crate) fn linked_subtable_paths_strict(
        main: &Table,
        id: SubtableId,
    ) -> MsResult<Vec<PathBuf>> {
        collect(main, &[id], true, 0)?
            .remove(&id)
            .ok_or_else(|| MsError::MissingSubtable(id.name().into()))
    }
}

/// Resolve a subtable reference against MAIN, validating the reference before using it.
pub fn strict_subtable_path(
    main: &Table,
    base: &Path,
    id: SubtableId,
) -> MsResult<Option<PathBuf>> {
    let Some(value) = main.keywords().get(id.name()) else {
        return Ok(None);
    };
    let (Value::TableRef(link) | Value::Scalar(ScalarValue::String(link))) = value else {
        return Err(MsError::InvalidInput(format!(
            "Invalid {} table link",
            id.name()
        )));
    };
    let path = casa_tables::add_directory(link, base);
    if !path.join("table.dat").is_file() {
        return Err(MsError::InvalidInput(format!(
            "{} table link is unavailable at {}",
            id.name(),
            path.display()
        )));
    }
    Ok(Some(path))
}

fn collect(
    main: &Table,
    ids: &[SubtableId],
    strict: bool,
    depth: usize,
) -> MsResult<HashMap<SubtableId, Vec<PathBuf>>> {
    if depth >= 128 {
        return Err(MsError::InvalidInput(
            "MeasurementSet subtable link chain is too deep".into(),
        ));
    }
    let Some(path) = main.path() else {
        return Ok(HashMap::new());
    };
    match Table::immediate_part_names(path, path)? {
        TablePartNames::Plain(_) if strict => {
            let mut links = HashMap::new();
            for &id in ids {
                if let Some(link) = strict_subtable_path(main, path, id)? {
                    links.insert(id, vec![link]);
                }
            }
            Ok(links)
        }
        TablePartNames::Plain(_) => Ok(ids
            .iter()
            .filter_map(|&id| subtable_path_from_main(main, path, id).map(|path| (id, vec![path])))
            .collect()),
        TablePartNames::Reference(parent) => {
            let parent = Table::open(TableOptions::new(parent))?;
            collect(&parent, ids, strict, depth + 1)
        }
        TablePartNames::Concatenation {
            parts,
            subtable_names,
        } => {
            let mut parts = parts.into_iter();
            let Some(first) = parts.next() else {
                return Ok(HashMap::new());
            };
            let main = Table::open(TableOptions::new(first))?;
            let mut result = collect(&main, ids, strict, depth + 1)?;
            let member_ids = ids
                .iter()
                .copied()
                .filter(|id| {
                    result.contains_key(id) && subtable_names.iter().any(|name| name == id.name())
                })
                .collect::<Vec<_>>();
            if member_ids.is_empty() {
                return Ok(result);
            }
            for path in parts {
                let main = Table::open(TableOptions::new(path))?;
                let mut linked = collect(&main, &member_ids, strict, depth + 1)?;
                for id in &member_ids {
                    if let Some(paths) = linked.remove(id) {
                        if let Some(existing) = result.get_mut(id) {
                            existing.extend(paths);
                        }
                    } else {
                        result.remove(id);
                    }
                }
            }
            Ok(result)
        }
    }
}
