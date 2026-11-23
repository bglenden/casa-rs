// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve a subtable through casacore's on-disk MAIN descriptors.

use std::collections::HashMap;
use std::path::PathBuf;

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
        collect(main, &[id], 0)?
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
        collect(main, ids, 0)
    }
}

fn collect(
    main: &Table,
    ids: &[SubtableId],
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
        TablePartNames::Plain(_) => Ok(ids
            .iter()
            .filter_map(|&id| subtable_path_from_main(main, path, id).map(|path| (id, vec![path])))
            .collect()),
        TablePartNames::Reference(parent) => {
            let parent = Table::open(TableOptions::new(parent))?;
            collect(&parent, ids, depth + 1)
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
            let mut result = collect(&main, ids, depth + 1)?;
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
                let mut linked = collect(&main, &member_ids, depth + 1)?;
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
