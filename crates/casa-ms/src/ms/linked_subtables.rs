// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve a subtable through casacore's on-disk MAIN descriptors.

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
        let mut paths = Vec::new();
        collect(main, id, 0, &mut paths)?;
        Ok(paths)
    }
}

fn collect(
    main: &Table,
    id: SubtableId,
    depth: usize,
    subtables: &mut Vec<PathBuf>,
) -> MsResult<()> {
    if depth >= 128 {
        return Err(MsError::InvalidInput(
            "MeasurementSet subtable link chain is too deep".into(),
        ));
    }
    let missing = || MsError::MissingSubtable(id.name().to_string());
    let path = main.path().ok_or_else(missing)?;
    match Table::immediate_part_names(path, path)? {
        TablePartNames::Plain(_) => {
            subtables.push(subtable_path_from_main(main, path, id).ok_or_else(missing)?);
        }
        TablePartNames::Reference(parent) => {
            let parent = Table::open(TableOptions::new(parent))?;
            collect(&parent, id, depth + 1, subtables)?;
        }
        TablePartNames::Concatenation {
            parts,
            subtable_names,
        } => {
            if parts.is_empty() {
                return Err(missing());
            }
            let count = if subtable_names.iter().any(|name| name == id.name()) {
                parts.len()
            } else {
                1
            };
            for path in parts.into_iter().take(count) {
                let parent = Table::open(TableOptions::new(path))?;
                collect(&parent, id, depth + 1, subtables)?;
            }
        }
    }
    Ok(())
}
