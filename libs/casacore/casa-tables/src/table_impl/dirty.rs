// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scope of edits that a locked handle has yet to persist.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, PoisonError};

use super::{RecordValue, TableError, TableImpl};

#[derive(Debug, Default, Clone)]
pub(crate) struct DirtyChanges {
    pub(crate) metadata: bool,
    /// Row count/order or schema changed, or a raw mutable row slice escaped.
    pub(crate) whole: bool,
    /// First row of a tracked append, with earlier rows still in disk order.
    pub(crate) appended_from: Option<usize>,
    pub(crate) added_columns: BTreeSet<String>,
    pub(crate) cells: HashMap<String, BTreeSet<usize>>,
}

impl DirtyChanges {
    fn is_empty(&self) -> bool {
        !self.metadata
            && !self.whole
            && self.appended_from.is_none()
            && self.added_columns.is_empty()
            && self.cells.is_empty()
    }
}

impl TableImpl {
    pub(crate) fn track_dirty_changes(&mut self) {
        self.dirty = Some(Mutex::new(DirtyChanges::default()));
    }

    pub(crate) fn dirty_changes(&self) -> Option<DirtyChanges> {
        self.dirty
            .as_ref()
            .map(|dirty| dirty.lock().unwrap_or_else(PoisonError::into_inner).clone())
    }

    pub(crate) fn has_dirty_changes(&self) -> Option<bool> {
        self.dirty.as_ref().map(|dirty| {
            !dirty
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        })
    }

    pub(crate) fn note_change(&mut self) {
        self.record_change(|dirty| dirty.whole = true);
    }

    pub(crate) fn note_row_append(&mut self, row: usize) {
        self.record_change(|dirty| {
            dirty.appended_from.get_or_insert(row);
        });
    }

    pub(crate) fn note_append_saved(&mut self) {
        if let Some(dirty) = &mut self.dirty {
            let dirty = dirty.get_mut().unwrap_or_else(PoisonError::into_inner);
            dirty.appended_from = None;
            dirty.cells.clear();
        }
    }

    pub(crate) fn note_column_added(&mut self, column: &str) {
        self.record_change(|dirty| {
            dirty.added_columns.insert(column.to_owned());
        });
    }

    pub(crate) fn note_added_column_saved(&mut self, column: &str) {
        if let Some(dirty) = &mut self.dirty {
            dirty
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .added_columns
                .remove(column);
        }
    }

    pub(crate) fn note_metadata_change(&mut self) {
        self.record_change(|dirty| dirty.metadata = true);
    }

    pub(crate) fn note_cell_change(&mut self, row: usize, name: &str) {
        self.record_change(|dirty| {
            if let Some(rows) = dirty.cells.get_mut(name) {
                rows.insert(row);
            } else {
                dirty.cells.insert(name.to_owned(), BTreeSet::from([row]));
            }
        });
    }

    pub(crate) fn note_row_change(&mut self, row: usize) {
        self.generation = self.generation.wrapping_add(1);
        if let Some(dirty) = &mut self.dirty {
            let dirty = dirty.get_mut().unwrap_or_else(PoisonError::into_inner);
            if let Some(schema) = &self.schema {
                for column in schema.columns() {
                    if let Some(rows) = dirty.cells.get_mut(column.name()) {
                        rows.insert(row);
                    } else {
                        dirty
                            .cells
                            .insert(column.name().to_owned(), BTreeSet::from([row]));
                    }
                }
            } else {
                dirty.whole = true;
            }
        }
    }

    fn record_change(&mut self, register: impl FnOnce(&mut DirtyChanges)) {
        self.generation = self.generation.wrapping_add(1);
        if let Some(dirty) = &mut self.dirty {
            register(dirty.get_mut().unwrap_or_else(PoisonError::into_inner));
        }
    }

    pub(crate) fn note_metadata_saved(&self) {
        if let Some(dirty) = &self.dirty {
            dirty
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .metadata = false;
        }
    }

    pub(crate) fn note_selected_cells_saved(&mut self, columns: &[&str], sorted_rows: &[usize]) {
        let Some(dirty) = &mut self.dirty else {
            return;
        };
        let dirty = dirty.get_mut().unwrap_or_else(PoisonError::into_inner);
        for column in columns {
            if sorted_rows.is_empty() {
                dirty.cells.remove(*column);
            } else if let Some(rows) = dirty.cells.get_mut(*column) {
                rows.retain(|row| sorted_rows.binary_search(row).is_err());
                if rows.is_empty() {
                    dirty.cells.remove(*column);
                }
            }
        }
    }

    pub(crate) fn clear_dirty_changes(&self) {
        if let Some(dirty) = &self.dirty {
            *dirty.lock().unwrap_or_else(PoisonError::into_inner) = DirtyChanges::default();
        }
    }

    /// Apply a typed or record cell edit to a materialized row and clear
    /// that cell's undefined marker without marking the whole row dirty.
    pub(crate) fn editable_cell_row(
        &mut self,
        row: usize,
        name: &str,
    ) -> Result<&mut RecordValue, TableError> {
        self.note_cell_change(row, name);
        let loaded = self.ensure_loaded_mut()?;
        if let Some(undefined) = loaded.undefined_cells.get_mut(row) {
            undefined.remove(name);
        }
        let row_count = loaded.rows.len();
        loaded.rows.get_mut(row).ok_or(TableError::RowOutOfBounds {
            row_index: row,
            row_count,
        })
    }
}
