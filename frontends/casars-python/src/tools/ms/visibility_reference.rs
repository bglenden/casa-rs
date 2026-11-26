// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reuse the open physical tables for a monotone view of a concatenation.

use casa_tables::{Table, TableOptions, TablePartNames};
use pyo3::prelude::*;

use super::{runtime_error, visibility::VisibilityMain};

pub(super) struct ConcatenatedReference {
    root: Table,
    members: Vec<Table>,
    rows: Vec<usize>,
}

impl ConcatenatedReference {
    pub(super) fn open(view: &Table) -> PyResult<Option<Self>> {
        let Some(schema) = view.schema() else {
            return Ok(None);
        };
        let columns = schema
            .columns()
            .iter()
            .map(|column| column.name())
            .collect::<Vec<_>>();
        let rows = (0..view.row_count()).collect::<Vec<_>>();
        let Some(projection) = view
            .reference_root_projection(&rows, &columns)
            .map_err(runtime_error)?
        else {
            return Ok(None);
        };
        if !projection.rows.is_sorted() || projection.columns != columns {
            return Ok(None);
        }
        let path = &projection.path;
        let TablePartNames::Concatenation { parts, .. } =
            Table::immediate_part_names(path, path).map_err(runtime_error)?
        else {
            return Ok(None);
        };
        let root = Table::open(TableOptions::new(path)).map_err(runtime_error)?;
        if root.schema() != Some(schema) {
            return Ok(None);
        }
        let members = VisibilityMain::open_plain_members(parts)?;
        if members.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            root,
            members,
            rows: projection.rows,
        }))
    }

    pub(super) fn project(&self, selected: &[usize]) -> Option<(&Table, &[Table], Vec<usize>)> {
        if !selected.is_sorted() || selected.last().is_some_and(|&row| row >= self.rows.len()) {
            return None;
        }
        let rows = selected.iter().map(|&row| self.rows[row]).collect();
        Some((&self.root, &self.members, rows))
    }
}
