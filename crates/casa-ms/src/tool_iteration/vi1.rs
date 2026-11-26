// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's deprecated VisibilityIterator chunk cursor.

use std::collections::HashMap;
use std::sync::Arc;

use casa_core::{ArrayValue, Value};
use casa_tables::{PlainReferenceRead, Table, TableOptions};

use super::ToolIteration;
use crate::error::{MsError, MsResult};
use crate::grouping::DEFAULT_SORT_COLUMNS;

/// The first visibility buffer of each major chunk of a captured MAIN view.
pub struct Vi1ToolIteration {
    cursor: ToolIteration,
    row_view: Option<Arc<[usize]>>,
    duplicate_positions: Option<Vec<usize>>,
}

impl Vi1ToolIteration {
    /// Build a TIME-ordered iterator and position it on the first chunk.
    ///
    /// Default sorting also groups by array, field and data description.
    /// With positive row blocking, VI1's inclusive bound returns up to
    /// `maxrows + 1` rows from each chunk's first buffer.
    ///
    /// # Errors
    /// Returns an error if the source has no rows or a required sort key
    /// cannot be read.
    pub fn new(
        main: &Table,
        reference: Option<&PlainReferenceRead>,
        selected: Option<Arc<[usize]>>,
        interval: f64,
        maxrows: i64,
        defaults: bool,
    ) -> MsResult<Self> {
        let block = if maxrows > 0 {
            i64::from(maxrows as i32)
        } else if interval <= 0.0 {
            i64::from(
                selected
                    .as_deref()
                    .map_or_else(|| main.row_count(), <[usize]>::len) as i32,
            )
        } else {
            0
        };
        // ToolIteration uses 1 to select one integration per buffer.
        let first_buffer_bound = block.max(0) + 1;
        let view = Vi1RowOrder::new(main, reference, selected.as_deref(), defaults)?;
        let mut cursor = ToolIteration::new(
            main,
            reference,
            view.cached.as_deref().or(selected.as_deref()),
            &["TIME".into()],
            interval,
            first_buffer_bound,
            defaults,
        )?
        .ok_or_else(|| MsError::InvalidInput("TIME must be a known MS sort key".into()))?;
        cursor.rewind();
        Ok(Self {
            cursor,
            row_view: selected,
            duplicate_positions: view.duplicates,
        })
    }

    /// Return to the first chunk, including after exhaustion.
    pub fn rewind(&mut self) {
        self.cursor.rewind();
    }

    /// The iterator has passed its last major chunk.
    pub fn exhausted(&self) -> bool {
        self.cursor.exhausted
    }

    /// Advance to the following major chunk. The first call at the last
    /// chunk reports success and leaves its first buffer available to read.
    pub fn advance(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        let current = self.cursor.position.expect("VI1 is positioned on creation");
        let end = self.cursor.ends[current];
        let groups = self
            .cursor
            .outer_ends
            .as_ref()
            .expect("VI1 has row buffers");
        let group_end = groups[groups.partition_point(|&group| group < end)];
        let next = self.cursor.ends.partition_point(|&end| end <= group_end);
        if next == self.cursor.ends.len() {
            self.cursor.exhausted = true;
        } else {
            self.cursor.position = Some(next);
        }
        true
    }

    /// Logical MAIN row indices in the current first buffer.
    pub fn rows(&self) -> &[usize] {
        self.cursor.rows().expect("VI1 is positioned on creation")
    }

    /// Positions in the working view. Repeated physical rows report their
    /// last occurrence in that view, as in casacore's rowNumbers(view).
    /// # Errors
    /// The captured cursor contains a row outside its original view.
    pub fn row_numbers(&self) -> MsResult<Vec<i64>> {
        self.rows()
            .iter()
            .map(|&row| {
                let position = if let Some(view) = &self.row_view {
                    view.binary_search(&row).map_err(|_| {
                        MsError::InvalidInput("VI1 buffer row is outside its captured view".into())
                    })?
                } else {
                    row
                };
                Ok(self
                    .duplicate_positions
                    .as_ref()
                    .map_or(position, |positions| positions[position]) as i64)
            })
            .collect()
    }
}

struct Vi1RowOrder {
    cached: Option<Vec<usize>>,
    duplicates: Option<Vec<usize>>,
}

impl Vi1RowOrder {
    fn new(
        main: &Table,
        reference: Option<&PlainReferenceRead>,
        selected: Option<&[usize]>,
        defaults: bool,
    ) -> MsResult<Self> {
        let root;
        let owner = if let Some(reference) = reference {
            reference.parent()
        } else if let Some(projection) = main.reference_write_projection(&[], &[])? {
            root = Table::open(TableOptions::new(projection.path))?;
            &root
        } else {
            main
        };
        let count = selected.map_or_else(|| main.row_count(), <[usize]>::len);
        let cached = cached_full_order(owner, count, defaults)?;
        if std::ptr::eq(main, owner) {
            return Ok(Self {
                cached,
                duplicates: None,
            });
        }
        let root_rows = main.root_row_numbers()?;
        let working = match selected {
            Some(rows) => rows
                .iter()
                .map(|&row| {
                    root_rows.get(row).copied().ok_or_else(|| {
                        MsError::InvalidInput("VI1 selected row no longer exists".into())
                    })
                })
                .collect::<MsResult<_>>()?,
            None => root_rows,
        };
        let last = working.iter().copied().zip(0..).collect::<HashMap<_, _>>();
        let duplicates = (last.len() != working.len())
            .then(|| working.iter().map(|physical| last[physical]).collect());
        // MSIter reuses a matching full-table sort even if the input list
        // arrived in a different order. Convert cached physical rows back
        // into this view before reading cells.
        let cached = cached.and_then(|rows| {
            rows.into_iter()
                .map(|physical| {
                    let position = *last.get(&(physical as u64))?;
                    Some(selected.map_or(position, |rows| rows[position]))
                })
                .collect()
        });
        Ok(Self { cached, duplicates })
    }
}

fn cached_full_order(root: &Table, count: usize, defaults: bool) -> MsResult<Option<Vec<usize>>> {
    if count != root.row_count() {
        return Ok(None);
    }
    let keys = if defaults {
        DEFAULT_SORT_COLUMNS
    } else {
        &["TIME"]
    };
    let Some(Value::Array(ArrayValue::String(saved))) = root.keywords().get("SORT_COLUMNS") else {
        return Ok(None);
    };
    if !saved.iter().map(String::as_str).eq(keys.iter().copied()) {
        return Ok(None);
    }
    let (Some(owner), Some(Value::TableRef(link))) =
        (root.path(), root.keywords().get("SORTED_TABLE"))
    else {
        return Ok(None);
    };
    let sorted = Table::open(TableOptions::new(casa_tables::add_directory(link, owner)))?;
    if sorted.row_count() != count {
        return Ok(None);
    }
    let requested: Vec<_> = (0..count).collect();
    let Some(projection) = sorted.reference_write_projection(&requested, &[])? else {
        return Ok(None);
    };
    let same_root = owner.canonicalize().is_ok_and(|physical| {
        projection
            .path
            .canonicalize()
            .is_ok_and(|path| path == physical)
    });
    Ok(same_root.then_some(projection.rows))
}
