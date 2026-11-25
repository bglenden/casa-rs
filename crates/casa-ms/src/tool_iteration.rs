// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row grouping and cursor state for the public CASA `ms` iterator.

use std::cmp::Ordering;

use casa_tables::{ColumnType, PlainReferenceRead, RequiredScalarColumnValues, Table};
use casa_types::{ArrayValue, Value};

use crate::error::{MsError, MsResult};
use crate::grouping::DEFAULT_SORT_COLUMNS;
use crate::schema::main_table::{OPTIONAL_COLUMNS, REQUIRED_COLUMNS};
use crate::tool_visibility::read_reference_scalars;

mod legacy;
pub use legacy::{LegacyIterationOptions, LegacyToolIteration};

/// A snapshot of the selected MAIN rows in iteration order.
///
/// The attachment's ordinary row selection is independent of the iterator.
/// Each group consists of a range in one shared ordered row vector.
pub struct ToolIteration {
    rows: Vec<usize>,
    ends: Vec<usize>,
    outer_ends: Option<Vec<usize>>,
    position: Option<usize>,
    exhausted: bool,
}

impl ToolIteration {
    /// Prepare iteration without placing the cursor on a data buffer.
    ///
    /// An unrecognized CASA column returns `Ok(None)`. Recognized columns
    /// must be available as scalar columns on the attached MAIN table.
    ///
    /// # Errors
    /// Returns an error for an empty view, an invalid sort key or a read error.
    pub fn new(
        main: &Table,
        reference: Option<&PlainReferenceRead>,
        selected: Option<&[usize]>,
        columns: &[String],
        interval: f64,
        maxrows: i64,
        defaults: bool,
    ) -> MsResult<Option<Self>> {
        let requested = match columns.first() {
            None => &[],
            Some(first) if first.is_empty() => &[],
            Some(_) => columns,
        };
        if requested.iter().any(|name| {
            !REQUIRED_COLUMNS
                .iter()
                .chain(OPTIONAL_COLUMNS)
                .any(|column| column.name == name)
        }) {
            return Ok(None);
        }
        let persisted;
        let requested: &[String] = if !requested.is_empty() {
            requested
        } else if let Some(Value::Array(ArrayValue::String(columns))) =
            main.keywords().get("SORT_COLUMNS")
        {
            persisted = columns.iter().cloned().collect::<Vec<_>>();
            &persisted
        } else {
            &[]
        };
        let mut keys = Vec::new();
        if defaults {
            keys.extend(
                DEFAULT_SORT_COLUMNS
                    .iter()
                    .copied()
                    .filter(|name| !requested.iter().any(|column| column == name)),
            );
        }
        for name in requested.iter().map(String::as_str) {
            if !keys.contains(&name) {
                keys.push(name);
            }
        }
        if interval != 0.0 && !keys.contains(&"TIME") {
            keys.push("TIME");
        }

        let rows = selected.map_or_else(|| (0..main.row_count()).collect(), <[usize]>::to_vec);
        if rows.is_empty() {
            return Err(MsError::InvalidInput(
                "MSIter::construct - Input MeasurementSet.has zero selected rows".into(),
            ));
        }
        let mut read_keys = keys.clone();
        if maxrows <= 1 && !read_keys.contains(&"TIME") {
            read_keys.push("TIME");
        }
        for &name in &read_keys {
            let column = main
                .schema()
                .and_then(|schema| schema.column(name))
                .ok_or_else(|| MsError::InvalidInput(format!("Table column {name} is unknown")))?;
            if column.column_type() != &ColumnType::Scalar {
                return Err(MsError::InvalidInput(format!(
                    "Invalid table sort: Sort column {name} is not a scalar"
                )));
            }
        }

        let values = if let Some(reference) = reference {
            let parent_rows = reference.parent_rows(&rows)?;
            read_reference_scalars(reference, &read_keys, &parent_rows)?
        } else if selected.is_some() {
            main.required_scalar_columns_owned_for_rows(&read_keys, &rows)?
        } else {
            main.required_scalar_columns_owned(&read_keys)?
        };
        let time = match values.get("TIME") {
            Some(RequiredScalarColumnValues::Float64(times)) => times.as_slice(),
            None => &[],
            _ => return Err(MsError::InvalidInput("TIME must contain a double".into())),
        };
        if time.iter().any(|value| !value.is_finite()) {
            return Err(MsError::InvalidInput(
                "TIME contains a nonfinite value".into(),
            ));
        }
        let keys: Vec<_> = keys.iter().map(|name| (*name, &values[*name])).collect();
        let mut order: Vec<usize> = (0..rows.len()).collect();
        let compare = |&left: &usize, &right: &usize| {
            keys.iter()
                .map(|(_, values)| compare_scalar(values, left, right))
                .find(|&order| order != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        };
        if !order.is_sorted_by(|left, right| compare(left, right) != Ordering::Greater) {
            order.sort_by(compare);
        }

        let mut ends = Vec::new();
        let mut outer_ends = (maxrows > 0).then(Vec::new);
        let mut start = 0;
        while start < order.len() {
            let first = order[start];
            let mut end = start + 1;
            while end < order.len()
                && keys.iter().all(|(name, values)| {
                    let row = order[end];
                    if *name == "TIME" {
                        same_interval(time[first], time[row], interval)
                    } else {
                        compare_scalar(values, first, row) == Ordering::Equal
                    }
                })
            {
                end += 1;
            }
            if maxrows > 1 {
                let block = maxrows as usize;
                ends.extend((start..end).step_by(block).skip(1));
            } else {
                // With no fixed row blocking, the visibility buffer enumerates
                // each integration in time order within an outer group.
                let group = &mut order[start..end];
                group.sort_by(|&left, &right| time[left].total_cmp(&time[right]));
                if maxrows == 1 {
                    ends.extend((start + 1..end).filter(|&i| time[order[i - 1]] != time[order[i]]));
                }
            }
            ends.push(end);
            if let Some(outer_ends) = &mut outer_ends {
                outer_ends.push(end);
            }
            start = end;
        }

        Ok(Some(Self {
            rows: order.into_iter().map(|index| rows[index]).collect(),
            ends,
            outer_ends,
            position: None,
            exhausted: false,
        }))
    }

    /// Position on the first group again.
    pub fn rewind(&mut self) {
        self.position = Some(0);
        self.exhausted = false;
    }

    /// Return to whole outer groups while retaining the captured row view.
    /// The tool uses this when resetting the attachment's selection.
    pub fn reset_blocking(&mut self) {
        if let Some(outer_ends) = self.outer_ends.take() {
            self.position = self
                .position
                .map(|position| outer_ends.partition_point(|end| *end < self.ends[position]));
            self.ends = outer_ends;
        }
    }

    /// Move to the next group. An exhausted blocked cursor continues to expose
    /// the last buffer, but another advance is an error.
    ///
    /// # Errors
    /// Row-blocked traversal requires a positioned and unexhausted cursor.
    pub fn advance(&mut self) -> MsResult<bool> {
        if self.outer_ends.is_some() && (self.position.is_none() || self.exhausted) {
            return Err(MsError::InvalidInput(
                "Attempt to advance subchunk past end of chunk".into(),
            ));
        }
        if self.exhausted {
            return Err(MsError::InvalidInput(
                "Attempt to advance past end of data using nextChunk".into(),
            ));
        }
        let next = self.position.unwrap_or(0) + 1;
        if next >= self.ends.len() {
            self.exhausted = true;
            return Ok(false);
        }
        self.position = Some(next);
        Ok(true)
    }

    /// Physical MAIN rows in the current buffer.
    ///
    /// # Errors
    /// The cursor must be positioned by `rewind` or `advance`.
    pub fn rows(&self) -> MsResult<&[usize]> {
        let position = self.position.ok_or_else(|| {
            MsError::InvalidInput(if self.outer_ends.is_some() {
                "VisBuffer not positioned to a subchunk".into()
            } else {
                "Call to origin without first initializing chunk".into()
            })
        })?;
        let start = if position == 0 {
            0
        } else {
            self.ends[position - 1]
        };
        Ok(&self.rows[start..self.ends[position]])
    }
}

fn compare_scalar(values: &RequiredScalarColumnValues, a: usize, b: usize) -> Ordering {
    match values {
        RequiredScalarColumnValues::Bool(values) => values[a].cmp(&values[b]),
        RequiredScalarColumnValues::Int32(values) => values[a].cmp(&values[b]),
        RequiredScalarColumnValues::Float32(values) => values[a].total_cmp(&values[b]),
        RequiredScalarColumnValues::Float64(values) => values[a].total_cmp(&values[b]),
    }
}

fn same_interval(first: f64, row: f64, interval: f64) -> bool {
    if interval == 0.0 || first == row {
        return true;
    }
    if interval.abs() < 2.0 * f64::MIN_POSITIVE {
        return false;
    }
    let anchor = first - 0.01;
    ((first - anchor) / interval).floor() == ((row - anchor) / interval).floor()
}
