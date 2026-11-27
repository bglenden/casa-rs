// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy selector's whole-group cursor and physical row blocks.

use std::borrow::Cow;
use std::cmp::Ordering;

use casa_core::{ArrayValue, Value};
use casa_tables::{ColumnType, PlainReferenceRead, RequiredScalarColumnValues, Table};

use super::{compare_scalar, same_interval};
use crate::tool_visibility::read_reference_scalars;
use casa_ms::DEFAULT_SORT_COLUMNS;
use casa_ms::schema::main_table::{OPTIONAL_COLUMNS, REQUIRED_COLUMNS};
use casa_ms::{MsError, MsResult};

/// The source selection and grouping parameters for a legacy MS cursor.
#[derive(Clone, Copy)]
pub struct LegacyIterationOptions<'a> {
    /// Working-view row positions selected by the legacy selector.
    pub selected: Option<&'a [usize]>,
    /// Logical MAIN rows exposed by the working view, if selected.
    pub working_rows: Option<&'a [usize]>,
    /// Requested casacore sort keys, in priority order.
    pub columns: &'a [String],
    /// Length of a time bin in seconds. Zero groups the full key interval.
    pub interval: f64,
    /// Limit on rows per returned block. Zero keeps each group intact.
    pub max_rows: i64,
    /// Add the MSIter default keys that were not requested.
    pub defaults: bool,
}

/// Legacy grouping order with an independent row-block position.
pub struct LegacyToolIteration {
    rows: Vec<usize>,
    ends: Vec<usize>,
    keys: Vec<String>,
    block: usize,
    group: usize,
    offset: usize,
    positioned: bool,
    exhausted: bool,
}

impl LegacyToolIteration {
    /// Capture the selected working rows and their sorted iterator groups.
    ///
    /// # Errors
    /// Returns an error for invalid scalar data or a changed working projection.
    /// An unrecognized requested column returns `Ok(None)`.
    pub fn new(
        main: &Table,
        reference: Option<&PlainReferenceRead>,
        options: LegacyIterationOptions<'_>,
    ) -> MsResult<Option<Self>> {
        let requested = if matches!(options.columns, [only] if only.is_empty()) {
            &[]
        } else {
            options.columns
        };
        if requested.iter().any(|name| !known_column(name)) {
            return Ok(None);
        }
        let keys = sort_keys(main, requested, options.interval, options.defaults);
        let count = options
            .working_rows
            .map_or_else(|| main.row_count(), <[usize]>::len);
        let rows = options
            .selected
            .map_or_else(|| (0..count).collect::<Vec<_>>(), <[usize]>::to_vec);
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.iter().any(|&row| row >= count) {
            return Err(MsError::InvalidInput(
                "legacy iterator row no longer exists".into(),
            ));
        }
        let physical = if let Some(working) = options.working_rows {
            Cow::Owned(rows.iter().map(|&row| working[row]).collect())
        } else {
            Cow::Borrowed(rows.as_slice())
        };
        let names = keys.iter().map(String::as_str).collect::<Vec<_>>();
        for name in &names {
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
        let columns = if let Some(reference) = reference {
            read_reference_scalars(reference, &names, &reference.parent_rows(&physical)?)?
        } else if options.selected.is_none() && options.working_rows.is_none() {
            main.required_scalar_columns_owned(&names)?
        } else {
            main.required_scalar_columns_owned_for_rows(&names, &physical)?
        };
        if let Some(RequiredScalarColumnValues::Float64(times)) = columns.get("TIME")
            && times.iter().any(|time| !time.is_finite())
        {
            return Err(MsError::InvalidInput(
                "TIME contains a nonfinite value".into(),
            ));
        }
        let sorted = names
            .iter()
            .map(|&name| (name, &columns[name]))
            .collect::<Vec<_>>();
        let mut order = (0..rows.len()).collect::<Vec<_>>();
        let compare = |&left: &usize, &right: &usize| {
            sorted
                .iter()
                .map(|(_, values)| compare_scalar(values, left, right))
                .find(|&result| result != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        };
        if !order.is_sorted_by(|left, right| compare(left, right) != Ordering::Greater) {
            order.sort_by(compare);
        }
        let mut ends = Vec::new();
        let mut start = 0;
        while start < order.len() {
            let first = order[start];
            let mut end = start + 1;
            while end < order.len()
                && sorted.iter().all(|(name, values)| {
                    let row = order[end];
                    match (name, values) {
                        (&"TIME", RequiredScalarColumnValues::Float64(times)) => {
                            same_interval(times[first], times[row], options.interval)
                        }
                        _ => compare_scalar(values, first, row) == Ordering::Equal,
                    }
                })
            {
                end += 1;
            }
            ends.push(end);
            start = end;
        }
        let rows = order.into_iter().map(|index| rows[index]).collect();
        let block = usize::try_from(options.max_rows)
            .ok()
            .filter(|&count| count > 0);
        Ok(Some(Self {
            rows,
            ends,
            keys,
            block: block.unwrap_or(usize::MAX),
            group: 0,
            offset: 0,
            positioned: false,
            exhausted: false,
        }))
    }

    /// Keys of the ordered view, suitable for the SORT_COLUMNS keyword.
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Working-view rows in the sort order, before grouping or blocking.
    pub fn ordered_rows(&self) -> &[usize] {
        &self.rows
    }

    /// Start at the first row block of the first outer group.
    pub fn rewind(&mut self) -> &[usize] {
        self.group = 0;
        self.offset = 0;
        self.positioned = true;
        self.exhausted = false;
        self.current()
    }

    /// Advance to the next nonempty block, or report the end of the view.
    /// Before rewind, the first outer group is already selected internally.
    pub fn advance(&mut self) -> Option<&[usize]> {
        if self.exhausted {
            return None;
        }
        let next = self.offset.saturating_add(self.block);
        if self.positioned && next < self.ends[self.group] - self.group_start() {
            self.offset = next;
        } else {
            self.group += 1;
            self.offset = 0;
            if self.group >= self.ends.len() {
                self.exhausted = true;
                return None;
            }
        }
        self.positioned = true;
        Some(self.current())
    }

    fn group_start(&self) -> usize {
        self.group
            .checked_sub(1)
            .map_or(0, |previous| self.ends[previous])
    }

    fn current(&self) -> &[usize] {
        let start = self.group_start() + self.offset;
        &self.rows[start..self.ends[self.group].min(start.saturating_add(self.block))]
    }
}

fn known_column(name: &str) -> bool {
    REQUIRED_COLUMNS
        .iter()
        .chain(OPTIONAL_COLUMNS)
        .any(|column| column.name == name)
}

fn sort_keys(main: &Table, requested: &[String], interval: f64, defaults: bool) -> Vec<String> {
    let persisted;
    let requested = if requested.is_empty()
        && let Some(Value::Array(ArrayValue::String(columns))) = main.keywords().get("SORT_COLUMNS")
    {
        persisted = columns.iter().cloned().collect::<Vec<_>>();
        &persisted
    } else {
        requested
    };
    let mut keys = Vec::new();
    if defaults {
        keys.extend(
            DEFAULT_SORT_COLUMNS
                .iter()
                .copied()
                .filter(|name| !requested.iter().any(|column| column == name))
                .map(str::to_owned),
        );
    }
    keys.extend_from_slice(requested);
    if interval != 0.0 && !keys.iter().any(|name| name == "TIME") {
        keys.push("TIME".into());
    }
    keys
}
