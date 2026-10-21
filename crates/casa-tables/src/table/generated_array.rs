// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generated array-column overrides: array cells a writer produces by row
//! number instead of holding one [`Value`] per cell.
//!
//! The scalar counterpart is [`GeneratedScalarColumn`](super::GeneratedScalarColumn).
//! A table saved with such overrides through
//! [`TableWritePlan::save_with_column_overrides`](super::TableWritePlan::save_with_column_overrides)
//! is written by the StandardStMan and IncrementalStMan group writers a
//! cell at a time, so a column of any row count costs only the generator's
//! own state.

use std::borrow::Cow;
use std::sync::Arc;

use casa_types::{ArrayValue, Value};

use super::TableError;

/// An array value that starts at `start_row` and holds until the next run
/// starts or the generated column ends.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedArrayValueRun {
    start_row: usize,
    /// Held as [`Value::Array`] so writers can borrow the cell.
    value: Option<Value>,
}

impl GeneratedArrayValueRun {
    /// A run of `value` from `start_row`; `None` leaves the cells undefined.
    pub fn new(start_row: usize, value: Option<ArrayValue>) -> Self {
        Self {
            start_row,
            value: value.map(Value::Array),
        }
    }

    /// First row covered by this run.
    pub fn start_row(&self) -> usize {
        self.start_row
    }

    /// Value active from `start_row` until the next run.
    pub fn value(&self) -> Option<&ArrayValue> {
        match &self.value {
            Some(Value::Array(array)) => Some(array),
            _ => None,
        }
    }

    pub(crate) fn cell(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}

/// The per-row generator of a [`GeneratedArrayColumn`].
type ArrayCellFn = dyn Fn(usize) -> Option<ArrayValue> + Send + Sync;

#[derive(Clone)]
enum GeneratedArrayColumnValues {
    PerRow(Arc<ArrayCellFn>),
    Runs(Arc<Vec<GeneratedArrayValueRun>>),
}

/// Generated array-column override for high-volume table writers.
///
/// The cells are given by row number, either by a closure evaluated for each
/// row ([`new`](Self::new)) or as runs of one value
/// ([`from_array_runs`](Self::from_array_runs), [`constant`](Self::constant)),
/// so a writer such as a simulator's POINTING subtable never materializes a
/// [`Value`] per cell. Insert it with
/// [`ColumnOverrides::insert_generated_array`](super::ColumnOverrides::insert_generated_array).
///
/// Only StandardStMan and IncrementalStMan write generated array columns,
/// and every column of the data-manager group must then be overridden.
/// IncrementalStMan stores fixed-shape arrays only. Run values are checked
/// against the column's schema when the save starts; a per-row closure's
/// cells are checked against the column's type and shape as they are
/// written.
#[derive(Clone)]
pub struct GeneratedArrayColumn {
    row_count: usize,
    values: GeneratedArrayColumnValues,
}

impl GeneratedArrayColumn {
    /// A column of `row_count` cells, cell `row` being `value_fn(row)`;
    /// `None` leaves the cell undefined.
    pub fn new(
        row_count: usize,
        value_fn: impl Fn(usize) -> Option<ArrayValue> + Send + Sync + 'static,
    ) -> Self {
        Self {
            row_count,
            values: GeneratedArrayColumnValues::PerRow(Arc::new(value_fn)),
        }
    }

    /// A column of `row_count` cells that all hold `value`.
    pub fn constant(row_count: usize, value: Option<ArrayValue>) -> Self {
        let runs = if row_count == 0 {
            Vec::new()
        } else {
            vec![GeneratedArrayValueRun::new(0, value)]
        };
        Self {
            row_count,
            values: GeneratedArrayColumnValues::Runs(Arc::new(runs)),
        }
    }

    /// A column of `row_count` cells given as runs.
    ///
    /// The first run must start at row 0, the runs must start at strictly
    /// increasing rows below `row_count`, and each holds until the next
    /// starts or the column ends. For zero rows, `runs` must be empty.
    ///
    /// # Errors
    ///
    /// [`TableError::InvalidGeneratedArrayRuns`] when the run starts break
    /// those rules.
    pub fn from_array_runs(
        row_count: usize,
        runs: Vec<GeneratedArrayValueRun>,
    ) -> Result<Self, TableError> {
        if let Some(message) = run_starts_problem(
            "array",
            row_count,
            runs.iter().map(GeneratedArrayValueRun::start_row),
        ) {
            return Err(TableError::InvalidGeneratedArrayRuns { message });
        }
        Ok(Self {
            row_count,
            values: GeneratedArrayColumnValues::Runs(Arc::new(runs)),
        })
    }

    /// Number of rows covered by this generated column.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// The array in `row`, or `None` for an undefined cell or a row past the
    /// column's end.
    pub fn value(&self, row: usize) -> Option<ArrayValue> {
        match self.cell(row)? {
            Cow::Owned(Value::Array(array)) => Some(array),
            Cow::Borrowed(Value::Array(array)) => Some(array.clone()),
            _ => None,
        }
    }

    /// The cell in `row` as a [`Value::Array`], borrowed from a run or owned
    /// from the per-row generator.
    pub(crate) fn cell(&self, row: usize) -> Option<Cow<'_, Value>> {
        if row >= self.row_count {
            return None;
        }
        match &self.values {
            GeneratedArrayColumnValues::PerRow(value_fn) => {
                value_fn(row).map(|array| Cow::Owned(Value::Array(array)))
            }
            GeneratedArrayColumnValues::Runs(runs) => {
                let index = runs.partition_point(|run| run.start_row <= row);
                index
                    .checked_sub(1)
                    .and_then(|run| runs[run].cell())
                    .map(Cow::Borrowed)
            }
        }
    }

    /// The runs, for a column given as runs.
    pub(crate) fn array_runs(&self) -> Option<&[GeneratedArrayValueRun]> {
        match &self.values {
            GeneratedArrayColumnValues::PerRow(_) => None,
            GeneratedArrayColumnValues::Runs(runs) => Some(runs),
        }
    }
}

impl std::fmt::Debug for GeneratedArrayColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.values {
            GeneratedArrayColumnValues::PerRow(_) => "per_row",
            GeneratedArrayColumnValues::Runs(_) => "runs",
        };
        f.debug_struct("GeneratedArrayColumn")
            .field("row_count", &self.row_count)
            .field("mode", &mode)
            .finish_non_exhaustive()
    }
}

/// Why the run starts `starts` of a generated `kind` column of `row_count`
/// rows are malformed, or `None` when they are valid: the first starts at
/// row 0, the starts increase strictly and stay below `row_count`, and a
/// column of zero rows has no runs.
pub(super) fn run_starts_problem(
    kind: &str,
    row_count: usize,
    mut starts: impl Iterator<Item = usize>,
) -> Option<String> {
    let Some(first) = starts.next() else {
        return (row_count > 0).then(|| format!("generated {kind} runs must start at row 0"));
    };
    if row_count == 0 {
        return Some(format!("generated {kind} runs for zero rows must be empty"));
    }
    if first != 0 {
        return Some(format!("generated {kind} runs must start at row 0"));
    }
    let mut previous = first;
    for start in starts {
        if start <= previous {
            return Some(format!("generated {kind} runs must be strictly increasing"));
        }
        previous = start;
    }
    (previous >= row_count)
        .then(|| format!("generated {kind} run starts beyond row count {row_count}"))
}
