// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// A scalar value that starts at `start_row` and remains active until the next
/// run start or the end of the generated column.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedScalarValueRun {
    pub(super) start_row: usize,
    pub(super) value: Option<ScalarValue>,
}

impl GeneratedScalarValueRun {
    /// Create a scalar run starting at `start_row`.
    pub fn new(start_row: usize, value: Option<ScalarValue>) -> Self {
        Self { start_row, value }
    }

    /// First row covered by this run.
    pub fn start_row(&self) -> usize {
        self.start_row
    }

    /// Value active from `start_row` until the next run.
    pub fn value(&self) -> Option<&ScalarValue> {
        self.value.as_ref()
    }

    pub(crate) fn cloned_value(&self) -> Option<ScalarValue> {
        self.value.clone()
    }
}

#[derive(Clone)]
pub(super) enum GeneratedScalarColumnValues {
    PerRow(Arc<dyn Fn(usize) -> Option<ScalarValue> + Send + Sync>),
    Runs(Arc<Vec<GeneratedScalarValueRun>>),
}

/// Generated scalar-column override for high-volume table writers.
///
/// This lets callers provide scalar cells by row index without first
/// materializing one generic [`Value`] per cell. It is intended for writers
/// such as synthetic MeasurementSet generation where MAIN scalar columns are
/// deterministic functions of row number.
#[derive(Clone)]
pub struct GeneratedScalarColumn {
    pub(super) row_count: usize,
    pub(super) values: GeneratedScalarColumnValues,
}

impl GeneratedScalarColumn {
    /// Create a generated scalar override with exactly `row_count` rows.
    pub fn new(
        row_count: usize,
        value_fn: impl Fn(usize) -> Option<ScalarValue> + Send + Sync + 'static,
    ) -> Self {
        Self {
            row_count,
            values: GeneratedScalarColumnValues::PerRow(Arc::new(value_fn)),
        }
    }

    /// Create a generated scalar override where one value applies to every row.
    pub fn constant(row_count: usize, value: Option<ScalarValue>) -> Self {
        let runs = if row_count == 0 {
            Vec::new()
        } else {
            vec![GeneratedScalarValueRun::new(0, value)]
        };
        Self {
            row_count,
            values: GeneratedScalarColumnValues::Runs(Arc::new(runs)),
        }
    }

    /// Create a generated scalar override from sparse run starts.
    ///
    /// The first run must start at row 0. Runs must be strictly increasing and
    /// start before `row_count`. Each run remains active until the next run
    /// start or the end of the generated column.
    pub fn from_scalar_runs(
        row_count: usize,
        runs: Vec<GeneratedScalarValueRun>,
    ) -> Result<Self, TableError> {
        if let Some(message) = generated_array::run_starts_problem(
            "scalar",
            row_count,
            runs.iter().map(GeneratedScalarValueRun::start_row),
        ) {
            return Err(TableError::InvalidGeneratedScalarRuns { message });
        }
        Ok(Self {
            row_count,
            values: GeneratedScalarColumnValues::Runs(Arc::new(runs)),
        })
    }

    /// Number of rows covered by this generated column.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Return the scalar value for `row`, or `None` for an undefined cell.
    pub fn value(&self, row: usize) -> Option<ScalarValue> {
        if row >= self.row_count {
            return None;
        }
        match &self.values {
            GeneratedScalarColumnValues::PerRow(value_fn) => value_fn(row),
            GeneratedScalarColumnValues::Runs(runs) => {
                let idx = runs.partition_point(|run| run.start_row <= row);
                idx.checked_sub(1)
                    .and_then(|run_idx| runs.get(run_idx))
                    .and_then(GeneratedScalarValueRun::cloned_value)
            }
        }
    }

    pub(crate) fn scalar_runs(&self) -> Option<&[GeneratedScalarValueRun]> {
        match &self.values {
            GeneratedScalarColumnValues::PerRow(_) => None,
            GeneratedScalarColumnValues::Runs(runs) => Some(runs),
        }
    }
}

impl std::fmt::Debug for GeneratedScalarColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.values {
            GeneratedScalarColumnValues::PerRow(_) => "per_row",
            GeneratedScalarColumnValues::Runs(_) => "runs",
        };
        f.debug_struct("GeneratedScalarColumn")
            .field("row_count", &self.row_count)
            .field("mode", &mode)
            .finish_non_exhaustive()
    }
}

/// Per-column override used by high-volume save paths.
#[derive(Clone, Debug)]
pub enum ColumnOverride {
    /// Complete materialized values for a column.
    Values(Vec<Option<Value>>),
    /// Scalar values generated on demand by row number.
    GeneratedScalar(GeneratedScalarColumn),
    /// Array values generated on demand by row number; see
    /// [`GeneratedArrayColumn`] for the storage managers that write them.
    GeneratedArray(GeneratedArrayColumn),
    /// Fixed-width scalar values read from a bounded construction source.
    StreamedScalar(StreamedScalarColumn),
    /// The column is written by another path after table metadata is saved.
    ///
    /// This is only valid for single-column data-manager groups whose on-disk
    /// payload is installed separately, such as streamed tiled MeasurementSet
    /// visibility columns.
    Deferred,
}

impl ColumnOverride {
    fn row_count(&self) -> Option<usize> {
        match self {
            Self::Values(values) => Some(values.len()),
            Self::GeneratedScalar(column) => Some(column.row_count()),
            Self::GeneratedArray(column) => Some(column.row_count()),
            Self::StreamedScalar(column) => Some(column.row_count()),
            Self::Deferred => None,
        }
    }
}

/// Column overrides for [`TableWritePlan::save_with_column_overrides`].
#[derive(Clone, Debug, Default)]
pub struct ColumnOverrides {
    pub(super) row_count: Option<usize>,
    pub(super) columns: HashMap<String, ColumnOverride>,
}

impl ColumnOverrides {
    /// Create an empty override set whose row count is inferred from the table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an override set for a table with `row_count` rows.
    pub fn for_row_count(row_count: usize) -> Self {
        Self {
            row_count: Some(row_count),
            columns: HashMap::new(),
        }
    }

    /// Return the explicit row count, if present.
    pub fn row_count(&self) -> Option<usize> {
        self.row_count
    }

    /// Resolve the effective row count for saving.
    pub(crate) fn effective_row_count(&self, table_row_count: usize) -> Result<usize, TableError> {
        let row_count = self.row_count.unwrap_or(table_row_count);
        for (column, override_value) in &self.columns {
            if let Some(column_rows) = override_value.row_count()
                && column_rows != row_count
            {
                return Err(TableError::Storage(format!(
                    "column override {column} has {column_rows} values for {row_count} rows"
                )));
            }
        }
        Ok(row_count)
    }

    /// Insert materialized override values.
    pub fn insert_values(
        &mut self,
        column: impl Into<String>,
        values: Vec<Option<Value>>,
    ) -> Option<ColumnOverride> {
        self.columns
            .insert(column.into(), ColumnOverride::Values(values))
    }

    /// Insert a generated scalar override.
    pub fn insert_generated_scalar(
        &mut self,
        column: impl Into<String>,
        values: GeneratedScalarColumn,
    ) -> Option<ColumnOverride> {
        self.columns
            .insert(column.into(), ColumnOverride::GeneratedScalar(values))
    }

    /// Insert a generated array override.
    pub fn insert_generated_array(
        &mut self,
        column: impl Into<String>,
        values: GeneratedArrayColumn,
    ) -> Option<ColumnOverride> {
        self.columns
            .insert(column.into(), ColumnOverride::GeneratedArray(values))
    }

    /// Insert a bounded file-backed scalar construction source.
    #[doc(hidden)]
    pub fn insert_streamed_scalar(
        &mut self,
        column: impl Into<String>,
        values: StreamedScalarColumn,
    ) -> Option<ColumnOverride> {
        self.columns
            .insert(column.into(), ColumnOverride::StreamedScalar(values))
    }

    /// Mark a column as externally streamed/deferred.
    pub fn insert_deferred(&mut self, column: impl Into<String>) -> Option<ColumnOverride> {
        self.columns.insert(column.into(), ColumnOverride::Deferred)
    }

    /// Return a column override by name.
    pub fn get(&self, column: &str) -> Option<&ColumnOverride> {
        self.columns.get(column)
    }

    /// Return whether an override exists for a column.
    pub fn contains_key(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    /// Iterate over overridden column names and override values.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ColumnOverride)> {
        self.columns.iter()
    }

    /// Number of overridden columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Return whether no columns are overridden.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}
