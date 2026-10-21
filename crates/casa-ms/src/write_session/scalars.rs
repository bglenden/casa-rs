// SPDX-License-Identifier: LGPL-3.0-or-later
//! A creation session's scalar column writers.
//!
//! A producer resolves each column once ([`MeasurementSetWriteSession::scalar_column`])
//! and appends whole runs of cells per batch
//! ([`MeasurementSetWriteSession::push_scalar_column`],
//! [`MeasurementSetWriteSession::push_repeated_scalar`]). Pushing one cell
//! at a time by column name ([`MeasurementSetWriteSession::push_scalar_row`])
//! stays for callers that produce row by row, at the price of a name lookup
//! and a validation per cell.

use std::collections::HashMap;
use std::path::Path;

use casa_tables::{StreamedScalarValues, StreamingScalarColumnWriter};
use casa_types::ScalarValue;

use super::{
    MeasurementSetScalarColumnPlan, MeasurementSetWriteError, MeasurementSetWriteSession,
    MeasurementSetWriteSessionState,
};

/// A scalar column of a creation session, resolved once by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarColumnId(usize);

/// The scalar column writers of one creation session, in plan order.
#[derive(Default)]
pub(super) struct ScalarWriters {
    writers: Vec<Option<StreamingScalarColumnWriter>>,
    index: HashMap<String, usize>,
}

impl ScalarWriters {
    /// One temporary construction source per planned column in `output`.
    pub(super) fn create(
        output: &Path,
        row_count: usize,
        columns: &[MeasurementSetScalarColumnPlan],
    ) -> Result<Self, MeasurementSetWriteError> {
        let mut writers = Self::default();
        for column in columns {
            let writer = StreamingScalarColumnWriter::create(
                output.join(format!(".casa-rs.{}.scalar.tmp", column.name)),
                row_count,
                column.value_type,
            )
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            writers
                .index
                .insert(column.name.clone(), writers.writers.len());
            writers.writers.push(Some(writer));
        }
        Ok(writers)
    }

    /// Whether every writer has been taken for finalization.
    pub(super) fn is_empty(&self) -> bool {
        self.writers.iter().all(Option::is_none)
    }

    /// Take a column's writer to finish it.
    pub(super) fn take(&mut self, column: &str) -> Option<StreamingScalarColumnWriter> {
        let index = *self.index.get(column)?;
        self.writers[index].take()
    }

    fn id(&self, column: &str) -> Result<ScalarColumnId, MeasurementSetWriteError> {
        self.index
            .get(column)
            .map(|&index| ScalarColumnId(index))
            .ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "column {column} is not owned by this scalar creation session"
                ))
            })
    }

    fn writer_mut(
        &mut self,
        column: ScalarColumnId,
    ) -> Result<&mut StreamingScalarColumnWriter, MeasurementSetWriteError> {
        self.writers
            .get_mut(column.0)
            .and_then(Option::as_mut)
            .ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(
                    "scalar column is not open in this creation session".to_string(),
                )
            })
    }
}

fn column_error(error: casa_tables::TableError) -> MeasurementSetWriteError {
    MeasurementSetWriteError::Column(error.to_string())
}

impl MeasurementSetWriteSession {
    /// Resolve a scalar column of this creation session, once, for the
    /// columnar pushes.
    ///
    /// # Errors
    ///
    /// A mutation session, or a column the plan does not create.
    pub fn scalar_column(&self, column: &str) -> Result<ScalarColumnId, MeasurementSetWriteError> {
        match &self.state {
            MeasurementSetWriteSessionState::Creation { scalar_writers, .. }
            | MeasurementSetWriteSessionState::VariableArrayCreation { scalar_writers, .. } => {
                scalar_writers.id(column)
            }
            MeasurementSetWriteSessionState::Mutation { .. } => Err(not_creation()),
        }
    }

    /// Append a run of defined cells to one scalar column, in output row
    /// order.
    ///
    /// # Errors
    ///
    /// Values of another type than the column's, more rows than planned, or
    /// a write failure.
    pub fn push_scalar_column(
        &mut self,
        column: ScalarColumnId,
        values: StreamedScalarValues<'_>,
    ) -> Result<(), MeasurementSetWriteError> {
        self.scalar_writers_mut()?
            .writer_mut(column)?
            .push_values(values)
            .map_err(column_error)
    }

    /// Append `count` copies of one cell (`None` for undefined cells) to a
    /// scalar column, as for a column that is constant over a batch.
    ///
    /// # Errors
    ///
    /// A value of another type than the column's, more rows than planned, or
    /// a write failure.
    pub fn push_repeated_scalar(
        &mut self,
        column: ScalarColumnId,
        value: Option<ScalarValue>,
        count: usize,
    ) -> Result<(), MeasurementSetWriteError> {
        self.scalar_writers_mut()?
            .writer_mut(column)?
            .push_repeated(value, count)
            .map_err(column_error)
    }

    /// Append one fixed-width scalar cell in output row order.
    #[doc(hidden)]
    pub fn push_scalar_row(
        &mut self,
        column: &str,
        value: Option<ScalarValue>,
    ) -> Result<(), MeasurementSetWriteError> {
        let writers = self.scalar_writers_mut()?;
        let id = writers.id(column)?;
        writers.writer_mut(id)?.push(value).map_err(column_error)
    }

    fn scalar_writers_mut(&mut self) -> Result<&mut ScalarWriters, MeasurementSetWriteError> {
        match &mut self.state {
            MeasurementSetWriteSessionState::Creation { scalar_writers, .. }
            | MeasurementSetWriteSessionState::VariableArrayCreation { scalar_writers, .. } => {
                Ok(scalar_writers)
            }
            MeasurementSetWriteSessionState::Mutation { .. } => Err(not_creation()),
        }
    }
}

fn not_creation() -> MeasurementSetWriteError {
    MeasurementSetWriteError::InvalidPlan("scalar cells require a creation session".to_string())
}
