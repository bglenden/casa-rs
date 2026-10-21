// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column-shaped batches of standard visibility rows.
//!
//! A producer fills one [`VisibilityColumns`] per batch, a column at a time,
//! and hands it to the creation session's background writer. The writer
//! hands each batch's buffers back once it has written them, so a producer
//! that takes its batches from [`MeasurementSetWriteSession::visibility_columns`]
//! allocates them once for the whole write. Then at most the writer's queue
//! and two batches are live: the producer's, and the one being written or
//! handed back. A producer that builds its own batches leaves one handed-back
//! batch waiting, one more.

use std::sync::mpsc;

use casa_tables::StreamingTiledPrimitiveWriter;
use num_complex::Complex32;

use super::{
    MeasurementSetWriteBatch, MeasurementSetWriteError, MeasurementSetWriteSession,
    MeasurementSetWriteSessionState,
};

/// One batch of standard visibility rows, column by column, for
/// [`MeasurementSetWriteSession::send_visibility_columns`].
///
/// FLAG_CATEGORY is empty in every row.
#[derive(Debug, Default)]
pub struct VisibilityColumns {
    /// Each row's DATA cell, `correlations × channels` samples with the
    /// correlation fastest (casacore's cell order), rows back to back.
    pub data: Vec<Complex32>,
    /// One value per row, which every sample of the row's FLAG cell takes,
    /// as a simulator flags whole rows.
    pub flag_row: Vec<bool>,
    /// One coordinate triple per row, in metres.
    pub uvw: Vec<[f64; 3]>,
    /// One value per row, which every correlation of the row's WEIGHT cell
    /// takes; empty for a weight of 1 in every row.
    pub weight: Vec<f32>,
    /// One value per row, which every correlation of the row's SIGMA cell
    /// takes; empty for a sigma of 1 in every row.
    pub sigma: Vec<f32>,
}

impl VisibilityColumns {
    /// Rows in the batch, one per `flag_row` entry.
    pub fn row_count(&self) -> usize {
        self.flag_row.len()
    }

    /// An empty batch that holds `rows` rows of `samples`-sample cells
    /// without reallocating.
    fn with_capacity(rows: usize, samples: usize) -> Self {
        Self {
            data: Vec::with_capacity(rows.saturating_mul(samples)),
            flag_row: Vec::with_capacity(rows),
            uvw: Vec::with_capacity(rows),
            weight: Vec::with_capacity(rows),
            sigma: Vec::with_capacity(rows),
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.flag_row.clear();
        self.uvw.clear();
        self.weight.clear();
        self.sigma.clear();
    }

    /// Check that every column holds the batch's rows of `samples`-sample
    /// cells.
    pub(super) fn validate(&self, samples: usize) -> Result<(), MeasurementSetWriteError> {
        let rows = self.row_count();
        let per_row_or_empty = |len: usize| len == rows || len == 0;
        if self.uvw.len() != rows
            || Some(self.data.len()) != rows.checked_mul(samples)
            || !per_row_or_empty(self.weight.len())
            || !per_row_or_empty(self.sigma.len())
        {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "batch columns differ: FLAG_ROW has {rows} rows, UVW {}, WEIGHT {}, SIGMA {}, \
                 and DATA {} samples where {rows} rows of {samples} need {}",
                self.uvw.len(),
                self.weight.len(),
                self.sigma.len(),
                self.data.len(),
                rows.saturating_mul(samples)
            )));
        }
        Ok(())
    }
}

impl MeasurementSetWriteSession {
    /// An empty batch for [`Self::send_visibility_columns`], with room for
    /// the plan's batch rows. It holds the buffers of a batch the writer has
    /// finished with, when there is one.
    pub fn visibility_columns(&self) -> VisibilityColumns {
        if let MeasurementSetWriteSessionState::Creation { recycled, .. } = &self.state
            && let Ok(batch) = recycled.try_recv()
        {
            return batch;
        }
        let samples = self
            .plan
            .correlation_count
            .saturating_mul(self.plan.channel_count);
        VisibilityColumns::with_capacity(self.plan.batch_rows, samples)
    }

    /// Queue a batch of whole rows, in output row order, for the background
    /// writer. Blocks while the writer's queue is full.
    ///
    /// # Errors
    ///
    /// A session that is not a visibility creation session, columns of
    /// different lengths or the wrong cell size, more rows than a batch or
    /// the plan allows, or a writer that has stopped.
    pub fn send_visibility_columns(
        &self,
        batch: VisibilityColumns,
    ) -> Result<(), MeasurementSetWriteError> {
        self.send_batch(MeasurementSetWriteBatch::Columns(batch))
    }
}

/// Write `row_count` WEIGHT or SIGMA cells: each row's value in every
/// correlation, or `unit` (every correlation 1) in every row when `per_row`
/// is empty.
pub(super) fn push_per_row_cells(
    writer: &mut StreamingTiledPrimitiveWriter,
    per_row: &[f32],
    unit: &[f32],
    row_count: usize,
) -> Result<(), MeasurementSetWriteError> {
    let column_error =
        |error: casa_tables::StorageError| MeasurementSetWriteError::Column(error.to_string());
    if per_row.is_empty() {
        return writer
            .push_repeated_f32_row(unit, row_count)
            .map_err(column_error);
    }
    let mut cell = vec![0.0f32; unit.len()];
    for &value in per_row {
        cell.fill(value);
        writer.push_f32_row(&cell).map_err(column_error)?;
    }
    Ok(())
}

/// Hand a written batch's buffers back to the producer, unless one is
/// already waiting there or the producer has gone; then they are freed.
pub(super) fn recycle(returns: &mpsc::SyncSender<VisibilityColumns>, mut batch: VisibilityColumns) {
    batch.clear();
    let _ = returns.try_send(batch);
}
