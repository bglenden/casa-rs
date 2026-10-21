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

use num_complex::Complex32;

use super::{
    MeasurementSetWriteBatch, MeasurementSetWriteError, MeasurementSetWriteSession,
    MeasurementSetWriteSessionState,
};

/// One batch of standard visibility rows, column by column, for
/// [`MeasurementSetWriteSession::send_visibility_columns`].
///
/// WEIGHT and SIGMA are 1 and FLAG_CATEGORY is empty in every row. The write
/// plan already charges a per-row WEIGHT and SIGMA, should a producer need
/// them as further columns here.
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
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.flag_row.clear();
        self.uvw.clear();
    }

    /// Check that every column holds the batch's rows of `samples`-sample
    /// cells.
    pub(super) fn validate(&self, samples: usize) -> Result<(), MeasurementSetWriteError> {
        let rows = self.row_count();
        if self.uvw.len() != rows || Some(self.data.len()) != rows.checked_mul(samples) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "batch columns differ: FLAG_ROW has {rows} rows, UVW {}, and DATA {} samples \
                 where {rows} rows of {samples} need {}",
                self.uvw.len(),
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

/// Hand a written batch's buffers back to the producer, unless one is
/// already waiting there or the producer has gone; then they are freed.
pub(super) fn recycle(returns: &mpsc::SyncSender<VisibilityColumns>, mut batch: VisibilityColumns) {
    batch.clear();
    let _ = returns.try_send(batch);
}
