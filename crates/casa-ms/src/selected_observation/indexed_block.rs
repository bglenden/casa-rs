// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{error::Error, mem::size_of, ops::Range};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, SelectedObservationRunChannel,
    SelectedObservationRunCorrelation, SelectedObservationRunRow, SelectedObservationSampleView,
    SelectedSpectralEvaluation,
};

use crate::derived::engine::MsCalEngine;

use super::{
    BoundObservationSourceError, SelectedObservationTraversalError,
    SelectedObservationTraversalRun,
    access::{BlockVisitError, SelectedRowSpectralSelection},
    maximum_selected_correlations,
    spectral_evaluation::SpectralEvaluationProjector,
};

/// Admission bound for one reusable index of complete selected row/channel runs.
#[derive(Clone, Copy, Debug)]
pub struct SelectedObservationBlockIndexPlan {
    rows: usize,
    runs: usize,
    correlations: usize,
    capacity_bytes: usize,
}

impl SelectedObservationBlockIndexPlan {
    /// Plan a bounded window independently of the physical source-block size.
    pub fn new(
        problem: &CompiledProblem,
        maximum_runs: usize,
    ) -> Result<Self, BoundObservationSourceError> {
        let overflow = || BoundObservationSourceError::MeasurementOverflow;
        let minimum_channels = problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .flat_map(|source| source.selection().spectral_windows())
            .map(|selection| selection.channel_indices().len())
            .min()
            .unwrap_or(1)
            .max(1);
        let runs = maximum_runs;
        // A contiguous window may start partway through its first row.
        let rows = runs.min(
            runs.div_ceil(minimum_channels)
                .checked_add(1)
                .ok_or_else(overflow)?,
        );
        let correlations = runs
            .checked_mul(maximum_selected_correlations(problem))
            .ok_or_else(overflow)?;
        let capacity_bytes = index_bytes(rows, runs, correlations).ok_or_else(overflow)?;
        Ok(Self {
            rows,
            runs,
            correlations,
            capacity_bytes,
        })
    }

    /// Maximum number of complete row/channel runs in one indexed window.
    #[must_use]
    pub const fn maximum_runs(self) -> usize {
        self.runs
    }

    /// Bound a contiguous window by both run and row admission. A source spectral
    /// window can contain fewer channels per row than the original selection.
    pub fn next_range(
        self,
        block: &super::SelectedObservationBlock,
        start: usize,
    ) -> Result<Range<usize>, BoundObservationSourceError> {
        let count = block.selected_run_count()?;
        let channels = block.selected_channels_per_row()?;
        if start > count || channels == 0 || self.runs == 0 || self.rows == 0 {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        let row_bound = self.rows.saturating_mul(channels) - start % channels;
        Ok(start..start.saturating_add(self.runs.min(row_bound)).min(count))
    }

    /// Resident index bytes; row-domain payloads remain shared with the source block.
    #[must_use]
    pub const fn capacity_bytes(self) -> usize {
        self.capacity_bytes
    }

    /// Allocate reusable bounded storage after the caller admits its residency.
    #[must_use]
    pub fn create_index(self) -> SelectedObservationBlockIndex {
        SelectedObservationBlockIndex {
            plan: self,
            rows: Vec::with_capacity(self.rows),
            runs: Vec::with_capacity(self.runs),
            correlations: Vec::with_capacity(self.correlations),
            binding: None,
        }
    }
}

fn index_bytes(rows: usize, runs: usize, correlations: usize) -> Option<usize> {
    size_of::<SelectedObservationBlockIndex>()
        .checked_add(rows.checked_mul(size_of::<SelectedObservationRunRow>())?)?
        .checked_add(runs.checked_mul(size_of::<IndexedRun>())?)?
        .checked_add(correlations.checked_mul(size_of::<SelectedObservationRunCorrelation>())?)
}

struct IndexedRun {
    row: usize,
    channel: SelectedObservationRunChannel,
    correlations: Range<usize>,
}

/// Reusable source-block index. It holds each row once and compact correlation records,
/// never materialized selected samples or projected spectral evaluations.
pub struct SelectedObservationBlockIndex {
    plan: SelectedObservationBlockIndexPlan,
    rows: Vec<SelectedObservationRunRow>,
    runs: Vec<IndexedRun>,
    correlations: Vec<SelectedObservationRunCorrelation>,
    pub(super) binding: Option<(CompiledProblemId, (u64, u64, u64))>,
}

impl SelectedObservationBlockIndex {
    /// Populated bytes, excluding the source-owned shared row-domain payloads.
    pub fn current_bytes(&self) -> Result<usize, BoundObservationSourceError> {
        index_bytes(self.rows.len(), self.runs.len(), self.correlations.len())
            .ok_or(BoundObservationSourceError::MeasurementOverflow)
    }

    /// Allocated bytes, excluding the source-owned shared row-domain payloads.
    pub fn capacity_bytes(&self) -> Result<usize, BoundObservationSourceError> {
        index_bytes(
            self.rows.capacity(),
            self.runs.capacity(),
            self.correlations.capacity(),
        )
        .ok_or(BoundObservationSourceError::MeasurementOverflow)
    }

    /// Release row-domain references before the source block is refilled, retaining
    /// the index allocations. This keeps shared payloads within source residency.
    pub fn clear(&mut self) {
        self.binding = None;
        self.rows.clear();
        self.runs.clear();
        self.correlations.clear();
    }

    /// Borrow the inspected window with its unchanged source block. The owner,
    /// traversal and block ordinal bind the view without scanning its content.
    pub fn view<'a>(
        &'a self,
        block: &'a super::SelectedObservationBlock,
        problem: &'a CompiledProblem,
    ) -> Result<SelectedObservationIndexedBlock<'a>, BoundObservationSourceError> {
        if self.binding
            != block
                .index_binding
                .map(|binding| (problem.problem_id(), binding))
            || self.binding.is_none()
        {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        let (geometry_engine, selection) = block.projection_context()?;
        Ok(SelectedObservationIndexedBlock {
            problem,
            index: self,
            geometry_engine,
            selection,
        })
    }

    pub(super) fn push(
        &mut self,
        row: &SelectedObservationRunRow,
        channel: SelectedObservationRunChannel,
        correlations: &[SelectedObservationRunCorrelation],
    ) -> Result<(), BoundObservationSourceError> {
        let new_row = self
            .rows
            .last()
            .is_none_or(|last| last.physical_row != row.physical_row);
        if self.runs.len() == self.plan.runs
            || (new_row && self.rows.len() == self.plan.rows)
            || correlations.len()
                > self
                    .plan
                    .correlations
                    .saturating_sub(self.correlations.len())
        {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        if new_row {
            self.rows.push(row.clone());
        }
        let start = self.correlations.len();
        self.correlations.extend_from_slice(correlations);
        self.runs.push(IndexedRun {
            row: self.rows.len() - 1,
            channel,
            correlations: start..self.correlations.len(),
        });
        Ok(())
    }
}

/// Immutable, inspected view borrowing both the index and its source-owned geometry engine.
/// Its lifetime prevents source refill or index reuse until all projections finish.
pub struct SelectedObservationIndexedBlock<'a> {
    pub(super) problem: &'a CompiledProblem,
    pub(super) index: &'a SelectedObservationBlockIndex,
    pub(super) geometry_engine: &'a MsCalEngine,
    pub(super) selection: SelectedRowSpectralSelection,
}

impl SelectedObservationIndexedBlock<'_> {
    /// Number of complete runs available for disjoint range partitioning.
    #[must_use]
    pub fn run_count(&self) -> usize {
        self.index.runs.len()
    }

    /// Project one contiguous range using caller-owned scratch. No source inspection,
    /// generation proof, or source read is repeated here. Callback references are transient.
    pub fn visit_range<E: Error + 'static>(
        &self,
        projector: &mut SelectedObservationProjector,
        range: Range<usize>,
        mut consume: impl FnMut(SelectedObservationTraversalRun<'_>) -> Result<(), E>,
    ) -> Result<(), SelectedObservationTraversalError<E>> {
        let runs = self
            .index
            .runs
            .get(range)
            .ok_or(SelectedObservationTraversalError::Source(
                BoundObservationSourceError::StoredSampleShapeMismatch,
            ))?;
        for run in runs {
            let row = &self.index.rows[run.row];
            let correlations = &self.index.correlations[run.correlations.clone()];
            projector.evaluations.clear();
            if correlations.len() > projector.evaluations.capacity() {
                return Err(SelectedObservationTraversalError::Source(
                    BoundObservationSourceError::StoredSampleShapeMismatch,
                ));
            }
            for correlation in correlations {
                let sample =
                    SelectedObservationSampleView::from_run(row, &run.channel, correlation);
                projector.evaluations.push(
                    projector
                        .spectral_evaluator
                        .project(self.problem, sample, self.geometry_engine, self.selection)
                        .map_err(SelectedObservationTraversalError::Source)?
                        .spectral_evaluation(),
                );
            }
            consume(SelectedObservationTraversalRun::new(
                row,
                run.channel,
                correlations,
                &projector.evaluations,
            ))
            .map_err(SelectedObservationTraversalError::Consumer)?;
        }
        Ok(())
    }
}

/// Worker-local spectral caches and bounded evaluation scratch; scheduling stays with the caller.
pub struct SelectedObservationProjector {
    spectral_evaluator: SpectralEvaluationProjector,
    correlations: Vec<SelectedObservationRunCorrelation>,
    evaluations: Vec<SelectedSpectralEvaluation>,
}

impl SelectedObservationProjector {
    /// Allocate scratch for the compiled problem's maximum correlation group.
    #[must_use]
    pub fn new(problem: &CompiledProblem) -> Self {
        Self {
            spectral_evaluator: SpectralEvaluationProjector::new(),
            correlations: Vec::with_capacity(maximum_selected_correlations(problem)),
            evaluations: Vec::with_capacity(maximum_selected_correlations(problem)),
        }
    }

    /// Project a borrowed source range directly, using only worker-local bounded
    /// correlation and spectral scratch. No index, selected sample collection,
    /// source read, or generation inspection is created by this traversal.
    /// The caller retains the block until every disjoint worker range completes
    /// and inspects its runs once in canonical order through the source consumer.
    pub fn visit_block_range<E: Error + 'static>(
        &mut self,
        problem: &CompiledProblem,
        block: &super::SelectedObservationBlock,
        range: Range<usize>,
        mut consume: impl FnMut(SelectedObservationTraversalRun<'_>) -> Result<(), E>,
    ) -> Result<(), SelectedObservationTraversalError<E>> {
        if block.index_binding.is_none() {
            return Err(SelectedObservationTraversalError::Source(
                BoundObservationSourceError::StoredSampleShapeMismatch,
            ));
        }
        let spectral_evaluator = &mut self.spectral_evaluator;
        let evaluations = &mut self.evaluations;
        block
            .visit_selected_sample_range(
                problem,
                &mut self.correlations,
                range,
                |row, channel, correlations, geometry_engine, selection| {
                    evaluations.clear();
                    if correlations.len() > evaluations.capacity() {
                        return Err(SelectedObservationTraversalError::Source(
                            BoundObservationSourceError::StoredSampleShapeMismatch,
                        ));
                    }
                    for correlation in correlations {
                        let sample =
                            SelectedObservationSampleView::from_run(row, &channel, correlation);
                        evaluations.push(
                            spectral_evaluator
                                .project(problem, sample, geometry_engine, selection)
                                .map_err(SelectedObservationTraversalError::Source)?
                                .spectral_evaluation(),
                        );
                    }
                    consume(SelectedObservationTraversalRun::new(
                        row,
                        channel,
                        correlations,
                        evaluations,
                    ))
                    .map_err(SelectedObservationTraversalError::Consumer)
                },
            )
            .map_err(|error| match error {
                BlockVisitError::Source(error) => SelectedObservationTraversalError::Source(error),
                BlockVisitError::Consumer(error) => error,
            })
    }

    /// Correlation/evaluation scratch and inline projector bytes required before allocation.
    pub fn required_bytes(problem: &CompiledProblem) -> Option<usize> {
        maximum_selected_correlations(problem)
            .checked_mul(
                size_of::<SelectedObservationRunCorrelation>()
                    .checked_add(size_of::<SelectedSpectralEvaluation>())?,
            )?
            .checked_add(size_of::<Self>())
    }

    /// Correlation/evaluation scratch and inline projector bytes currently allocated.
    pub fn capacity_bytes(&self) -> Option<usize> {
        self.evaluations
            .capacity()
            .checked_mul(size_of::<SelectedSpectralEvaluation>())?
            .checked_add(
                self.correlations
                    .capacity()
                    .checked_mul(size_of::<SelectedObservationRunCorrelation>())?,
            )?
            .checked_add(size_of::<Self>())
    }
}
