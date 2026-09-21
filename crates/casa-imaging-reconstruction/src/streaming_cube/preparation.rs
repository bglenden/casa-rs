// SPDX-License-Identifier: LGPL-3.0-or-later

//! Whole-row native preparation on the admitted imaging team. Projection and
//! spectral evaluation feed the existing scalar weighting kernels here; exact
//! sums and source-coverage encoding stay with their worker until ordered join.
//! No prepared-sample collection or weighted replay allocation is constructed.

use std::io;

use super::*;
use crate::streaming_cube::input::{NativeBlock, NativeInput, NativeLayout};

const NATIVE_COVERAGE_DOMAIN: &[u8] = b"casa-rs-native-weighting-row-coverage";
const NATIVE_COVERAGE_VERSION: u32 = 1;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn natural_sum(
    problem: &CompiledProblem,
    plan: &WeightingPlan,
) -> io::Result<WeightingSumWeightPhase> {
    if !matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
        return Err(invalid("native preparation requires natural weighting"));
    }
    begin_weighting_generation(problem, plan)
        .and_then(|density| density.finish(problem))
        .map_err(io::Error::other)
}

fn sum_required_bytes(plan: &WeightingPlan) -> io::Result<usize> {
    exact_sum_capacity_bytes(plan.grid.output_planes)
        .and_then(|bytes| {
            bytes.checked_add(
                plan.grid
                    .output_planes
                    .checked_mul(size_of::<ExactF64Sum>())?,
            )
        })
        .and_then(|bytes| bytes.checked_add(plan.grid.planes.checked_mul(size_of::<f64>())?))
        .and_then(|bytes| bytes.checked_add(size_of::<WeightingSumWeightPhase>()))
        .ok_or_else(|| invalid("native sum-weight capacity overflow"))
}

/// Preparation completion owner. Whole rows are committed in canonical source
/// order; numerical sums merge exact integer bins, independent of scheduling.
pub struct NativeWeightingPreparation {
    sum: WeightingSumWeightPhase,
    coverage: Sha256,
    work: coverage::CoverageProofWork,
    previous_row: Option<u64>,
    rows: u64,
    failed: bool,
}

impl NativeWeightingPreparation {
    /// Begin the single natural-weighting payload traversal without allocating
    /// the historical weighted replay block.
    pub fn new(problem: &CompiledProblem, plan: &WeightingPlan) -> io::Result<Self> {
        let mut coverage = Sha256::new();
        coverage.update(NATIVE_COVERAGE_DOMAIN);
        coverage.update(NATIVE_COVERAGE_VERSION.to_be_bytes());
        Ok(Self {
            sum: natural_sum(problem, plan)?,
            coverage,
            work: coverage::CoverageProofWork {
                bytes: (NATIVE_COVERAGE_DOMAIN.len() + size_of::<u32>()) as u64,
                hash_calls: 2,
            },
            previous_row: None,
            rows: 0,
            failed: false,
        })
    }

    /// Required coordinator state, including the conservative exact-bin bound.
    pub fn coordinator_required_bytes(plan: &WeightingPlan) -> io::Result<usize> {
        sum_required_bytes(plan)?
            .checked_add(size_of::<Self>())
            .ok_or_else(|| invalid("native coordinator capacity overflow"))
    }

    /// Worker allocation bound. Runtime additionally admits its projector and
    /// spectral cache; those owners remain outside reconstruction.
    pub fn worker_required_bytes(
        problem: &CompiledProblem,
        plan: &WeightingPlan,
        maximum_rows: usize,
        channels: usize,
        correlations: usize,
    ) -> io::Result<usize> {
        if plan.problem != problem.problem_id()
            || plan.commitment != problem.weighting().commitment_id()
        {
            return Err(invalid(
                "native preparation plan belongs to another problem",
            ));
        }
        NativeBlock::required_bytes(maximum_rows, channels, correlations)?
            .checked_add(sum_required_bytes(plan)?)
            .and_then(|bytes| bytes.checked_add(channels.checked_mul(size_of::<u32>())?))
            .and_then(|bytes| bytes.checked_add(size_of::<NativeLayout>()))
            .and_then(|bytes| bytes.checked_add(maximum_rows.checked_mul(size_of::<[u8; 32]>())?))
            .and_then(|bytes| bytes.checked_add(size_of::<NativeInput>()))
            .and_then(|bytes| bytes.checked_add(size_of::<NativePreparationWorker>()))
            .ok_or_else(|| invalid("native preparation worker capacity overflow"))
    }

    /// Make one bounded worker. The shared source layout is small; payload
    /// ownership transfers into its flat buffer and never into sample objects.
    pub fn worker(
        &self,
        problem: &CompiledProblem,
        plan: &WeightingPlan,
        layout: NativeLayout,
        block: NativeBlock,
        finite_values: FiniteValuePolicy,
    ) -> io::Result<NativePreparationWorker> {
        if self.failed || self.sum.problem != problem.problem_id() {
            return Err(invalid("native weighting preparation failed or mismatched"));
        }
        let input = NativeInput::new(layout, block, finite_values)?;
        Ok(NativePreparationWorker {
            sum: natural_sum(problem, plan)?,
            row_digests: Vec::with_capacity(input.maximum_rows()),
            input,
            row_coverage: CoverageEncoder::new(),
            work: coverage::CoverageProofWork {
                bytes: 0,
                hash_calls: 0,
            },
            batch_open: false,
            batch_finished: false,
            failed: false,
        })
    }

    /// Join a completed worker in source order, merging exact bins and row
    /// digests only. The native payload remains borrowed for the storage sink.
    pub fn commit<'a>(
        &mut self,
        worker: &'a mut NativePreparationWorker,
    ) -> io::Result<&'a NativeBlock> {
        let result = self.commit_inner(worker);
        if result.is_err() {
            self.failed = true;
            worker.failed = true;
        }
        result?;
        Ok(worker.block())
    }

    fn commit_inner(&mut self, worker: &mut NativePreparationWorker) -> io::Result<()> {
        if self.failed
            || worker.failed
            || !worker.batch_finished
            || self.sum.problem != worker.sum.problem
            || self.sum.commitment != worker.sum.commitment
            || self.sum.sum_weights.len() != worker.sum.sum_weights.len()
        {
            return Err(invalid("unready or mismatched native preparation worker"));
        }
        let block = worker.block();
        if block.metadata.len() != worker.row_digests.len()
            || worker.sum.sum_sample_count != block.values.len() as u64
        {
            return Err(invalid("native preparation row or sample count mismatch"));
        }
        for (row, digest) in block.metadata.iter().zip(&worker.row_digests) {
            if self
                .previous_row
                .is_some_and(|previous| row.physical_row <= previous)
            {
                return Err(invalid(
                    "native preparation rows committed out of source order",
                ));
            }
            self.coverage.update([1]);
            self.coverage.update(row.physical_row.to_be_bytes());
            self.coverage.update(digest);
            self.work.bytes += 1 + 8 + 32;
            self.work.hash_calls += 3;
            self.previous_row = Some(row.physical_row);
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| invalid("native row count overflow"))?;
        }
        for (sum, partial) in self
            .sum
            .sum_weights
            .iter_mut()
            .zip(&mut worker.sum.sum_weights)
        {
            sum.merge(std::mem::take(partial))
                .map_err(io::Error::other)?;
        }
        self.sum.sum_sample_count = self
            .sum
            .sum_sample_count
            .checked_add(worker.sum.sum_sample_count)
            .ok_or_else(|| invalid("native sample count overflow"))?;
        self.work.bytes = self
            .work
            .bytes
            .checked_add(worker.work.bytes)
            .ok_or_else(|| invalid("native coverage byte count overflow"))?;
        self.work.hash_calls = self
            .work
            .hash_calls
            .checked_add(worker.work.hash_calls)
            .ok_or_else(|| invalid("native coverage call count overflow"))?;
        worker.sum.sum_sample_count = 0;
        if let Some(rows) = &mut worker.sum.cube_rows {
            *rows = CubeWeightRows::new();
        }
        worker.row_digests.clear();
        worker.work = coverage::CoverageProofWork {
            bytes: 0,
            hash_calls: 0,
        };
        worker.input.commit_batch();
        worker.batch_open = false;
        worker.batch_finished = false;
        Ok(())
    }

    /// Finish algorithmic state and native source coverage. Source traversal
    /// completion and its run association must still be supplied by the runtime.
    pub fn finish(mut self) -> io::Result<(WeightingAlgorithmState, WeightingReplaySummary)> {
        if self.failed || self.rows == 0 {
            return Err(invalid("failed or empty native weighting preparation"));
        }
        self.sum.density_sample_count = self.sum.sum_sample_count;
        let sample_count = self.sum.sum_sample_count;
        let state = self.sum.finish().map_err(io::Error::other)?;
        state.next_replay.store(1, Ordering::Relaxed);
        self.coverage.update([2]);
        self.coverage.update(self.rows.to_be_bytes());
        self.coverage.update(sample_count.to_be_bytes());
        self.coverage.update(state.generation_id.as_bytes());
        self.work.bytes += 1 + 8 + 8 + 32;
        self.work.hash_calls += 4;
        let coverage = WeightingReplayCoverageId(LogicalIdentity::from_sha256(
            self.coverage.finalize().into(),
        ));
        // Rows, unlike buffer batches, are canonical across worker counts.
        let block_count = self.rows;
        let replay_id =
            replay_identity(state.generation_id, coverage, sample_count, block_count, 0);
        let mut residency = state.generation_residency;
        residency.weighted_block_bytes = 0;
        residency.weighted_sample_bytes = 0;
        residency.simultaneous_selected_weighted_bytes = 0;
        let summary = WeightingReplaySummary {
            replay_id,
            generation: state.generation_id,
            coverage,
            sample_count,
            block_count,
            replay_sequence: 0,
            coverage_proof_bytes: self.work.bytes,
            coverage_proof_hash_calls: self.work.hash_calls,
            residency,
        };
        Ok((state, summary))
    }
}

/// Reusable flat native buffer plus exact sums for a disjoint whole-row range.
pub struct NativePreparationWorker {
    sum: WeightingSumWeightPhase,
    input: NativeInput,
    row_digests: Vec<[u8; 32]>,
    row_coverage: CoverageEncoder,
    work: coverage::CoverageProofWork,
    batch_open: bool,
    batch_finished: bool,
    failed: bool,
}

impl NativePreparationWorker {
    /// Begin filling after the preceding native block was synchronously written.
    pub fn begin_batch(&mut self) -> io::Result<()> {
        if self.failed || self.batch_open || self.batch_finished {
            return Err(invalid("native preparation worker batch already open"));
        }
        self.input.begin_batch()?;
        self.batch_open = true;
        Ok(())
    }

    /// Apply the shared science and immediately pack the result. The selected
    /// view is borrowed, and the sole weighted value dies when this call ends.
    pub fn consume<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: impl Into<SelectedObservationSampleView<'a>>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> io::Result<()> {
        let result: io::Result<()> = (|| {
            if self.failed || !self.batch_open || self.batch_finished {
                return Err(invalid(
                    "native preparation worker is not accepting samples",
                ));
            }
            let weighted = self
                .sum
                .prepare_sample(
                    problem,
                    sample.into(),
                    output_frame_frequency_hz,
                    contributions,
                )
                .map_err(io::Error::other)?;
            self.input.push_sample(&weighted)?;
            self.sum
                .accumulate_prepared(problem, &weighted)
                .map_err(io::Error::other)?;
            self.row_coverage.push(&weighted);
            if self.sum.sum_sample_count % self.input.row_samples() as u64 == 0 {
                let encoder = std::mem::replace(&mut self.row_coverage, CoverageEncoder::new());
                let (digest, work) = encoder.finish_row(self.input.row_samples() as u64);
                self.row_digests.push(digest);
                self.work.bytes += work.bytes;
                self.work.hash_calls += work.hash_calls;
            }
            Ok(())
        })();
        self.failed = result.is_err();
        result
    }

    /// Finish a nonempty whole-row batch, including a shorter terminal batch.
    pub fn finish_batch(&mut self) -> io::Result<&NativeBlock> {
        if self.failed || !self.batch_open || self.batch_finished {
            return Err(invalid("native preparation worker cannot finish its batch"));
        }
        let result: io::Result<()> = (|| {
            if let Some(rows) = &mut self.sum.cube_rows {
                rows.cursor.finish().map_err(io::Error::other)?;
            }
            self.input.finish_batch()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result?;
        self.batch_finished = true;
        Ok(self.block())
    }

    /// Borrow the native arrays until the next admitted refill begins.
    pub fn block(&self) -> &NativeBlock {
        self.input.block()
    }

    /// Shared source axes for the retained native store.
    pub fn layout(&self) -> &NativeLayout {
        self.input.layout()
    }
}
