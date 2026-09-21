// SPDX-License-Identifier: LGPL-3.0-or-later

//! Whole-row native preparation on the admitted imaging team. Projection and
//! spectral evaluation feed the shared weighting primitives here; exact
//! sums and source-coverage encoding stay with their worker until ordered join.
//! No prepared-sample collection or weighted replay allocation is constructed.

use std::io;

use super::*;
use crate::spectral_operator::accept_polarization_value;
use crate::streaming_cube::input::{NativeBlock, NativeLayout, RowMetadata};
use casa_imaging_model::{
    FrequencyFrame, SelectedObservationRunChannel, SelectedObservationRunCorrelation,
    SelectedObservationRunRow, SelectedRowSpectralGeometry,
};
use num_complex::Complex64;

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
        let maximum_rows = block.metadata.len();
        block.validate_shape(
            maximum_rows,
            layout.channels.len(),
            layout.correlations.len(),
        )?;
        let sum = natural_sum(problem, plan)?;
        Ok(NativePreparationWorker {
            sum,
            row_digests: Vec::with_capacity(maximum_rows),
            block,
            layout,
            maximum_rows,
            channel: 0,
            previous_row: None,
            finite_values,
            output_frame: problem.geometry().spectral().output_frame(),
            taper: problem.weighting().uv_taper(),
            maximum_terms: maximum_spectral_terms(problem),
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
    block: NativeBlock,
    layout: NativeLayout,
    maximum_rows: usize,
    channel: usize,
    previous_row: Option<u64>,
    finite_values: FiniteValuePolicy,
    output_frame: FrequencyFrame,
    taper: Option<UvTaper>,
    maximum_terms: usize,
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
        self.block
            .set_shape(self.maximum_rows, self.layout.channels.len())?;
        self.batch_open = true;
        Ok(())
    }

    /// Prepare one borrowed row/channel correlation slice directly into the
    /// worker's arrays. Row geometry is installed once; group weighting and
    /// spectral values are computed once per channel, with per-value validation.
    pub fn consume_channel(
        &mut self,
        row: &SelectedObservationRunRow,
        channel: SelectedObservationRunChannel,
        correlations: &[SelectedObservationRunCorrelation],
        geometry: SelectedRowSpectralGeometry,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> io::Result<()> {
        let result: io::Result<()> = (|| {
            if self.failed || !self.batch_open || self.batch_finished {
                return Err(invalid(
                    "native preparation worker is not accepting channels",
                ));
            }
            let row_index = self.row_digests.len();
            if row_index == self.maximum_rows
                || correlations.len() != self.layout.correlations.len()
                || channel.channel_index != self.layout.channels[self.channel]
                || contributions.len() > self.maximum_terms
                || !output_frame_frequency_hz.is_finite()
                || output_frame_frequency_hz <= 0.0
            {
                return Err(invalid("native channel exceeds shape or stencil bound"));
            }
            let first = &correlations[0];
            let view = SelectedObservationSampleView::from_run(row, &channel, first);
            let address = view.address();
            if !self.layout.contains_source(address) {
                return Err(invalid("native source identity mismatch"));
            }
            if !geometry.matches_sample(view, self.output_frame)
                || [Some(geometry.first()), geometry.second()]
                    .into_iter()
                    .flatten()
                    .any(|(index, hz)| {
                        index == channel.channel_index
                            && hz.to_bits() != output_frame_frequency_hz.to_bits()
                    })
            {
                return Err(io::Error::other(
                    WeightingError::RowSpectralGeometryMismatch,
                ));
            }
            let native_geometry = NativeRowSpectralGeometry {
                channels: geometry.selected_channels(),
                first: geometry.first(),
                second: geometry.second(),
                lattice_first_pair_hz: geometry.lattice_first_pair_hz(),
            };
            if self.channel == 0 {
                if self
                    .previous_row
                    .is_some_and(|previous| row.physical_row <= previous)
                    || native_geometry.channels != self.block.channels
                    || native_geometry.first.0 != self.layout.channels[0]
                    || native_geometry.second.map(|pair| pair.0) != Some(self.layout.channels[1])
                    || row.domain_projections.len() != 1
                {
                    return Err(invalid("native row order or geometry mismatch"));
                }
                let projection = row
                    .domain_projections
                    .get(0)
                    .ok_or_else(|| invalid("native primary projection missing"))?
                    .model();
                self.block.metadata[row_index] = RowMetadata {
                    physical_row: row.physical_row,
                    uvw_m: projection.transformed_uvw_m(),
                    phase_shift_m: projection.phase_shift_m(),
                    original_pair_hz: native_geometry
                        .first_pair_hz()
                        .ok_or_else(|| invalid("missing native frequency pair"))?,
                };
                self.previous_row = Some(row.physical_row);
            } else if self.previous_row != Some(row.physical_row) {
                return Err(invalid("native row ended before its selected channels"));
            }
            let group_flag = correlations.iter().any(|value| value.channel_flag);
            let input = casa_unpolarized_input_weight(SelectedInputWeightGroup::correlation_run(
                first.input_weight,
                correlations.last().expect("nonempty layout").input_weight,
                correlations.len(),
            ));
            let input_value = input_weight_value(
                input,
                group_flag
                    || row.row_flag
                    || correlations
                        .iter()
                        .all(|value| value.parallel_hand_group_flag),
                self.finite_values,
            )
            .map_err(io::Error::other)?;
            let natural_weight = |frequency_hz| -> io::Result<f64> {
                if input_value == 0.0 {
                    Ok(0.0)
                } else {
                    let uv = if self.taper.is_some() {
                        uv_lambda_for_coordinates(row.coordinates.density_uvw_m, frequency_hz)
                    } else {
                        [0.0; 2]
                    };
                    apply_weight_taper(input_value, self.taper, uv).map_err(io::Error::other)
                }
            };
            let base_weight = natural_weight(output_frame_frequency_hz)?;
            let cell = row_index * self.block.channels + self.channel;
            self.block.frequencies_hz[cell] = output_frame_frequency_hz;
            let start = cell * correlations.len();
            let end = start + correlations.len();
            let spectral_values: SmallVec<[WeightingSpectralValue; 4]> = contributions
                .iter()
                .map(|contribution| {
                    Ok(WeightingSpectralValue {
                        contribution,
                        imaging_weight: natural_weight(contribution.evaluation_frequency_hz())?,
                    })
                })
                .collect::<io::Result<_>>()?;
            let flagged_spectral_values: SmallVec<[WeightingSpectralValue; 4]> = if correlations
                .iter()
                .any(|sample| sample.parallel_hand_group_flag)
            {
                spectral_values
                    .iter()
                    .map(|value| WeightingSpectralValue {
                        contribution: value.contribution,
                        imaging_weight: 0.0,
                    })
                    .collect()
            } else {
                SmallVec::new()
            };
            for (ordinal, ((((sample, value), weight), flag), weight_flag)) in correlations
                .iter()
                .zip(&mut self.block.values[start..end])
                .zip(&mut self.block.weights[start..end])
                .zip(&mut self.block.flags[start..end])
                .zip(&mut self.block.weight_flags[start..end])
                .enumerate()
            {
                if (sample.correlation_index, sample.correlation_type)
                    != self.layout.correlations[ordinal]
                {
                    return Err(invalid("native correlation order mismatch"));
                }
                *value = match sample.visibility {
                    SelectedVisibilitySample::Float32(value) => {
                        Complex64::new(f64::from(value), 0.0)
                    }
                    SelectedVisibilitySample::Complex32([re, im]) => {
                        Complex64::new(f64::from(re), f64::from(im))
                    }
                };
                *weight = if sample.parallel_hand_group_flag {
                    0.0
                } else {
                    base_weight
                };
                *flag = !accept_polarization_value(
                    sample.visibility,
                    sample.input_weight,
                    row.row_flag || sample.channel_flag,
                    self.finite_values,
                )
                .map_err(io::Error::other)?;
                *weight_flag = group_flag || sample.parallel_hand_group_flag || row.row_flag;
                let spectral = if sample.parallel_hand_group_flag {
                    flagged_spectral_values.as_slice()
                } else {
                    spectral_values.as_slice()
                };
                if let Some(value) = spectral.first() {
                    self.sum.sum_weights[0]
                        .add(value.imaging_weight)
                        .map_err(io::Error::other)?;
                }
                let mut member_address = address;
                member_address.correlation_index = sample.correlation_index;
                member_address.correlation_type = sample.correlation_type;
                self.row_coverage.push_parts(
                    member_address,
                    Some(native_geometry),
                    output_frame_frequency_hz,
                    spectral,
                );
            }
            self.sum.sum_sample_count = self
                .sum
                .sum_sample_count
                .checked_add(correlations.len() as u64)
                .ok_or_else(|| invalid("native sample count overflow"))?;
            self.channel += 1;
            if self.channel == self.block.channels {
                let encoder = std::mem::replace(&mut self.row_coverage, CoverageEncoder::new());
                let (digest, work) =
                    encoder.finish_row((self.block.channels * self.block.correlations) as u64);
                self.row_digests.push(digest);
                self.work.bytes += work.bytes;
                self.work.hash_calls += work.hash_calls;
                self.channel = 0;
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
            if self.channel != 0 || self.row_digests.is_empty() {
                return Err(invalid("empty or incomplete native batch"));
            }
            self.block
                .set_shape(self.row_digests.len(), self.layout.channels.len())?;
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
        &self.block
    }

    /// Shared source axes for the retained native store.
    pub fn layout(&self) -> &NativeLayout {
        &self.layout
    }
}
