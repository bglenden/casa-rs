// SPDX-License-Identifier: LGPL-3.0-or-later

//! Natural weighting over borrowed source columns. Source coverage belongs to
//! casa-ms's ordered block cursor; this module owns only numerical preparation.

use super::*;
use casa_imaging_model::{
    ReconstructionBasis, SelectedNumericRow, SelectedNumericVisibility, SelectedNumericWeights,
};

/// Row-independent natural-weighting setup and its initial numerical reduction.
pub struct BulkNaturalWeighting {
    sum: Option<WeightingSumWeightPhase>,
    finite: FiniteValuePolicy,
    centres: Vec<f64>,
    boundaries: Vec<f64>,
    constant: bool,
    samples: u64,
    rows: u64,
}

/// Reusable selected-channel/correlation outputs. Values remain source-owned;
/// these arrays contain only flags and the effective numerical weights.
pub struct NaturalRowPreparation {
    weights: Vec<f32>,
    flags: Vec<bool>,
    weight_flags: Vec<bool>,
}

impl NaturalRowPreparation {
    /// Allocate the exact admitted row shape once, then reuse it across rows.
    pub fn new(rows: usize, channels: usize, correlations: usize) -> Result<Self, WeightingError> {
        let capacity = rows
            .checked_mul(channels)
            .and_then(|n| n.checked_mul(correlations))
            .filter(|&n| n > 0 && n <= isize::MAX as usize / 6)
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok(Self {
            weights: vec![0.0; capacity],
            flags: vec![false; capacity],
            weight_flags: vec![false; capacity],
        })
    }

    /// Effective natural weights in selected channel/correlation order.
    pub fn weights(&self) -> &[f32] {
        &self.weights
    }
    /// Numerical input rejection flags.
    pub fn flags(&self) -> &[bool] {
        &self.flags
    }
    /// CASA complete-correlation weight-group flags.
    pub fn weight_flags(&self) -> &[bool] {
        &self.weight_flags
    }

    /// Borrow all three flat outputs for disjoint row-chunk preparation.
    pub fn buffers_mut(&mut self) -> (&mut [f32], &mut [bool], &mut [bool]) {
        (&mut self.weights, &mut self.flags, &mut self.weight_flags)
    }
}

impl BulkNaturalWeighting {
    /// Associate a subsequent exact source traversal with the retained weights.
    /// The runtime must validate the fresh source completion against this proof.
    pub fn replay_summary(
        state: &WeightingAlgorithmState,
        proof: FrozenWeightingCoverageProof,
        rows: u64,
    ) -> Result<WeightingReplaySummary, WeightingError> {
        if proof.generation != state.generation_id
            || proof.problem != state.problem
            || proof.commitment != state.commitment
            || proof.weighted_sample_count != state.sample_count
            || rows == 0
        {
            return Err(WeightingError::CoverageMismatch);
        }
        let sequence = state.next_replay.fetch_add(1, Ordering::Relaxed);
        let replay_id = replay_identity(
            state.generation_id,
            proof.coverage,
            state.sample_count,
            rows,
            sequence,
        );
        Ok(WeightingReplaySummary {
            replay_id,
            generation: state.generation_id,
            coverage: proof.coverage,
            sample_count: state.sample_count,
            block_count: rows,
            replay_sequence: sequence,
            coverage_proof_bytes: 0,
            coverage_proof_hash_calls: 0,
            residency: state.generation_residency,
        })
    }
    /// Begin the ordinary constant-MFS or linear-cube natural-weighting route.
    pub fn new(
        problem: &CompiledProblem,
        plan: &WeightingPlan,
        initial: bool,
    ) -> Result<Self, WeightingError> {
        if problem.weighting().scheme() != WeightingScheme::Natural
            || problem.weighting().uv_taper().is_some()
            || problem.visibility_transform().is_some()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let constant = matches!(
            problem.reconstruction().basis(),
            ReconstructionBasis::Constant
        );
        if !constant
            && (!matches!(
                problem.reconstruction().basis(),
                ReconstructionBasis::ChannelLocal { .. }
            ) || problem.science().spectral().sampling().kernel()
                != casa_imaging_model::SpectralKernel::Linear)
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let axis = problem.geometry().spectral();
        let centres = (0..axis.output_channels())
            .map(|i| {
                axis.channel_centre_hz(i)
                    .ok_or(WeightingError::ProblemMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let boundaries = (0..=axis.output_channels())
            .map(|i| {
                axis.channel_boundary_hz(i)
                    .ok_or(WeightingError::ProblemMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            sum: initial
                .then(|| begin_weighting_generation(problem, plan)?.finish(problem))
                .transpose()?,
            finite: problem.numerics().finite_values(),
            centres,
            boundaries,
            constant,
            samples: 0,
            rows: 0,
        })
    }

    /// Apply the existing CASA natural-weight/flag rules in a tight row loop.
    /// `initial` controls only the once-per-source reduction, never the science.
    pub fn prepare_row(
        &self,
        row: SelectedNumericRow<'_>,
        frequencies: &[f64],
        boundaries: &[[f64; 2]],
        weights_out: &mut [f32],
        flags_out: &mut [bool],
        weight_flags_out: &mut [bool],
        initial: bool,
    ) -> Result<f64, WeightingError> {
        let correlations = row.correlations.len();
        let count = row
            .channels
            .len()
            .checked_mul(correlations)
            .ok_or(WeightingError::ResidencyOverflow)?;
        if !row.has_exact_shape()
            || weights_out.len() != count
            || flags_out.len() != count
            || weight_flags_out.len() != count
            || frequencies.len() != row.channels.len()
            || boundaries.len() != frequencies.len()
        {
            return Err(WeightingError::CoverageMismatch);
        }
        let mut sum = 0.0;
        for (channel, coordinate) in row.channels.iter().enumerate() {
            let source = (coordinate.channel_index - row.first_stored_channel) as usize
                * row.stored_correlations;
            let weights = match row.weights {
                SelectedNumericWeights::PerRow(values) => values,
                SelectedNumericWeights::PerChannel(values) => {
                    &values[source..source + row.stored_correlations]
                }
            };
            let group_flag = row
                .correlations
                .iter()
                .any(|product| row.flags[source + product.correlation_index() as usize]);
            let parallel_flag = row.correlations.iter().any(|product| {
                product.correlation_type().contributes_to_stokes_i()
                    && row.flags[source + product.correlation_index() as usize]
            });
            let input = casa_unpolarized_input_weight(SelectedInputWeightGroup::correlation_run(
                weights[row.correlations[0].correlation_index() as usize],
                weights[row.correlations[correlations - 1].correlation_index() as usize],
                correlations,
            ));
            let base = input_weight_value(input, group_flag || row.row.row_flag, self.finite)?;
            let mapped = initial
                && (self.constant
                    || !crate::spectral_sampling::linear_terms(
                        &self.centres,
                        &self.boundaries,
                        boundaries[channel],
                        frequencies[channel],
                    )
                    .is_empty());
            for (ordinal, product) in row.correlations.iter().enumerate() {
                let corr = product.correlation_index() as usize;
                let index = source + corr;
                let destination = channel * correlations + ordinal;
                let value = match row.visibility {
                    SelectedNumericVisibility::Float32(values) => {
                        SelectedVisibilitySample::Float32(values[index])
                    }
                    SelectedNumericVisibility::Complex32(values) => {
                        SelectedVisibilitySample::Complex32([values[index].re, values[index].im])
                    }
                };
                let rejected_group = if product.correlation_type().contributes_to_stokes_i() {
                    parallel_flag
                } else {
                    row.flags[index]
                };
                weights_out[destination] = if rejected_group { 0.0 } else { base as f32 };
                flags_out[destination] = !crate::spectral_operator::accept_polarization_value(
                    value,
                    weights[corr],
                    row.row.row_flag || row.flags[index],
                    self.finite,
                )
                .map_err(|_| WeightingError::CoverageMismatch)?;
                weight_flags_out[destination] = group_flag || rejected_group || row.row.row_flag;
                if mapped && !rejected_group {
                    sum += base;
                }
            }
        }
        Ok(sum)
    }

    /// Publish chunk-local discovery totals only after every chunk has joined.
    pub fn commit_prepared_chunk(
        &mut self,
        sum: f64,
        samples: u64,
        rows: u64,
    ) -> Result<(), WeightingError> {
        let next_samples = self
            .samples
            .checked_add(samples)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let next_rows = self
            .rows
            .checked_add(rows)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let phase = self.sum.as_mut().ok_or(WeightingError::CoverageMismatch)?;
        phase.sum_weights[0].add(sum)?;
        self.samples = next_samples;
        self.rows = next_rows;
        Ok(())
    }

    /// Bind the numerical reduction to a successfully completed exact source
    /// traversal. This versioned identity associates owners, not product content.
    pub fn finish(
        mut self,
        selected: SelectedObservationGenerationId,
        sample_count: u64,
    ) -> Result<
        (
            WeightingAlgorithmState,
            WeightingReplaySummary,
            FrozenWeightingCoverageProof,
        ),
        WeightingError,
    > {
        if self.samples == 0 || self.samples != sample_count {
            return Err(WeightingError::CoverageMismatch);
        }
        let mut sum = self.sum.take().ok_or(WeightingError::CoverageMismatch)?;
        sum.sum_sample_count = self.samples;
        sum.density_sample_count = self.samples;
        let state = sum.finish()?;
        state.next_replay.store(1, Ordering::Relaxed);
        let mut binding = Sha256::new();
        binding.update(b"casa-rs-ordered-source-weighting-binding-v1");
        binding.update(selected.as_bytes());
        binding.update(state.problem.as_bytes());
        binding.update(state.generation_id.as_bytes());
        binding.update(self.samples.to_le_bytes());
        binding.update(self.rows.to_le_bytes());
        let coverage =
            WeightingReplayCoverageId(LogicalIdentity::from_sha256(binding.finalize().into()));
        let replay_id = replay_identity(state.generation_id, coverage, self.samples, self.rows, 0);
        let mut residency = state.generation_residency;
        residency.weighted_block_bytes = 0;
        residency.weighted_sample_bytes = 0;
        residency.simultaneous_selected_weighted_bytes = 0;
        let summary = WeightingReplaySummary {
            replay_id,
            generation: state.generation_id,
            coverage,
            sample_count: self.samples,
            block_count: self.rows,
            replay_sequence: 0,
            coverage_proof_bytes: 0,
            coverage_proof_hash_calls: 0,
            residency,
        };
        let proof = FrozenWeightingCoverageProof {
            problem: state.problem,
            commitment: state.commitment,
            generation: state.generation_id,
            coverage,
            selected_generation: selected,
            selected_sample_count: sample_count,
            continuum_transform_generation: None,
            weighted_sample_count: self.samples,
        };
        Ok((state, summary, proof))
    }
}
