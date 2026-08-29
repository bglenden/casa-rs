// SPDX-License-Identifier: LGPL-3.0-or-later

//! Run-scoped, disk-streamable normal-operator replay for constant and channel-local bases.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    mem::size_of,
    sync::Arc,
};

use casa_imaging_model::{
    CompiledProblem, ContinuumTransformGenerationId, LogicalIdentity, ReconstructionBasis,
    SelectedObservationGenerationId,
};
use num_complex::Complex64;
use sha2::{Digest, Sha256};

use crate::{
    Encoder, FinalNormalState, ModelGeneration,
    spectral_operator::{
        CompleteDataOwnerCompletion, CompleteDataOwnerResult, OVERSAMPLING,
        PreparedSpectralOperator, SPEED_OF_LIGHT_M_PER_S, SUPPORT, SampleTaps,
        SpectralOperatorError, SpectralOperatorPass, SpectralOperatorSpecification,
        SpectralPrimitiveCatalog, SpectralSlabOperator, StandardConvolution, TapSpan,
        accept_weighted_input,
    },
    weighting::{
        CoverageEncoder, WeightingReplayChunk, WeightingReplayCoverageId, WeightingReplayId,
        WeightingReplaySummary,
    },
};

const RECORD_DOMAIN: &[u8] = b"casa-rs-gridded-normal-operator";
const RECORD_VERSION: u32 = 2;
const TAP_KEY_BITS: u32 = 38;
const TAP_KEY_MASK: u64 = (1_u64 << TAP_KEY_BITS) - 1;
const CHANNEL_KEY_BITS: u32 = 24;
const CHANNEL_KEY_MASK: u64 = (1_u64 << CHANNEL_KEY_BITS) - 1;
const GROUP_END_BIT: u64 = 1_u64 << (TAP_KEY_BITS + CHANNEL_KEY_BITS);
const RECORD_KEY_MASK: u64 = (GROUP_END_BIT << 1) - 1;

/// Width of every opaque gridded normal-operator record.
pub const GRIDDED_NORMAL_OPERATOR_RECORD_BYTES: usize = 32;

/// Stable maximum work fanout for one bounded gridded-normal block.
///
/// This is reconstruction-owned and independent of the admitted worker count;
/// changing schedules therefore cannot change work identities or reduction order.
pub const GRIDDED_NORMAL_MAXIMUM_PARTITIONS_PER_BLOCK: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReducedRecordKey {
    output_channel: u32,
    taps: u64,
    forward_real: u64,
    forward_imaginary: u64,
    imaging_weight: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DecodedRecord {
    output_channel: usize,
    taps: SampleTaps,
    forward_scale: Complex64,
    imaging_weight: f64,
    group_end: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockDescriptor {
    source_samples: u64,
    record_count: u64,
    digest: [u8; 32],
}

/// One deterministically reduced bounded block in the private fixed-width encoding.
///
/// Runtime may write [`Self::encoded_bytes`] verbatim. It must preserve the
/// block sequence and use the sealed program to validate bytes read back.
#[doc(hidden)]
#[derive(Debug)]
pub struct GriddedNormalOperatorBlock {
    sequence: u64,
    encoded: Box<[u8]>,
    measurements: GriddedNormalOperatorBlockMeasurements,
}

/// Exact code-owned allocation requests, capacity growth, and map insertions.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalOperatorBlockMeasurements {
    pub source_group_vector_allocations: u64,
    pub source_group_capacity_growth_bytes: u64,
    pub reduction_map_entry_insertions: u64,
    pub multiplicity_vector_allocations: u64,
    pub multiplicity_capacity_growth_bytes: u64,
    pub encoded_buffer_allocations: u64,
    pub encoded_buffer_bytes: u64,
    pub descriptor_vector_allocations: u64,
    pub descriptor_capacity_growth_bytes: u64,
}

impl GriddedNormalOperatorBlock {
    /// Return the canonical zero-based source-block sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Return the number of fixed-width records after block-local reduction.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        u64::try_from(self.encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .expect("record count fits u64")
    }

    /// Borrow the private fixed-width encoding for bounded runtime streaming.
    #[must_use]
    pub const fn encoded_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Return exact measured events owned by compilation of this block.
    #[must_use]
    pub const fn measurements(&self) -> GriddedNormalOperatorBlockMeasurements {
        self.measurements
    }
}

/// Reconstruction owner that compiles canonical weighted blocks into records.
#[doc(hidden)]
pub struct GriddedNormalOperatorCompiler {
    specification: SpectralOperatorSpecification,
    binding: LogicalIdentity,
    finite_values: casa_imaging_model::FiniteValuePolicy,
    gridder: StandardConvolution,
    next_block_sequence: u64,
    sample_count: u64,
    record_count: u64,
    coverage: CoverageEncoder,
    descriptors: Vec<BlockDescriptor>,
}

impl GriddedNormalOperatorCompiler {
    /// Compile the first vertical record owner for a supported reconstruction basis.
    pub fn new(problem: &CompiledProblem) -> Result<Self, SpectralOperatorError> {
        require_supported_basis(&problem.reconstruction().basis())?;
        let specification = SpectralOperatorSpecification::new(problem)?;
        validate_record_geometry(&specification)?;
        let geometry = specification.operator_geometry();
        let binding = static_binding(&specification);
        Ok(Self {
            finite_values: specification.finite_values(),
            gridder: StandardConvolution::new(&geometry),
            specification,
            binding,
            next_block_sequence: 0,
            sample_count: 0,
            record_count: 0,
            coverage: CoverageEncoder::new(),
            descriptors: Vec::new(),
        })
    }

    /// Reduce one complete canonical weighting block and return its opaque bytes.
    pub fn compile_block(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<GriddedNormalOperatorBlock, SpectralOperatorError> {
        if block.sequence() != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.coverage.adopt(block.coverage_checkpoint());
        let mut groups = BTreeMap::<Vec<ReducedRecordKey>, Vec<f64>>::new();
        let mut measurements = GriddedNormalOperatorBlockMeasurements::default();
        for weighted in block.samples() {
            let selected = weighted.selected();
            if !accept_weighted_input(selected, self.finite_values)?
                || !selected
                    .address()
                    .correlation_type
                    .contributes_to_stokes_i()
            {
                continue;
            }
            let uvw_m = selected.transformed_uvw_m();
            if !selected.phase_shift_m().is_finite() || uvw_m.iter().any(|value| !value.is_finite())
            {
                return Err(SpectralOperatorError::InvalidSample);
            }
            let mut group = Vec::new();
            let mut has_positive_weight = false;
            for spectral in weighted.spectral_values() {
                let contribution = spectral.contribution();
                let output_channel = usize::try_from(contribution.output_channel())
                    .map_err(|_| SpectralOperatorError::InvalidSample)?;
                let frequency_hz = contribution.evaluation_frequency_hz();
                if output_channel >= self.specification.slab().total_channels()
                    || !frequency_hz.is_finite()
                    || frequency_hz <= 0.0
                    || !spectral.imaging_weight().is_finite()
                    || spectral.imaging_weight() < 0.0
                    || !contribution.factor().is_finite()
                    || contribution.factor() == 0.0
                {
                    return Err(SpectralOperatorError::InvalidSample);
                }
                let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                let Some(taps) = self.gridder.taps([uvw_m[0] * scale, uvw_m[1] * scale]) else {
                    continue;
                };
                let phase_angle = std::f64::consts::TAU * selected.phase_shift_m() * frequency_hz
                    / SPEED_OF_LIGHT_M_PER_S;
                let forward_scale = Complex64::from_polar(contribution.factor(), -phase_angle);
                if !forward_scale.re.is_finite() || !forward_scale.im.is_finite() {
                    return Err(SpectralOperatorError::GeneratedNonfinite);
                }
                has_positive_weight |= spectral.imaging_weight() > 0.0;
                let old_capacity = group.capacity();
                group.push(ReducedRecordKey {
                    output_channel: contribution.output_channel(),
                    taps: encode_taps(taps)?,
                    forward_real: canonical_zero_bits(forward_scale.re),
                    forward_imaginary: canonical_zero_bits(forward_scale.im),
                    imaging_weight: canonical_zero_bits(spectral.imaging_weight()),
                });
                record_vector_growth(
                    old_capacity,
                    group.capacity(),
                    size_of::<ReducedRecordKey>(),
                    &mut measurements.source_group_vector_allocations,
                    &mut measurements.source_group_capacity_growth_bytes,
                )?;
            }
            if !group.is_empty() && has_positive_weight {
                let multiplicities = match groups.entry(group) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        measurements.reduction_map_entry_insertions = measurements
                            .reduction_map_entry_insertions
                            .checked_add(1)
                            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                        entry.insert(Vec::new())
                    }
                };
                let old_capacity = multiplicities.capacity();
                multiplicities.push(1.0);
                record_vector_growth(
                    old_capacity,
                    multiplicities.capacity(),
                    size_of::<f64>(),
                    &mut measurements.multiplicity_vector_allocations,
                    &mut measurements.multiplicity_capacity_growth_bytes,
                )?;
            }
        }
        let encoded = encode_reduced(groups)?;
        if !encoded.is_empty() {
            measurements.encoded_buffer_allocations = 1;
        }
        measurements.encoded_buffer_bytes =
            u64::try_from(encoded.len()).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        let source_samples = u64::try_from(block.samples().len())
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        let record_count = u64::try_from(encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        self.sample_count = self
            .sample_count
            .checked_add(source_samples)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        self.record_count = self
            .record_count
            .checked_add(record_count)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        let old_descriptor_capacity = self.descriptors.capacity();
        self.descriptors.push(BlockDescriptor {
            source_samples,
            record_count,
            digest: Sha256::digest(&encoded).into(),
        });
        record_vector_growth(
            old_descriptor_capacity,
            self.descriptors.capacity(),
            size_of::<BlockDescriptor>(),
            &mut measurements.descriptor_vector_allocations,
            &mut measurements.descriptor_capacity_growth_bytes,
        )?;
        let result = GriddedNormalOperatorBlock {
            sequence: self.next_block_sequence,
            encoded,
            measurements,
        };
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        Ok(result)
    }

    /// Seal exhaustive coverage and exact per-block byte identities.
    pub fn complete(
        self,
        replay: &WeightingReplaySummary,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<GriddedNormalOperatorProgram, SpectralOperatorError> {
        if self.sample_count != replay.sample_count()
            || self.next_block_sequence != replay.block_count()
            || self.descriptors.len()
                != usize::try_from(replay.block_count())
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let (coverage, _) = self
            .coverage
            .finish(replay.weighting_generation(), self.sample_count);
        if coverage != replay.coverage() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let identity = program_identity(
            self.binding,
            replay,
            selected_generation,
            continuum_transform_generation,
            self.record_count,
            &self.descriptors,
        );
        Ok(GriddedNormalOperatorProgram {
            manifest: Arc::new(GriddedNormalOperatorManifest {
                identity,
                specification: self.specification,
                weighting_generation: replay.weighting_generation(),
                replay: replay.replay_id(),
                coverage,
                selected_generation,
                continuum_transform_generation,
                sample_count: replay.sample_count(),
                record_count: self.record_count,
                descriptors: self.descriptors.into_boxed_slice(),
            }),
        })
    }
}

fn record_vector_growth(
    old_capacity: usize,
    new_capacity: usize,
    element_bytes: usize,
    allocation_operations: &mut u64,
    capacity_growth_bytes: &mut u64,
) -> Result<(), SpectralOperatorError> {
    if new_capacity == old_capacity {
        return Ok(());
    }
    *allocation_operations = allocation_operations
        .checked_add(1)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let growth = new_capacity
        .checked_sub(old_capacity)
        .and_then(|elements| elements.checked_mul(element_bytes))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    *capacity_growth_bytes = capacity_growth_bytes
        .checked_add(growth)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(())
}

struct GriddedNormalOperatorManifest {
    identity: LogicalIdentity,
    specification: SpectralOperatorSpecification,
    weighting_generation: crate::WeightingGenerationId,
    replay: WeightingReplayId,
    coverage: WeightingReplayCoverageId,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    sample_count: u64,
    record_count: u64,
    descriptors: Box<[BlockDescriptor]>,
}

/// Sealed manifest for one exhaustive private gridded replay artifact.
#[doc(hidden)]
#[derive(Clone)]
pub struct GriddedNormalOperatorProgram {
    manifest: Arc<GriddedNormalOperatorManifest>,
}

impl GriddedNormalOperatorProgram {
    /// Return the reconstruction-minted identity of this exact framed program.
    #[must_use]
    pub fn identity(&self) -> LogicalIdentity {
        self.manifest.identity
    }

    /// Return the private-format schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        RECORD_VERSION
    }

    /// Return the number of framed source blocks in this program.
    #[must_use]
    pub fn block_count(&self) -> u64 {
        u64::try_from(self.manifest.descriptors.len()).expect("block count fits u64")
    }

    /// Return the total number of reduced fixed-width records.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.manifest.record_count
    }

    /// Return the exact encoded byte count for one block.
    #[must_use]
    pub fn block_encoded_bytes(&self, sequence: u64) -> Option<usize> {
        let descriptor = self
            .manifest
            .descriptors
            .get(usize::try_from(sequence).ok()?)?;
        usize::try_from(descriptor.record_count)
            .ok()?
            .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
    }

    /// Bind a model and prior invariant normal state to the gridded apply owner.
    pub fn begin_apply(
        &self,
        problem: &CompiledProblem,
        model: &ModelGeneration,
        prior: FinalNormalState,
        prepared: PreparedSpectralOperator,
    ) -> Result<GriddedNormalOperatorApply, SpectralOperatorError> {
        require_supported_basis(&problem.reconstruction().basis())?;
        let (prepared_specification, workload, fft) = prepared.into_parts();
        if problem.problem_id() != self.manifest.specification.problem_id()
            || prepared_specification != self.manifest.specification
            || workload.pass() != SpectralOperatorPass::ResidualRefresh
            || prior.problem_id() != self.manifest.specification.problem_id()
            || prior.geometry_id() != self.manifest.specification.geometry_id()
            || prior.numerics_id() != self.manifest.specification.numerics_id()
            || prior.weighting_commitment_id()
                != self.manifest.specification.weighting_commitment_id()
            || prior.weighting_generation() != self.manifest.weighting_generation
            || prior.replay_id() != self.manifest.replay
            || prior.coverage() != self.manifest.coverage
            || prior.selected_generation() != self.manifest.selected_generation
            || prior.continuum_transform_generation()
                != self.manifest.continuum_transform_generation
            || prior.sample_count() != self.manifest.sample_count
            || prior.block_count() != self.block_count()
            || prior.catalog()
                != if self.manifest.specification.slab().total_channels() == 1 {
                    crate::NormalStateCatalog::UnnormalizedPlaneV1
                } else {
                    crate::NormalStateCatalog::UnnormalizedChannelSlabV1
                }
            || prior.channel_count() != self.manifest.specification.slab().core_depth()
        {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        let model_generation = model.generation_id();
        let mut operator = SpectralSlabOperator::new(prepared_specification, workload, fft);
        operator.prepare_residual_model(model, Some(prior.into_reusable()))?;
        Ok(GriddedNormalOperatorApply {
            program: self.clone(),
            operator,
            model_generation,
            next_block_sequence: 0,
            applied_records: 0,
            active_block: None,
        })
    }
}

/// Model-bound owner that applies only sealed gridded records.
#[doc(hidden)]
pub struct GriddedNormalOperatorApply {
    program: GriddedNormalOperatorProgram,
    operator: SpectralSlabOperator,
    model_generation: crate::ModelGenerationId,
    next_block_sequence: u64,
    applied_records: u64,
    active_block: Option<ActiveGriddedNormalBlock>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ActiveGriddedNormalBlock {
    sequence: u64,
    next_record_ordinal: u64,
    group_start_record: u64,
    group_prediction: Complex64,
}

/// One stable record-range partition prepared by reconstruction.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalPredictionWork {
    block_sequence: u64,
    partition_key: u64,
    first_record: u64,
    record_count: u64,
    block_record_count: u64,
    encoded_start: usize,
    encoded_end: usize,
}

impl GriddedNormalPredictionWork {
    /// Return the worker-count-independent key of this ordered partition.
    #[must_use]
    pub const fn partition_key(self) -> u64 {
        self.partition_key
    }

    /// Return the exact number of encoded records borrowed by this work item.
    #[must_use]
    pub const fn record_count(self) -> u64 {
        self.record_count
    }
}

/// Read-only predictions for one ordered record-group partition.
#[doc(hidden)]
#[derive(Debug)]
pub struct GriddedNormalPredictionPartial {
    work: GriddedNormalPredictionWork,
    predictions: Box<[Complex64]>,
    resident_bytes: u64,
}

impl GriddedNormalPredictionPartial {
    /// Return exact heap bytes retained by this partial prediction payload.
    #[must_use]
    pub const fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }
}

impl GriddedNormalOperatorApply {
    /// Validate one borrowed frame and return its stable bounded work count.
    pub fn prediction_partition_count(
        &self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<usize, SpectralOperatorError> {
        let descriptor = self
            .program
            .manifest
            .descriptors
            .get(
                usize::try_from(sequence)
                    .map_err(|_| SpectralOperatorError::GriddedRecordMismatch)?,
            )
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        if sequence != self.next_block_sequence || self.active_block.is_some() {
            return Err(SpectralOperatorError::BlockSequence);
        }
        validate_encoded_block(descriptor, encoded)?;
        prediction_partition_count(
            usize::try_from(descriptor.record_count)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
        )
    }

    /// Return one worker-count-independent record partition by local ordinal.
    pub fn prediction_partition(
        &self,
        sequence: u64,
        encoded: &[u8],
        local_ordinal: usize,
    ) -> Result<GriddedNormalPredictionWork, SpectralOperatorError> {
        let descriptor = self
            .program
            .manifest
            .descriptors
            .get(
                usize::try_from(sequence)
                    .map_err(|_| SpectralOperatorError::GriddedRecordMismatch)?,
            )
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        if sequence != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let record_count = usize::try_from(descriptor.record_count)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        if encoded.len()
            != record_count
                .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?
        {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        prediction_work(sequence, record_count, local_ordinal)
    }

    /// Predict one prepared partition without mutating gridded normal state.
    pub fn predict_partition(
        &self,
        encoded: &[u8],
        work: GriddedNormalPredictionWork,
    ) -> Result<GriddedNormalPredictionPartial, SpectralOperatorError> {
        if work.block_sequence != self.next_block_sequence
            || self
                .active_block
                .is_some_and(|active| work.first_record < active.next_record_ordinal)
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        predict_partition_records(
            encoded,
            work,
            self.program.manifest.specification.grid_shape(),
            self.program.manifest.specification.slab().total_channels(),
            |record| {
                self.operator.predict_gridded_normal(
                    record.output_channel,
                    record.taps,
                    record.forward_scale,
                )
            },
        )
    }

    /// Commit one prediction partial in canonical record-group order.
    pub fn commit_prediction(
        &mut self,
        encoded: &[u8],
        partial: GriddedNormalPredictionPartial,
    ) -> Result<(), SpectralOperatorError> {
        let work = partial.work;
        let mut active = self.active_block.unwrap_or(ActiveGriddedNormalBlock {
            sequence: work.block_sequence,
            next_record_ordinal: 0,
            group_start_record: 0,
            group_prediction: Complex64::default(),
        });
        if work.block_sequence != self.next_block_sequence
            || active.sequence != work.block_sequence
            || work.first_record != active.next_record_ordinal
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let grid_shape = self.program.manifest.specification.grid_shape();
        let output_channels = self.program.manifest.specification.slab().total_channels();
        commit_prediction_records(
            encoded,
            partial,
            grid_shape,
            output_channels,
            &mut active,
            |record, predicted| {
                self.operator.apply_gridded_normal(
                    record.output_channel,
                    record.taps,
                    predicted,
                    record.forward_scale.conj() * record.imaging_weight,
                )
            },
        )?;
        if active.next_record_ordinal > work.block_record_count {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        if active.next_record_ordinal == work.block_record_count {
            if active.group_start_record != work.block_record_count
                || active.group_prediction != Complex64::default()
            {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }
            self.applied_records = self
                .applied_records
                .checked_add(work.block_record_count)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            self.next_block_sequence = self
                .next_block_sequence
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            self.active_block = None;
        } else {
            self.active_block = Some(active);
        }
        Ok(())
    }

    /// Validate and apply one borrowed frame through the ordered partial path.
    pub fn apply_encoded_block(
        &mut self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<(), SpectralOperatorError> {
        let partition_count = self.prediction_partition_count(sequence, encoded)?;
        for ordinal in 0..partition_count {
            let work = self.prediction_partition(sequence, encoded, ordinal)?;
            let partial = self.predict_partition(encoded, work)?;
            self.commit_prediction(encoded, partial)?;
        }
        Ok(())
    }

    /// Finish `dirty - A* W A x` and return ordinary Major-Cycle input.
    pub fn finish(self) -> Result<CompleteDataOwnerResult, SpectralOperatorError> {
        if self.next_block_sequence != self.program.block_count()
            || self.applied_records != self.program.manifest.record_count
            || self.active_block.is_some()
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let primitives = self.operator.finish_gridded_normal(self.model_generation)?;
        let primitive_catalog = if self.program.manifest.specification.slab().total_channels() == 1
        {
            SpectralPrimitiveCatalog::UnnormalizedPlaneV1
        } else {
            SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1
        };
        Ok(CompleteDataOwnerResult {
            primitives,
            completion: CompleteDataOwnerCompletion {
                problem: self.program.manifest.specification.problem_id(),
                geometry: self.program.manifest.specification.geometry_id(),
                numerics: self.program.manifest.specification.numerics_id(),
                weighting_commitment: self
                    .program
                    .manifest
                    .specification
                    .weighting_commitment_id(),
                weighting_generation: self.program.manifest.weighting_generation,
                replay: self.program.manifest.replay,
                coverage: self.program.manifest.coverage,
                coverage_proof_bytes: 0,
                coverage_proof_hash_calls: 0,
                primitives: primitive_catalog,
                selected_generation: self.program.manifest.selected_generation,
                continuum_transform_generation: self
                    .program
                    .manifest
                    .continuum_transform_generation,
                sample_count: self.program.manifest.sample_count,
                block_count: self.program.block_count(),
            },
        })
    }
}

fn prediction_partition_count(record_count: usize) -> Result<usize, SpectralOperatorError> {
    Ok(record_count.clamp(1, GRIDDED_NORMAL_MAXIMUM_PARTITIONS_PER_BLOCK))
}

fn prediction_work(
    block_sequence: u64,
    record_count: usize,
    local_ordinal: usize,
) -> Result<GriddedNormalPredictionWork, SpectralOperatorError> {
    let partition_count = prediction_partition_count(record_count)?;
    if local_ordinal >= partition_count {
        return Err(SpectralOperatorError::IncompleteCoverage);
    }
    let start_record = local_ordinal
        .checked_mul(record_count)
        .and_then(|records| records.checked_div(partition_count))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let end_record = local_ordinal
        .checked_add(1)
        .and_then(|ordinal| ordinal.checked_mul(record_count))
        .and_then(|records| records.checked_div(partition_count))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let encoded_start = start_record
        .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let encoded_end = end_record
        .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(GriddedNormalPredictionWork {
        block_sequence,
        partition_key: u64::try_from(start_record)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
        first_record: u64::try_from(start_record)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
        record_count: u64::try_from(
            end_record
                .checked_sub(start_record)
                .ok_or(SpectralOperatorError::CoverageOverflow)?,
        )
        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
        block_record_count: u64::try_from(record_count)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
        encoded_start,
        encoded_end,
    })
}

fn predict_partition_records<F>(
    encoded: &[u8],
    work: GriddedNormalPredictionWork,
    grid_shape: [usize; 2],
    output_channels: usize,
    mut predict: F,
) -> Result<GriddedNormalPredictionPartial, SpectralOperatorError>
where
    F: FnMut(DecodedRecord) -> Result<Complex64, SpectralOperatorError>,
{
    let partition = encoded
        .get(work.encoded_start..work.encoded_end)
        .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
    let prediction_count =
        usize::try_from(work.record_count).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    let mut predictions = Vec::new();
    predictions
        .try_reserve_exact(prediction_count)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    for bytes in partition.chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES) {
        let record = decode_record(bytes, grid_shape, output_channels)?;
        let predicted = predict(record)?;
        if !predicted.re.is_finite() || !predicted.im.is_finite() {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        predictions.push(predicted);
    }
    if predictions.len() != prediction_count {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let resident_bytes = prediction_count
        .checked_mul(size_of::<Complex64>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(GriddedNormalPredictionPartial {
        work,
        predictions: predictions.into_boxed_slice(),
        resident_bytes,
    })
}

fn commit_prediction_records<F>(
    encoded: &[u8],
    partial: GriddedNormalPredictionPartial,
    grid_shape: [usize; 2],
    output_channels: usize,
    active: &mut ActiveGriddedNormalBlock,
    mut apply: F,
) -> Result<(), SpectralOperatorError>
where
    F: FnMut(DecodedRecord, Complex64) -> Result<(), SpectralOperatorError>,
{
    let partition = encoded
        .get(partial.work.encoded_start..partial.work.encoded_end)
        .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
    let prediction_count = usize::try_from(partial.work.record_count)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    if partial.predictions.len() != prediction_count {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    for (offset, (bytes, predicted)) in partition
        .chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
        .zip(partial.predictions.iter().copied())
        .enumerate()
    {
        let record = decode_record(bytes, grid_shape, output_channels)?;
        active.group_prediction += predicted;
        if record.group_end {
            if !active.group_prediction.re.is_finite() || !active.group_prediction.im.is_finite() {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            let record_ordinal = usize::try_from(active.next_record_ordinal)
                .ok()
                .and_then(|first| first.checked_add(offset))
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            let group_start = usize::try_from(active.group_start_record)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let group_end = record_ordinal
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            let group_start = group_start
                .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            let group_end = group_end
                .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            for bytes in encoded
                .get(group_start..group_end)
                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?
                .chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            {
                apply(
                    decode_record(bytes, grid_shape, output_channels)?,
                    active.group_prediction,
                )?;
            }
            active.group_start_record = u64::try_from(record_ordinal)
                .ok()
                .and_then(|record| record.checked_add(1))
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            active.group_prediction = Complex64::default();
        }
    }
    active.next_record_ordinal = active
        .next_record_ordinal
        .checked_add(partial.work.record_count)
        .ok_or(SpectralOperatorError::CoverageOverflow)?;
    Ok(())
}

fn require_supported_basis(basis: &ReconstructionBasis) -> Result<(), SpectralOperatorError> {
    if matches!(
        basis,
        ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. }
    ) {
        Ok(())
    } else {
        Err(SpectralOperatorError::UnsupportedGriddedReplay)
    }
}

fn validate_record_geometry(
    specification: &SpectralOperatorSpecification,
) -> Result<(), SpectralOperatorError> {
    if specification.slab().core_depth() != specification.slab().total_channels()
        || specification.slab().resident_depth() != specification.slab().total_channels()
        || specification.slab().total_channels() > 1 << CHANNEL_KEY_BITS
        || specification
            .grid_shape()
            .into_iter()
            .any(|extent| extent > 1 << 12)
    {
        return Err(SpectralOperatorError::UnsupportedGriddedReplay);
    }
    Ok(())
}

fn static_binding(specification: &SpectralOperatorSpecification) -> LogicalIdentity {
    let mut encoder = Encoder::new(RECORD_DOMAIN, RECORD_VERSION);
    encoder.identity(specification.problem_id().as_bytes());
    encoder.identity(specification.geometry_id().as_bytes());
    encoder.identity(specification.numerics_id().as_bytes());
    encoder.identity(specification.weighting_commitment_id().as_bytes());
    encoder.usize(specification.grid_shape()[0]);
    encoder.usize(specification.grid_shape()[1]);
    encoder.usize(specification.slab().total_channels());
    encoder.usize(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
    LogicalIdentity::from_sha256(encoder.finish())
}

fn program_identity(
    binding: LogicalIdentity,
    replay: &WeightingReplaySummary,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    record_count: u64,
    descriptors: &[BlockDescriptor],
) -> LogicalIdentity {
    let mut encoder = Encoder::new(RECORD_DOMAIN, RECORD_VERSION + 1);
    encoder.identity(binding.as_bytes());
    encoder.identity(replay.weighting_generation().as_bytes());
    encoder.identity(replay.replay_id().as_bytes());
    encoder.identity(replay.coverage().as_bytes());
    encoder.identity(selected_generation.as_bytes());
    match continuum_transform_generation {
        Some(generation) => {
            encoder.u8(1);
            encoder.identity(generation.as_bytes());
        }
        None => encoder.u8(0),
    }
    encoder.u64(replay.sample_count());
    encoder.u64(replay.block_count());
    encoder.u64(record_count);
    for descriptor in descriptors {
        encoder.u64(descriptor.source_samples);
        encoder.u64(descriptor.record_count);
        encoder.identity(descriptor.digest);
    }
    LogicalIdentity::from_sha256(encoder.finish())
}

fn encode_reduced(
    groups: BTreeMap<Vec<ReducedRecordKey>, Vec<f64>>,
) -> Result<Box<[u8]>, SpectralOperatorError> {
    let record_count = groups.keys().try_fold(0_usize, |total, records| {
        total
            .checked_add(records.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    })?;
    let capacity = record_count
        .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    for (records, mut multiplicities) in groups {
        if records.is_empty() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        multiplicities.sort_by(f64::total_cmp);
        let multiplicity = compensated_sum(&multiplicities)?;
        let last = records.len() - 1;
        for (index, record) in records.into_iter().enumerate() {
            let output_channel = u64::from(record.output_channel);
            if output_channel > CHANNEL_KEY_MASK || record.taps & !TAP_KEY_MASK != 0 {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }
            let key = record.taps
                | (output_channel << TAP_KEY_BITS)
                | if index == last { GROUP_END_BIT } else { 0 };
            let forward_real = f64::from_bits(record.forward_real);
            let forward_imaginary = f64::from_bits(record.forward_imaginary);
            let imaging_weight = f64::from_bits(record.imaging_weight) * multiplicity;
            if !forward_real.is_finite()
                || !forward_imaginary.is_finite()
                || (forward_real == 0.0 && forward_imaginary == 0.0)
                || !imaging_weight.is_finite()
                || imaging_weight.is_sign_negative()
            {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            encoded.extend_from_slice(&key.to_le_bytes());
            encoded.extend_from_slice(&forward_real.to_le_bytes());
            encoded.extend_from_slice(&forward_imaginary.to_le_bytes());
            encoded.extend_from_slice(&imaging_weight.to_le_bytes());
        }
    }
    Ok(encoded.into_boxed_slice())
}

fn canonical_zero_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

fn compensated_sum(values: &[f64]) -> Result<f64, SpectralOperatorError> {
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for value in values {
        let corrected = *value - compensation;
        let updated = sum + corrected;
        compensation = (updated - sum) - corrected;
        sum = updated;
    }
    if sum.is_finite() && !sum.is_sign_negative() {
        Ok(sum)
    } else {
        Err(SpectralOperatorError::GeneratedNonfinite)
    }
}

fn encode_taps(taps: SampleTaps) -> Result<u64, SpectralOperatorError> {
    if taps.x.start >= 1 << 12
        || taps.y.start >= 1 << 12
        || taps.x.weight_index >= 1 << 7
        || taps.y.weight_index >= 1 << 7
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(taps.x.start as u64
        | ((taps.y.start as u64) << 12)
        | ((taps.x.weight_index as u64) << 24)
        | ((taps.y.weight_index as u64) << 31))
}

fn decode_record(
    encoded: &[u8],
    grid_shape: [usize; 2],
    output_channels: usize,
) -> Result<DecodedRecord, SpectralOperatorError> {
    if encoded.len() != GRIDDED_NORMAL_OPERATOR_RECORD_BYTES {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let key = u64::from_le_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let forward_real = f64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let forward_imaginary = f64::from_le_bytes(
        encoded[16..24]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let imaging_weight = f64::from_le_bytes(
        encoded[24..]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let output_channel = usize::try_from((key >> TAP_KEY_BITS) & CHANNEL_KEY_MASK)
        .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
    if key & !RECORD_KEY_MASK != 0
        || output_channel >= output_channels
        || !forward_real.is_finite()
        || !forward_imaginary.is_finite()
        || (forward_real == 0.0 && forward_imaginary == 0.0)
        || !imaging_weight.is_finite()
        || imaging_weight.is_sign_negative()
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let tap_key = key & TAP_KEY_MASK;
    let taps = SampleTaps {
        x: TapSpan {
            start: (tap_key & 0x0fff) as usize,
            weight_index: ((tap_key >> 24) & 0x7f) as usize,
        },
        y: TapSpan {
            start: ((tap_key >> 12) & 0x0fff) as usize,
            weight_index: ((tap_key >> 31) & 0x7f) as usize,
        },
    };
    if taps.x.weight_index > OVERSAMPLING
        || taps.y.weight_index > OVERSAMPLING
        || taps
            .x
            .start
            .checked_add(2 * SUPPORT)
            .is_none_or(|end| end >= grid_shape[0])
        || taps
            .y
            .start
            .checked_add(2 * SUPPORT)
            .is_none_or(|end| end >= grid_shape[1])
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(DecodedRecord {
        output_channel,
        taps,
        forward_scale: Complex64::new(forward_real, forward_imaginary),
        imaging_weight,
        group_end: key & GROUP_END_BIT != 0,
    })
}

fn validate_encoded_block(
    descriptor: &BlockDescriptor,
    encoded: &[u8],
) -> Result<(), SpectralOperatorError> {
    let expected_bytes = usize::try_from(descriptor.record_count)
        .ok()
        .and_then(|records| records.checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES))
        .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
    if encoded.len() != expected_bytes
        || <[u8; 32]>::from(Sha256::digest(encoded)) != descriptor.digest
    {
        return Err(SpectralOperatorError::GriddedRecordMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ndarray::Array2;
    use num_complex::Complex64;

    use super::*;
    use crate::spectral_operator::SpectralOperatorGeometry;

    fn geometry() -> SpectralOperatorGeometry {
        SpectralOperatorGeometry {
            image_shape: [8, 8],
            grid_shape: [10, 10],
            image_blc: [1, 1],
            increment_rad: [-2.0e-3, 2.0e-3],
        }
    }

    fn scalar_groups(
        contributions: impl IntoIterator<Item = (SampleTaps, f64)>,
    ) -> BTreeMap<Vec<ReducedRecordKey>, Vec<f64>> {
        let mut groups = BTreeMap::new();
        for (taps, coefficient) in contributions {
            groups
                .entry(vec![ReducedRecordKey {
                    output_channel: 0,
                    taps: encode_taps(taps).expect("encode scalar taps"),
                    forward_real: 1.0_f64.to_bits(),
                    forward_imaginary: 0,
                    imaging_weight: 1.0_f64.to_bits(),
                }])
                .or_insert_with(Vec::new)
                .push(coefficient);
        }
        groups
    }

    #[test]
    fn records_are_fixed_width_canonical_and_permutation_independent() {
        let gridder = StandardConvolution::new(&geometry());
        let first = gridder.taps([0.0, 0.0]).expect("central taps");
        let second = gridder
            .taps([20.0, -14.0])
            .expect("offset taps remain on grid");
        let left = encode_reduced(scalar_groups([
            (first, 0.75),
            (second, 0.4),
            (first, 1.25),
            (second, 0.6),
        ]))
        .expect("encode records");
        let right = encode_reduced(scalar_groups([
            (second, 0.6),
            (first, 1.25),
            (second, 0.4),
            (first, 0.75),
        ]))
        .expect("encode permuted records");
        assert_eq!(left, right);
        assert_eq!(left.len(), 2 * GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
    }

    #[test]
    fn compilation_measurements_distinguish_allocations_from_map_insertions() {
        let mut operations = 0;
        let mut bytes = 0;
        record_vector_growth(0, 4, 48, &mut operations, &mut bytes).expect("first allocation");
        record_vector_growth(4, 4, 48, &mut operations, &mut bytes).expect("reuse");
        record_vector_growth(4, 8, 48, &mut operations, &mut bytes).expect("reallocation");
        assert_eq!(operations, 2);
        assert_eq!(bytes, 384);
    }

    #[test]
    fn grouped_record_apply_matches_scalar_normal_operator_below_ceiling() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let mut model_grid = Array2::<Complex64>::zeros(shape);
        for ((x, y), value) in model_grid.indexed_iter_mut() {
            *value = Complex64::new(
                (x * shape.1 + y) as f64 * 0.013 - 0.4,
                x as f64 * -0.017 + y as f64 * 0.009,
            );
        }
        let first = gridder.taps([0.0, 0.0]).expect("central taps");
        let second = gridder
            .taps([20.0, -14.0])
            .expect("offset taps remain on grid");
        let contributions = [
            (first, 0.75),
            (first, 1.25),
            (second, 0.4),
            (second, 0.6),
            (first, 0.125),
            (first, 0.375),
        ];
        let mut direct = Array2::<Complex64>::zeros(shape);
        let mut direct_compensation = Array2::<Complex64>::zeros(shape);
        for (taps, coefficient) in contributions {
            let predicted = gridder.degrid(&model_grid, taps);
            gridder.grid_compensated(
                &mut direct,
                &mut direct_compensation,
                taps,
                predicted * coefficient,
            );
        }
        let encoded = encode_reduced(scalar_groups(contributions)).expect("reduce records");
        let mut grouped = Array2::<Complex64>::zeros(shape);
        let mut grouped_compensation = Array2::<Complex64>::zeros(shape);
        for record in encoded.chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES) {
            let record = decode_record(record, geometry.grid_shape, 1).expect("decode record");
            let predicted = gridder.degrid(&model_grid, record.taps) * record.forward_scale;
            gridder.grid_compensated(
                &mut grouped,
                &mut grouped_compensation,
                record.taps,
                predicted * record.forward_scale.conj() * record.imaging_weight,
            );
        }
        let squared_error = direct
            .iter()
            .zip(&grouped)
            .map(|(expected, actual)| (*actual - *expected).norm_sqr())
            .sum::<f64>();
        let squared_reference = direct.iter().map(Complex64::norm_sqr).sum::<f64>();
        let normalized_rms = (squared_error / squared_reference.max(f64::MIN_POSITIVE)).sqrt();
        assert!(normalized_rms <= 0.001, "normalized RMS {normalized_rms}");

        let dirty = Complex64::new(4.0, -0.5);
        let normal = Complex64::new(1.25, 0.75);
        assert_eq!(dirty - normal, Complex64::new(2.75, -1.25));
    }

    #[test]
    fn prediction_partitions_are_stable_and_schedule_independent() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let mut model_grid = Array2::<Complex64>::zeros(shape);
        for ((x, y), value) in model_grid.indexed_iter_mut() {
            *value = Complex64::new(
                (x * shape.1 + y) as f64 * 0.013 - 0.4,
                x as f64 * -0.017 + y as f64 * 0.009,
            );
        }
        let mut distinct_taps = BTreeMap::new();
        for x in -30..=30 {
            for y in -30..=30 {
                if let Some(taps) = gridder.taps([f64::from(x), f64::from(y)]) {
                    distinct_taps
                        .entry(encode_taps(taps).expect("encode taps"))
                        .or_insert(taps);
                }
            }
        }
        let contributions = distinct_taps
            .into_values()
            .take(6)
            .enumerate()
            .map(|(index, taps)| (taps, 0.5 + index as f64 * 0.125))
            .collect::<Vec<_>>();
        assert_eq!(contributions.len(), 6);
        let mut groups = BTreeMap::new();
        groups.insert(
            contributions
                .into_iter()
                .map(|(taps, imaging_weight)| ReducedRecordKey {
                    output_channel: 0,
                    taps: encode_taps(taps).expect("encode grouped taps"),
                    forward_real: 1.0_f64.to_bits(),
                    forward_imaginary: 0,
                    imaging_weight: imaging_weight.to_bits(),
                })
                .collect::<Vec<_>>(),
            vec![1.0],
        );
        let encoded = encode_reduced(groups).expect("encode one multi-record group");

        let mut expected = Array2::<Complex64>::zeros(shape);
        let mut expected_compensation = Array2::<Complex64>::zeros(shape);
        let records = encoded
            .chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .map(|bytes| decode_record(bytes, geometry.grid_shape, 1).expect("decode record"))
            .collect::<Vec<_>>();
        let predicted = records
            .iter()
            .try_fold(
                Complex64::default(),
                |sum, record| -> Result<_, SpectralOperatorError> {
                    Ok(sum + gridder.degrid(&model_grid, record.taps) * record.forward_scale)
                },
            )
            .expect("predict group");
        for record in &records {
            gridder.grid_compensated(
                &mut expected,
                &mut expected_compensation,
                record.taps,
                predicted * record.forward_scale.conj() * record.imaging_weight,
            );
        }

        let run = |workers: usize| {
            let partition_count =
                prediction_partition_count(records.len()).expect("partition count");
            let identities = (0..partition_count)
                .map(|ordinal| prediction_work(7, records.len(), ordinal).expect("partition work"))
                .map(GriddedNormalPredictionWork::partition_key)
                .collect::<Vec<_>>();
            let mut grid = Array2::<Complex64>::zeros(shape);
            let mut compensation = Array2::<Complex64>::zeros(shape);
            let mut active = ActiveGriddedNormalBlock {
                sequence: 7,
                next_record_ordinal: 0,
                group_start_record: 0,
                group_prediction: Complex64::default(),
            };
            for wave_start in (0..partition_count).step_by(workers) {
                let wave_end = (wave_start + workers).min(partition_count);
                let partials = (wave_start..wave_end)
                    .map(|ordinal| {
                        let work =
                            prediction_work(7, records.len(), ordinal).expect("partition work");
                        predict_partition_records(
                            &encoded,
                            work,
                            geometry.grid_shape,
                            1,
                            |record| {
                                Ok(gridder.degrid(&model_grid, record.taps) * record.forward_scale)
                            },
                        )
                        .expect("predict partition")
                    })
                    .collect::<Vec<_>>();
                for partial in partials {
                    assert_eq!(
                        partial.resident_bytes(),
                        u64::try_from(size_of::<Complex64>()).expect("partial bytes fit u64")
                    );
                    commit_prediction_records(
                        &encoded,
                        partial,
                        geometry.grid_shape,
                        1,
                        &mut active,
                        |record, predicted| {
                            gridder.grid_compensated(
                                &mut grid,
                                &mut compensation,
                                record.taps,
                                predicted * record.forward_scale.conj() * record.imaging_weight,
                            );
                            Ok(())
                        },
                    )
                    .expect("commit partition");
                }
            }
            assert_eq!(active.next_record_ordinal, records.len() as u64);
            assert_eq!(active.group_start_record, records.len() as u64);
            assert_eq!(active.group_prediction, Complex64::default());
            (identities, grid)
        };

        let (serial_identities, serial_grid) = run(1);
        let (parallel_identities, parallel_grid) = run(3);
        assert_eq!(serial_identities, vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(parallel_identities, serial_identities);
        assert_eq!(serial_grid, expected);
        assert_eq!(parallel_grid, serial_grid);
    }

    #[test]
    fn corrupt_truncated_and_reserved_records_fail_closed() {
        let gridder = StandardConvolution::new(&geometry());
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let encoded = encode_reduced(scalar_groups([(taps, 1.0)])).expect("encode record");
        let descriptor = BlockDescriptor {
            source_samples: 1,
            record_count: 1,
            digest: Sha256::digest(&encoded).into(),
        };
        assert!(validate_encoded_block(&descriptor, &encoded).is_ok());
        assert_eq!(
            validate_encoded_block(&descriptor, &encoded[..15]),
            Err(SpectralOperatorError::GriddedRecordMismatch)
        );
        let mut corrupt = encoded.to_vec();
        corrupt[0] ^= 1;
        assert_eq!(
            validate_encoded_block(&descriptor, &corrupt),
            Err(SpectralOperatorError::GriddedRecordMismatch)
        );

        let mut reserved = encoded.to_vec();
        let key = u64::from_le_bytes(reserved[..8].try_into().expect("key")) | (1_u64 << 63);
        reserved[..8].copy_from_slice(&key.to_le_bytes());
        assert_eq!(
            decode_record(&reserved, geometry().grid_shape, 1),
            Err(SpectralOperatorError::InvalidGriddedRecord)
        );
    }

    #[test]
    fn constant_and_channel_local_bases_are_admitted() {
        assert!(require_supported_basis(&ReconstructionBasis::Constant).is_ok());
        assert!(
            require_supported_basis(&ReconstructionBasis::ChannelLocal { channels: 2 }).is_ok()
        );
        assert_eq!(
            require_supported_basis(&ReconstructionBasis::Taylor { terms: 2 }),
            Err(SpectralOperatorError::UnsupportedGriddedReplay)
        );
    }
}
