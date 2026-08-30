// SPDX-License-Identifier: LGPL-3.0-or-later

//! Run-scoped, disk-streamable normal-operator replay for constant and channel-local bases.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    mem::size_of,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use casa_imaging_model::{
    CompiledProblem, ContinuumTransformGenerationId, LogicalIdentity, ReconstructionBasis,
    SelectedObservationGenerationId,
};
use ndarray::Array2;
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
const GRIDDED_NORMAL_TAPS_PER_RECORD: u64 = ((SUPPORT * 2 + 1) * (SUPPORT * 2 + 1)) as u64;

/// Width of every opaque gridded normal-operator record.
pub const GRIDDED_NORMAL_OPERATOR_RECORD_BYTES: usize = 32;

/// Stable spatial work fanout for every bounded gridded-normal block.
///
/// The four identities are the center-owned quadrants of the reconstruction
/// grid. They do not vary with record count or admitted worker count.
pub const GRIDDED_NORMAL_SECTOR_COUNT: usize = 4;

/// Project exact reusable route capacity for retained frame ordinals.
///
/// `record_count` is the sum of the planned per-ordinal record capacities.
/// Every ordinal retains three such vectors (`Complex64`, `u32`, and an
/// eight-byte route) plus one prepared-frame descriptor. The storage is shared
/// by all workers.
#[doc(hidden)]
pub fn gridded_normal_route_capacity_bytes(record_count: usize, frame_count: usize) -> Option<u64> {
    let record_bytes = record_count
        .checked_mul(size_of::<Complex64>() + size_of::<u32>() + size_of::<GriddedNormalRoute>())?;
    let frame_bytes = frame_count.checked_mul(size_of::<PreparedGriddedNormalBlock>())?;
    let window = record_bytes.checked_add(frame_bytes)?;
    u64::try_from(window).ok()
}

/// Exact reconstruction-owned work performed while routing gridded replay.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalRoutingMeasurements {
    pub frames_routed: u64,
    pub encoded_records: u64,
    pub routed_record_memberships: u64,
    pub prediction_groups: u64,
    pub degrid_records: u64,
    pub grid_records: u64,
    pub sector_rescans: u64,
    pub peak_physical_route_capacity_bytes: u64,
}

/// Exact reconstruction-owned complex-value residency of sector accumulation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalSectorResidency {
    sector_accumulator_complex_values: usize,
    merge_complex_values: usize,
    peak_complex_values: usize,
}

impl GriddedNormalSectorResidency {
    /// Return persistent sector grid plus compensation values.
    #[must_use]
    pub const fn sector_accumulator_complex_values(self) -> usize {
        self.sector_accumulator_complex_values
    }

    /// Return contiguous grid plus compensation values allocated for final merge.
    #[must_use]
    pub const fn merge_complex_values(self) -> usize {
        self.merge_complex_values
    }

    /// Return the exact peak while all sectors and final merge buffers coexist.
    #[must_use]
    pub const fn peak_complex_values(self) -> usize {
        self.peak_complex_values
    }
}

/// Project exact worker-independent sector and final-merge complex-value residency.
#[doc(hidden)]
pub fn gridded_normal_sector_residency(
    grid_shape: [usize; 2],
    core_depth: usize,
) -> Result<GriddedNormalSectorResidency, SpectralOperatorError> {
    if core_depth == 0 {
        return Err(SpectralOperatorError::InvalidSlab);
    }
    let sector_halo_cells =
        (0..GRIDDED_NORMAL_SECTOR_COUNT).try_fold(0_usize, |total, sector_id| {
            let geometry = GriddedNormalSectorGeometry::new(grid_shape, sector_id)?;
            geometry
                .cell_count()
                .and_then(|cells| total.checked_add(cells))
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })?;
    let sector_accumulator_complex_values = sector_halo_cells
        .checked_mul(core_depth)
        .and_then(|values| values.checked_mul(2))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let merge_complex_values = grid_shape[0]
        .checked_mul(grid_shape[1])
        .and_then(|cells| cells.checked_mul(core_depth))
        .and_then(|values| values.checked_mul(2))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let peak_complex_values = sector_accumulator_complex_values
        .checked_add(merge_complex_values)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(GriddedNormalSectorResidency {
        sector_accumulator_complex_values,
        merge_complex_values,
        peak_complex_values,
    })
}

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
        let maximum_records = self
            .manifest
            .descriptors
            .iter()
            .map(|descriptor| descriptor.record_count)
            .max()
            .ok_or(SpectralOperatorError::ResidencyOverflow)
            .and_then(|records| {
                usize::try_from(records).map_err(|_| SpectralOperatorError::ResidencyOverflow)
            })?;
        self.begin_apply_with_route_capacities(problem, model, prior, prepared, &[maximum_records])
    }

    /// Bind the runtime-planned retained route slots to the existing apply owner.
    #[doc(hidden)]
    pub fn begin_apply_with_route_capacities(
        &self,
        problem: &CompiledProblem,
        model: &ModelGeneration,
        prior: FinalNormalState,
        prepared: PreparedSpectralOperator,
        route_slot_record_capacities: &[usize],
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
        let mut operator =
            SpectralSlabOperator::new_gridded_normal(prepared_specification, workload, fft);
        operator.prepare_gridded_normal_model(model, prior.into_reusable())?;
        let prepared =
            PreparedGriddedNormalWindow::with_record_capacities(route_slot_record_capacities)?;
        let sector_shape = self.manifest.specification.grid_shape();
        let sector_depth = self.manifest.specification.slab().core_depth();
        Ok(GriddedNormalOperatorApply {
            program: self.clone(),
            operator,
            model_generation,
            next_block_sequence: 0,
            applied_records: 0,
            next_sector_commit: 0,
            prepared: RwLock::new(prepared),
            routing: GriddedNormalRoutingCounters::default(),
            sectors: std::array::from_fn(|sector_id| {
                Mutex::new(GriddedNormalSectorAccumulator::new(
                    sector_shape,
                    sector_depth,
                    sector_id,
                ))
            }),
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
    next_sector_commit: usize,
    prepared: RwLock<PreparedGriddedNormalWindow>,
    routing: GriddedNormalRoutingCounters,
    sectors: [Mutex<GriddedNormalSectorAccumulator>; GRIDDED_NORMAL_SECTOR_COUNT],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct GriddedNormalRoute {
    record_ordinal: u32,
    group_ordinal: u32,
}

#[derive(Default)]
struct PreparedGriddedNormalBlock {
    sequence: Option<u64>,
    record_count: u64,
    predictions: Vec<Complex64>,
    classifications: Vec<u32>,
    routes: Vec<GriddedNormalRoute>,
    sector_offsets: [u32; GRIDDED_NORMAL_SECTOR_COUNT + 1],
}

struct PreparedGriddedNormalWindow {
    blocks: Vec<PreparedGriddedNormalBlock>,
    active_frames: usize,
    first_sequence: Option<u64>,
    record_count: u64,
}

impl PreparedGriddedNormalWindow {
    fn with_record_capacities(record_capacities: &[usize]) -> Result<Self, SpectralOperatorError> {
        if record_capacities.is_empty() {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(record_capacities.len())
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        if blocks.capacity() != record_capacities.len() {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        for &record_capacity in record_capacities {
            blocks.push(PreparedGriddedNormalBlock::with_record_capacity(
                record_capacity,
            )?);
        }
        Ok(Self {
            blocks,
            active_frames: 0,
            first_sequence: None,
            record_count: 0,
        })
    }

    fn clear_active(&mut self) -> Result<(), SpectralOperatorError> {
        for block in self.blocks.iter_mut().take(self.active_frames) {
            let sequence = block.sequence.ok_or(SpectralOperatorError::BlockSequence)?;
            block.finish_block(sequence, block.record_count)?;
        }
        self.active_frames = 0;
        self.first_sequence = None;
        self.record_count = 0;
        Ok(())
    }

    fn capacity_bytes(&self) -> Result<u64, SpectralOperatorError> {
        let block_metadata = capacity_bytes::<PreparedGriddedNormalBlock>(self.blocks.capacity())?;
        self.blocks.iter().try_fold(block_metadata, |total, block| {
            total
                .checked_add(block.heap_capacity_bytes()?)
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })
    }

    fn active_block(
        &self,
        ordinal: usize,
    ) -> Result<&PreparedGriddedNormalBlock, SpectralOperatorError> {
        if ordinal >= self.active_frames {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.blocks
            .get(ordinal)
            .ok_or(SpectralOperatorError::BlockSequence)
    }
}

impl PreparedGriddedNormalBlock {
    fn with_record_capacity(record_capacity: usize) -> Result<Self, SpectralOperatorError> {
        let mut block = Self::default();
        block
            .predictions
            .try_reserve_exact(record_capacity)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        block
            .classifications
            .try_reserve_exact(record_capacity)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        block
            .routes
            .try_reserve_exact(record_capacity)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        if block.record_capacity()? != record_capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(block)
    }

    fn record_capacity(&self) -> Result<usize, SpectralOperatorError> {
        let capacity = self.routes.capacity();
        if self.predictions.capacity() != capacity || self.classifications.capacity() != capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(capacity)
    }

    fn prepare<P>(
        &mut self,
        sequence: u64,
        encoded: &[u8],
        grid_shape: [usize; 2],
        output_channels: usize,
        mut predict: P,
    ) -> Result<(), SpectralOperatorError>
    where
        P: FnMut(DecodedRecord) -> Result<Complex64, SpectralOperatorError>,
    {
        if self.sequence.is_some() {
            return Err(SpectralOperatorError::BlockSequence);
        }
        if encoded.len() % GRIDDED_NORMAL_OPERATOR_RECORD_BYTES != 0 {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let record_count = encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
        let record_capacity = self.record_capacity()?;
        if record_count > record_capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let record_count_u32 =
            u32::try_from(record_count).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        self.predictions.clear();
        self.classifications.clear();
        self.routes.clear();
        self.sector_offsets.fill(0);

        let prepared = (|| {
            let mut prediction = Complex64::default();
            let mut group_open = false;
            let mut sector_counts = [0_u32; GRIDDED_NORMAL_SECTOR_COUNT];
            for bytes in encoded.chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES) {
                let record = decode_record(bytes, grid_shape, output_channels)?;
                let group_ordinal = u32::try_from(self.predictions.len())
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                if group_ordinal > u32::MAX >> 2 {
                    return Err(SpectralOperatorError::CoverageOverflow);
                }
                let sector_id = sector_for_taps(record.taps, grid_shape)?;
                self.classifications
                    .push((group_ordinal << 2) | sector_id as u32);
                sector_counts[sector_id] = sector_counts[sector_id]
                    .checked_add(1)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                prediction += predict(record)?;
                group_open = !record.group_end;
                if record.group_end {
                    if !prediction.re.is_finite() || !prediction.im.is_finite() {
                        return Err(SpectralOperatorError::GeneratedNonfinite);
                    }
                    self.predictions.push(prediction);
                    prediction = Complex64::default();
                }
            }
            if group_open {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }

            for (sector_id, sector_count) in sector_counts.into_iter().enumerate() {
                self.sector_offsets[sector_id + 1] = self.sector_offsets[sector_id]
                    .checked_add(sector_count)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
            }
            if self.sector_offsets[GRIDDED_NORMAL_SECTOR_COUNT] != record_count_u32 {
                return Err(SpectralOperatorError::IncompleteCoverage);
            }
            self.routes.resize(
                record_count,
                GriddedNormalRoute {
                    record_ordinal: 0,
                    group_ordinal: 0,
                },
            );
            let mut sector_cursors = [
                self.sector_offsets[0],
                self.sector_offsets[1],
                self.sector_offsets[2],
                self.sector_offsets[3],
            ];
            for (record_ordinal, classification) in self.classifications.iter().copied().enumerate()
            {
                let sector_id = usize::try_from(classification & 0b11)
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                let route_ordinal = usize::try_from(sector_cursors[sector_id])
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                self.routes[route_ordinal] = GriddedNormalRoute {
                    record_ordinal: u32::try_from(record_ordinal)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    group_ordinal: classification >> 2,
                };
                sector_cursors[sector_id] = sector_cursors[sector_id]
                    .checked_add(1)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
            }
            for (sector_id, sector_cursor) in sector_cursors.into_iter().enumerate() {
                if sector_cursor != self.sector_offsets[sector_id + 1] {
                    return Err(SpectralOperatorError::IncompleteCoverage);
                }
            }
            self.classifications.clear();
            self.record_count =
                u64::try_from(record_count).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            self.sequence = Some(sequence);
            if self.record_capacity()? != record_capacity {
                return Err(SpectralOperatorError::ResidencyOverflow);
            }
            Ok(())
        })();
        if prepared.is_err() {
            self.record_count = 0;
            self.predictions.clear();
            self.classifications.clear();
            self.routes.clear();
            self.sector_offsets.fill(0);
        }
        prepared
    }

    fn heap_capacity_bytes(&self) -> Result<u64, SpectralOperatorError> {
        let prediction_bytes = capacity_bytes::<Complex64>(self.predictions.capacity())?;
        let classification_bytes = capacity_bytes::<u32>(self.classifications.capacity())?;
        let route_bytes = capacity_bytes::<GriddedNormalRoute>(self.routes.capacity())?;
        prediction_bytes
            .checked_add(classification_bytes)
            .and_then(|total| total.checked_add(route_bytes))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    #[cfg(test)]
    fn capacity_bytes(&self) -> Result<u64, SpectralOperatorError> {
        self.heap_capacity_bytes()?
            .checked_add(size_of::<Self>() as u64)
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    fn routes_for_sector(
        &self,
        sector_id: usize,
    ) -> Result<&[GriddedNormalRoute], SpectralOperatorError> {
        let start = usize::try_from(
            *self
                .sector_offsets
                .get(sector_id)
                .ok_or(SpectralOperatorError::IncompleteCoverage)?,
        )
        .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        let end = usize::try_from(
            *self
                .sector_offsets
                .get(sector_id + 1)
                .ok_or(SpectralOperatorError::IncompleteCoverage)?,
        )
        .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        self.routes
            .get(start..end)
            .ok_or(SpectralOperatorError::IncompleteCoverage)
    }

    fn finish_block(
        &mut self,
        sequence: u64,
        record_count: u64,
    ) -> Result<(), SpectralOperatorError> {
        if self.sequence != Some(sequence) || self.record_count != record_count {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.sequence = None;
        self.record_count = 0;
        self.predictions.clear();
        self.classifications.clear();
        self.routes.clear();
        self.sector_offsets.fill(0);
        Ok(())
    }
}

fn capacity_bytes<T>(capacity: usize) -> Result<u64, SpectralOperatorError> {
    u64::try_from(capacity)
        .ok()
        .and_then(|capacity| capacity.checked_mul(size_of::<T>() as u64))
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

#[derive(Default)]
struct GriddedNormalRoutingCounters {
    frames_routed: AtomicU64,
    encoded_records: AtomicU64,
    routed_record_memberships: AtomicU64,
    prediction_groups: AtomicU64,
    degrid_records: AtomicU64,
    grid_records: AtomicU64,
    sector_rescans: AtomicU64,
    peak_physical_route_capacity_bytes: AtomicU64,
}

impl GriddedNormalRoutingCounters {
    fn record_routing(
        &self,
        frames: u64,
        encoded_records: u64,
        routed_record_memberships: u64,
        prediction_groups: u64,
        physical_capacity_bytes: u64,
    ) {
        self.frames_routed.fetch_add(frames, Ordering::Relaxed);
        self.encoded_records
            .fetch_add(encoded_records, Ordering::Relaxed);
        self.routed_record_memberships
            .fetch_add(routed_record_memberships, Ordering::Relaxed);
        self.prediction_groups
            .fetch_add(prediction_groups, Ordering::Relaxed);
        self.degrid_records
            .fetch_add(encoded_records, Ordering::Relaxed);
        self.peak_physical_route_capacity_bytes
            .fetch_max(physical_capacity_bytes, Ordering::Relaxed);
    }

    fn record_grid(&self, records: u64) {
        self.grid_records.fetch_add(records, Ordering::Relaxed);
    }

    fn snapshot(&self) -> GriddedNormalRoutingMeasurements {
        GriddedNormalRoutingMeasurements {
            frames_routed: self.frames_routed.load(Ordering::Relaxed),
            encoded_records: self.encoded_records.load(Ordering::Relaxed),
            routed_record_memberships: self.routed_record_memberships.load(Ordering::Relaxed),
            prediction_groups: self.prediction_groups.load(Ordering::Relaxed),
            degrid_records: self.degrid_records.load(Ordering::Relaxed),
            grid_records: self.grid_records.load(Ordering::Relaxed),
            sector_rescans: self.sector_rescans.load(Ordering::Relaxed),
            peak_physical_route_capacity_bytes: self
                .peak_physical_route_capacity_bytes
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GriddedNormalSectorGeometry {
    sector_id: usize,
    origin: [usize; 2],
    shape: [usize; 2],
}

impl GriddedNormalSectorGeometry {
    fn new(grid_shape: [usize; 2], sector_id: usize) -> Result<Self, SpectralOperatorError> {
        if sector_id >= GRIDDED_NORMAL_SECTOR_COUNT
            || grid_shape.into_iter().any(|extent| extent < 2)
        {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let split = [grid_shape[0] / 2, grid_shape[1] / 2];
        let upper = [sector_id / 2 != 0, sector_id % 2 != 0];
        let core_start = [
            if upper[0] { split[0] } else { 0 },
            if upper[1] { split[1] } else { 0 },
        ];
        let core_end = [
            if upper[0] { grid_shape[0] } else { split[0] },
            if upper[1] { grid_shape[1] } else { split[1] },
        ];
        let origin = [
            core_start[0].saturating_sub(SUPPORT),
            core_start[1].saturating_sub(SUPPORT),
        ];
        let halo_end = [
            core_end[0].saturating_add(SUPPORT).min(grid_shape[0]),
            core_end[1].saturating_add(SUPPORT).min(grid_shape[1]),
        ];
        let shape = [
            halo_end[0]
                .checked_sub(origin[0])
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
            halo_end[1]
                .checked_sub(origin[1])
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        ];
        Ok(Self {
            sector_id,
            origin,
            shape,
        })
    }

    fn cell_count(self) -> Option<usize> {
        self.shape[0].checked_mul(self.shape[1])
    }

    fn translated_taps(self, taps: SampleTaps) -> Result<SampleTaps, SpectralOperatorError> {
        let translated = SampleTaps {
            x: TapSpan {
                start: taps
                    .x
                    .start
                    .checked_sub(self.origin[0])
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                weight_index: taps.x.weight_index,
            },
            y: TapSpan {
                start: taps
                    .y
                    .start
                    .checked_sub(self.origin[1])
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                weight_index: taps.y.weight_index,
            },
        };
        if translated
            .x
            .start
            .checked_add(2 * SUPPORT)
            .is_none_or(|end| end >= self.shape[0])
            || translated
                .y
                .start
                .checked_add(2 * SUPPORT)
                .is_none_or(|end| end >= self.shape[1])
        {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        Ok(translated)
    }
}

struct GriddedNormalSectorAccumulator {
    geometry: GriddedNormalSectorGeometry,
    grids: Vec<Array2<Complex64>>,
    compensations: Vec<Array2<Complex64>>,
}

impl GriddedNormalSectorAccumulator {
    fn new(grid_shape: [usize; 2], core_depth: usize, sector_id: usize) -> Self {
        let geometry = GriddedNormalSectorGeometry::new(grid_shape, sector_id)
            .expect("validated gridded-normal geometry admits four sectors");
        let shape = (geometry.shape[0], geometry.shape[1]);
        let planes = || (0..core_depth).map(|_| Array2::zeros(shape)).collect();
        Self {
            geometry,
            grids: planes(),
            compensations: planes(),
        }
    }

    #[inline(never)]
    fn execute_routes(
        &mut self,
        operator: &SpectralSlabOperator,
        encoded: &[u8],
        grid_shape: [usize; 2],
        output_channels: usize,
        sector_id: usize,
        prepared: &PreparedGriddedNormalBlock,
    ) -> Result<(), SpectralOperatorError> {
        if sector_id >= GRIDDED_NORMAL_SECTOR_COUNT
            || encoded.len() % GRIDDED_NORMAL_OPERATOR_RECORD_BYTES != 0
        {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let routes = prepared.routes_for_sector(sector_id)?;
        for route in routes {
            let start = usize::try_from(route.record_ordinal)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            let end = start
                .checked_add(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            let record = decode_record(
                encoded
                    .get(start..end)
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                grid_shape,
                output_channels,
            )?;
            if sector_for_taps(record.taps, grid_shape)? != sector_id {
                return Err(SpectralOperatorError::GriddedRecordMismatch);
            }
            let predicted = *prepared
                .predictions
                .get(
                    usize::try_from(route.group_ordinal)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                )
                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
            operator.grid_gridded_normal_sector(
                &mut self.grids,
                &mut self.compensations,
                self.geometry.translated_taps(record.taps)?,
                record.output_channel,
                predicted,
                record.forward_scale.conj() * record.imaging_weight,
            )?;
        }
        Ok(())
    }
}

/// One stable center-owned spatial partition prepared by reconstruction.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalSectorWork {
    first_block_sequence: u64,
    frame_count: u64,
    sector_id: usize,
    window_record_count: u64,
    routed_record_count: u64,
    tap_visit_count: u64,
    shared_route_capacity_bytes: u64,
}

impl GriddedNormalSectorWork {
    /// Return the worker-count-independent spatial-sector key.
    #[must_use]
    pub const fn partition_key(self) -> u64 {
        self.sector_id as u64
    }

    /// Return the exclusive reconstruction accumulation region.
    #[must_use]
    pub const fn sector_id(self) -> u64 {
        self.sector_id as u64
    }

    /// Return exact encoded records owned by this sector.
    #[must_use]
    pub const fn routed_record_count(self) -> u64 {
        self.routed_record_count
    }

    /// Return exact convolutional tap visits owned by this sector.
    #[must_use]
    pub const fn tap_visit_count(self) -> u64 {
        self.tap_visit_count
    }

    /// Return shared reusable route capacity exactly once across the four works.
    #[must_use]
    pub const fn shared_route_capacity_bytes(self) -> u64 {
        if self.sector_id == 0 {
            self.shared_route_capacity_bytes
        } else {
            0
        }
    }
}

fn validate_sector_partition_ordinal(
    next_sector_commit: usize,
    local_ordinal: usize,
) -> Result<(), SpectralOperatorError> {
    if local_ordinal >= GRIDDED_NORMAL_SECTOR_COUNT || local_ordinal < next_sector_commit {
        Err(SpectralOperatorError::IncompleteCoverage)
    } else {
        Ok(())
    }
}

/// Fixed-size evidence that one sector applied a complete borrowed block.
#[doc(hidden)]
#[derive(Debug)]
pub struct GriddedNormalSectorPartial {
    work: GriddedNormalSectorWork,
}

impl GriddedNormalOperatorApply {
    /// Validate one borrowed frame and return the four stable spatial owners.
    pub fn sector_partition_count(
        &self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<usize, SpectralOperatorError> {
        self.sector_window_partition_count(std::iter::once((sequence, encoded)))
    }

    /// Validate one ordered borrowed-frame window and prepare its shared routes.
    pub fn sector_window_partition_count<'a, I>(
        &self,
        frames: I,
    ) -> Result<usize, SpectralOperatorError>
    where
        I: IntoIterator<Item = (u64, &'a [u8])>,
    {
        if self.next_sector_commit != 0 {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let mut prepared = self
            .prepared
            .write()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if prepared.active_frames != 0 {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let frames = frames.into_iter();
        let (minimum_frames, maximum_frames) = frames.size_hint();
        if maximum_frames == Some(minimum_frames) && minimum_frames > prepared.blocks.len() {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let mut encoded_records = 0_u64;
        let mut routed_record_memberships = 0_u64;
        let mut prediction_groups = 0_u64;
        let mut frame_count = 0usize;
        let result = (|| {
            for (ordinal, (sequence, encoded)) in frames.enumerate() {
                let ordinal_u64 =
                    u64::try_from(ordinal).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                let expected = self
                    .next_block_sequence
                    .checked_add(ordinal_u64)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                if sequence != expected {
                    return Err(SpectralOperatorError::BlockSequence);
                }
                let descriptor = self
                    .program
                    .manifest
                    .descriptors
                    .get(
                        usize::try_from(sequence)
                            .map_err(|_| SpectralOperatorError::GriddedRecordMismatch)?,
                    )
                    .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
                validate_encoded_block(descriptor, encoded)?;
                let (routed, predictions) = {
                    let block = prepared
                        .blocks
                        .get_mut(ordinal)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                    block.prepare(
                        sequence,
                        encoded,
                        self.program.manifest.specification.grid_shape(),
                        self.program.manifest.specification.slab().total_channels(),
                        |record| {
                            self.operator.predict_gridded_normal(
                                record.output_channel,
                                record.taps,
                                record.forward_scale,
                            )
                        },
                    )?;
                    (
                        u64::try_from(block.routes.len())
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                        u64::try_from(block.predictions.len())
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    )
                };
                prepared.active_frames = ordinal + 1;
                encoded_records = encoded_records
                    .checked_add(descriptor.record_count)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                if routed != descriptor.record_count {
                    return Err(SpectralOperatorError::IncompleteCoverage);
                }
                routed_record_memberships = routed_record_memberships
                    .checked_add(routed)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                prediction_groups = prediction_groups
                    .checked_add(predictions)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                frame_count = ordinal + 1;
            }
            if frame_count == 0 {
                return Err(SpectralOperatorError::IncompleteCoverage);
            }
            prepared.first_sequence = Some(self.next_block_sequence);
            prepared.record_count = encoded_records;
            prepared.capacity_bytes()
        })();
        let physical_capacity_bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                prepared.clear_active()?;
                return Err(error);
            }
        };
        self.routing.record_routing(
            u64::try_from(frame_count).map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            encoded_records,
            routed_record_memberships,
            prediction_groups,
            physical_capacity_bytes,
        );
        Ok(GRIDDED_NORMAL_SECTOR_COUNT)
    }

    /// Return one worker-count-independent spatial partition by local ordinal.
    pub fn sector_partition(
        &self,
        sequence: u64,
        encoded: &[u8],
        local_ordinal: usize,
    ) -> Result<GriddedNormalSectorWork, SpectralOperatorError> {
        let descriptor = self
            .program
            .manifest
            .descriptors
            .get(
                usize::try_from(sequence)
                    .map_err(|_| SpectralOperatorError::GriddedRecordMismatch)?,
            )
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        validate_encoded_block(descriptor, encoded)?;
        self.sector_window_partition(sequence, 1, local_ordinal)
    }

    /// Return one worker-count-independent sector for the active frame window.
    pub fn sector_window_partition(
        &self,
        first_sequence: u64,
        frame_count: usize,
        local_ordinal: usize,
    ) -> Result<GriddedNormalSectorWork, SpectralOperatorError> {
        validate_sector_partition_ordinal(self.next_sector_commit, local_ordinal)?;
        let prepared = self
            .prepared
            .read()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if prepared.first_sequence != Some(first_sequence)
            || prepared.active_frames != frame_count
            || first_sequence != self.next_block_sequence
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let routed_record_count =
            (0..prepared.active_frames).try_fold(0_u64, |total, ordinal| {
                let routes = prepared
                    .active_block(ordinal)?
                    .routes_for_sector(local_ordinal)?;
                total
                    .checked_add(
                        u64::try_from(routes.len())
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    )
                    .ok_or(SpectralOperatorError::CoverageOverflow)
            })?;
        Ok(GriddedNormalSectorWork {
            first_block_sequence: first_sequence,
            frame_count: u64::try_from(frame_count)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            sector_id: local_ordinal,
            window_record_count: prepared.record_count,
            routed_record_count,
            tap_visit_count: routed_record_count
                .checked_mul(GRIDDED_NORMAL_TAPS_PER_RECORD)
                .ok_or(SpectralOperatorError::CoverageOverflow)?,
            shared_route_capacity_bytes: prepared.capacity_bytes()?,
        })
    }

    /// Fuse full-group prediction and compensated gridding into one spatial owner.
    pub fn execute_sector(
        &self,
        encoded: &[u8],
        work: GriddedNormalSectorWork,
    ) -> Result<GriddedNormalSectorPartial, SpectralOperatorError> {
        self.execute_sector_window(std::iter::once((work.first_block_sequence, encoded)), work)
    }

    /// Apply one sector to every prepared frame in increasing source order.
    pub fn execute_sector_window<'a, I>(
        &self,
        frames: I,
        work: GriddedNormalSectorWork,
    ) -> Result<GriddedNormalSectorPartial, SpectralOperatorError>
    where
        I: IntoIterator<Item = (u64, &'a [u8])>,
    {
        if work.first_block_sequence != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let prepared = self
            .prepared
            .read()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if prepared.first_sequence != Some(work.first_block_sequence)
            || u64::try_from(prepared.active_frames)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                != work.frame_count
            || prepared.record_count != work.window_record_count
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let mut sector = self.sectors[work.sector_id]
            .lock()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        let operator = &self.operator;
        let mut route_count = 0_u64;
        let mut frame_count = 0usize;
        for (ordinal, (sequence, encoded)) in frames.into_iter().enumerate() {
            let block = prepared.active_block(ordinal)?;
            if block.sequence != Some(sequence) {
                return Err(SpectralOperatorError::BlockSequence);
            }
            route_count = route_count
                .checked_add(
                    u64::try_from(block.routes_for_sector(work.sector_id)?.len())
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                )
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            sector.execute_routes(
                operator,
                encoded,
                self.program.manifest.specification.grid_shape(),
                self.program.manifest.specification.slab().total_channels(),
                work.sector_id,
                block,
            )?;
            frame_count = ordinal + 1;
        }
        if u64::try_from(frame_count).map_err(|_| SpectralOperatorError::CoverageOverflow)?
            != work.frame_count
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.routing.record_grid(route_count);
        Ok(GriddedNormalSectorPartial { work })
    }

    /// Commit fixed-size coverage bookkeeping after sector-owned science completed.
    pub fn commit_sector(
        &mut self,
        partial: GriddedNormalSectorPartial,
    ) -> Result<(), SpectralOperatorError> {
        let work = partial.work;
        if work.first_block_sequence != self.next_block_sequence
            || work.sector_id != self.next_sector_commit
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        if self.next_sector_commit + 1 == GRIDDED_NORMAL_SECTOR_COUNT {
            let applied_records = self
                .applied_records
                .checked_add(work.window_record_count)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            let next_block_sequence = self
                .next_block_sequence
                .checked_add(work.frame_count)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            self.prepared
                .write()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?
                .clear_active()?;
            self.applied_records = applied_records;
            self.next_block_sequence = next_block_sequence;
            self.next_sector_commit = 0;
        } else {
            self.next_sector_commit += 1;
        }
        Ok(())
    }

    /// Validate and apply one borrowed frame through the spatial-owner path.
    pub fn apply_encoded_block(
        &mut self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<(), SpectralOperatorError> {
        let partition_count = self.sector_partition_count(sequence, encoded)?;
        for ordinal in 0..partition_count {
            let work = self.sector_partition(sequence, encoded, ordinal)?;
            let partial = self.execute_sector(encoded, work)?;
            self.commit_sector(partial)?;
        }
        Ok(())
    }

    /// Snapshot exact route-once measurements without changing execution state.
    #[must_use]
    pub fn routing_measurements(&self) -> GriddedNormalRoutingMeasurements {
        self.routing.snapshot()
    }

    /// Finish `dirty - A* W A x` and return ordinary Major-Cycle input.
    pub fn finish(self) -> Result<CompleteDataOwnerResult, SpectralOperatorError> {
        self.finish_with_routing_measurements()
            .map(|(result, _measurements)| result)
    }

    /// Finish and return the final immutable route-once measurement snapshot.
    pub fn finish_with_routing_measurements(
        self,
    ) -> Result<(CompleteDataOwnerResult, GriddedNormalRoutingMeasurements), SpectralOperatorError>
    {
        if self.next_block_sequence != self.program.block_count()
            || self.applied_records != self.program.manifest.record_count
            || self.next_sector_commit != 0
            || self
                .prepared
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?
                .active_frames
                != 0
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let measurements = self.routing_measurements();
        let Self {
            program,
            operator,
            model_generation,
            sectors,
            ..
        } = self;
        let normal_grids = merge_sector_accumulators(
            sectors,
            program.manifest.specification.grid_shape(),
            program.manifest.specification.slab().core_depth(),
        )?;
        let primitives =
            operator.finish_gridded_normal_from_grids(model_generation, normal_grids)?;
        let primitive_catalog = if program.manifest.specification.slab().total_channels() == 1 {
            SpectralPrimitiveCatalog::UnnormalizedPlaneV1
        } else {
            SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1
        };
        Ok((
            CompleteDataOwnerResult {
                primitives,
                completion: CompleteDataOwnerCompletion {
                    problem: program.manifest.specification.problem_id(),
                    geometry: program.manifest.specification.geometry_id(),
                    numerics: program.manifest.specification.numerics_id(),
                    weighting_commitment: program.manifest.specification.weighting_commitment_id(),
                    weighting_generation: program.manifest.weighting_generation,
                    replay: program.manifest.replay,
                    coverage: program.manifest.coverage,
                    coverage_proof_bytes: 0,
                    coverage_proof_hash_calls: 0,
                    primitives: primitive_catalog,
                    selected_generation: program.manifest.selected_generation,
                    continuum_transform_generation: program.manifest.continuum_transform_generation,
                    sample_count: program.manifest.sample_count,
                    block_count: program.block_count(),
                },
            },
            measurements,
        ))
    }
}

#[cfg(test)]
fn execute_sector_routes_for_test<A>(
    encoded: &[u8],
    grid_shape: [usize; 2],
    output_channels: usize,
    sector_id: usize,
    prepared: &PreparedGriddedNormalBlock,
    mut apply: A,
) -> Result<(), SpectralOperatorError>
where
    A: FnMut(DecodedRecord, Complex64) -> Result<(), SpectralOperatorError>,
{
    if sector_id >= GRIDDED_NORMAL_SECTOR_COUNT
        || encoded.len() % GRIDDED_NORMAL_OPERATOR_RECORD_BYTES != 0
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    for route in prepared.routes_for_sector(sector_id)? {
        let start = usize::try_from(route.record_ordinal)
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?
            .checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let end = start
            .checked_add(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let record = decode_record(
            encoded
                .get(start..end)
                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
            grid_shape,
            output_channels,
        )?;
        if sector_for_taps(record.taps, grid_shape)? != sector_id {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        let predicted = *prepared
            .predictions
            .get(
                usize::try_from(route.group_ordinal)
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            )
            .ok_or(SpectralOperatorError::IncompleteCoverage)?;
        apply(record, predicted)?;
    }
    Ok(())
}

fn sector_for_taps(
    taps: SampleTaps,
    grid_shape: [usize; 2],
) -> Result<usize, SpectralOperatorError> {
    let center = [
        taps.x
            .start
            .checked_add(SUPPORT)
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
        taps.y
            .start
            .checked_add(SUPPORT)
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
    ];
    if center[0] >= grid_shape[0] || center[1] >= grid_shape[1] {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(usize::from(center[0] >= grid_shape[0] / 2) * 2
        + usize::from(center[1] >= grid_shape[1] / 2))
}

fn merge_sector_accumulators(
    sectors: [Mutex<GriddedNormalSectorAccumulator>; GRIDDED_NORMAL_SECTOR_COUNT],
    grid_shape: [usize; 2],
    core_depth: usize,
) -> Result<Vec<Array2<Complex64>>, SpectralOperatorError> {
    let shape = (grid_shape[0], grid_shape[1]);
    let mut grids: Vec<Array2<Complex64>> = (0..core_depth).map(|_| Array2::zeros(shape)).collect();
    let mut compensations: Vec<Array2<Complex64>> =
        (0..core_depth).map(|_| Array2::zeros(shape)).collect();
    for (sector_id, sector) in sectors.into_iter().enumerate() {
        let sector = sector
            .into_inner()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if sector.geometry.sector_id != sector_id
            || sector.grids.len() != core_depth
            || sector.compensations.len() != core_depth
        {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        for plane in 0..core_depth {
            for ((local_x, local_y), value) in sector.grids[plane].indexed_iter() {
                if *value == Complex64::default() {
                    continue;
                }
                let cell = &mut grids[plane][(
                    sector.geometry.origin[0] + local_x,
                    sector.geometry.origin[1] + local_y,
                )];
                let compensation = &mut compensations[plane][(
                    sector.geometry.origin[0] + local_x,
                    sector.geometry.origin[1] + local_y,
                )];
                let contribution = *value - *compensation;
                let updated = *cell + contribution;
                *compensation = (updated - *cell) - contribution;
                *cell = updated;
            }
        }
    }
    Ok(grids)
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
    fn spatial_sectors_preserve_group_boundaries_and_schedule_independent_products() {
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
        let mut taps_by_sector = BTreeMap::new();
        for x in -100..=100 {
            for y in -100..=100 {
                if let Some(taps) = gridder.taps([f64::from(x), f64::from(y)]) {
                    let sector = sector_for_taps(taps, geometry.grid_shape).expect("sector");
                    taps_by_sector.entry(sector).or_insert(taps);
                }
            }
        }
        assert_eq!(taps_by_sector.len(), GRIDDED_NORMAL_SECTOR_COUNT);
        let cross_sector_group = taps_by_sector
            .values()
            .copied()
            .enumerate()
            .map(|(index, taps)| ReducedRecordKey {
                output_channel: 0,
                taps: encode_taps(taps).expect("encode grouped taps"),
                forward_real: (1.0 + index as f64 * 0.125).to_bits(),
                forward_imaginary: (index as f64 * -0.025).to_bits(),
                imaging_weight: (0.5 + index as f64 * 0.125).to_bits(),
            })
            .collect::<Vec<_>>();
        let mut groups = BTreeMap::new();
        groups.insert(cross_sector_group, vec![1.0]);
        let second_taps = *taps_by_sector.get(&0).expect("lower sector taps");
        groups.insert(
            vec![ReducedRecordKey {
                output_channel: 0,
                taps: encode_taps(second_taps).expect("encode scalar taps"),
                forward_real: 0.75_f64.to_bits(),
                forward_imaginary: 0.125_f64.to_bits(),
                imaging_weight: 0.25_f64.to_bits(),
            }],
            vec![1.0],
        );
        let encoded = encode_reduced(groups).expect("encode two groups");

        let mut expected = Array2::<Complex64>::zeros(shape);
        let mut expected_compensation = Array2::<Complex64>::zeros(shape);
        let mut group_start = 0;
        let mut group_count = 0;
        while group_start < encoded.len() {
            let mut group_end = group_start;
            loop {
                group_end += GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
                let record = decode_record(
                    &encoded[group_end - GRIDDED_NORMAL_OPERATOR_RECORD_BYTES..group_end],
                    geometry.grid_shape,
                    1,
                )
                .expect("decode group record");
                if record.group_end {
                    break;
                }
            }
            let records = encoded[group_start..group_end]
                .chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .map(|bytes| decode_record(bytes, geometry.grid_shape, 1).expect("decode record"))
                .collect::<Vec<_>>();
            let predicted = records.iter().fold(Complex64::default(), |sum, record| {
                sum + gridder.degrid(&model_grid, record.taps) * record.forward_scale
            });
            for record in records {
                gridder.grid_compensated(
                    &mut expected,
                    &mut expected_compensation,
                    record.taps,
                    predicted * record.forward_scale.conj() * record.imaging_weight,
                );
            }
            group_count += 1;
            group_start = group_end;
        }
        assert_eq!(group_count, 2);

        let run = |execution_order: [usize; GRIDDED_NORMAL_SECTOR_COUNT]| {
            let mut degrid_records = 0_u64;
            let record_count = encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
            let mut prepared = PreparedGriddedNormalBlock::with_record_capacity(record_count)
                .expect("planned route capacity");
            prepared
                .prepare(7, &encoded, geometry.grid_shape, 1, |record| {
                    degrid_records += 1;
                    Ok(gridder.degrid(&model_grid, record.taps) * record.forward_scale)
                })
                .expect("route encoded frame once");
            let route_capacity_bytes = prepared.capacity_bytes().expect("route capacity");
            let identities = (0..GRIDDED_NORMAL_SECTOR_COUNT)
                .map(|sector_id| {
                    let routed_record_count = u64::try_from(
                        prepared
                            .routes_for_sector(sector_id)
                            .expect("sector routes")
                            .len(),
                    )
                    .expect("routed record count fits");
                    GriddedNormalSectorWork {
                        first_block_sequence: 7,
                        frame_count: 1,
                        sector_id,
                        window_record_count: u64::try_from(
                            encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES,
                        )
                        .expect("record count fits"),
                        routed_record_count,
                        tap_visit_count: routed_record_count * GRIDDED_NORMAL_TAPS_PER_RECORD,
                        shared_route_capacity_bytes: route_capacity_bytes,
                    }
                })
                .map(GriddedNormalSectorWork::partition_key)
                .collect::<Vec<_>>();
            let mut sectors: [GriddedNormalSectorAccumulator; GRIDDED_NORMAL_SECTOR_COUNT] =
                std::array::from_fn(|sector_id| {
                    GriddedNormalSectorAccumulator::new(geometry.grid_shape, 1, sector_id)
                });
            for sector_id in execution_order {
                let sector = &mut sectors[sector_id];
                execute_sector_routes_for_test(
                    &encoded,
                    geometry.grid_shape,
                    1,
                    sector_id,
                    &prepared,
                    |record, predicted| {
                        let local_taps = sector.geometry.translated_taps(record.taps)?;
                        gridder.grid_compensated(
                            &mut sector.grids[0],
                            &mut sector.compensations[0],
                            local_taps,
                            predicted * record.forward_scale.conj() * record.imaging_weight,
                        );
                        Ok(())
                    },
                )
                .expect("execute sector");
            }
            let [grid] = merge_sector_accumulators(sectors.map(Mutex::new), geometry.grid_shape, 1)
                .expect("merge sectors")
                .try_into()
                .expect("one output channel");
            (
                identities,
                grid,
                degrid_records,
                prepared.predictions.len(),
                prepared.routes.len(),
                route_capacity_bytes,
            )
        };

        let (
            serial_identities,
            serial_grid,
            degrid_records,
            prediction_groups,
            routed_records,
            route_capacity_bytes,
        ) = run([0, 1, 2, 3]);
        let (permuted_identities, permuted_grid, ..) = run([3, 1, 0, 2]);
        assert_eq!(serial_identities, vec![0, 1, 2, 3]);
        assert_eq!(permuted_identities, serial_identities);
        assert_eq!(permuted_grid, serial_grid);
        let record_count = encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
        assert_eq!(degrid_records, u64::try_from(record_count).unwrap());
        assert_eq!(prediction_groups, 2);
        assert_eq!(routed_records, record_count);
        assert_eq!(size_of::<GriddedNormalRoute>(), 8);
        assert!(
            route_capacity_bytes <= gridded_normal_route_capacity_bytes(record_count, 1).unwrap()
        );
        let squared_error = expected
            .iter()
            .zip(&serial_grid)
            .map(|(expected, actual)| (*actual - *expected).norm_sqr())
            .sum::<f64>();
        let squared_reference = expected.iter().map(Complex64::norm_sqr).sum::<f64>();
        let normalized_rms = (squared_error / squared_reference.max(f64::MIN_POSITIVE)).sqrt();
        assert!(normalized_rms <= 0.001, "normalized RMS {normalized_rms}");
    }

    #[test]
    fn sector_partitions_remain_available_after_one_and_two_worker_waves() {
        for worker_count in [1, 2, 4] {
            let mut next_sector_commit = 0;
            while next_sector_commit < GRIDDED_NORMAL_SECTOR_COUNT {
                let wave_end = (next_sector_commit + worker_count).min(GRIDDED_NORMAL_SECTOR_COUNT);
                for ordinal in next_sector_commit..wave_end {
                    validate_sector_partition_ordinal(next_sector_commit, ordinal)
                        .expect("uncommitted sector remains partitionable");
                }
                next_sector_commit = wave_end;
            }
        }
        assert_eq!(
            validate_sector_partition_ordinal(2, 1),
            Err(SpectralOperatorError::IncompleteCoverage)
        );
    }

    #[test]
    fn routing_measurements_report_exact_work_and_physical_peak() {
        let counters = GriddedNormalRoutingCounters::default();
        counters.record_routing(1, 5, 5, 2, 160);
        counters.record_grid(3);
        counters.record_grid(2);
        counters.record_routing(1, 2, 2, 1, 96);
        assert_eq!(
            counters.snapshot(),
            GriddedNormalRoutingMeasurements {
                frames_routed: 2,
                encoded_records: 7,
                routed_record_memberships: 7,
                prediction_groups: 3,
                degrid_records: 7,
                grid_records: 5,
                sector_rescans: 0,
                peak_physical_route_capacity_bytes: 160,
            }
        );
    }

    #[test]
    fn prepared_routes_are_immutable_until_completion_and_reuse_capacity() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let encoded = encode_reduced(scalar_groups([(taps, 1.0)])).expect("encode record");
        let mut prepared =
            PreparedGriddedNormalBlock::with_record_capacity(1).expect("planned route capacity");
        prepared
            .prepare(0, &encoded, geometry.grid_shape, 1, |_| {
                Ok(Complex64::new(1.0, 0.0))
            })
            .expect("prepare first frame");
        let capacity = prepared.capacity_bytes().expect("physical capacity");
        assert_eq!(
            prepared
                .prepare(0, &encoded, geometry.grid_shape, 1, |_| {
                    Ok(Complex64::new(2.0, 0.0))
                })
                .expect_err("active routes cannot be overwritten"),
            SpectralOperatorError::BlockSequence
        );
        assert_eq!(prepared.predictions, vec![Complex64::new(1.0, 0.0)]);
        prepared.finish_block(0, 1).expect("finish all sector work");
        prepared
            .prepare(1, &encoded, geometry.grid_shape, 1, |_| {
                Ok(Complex64::new(2.0, 0.0))
            })
            .expect("reuse storage for next frame");
        assert_eq!(prepared.capacity_bytes().unwrap(), capacity);
        assert_eq!(prepared.predictions, vec![Complex64::new(2.0, 0.0)]);
    }

    #[test]
    fn prepared_window_metadata_and_routes_fit_the_exact_frame_bound() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let encoded = encode_reduced(scalar_groups([(taps, 1.0)])).expect("encode record");

        for frame_count in [1, 3, 64] {
            let mut window =
                PreparedGriddedNormalWindow::with_record_capacities(&vec![1; frame_count])
                    .expect("bounded window route");
            for sequence in 0..frame_count {
                window.blocks[sequence]
                    .prepare(sequence as u64, &encoded, geometry.grid_shape, 1, |_| {
                        Ok(Complex64::new(1.0, 0.0))
                    })
                    .expect("prepare bounded frame");
            }
            assert_eq!(
                window.capacity_bytes().unwrap(),
                gridded_normal_route_capacity_bytes(frame_count, frame_count).unwrap(),
                "{frame_count}-frame route exceeded its exact planner bound"
            );
        }
    }

    #[test]
    fn prepared_two_window_route_reports_exact_3364_peak_without_growth() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let one_record = encode_reduced(scalar_groups([(taps, 1.0)])).expect("encode record");
        let hundred_records = one_record.repeat(100);
        let mut window = PreparedGriddedNormalWindow::with_record_capacities(&[100, 1, 1, 1])
            .expect("planned heterogeneous route");
        let planned_capacity = window.capacity_bytes().expect("physical route capacity");
        assert_eq!(planned_capacity, 3_364);

        window.blocks[0]
            .prepare(0, &hundred_records, geometry.grid_shape, 1, |_| {
                Ok(Complex64::new(1.0, 0.0))
            })
            .expect("prepare one large frame");
        window.active_frames = 1;
        assert_eq!(window.capacity_bytes().unwrap(), planned_capacity);
        window.clear_active().expect("release first window");

        for (ordinal, sequence) in (1_u64..=4).enumerate() {
            window.blocks[ordinal]
                .prepare(sequence, &one_record, geometry.grid_shape, 1, |_| {
                    Ok(Complex64::new(1.0, 0.0))
                })
                .expect("prepare small frame");
            assert_eq!(window.capacity_bytes().unwrap(), planned_capacity);
        }
        window.active_frames = 4;
        window.clear_active().expect("release second window");
        assert_eq!(window.capacity_bytes().unwrap(), planned_capacity);
        assert_eq!(
            window
                .blocks
                .iter()
                .map(PreparedGriddedNormalBlock::record_capacity)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![100, 1, 1, 1]
        );
    }

    #[test]
    fn sector_residency_formula_is_exact_and_worker_independent() {
        let projected = gridded_normal_sector_residency([10, 10], 3).expect("project residency");
        let sectors: [GriddedNormalSectorAccumulator; GRIDDED_NORMAL_SECTOR_COUNT] =
            std::array::from_fn(|sector_id| {
                GriddedNormalSectorAccumulator::new([10, 10], 3, sector_id)
            });
        let actual_sector_values = sectors.iter().fold(0, |total, sector| {
            total
                + sector.grids.iter().map(Array2::len).sum::<usize>()
                + sector.compensations.iter().map(Array2::len).sum::<usize>()
        });
        assert_eq!(actual_sector_values, 1_536);
        assert_eq!(projected.sector_accumulator_complex_values(), 1_536);
        assert_eq!(projected.merge_complex_values(), 600);
        assert_eq!(projected.peak_complex_values(), 2_136);

        let production =
            gridded_normal_sector_residency([1024, 1024], 1).expect("project 1024 grid");
        assert_eq!(production.sector_accumulator_complex_values(), 2_121_800);
        assert_eq!(production.merge_complex_values(), 2_097_152);
        assert_eq!(production.peak_complex_values(), 4_218_952);
        assert_eq!(
            production.peak_complex_values() * size_of::<Complex64>(),
            67_503_232
        );
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
