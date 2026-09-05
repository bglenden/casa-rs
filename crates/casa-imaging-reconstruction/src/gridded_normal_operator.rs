// SPDX-License-Identifier: LGPL-3.0-or-later

//! Run-scoped, disk-streamable normal-operator replay for bounded spectral bases.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    mem::size_of,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use casa_imaging_model::{
    CompiledProblem, ContinuumTransformGenerationId, LogicalIdentity, ReconstructionBasis,
    SelectedObservationGenerationId,
};
use ndarray::Array2;
use num_complex::Complex64;
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

mod two_domain;
use two_domain::{
    GriddedNormalClassification, GriddedNormalDomainTileCatalogs, GriddedNormalGroupSpan,
    GriddedNormalRoute, GriddedNormalTileAccumulator, PreparedGriddedNormalTwoDomainWindow,
};
pub use two_domain::{GriddedNormalPartial, GriddedNormalWork};

use crate::{
    Encoder, FinalNormalState, ModelGeneration, ScienceTraceDigest, imaging_science_trace_enabled,
    polarization_operator::{MuellerMatrix, PolarizationOperator},
    spectral_operator::{
        AwReplayCoordinates, CompleteDataOwnerCompletion, CompleteDataOwnerResult,
        ConvolutionOperator, PreparedSpectralOperator, ReusableNormalState, SPEED_OF_LIGHT_M_PER_S,
        SUPPORT, SampleTaps, SpectralOperatorError, SpectralOperatorPass,
        SpectralOperatorSpecification, SpectralPrimitiveCatalog, SpectralSlabOperator, TapSpan,
        WProjectionDiagnostics, accept_polarization_input, accept_weighted_input,
        aw_replay_coordinates, aw_stokes_i_mueller, combine_chart_updates, polarization_diagonal,
        polarization_effective_flags, selected_model_projection,
    },
    weighting::{
        CoverageEncoder, WeightingReplayChunk, WeightingReplayCoverageId, WeightingReplayId,
        WeightingReplaySummary,
    },
};

#[cfg(test)]
use crate::spectral_operator::{GriddedNormalLocalContribution, StandardConvolution};

const RECORD_DOMAIN: &[u8] = b"casa-rs-gridded-normal-operator";
const RECORD_VERSION: u32 = 8;
const TAP_KEY_BITS: u32 = 24;
const TAP_KEY_MASK: u64 = (1_u64 << TAP_KEY_BITS) - 1;
const CHANNEL_KEY_BITS: u32 = 24;
const CHANNEL_KEY_MASK: u64 = (1_u64 << CHANNEL_KEY_BITS) - 1;
const GROUP_END_BIT: u64 = 1_u64 << (TAP_KEY_BITS + CHANNEL_KEY_BITS);
const RECORD_KEY_MASK: u64 = (GROUP_END_BIT << 1) - 1;
const GRIDDED_NORMAL_TAPS_PER_RECORD: u64 = ((SUPPORT * 2 + 1) * (SUPPORT * 2 + 1)) as u64;
const GRIDDED_NORMAL_TILE_EDGE: usize = 32;
const GRIDDED_NORMAL_HOT_TILE_DUPLICATES: usize = GRIDDED_NORMAL_LANE_COUNT - 1;

/// Width of an opaque scalar gridded normal-operator record.
///
/// Taylor records use the problem-derived width returned by
/// [`gridded_normal_operator_record_bytes`].
pub const GRIDDED_NORMAL_OPERATOR_RECORD_BYTES: usize = 40;
const AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES: usize = 96;
const AW_MUELLER_SHIFT: u32 = TAP_KEY_BITS + CHANNEL_KEY_BITS;
const AW_GROUP_END_BIT: u64 = 1_u64 << (AW_MUELLER_SHIFT + 4);
const AW_RECORD_KEY_MASK: u64 = (AW_GROUP_END_BIT << 1) - 1;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum GriddedNormalRecordLayout {
    Scalar,
    ChannelLocal {
        channels: usize,
    },
    Taylor(crate::block_normal::BlockNormalPlan),
    // Frequency-dependent AW kernels cannot collapse samples into tap-key moments.
    TaylorWithCoordinates(crate::block_normal::BlockNormalPlan),
    TaylorViaChannelMajor {
        plan: crate::block_normal::BlockNormalPlan,
        channels: usize,
    },
    Joint {
        coefficient_terms: usize,
        normal_moments: usize,
    },
}

impl GriddedNormalRecordLayout {
    fn for_specification(specification: &SpectralOperatorSpecification) -> Self {
        if let Some(plan) = specification.channel_major_taylor_plan() {
            return Self::TaylorViaChannelMajor {
                plan,
                channels: specification.slab().core_depth(),
            };
        }
        if specification.joint_continuum_term_count().is_some() {
            return Self::Joint {
                coefficient_terms: specification.coefficient_terms(),
                normal_moments: specification.normal_moments(),
            };
        }
        match specification.block_normal_plan() {
            Some(plan) if plan.coefficient_term_count() > 1 => {
                if specification.aw_projection().is_some() {
                    Self::TaylorWithCoordinates(plan)
                } else {
                    Self::Taylor(plan)
                }
            }
            Some(_) => Self::Scalar,
            None => Self::ChannelLocal {
                channels: specification.coefficient_terms(),
            },
        }
    }

    pub(super) const fn coefficient_terms(self) -> usize {
        match self {
            Self::Scalar => 1,
            Self::ChannelLocal { channels } => channels,
            Self::Taylor(plan) | Self::TaylorWithCoordinates(plan) => plan.coefficient_term_count(),
            Self::TaylorViaChannelMajor { plan, .. } => plan.coefficient_term_count(),
            Self::Joint {
                coefficient_terms, ..
            } => coefficient_terms,
        }
    }

    pub(super) const fn normal_moments(self) -> usize {
        match self {
            Self::Scalar => 1,
            Self::ChannelLocal { channels } => channels,
            Self::Taylor(plan) | Self::TaylorWithCoordinates(plan) => plan.normal_moment_count(),
            Self::TaylorViaChannelMajor { plan, .. } => plan.normal_moment_count(),
            Self::Joint { normal_moments, .. } => normal_moments,
        }
    }

    pub(super) const fn prediction_width(self) -> usize {
        match self {
            Self::Scalar
            | Self::ChannelLocal { .. }
            | Self::TaylorWithCoordinates(_)
            | Self::TaylorViaChannelMajor { .. }
            | Self::Joint { .. } => 1,
            Self::Taylor(plan) => plan.coefficient_term_count(),
        }
    }

    pub(super) const fn accumulation_width(self, output_channels: usize) -> usize {
        match self {
            Self::Joint {
                coefficient_terms, ..
            } => coefficient_terms + output_channels,
            Self::Scalar => 1,
            Self::ChannelLocal { channels } => channels,
            Self::TaylorViaChannelMajor { channels, .. } => channels,
            Self::Taylor(plan) | Self::TaylorWithCoordinates(plan) => plan.coefficient_term_count(),
        }
    }

    pub(super) fn record_bytes(self) -> Result<usize, SpectralOperatorError> {
        match self {
            Self::Scalar | Self::ChannelLocal { .. } | Self::TaylorViaChannelMajor { .. } => {
                Ok(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            }
            Self::Joint { .. } => Ok(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES),
            Self::TaylorWithCoordinates(_) => Ok(AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES),
            Self::Taylor(plan) => plan
                .normal_moment_count()
                .checked_add(1)
                .and_then(|values| values.checked_mul(size_of::<u64>()))
                .ok_or(SpectralOperatorError::ResidencyOverflow),
        }
    }
}

fn record_bytes(
    layout: GriddedNormalRecordLayout,
    aw_projection: bool,
) -> Result<usize, SpectralOperatorError> {
    if aw_projection {
        if matches!(layout, GriddedNormalRecordLayout::Joint { .. }) {
            return Err(SpectralOperatorError::UnsupportedGriddedReplay);
        }
        Ok(AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
    } else {
        layout.record_bytes()
    }
}

/// Return the opaque record width selected by reconstruction for this problem.
#[doc(hidden)]
pub fn gridded_normal_operator_record_bytes(
    problem: &CompiledProblem,
) -> Result<usize, SpectralOperatorError> {
    let specification = SpectralOperatorSpecification::new(problem)?;
    record_bytes(
        GriddedNormalRecordLayout::for_specification(&specification),
        specification.aw_projection().is_some(),
    )
}

/// Stable logical fanout within each gridded-normal execution phase.
pub const GRIDDED_NORMAL_LANE_COUNT: usize = 4;

/// Prediction and accumulation each contribute four ordered logical works.
pub const GRIDDED_NORMAL_PARTITION_COUNT: usize = GRIDDED_NORMAL_LANE_COUNT * 2;

#[cfg(test)]
const GRIDDED_NORMAL_SECTOR_COUNT: usize = GRIDDED_NORMAL_LANE_COUNT;

/// Project exact reusable route capacity for retained frame ordinals.
///
/// `record_count` is the sum of the planned per-ordinal record capacities.
/// The storage is shared by all workers and includes the exact Taylor-width
/// prediction values and reusable per-lane algebra scratch.
#[doc(hidden)]
pub fn gridded_normal_route_capacity_bytes(
    record_count: usize,
    frame_count: usize,
    prediction_width: usize,
) -> Option<u64> {
    if prediction_width == 0 {
        return None;
    }
    let prediction_capacity = record_count
        .div_ceil(GRIDDED_NORMAL_LANE_COUNT)
        .checked_add(1)?;
    let record_bytes = record_count.checked_mul(
        size_of::<GriddedNormalGroupSpan>()
            + size_of::<GriddedNormalClassification>()
            + size_of::<GriddedNormalRoute>(),
    )?;
    let prediction_bytes = prediction_capacity
        .checked_mul(GRIDDED_NORMAL_LANE_COUNT)?
        .checked_mul(prediction_width)?
        .checked_mul(size_of::<Complex64>())?;
    let frame_bytes =
        frame_count.checked_mul(size_of::<usize>() + size_of::<u64>() + size_of::<u32>())?;
    let scratch_bytes = if prediction_width > 1 {
        prediction_width
            .checked_mul(size_of::<Complex64>())?
            .checked_add(
                prediction_width
                    .checked_mul(2)?
                    .checked_sub(1)?
                    .checked_mul(size_of::<f64>())?,
            )?
            .checked_mul(GRIDDED_NORMAL_LANE_COUNT)?
    } else {
        0
    };
    u64::try_from(
        record_bytes
            .checked_add(prediction_bytes)?
            .checked_add(frame_bytes)?
            .checked_add(scratch_bytes)?,
    )
    .ok()
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

/// Exact reconstruction-owned residency of tiled accumulation and merge state.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalExecutionResidency {
    tile_accumulator_complex_values: usize,
    merge_complex_values: usize,
    peak_complex_values: usize,
    metadata_bytes: usize,
}

impl GriddedNormalExecutionResidency {
    /// Return persistent tile/shard grid plus compensation values.
    #[must_use]
    pub const fn tile_accumulator_complex_values(self) -> usize {
        self.tile_accumulator_complex_values
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

    /// Return exact fixed tile catalog, owner, and accumulator metadata bytes.
    #[must_use]
    pub const fn metadata_bytes(self) -> usize {
        self.metadata_bytes
    }
}

/// Project exact worker-independent tile/shard and global-merge residency.
#[doc(hidden)]
pub fn gridded_normal_execution_residency(
    grid_shape: [usize; 2],
    coefficient_terms: usize,
    convolution_support: usize,
) -> Result<GriddedNormalExecutionResidency, SpectralOperatorError> {
    gridded_normal_domain_execution_residency([grid_shape], coefficient_terms, convolution_support)
}

/// Return the support radius of the canonical standard convolution kernel.
#[doc(hidden)]
#[must_use]
pub const fn standard_convolution_support() -> usize {
    SUPPORT
}

/// Project exact tiled accumulation and merge residency for all image domains.
#[doc(hidden)]
pub fn gridded_normal_domain_execution_residency(
    grid_shapes: impl IntoIterator<Item = [usize; 2]>,
    coefficient_terms: usize,
    convolution_support: usize,
) -> Result<GriddedNormalExecutionResidency, SpectralOperatorError> {
    two_domain::domain_execution_residency(grid_shapes, coefficient_terms, convolution_support)
}

/// Project exact AW tiled accumulation and merge residency for all image domains.
#[doc(hidden)]
pub fn gridded_normal_aw_domain_execution_residency(
    grid_shapes: impl IntoIterator<Item = [usize; 2]>,
    coefficient_terms: usize,
    convolution_support: usize,
) -> Result<GriddedNormalExecutionResidency, SpectralOperatorError> {
    two_domain::domain_execution_residency_for_projection(
        grid_shapes,
        coefficient_terms,
        convolution_support,
        true,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReducedRecordKey {
    chart_ordinal: u32,
    output_channel: u32,
    taps: u64,
    forward_real: u64,
    forward_imaginary: u64,
    imaging_weight: u64,
    aw: Option<AwRecordCoordinates>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AwRecordCoordinates {
    frequency_hz: u64,
    uvw_m: [u64; 3],
    prediction_w_m: u64,
    parallactic_angle_deg: u64,
    pointing_phase_gradient_rad_per_grid_cell: [u64; 2],
    mueller_element: u32,
}

impl From<AwReplayCoordinates> for AwRecordCoordinates {
    fn from(value: AwReplayCoordinates) -> Self {
        Self {
            frequency_hz: canonical_zero_bits(value.frequency_hz),
            uvw_m: value.uvw_m.map(canonical_zero_bits),
            prediction_w_m: canonical_zero_bits(value.prediction_w_m),
            parallactic_angle_deg: canonical_zero_bits(value.parallactic_angle_deg),
            pointing_phase_gradient_rad_per_grid_cell: value
                .pointing_phase_gradient_rad_per_grid_cell
                .map(canonical_zero_bits),
            mueller_element: value.mueller_element,
        }
    }
}

impl From<AwRecordCoordinates> for AwReplayCoordinates {
    fn from(value: AwRecordCoordinates) -> Self {
        Self {
            frequency_hz: f64::from_bits(value.frequency_hz),
            uvw_m: value.uvw_m.map(f64::from_bits),
            prediction_w_m: f64::from_bits(value.prediction_w_m),
            parallactic_angle_deg: f64::from_bits(value.parallactic_angle_deg),
            pointing_phase_gradient_rad_per_grid_cell: value
                .pointing_phase_gradient_rad_per_grid_cell
                .map(f64::from_bits),
            mueller_element: value.mueller_element,
        }
    }
}

struct ReducedRecordGroup {
    records: Vec<ReducedRecordKey>,
    multiplicity: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TaylorRecordKey {
    taps: u64,
    frequency_hz: u64,
    imaging_weight: u64,
}

struct ReducedTaylorRecord {
    taps: u64,
    moments: Box<[f64]>,
}

struct TaylorMomentAccumulator {
    moments: Vec<f64>,
    compensations: Vec<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DecodedRecord {
    chart_ordinal: usize,
    output_channel: usize,
    taps: SampleTaps,
    forward_scale: Complex64,
    imaging_weight: f64,
    group_end: bool,
    aw: Option<AwReplayCoordinates>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DecodedTaylorRecord<'a> {
    pub(super) taps: SampleTaps,
    moment_bytes: &'a [u8],
}

impl DecodedTaylorRecord<'_> {
    pub(super) fn fill_moments(self, moments: &mut [f64]) -> Result<(), SpectralOperatorError> {
        if self.moment_bytes.len() != std::mem::size_of_val(moments) {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        for (value, bytes) in moments
            .iter_mut()
            .zip(self.moment_bytes.chunks_exact(size_of::<f64>()))
        {
            *value = f64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
            );
            if !value.is_finite() {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }
        }
        Ok(())
    }
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
    record_bytes: usize,
    encoded: Box<[u8]>,
    measurements: GriddedNormalOperatorBlockMeasurements,
}

/// Whether compilation should derive diagnostic source cardinality.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceCardinalityObservation {
    /// Compile without cardinality accounting in the record hot path.
    Disabled,
    /// Derive exact cardinality while traversing grouped records for encoding.
    Enabled,
}

/// Exact source cardinality derived by an explicitly observed compilation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalSourceCardinality {
    pub groups: u64,
    pub records: u64,
}

/// Mutually exclusive timings from one explicitly observed compiler block.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalOperatorStageTimings {
    pub record_key_construction: Duration,
    pub grouping_reduction: Duration,
    pub encoding_checksum: Duration,
    pub completion: Duration,
}

/// Exact code-owned cardinality, allocation, growth, and insertion measurements.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalOperatorBlockMeasurements {
    pub source_cardinality: Option<GriddedNormalSourceCardinality>,
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
        u64::try_from(self.encoded.len() / self.record_bytes).expect("record count fits u64")
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
    record_layout: GriddedNormalRecordLayout,
    binding: LogicalIdentity,
    finite_values: casa_imaging_model::FiniteValuePolicy,
    gridders: Vec<ConvolutionOperator>,
    w_projection_diagnostics: Box<[WProjectionDiagnostics]>,
    next_block_sequence: u64,
    sample_count: u64,
    record_count: u64,
    coverage: CoverageEncoder,
    descriptors: Vec<BlockDescriptor>,
    source_cardinality_observation: SourceCardinalityObservation,
    aw_projection: bool,
    science_probe: Option<ImagingScienceProbe>,
}

#[derive(Default)]
struct ImagingScienceProbe {
    weighting: BTreeMap<(u32, usize), WeightingScienceAggregate>,
    sample: Option<WeightingScienceSample>,
}

#[derive(Clone, Copy, Default)]
struct WeightingScienceAggregate {
    accepted_count: u64,
    base_weight_sum: f64,
    raw_weight_sum: f64,
    post_briggs_sum: f64,
    taylor_factor_sum: f64,
    final_weight_sum: f64,
}

#[derive(Clone, Copy)]
struct WeightingScienceSample {
    address: casa_imaging_model::SelectedSampleAddress,
    output_channel: u32,
    chart: usize,
    mueller: u32,
    base_weight: f64,
    raw_weight: f64,
    post_briggs_weight: f64,
    frequency_hz: f64,
    spectral_factor: f64,
}

fn sample_address_key(
    address: casa_imaging_model::SelectedSampleAddress,
) -> ([u8; 32], u64, i32, u32, u32, u32, u32) {
    (
        address.measurement_set.identity().as_bytes(),
        address.physical_row,
        address.data_description_id,
        address.spectral_window_id,
        address.channel_index,
        address.polarization_id,
        address.correlation_index,
    )
}

impl ImagingScienceProbe {
    fn emit(self, layout: GriddedNormalRecordLayout) {
        for ((spw, term), aggregate) in self.weighting {
            eprintln!(
                "imaging_science_probe_v1 boundary=weighting_summary spw={spw} term={term} accepted_count={} base_weight_sum={:.17e} raw_weight_sum={:.17e} post_briggs_sum={:.17e} taylor_factor_sum={:.17e} final_pre_cf_sum={:.17e}",
                aggregate.accepted_count,
                aggregate.base_weight_sum,
                aggregate.raw_weight_sum,
                aggregate.post_briggs_sum,
                aggregate.taylor_factor_sum,
                aggregate.final_weight_sum,
            );
        }
        let Some(sample) = self.sample else {
            return;
        };
        let moments = match layout {
            GriddedNormalRecordLayout::TaylorWithCoordinates(plan)
            | GriddedNormalRecordLayout::TaylorViaChannelMajor { plan, .. } => {
                plan.normal_moment_count()
            }
            _ => 1,
        };
        for term in 0..moments {
            let taylor_factor = match layout {
                GriddedNormalRecordLayout::TaylorWithCoordinates(plan)
                | GriddedNormalRecordLayout::TaylorViaChannelMajor { plan, .. } => plan
                    .normalized_frequency(sample.frequency_hz)
                    .map(|x| x.powi(term as i32))
                    .unwrap_or(f64::NAN),
                _ => 1.0,
            };
            let final_weight = sample.post_briggs_weight
                * sample.spectral_factor
                * sample.spectral_factor
                * taylor_factor;
            eprintln!(
                "imaging_science_probe_v1 boundary=sample_weight ms={:?} row={} ddid={} spw={} channel={} polarization={} correlation={} output_channel={} chart={} mueller={} term={} base_weight={:.17e} raw_weight={:.17e} post_briggs_weight={:.17e} frequency_hz={:.17e} spectral_factor={:.17e} taylor_factor={:.17e} final_pre_cf_weight={:.17e}",
                sample.address.measurement_set,
                sample.address.physical_row,
                sample.address.data_description_id,
                sample.address.spectral_window_id,
                sample.address.channel_index,
                sample.address.polarization_id,
                sample.address.correlation_index,
                sample.output_channel,
                sample.chart,
                sample.mueller,
                term,
                sample.base_weight,
                sample.raw_weight,
                sample.post_briggs_weight,
                sample.frequency_hz,
                sample.spectral_factor,
                taylor_factor,
                final_weight,
            );
        }
    }
}

impl GriddedNormalOperatorCompiler {
    /// Compile the first vertical record owner for a supported reconstruction basis.
    pub fn new(
        problem: &CompiledProblem,
        source_cardinality_observation: SourceCardinalityObservation,
    ) -> Result<Self, SpectralOperatorError> {
        require_supported_basis(&problem.reconstruction().basis())?;
        let specification = SpectralOperatorSpecification::new(problem)?;
        validate_record_geometry(&specification)?;
        let record_layout = GriddedNormalRecordLayout::for_specification(&specification);
        let aw_projection = specification.aw_projection().is_some();
        let _ = record_bytes(record_layout, aw_projection)?;
        let gridders = specification
            .charts()
            .iter()
            .map(|chart| ConvolutionOperator::new(&chart.geometry(), specification.w_projection()))
            .collect::<Result<Vec<_>, _>>()?;
        let w_projection_diagnostics = gridders
            .iter()
            .filter_map(ConvolutionOperator::w_projection_diagnostics)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let binding = static_binding(&specification);
        Ok(Self {
            finite_values: specification.finite_values(),
            gridders,
            w_projection_diagnostics,
            specification,
            record_layout,
            binding,
            next_block_sequence: 0,
            sample_count: 0,
            record_count: 0,
            coverage: CoverageEncoder::new(),
            descriptors: Vec::new(),
            source_cardinality_observation,
            aw_projection,
            science_probe: (source_cardinality_observation
                == SourceCardinalityObservation::Enabled)
                .then(ImagingScienceProbe::default),
        })
    }

    /// Reduce one complete canonical weighting block and return its opaque bytes.
    pub fn compile_block(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<GriddedNormalOperatorBlock, SpectralOperatorError> {
        match (self.record_layout, self.source_cardinality_observation) {
            (
                GriddedNormalRecordLayout::Scalar
                | GriddedNormalRecordLayout::ChannelLocal { .. }
                | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
                | GriddedNormalRecordLayout::Joint { .. },
                SourceCardinalityObservation::Disabled,
            ) => self.compile_block_inner::<false>(block),
            (
                GriddedNormalRecordLayout::Scalar
                | GriddedNormalRecordLayout::ChannelLocal { .. }
                | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
                | GriddedNormalRecordLayout::Joint { .. },
                SourceCardinalityObservation::Enabled,
            ) => self.compile_block_inner::<true>(block),
            (GriddedNormalRecordLayout::Taylor(plan), SourceCardinalityObservation::Disabled) => {
                self.compile_taylor_block_inner::<false>(block, plan)
            }
            (GriddedNormalRecordLayout::Taylor(plan), SourceCardinalityObservation::Enabled) => {
                self.compile_taylor_block_inner::<true>(block, plan)
            }
        }
    }

    /// Compile one block while timing mutually exclusive owner phases.
    #[doc(hidden)]
    pub fn compile_block_observed(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<
        (
            GriddedNormalOperatorBlock,
            GriddedNormalOperatorStageTimings,
        ),
        SpectralOperatorError,
    > {
        debug_assert_eq!(
            self.source_cardinality_observation,
            SourceCardinalityObservation::Enabled
        );
        let mut timings = GriddedNormalOperatorStageTimings::default();

        let (encoded, digest, measurements) = match self.record_layout {
            GriddedNormalRecordLayout::Scalar
            | GriddedNormalRecordLayout::ChannelLocal { .. }
            | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
            | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
            | GriddedNormalRecordLayout::Joint { .. } => {
                let started = Instant::now();
                self.begin_block(block)?;
                let (source_groups, mut measurements) =
                    self.construct_record_keys_observed(block)?;
                timings.record_key_construction = started.elapsed();

                let started = Instant::now();
                let (groups, source_cardinality) =
                    group_and_reduce::<true>(source_groups, &mut measurements)?;
                measurements.source_cardinality = source_cardinality;
                timings.grouping_reduction = started.elapsed();

                let started = Instant::now();
                let (encoded, digest) =
                    encode_and_checksum_mode(groups, self.aw_projection, &mut measurements)?;
                timings.encoding_checksum = started.elapsed();
                (encoded, digest, measurements)
            }
            GriddedNormalRecordLayout::Taylor(plan) => {
                let started = Instant::now();
                self.begin_block(block)?;
                let (keys, mut measurements) = self.construct_taylor_record_keys(block)?;
                timings.record_key_construction = started.elapsed();

                let started = Instant::now();
                let (records, source_cardinality) =
                    group_and_reduce_taylor::<true>(keys, plan, &mut measurements)?;
                measurements.source_cardinality = source_cardinality;
                timings.grouping_reduction = started.elapsed();

                let started = Instant::now();
                let (encoded, digest) =
                    encode_taylor_and_checksum(records, plan, &mut measurements)?;
                timings.encoding_checksum = started.elapsed();
                (encoded, digest, measurements)
            }
        };

        let started = Instant::now();
        let result = self.commit_block(block, encoded, digest, measurements)?;
        timings.completion = started.elapsed();
        Ok((result, timings))
    }

    fn compile_block_inner<const OBSERVE_SOURCE_CARDINALITY: bool>(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<GriddedNormalOperatorBlock, SpectralOperatorError> {
        self.begin_block(block)?;
        let (source_groups, mut measurements) = self.construct_record_keys(block)?;
        let (groups, source_cardinality) =
            group_and_reduce::<OBSERVE_SOURCE_CARDINALITY>(source_groups, &mut measurements)?;
        measurements.source_cardinality = source_cardinality;
        let (encoded, digest) =
            encode_and_checksum_mode(groups, self.aw_projection, &mut measurements)?;
        self.commit_block(block, encoded, digest, measurements)
    }

    fn compile_taylor_block_inner<const OBSERVE_SOURCE_CARDINALITY: bool>(
        &mut self,
        block: &WeightingReplayChunk,
        plan: crate::block_normal::BlockNormalPlan,
    ) -> Result<GriddedNormalOperatorBlock, SpectralOperatorError> {
        self.begin_block(block)?;
        let (keys, mut measurements) = self.construct_taylor_record_keys(block)?;
        let (records, source_cardinality) =
            group_and_reduce_taylor::<OBSERVE_SOURCE_CARDINALITY>(keys, plan, &mut measurements)?;
        measurements.source_cardinality = source_cardinality;
        let (encoded, digest) = encode_taylor_and_checksum(records, plan, &mut measurements)?;
        self.commit_block(block, encoded, digest, measurements)
    }

    fn begin_block(&mut self, block: &WeightingReplayChunk) -> Result<(), SpectralOperatorError> {
        if block.sequence() != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.coverage.adopt(block.coverage_checkpoint());
        Ok(())
    }

    fn construct_record_keys(
        &self,
        block: &WeightingReplayChunk,
    ) -> Result<
        (
            Vec<Vec<ReducedRecordKey>>,
            GriddedNormalOperatorBlockMeasurements,
        ),
        SpectralOperatorError,
    > {
        if self.aw_projection {
            return self.construct_aw_record_keys(block);
        }
        let mut source_groups = Vec::new();
        let mut measurements = GriddedNormalOperatorBlockMeasurements::default();
        for correlations in block.correlation_groups() {
            let first = correlations
                .first()
                .ok_or(SpectralOperatorError::InvalidSample)?;
            let selected = first.selected();
            let operator = PolarizationOperator::compile(
                self.specification.polarization_coordinates(),
                &correlations
                    .iter()
                    .map(|weighted| weighted.selected().address().correlation_type)
                    .collect::<SmallVec<[_; 4]>>(),
                selected.parallactic_angles_rad(),
                MuellerMatrix::identity(),
            )
            .map_err(|_| SpectralOperatorError::InvalidSample)?;
            let flags = correlations
                .iter()
                .map(|weighted| {
                    accept_polarization_input(weighted.selected(), self.finite_values).map(|ok| !ok)
                })
                .collect::<Result<SmallVec<[_; 4]>, _>>()?;
            let flags = polarization_effective_flags(&operator, flags);
            for spectral_ordinal in 0..first.spectral_values().count() {
                let first_spectral = first
                    .spectral_values()
                    .nth(spectral_ordinal)
                    .ok_or(SpectralOperatorError::InvalidSample)?;
                let weights = correlations
                    .iter()
                    .map(|weighted| {
                        let spectral = weighted
                            .spectral_values()
                            .nth(spectral_ordinal)
                            .ok_or(SpectralOperatorError::InvalidSample)?;
                        if spectral.contribution() != first_spectral.contribution() {
                            return Err(SpectralOperatorError::InvalidSample);
                        }
                        Ok(spectral.imaging_weight())
                    })
                    .collect::<Result<SmallVec<[_; 4]>, _>>()?;
                let diagonal = polarization_diagonal(&operator, &weights, &flags);
                for (polarization, imaging_weight) in diagonal.into_iter().enumerate() {
                    if imaging_weight == 0.0 {
                        continue;
                    }
                    let contribution = first_spectral.contribution();
                    let output_channel = usize::try_from(contribution.output_channel())
                        .map_err(|_| SpectralOperatorError::InvalidSample)?;
                    if output_channel >= self.specification.slab().total_channels() {
                        return Err(SpectralOperatorError::InvalidSample);
                    }
                    let output_plane = output_channel
                        .checked_mul(self.specification.polarization_count())
                        .and_then(|plane| plane.checked_add(polarization))
                        .and_then(|plane| u32::try_from(plane).ok())
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                    let mut group = Vec::new();
                    for domain_ordinal in 0..self.specification.chart_count() {
                        let chart = &self.specification.charts()[domain_ordinal];
                        let (uvw_m, phase_shift_m) = selected_model_projection(
                            selected,
                            self.specification.chart_count(),
                            chart.domain_ordinal(),
                            chart.facet_ordinal(),
                        )?;
                        let frequency_hz = contribution.evaluation_frequency_hz();
                        let factor = contribution.factor();
                        if !frequency_hz.is_finite()
                            || frequency_hz <= 0.0
                            || !factor.is_finite()
                            || factor == 0.0
                        {
                            return Err(SpectralOperatorError::InvalidSample);
                        }
                        let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                        let Some(taps) = self.gridders[domain_ordinal].taps([
                            uvw_m[0] * scale,
                            uvw_m[1] * scale,
                            uvw_m[2] * scale,
                        ]) else {
                            continue;
                        };
                        let phase_angle = std::f64::consts::TAU * phase_shift_m * frequency_hz
                            / SPEED_OF_LIGHT_M_PER_S;
                        let forward_scale = Complex64::from_polar(factor, -phase_angle);
                        let old_capacity = group.capacity();
                        group.push(ReducedRecordKey {
                            chart_ordinal: u32::try_from(domain_ordinal)
                                .map_err(|_| SpectralOperatorError::DomainProjectionMismatch)?,
                            output_channel: output_plane,
                            taps: encode_taps(taps)?,
                            forward_real: canonical_zero_bits(forward_scale.re),
                            forward_imaginary: canonical_zero_bits(forward_scale.im),
                            imaging_weight: canonical_zero_bits(imaging_weight),
                            aw: None,
                        });
                        record_vector_growth(
                            old_capacity,
                            group.capacity(),
                            size_of::<ReducedRecordKey>(),
                            &mut measurements.source_group_vector_allocations,
                            &mut measurements.source_group_capacity_growth_bytes,
                        )?;
                    }
                    if !group.is_empty() {
                        source_groups.push(group);
                    }
                }
            }
        }
        Ok((source_groups, measurements))
    }

    fn construct_record_keys_observed(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<
        (
            Vec<Vec<ReducedRecordKey>>,
            GriddedNormalOperatorBlockMeasurements,
        ),
        SpectralOperatorError,
    > {
        if !self.aw_projection {
            return self.construct_record_keys(block);
        }
        self.observe_aw_weighting(block)?;
        self.construct_aw_record_keys(block)
    }

    fn observe_aw_weighting(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<(), SpectralOperatorError> {
        let polarization_coordinates = self.specification.polarization_coordinates();
        let finite_values = self.finite_values;
        let Some(probe) = self.science_probe.as_mut() else {
            return Ok(());
        };
        let taylor = match self.record_layout {
            GriddedNormalRecordLayout::TaylorWithCoordinates(plan)
            | GriddedNormalRecordLayout::TaylorViaChannelMajor { plan, .. } => Some(plan),
            _ => None,
        };
        for correlations in block.correlation_groups() {
            let first = correlations
                .first()
                .ok_or(SpectralOperatorError::InvalidSample)?;
            let operator = PolarizationOperator::compile(
                polarization_coordinates,
                &correlations
                    .iter()
                    .map(|weighted| weighted.selected().address().correlation_type)
                    .collect::<SmallVec<[_; 4]>>(),
                first.selected().parallactic_angles_rad(),
                MuellerMatrix::identity(),
            )
            .map_err(|_| SpectralOperatorError::InvalidSample)?;
            let flags = correlations
                .iter()
                .map(|weighted| {
                    accept_polarization_input(weighted.selected(), finite_values).map(|ok| !ok)
                })
                .collect::<Result<SmallVec<[_; 4]>, _>>()?;
            let flags = polarization_effective_flags(&operator, flags);
            for (ordinal, weighted) in correlations.iter().enumerate() {
                if flags[ordinal] {
                    continue;
                }
                let selected = weighted.selected();
                for spectral in weighted.spectral_values() {
                    let post_briggs = spectral.imaging_weight();
                    if post_briggs == 0.0 {
                        continue;
                    }
                    let contribution = spectral.contribution();
                    let frequency_hz = contribution.evaluation_frequency_hz();
                    let spectral_factor = contribution.factor();
                    let moments =
                        taylor.map_or(1, crate::block_normal::BlockNormalPlan::normal_moment_count);
                    for moment in 0..moments {
                        let taylor_factor = taylor
                            .map(|plan| {
                                plan.normalized_frequency(frequency_hz)
                                    .map(|x| x.powi(moment as i32))
                            })
                            .transpose()
                            .map_err(|_| SpectralOperatorError::InvalidSample)?
                            .unwrap_or(1.0);
                        let aggregate = probe
                            .weighting
                            .entry((selected.address().spectral_window_id, moment))
                            .or_default();
                        aggregate.accepted_count = aggregate
                            .accepted_count
                            .checked_add(1)
                            .ok_or(SpectralOperatorError::CoverageOverflow)?;
                        aggregate.base_weight_sum += f64::from(selected.input_weight);
                        aggregate.raw_weight_sum += f64::from(selected.raw_input_weight());
                        aggregate.post_briggs_sum += post_briggs;
                        aggregate.taylor_factor_sum += taylor_factor;
                        aggregate.final_weight_sum +=
                            post_briggs * spectral_factor * spectral_factor * taylor_factor;
                    }
                    let candidate = WeightingScienceSample {
                        address: selected.address(),
                        output_channel: contribution.output_channel(),
                        chart: 0,
                        mueller: aw_stokes_i_mueller(selected.address().correlation_type)?
                            .unwrap_or_default(),
                        base_weight: f64::from(selected.input_weight),
                        raw_weight: f64::from(selected.raw_input_weight()),
                        post_briggs_weight: post_briggs,
                        frequency_hz,
                        spectral_factor,
                    };
                    if probe.sample.is_none_or(|current| {
                        sample_address_key(candidate.address) < sample_address_key(current.address)
                    }) {
                        probe.sample = Some(candidate);
                    }
                }
            }
        }
        Ok(())
    }

    fn construct_aw_record_keys(
        &self,
        block: &WeightingReplayChunk,
    ) -> Result<
        (
            Vec<Vec<ReducedRecordKey>>,
            GriddedNormalOperatorBlockMeasurements,
        ),
        SpectralOperatorError,
    > {
        let contract = self
            .specification
            .aw_projection()
            .ok_or(SpectralOperatorError::ProblemMismatch)?;
        let mut source_groups = Vec::new();
        let mut measurements = GriddedNormalOperatorBlockMeasurements::default();
        for correlations in block.correlation_groups() {
            let first = correlations
                .first()
                .ok_or(SpectralOperatorError::InvalidSample)?;
            let selected = first.selected();
            let operator = PolarizationOperator::compile(
                self.specification.polarization_coordinates(),
                &correlations
                    .iter()
                    .map(|weighted| weighted.selected().address().correlation_type)
                    .collect::<SmallVec<[_; 4]>>(),
                selected.parallactic_angles_rad(),
                MuellerMatrix::identity(),
            )
            .map_err(|_| SpectralOperatorError::InvalidSample)?;
            let flags = correlations
                .iter()
                .map(|weighted| {
                    accept_polarization_input(weighted.selected(), self.finite_values).map(|ok| !ok)
                })
                .collect::<Result<SmallVec<[_; 4]>, _>>()?;
            let flags = polarization_effective_flags(&operator, flags);
            for (correlation_ordinal, weighted) in correlations.iter().enumerate() {
                let Some(mueller_element) =
                    aw_stokes_i_mueller(weighted.selected().address().correlation_type)?
                else {
                    continue;
                };
                if flags[correlation_ordinal] {
                    continue;
                }
                for spectral in weighted.spectral_values() {
                    let contribution = spectral.contribution();
                    let output_channel = usize::try_from(contribution.output_channel())
                        .map_err(|_| SpectralOperatorError::InvalidSample)?;
                    if output_channel >= self.specification.slab().total_channels() {
                        return Err(SpectralOperatorError::InvalidSample);
                    }
                    let imaging_weight = spectral.imaging_weight();
                    let frequency_hz = contribution.evaluation_frequency_hz();
                    let factor = contribution.factor();
                    if !imaging_weight.is_finite()
                        || imaging_weight < 0.0
                        || !frequency_hz.is_finite()
                        || frequency_hz <= 0.0
                        || !factor.is_finite()
                        || factor == 0.0
                    {
                        return Err(SpectralOperatorError::InvalidSample);
                    }
                    if imaging_weight == 0.0 {
                        continue;
                    }
                    let output_plane = u32::try_from(output_channel)
                        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
                    let mut group = Vec::new();
                    for chart in self.specification.charts() {
                        let (uvw_m, phase_shift_m) = selected_model_projection(
                            weighted.selected(),
                            self.specification.chart_count(),
                            chart.domain_ordinal(),
                            chart.facet_ordinal(),
                        )?;
                        let aw = aw_replay_coordinates(
                            weighted.selected(),
                            chart.geometry(),
                            contract.use_pointing(),
                            frequency_hz,
                            uvw_m,
                            mueller_element,
                        )?;
                        let phase_angle = std::f64::consts::TAU * phase_shift_m * frequency_hz
                            / SPEED_OF_LIGHT_M_PER_S;
                        let forward_scale = Complex64::from_polar(factor, -phase_angle);
                        let old_capacity = group.capacity();
                        group.push(ReducedRecordKey {
                            chart_ordinal: u32::try_from(chart.ordinal())
                                .map_err(|_| SpectralOperatorError::DomainProjectionMismatch)?,
                            output_channel: output_plane,
                            taps: 0,
                            forward_real: canonical_zero_bits(forward_scale.re),
                            forward_imaginary: canonical_zero_bits(forward_scale.im),
                            imaging_weight: canonical_zero_bits(imaging_weight),
                            aw: Some(AwRecordCoordinates::from(aw)),
                        });
                        record_vector_growth(
                            old_capacity,
                            group.capacity(),
                            size_of::<ReducedRecordKey>(),
                            &mut measurements.source_group_vector_allocations,
                            &mut measurements.source_group_capacity_growth_bytes,
                        )?;
                    }
                    if !group.is_empty() {
                        source_groups.push(group);
                    }
                }
            }
        }
        Ok((source_groups, measurements))
    }

    fn construct_taylor_record_keys(
        &self,
        block: &WeightingReplayChunk,
    ) -> Result<(Vec<TaylorRecordKey>, GriddedNormalOperatorBlockMeasurements), SpectralOperatorError>
    {
        let mut keys = Vec::new();
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
            let mut spectral_values = weighted.spectral_values();
            let spectral = spectral_values
                .next()
                .ok_or(SpectralOperatorError::InvalidSample)?;
            if spectral_values.next().is_some() {
                return Err(SpectralOperatorError::InvalidSample);
            }
            let contribution = spectral.contribution();
            let frequency_hz = contribution.evaluation_frequency_hz();
            let factor = contribution.factor();
            let imaging_weight = spectral.imaging_weight();
            if contribution.output_channel() != 0
                || !frequency_hz.is_finite()
                || frequency_hz <= 0.0
                || !factor.is_finite()
                || factor == 0.0
                || !imaging_weight.is_finite()
                || imaging_weight < 0.0
            {
                return Err(SpectralOperatorError::InvalidSample);
            }
            if imaging_weight == 0.0 {
                continue;
            }
            let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let Some(taps) =
                self.gridders[0].taps([uvw_m[0] * scale, uvw_m[1] * scale, uvw_m[2] * scale])
            else {
                continue;
            };
            let normal_weight = imaging_weight * factor * factor;
            if !normal_weight.is_finite() || normal_weight < 0.0 {
                return Err(SpectralOperatorError::InvalidSample);
            }
            let old_capacity = keys.capacity();
            keys.push(TaylorRecordKey {
                taps: encode_taps(taps)?,
                frequency_hz: canonical_zero_bits(frequency_hz),
                imaging_weight: canonical_zero_bits(normal_weight),
            });
            record_vector_growth(
                old_capacity,
                keys.capacity(),
                size_of::<TaylorRecordKey>(),
                &mut measurements.source_group_vector_allocations,
                &mut measurements.source_group_capacity_growth_bytes,
            )?;
        }
        Ok((keys, measurements))
    }

    fn commit_block(
        &mut self,
        block: &WeightingReplayChunk,
        encoded: Box<[u8]>,
        digest: [u8; 32],
        mut measurements: GriddedNormalOperatorBlockMeasurements,
    ) -> Result<GriddedNormalOperatorBlock, SpectralOperatorError> {
        let source_samples = u64::try_from(block.samples().len())
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        let record_bytes = record_bytes(self.record_layout, self.aw_projection)?;
        if encoded.len() % record_bytes != 0 {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let record_count = u64::try_from(encoded.len() / record_bytes)
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
            digest,
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
            record_bytes,
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
        mut self,
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
        if let Some(probe) = self.science_probe.take() {
            probe.emit(self.record_layout);
        }
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
                record_layout: self.record_layout,
                aw_projection: self.aw_projection,
                descriptors: self.descriptors.into_boxed_slice(),
                w_projection_diagnostics: self.w_projection_diagnostics,
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
    record_layout: GriddedNormalRecordLayout,
    aw_projection: bool,
    descriptors: Box<[BlockDescriptor]>,
    w_projection_diagnostics: Box<[WProjectionDiagnostics]>,
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

    /// Return exact W-kernel diagnostics bound into this program.
    #[must_use]
    pub fn w_projection_diagnostics(&self) -> &[WProjectionDiagnostics] {
        &self.manifest.w_projection_diagnostics
    }

    /// Return the private record width bound into this exact program.
    #[must_use]
    pub fn record_bytes(&self) -> usize {
        record_bytes(self.manifest.record_layout, self.manifest.aw_projection)
            .expect("sealed record layout has representable width")
    }

    /// Return the number of coefficient values predicted for each reduced record.
    #[must_use]
    pub fn prediction_width(&self) -> usize {
        self.manifest.record_layout.prediction_width()
    }

    /// Return the coefficient and common-residual grids accumulated per tile.
    #[must_use]
    pub fn accumulation_width(&self) -> usize {
        self.manifest
            .record_layout
            .accumulation_width(self.manifest.specification.slab().total_channels())
            * self.manifest.specification.polarization_count()
    }

    fn output_plane_count(&self) -> Result<usize, SpectralOperatorError> {
        self.manifest
            .specification
            .slab()
            .total_channels()
            .checked_mul(self.manifest.specification.polarization_count())
            .ok_or(SpectralOperatorError::ResidencyOverflow)
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
            .checked_mul(self.record_bytes())
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
        let (prepared_specification, workload, mut ffts, aw_projection) = prepared.into_parts();
        if prepared_specification.aw_projection().is_some() != aw_projection.is_some() {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
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
                != match self.manifest.record_layout {
                    GriddedNormalRecordLayout::Taylor(_)
                    | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                    | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. } => {
                        crate::NormalStateCatalog::UnnormalizedTaylorBlockV1
                    }
                    GriddedNormalRecordLayout::Joint { .. } => {
                        crate::NormalStateCatalog::UnnormalizedJointBlockV1
                    }
                    GriddedNormalRecordLayout::Scalar => {
                        crate::NormalStateCatalog::UnnormalizedPlaneV1
                    }
                    GriddedNormalRecordLayout::ChannelLocal { .. } => {
                        crate::NormalStateCatalog::UnnormalizedChannelSlabV1
                    }
                }
            || prior.channel_count() != self.manifest.specification.slab().core_depth()
            || prior.coefficient_term_count() != self.manifest.record_layout.coefficient_terms()
            || prior.normal_moment_count() != self.manifest.record_layout.normal_moments()
            || prior.polarization_count() != self.manifest.specification.polarization_count()
            || prior.domain_count() != self.manifest.specification.domain_count()
        {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        if ffts.len() != prepared_specification.chart_count() {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        let model_generation = model.generation_id();
        let reusable_domains = prior.into_reusable_domains()?;
        if reusable_domains.len() != prepared_specification.domain_count() {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        let prepared_specification = Arc::new(prepared_specification);
        let mut operators = Vec::with_capacity(prepared_specification.chart_count());
        for (chart, fft) in prepared_specification.charts().iter().zip(ffts.drain(..)) {
            let mut operator = SpectralSlabOperator::new_chart(
                Arc::clone(&prepared_specification),
                chart,
                workload,
                fft,
                0,
                aw_projection.clone(),
            )?;
            operator
                .prepare_gridded_normal_model(model, &reusable_domains[chart.domain_ordinal()])?;
            operators.push(operator);
        }
        let core_depth = self.accumulation_width();
        let convolution_support = operators
            .iter()
            .map(SpectralSlabOperator::convolution_maximum_support)
            .max()
            .ok_or(SpectralOperatorError::UnsupportedGeometry)?;
        let tile_catalogs = GriddedNormalDomainTileCatalogs::new_for_projection(
            self.manifest
                .specification
                .charts()
                .iter()
                .map(|chart| chart.geometry().grid_shape),
            convolution_support,
            self.manifest.aw_projection,
        )?;
        let two_domain = PreparedGriddedNormalTwoDomainWindow::with_projection_record_capacities(
            route_slot_record_capacities,
            tile_catalogs.tile_count(),
            self.manifest.record_layout,
            self.manifest.aw_projection,
        )?;
        let tile_accumulators = tile_catalogs.accumulators(core_depth)?;
        let domain_planes = || {
            self.manifest
                .specification
                .charts()
                .iter()
                .map(|chart| {
                    let shape = chart.geometry().grid_shape;
                    (0..core_depth)
                        .map(|_| Array2::zeros((shape[0], shape[1])))
                        .collect()
                })
                .collect()
        };
        #[cfg(test)]
        let primary_grid_shape = self.manifest.specification.grid_shape();
        Ok(GriddedNormalOperatorApply {
            program: self.clone(),
            operators,
            reusable_domains,
            model_generation,
            next_block_sequence: 0,
            applied_records: 0,
            next_partition_commit: 0,
            two_domain: RwLock::new(two_domain),
            tile_catalogs,
            tile_accumulators,
            normal_grids: domain_planes(),
            normal_compensations: domain_planes(),
            science_prediction_trace: imaging_science_trace_enabled().then(ScienceTraceDigest::new),
            #[cfg(test)]
            next_sector_commit: 0,
            #[cfg(test)]
            prepared: RwLock::new(PreparedGriddedNormalWindow::with_record_capacities(
                route_slot_record_capacities,
            )?),
            #[cfg(test)]
            sectors: std::array::from_fn(|sector_id| {
                Mutex::new(GriddedNormalSectorAccumulator::new(
                    primary_grid_shape,
                    core_depth,
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
    operators: Vec<SpectralSlabOperator>,
    reusable_domains: Vec<ReusableNormalState>,
    model_generation: crate::ModelGenerationId,
    next_block_sequence: u64,
    applied_records: u64,
    next_partition_commit: usize,
    two_domain: RwLock<PreparedGriddedNormalTwoDomainWindow>,
    tile_catalogs: GriddedNormalDomainTileCatalogs,
    tile_accumulators: Vec<Mutex<GriddedNormalTileAccumulator>>,
    normal_grids: Vec<Vec<Array2<Complex64>>>,
    normal_compensations: Vec<Vec<Array2<Complex64>>>,
    science_prediction_trace: Option<ScienceTraceDigest>,
    #[cfg(test)]
    next_sector_commit: usize,
    #[cfg(test)]
    prepared: RwLock<PreparedGriddedNormalWindow>,
    #[cfg(test)]
    sectors: [Mutex<GriddedNormalSectorAccumulator>; GRIDDED_NORMAL_SECTOR_COUNT],
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct GriddedNormalSectorRoute {
    record_ordinal: u32,
    group_ordinal: u32,
}

#[cfg(test)]
#[derive(Default)]
struct PreparedGriddedNormalBlock {
    sequence: Option<u64>,
    record_count: u64,
    predictions: Vec<Complex64>,
    classifications: Vec<u32>,
    routes: Vec<GriddedNormalSectorRoute>,
    sector_offsets: [u32; GRIDDED_NORMAL_SECTOR_COUNT + 1],
}

#[cfg(test)]
struct PreparedGriddedNormalWindow {
    blocks: Vec<PreparedGriddedNormalBlock>,
    active_frames: usize,
    first_sequence: Option<u64>,
    record_count: u64,
    prediction_groups: u64,
}

#[cfg(test)]
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
            prediction_groups: 0,
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

    fn routing_measurements(
        &self,
        routed_frames: u64,
        routed_records: u64,
        applied_records: u64,
        route_capacity_bytes: u64,
    ) -> GriddedNormalRoutingMeasurements {
        GriddedNormalRoutingMeasurements {
            frames_routed: routed_frames,
            encoded_records: routed_records,
            routed_record_memberships: routed_records,
            prediction_groups: self.prediction_groups,
            degrid_records: routed_records,
            grid_records: applied_records,
            sector_rescans: 0,
            peak_physical_route_capacity_bytes: route_capacity_bytes,
        }
    }
}

#[cfg(test)]
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
                GriddedNormalSectorRoute {
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
                self.routes[route_ordinal] = GriddedNormalSectorRoute {
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
        let route_bytes = capacity_bytes::<GriddedNormalSectorRoute>(self.routes.capacity())?;
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
    ) -> Result<&[GriddedNormalSectorRoute], SpectralOperatorError> {
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

#[cfg(test)]
fn capacity_bytes<T>(capacity: usize) -> Result<u64, SpectralOperatorError> {
    u64::try_from(capacity)
        .ok()
        .and_then(|capacity| capacity.checked_mul(size_of::<T>() as u64))
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GriddedNormalSectorGeometry {
    sector_id: usize,
    origin: [usize; 2],
    shape: [usize; 2],
}

#[cfg(test)]
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

#[cfg(test)]
struct GriddedNormalSectorAccumulator {
    geometry: GriddedNormalSectorGeometry,
    grids: Vec<Array2<Complex64>>,
    compensations: Vec<Array2<Complex64>>,
}

#[cfg(test)]
struct GriddedNormalSectorRouteContext<'a> {
    operator: &'a SpectralSlabOperator,
    encoded: &'a [u8],
    grid_shape: [usize; 2],
    output_channels: usize,
    polarizations: usize,
    sector_id: usize,
    prepared: &'a PreparedGriddedNormalBlock,
}

#[cfg(test)]
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
        context: GriddedNormalSectorRouteContext<'_>,
    ) -> Result<(), SpectralOperatorError> {
        let GriddedNormalSectorRouteContext {
            operator,
            encoded,
            grid_shape,
            output_channels,
            polarizations,
            sector_id,
            prepared,
        } = context;
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
            operator.grid_gridded_normal_local_polarization(
                &mut self.grids,
                &mut self.compensations,
                GriddedNormalLocalContribution::new(
                    self.geometry.translated_taps(record.taps)?,
                    record.output_channel / polarizations,
                    record.output_channel % polarizations,
                    predicted,
                    record.forward_scale.conj() * record.imaging_weight,
                ),
            )?;
        }
        Ok(())
    }
}

/// One stable center-owned spatial partition prepared by reconstruction.
#[doc(hidden)]
#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
#[derive(Debug)]
pub struct GriddedNormalSectorPartial {
    work: GriddedNormalSectorWork,
}

impl GriddedNormalOperatorApply {
    /// Validate one borrowed frame and return the four stable spatial owners.
    #[cfg(test)]
    pub fn sector_partition_count(
        &self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<usize, SpectralOperatorError> {
        self.sector_window_partition_count(std::iter::once((sequence, encoded)))
    }

    /// Validate one ordered borrowed-frame window and prepare its shared routes.
    #[cfg(test)]
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
                validate_encoded_block(descriptor, encoded, self.program.record_bytes())?;
                let (routed, predictions) = {
                    let block = prepared
                        .blocks
                        .get_mut(ordinal)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                    block.prepare(
                        sequence,
                        encoded,
                        self.program.manifest.specification.grid_shape(),
                        self.program.output_plane_count()?,
                        |record| {
                            let polarizations =
                                self.program.manifest.specification.polarization_count();
                            self.operators[0].predict_gridded_normal_polarization(
                                record.output_channel / polarizations,
                                record.output_channel % polarizations,
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
        if routed_record_memberships != encoded_records {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        prepared.prediction_groups = prepared
            .prediction_groups
            .checked_add(prediction_groups)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        debug_assert_eq!(
            prepared.capacity_bytes().ok(),
            Some(physical_capacity_bytes)
        );
        Ok(GRIDDED_NORMAL_SECTOR_COUNT)
    }

    /// Return one worker-count-independent spatial partition by local ordinal.
    #[cfg(test)]
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
        validate_encoded_block(descriptor, encoded, self.program.record_bytes())?;
        self.sector_window_partition(sequence, 1, local_ordinal)
    }

    /// Return one worker-count-independent sector for the active frame window.
    #[cfg(test)]
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
    #[cfg(test)]
    pub fn execute_sector(
        &self,
        encoded: &[u8],
        work: GriddedNormalSectorWork,
    ) -> Result<GriddedNormalSectorPartial, SpectralOperatorError> {
        self.execute_sector_window(std::iter::once((work.first_block_sequence, encoded)), work)
    }

    /// Apply one sector to every prepared frame in increasing source order.
    #[cfg(test)]
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
        let operator = &self.operators[0];
        let mut frame_count = 0usize;
        for (ordinal, (sequence, encoded)) in frames.into_iter().enumerate() {
            let block = prepared.active_block(ordinal)?;
            if block.sequence != Some(sequence) {
                return Err(SpectralOperatorError::BlockSequence);
            }
            sector.execute_routes(GriddedNormalSectorRouteContext {
                operator,
                encoded,
                grid_shape: self.program.manifest.specification.grid_shape(),
                output_channels: self.program.output_plane_count()?,
                polarizations: self.program.manifest.specification.polarization_count(),
                sector_id: work.sector_id,
                prepared: block,
            })?;
            frame_count = ordinal + 1;
        }
        if u64::try_from(frame_count).map_err(|_| SpectralOperatorError::CoverageOverflow)?
            != work.frame_count
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        Ok(GriddedNormalSectorPartial { work })
    }

    /// Commit fixed-size coverage bookkeeping after sector-owned science completed.
    #[cfg(test)]
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

    /// Snapshot exact route-once measurements without changing execution state.
    #[must_use]
    pub fn routing_measurements(&self) -> GriddedNormalRoutingMeasurements {
        self.two_domain_routing_measurements()
    }

    /// Finish `dirty - A* W A x` and return ordinary Major-Cycle input.
    pub fn finish(self) -> Result<CompleteDataOwnerResult, SpectralOperatorError> {
        self.finish_with_routing_measurements()
            .map(|(result, _measurements)| result)
    }

    /// Finish and return the final immutable route-once measurement snapshot.
    pub fn finish_with_routing_measurements(
        mut self,
    ) -> Result<(CompleteDataOwnerResult, GriddedNormalRoutingMeasurements), SpectralOperatorError>
    {
        let active_frames = self
            .two_domain
            .read()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?
            .active_frames;
        if self.next_block_sequence != self.program.block_count()
            || self.applied_records != self.program.manifest.record_count
            || self.next_partition_commit != 0
            || active_frames != 0
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if let Some(trace) = self.science_prediction_trace.take() {
            trace.emit("predicted_visibility");
        }
        let measurements = self.routing_measurements();
        let Self {
            program,
            operators,
            reusable_domains,
            model_generation,
            normal_grids,
            ..
        } = self;
        let domains = combine_chart_updates(
            &program.manifest.specification,
            reusable_domains,
            operators
                .into_iter()
                .zip(normal_grids)
                .map(|(operator, grids)| {
                    operator.finish_gridded_normal_from_grids(model_generation, grids)
                }),
        )?;
        let primitive_catalog = match program.manifest.record_layout {
            GriddedNormalRecordLayout::Taylor(_)
            | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
            | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. } => {
                SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1
            }
            GriddedNormalRecordLayout::Joint { .. } => {
                SpectralPrimitiveCatalog::UnnormalizedJointBlockV1
            }
            GriddedNormalRecordLayout::Scalar => SpectralPrimitiveCatalog::UnnormalizedPlaneV1,
            GriddedNormalRecordLayout::ChannelLocal { .. } => {
                SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1
            }
        };
        Ok((
            CompleteDataOwnerResult {
                domains,
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

#[cfg(test)]
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

#[cfg(test)]
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
        ReconstructionBasis::Constant
            | ReconstructionBasis::ChannelLocal { .. }
            | ReconstructionBasis::Taylor { terms: 2.. }
            | ReconstructionBasis::TaylorViaChannelMajor { terms: 2.., .. }
            | ReconstructionBasis::JointContinuumLine { .. }
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
        || specification
            .slab()
            .total_channels()
            .checked_mul(specification.polarization_count())
            .is_none_or(|planes| planes > 1 << CHANNEL_KEY_BITS)
        || (specification.chart_count() > specification.domain_count()
            && !matches!(
                specification.block_normal_plan(),
                Some(plan) if plan.coefficient_term_count() == 1
            ))
        || specification.charts().iter().any(|chart| {
            chart
                .geometry()
                .grid_shape
                .into_iter()
                .any(|extent| extent > 1 << 12)
        })
    {
        return Err(SpectralOperatorError::UnsupportedGriddedReplay);
    }
    Ok(())
}

fn static_binding(specification: &SpectralOperatorSpecification) -> LogicalIdentity {
    let record_layout = GriddedNormalRecordLayout::for_specification(specification);
    let aw_projection = specification.aw_projection().is_some();
    let mut encoder = Encoder::new(RECORD_DOMAIN, RECORD_VERSION);
    encoder.identity(specification.problem_id().as_bytes());
    encoder.identity(specification.geometry_id().as_bytes());
    encoder.identity(specification.numerics_id().as_bytes());
    encoder.identity(specification.weighting_commitment_id().as_bytes());
    encoder.u8(u8::from(aw_projection));
    encoder.usize(specification.chart_count());
    for chart in specification.charts() {
        encoder.usize(chart.ordinal());
        encoder.usize(chart.domain_ordinal());
        encoder.usize(chart.facet_ordinal());
        for value in chart
            .window()
            .origin()
            .into_iter()
            .chain(chart.window().end_exclusive())
        {
            encoder.usize(value);
        }
        encoder.usize(chart.geometry().grid_shape[0]);
        encoder.usize(chart.geometry().grid_shape[1]);
    }
    encoder.usize(specification.slab().total_channels());
    match record_layout {
        GriddedNormalRecordLayout::Scalar => {
            encoder.u8(0);
            encoder.usize(
                record_bytes(record_layout, aw_projection).expect("validated scalar record width"),
            );
        }
        GriddedNormalRecordLayout::ChannelLocal { channels } => {
            encoder.u8(3);
            encoder.usize(channels);
            encoder.usize(
                record_bytes(record_layout, aw_projection)
                    .expect("validated channel-local record width"),
            );
        }
        GriddedNormalRecordLayout::Taylor(plan)
        | GriddedNormalRecordLayout::TaylorWithCoordinates(plan) => {
            encoder.u8(
                if matches!(record_layout, GriddedNormalRecordLayout::Taylor(_)) {
                    1
                } else {
                    5
                },
            );
            encoder.usize(plan.coefficient_term_count());
            encoder.usize(plan.normal_moment_count());
            encoder.u64(plan.reference_frequency_hz().to_bits());
            encoder.usize(
                record_bytes(record_layout, aw_projection).expect("validated Taylor record width"),
            );
        }
        GriddedNormalRecordLayout::TaylorViaChannelMajor { plan, channels } => {
            encoder.u8(4);
            encoder.usize(plan.coefficient_term_count());
            encoder.usize(plan.normal_moment_count());
            encoder.u64(plan.reference_frequency_hz().to_bits());
            encoder.usize(channels);
            encoder.usize(
                record_bytes(record_layout, aw_projection)
                    .expect("validated channel-major record width"),
            );
        }
        GriddedNormalRecordLayout::Joint {
            coefficient_terms,
            normal_moments,
        } => {
            encoder.u8(2);
            encoder.usize(coefficient_terms);
            encoder.usize(normal_moments);
            encoder.usize(
                record_bytes(record_layout, aw_projection).expect("validated joint record width"),
            );
        }
    }
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

fn group_and_reduce<const OBSERVE_SOURCE_CARDINALITY: bool>(
    source_groups: Vec<Vec<ReducedRecordKey>>,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<
    (
        Vec<ReducedRecordGroup>,
        Option<GriddedNormalSourceCardinality>,
    ),
    SpectralOperatorError,
> {
    let mut groups = BTreeMap::<Vec<ReducedRecordKey>, Vec<f64>>::new();
    for group in source_groups {
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
    reduce_groups::<OBSERVE_SOURCE_CARDINALITY>(groups)
}

fn group_and_reduce_taylor<const OBSERVE_SOURCE_CARDINALITY: bool>(
    keys: Vec<TaylorRecordKey>,
    plan: crate::block_normal::BlockNormalPlan,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<
    (
        Vec<ReducedTaylorRecord>,
        Option<GriddedNormalSourceCardinality>,
    ),
    SpectralOperatorError,
> {
    let source_cardinality = if OBSERVE_SOURCE_CARDINALITY {
        let count =
            u64::try_from(keys.len()).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        Some(GriddedNormalSourceCardinality {
            groups: count,
            records: count,
        })
    } else {
        None
    };
    let normal_moments = plan.normal_moment_count();
    let mut scratch = measured_zeroed_f64_buffer(normal_moments, measurements)?;
    let mut grouped = BTreeMap::<u64, TaylorMomentAccumulator>::new();
    for key in keys {
        plan.fill_normal_moment_weights(
            f64::from_bits(key.frequency_hz),
            f64::from_bits(key.imaging_weight),
            &mut scratch,
        )
        .map_err(|_| SpectralOperatorError::InvalidSample)?;
        let accumulator = match grouped.entry(key.taps) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                measurements.reduction_map_entry_insertions = measurements
                    .reduction_map_entry_insertions
                    .checked_add(1)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                entry.insert(TaylorMomentAccumulator {
                    moments: measured_zeroed_f64_buffer(normal_moments, measurements)?,
                    compensations: measured_zeroed_f64_buffer(normal_moments, measurements)?,
                })
            }
        };
        for ((sum, compensation), value) in accumulator
            .moments
            .iter_mut()
            .zip(&mut accumulator.compensations)
            .zip(&scratch)
        {
            let contribution = *value - *compensation;
            let updated = *sum + contribution;
            *compensation = (updated - *sum) - contribution;
            *sum = updated;
            if !sum.is_finite() || !compensation.is_finite() {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
        }
    }
    let records = grouped
        .into_iter()
        .map(|(taps, accumulator)| ReducedTaylorRecord {
            taps,
            moments: accumulator
                .moments
                .into_iter()
                .map(|value| f64::from_bits(canonical_zero_bits(value)))
                .collect(),
        })
        .collect();
    Ok((records, source_cardinality))
}

fn measured_zeroed_f64_buffer(
    length: usize,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<Vec<f64>, SpectralOperatorError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    values.resize(length, 0.0);
    measurements.multiplicity_vector_allocations = measurements
        .multiplicity_vector_allocations
        .checked_add(u64::from(length != 0))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let capacity_bytes = values
        .capacity()
        .checked_mul(size_of::<f64>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    measurements.multiplicity_capacity_growth_bytes = measurements
        .multiplicity_capacity_growth_bytes
        .checked_add(capacity_bytes)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(values)
}

fn reduce_groups<const OBSERVE_SOURCE_CARDINALITY: bool>(
    groups: BTreeMap<Vec<ReducedRecordKey>, Vec<f64>>,
) -> Result<
    (
        Vec<ReducedRecordGroup>,
        Option<GriddedNormalSourceCardinality>,
    ),
    SpectralOperatorError,
> {
    let mut source_groups = 0_usize;
    let mut source_records = 0_usize;
    let mut reduced = Vec::with_capacity(groups.len());
    for (records, mut multiplicities) in groups {
        if records.is_empty() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        if OBSERVE_SOURCE_CARDINALITY {
            source_groups = source_groups
                .checked_add(multiplicities.len())
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            source_records = source_records
                .checked_add(
                    records
                        .len()
                        .checked_mul(multiplicities.len())
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?,
                )
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        }
        multiplicities.sort_by(f64::total_cmp);
        reduced.push(ReducedRecordGroup {
            records,
            multiplicity: compensated_sum(&multiplicities)?,
        });
    }
    let source_cardinality = if OBSERVE_SOURCE_CARDINALITY {
        Some(GriddedNormalSourceCardinality {
            groups: u64::try_from(source_groups)
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            records: u64::try_from(source_records)
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
        })
    } else {
        None
    };
    Ok((reduced, source_cardinality))
}

#[cfg(test)]
fn encode_and_checksum(
    groups: Vec<ReducedRecordGroup>,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<(Box<[u8]>, [u8; 32]), SpectralOperatorError> {
    encode_and_checksum_mode(groups, false, measurements)
}

fn encode_and_checksum_mode(
    groups: Vec<ReducedRecordGroup>,
    aw_projection: bool,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<(Box<[u8]>, [u8; 32]), SpectralOperatorError> {
    let record_count = groups.iter().try_fold(0_usize, |total, group| {
        total
            .checked_add(group.records.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    })?;
    let capacity = record_count
        .checked_mul(if aw_projection {
            AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES
        } else {
            GRIDDED_NORMAL_OPERATOR_RECORD_BYTES
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    for group in groups {
        let last = group.records.len() - 1;
        for (index, record) in group.records.into_iter().enumerate() {
            if aw_projection {
                let aw = record
                    .aw
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
                let output_channel = u64::from(record.output_channel);
                if output_channel > CHANNEL_KEY_MASK
                    || record.chart_ordinal >= 1 << 24
                    || aw.mueller_element >= 16
                {
                    return Err(SpectralOperatorError::InvalidGriddedRecord);
                }
                let key = output_channel
                    | (u64::from(record.chart_ordinal) << TAP_KEY_BITS)
                    | (u64::from(aw.mueller_element) << AW_MUELLER_SHIFT)
                    | if index == last { AW_GROUP_END_BIT } else { 0 };
                let forward_real = f64::from_bits(record.forward_real);
                let forward_imaginary = f64::from_bits(record.forward_imaginary);
                let imaging_weight = f64::from_bits(record.imaging_weight) * group.multiplicity;
                let coordinates: AwReplayCoordinates = aw.into();
                if !valid_aw_coordinates(coordinates)
                    || !forward_real.is_finite()
                    || !forward_imaginary.is_finite()
                    || (forward_real == 0.0 && forward_imaginary == 0.0)
                    || !imaging_weight.is_finite()
                    || imaging_weight.is_sign_negative()
                {
                    return Err(SpectralOperatorError::GeneratedNonfinite);
                }
                encoded.extend_from_slice(&key.to_le_bytes());
                for value in [
                    coordinates.frequency_hz,
                    coordinates.uvw_m[0],
                    coordinates.uvw_m[1],
                    coordinates.uvw_m[2],
                    coordinates.prediction_w_m,
                    coordinates.parallactic_angle_deg,
                    coordinates.pointing_phase_gradient_rad_per_grid_cell[0],
                    coordinates.pointing_phase_gradient_rad_per_grid_cell[1],
                    forward_real,
                    forward_imaginary,
                    imaging_weight,
                ] {
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                continue;
            }
            if record.aw.is_some() {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }
            let output_channel = u64::from(record.output_channel);
            if output_channel > CHANNEL_KEY_MASK || record.chart_ordinal >= 1 << 24 {
                return Err(SpectralOperatorError::InvalidGriddedRecord);
            }
            let key = (record.taps & TAP_KEY_MASK)
                | (output_channel << TAP_KEY_BITS)
                | if index == last { GROUP_END_BIT } else { 0 };
            let route = u64::from(record.chart_ordinal) | ((record.taps >> 24) << 24);
            let forward_real = f64::from_bits(record.forward_real);
            let forward_imaginary = f64::from_bits(record.forward_imaginary);
            let imaging_weight = f64::from_bits(record.imaging_weight) * group.multiplicity;
            if !forward_real.is_finite()
                || !forward_imaginary.is_finite()
                || (forward_real == 0.0 && forward_imaginary == 0.0)
                || !imaging_weight.is_finite()
                || imaging_weight.is_sign_negative()
            {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            encoded.extend_from_slice(&key.to_le_bytes());
            encoded.extend_from_slice(&route.to_le_bytes());
            encoded.extend_from_slice(&forward_real.to_le_bytes());
            encoded.extend_from_slice(&forward_imaginary.to_le_bytes());
            encoded.extend_from_slice(&imaging_weight.to_le_bytes());
        }
    }
    let encoded = encoded.into_boxed_slice();
    if !encoded.is_empty() {
        measurements.encoded_buffer_allocations = 1;
    }
    measurements.encoded_buffer_bytes =
        u64::try_from(encoded.len()).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    let digest = Sha256::digest(&encoded).into();
    Ok((encoded, digest))
}

fn valid_aw_coordinates(value: AwReplayCoordinates) -> bool {
    value.frequency_hz.is_finite()
        && value.frequency_hz > 0.0
        && value.uvw_m.into_iter().all(f64::is_finite)
        && value.prediction_w_m.is_finite()
        && value.parallactic_angle_deg.is_finite()
        && value
            .pointing_phase_gradient_rad_per_grid_cell
            .into_iter()
            .all(f64::is_finite)
        && value.mueller_element < 16
}

fn encode_taylor_and_checksum(
    records: Vec<ReducedTaylorRecord>,
    plan: crate::block_normal::BlockNormalPlan,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<(Box<[u8]>, [u8; 32]), SpectralOperatorError> {
    let record_bytes = GriddedNormalRecordLayout::Taylor(plan).record_bytes()?;
    let capacity = records
        .len()
        .checked_mul(record_bytes)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    for record in records {
        if record.moments.len() != plan.normal_moment_count()
            || record.moments.iter().any(|value| !value.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        encoded.extend_from_slice(&record.taps.to_le_bytes());
        for moment in record.moments {
            encoded.extend_from_slice(&moment.to_le_bytes());
        }
    }
    if encoded.len() != capacity {
        return Err(SpectralOperatorError::ResidencyOverflow);
    }
    let encoded = encoded.into_boxed_slice();
    if !encoded.is_empty() {
        measurements.encoded_buffer_allocations = 1;
    }
    measurements.encoded_buffer_bytes =
        u64::try_from(encoded.len()).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    let digest = Sha256::digest(&encoded).into();
    Ok((encoded, digest))
}

#[cfg(test)]
fn encode_reduced<const OBSERVE_SOURCE_CARDINALITY: bool>(
    groups: BTreeMap<Vec<ReducedRecordKey>, Vec<f64>>,
) -> Result<(Box<[u8]>, Option<GriddedNormalSourceCardinality>), SpectralOperatorError> {
    let (groups, source_cardinality) = reduce_groups::<OBSERVE_SOURCE_CARDINALITY>(groups)?;
    let (encoded, _) = encode_and_checksum(
        groups,
        &mut GriddedNormalOperatorBlockMeasurements::default(),
    )?;
    Ok((encoded, source_cardinality))
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
        || taps.x.weight_index >= 1 << 32
        || taps.y.weight_index >= 1 << 8
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(taps.x.start as u64
        | ((taps.y.start as u64) << 12)
        | ((taps.x.weight_index as u64) << 24)
        | ((taps.y.weight_index as u64) << 56))
}

#[cfg(test)]
fn decode_record(
    encoded: &[u8],
    grid_shape: [usize; 2],
    output_channels: usize,
) -> Result<DecodedRecord, SpectralOperatorError> {
    let record = decode_record_for_shape(encoded, grid_shape, output_channels)?;
    if record.chart_ordinal != 0 {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(record)
}

fn decode_domain_record(
    encoded: &[u8],
    catalogs: &GriddedNormalDomainTileCatalogs,
    output_channels: usize,
) -> Result<DecodedRecord, SpectralOperatorError> {
    if encoded.len() == AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES {
        let record = decode_aw_record(encoded, output_channels)?;
        if catalogs.grid_shape(record.chart_ordinal).is_none() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        return Ok(record);
    }
    if encoded.len() != GRIDDED_NORMAL_OPERATOR_RECORD_BYTES {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let route = u64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let chart_ordinal = usize::try_from(route & 0x00ff_ffff)
        .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
    let grid_shape = catalogs
        .grid_shape(chart_ordinal)
        .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
    decode_record_for_shape(encoded, grid_shape, output_channels)
}

fn decode_aw_record(
    encoded: &[u8],
    output_channels: usize,
) -> Result<DecodedRecord, SpectralOperatorError> {
    if encoded.len() != AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let key = u64::from_le_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let value = |ordinal: usize| -> Result<f64, SpectralOperatorError> {
        let start = 8 + ordinal * size_of::<f64>();
        Ok(f64::from_le_bytes(
            encoded
                .get(start..start + size_of::<f64>())
                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?
                .try_into()
                .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
        ))
    };
    let output_channel = usize::try_from(key & CHANNEL_KEY_MASK)
        .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
    let chart_ordinal = usize::try_from((key >> TAP_KEY_BITS) & CHANNEL_KEY_MASK)
        .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
    let aw = AwReplayCoordinates {
        frequency_hz: value(0)?,
        uvw_m: [value(1)?, value(2)?, value(3)?],
        prediction_w_m: value(4)?,
        parallactic_angle_deg: value(5)?,
        pointing_phase_gradient_rad_per_grid_cell: [value(6)?, value(7)?],
        mueller_element: u32::try_from((key >> AW_MUELLER_SHIFT) & 0x0f)
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    };
    let forward_scale = Complex64::new(value(8)?, value(9)?);
    let imaging_weight = value(10)?;
    if key & !AW_RECORD_KEY_MASK != 0
        || output_channel >= output_channels
        || !valid_aw_coordinates(aw)
        || !forward_scale.re.is_finite()
        || !forward_scale.im.is_finite()
        || (forward_scale.re == 0.0 && forward_scale.im == 0.0)
        || !imaging_weight.is_finite()
        || imaging_weight.is_sign_negative()
    {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    Ok(DecodedRecord {
        chart_ordinal,
        output_channel,
        taps: SampleTaps {
            x: TapSpan {
                start: 0,
                weight_index: 0,
            },
            y: TapSpan {
                start: 0,
                weight_index: 0,
            },
        },
        forward_scale,
        imaging_weight,
        group_end: key & AW_GROUP_END_BIT != 0,
        aw: Some(aw),
    })
}

fn decode_record_for_shape(
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
        encoded[16..24]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let forward_imaginary = f64::from_le_bytes(
        encoded[24..32]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let imaging_weight = f64::from_le_bytes(
        encoded[32..]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let output_channel = usize::try_from((key >> TAP_KEY_BITS) & CHANNEL_KEY_MASK)
        .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
    let route = u64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let chart_ordinal = usize::try_from(route & 0x00ff_ffff)
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
    let taps = decode_tap_key((key & TAP_KEY_MASK) | ((route >> 24) << 24), grid_shape)?;
    Ok(DecodedRecord {
        chart_ordinal,
        output_channel,
        taps,
        forward_scale: Complex64::new(forward_real, forward_imaginary),
        imaging_weight,
        group_end: key & GROUP_END_BIT != 0,
        aw: None,
    })
}

pub(super) fn decode_taylor_record(
    encoded: &[u8],
    grid_shape: [usize; 2],
    normal_moments: usize,
) -> Result<DecodedTaylorRecord<'_>, SpectralOperatorError> {
    let expected_bytes = normal_moments
        .checked_add(1)
        .and_then(|values| values.checked_mul(size_of::<u64>()))
        .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
    if encoded.len() != expected_bytes || normal_moments == 0 {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let key = u64::from_le_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
    );
    let moment_bytes = &encoded[8..];
    for bytes in moment_bytes.chunks_exact(size_of::<f64>()) {
        let value = f64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?,
        );
        if !value.is_finite() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
    }
    Ok(DecodedTaylorRecord {
        taps: decode_tap_key(key, grid_shape)?,
        moment_bytes,
    })
}

fn decode_tap_key(
    tap_key: u64,
    grid_shape: [usize; 2],
) -> Result<SampleTaps, SpectralOperatorError> {
    let taps = SampleTaps {
        x: TapSpan {
            start: (tap_key & 0x0fff) as usize,
            weight_index: ((tap_key >> 24) & 0xffff_ffff) as usize,
        },
        y: TapSpan {
            start: ((tap_key >> 12) & 0x0fff) as usize,
            weight_index: ((tap_key >> 56) & 0xff) as usize,
        },
    };
    if taps
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
    Ok(taps)
}

fn validate_encoded_block(
    descriptor: &BlockDescriptor,
    encoded: &[u8],
    record_bytes: usize,
) -> Result<(), SpectralOperatorError> {
    let expected_bytes = usize::try_from(descriptor.record_count)
        .ok()
        .and_then(|records| records.checked_mul(record_bytes))
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
            reference_pixel: [4.0, 4.0],
            increment_rad: [-2.0e-3, 2.0e-3],
            direction: casa_imaging_model::DirectionCoordinateSpec::new(
                casa_imaging_model::Projection::Sin,
                casa_imaging_model::SkyDirection::new(
                    casa_imaging_model::DirectionFrame::J2000,
                    1.0,
                    -0.5,
                ),
                [4.0, 4.0],
                [-2.0e-3, 2.0e-3],
                [[1.0, 0.0], [0.0, 1.0]],
                [180.0, 0.0],
            ),
        }
    }

    fn t42_taps() -> SampleTaps {
        SampleTaps {
            x: TapSpan {
                start: 1,
                weight_index: 7,
            },
            y: TapSpan {
                start: 2,
                weight_index: 11,
            },
        }
    }

    #[test]
    fn t42_taylor_v5_codec_has_dynamic_width_and_rejects_truncation_and_nonfinite_moments() {
        let plan = crate::block_normal::BlockNormalPlan::taylor(1.0e9, 3).unwrap();
        let layout = GriddedNormalRecordLayout::Taylor(plan);
        assert_eq!(RECORD_VERSION, 8);
        assert_eq!(layout.record_bytes().unwrap(), 48);
        assert_eq!(
            GriddedNormalRecordLayout::Taylor(
                crate::block_normal::BlockNormalPlan::taylor(1.0e9, 2).unwrap()
            )
            .record_bytes()
            .unwrap(),
            32
        );

        let moments = [2.0, -0.25, 0.125, -0.03125, 0.0078125];
        let (encoded, _) = encode_taylor_and_checksum(
            vec![ReducedTaylorRecord {
                taps: encode_taps(t42_taps()).unwrap(),
                moments: moments.into(),
            }],
            plan,
            &mut GriddedNormalOperatorBlockMeasurements::default(),
        )
        .unwrap();
        assert_eq!(encoded.len(), 48);
        let decoded = decode_taylor_record(&encoded, [10, 10], 5).unwrap();
        assert_eq!(decoded.taps, t42_taps());
        let mut decoded_moments = [0.0; 5];
        decoded.fill_moments(&mut decoded_moments).unwrap();
        assert_eq!(decoded_moments, moments);
        assert!(matches!(
            decode_taylor_record(&encoded[..32], [10, 10], 5),
            Err(SpectralOperatorError::InvalidGriddedRecord)
        ));

        let mut corrupt = encoded.into_vec();
        corrupt[8..16].copy_from_slice(&f64::NAN.to_le_bytes());
        assert!(matches!(
            decode_taylor_record(&corrupt, [10, 10], 5),
            Err(SpectralOperatorError::InvalidGriddedRecord)
        ));
    }

    #[test]
    fn t51_direct_taylor_aw_replay_uses_the_shared_coordinate_record() {
        let plan = crate::block_normal::BlockNormalPlan::taylor(3.0e9, 2).unwrap();
        let layout = GriddedNormalRecordLayout::Taylor(plan);
        assert_eq!(
            record_bytes(layout, true).expect("direct Taylor AW CLEAN must admit replay"),
            AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES
        );
        let coordinates = GriddedNormalRecordLayout::TaylorWithCoordinates(plan);
        assert_eq!(record_bytes(coordinates, true).unwrap(), 96);
        assert_eq!(coordinates.prediction_width(), 1);
        assert_eq!(coordinates.accumulation_width(1), 2);
        assert_eq!(coordinates.coefficient_terms(), 2);
        assert_eq!(coordinates.normal_moments(), 3);
        assert_eq!(record_bytes(layout, false).unwrap(), 32);
    }

    #[test]
    fn t51_aw_codec_preserves_replay_coordinates_and_diagonal_mueller_cells() {
        let coordinates = [
            AwReplayCoordinates {
                frequency_hz: 1.25e9,
                uvw_m: [125.0, -72.5, 911.25],
                prediction_w_m: 925.5,
                parallactic_angle_deg: 37.5,
                pointing_phase_gradient_rad_per_grid_cell: [1.25e-4, -2.5e-4],
                mueller_element: 0,
            },
            AwReplayCoordinates {
                frequency_hz: 1.75e9,
                uvw_m: [-14.0, 88.0, -413.5],
                prediction_w_m: -421.75,
                parallactic_angle_deg: 312.0,
                pointing_phase_gradient_rad_per_grid_cell: [-3.0e-4, 4.5e-4],
                mueller_element: 15,
            },
        ];
        let records = coordinates
            .into_iter()
            .enumerate()
            .map(|(ordinal, coordinates)| ReducedRecordKey {
                chart_ordinal: (ordinal + 2) as u32,
                output_channel: (ordinal + 1) as u32,
                taps: 0,
                forward_real: canonical_zero_bits(0.75 + ordinal as f64),
                forward_imaginary: canonical_zero_bits(-0.25 - ordinal as f64),
                imaging_weight: canonical_zero_bits(2.0 + ordinal as f64),
                aw: Some(AwRecordCoordinates::from(coordinates)),
            })
            .collect();
        let (encoded, _) = encode_and_checksum_mode(
            vec![ReducedRecordGroup {
                records,
                multiplicity: 2.0,
            }],
            true,
            &mut GriddedNormalOperatorBlockMeasurements::default(),
        )
        .expect("encode AW replay records");

        assert_eq!(encoded.len(), 2 * AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
        let decoded = encoded
            .chunks_exact(AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .map(|record| decode_aw_record(record, 4).expect("decode AW replay record"))
            .collect::<Vec<_>>();
        assert_eq!(decoded[0].chart_ordinal, 2);
        assert_eq!(decoded[0].output_channel, 1);
        assert_eq!(decoded[0].aw, Some(coordinates[0]));
        assert_eq!(decoded[0].forward_scale, Complex64::new(0.75, -0.25));
        assert_eq!(decoded[0].imaging_weight, 4.0);
        assert!(!decoded[0].group_end);
        assert_eq!(decoded[1].chart_ordinal, 3);
        assert_eq!(decoded[1].output_channel, 2);
        assert_eq!(decoded[1].aw, Some(coordinates[1]));
        assert_eq!(decoded[1].forward_scale, Complex64::new(1.75, -1.25));
        assert_eq!(decoded[1].imaging_weight, 6.0);
        assert!(decoded[1].group_end);
        assert_eq!(RECORD_VERSION, 8);
        assert_eq!(AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES, 96);
        assert!(matches!(
            decode_aw_record(&encoded[..88], 4),
            Err(SpectralOperatorError::InvalidGriddedRecord)
        ));
        let mut corrupt = encoded[..AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES].to_vec();
        corrupt[40..48].copy_from_slice(&f64::NAN.to_le_bytes());
        assert!(matches!(
            decode_aw_record(&corrupt, 4),
            Err(SpectralOperatorError::InvalidGriddedRecord)
        ));
    }

    #[test]
    fn t42_v5_domain_scalar_cannot_enter_a_taylor_program() {
        fn common_static_binding(version: u32) -> Encoder {
            let mut encoder = Encoder::new(RECORD_DOMAIN, version);
            encoder.identity([1; 32]);
            encoder.identity([2; 32]);
            encoder.identity([3; 32]);
            encoder.identity([4; 32]);
            encoder.usize(10);
            encoder.usize(10);
            encoder.usize(1);
            encoder
        }

        let mut legacy_v2 = common_static_binding(2);
        legacy_v2.usize(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
        let legacy_v2 = LogicalIdentity::from_sha256(legacy_v2.finish());

        let plan = crate::block_normal::BlockNormalPlan::taylor(1.0e9, 2).unwrap();
        let layout = GriddedNormalRecordLayout::Taylor(plan);
        let mut taylor_v4 = common_static_binding(RECORD_VERSION);
        taylor_v4.u8(1);
        taylor_v4.usize(plan.coefficient_term_count());
        taylor_v4.usize(plan.normal_moment_count());
        taylor_v4.u64(plan.reference_frequency_hz().to_bits());
        taylor_v4.usize(layout.record_bytes().unwrap());
        let taylor_v4 = LogicalIdentity::from_sha256(taylor_v4.finish());

        assert_eq!(RECORD_VERSION, 8);
        assert_eq!(layout.record_bytes().unwrap(), 32);
        assert_ne!(
            legacy_v2, taylor_v4,
            "the v5 Taylor layout has a distinct static schema binding"
        );

        let (legacy_scalar, _) =
            encode_reduced::<false>(scalar_groups([(t42_taps(), 1.0)])).unwrap();
        assert_eq!(legacy_scalar.len(), GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
        assert_ne!(legacy_scalar.len(), layout.record_bytes().unwrap());
        let legacy_descriptor = BlockDescriptor {
            source_samples: 1,
            record_count: 1,
            digest: Sha256::digest(&legacy_scalar).into(),
        };
        assert_eq!(
            validate_encoded_block(&legacy_descriptor, &legacy_scalar, 32),
            Err(SpectralOperatorError::GriddedRecordMismatch),
            "domain-tagged scalar framing cannot collide with Taylor framing"
        );
        assert!(
            matches!(
                decode_taylor_record(&legacy_scalar, [10, 10], plan.normal_moment_count()),
                Err(SpectralOperatorError::InvalidGriddedRecord)
            ),
            "v4 Taylor never falls back to the domain scalar decoder"
        );

        let (taylor, _) = encode_taylor_and_checksum(
            vec![ReducedTaylorRecord {
                taps: encode_taps(t42_taps()).unwrap(),
                moments: [1.0, -0.25, 0.0625].into(),
            }],
            plan,
            &mut GriddedNormalOperatorBlockMeasurements::default(),
        )
        .unwrap();
        let taylor_descriptor = BlockDescriptor {
            source_samples: 1,
            record_count: 1,
            digest: Sha256::digest(&taylor).into(),
        };
        assert_eq!(
            validate_encoded_block(&taylor_descriptor, &legacy_scalar, 32),
            Err(SpectralOperatorError::GriddedRecordMismatch),
            "a sealed Taylor descriptor rejects same-width legacy bytes before decode"
        );
    }

    #[test]
    fn t42_taylor_reduction_is_compensated_signed_and_observer_free_by_default() {
        let plan = crate::block_normal::BlockNormalPlan::taylor(1.0e9, 2).unwrap();
        let taps = encode_taps(t42_taps()).unwrap();
        let keys = [0.8e9_f64, 1.2e9_f64]
            .map(|frequency_hz| TaylorRecordKey {
                taps,
                frequency_hz: frequency_hz.to_bits(),
                imaging_weight: 1.0_f64.to_bits(),
            })
            .to_vec();
        let mut unobserved_measurements = GriddedNormalOperatorBlockMeasurements::default();
        let (unobserved, cardinality) =
            group_and_reduce_taylor::<false>(keys.clone(), plan, &mut unobserved_measurements)
                .unwrap();
        assert_eq!(cardinality, None);
        assert_eq!(unobserved.len(), 1);

        let mut observed_measurements = GriddedNormalOperatorBlockMeasurements::default();
        let (observed, cardinality) =
            group_and_reduce_taylor::<true>(keys, plan, &mut observed_measurements).unwrap();
        assert_eq!(
            cardinality,
            Some(GriddedNormalSourceCardinality {
                groups: 2,
                records: 2,
            })
        );
        assert_eq!(observed.len(), 1);
        assert_eq!(unobserved[0].taps, observed[0].taps);
        assert_eq!(unobserved[0].moments, observed[0].moments);

        let mut low = [0.0; 3];
        let mut high = [0.0; 3];
        plan.fill_normal_moment_weights(0.8e9, 1.0, &mut low)
            .unwrap();
        plan.fill_normal_moment_weights(1.2e9, 1.0, &mut high)
            .unwrap();
        let expected = [low[0] + high[0], 0.0, low[2] + high[2]];
        assert_eq!(&*observed[0].moments, &expected);
        assert_eq!(observed[0].moments[1].to_bits(), 0.0_f64.to_bits());
    }

    fn scalar_groups(
        contributions: impl IntoIterator<Item = (SampleTaps, f64)>,
    ) -> BTreeMap<Vec<ReducedRecordKey>, Vec<f64>> {
        let mut groups = BTreeMap::new();
        for (taps, coefficient) in contributions {
            groups
                .entry(vec![ReducedRecordKey {
                    chart_ordinal: 0,
                    output_channel: 0,
                    taps: encode_taps(taps).expect("encode scalar taps"),
                    forward_real: 1.0_f64.to_bits(),
                    forward_imaginary: 0,
                    imaging_weight: 1.0_f64.to_bits(),
                    aw: None,
                }])
                .or_insert_with(Vec::new)
                .push(coefficient);
        }
        groups
    }

    #[test]
    fn domain_records_share_one_prediction_group_and_retain_canonical_ordinals() {
        let taps = t42_taps();
        let record = |chart_ordinal, forward_real: f64| ReducedRecordKey {
            chart_ordinal,
            output_channel: 0,
            taps: encode_taps(taps).expect("encode domain taps"),
            forward_real: forward_real.to_bits(),
            forward_imaginary: 0,
            imaging_weight: 1.0_f64.to_bits(),
            aw: None,
        };
        let mut groups = BTreeMap::new();
        groups.insert(vec![record(0, 1.0), record(1, 2.0)], vec![1.0]);
        let (encoded, _) = encode_reduced::<false>(groups).expect("encode shared domain group");
        assert_eq!(encoded.len(), 2 * GRIDDED_NORMAL_OPERATOR_RECORD_BYTES);
        let catalogs = GriddedNormalDomainTileCatalogs::new([[10, 10], [10, 10]], SUPPORT)
            .expect("two domain catalogs");
        let decoded = encoded
            .chunks_exact(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .map(|record| decode_domain_record(record, &catalogs, 1).expect("decode domain record"))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded
                .iter()
                .map(|record| record.chart_ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(!decoded[0].group_end);
        assert!(decoded[1].group_end);
        assert_eq!(decoded[0].forward_scale.re, 1.0);
        assert_eq!(decoded[1].forward_scale.re, 2.0);
    }

    #[test]
    fn t49_w_plan_round_trips_through_compact_tiled_replay() {
        let geometry = SpectralOperatorGeometry {
            image_shape: [48, 48],
            grid_shape: [64, 64],
            image_blc: [8, 8],
            ..geometry()
        };
        let operator = ConvolutionOperator::new(
            &geometry,
            Some(
                casa_imaging_model::WProjectionContract::new(
                    10_000.0,
                    std::num::NonZeroUsize::new(9),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let taps = operator
            .taps([12.25, -7.5, 4_000.0])
            .expect("W sample fits the grid");
        let (encoded, _) = encode_reduced::<false>(scalar_groups([(taps, 1.0)])).unwrap();
        let support = operator.maximum_support();
        let catalogs =
            GriddedNormalDomainTileCatalogs::new([geometry.grid_shape], support).unwrap();
        let decoded = decode_domain_record(&encoded, &catalogs, 1).unwrap();
        assert_eq!(decoded.taps, taps);

        let tile = catalogs.tile_ordinal(0, taps).unwrap();
        let (_, tile_geometry) = catalogs.geometry(tile).unwrap();
        let local_taps = tile_geometry.translated_taps(decoded.taps).unwrap();
        let shape = tile_geometry.shape();
        let mut local = Array2::zeros((shape[0], shape[1]));
        let mut local_compensation = Array2::zeros((shape[0], shape[1]));
        let value = Complex64::new(0.75, -0.4);
        operator
            .grid_compensated(&mut local, &mut local_compensation, local_taps, value)
            .unwrap();

        let mut global = Array2::zeros((geometry.grid_shape[0], geometry.grid_shape[1]));
        let mut global_compensation = global.clone();
        operator
            .grid_compensated(&mut global, &mut global_compensation, taps, value)
            .unwrap();
        let origin = tile_geometry.origin();
        for x in 0..shape[0] {
            for y in 0..shape[1] {
                assert_eq!(local[(x, y)], global[(origin[0] + x, origin[1] + y)]);
            }
        }
    }

    #[test]
    fn records_are_fixed_width_canonical_and_permutation_independent() {
        let gridder = StandardConvolution::new(&geometry());
        let first = gridder.taps([0.0, 0.0]).expect("central taps");
        let second = gridder
            .taps([20.0, -14.0])
            .expect("offset taps remain on grid");
        let (left, _) = encode_reduced::<false>(scalar_groups([
            (first, 0.75),
            (second, 0.4),
            (first, 1.25),
            (second, 0.6),
        ]))
        .expect("encode records");
        let (right, _) = encode_reduced::<false>(scalar_groups([
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
    fn t49_w_records_are_canonical_and_permutation_independent() {
        let mut geometry = geometry();
        geometry.image_shape = [48, 48];
        geometry.grid_shape = [64, 64];
        geometry.image_blc = [8, 8];
        let operator = ConvolutionOperator::new(
            &geometry,
            Some(
                casa_imaging_model::WProjectionContract::new(
                    10_000.0,
                    std::num::NonZeroUsize::new(9),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let first = operator
            .taps([12.25, -7.5, 4_000.0])
            .expect("first W sample fits the grid");
        let second = operator
            .taps([-8.5, 9.25, -7_000.0])
            .expect("second W sample fits the grid");
        let (left, _) = encode_reduced::<false>(scalar_groups([
            (first, 0.75),
            (second, 0.4),
            (first, 1.25),
            (second, 0.6),
        ]))
        .expect("encode W records");
        let (right, _) = encode_reduced::<false>(scalar_groups([
            (second, 0.6),
            (first, 1.25),
            (second, 0.4),
            (first, 0.75),
        ]))
        .expect("encode permuted W records");
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
        let (encoded, _) =
            encode_reduced::<false>(scalar_groups(contributions)).expect("reduce records");
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
                chart_ordinal: 0,
                output_channel: 0,
                taps: encode_taps(taps).expect("encode grouped taps"),
                forward_real: (1.0 + index as f64 * 0.125).to_bits(),
                forward_imaginary: (index as f64 * -0.025).to_bits(),
                imaging_weight: (0.5 + index as f64 * 0.125).to_bits(),
                aw: None,
            })
            .collect::<Vec<_>>();
        let mut groups = BTreeMap::new();
        groups.insert(cross_sector_group, vec![1.0]);
        let second_taps = *taps_by_sector.get(&0).expect("lower sector taps");
        groups.insert(
            vec![ReducedRecordKey {
                chart_ordinal: 0,
                output_channel: 0,
                taps: encode_taps(second_taps).expect("encode scalar taps"),
                forward_real: 0.75_f64.to_bits(),
                forward_imaginary: 0.125_f64.to_bits(),
                imaging_weight: 0.25_f64.to_bits(),
                aw: None,
            }],
            vec![1.0],
        );
        let (encoded, _) = encode_reduced::<false>(groups).expect("encode two groups");

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
        assert_eq!(size_of::<GriddedNormalSectorRoute>(), 8);
        assert!(
            route_capacity_bytes
                <= gridded_normal_route_capacity_bytes(record_count, 1, 1).unwrap()
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
        let mut prepared = PreparedGriddedNormalWindow::with_record_capacities(&[7])
            .expect("planned route capacity");
        prepared.prediction_groups = 3;
        assert_eq!(
            prepared.routing_measurements(2, 7, 5, 160),
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
        let (encoded, _) =
            encode_reduced::<false>(scalar_groups([(taps, 1.0)])).expect("encode record");
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
    fn quadrant_oracle_route_capacity_does_not_grow_during_prepare() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let (encoded, _) =
            encode_reduced::<false>(scalar_groups([(taps, 1.0)])).expect("encode record");

        for frame_count in [1, 3, 64] {
            let mut window =
                PreparedGriddedNormalWindow::with_record_capacities(&vec![1; frame_count])
                    .expect("bounded window route");
            let planned = window.capacity_bytes().unwrap();
            for sequence in 0..frame_count {
                window.blocks[sequence]
                    .prepare(sequence as u64, &encoded, geometry.grid_shape, 1, |_| {
                        Ok(Complex64::new(1.0, 0.0))
                    })
                    .expect("prepare bounded frame");
            }
            assert_eq!(window.capacity_bytes().unwrap(), planned);
        }
    }

    #[test]
    fn prepared_two_window_route_reports_exact_3364_peak_without_growth() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let (one_record, _) =
            encode_reduced::<false>(scalar_groups([(taps, 1.0)])).expect("encode record");
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
    fn image_domain_residency_formula_is_exact_and_worker_independent() {
        let shapes = [[10, 10], [14, 12]];
        let depth = 3;
        let projected = gridded_normal_domain_execution_residency(shapes, depth, SUPPORT)
            .expect("project domain residency");
        let catalogs =
            GriddedNormalDomainTileCatalogs::new(shapes, SUPPORT).expect("domain tile catalogs");
        let accumulators = catalogs.accumulators(depth).expect("tile accumulators");
        let actual_tile_values = accumulators.iter().fold(0, |total, accumulator| {
            let accumulator = accumulator.lock().expect("tile owner");
            total
                + accumulator.grids.iter().map(Array2::len).sum::<usize>()
                + accumulator
                    .compensations
                    .iter()
                    .map(Array2::len)
                    .sum::<usize>()
        });
        assert_eq!(
            projected.tile_accumulator_complex_values(),
            actual_tile_values
        );
        assert_eq!(
            projected.merge_complex_values(),
            shapes
                .iter()
                .map(|shape| shape[0] * shape[1])
                .sum::<usize>()
                * depth
                * 2
        );
        assert_eq!(
            projected.peak_complex_values(),
            projected.tile_accumulator_complex_values() + projected.merge_complex_values()
        );

        let production = gridded_normal_execution_residency([1024, 1024], 1, SUPPORT)
            .expect("project 1024 grid");
        assert!(
            production.tile_accumulator_complex_values() < 3 * production.merge_complex_values(),
            "tile halos plus three hot shards remain bounded below three compensated grids"
        );
    }

    #[test]
    fn corrupt_truncated_and_reserved_records_fail_closed() {
        let gridder = StandardConvolution::new(&geometry());
        let taps = gridder.taps([0.0, 0.0]).expect("central taps");
        let (encoded, _) =
            encode_reduced::<false>(scalar_groups([(taps, 1.0)])).expect("encode record");
        let descriptor = BlockDescriptor {
            source_samples: 1,
            record_count: 1,
            digest: Sha256::digest(&encoded).into(),
        };
        assert!(
            validate_encoded_block(&descriptor, &encoded, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
                .is_ok()
        );
        assert_eq!(
            validate_encoded_block(
                &descriptor,
                &encoded[..15],
                GRIDDED_NORMAL_OPERATOR_RECORD_BYTES
            ),
            Err(SpectralOperatorError::GriddedRecordMismatch)
        );
        let mut corrupt = encoded.to_vec();
        corrupt[0] ^= 1;
        assert_eq!(
            validate_encoded_block(&descriptor, &corrupt, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES),
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
    fn t42_constant_channel_local_and_taylor_bases_are_admitted() {
        assert!(require_supported_basis(&ReconstructionBasis::Constant).is_ok());
        assert!(
            require_supported_basis(&ReconstructionBasis::ChannelLocal { channels: 2 }).is_ok()
        );
        assert!(require_supported_basis(&ReconstructionBasis::Taylor { terms: 2 }).is_ok());
        assert_eq!(
            require_supported_basis(&ReconstructionBasis::Taylor { terms: 1 }),
            Err(SpectralOperatorError::UnsupportedGriddedReplay)
        );
    }
}
