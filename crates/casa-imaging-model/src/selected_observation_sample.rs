// SPDX-License-Identifier: LGPL-3.0-or-later

//! Backend-free values carried by a selected-observation sample.

use std::{fmt, mem::size_of, sync::Arc};

use smallvec::SmallVec;

use crate::{
    compiled_problem::CanonicalEncoder,
    geometry::{
        Epoch, FrequencyFrame, SkyDirection, UvwCoordinateLaw, encode_sky_direction,
        frequency_frame_tag, time_scale_tag,
    },
    observation::{CorrelationType, MeasurementSetIdentity, correlation_type_tag},
};

const SELECTED_OBSERVATION_GENERATION_DOMAIN: &[u8] = b"casa-rs-selected-observation-generation";
const SELECTED_OBSERVATION_GENERATION_VERSION: u32 = 8;
const GENERATION_ROW_RUN_MARKER: u8 = 0xa1;
const GENERATION_ROW_RUN_TERMINAL: u8 = 0xaf;
const GENERATION_CHANNEL_RUN_MARKER: u8 = 0xb1;
const GENERATION_CHANNEL_RUN_TERMINAL: u8 = 0xbf;
const GENERATION_CORRELATION_MARKER: u8 = 0xc1;
const GENERATION_TERMINAL_MARKER: u8 = 0xff;

/// Reported source position and spectral/polarization coordinate of one sample.
///
/// Authoritative traversal validates these public values against the compiled
/// selected-observation commitment; constructing this record performs no such
/// validation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSampleAddress {
    /// Logical MeasurementSet identity from the compiled observation commitment.
    pub measurement_set: MeasurementSetIdentity,
    /// Physical MAIN row number.
    pub physical_row: u64,
    /// MAIN `DATA_DESC_ID`.
    pub data_description_id: i32,
    /// Resolved `SPECTRAL_WINDOW_ID`.
    pub spectral_window_id: u32,
    /// Zero-based native channel index.
    pub channel_index: u32,
    /// Native channel centre frequency in hertz.
    pub frequency_centre_hz: f64,
    /// Lower native channel boundary in hertz.
    pub frequency_lower_hz: f64,
    /// Upper native channel boundary in hertz.
    pub frequency_upper_hz: f64,
    /// Signed native channel width in hertz.
    pub channel_width_hz: f64,
    /// Reference frame of the channel frequencies.
    pub frequency_frame: FrequencyFrame,
    /// Resolved `POLARIZATION_ID`.
    pub polarization_id: u32,
    /// Zero-based correlation array index.
    pub correlation_index: u32,
    /// Physical correlation coordinate.
    pub correlation_type: CorrelationType,
}

/// Visibility value in its MeasurementSet storage representation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SelectedVisibilitySample {
    /// Single-precision real `FLOAT_DATA` value.
    Float32(f32),
    /// Single-precision complex visibility as `[real, imaginary]`.
    Complex32([f32; 2]),
}

/// Whether one sample is a prediction destination.
///
/// This descriptor carries no produced prediction value. Operator execution
/// owns prediction and residual values downstream of selected-observation I/O.
/// It is traversal provenance, not selected-content generation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedPredictionTarget {
    /// No prediction output was requested for this sample.
    NotRequested,
    /// The sample addresses one `MODEL_DATA` destination.
    ModelData,
}

/// Per-antenna pointing directions evaluated for one baseline sample.
///
/// Retaining both directions preserves antenna-dependent boresight offsets. A downstream
/// operator may derive a baseline-effective direction according to its own closed response law.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPointingDirections {
    /// Evaluated direction for MAIN `ANTENNA1`.
    pub antenna1: SkyDirection,
    /// Evaluated direction for MAIN `ANTENNA2`.
    pub antenna2: SkyDirection,
}

/// One visibility-coordinate projection to a declared image phase centre.
///
/// Raw MeasurementSet coordinates remain row facts stored once in
/// [`SelectedSampleCoordinates`]. This value carries only the operator-ready
/// transform and signed geometric path to one compiled semantic centre.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPhaseCentreProjection {
    transformed_uvw_m: [f64; 3],
    phase_shift_m: f64,
}

impl SelectedPhaseCentreProjection {
    /// Construct one finite UVW and phase-path projection.
    #[must_use]
    pub fn new(transformed_uvw_m: [f64; 3], phase_shift_m: f64) -> Option<Self> {
        (transformed_uvw_m.iter().all(|value| value.is_finite()) && phase_shift_m.is_finite())
            .then_some(Self {
                transformed_uvw_m,
                phase_shift_m,
            })
    }

    /// Return operator-ready UVW coordinates in metres.
    #[must_use]
    pub const fn transformed_uvw_m(self) -> [f64; 3] {
        self.transformed_uvw_m
    }

    /// Return the signed geometric phase-shift path in metres.
    #[must_use]
    pub const fn phase_shift_m(self) -> f64 {
        self.phase_shift_m
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SelectedPsfPhaseCentreProjection {
    SharedWithModel,
    Distinct(SelectedPhaseCentreProjection),
}

/// Model and PSF coordinate projections for one canonical image domain.
///
/// The ordinal indexes [`crate::CompiledGeometry::domains`] directly. Domain
/// role strings therefore remain compiler-owned and are not repeated for each
/// selected row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedImageDomainProjection {
    domain_ordinal: u32,
    facet_ordinal: u32,
    model: SelectedPhaseCentreProjection,
    psf: SelectedPsfPhaseCentreProjection,
}

impl SelectedImageDomainProjection {
    /// Construct one domain with independently declared model and PSF projections.
    #[must_use]
    pub const fn new(
        domain_ordinal: u32,
        model: SelectedPhaseCentreProjection,
        psf: SelectedPhaseCentreProjection,
    ) -> Self {
        Self::new_facet(domain_ordinal, 0, model, psf)
    }

    /// Construct one facet chart with independently declared model and PSF projections.
    #[must_use]
    pub const fn new_facet(
        domain_ordinal: u32,
        facet_ordinal: u32,
        model: SelectedPhaseCentreProjection,
        psf: SelectedPhaseCentreProjection,
    ) -> Self {
        Self {
            domain_ordinal,
            facet_ordinal,
            model,
            psf: SelectedPsfPhaseCentreProjection::Distinct(psf),
        }
    }

    /// Construct a domain whose compiled PSF and model centres are identical.
    #[must_use]
    pub const fn with_shared_psf(
        domain_ordinal: u32,
        model: SelectedPhaseCentreProjection,
    ) -> Self {
        Self::facet_with_shared_psf(domain_ordinal, 0, model)
    }

    /// Construct a facet chart whose compiled PSF and model centres are identical.
    #[must_use]
    pub const fn facet_with_shared_psf(
        domain_ordinal: u32,
        facet_ordinal: u32,
        model: SelectedPhaseCentreProjection,
    ) -> Self {
        Self {
            domain_ordinal,
            facet_ordinal,
            model,
            psf: SelectedPsfPhaseCentreProjection::SharedWithModel,
        }
    }

    /// Return the index into canonical compiled image domains.
    #[must_use]
    pub const fn domain_ordinal(self) -> u32 {
        self.domain_ordinal
    }

    /// Return the index into the domain's canonical compiled facet charts.
    #[must_use]
    pub const fn facet_ordinal(self) -> u32 {
        self.facet_ordinal
    }

    /// Return the chart-model phase-centre projection.
    #[must_use]
    pub const fn model(self) -> SelectedPhaseCentreProjection {
        self.model
    }

    /// Return the point-spread-function phase-centre projection.
    #[must_use]
    pub const fn psf(self) -> SelectedPhaseCentreProjection {
        match self.psf {
            SelectedPsfPhaseCentreProjection::SharedWithModel => self.model,
            SelectedPsfPhaseCentreProjection::Distinct(psf) => psf,
        }
    }

    /// Return whether model and PSF centres share one evaluated projection.
    #[must_use]
    pub const fn psf_shares_model(self) -> bool {
        matches!(self.psf, SelectedPsfPhaseCentreProjection::SharedWithModel)
    }
}

/// Canonically ordered, compiled-chart-bounded projections for one selected row.
///
/// Entries are required to be contiguous in domain-major, facet-minor order.
/// The collection is immutable and cheaply shared between a retained source
/// block, selected-run validation, and scientific consumers.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedImageDomainProjections {
    entries: Arc<[SelectedImageDomainProjection]>,
}

impl SelectedImageDomainProjections {
    /// Construct the canonical one-domain collection when model and PSF centres are identical.
    #[must_use]
    pub fn one_domain_with_shared_psf(model: SelectedPhaseCentreProjection) -> Self {
        Self {
            entries: Arc::from([SelectedImageDomainProjection::with_shared_psf(0, model)]),
        }
    }

    /// Construct a non-empty canonical projection sequence.
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = SelectedImageDomainProjection>) -> Option<Self> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries
            .first()
            .is_none_or(|entry| entry.domain_ordinal() != 0 || entry.facet_ordinal() != 0)
            || entries.windows(2).any(|pair| {
                let previous = pair[0];
                let next = pair[1];
                !((next.domain_ordinal() == previous.domain_ordinal()
                    && previous.facet_ordinal().checked_add(1) == Some(next.facet_ordinal()))
                    || (previous.domain_ordinal().checked_add(1) == Some(next.domain_ordinal())
                        && next.facet_ordinal() == 0))
            })
        {
            return None;
        }
        Some(Self {
            entries: Arc::from(entries.into_boxed_slice()),
        })
    }

    /// Return the exact number of compiled-domain projections.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether no domain projection is present.
    ///
    /// Canonically constructed collections are never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate in canonical compiled-domain order (main first, then outliers).
    pub fn iter(&self) -> impl Iterator<Item = SelectedImageDomainProjection> + '_ {
        self.entries.iter().copied()
    }

    /// Return facet zero for one canonical compiled-domain ordinal.
    #[must_use]
    pub fn get(&self, domain_ordinal: u32) -> Option<SelectedImageDomainProjection> {
        self.get_facet(domain_ordinal, 0)
    }

    /// Return one projection by canonical compiled domain and facet ordinals.
    #[must_use]
    pub fn get_facet(
        &self,
        domain_ordinal: u32,
        facet_ordinal: u32,
    ) -> Option<SelectedImageDomainProjection> {
        self.entries
            .binary_search_by_key(&(domain_ordinal, facet_ordinal), |entry| {
                (entry.domain_ordinal(), entry.facet_ordinal())
            })
            .ok()
            .map(|index| self.entries[index])
    }

    /// Return retained heap payload bytes for residency accounting.
    #[doc(hidden)]
    #[must_use]
    pub fn retained_heap_bytes(&self) -> Option<usize> {
        Self::retained_heap_bytes_for_len(self.entries.len())
    }

    /// Return retained heap payload bytes for a compiled domain cardinality.
    #[doc(hidden)]
    #[must_use]
    pub const fn retained_heap_bytes_for_len(domain_count: usize) -> Option<usize> {
        match domain_count.checked_mul(size_of::<SelectedImageDomainProjection>()) {
            Some(payload) => payload.checked_add(2 * size_of::<usize>()),
            None => None,
        }
    }
}

/// Reported evaluated coordinates consumed by weighting and paired operators.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSampleCoordinates {
    /// Raw MeasurementSet `[u, v, w]` coordinate in metres.
    pub raw_uvw_m: [f64; 3],
    /// `[u, v, w]` coordinate used for weighting-density evaluation, in metres.
    pub density_uvw_m: [f64; 3],
    /// `[u, v, w]` coordinate transformed for operator evaluation, in metres.
    pub transformed_uvw_m: [f64; 3],
    /// Signed geometric phase-shift path length in metres.
    pub phase_shift_m: f64,
    /// Compiled UVW convention.
    pub uvw_law: UvwCoordinateLaw,
    /// MAIN `TIME` epoch.
    pub time: Epoch,
    /// MAIN `TIME_CENTROID` epoch.
    pub time_centroid: Epoch,
    /// MAIN `INTERVAL` in seconds.
    pub interval_seconds: f64,
    /// MAIN `EXPOSURE` in seconds.
    pub exposure_seconds: f64,
    /// Nominal parallactic angles for `ANTENNA1` and `ANTENNA2`, in radians.
    ///
    /// FEED receptor-angle offsets remain instrument-response inputs and are
    /// deliberately not folded into this source-derived coordinate.
    pub parallactic_angles_rad: [f64; 2],
    /// Evaluated phase direction.
    pub phase_direction: SkyDirection,
    /// Evaluated delay direction.
    pub delay_direction: SkyDirection,
    /// Evaluated per-antenna pointing directions.
    pub pointing_directions: SelectedPointingDirections,
}

/// CASA aperture class selected from owner-controlled ANTENNA metadata.
///
/// These are scientific response identities rather than physical antenna IDs.
/// Multiple antennas with the same class therefore share one cached response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AntennaResponseClass {
    /// Physical 12 m ALMA dish evaluated with CASA's 10.7 m effective aperture.
    CasaAlma12m,
    /// Physical 7 m ACA dish evaluated with CASA's 6.25 m effective aperture.
    CasaAca7m,
}

/// Ordered response classes for one selected interferometric baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedAntennaResponses {
    /// Response class of MAIN `ANTENNA1`.
    pub antenna1: AntennaResponseClass,
    /// Response class of MAIN `ANTENNA2`.
    pub antenna2: AntennaResponseClass,
    /// Largest response class present in this MeasurementSet response family.
    ///
    /// CASA crops every heterogeneous convolution plane to one family-wide
    /// extent so resampling has the same boundary domain for every pair.
    pub family_envelope: AntennaResponseClass,
}

/// Reported per-sample MeasurementSet provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedSampleMetadata {
    /// MAIN `FIELD_ID`.
    pub field_id: i32,
    /// MAIN `ANTENNA1`.
    pub antenna1: i32,
    /// MAIN `ANTENNA2`.
    pub antenna2: i32,
    /// Owner-derived paired response, present for a direction-dependent model.
    pub antenna_responses: Option<SelectedAntennaResponses>,
    /// MAIN `FEED1`.
    pub feed1: i32,
    /// MAIN `FEED2`.
    pub feed2: i32,
    /// MAIN `SCAN_NUMBER`.
    pub scan_number: i32,
    /// MAIN `STATE_ID`.
    pub state_id: i32,
    /// MAIN `OBSERVATION_ID`.
    pub observation_id: i32,
    /// MAIN `ARRAY_ID`.
    pub array_id: i32,
}

/// Row-shared portion of one selected-observation run.
///
/// This value is a backend-free report, not traversal authority. Keeping it
/// separate lets a storage owner lend one evaluated row to all selected
/// channel/correlation members without rebuilding the large row record for
/// every scalar sample.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationRunRow {
    /// Logical MeasurementSet identity from the compiled observation commitment.
    pub measurement_set: MeasurementSetIdentity,
    /// Physical MAIN row number.
    pub physical_row: u64,
    /// MAIN `DATA_DESC_ID`.
    pub data_description_id: i32,
    /// Resolved `SPECTRAL_WINDOW_ID`.
    pub spectral_window_id: u32,
    /// Resolved `POLARIZATION_ID`.
    pub polarization_id: u32,
    /// Prediction destination declared by the observation transaction.
    pub prediction_target: SelectedPredictionTarget,
    /// MAIN `FLAG_ROW` value.
    pub row_flag: bool,
    /// Evaluated science coordinates shared by the row.
    pub coordinates: SelectedSampleCoordinates,
    /// Canonical model and PSF projections shared by every row member.
    pub domain_projections: SelectedImageDomainProjections,
    /// Per-row MeasurementSet provenance.
    pub metadata: SelectedSampleMetadata,
}

impl SelectedObservationRunRow {
    /// Return canonical compiled-domain projections for this selected row.
    #[must_use]
    pub const fn domain_projections(&self) -> &SelectedImageDomainProjections {
        &self.domain_projections
    }
}

/// Channel-shared portion of one selected-observation run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationRunChannel {
    /// Zero-based native channel index.
    pub channel_index: u32,
    /// Native channel centre frequency in hertz.
    pub frequency_centre_hz: f64,
    /// Lower native channel boundary in hertz.
    pub frequency_lower_hz: f64,
    /// Upper native channel boundary in hertz.
    pub frequency_upper_hz: f64,
    /// Signed native channel width in hertz.
    pub channel_width_hz: f64,
    /// Reference frame of the channel frequencies.
    pub frequency_frame: FrequencyFrame,
}

/// Correlation-local portion of one selected-observation run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationRunCorrelation {
    /// Zero-based correlation array index.
    pub correlation_index: u32,
    /// Physical correlation coordinate.
    pub correlation_type: CorrelationType,
    /// Visibility value in its MeasurementSet storage representation.
    pub visibility: SelectedVisibilitySample,
    /// Selected channel/correlation `FLAG` value.
    pub channel_flag: bool,
    /// CASA complete-parallel-hand Stokes-I flag.
    pub parallel_hand_group_flag: bool,
    /// Selected `WEIGHT` or `WEIGHT_SPECTRUM` value.
    pub input_weight: f32,
}

/// One source-sample contribution to a compiled output spectral channel.
///
/// The factor is the reconstruction-evaluated interpolation or integration
/// coefficient. Cubic coefficients may be signed. This reported value carries no traversal authority
/// and is deliberately outside [`SelectedObservationSample`]'s persisted
/// schema and content identity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralContribution {
    output_channel: u32,
    factor: f64,
    evaluation_frequency_hz: f64,
}

impl SelectedSpectralContribution {
    /// Construct one finite, non-zero contribution at its
    /// owner-evaluated frequency in the compiled output frame.
    #[must_use]
    pub fn new(output_channel: u32, factor: f64, evaluation_frequency_hz: f64) -> Option<Self> {
        (factor.is_finite()
            && factor != 0.0
            && evaluation_frequency_hz.is_finite()
            && evaluation_frequency_hz > 0.0)
            .then_some(Self {
                output_channel,
                factor,
                evaluation_frequency_hz,
            })
    }

    /// Return the zero-based compiled output-channel index.
    #[must_use]
    pub const fn output_channel(self) -> u32 {
        self.output_channel
    }

    /// Return the interpolation or averaging coefficient.
    #[must_use]
    pub const fn factor(self) -> f64 {
        self.factor
    }

    /// Return the source sample frequency evaluated in the compiled output frame.
    #[must_use]
    pub const fn evaluation_frequency_hz(self) -> f64 {
        self.evaluation_frequency_hz
    }
}

/// Compact sparse output-channel stencil compiled for one selected sample.
///
/// Four terms remain inline for nearest, linear, and cubic sampling. A
/// planner-bounded integration stencil may spill only when its declared bound
/// exceeds that inline capacity. Empty coverage is represented explicitly.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedSpectralContributions {
    entries: SmallVec<[SelectedSpectralContribution; 4]>,
}

impl SelectedSpectralContributions {
    /// Construct a compact contribution set with distinct output channels in
    /// reconstruction-reported order.
    #[must_use]
    pub fn new<I, T>(entries: I) -> Option<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<Option<SelectedSpectralContribution>>,
    {
        let entries = entries
            .into_iter()
            .filter_map(Into::into)
            .collect::<SmallVec<[_; 4]>>();
        if entries.iter().enumerate().any(|(index, entry)| {
            entries[..index]
                .iter()
                .any(|prior| prior.output_channel == entry.output_channel)
        }) {
            return None;
        }
        Some(Self { entries })
    }

    /// Return an empty contribution set.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entries: SmallVec::new(),
        }
    }

    /// Iterate through contributions in owner-reported order.
    pub fn iter(&self) -> impl Iterator<Item = SelectedSpectralContribution> + '_ {
        self.entries.iter().copied()
    }

    /// Return the exact number of non-zero terms.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether this stencil has no mapped output support.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One channel interval in a named spectral frame.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralInterval {
    centre_hz: f64,
    first_boundary_hz: f64,
    second_boundary_hz: f64,
}

impl SelectedSpectralInterval {
    /// Construct a finite positive centre and two distinct finite positive boundaries.
    #[must_use]
    pub fn new(centre_hz: f64, first_boundary_hz: f64, second_boundary_hz: f64) -> Option<Self> {
        (centre_hz.is_finite()
            && centre_hz > 0.0
            && first_boundary_hz.is_finite()
            && first_boundary_hz > 0.0
            && second_boundary_hz.is_finite()
            && second_boundary_hz > 0.0
            && first_boundary_hz != second_boundary_hz)
            .then_some(Self {
                centre_hz,
                first_boundary_hz,
                second_boundary_hz,
            })
    }

    /// Return the channel centre.
    #[must_use]
    pub const fn centre_hz(self) -> f64 {
        self.centre_hz
    }

    /// Return boundaries in original channel order, preserving descending axes.
    #[must_use]
    pub const fn boundaries_hz(self) -> [f64; 2] {
        [self.first_boundary_hz, self.second_boundary_hz]
    }

    /// Return the positive channel width.
    #[must_use]
    pub fn width_hz(self) -> f64 {
        (self.second_boundary_hz - self.first_boundary_hz).abs()
    }
}

/// Source-backed frame/interval evaluation for one selected spectral sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralEvaluation {
    native: SelectedSpectralInterval,
    output_frame: SelectedSpectralInterval,
    effective_weight: f64,
    valid: bool,
}

impl SelectedSpectralEvaluation {
    /// Construct a source-backed trace, keeping native and transformed coordinates distinct.
    #[must_use]
    pub fn new(
        native: SelectedSpectralInterval,
        output_frame: SelectedSpectralInterval,
        effective_weight: f64,
        valid: bool,
    ) -> Option<Self> {
        (effective_weight.is_finite() && effective_weight >= 0.0).then_some(Self {
            native,
            output_frame,
            effective_weight,
            valid,
        })
    }

    /// Return the exact native-frame centre and boundaries read from the source.
    #[must_use]
    pub const fn native(self) -> SelectedSpectralInterval {
        self.native
    }

    /// Return the centre and boundaries evaluated in the compiled output frame.
    #[must_use]
    pub const fn output_frame(self) -> SelectedSpectralInterval {
        self.output_frame
    }

    /// Return the source weight after exact flag validity has been applied.
    #[must_use]
    pub const fn effective_weight(self) -> f64 {
        self.effective_weight
    }

    /// Return whether channel, group, row flags and numerical weight admit the sample.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.valid
    }
}

/// One backend-independent selected-observation sample report.
///
/// This is a closed value schema only. Constructing it does not mint content
/// identity, prove traversal coverage, bind retained access or an execution
/// attempt, or authorize downstream weighting or publication.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationSample {
    /// Reported source and sample coordinate.
    pub address: SelectedSampleAddress,
    /// Selected visibility value in its MeasurementSet storage representation.
    pub visibility: SelectedVisibilitySample,
    /// Prediction-destination descriptor, without a produced value.
    pub prediction_target: SelectedPredictionTarget,
    /// Selected channel/correlation `FLAG` value.
    pub channel_flag: bool,
    /// CASA imaging flag for the complete selected parallel-hand group at this row/channel.
    ///
    /// Stokes-I imaging rejects both parallel hands when either selected hand is flagged. This
    /// derived value preserves that operator input without replacing the exact per-cell
    /// [`Self::channel_flag`] report.
    pub parallel_hand_group_flag: bool,
    /// MAIN `FLAG_ROW` value.
    pub row_flag: bool,
    /// Selected `WEIGHT` or `WEIGHT_SPECTRUM` value in MS `Float` storage precision.
    pub input_weight: f32,
    /// Evaluated science coordinates.
    pub coordinates: SelectedSampleCoordinates,
    /// Canonical model and PSF projections for every compiled image domain.
    pub domain_projections: SelectedImageDomainProjections,
    /// Reported per-sample provenance.
    pub metadata: SelectedSampleMetadata,
}

impl SelectedObservationSample {
    /// Closed schema version of the selected-sample value record.
    pub const SCHEMA_VERSION: u32 = 5;

    /// Borrow this scalar record through the same interface used by a
    /// row/channel run.
    #[must_use]
    pub const fn as_view(&self) -> SelectedObservationSampleView<'_> {
        SelectedObservationSampleView::from_scalar(self)
    }
}

/// Raw selected weight values that define one CASA unpolarized imaging-weight group.
///
/// This traversal-only descriptor is not part of [`SelectedObservationSample`]'s
/// value schema or content identity. Its values and grouping are already bound by
/// the ordered selected correlations and their exact [`SelectedObservationSample::input_weight`]
/// values. Reconstruction owns the scientific rule that turns these raw values
/// into one imaging weight.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedInputWeightGroup {
    kind: SelectedInputWeightGroupKind,
    imaging_flag: bool,
    density_owner: bool,
    terminal_member: bool,
    members: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SelectedInputWeightGroupKind {
    Single(f32),
    ParallelHands { first: f32, last: f32 },
}

impl SelectedInputWeightGroup {
    /// Describe one selected correlation with no paired parallel hand.
    #[must_use]
    pub const fn single(input_weight: f32) -> Self {
        Self {
            kind: SelectedInputWeightGroupKind::Single(input_weight),
            imaging_flag: false,
            density_owner: true,
            terminal_member: true,
            members: 1,
        }
    }

    /// Describe the canonical first and last parallel-hand weights.
    #[must_use]
    pub const fn parallel_hands(first: f32, last: f32) -> Self {
        Self {
            kind: SelectedInputWeightGroupKind::ParallelHands { first, last },
            imaging_flag: false,
            density_owner: true,
            terminal_member: false,
            members: 2,
        }
    }

    /// Describe one complete canonical row/channel correlation run.
    #[doc(hidden)]
    #[must_use]
    pub const fn correlation_run(first: f32, last: f32, members: usize) -> Self {
        Self {
            kind: if members == 1 {
                SelectedInputWeightGroupKind::Single(first)
            } else {
                SelectedInputWeightGroupKind::ParallelHands { first, last }
            },
            imaging_flag: false,
            density_owner: true,
            terminal_member: members == 1,
            members,
        }
    }

    /// Apply CASA's row/channel imaging flag shared by every correlation member.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_imaging_flag(mut self, imaging_flag: bool) -> Self {
        self.imaging_flag = imaging_flag;
        self
    }

    /// Return CASA's OR-reduced flag for this complete correlation group.
    #[doc(hidden)]
    #[must_use]
    pub const fn imaging_flag(self) -> bool {
        self.imaging_flag
    }

    /// Mark whether this member canonically owns the group's one density contribution.
    #[must_use]
    pub const fn with_density_owner(mut self, density_owner: bool) -> Self {
        self.density_owner = density_owner;
        self
    }

    /// Mark whether this member closes the canonical correlation group.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_terminal_member(mut self, terminal_member: bool) -> Self {
        self.terminal_member = terminal_member;
        self
    }

    /// Return whether this member owns the group's one density contribution.
    #[must_use]
    pub const fn is_density_owner(self) -> bool {
        self.density_owner
    }

    /// Return whether this member closes the canonical correlation group.
    #[doc(hidden)]
    #[must_use]
    pub const fn is_terminal_member(self) -> bool {
        self.terminal_member
    }

    /// Return the number of canonical correlations in this row/channel run.
    #[doc(hidden)]
    #[must_use]
    pub const fn member_count(self) -> usize {
        self.members
    }

    /// Return the canonical first weight and an optional last parallel-hand weight.
    #[must_use]
    pub const fn endpoints(self) -> (f32, Option<f32>) {
        match self.kind {
            SelectedInputWeightGroupKind::Single(weight) => (weight, None),
            SelectedInputWeightGroupKind::ParallelHands { first, last } => (first, Some(last)),
        }
    }
}

/// Borrowed view of one selected sample, either from a scalar record or from
/// shared row/channel run components.
///
/// The view cannot outlive its source block and carries no traversal authority.
/// It gives validators and scientific owners one interface while allowing the
/// storage owner to keep repeated row and channel fields shared.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationSampleView<'a> {
    storage: SelectedObservationSampleStorage<'a>,
    input_weight_group: SelectedInputWeightGroup,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SelectedObservationSampleStorage<'a> {
    Scalar(&'a SelectedObservationSample),
    Run {
        row: &'a SelectedObservationRunRow,
        channel: &'a SelectedObservationRunChannel,
        correlation: &'a SelectedObservationRunCorrelation,
    },
}

impl<'a> From<&'a SelectedObservationSample> for SelectedObservationSampleView<'a> {
    fn from(sample: &'a SelectedObservationSample) -> Self {
        Self::from_scalar(sample)
    }
}

impl<'a> SelectedObservationSampleView<'a> {
    const fn from_scalar(sample: &'a SelectedObservationSample) -> Self {
        Self {
            storage: SelectedObservationSampleStorage::Scalar(sample),
            input_weight_group: SelectedInputWeightGroup::single(sample.input_weight),
        }
    }

    /// Borrow one member of a row/channel run.
    #[must_use]
    pub const fn from_run(
        row: &'a SelectedObservationRunRow,
        channel: &'a SelectedObservationRunChannel,
        correlation: &'a SelectedObservationRunCorrelation,
    ) -> Self {
        Self {
            storage: SelectedObservationSampleStorage::Run {
                row,
                channel,
                correlation,
            },
            input_weight_group: SelectedInputWeightGroup::single(correlation.input_weight),
        }
    }

    /// Attach the storage owner's raw weight-group descriptor to this borrowed view.
    ///
    /// The descriptor is deliberately absent from [`Self::to_owned`]: every raw
    /// member already participates in selected-observation content identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_input_weight_group(mut self, group: SelectedInputWeightGroup) -> Self {
        self.input_weight_group = group;
        self
    }

    /// Return the raw weight group used by shared reconstruction weighting.
    #[doc(hidden)]
    #[must_use]
    pub const fn input_weight_group(self) -> SelectedInputWeightGroup {
        self.input_weight_group
    }

    /// Return the exact selected-sample address.
    #[must_use]
    pub const fn address(self) -> SelectedSampleAddress {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.address,
            SelectedObservationSampleStorage::Run {
                row,
                channel,
                correlation,
            } => SelectedSampleAddress {
                measurement_set: row.measurement_set,
                physical_row: row.physical_row,
                data_description_id: row.data_description_id,
                spectral_window_id: row.spectral_window_id,
                channel_index: channel.channel_index,
                frequency_centre_hz: channel.frequency_centre_hz,
                frequency_lower_hz: channel.frequency_lower_hz,
                frequency_upper_hz: channel.frequency_upper_hz,
                channel_width_hz: channel.channel_width_hz,
                frequency_frame: channel.frequency_frame,
                polarization_id: row.polarization_id,
                correlation_index: correlation.correlation_index,
                correlation_type: correlation.correlation_type,
            },
        }
    }

    /// Return the selected visibility value.
    #[must_use]
    pub const fn visibility(self) -> SelectedVisibilitySample {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.visibility,
            SelectedObservationSampleStorage::Run { correlation, .. } => correlation.visibility,
        }
    }

    /// Return the declared prediction destination.
    #[must_use]
    pub const fn prediction_target(self) -> SelectedPredictionTarget {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.prediction_target,
            SelectedObservationSampleStorage::Run { row, .. } => row.prediction_target,
        }
    }

    /// Return the selected cell flag.
    #[must_use]
    pub const fn channel_flag(self) -> bool {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.channel_flag,
            SelectedObservationSampleStorage::Run { correlation, .. } => correlation.channel_flag,
        }
    }

    /// Return the complete selected parallel-hand flag.
    #[must_use]
    pub const fn parallel_hand_group_flag(self) -> bool {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.parallel_hand_group_flag,
            SelectedObservationSampleStorage::Run { correlation, .. } => {
                correlation.parallel_hand_group_flag
            }
        }
    }

    /// Return the MAIN row flag.
    #[must_use]
    pub const fn row_flag(self) -> bool {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.row_flag,
            SelectedObservationSampleStorage::Run { row, .. } => row.row_flag,
        }
    }

    /// Return the selected input weight.
    #[must_use]
    pub const fn input_weight(self) -> f32 {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => sample.input_weight,
            SelectedObservationSampleStorage::Run { correlation, .. } => correlation.input_weight,
        }
    }

    /// Return evaluated row coordinates.
    #[must_use]
    pub const fn coordinates(self) -> &'a SelectedSampleCoordinates {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => &sample.coordinates,
            SelectedObservationSampleStorage::Run { row, .. } => &row.coordinates,
        }
    }

    /// Return canonical per-domain projections.
    #[must_use]
    pub const fn domain_projections(self) -> &'a SelectedImageDomainProjections {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => &sample.domain_projections,
            SelectedObservationSampleStorage::Run { row, .. } => &row.domain_projections,
        }
    }

    /// Return per-row MeasurementSet provenance.
    #[must_use]
    pub const fn metadata(self) -> &'a SelectedSampleMetadata {
        match self.storage {
            SelectedObservationSampleStorage::Scalar(sample) => &sample.metadata,
            SelectedObservationSampleStorage::Run { row, .. } => &row.metadata,
        }
    }

    /// Materialize the closed scalar record only for an owner that must retain
    /// every field independently of the source block.
    #[must_use]
    pub fn to_owned(self) -> SelectedObservationSample {
        SelectedObservationSample {
            address: self.address(),
            visibility: self.visibility(),
            prediction_target: self.prediction_target(),
            channel_flag: self.channel_flag(),
            parallel_hand_group_flag: self.parallel_hand_group_flag(),
            row_flag: self.row_flag(),
            input_weight: self.input_weight(),
            coordinates: *self.coordinates(),
            domain_projections: self.domain_projections().clone(),
            metadata: *self.metadata(),
        }
    }
}

/// Content digest of one ordered selected-observation sample sequence.
///
/// This ID is minted while the selected-observation owner validates and
/// exhausts the canonical sample stream. It remains distinct from retained
/// source access, traversal-completion evidence, and prediction destination.
/// Prediction and residual consumers of the same selected values therefore
/// bind the same generation.
///
/// Physical block boundaries are absent, but iteration order is identity
/// bearing. The traversal owner must therefore emit one canonical logical
/// sample order for every legal physical schedule.
///
/// There is deliberately no public constructor from raw digest bytes or
/// caller-supplied sample reports.
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationGenerationId;
///
/// let _ = SelectedObservationGenerationId::from_sha256([0; 32]);
/// ```
///
/// ```compile_fail
/// use casa_imaging_model::{
///     SelectedObservationGenerationId, SelectedObservationSample,
/// };
///
/// let samples = std::iter::empty::<&SelectedObservationSample>();
/// let _ = SelectedObservationGenerationId::from_samples(samples);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedObservationGenerationId([u8; 32]);

impl SelectedObservationGenerationId {
    /// Content identity schema version.
    pub const SCHEMA_VERSION: u32 = SELECTED_OBSERVATION_GENERATION_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

pub(crate) struct SelectedObservationGenerationEncoder {
    encoder: CanonicalEncoder,
    row_run: Option<GenerationRowContent>,
    channel_run: Option<GenerationChannelContent>,
    row_run_count: u64,
    row_run_sample_count: u64,
    channel_run_count: u64,
    channel_run_sample_count: u64,
    sample_count: u64,
}

#[derive(Clone, PartialEq)]
struct GenerationRowContent {
    data_description_id: i32,
    spectral_window_id: u32,
    polarization_id: u32,
    row_flag: bool,
    coordinates: SelectedSampleCoordinates,
    domain_projections: SelectedImageDomainProjections,
    metadata: SelectedSampleMetadata,
}

impl GenerationRowContent {
    fn from_view(sample: SelectedObservationSampleView<'_>) -> Self {
        let address = sample.address();
        Self {
            data_description_id: address.data_description_id,
            spectral_window_id: address.spectral_window_id,
            polarization_id: address.polarization_id,
            row_flag: sample.row_flag(),
            coordinates: *sample.coordinates(),
            domain_projections: sample.domain_projections().clone(),
            metadata: *sample.metadata(),
        }
    }

    fn from_run(row: &SelectedObservationRunRow) -> Self {
        Self {
            data_description_id: row.data_description_id,
            spectral_window_id: row.spectral_window_id,
            polarization_id: row.polarization_id,
            row_flag: row.row_flag,
            coordinates: row.coordinates,
            domain_projections: row.domain_projections.clone(),
            metadata: row.metadata,
        }
    }

    fn matches_run(&self, row: &SelectedObservationRunRow) -> bool {
        self.data_description_id == row.data_description_id
            && self.spectral_window_id == row.spectral_window_id
            && self.polarization_id == row.polarization_id
            && self.row_flag == row.row_flag
            && self.coordinates == row.coordinates
            && self.domain_projections == row.domain_projections
            && self.metadata == row.metadata
    }
}

#[derive(Clone, Copy, PartialEq)]
struct GenerationChannelContent {
    channel_index: u32,
    frequency_centre_hz: f64,
    frequency_lower_hz: f64,
    frequency_upper_hz: f64,
    channel_width_hz: f64,
    frequency_frame: FrequencyFrame,
}

impl GenerationChannelContent {
    fn from_view(sample: SelectedObservationSampleView<'_>) -> Self {
        let address = sample.address();
        Self {
            channel_index: address.channel_index,
            frequency_centre_hz: address.frequency_centre_hz,
            frequency_lower_hz: address.frequency_lower_hz,
            frequency_upper_hz: address.frequency_upper_hz,
            channel_width_hz: address.channel_width_hz,
            frequency_frame: address.frequency_frame,
        }
    }

    fn from_run(channel: &SelectedObservationRunChannel) -> Self {
        Self {
            channel_index: channel.channel_index,
            frequency_centre_hz: channel.frequency_centre_hz,
            frequency_lower_hz: channel.frequency_lower_hz,
            frequency_upper_hz: channel.frequency_upper_hz,
            channel_width_hz: channel.channel_width_hz,
            frequency_frame: channel.frequency_frame,
        }
    }
}

#[derive(Clone, Copy)]
struct GenerationCorrelationContent {
    correlation_index: u32,
    correlation_type: CorrelationType,
    visibility: SelectedVisibilitySample,
    channel_flag: bool,
    parallel_hand_group_flag: bool,
    input_weight: f32,
}

impl GenerationCorrelationContent {
    fn from_view(sample: SelectedObservationSampleView<'_>) -> Self {
        let address = sample.address();
        Self {
            correlation_index: address.correlation_index,
            correlation_type: address.correlation_type,
            visibility: sample.visibility(),
            channel_flag: sample.channel_flag(),
            parallel_hand_group_flag: sample.parallel_hand_group_flag(),
            input_weight: sample.input_weight(),
        }
    }

    const fn from_run(correlation: &SelectedObservationRunCorrelation) -> Self {
        Self {
            correlation_index: correlation.correlation_index,
            correlation_type: correlation.correlation_type,
            visibility: correlation.visibility,
            channel_flag: correlation.channel_flag,
            parallel_hand_group_flag: correlation.parallel_hand_group_flag,
            input_weight: correlation.input_weight,
        }
    }
}

impl SelectedObservationGenerationEncoder {
    pub(crate) fn new() -> Self {
        let mut encoder = CanonicalEncoder::new();
        encoder.bytes(SELECTED_OBSERVATION_GENERATION_DOMAIN);
        encoder.u32(SELECTED_OBSERVATION_GENERATION_VERSION);
        Self {
            encoder,
            row_run: None,
            channel_run: None,
            row_run_count: 0,
            row_run_sample_count: 0,
            channel_run_count: 0,
            channel_run_sample_count: 0,
            sample_count: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn push(&mut self, sample: &SelectedObservationSample) {
        self.push_view(sample.as_view());
    }

    pub(crate) fn push_view(&mut self, sample: SelectedObservationSampleView<'_>) {
        let row = GenerationRowContent::from_view(sample);
        if self.row_run.as_ref() != Some(&row) {
            self.finish_row_run();
            self.encoder.u8(GENERATION_ROW_RUN_MARKER);
            encode_generation_row_content(&mut self.encoder, &row);
            self.row_run = Some(row);
            self.row_run_count = self
                .row_run_count
                .checked_add(1)
                .expect("selected-observation row-run count fits u64");
        }
        let channel = GenerationChannelContent::from_view(sample);
        if self.channel_run != Some(channel) {
            self.finish_channel_run();
            self.encoder.u8(GENERATION_CHANNEL_RUN_MARKER);
            encode_generation_channel_content(&mut self.encoder, &channel);
            self.channel_run = Some(channel);
            self.channel_run_count = self
                .channel_run_count
                .checked_add(1)
                .expect("selected-observation channel-run count fits u64");
        }
        self.encoder.u8(GENERATION_CORRELATION_MARKER);
        encode_generation_correlation_content(
            &mut self.encoder,
            GenerationCorrelationContent::from_view(sample),
        );
        self.channel_run_sample_count = self
            .channel_run_sample_count
            .checked_add(1)
            .expect("selected-observation channel-run sample count fits u64");
        self.row_run_sample_count = self
            .row_run_sample_count
            .checked_add(1)
            .expect("selected-observation row-run sample count fits u64");
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .expect("selected-observation sample count fits u64");
    }

    pub(crate) fn push_run(
        &mut self,
        row: &SelectedObservationRunRow,
        channel: &SelectedObservationRunChannel,
        correlations: &[SelectedObservationRunCorrelation],
    ) {
        debug_assert!(!correlations.is_empty());
        if !self
            .row_run
            .as_ref()
            .is_some_and(|content| content.matches_run(row))
        {
            self.finish_row_run();
            let row_content = GenerationRowContent::from_run(row);
            self.encoder.u8(GENERATION_ROW_RUN_MARKER);
            encode_generation_row_content(&mut self.encoder, &row_content);
            self.row_run = Some(row_content);
            self.row_run_count = self
                .row_run_count
                .checked_add(1)
                .expect("selected-observation row-run count fits u64");
        }
        let channel_content = GenerationChannelContent::from_run(channel);
        if self.channel_run != Some(channel_content) {
            self.finish_channel_run();
            self.encoder.u8(GENERATION_CHANNEL_RUN_MARKER);
            encode_generation_channel_content(&mut self.encoder, &channel_content);
            self.channel_run = Some(channel_content);
            self.channel_run_count = self
                .channel_run_count
                .checked_add(1)
                .expect("selected-observation channel-run count fits u64");
        }
        for correlation in correlations {
            self.encoder.u8(GENERATION_CORRELATION_MARKER);
            encode_generation_correlation_content(
                &mut self.encoder,
                GenerationCorrelationContent::from_run(correlation),
            );
        }
        let sample_count =
            u64::try_from(correlations.len()).expect("selected-observation run length fits u64");
        self.channel_run_sample_count = self
            .channel_run_sample_count
            .checked_add(sample_count)
            .expect("selected-observation channel-run sample count fits u64");
        self.row_run_sample_count = self
            .row_run_sample_count
            .checked_add(sample_count)
            .expect("selected-observation row-run sample count fits u64");
        self.sample_count = self
            .sample_count
            .checked_add(sample_count)
            .expect("selected-observation sample count fits u64");
    }

    pub(crate) const fn proof_bytes(&self) -> u64 {
        self.encoder.proof_bytes()
    }

    pub(crate) const fn proof_hash_calls(&self) -> u64 {
        self.encoder.proof_hash_calls()
    }

    pub(crate) fn finish(mut self) -> (SelectedObservationGenerationId, u64) {
        self.finish_row_run();
        self.encoder.u8(GENERATION_TERMINAL_MARKER);
        self.encoder.u64(self.row_run_count);
        self.encoder.u64(self.sample_count);
        (
            SelectedObservationGenerationId(self.encoder.finish()),
            self.sample_count,
        )
    }

    fn finish_channel_run(&mut self) {
        if self.channel_run.take().is_some() {
            self.encoder.u8(GENERATION_CHANNEL_RUN_TERMINAL);
            self.encoder.u64(self.channel_run_sample_count);
            self.channel_run_sample_count = 0;
        }
    }

    fn finish_row_run(&mut self) {
        if self.row_run.take().is_some() {
            self.finish_channel_run();
            self.encoder.u8(GENERATION_ROW_RUN_TERMINAL);
            self.encoder.u64(self.channel_run_count);
            self.encoder.u64(self.row_run_sample_count);
            self.channel_run_count = 0;
            self.row_run_sample_count = 0;
        }
    }
}

impl fmt::Debug for SelectedObservationGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SelectedObservationGenerationId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for SelectedObservationGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

fn encode_generation_row_content(encoder: &mut CanonicalEncoder, content: &GenerationRowContent) {
    encoder.i32(content.data_description_id);
    encoder.u32(content.spectral_window_id);
    encoder.u32(content.polarization_id);
    encoder.u8(u8::from(content.row_flag));

    let coordinates = &content.coordinates;
    for value in coordinates.raw_uvw_m {
        encoder.f64(value);
    }
    for value in coordinates.density_uvw_m {
        encoder.f64(value);
    }
    for value in coordinates.transformed_uvw_m {
        encoder.f64(value);
    }
    encoder.f64(coordinates.phase_shift_m);
    encoder.usize(content.domain_projections.len());
    for projection in content.domain_projections.iter() {
        encoder.u32(projection.domain_ordinal());
        encode_phase_centre_projection(encoder, projection.model());
        encoder.u8(u8::from(projection.psf_shares_model()));
        encode_phase_centre_projection(encoder, projection.psf());
    }
    encoder.u8(match coordinates.uvw_law {
        UvwCoordinateLaw::PhaseTrackingCentre => 0,
        UvwCoordinateLaw::MosaicPhaseTrackingCentre => 1,
    });
    encode_epoch(encoder, coordinates.time);
    encode_epoch(encoder, coordinates.time_centroid);
    encoder.f64(coordinates.interval_seconds);
    encoder.f64(coordinates.exposure_seconds);
    encoder.f64(coordinates.parallactic_angles_rad[0]);
    encoder.f64(coordinates.parallactic_angles_rad[1]);
    encode_sky_direction(encoder, coordinates.phase_direction);
    encode_sky_direction(encoder, coordinates.delay_direction);
    encode_sky_direction(encoder, coordinates.pointing_directions.antenna1);
    encode_sky_direction(encoder, coordinates.pointing_directions.antenna2);
    encoder.i32(content.metadata.field_id);
    encoder.i32(content.metadata.antenna1);
    encoder.i32(content.metadata.antenna2);
    match content.metadata.antenna_responses {
        None => encoder.u8(0),
        Some(responses) => {
            encoder.u8(1);
            encode_antenna_response_class(encoder, responses.antenna1);
            encode_antenna_response_class(encoder, responses.antenna2);
            encode_antenna_response_class(encoder, responses.family_envelope);
        }
    }
    encoder.i32(content.metadata.feed1);
    encoder.i32(content.metadata.feed2);
    encoder.i32(content.metadata.scan_number);
    encoder.i32(content.metadata.state_id);
    encoder.i32(content.metadata.observation_id);
    encoder.i32(content.metadata.array_id);
}

fn encode_antenna_response_class(encoder: &mut CanonicalEncoder, class: AntennaResponseClass) {
    encoder.u8(match class {
        AntennaResponseClass::CasaAlma12m => 0,
        AntennaResponseClass::CasaAca7m => 1,
    });
}

fn encode_phase_centre_projection(
    encoder: &mut CanonicalEncoder,
    projection: SelectedPhaseCentreProjection,
) {
    for value in projection.transformed_uvw_m() {
        encoder.f64(value);
    }
    encoder.f64(projection.phase_shift_m());
}

fn encode_generation_channel_content(
    encoder: &mut CanonicalEncoder,
    content: &GenerationChannelContent,
) {
    encoder.u32(content.channel_index);
    encoder.f64(content.frequency_centre_hz);
    encoder.f64(content.frequency_lower_hz);
    encoder.f64(content.frequency_upper_hz);
    encoder.f64(content.channel_width_hz);
    encoder.u8(frequency_frame_tag(content.frequency_frame));
}

fn encode_generation_correlation_content(
    encoder: &mut CanonicalEncoder,
    content: GenerationCorrelationContent,
) {
    encoder.u32(content.correlation_index);
    encoder.u8(correlation_type_tag(content.correlation_type));
    match content.visibility {
        SelectedVisibilitySample::Float32(value) => {
            encoder.u8(0);
            encoder.f32(value);
        }
        SelectedVisibilitySample::Complex32([real, imaginary]) => {
            encoder.u8(1);
            encoder.f32(real);
            encoder.f32(imaginary);
        }
    }
    encoder.u8(u8::from(content.channel_flag));
    encoder.u8(u8::from(content.parallel_hand_group_flag));
    encoder.f32(content.input_weight);
}

fn encode_epoch(encoder: &mut CanonicalEncoder, epoch: Epoch) {
    encoder.f64(epoch.mjd_days());
    encoder.u8(time_scale_tag(epoch.scale()));
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compiled_problem::LogicalIdentity,
        geometry::{DirectionFrame, TimeScale},
    };

    type SampleMutation = (&'static str, fn(&mut SelectedObservationSample));

    const GENERATION_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/imaging-architecture/baselines/selected-observation-generation-v8.txt"
    ));

    const ENCODED_FIELDS: [&str; 51] = [
        "row.data_description_id:i32",
        "row.spectral_window_id:u32",
        "row.polarization_id:u32",
        "row.row_flag:u8",
        "row.raw_uvw_m:f64-x-y-z",
        "row.density_uvw_m:f64-x-y-z",
        "row.transformed_uvw_m:f64-x-y-z",
        "row.phase_shift_m:f64",
        "row.domain_projection_count:usize",
        "row.domain_projection.ordinal:u32",
        "row.domain_projection.model_uvw_m:f64-x-y-z",
        "row.domain_projection.model_phase_shift_m:f64",
        "row.domain_projection.psf_shares_model:u8",
        "row.domain_projection.psf_uvw_m:f64-x-y-z",
        "row.domain_projection.psf_phase_shift_m:f64",
        "row.uvw_law:tag-u8",
        "row.time:mjd-days-f64-then-scale-tag-u8",
        "row.time_centroid:mjd-days-f64-then-scale-tag-u8",
        "row.interval_seconds:f64",
        "row.exposure_seconds:f64",
        "row.parallactic_angle_antenna1_rad:f64",
        "row.parallactic_angle_antenna2_rad:f64",
        "row.phase_direction:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "row.delay_direction:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "row.pointing_antenna1:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "row.pointing_antenna2:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "row.field_id:i32",
        "row.antenna1:i32",
        "row.antenna2:i32",
        "row.antenna_responses:option-tag-u8",
        "row.antenna_response_antenna1:tag-u8-if-present",
        "row.antenna_response_antenna2:tag-u8-if-present",
        "row.antenna_response_family_envelope:tag-u8-if-present",
        "row.feed1:i32",
        "row.feed2:i32",
        "row.scan_number:i32",
        "row.state_id:i32",
        "row.observation_id:i32",
        "row.array_id:i32",
        "channel.channel_index:u32",
        "channel.frequency_centre_hz:f64",
        "channel.frequency_lower_hz:f64",
        "channel.frequency_upper_hz:f64",
        "channel.channel_width_hz:f64",
        "channel.frequency_frame:tag-u8",
        "correlation.correlation_index:u32",
        "correlation.correlation_type:tag-u8",
        "correlation.visibility:tag-u8-then-float32-f32-or-complex32-real-f32-imaginary-f32",
        "correlation.channel_flag:u8",
        "correlation.parallel_hand_group_flag:u8",
        "correlation.input_weight:f32",
    ];

    #[test]
    fn image_domain_projections_require_canonical_ordinals_and_share_equal_psf_values() {
        let model = SelectedPhaseCentreProjection::new([12.0, -4.0, 2.0], 0.125)
            .expect("finite model projection");
        let psf = SelectedPhaseCentreProjection::new([11.0, -3.0, 1.0], -0.25)
            .expect("finite PSF projection");
        let main = SelectedImageDomainProjection::with_shared_psf(0, model);
        let main_second = SelectedImageDomainProjection::facet_with_shared_psf(0, 1, psf);
        let outlier = SelectedImageDomainProjection::new(1, model, psf);
        let projections = SelectedImageDomainProjections::new([main, main_second, outlier])
            .expect("canonical main-first projections");

        assert_eq!(
            projections.iter().collect::<Vec<_>>(),
            vec![main, main_second, outlier]
        );
        assert_eq!(projections.get(0), Some(main));
        assert_eq!(projections.get(1), Some(outlier));
        assert_eq!(projections.get_facet(0, 1), Some(main_second));
        assert!(main.psf_shares_model());
        assert_eq!(main.psf(), main.model());
        assert!(!outlier.psf_shares_model());
        assert_eq!(outlier.psf(), psf);
        assert!(SelectedImageDomainProjections::new([outlier, main]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier, main_second]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier]).is_some());
        assert!(SelectedImageDomainProjections::new(std::iter::empty()).is_none());
        assert!(SelectedPhaseCentreProjection::new([f64::NAN, 0.0, 0.0], 0.0).is_none());
    }

    #[test]
    fn spectral_contributions_are_bounded_values_outside_the_selected_sample_schema() {
        let first = SelectedSpectralContribution::new(2, 0.25, 1.4e9).expect("finite coefficient");
        let second = SelectedSpectralContribution::new(3, 0.75, 1.4e9).expect("finite coefficient");
        let contributions = SelectedSpectralContributions::new([Some(first), Some(second)])
            .expect("two distinct output contributions");

        assert_eq!(first.output_channel(), 2);
        assert_eq!(first.factor(), 0.25);
        assert_eq!(first.evaluation_frequency_hz(), 1.4e9);
        assert_eq!(
            contributions.iter().collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(SelectedSpectralContributions::empty().iter().count(), 0);
        assert!(SelectedSpectralContribution::new(0, f64::NAN, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, -0.5, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, 1.5, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, 1.0, f64::NAN).is_none());
        assert!(SelectedSpectralContributions::new([None, Some(first)]).is_none());
        assert!(SelectedSpectralContributions::new([Some(first), Some(first)]).is_none());
        assert_eq!(SelectedObservationSample::SCHEMA_VERSION, 5);

        let samples = generation_fixture_samples();
        assert_eq!(
            generation(&[&samples]).to_string(),
            fixture_value("generation_sha256"),
            "reported spectral contributions must not alter selected-sample content identity"
        );
    }

    #[test]
    fn content_generation_is_block_independent_exhaustive_and_provenance_free() {
        let samples = generation_fixture_samples();
        let [first, second] = &samples;

        let one_block = generation(&[&samples]);
        let split = generation(&[&samples[..1], &samples[1..]]);
        assert_eq!(one_block, split, "block boundaries are physical choices");
        assert_ne!(
            one_block,
            generation(&[&[second.clone(), first.clone()]]),
            "logical sample order participates in content identity"
        );
        assert_eq!(SelectedObservationGenerationId::SCHEMA_VERSION, 8);

        let mutations: &[SampleMutation] = &[
            ("data description", |s| s.address.data_description_id += 1),
            ("spectral window", |s| s.address.spectral_window_id += 1),
            ("channel", |s| s.address.channel_index += 1),
            ("frequency centre", |s| s.address.frequency_centre_hz += 1.0),
            ("frequency lower", |s| s.address.frequency_lower_hz += 1.0),
            ("frequency upper", |s| s.address.frequency_upper_hz += 1.0),
            ("channel width", |s| s.address.channel_width_hz += 1.0),
            ("frequency frame", |s| {
                s.address.frequency_frame = FrequencyFrame::Barycentric
            }),
            ("polarization", |s| s.address.polarization_id += 1),
            ("correlation index", |s| s.address.correlation_index += 1),
            ("correlation type", |s| {
                s.address.correlation_type = CorrelationType::LinearYx
            }),
            ("visibility representation", |s| {
                s.visibility = SelectedVisibilitySample::Float32(1.25)
            }),
            ("visibility value", |s| {
                s.visibility = SelectedVisibilitySample::Complex32([1.5, -0.5])
            }),
            ("channel flag", |s| s.channel_flag = !s.channel_flag),
            ("parallel-hand group flag", |s| {
                s.parallel_hand_group_flag = !s.parallel_hand_group_flag
            }),
            ("row flag", |s| s.row_flag = !s.row_flag),
            ("input weight", |s| s.input_weight += 1.0),
            ("raw uvw", |s| s.coordinates.raw_uvw_m[0] += 1.0),
            ("density uvw", |s| s.coordinates.density_uvw_m[1] += 1.0),
            ("transformed uvw", |s| {
                s.coordinates.transformed_uvw_m[2] += 1.0
            }),
            ("phase shift", |s| s.coordinates.phase_shift_m += 1.0),
            ("time", |s| {
                s.coordinates.time = Epoch::new(59_001.0, TimeScale::Utc)
            }),
            ("time scale", |s| {
                s.coordinates.time = Epoch::new(59_000.0, TimeScale::Tai)
            }),
            ("time centroid", |s| {
                s.coordinates.time_centroid = Epoch::new(59_001.0, TimeScale::Utc)
            }),
            ("interval", |s| s.coordinates.interval_seconds += 1.0),
            ("exposure", |s| s.coordinates.exposure_seconds += 1.0),
            ("antenna1 parallactic angle", |s| {
                s.coordinates.parallactic_angles_rad[0] += 1.0
            }),
            ("antenna2 parallactic angle", |s| {
                s.coordinates.parallactic_angles_rad[1] += 1.0
            }),
            ("phase direction", |s| {
                s.coordinates.phase_direction = SkyDirection::new(DirectionFrame::J2000, 1.1, -0.5)
            }),
            ("direction frame", |s| {
                s.coordinates.phase_direction = SkyDirection::new(DirectionFrame::Icrs, 1.0, -0.5)
            }),
            ("delay direction", |s| {
                s.coordinates.delay_direction =
                    SkyDirection::new(DirectionFrame::J2000, 1.1, -0.500_5)
            }),
            ("antenna1 pointing direction", |s| {
                s.coordinates.pointing_directions.antenna1 =
                    SkyDirection::new(DirectionFrame::J2000, 1.1, -0.499)
            }),
            ("antenna2 pointing direction", |s| {
                s.coordinates.pointing_directions.antenna2 =
                    SkyDirection::new(DirectionFrame::J2000, 1.2, -0.499)
            }),
            ("field", |s| s.metadata.field_id += 1),
            ("antenna1", |s| s.metadata.antenna1 += 1),
            ("antenna2", |s| s.metadata.antenna2 += 1),
            ("antenna responses", |s| s.metadata.antenna_responses = None),
            ("feed1", |s| s.metadata.feed1 += 1),
            ("feed2", |s| s.metadata.feed2 += 1),
            ("scan", |s| s.metadata.scan_number += 1),
            ("state", |s| s.metadata.state_id += 1),
            ("observation", |s| s.metadata.observation_id += 1),
            ("array", |s| s.metadata.array_id += 1),
        ];
        let baseline = generation(&[std::slice::from_ref(first)]);
        for (name, mutate) in mutations {
            let mut changed = first.clone();
            mutate(&mut changed);
            assert_ne!(
                baseline,
                generation(&[&[changed]]),
                "{name} must participate in content identity"
            );
        }

        let mut relocated = first.clone();
        relocated.address.measurement_set =
            MeasurementSetIdentity::new(LogicalIdentity::from_sha256([9; 32]));
        relocated.address.physical_row = 999;
        assert_eq!(
            baseline,
            generation(&[&[relocated]]),
            "external source identity and physical row are provenance only"
        );
        let mut relocated_duplicate = first.clone();
        relocated_duplicate.address.measurement_set =
            MeasurementSetIdentity::new(LogicalIdentity::from_sha256([9; 32]));
        relocated_duplicate.address.physical_row = 999;
        assert_eq!(
            generation(&[&[first.clone(), first.clone()]]),
            generation(&[&[first.clone(), relocated_duplicate]]),
            "provenance cannot alter hierarchical run boundaries"
        );
        let mut residual_input = first.clone();
        residual_input.prediction_target = SelectedPredictionTarget::NotRequested;
        assert_eq!(
            baseline,
            generation(&[&[residual_input]]),
            "prediction and residual consumers bind the same selected-content generation"
        );
        assert_ne!(
            baseline,
            generation(&[&[first.clone(), first.clone()]]),
            "sample multiplicity participates in content identity"
        );

        let mut negative_zero = first.clone();
        negative_zero.coordinates.phase_shift_m = -0.0;
        let mut positive_zero = first.clone();
        positive_zero.coordinates.phase_shift_m = 0.0;
        assert_eq!(
            generation(&[&[negative_zero]]),
            generation(&[&[positive_zero]]),
            "IEEE signed zero is canonicalized"
        );
        let mut negative_weight_zero = first.clone();
        negative_weight_zero.input_weight = -0.0;
        let mut positive_weight_zero = first.clone();
        positive_weight_zero.input_weight = 0.0;
        assert_eq!(
            generation(&[&[negative_weight_zero]]),
            generation(&[&[positive_weight_zero]]),
            "f32 weight signed zero is canonicalized"
        );
        let mut negative_visibility_zero = first.clone();
        negative_visibility_zero.visibility = SelectedVisibilitySample::Float32(-0.0);
        let mut positive_visibility_zero = first.clone();
        positive_visibility_zero.visibility = SelectedVisibilitySample::Float32(0.0);
        assert_eq!(
            generation(&[&[negative_visibility_zero]]),
            generation(&[&[positive_visibility_zero]]),
            "f32 visibility signed zero is canonicalized"
        );

        assert_eq!(
            one_block.as_bytes(),
            [
                77, 197, 204, 165, 208, 30, 160, 65, 207, 107, 26, 113, 112, 39, 221, 124, 87, 241,
                59, 16, 59, 57, 137, 41, 5, 209, 82, 40, 81, 243, 230, 9,
            ],
            "schema-8 golden ratchet"
        );
    }

    #[test]
    fn run_generation_matches_the_scalar_encoding() {
        let first = sample();
        let mut second = first.clone();
        second.address.correlation_index = 2;
        second.address.correlation_type = CorrelationType::LinearYx;
        second.visibility = SelectedVisibilitySample::Complex32([-0.75, 0.25]);
        second.channel_flag = false;
        second.parallel_hand_group_flag = false;
        second.input_weight = 2.5;

        let row = SelectedObservationRunRow {
            measurement_set: first.address.measurement_set,
            physical_row: first.address.physical_row,
            data_description_id: first.address.data_description_id,
            spectral_window_id: first.address.spectral_window_id,
            polarization_id: first.address.polarization_id,
            prediction_target: first.prediction_target,
            row_flag: first.row_flag,
            coordinates: first.coordinates,
            domain_projections: first.domain_projections.clone(),
            metadata: first.metadata,
        };
        let channel = SelectedObservationRunChannel {
            channel_index: first.address.channel_index,
            frequency_centre_hz: first.address.frequency_centre_hz,
            frequency_lower_hz: first.address.frequency_lower_hz,
            frequency_upper_hz: first.address.frequency_upper_hz,
            channel_width_hz: first.address.channel_width_hz,
            frequency_frame: first.address.frequency_frame,
        };
        let correlations = [
            SelectedObservationRunCorrelation {
                correlation_index: first.address.correlation_index,
                correlation_type: first.address.correlation_type,
                visibility: first.visibility,
                channel_flag: first.channel_flag,
                parallel_hand_group_flag: first.parallel_hand_group_flag,
                input_weight: first.input_weight,
            },
            SelectedObservationRunCorrelation {
                correlation_index: second.address.correlation_index,
                correlation_type: second.address.correlation_type,
                visibility: second.visibility,
                channel_flag: second.channel_flag,
                parallel_hand_group_flag: second.parallel_hand_group_flag,
                input_weight: second.input_weight,
            },
        ];

        let mut run_encoder = SelectedObservationGenerationEncoder::new();
        run_encoder.push_run(&row, &channel, &correlations);
        let (run_generation, run_count) = run_encoder.finish();

        assert_eq!(
            run_generation,
            generation(&[&[first.clone(), second.clone()]])
        );
        assert_eq!(run_count, 2);

        let mut changed_row = row.clone();
        let changed = SelectedPhaseCentreProjection::new([12.5, -4.25, 3.25], 0.125)
            .expect("finite changed projection");
        changed_row.domain_projections =
            SelectedImageDomainProjections::new([SelectedImageDomainProjection::with_shared_psf(
                0, changed,
            )])
            .expect("canonical changed projection");
        let mut changed_encoder = SelectedObservationGenerationEncoder::new();
        changed_encoder.push_run(&changed_row, &channel, &correlations);
        assert_ne!(run_generation, changed_encoder.finish().0);
    }

    #[test]
    fn generation_fixture_pins_schema_encoder_order_and_digest() {
        assert_eq!(
            fixture_value("identity_domain"),
            "casa-rs-selected-observation-generation"
        );
        assert_eq!(fixture_value("generation_schema_version"), "8");
        assert_eq!(fixture_value("row_run_marker"), "0xa1");
        assert_eq!(fixture_value("row_run_terminal"), "0xaf");
        assert_eq!(fixture_value("channel_run_marker"), "0xb1");
        assert_eq!(fixture_value("channel_run_terminal"), "0xbf");
        assert_eq!(fixture_value("correlation_marker"), "0xc1");
        assert_eq!(fixture_value("terminal_marker"), "0xff");
        assert_eq!(
            fixture_value("terminal_counts"),
            "row-run-count-u64-le,sample-count-u64-le"
        );
        assert_eq!(
            fixture_value("excluded_provenance"),
            "measurement_set,physical_row,prediction_target"
        );

        let fields = GENERATION_FIXTURE
            .lines()
            .filter_map(|line| line.split_once('='))
            .filter_map(|(key, value)| key.starts_with("field.").then_some(value))
            .collect::<Vec<_>>();
        assert_eq!(fields, ENCODED_FIELDS);

        let samples = generation_fixture_samples();
        assert_eq!(
            fixture_value("fixture_sample_count"),
            samples.len().to_string()
        );
        assert_eq!(
            generation(&[&samples]).to_string(),
            fixture_value("generation_sha256")
        );
    }

    #[test]
    fn generation_proof_work_counts_exact_hash_updates() {
        let samples = generation_fixture_samples();
        let mut encoder = SelectedObservationGenerationEncoder::new();
        for sample in &samples {
            encoder.push(sample);
        }

        assert_eq!(
            (encoder.proof_bytes(), encoder.proof_hash_calls()),
            (524, 97),
        );
    }

    #[test]
    fn antenna_response_identity_encodes_absence_and_every_selected_class() {
        let mut absent = sample();
        absent.metadata.antenna_responses = None;
        let absent_generation = generation(&[&[absent.clone()]]);

        let responses = SelectedAntennaResponses {
            antenna1: AntennaResponseClass::CasaAlma12m,
            antenna2: AntennaResponseClass::CasaAca7m,
            family_envelope: AntennaResponseClass::CasaAlma12m,
        };
        let mut selected = absent;
        selected.metadata.antenna_responses = Some(responses);
        let selected_generation = generation(&[&[selected.clone()]]);
        assert_ne!(absent_generation, selected_generation);

        for changed in [
            SelectedAntennaResponses {
                antenna1: AntennaResponseClass::CasaAca7m,
                ..responses
            },
            SelectedAntennaResponses {
                antenna2: AntennaResponseClass::CasaAlma12m,
                ..responses
            },
            SelectedAntennaResponses {
                family_envelope: AntennaResponseClass::CasaAca7m,
                ..responses
            },
        ] {
            let mut sample = selected.clone();
            sample.metadata.antenna_responses = Some(changed);
            assert_ne!(selected_generation, generation(&[&[sample]]));
        }
    }

    fn fixture_value(key: &str) -> &str {
        GENERATION_FIXTURE
            .lines()
            .filter_map(|line| line.split_once('='))
            .find_map(|(candidate, value)| (candidate == key).then_some(value))
            .unwrap_or_else(|| panic!("generation fixture lacks {key}"))
    }

    fn generation_fixture_samples() -> [SelectedObservationSample; 2] {
        let first = sample();
        let mut second = first.clone();
        second.address.physical_row = 12;
        second.address.channel_index = 8;
        second.address.frequency_centre_hz += 1_000_000.0;
        [first, second]
    }

    fn generation(blocks: &[&[SelectedObservationSample]]) -> SelectedObservationGenerationId {
        let mut encoder = SelectedObservationGenerationEncoder::new();
        for sample in blocks.iter().flat_map(|block| block.iter()) {
            encoder.push(sample);
        }
        encoder.finish().0
    }

    fn one_domain_projections(
        coordinates: &SelectedSampleCoordinates,
    ) -> SelectedImageDomainProjections {
        let projection = SelectedPhaseCentreProjection::new(
            coordinates.transformed_uvw_m,
            coordinates.phase_shift_m,
        )
        .expect("finite fixture projection");
        SelectedImageDomainProjections::new([SelectedImageDomainProjection::with_shared_psf(
            0, projection,
        )])
        .expect("canonical fixture projections")
    }

    fn sample() -> SelectedObservationSample {
        let coordinates = SelectedSampleCoordinates {
            raw_uvw_m: [12.0, -4.0, 2.0],
            density_uvw_m: [12.5, -4.25, 2.25],
            transformed_uvw_m: [11.75, -3.75, 1.5],
            phase_shift_m: 0.125,
            uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
            time: Epoch::new(59_000.0, TimeScale::Utc),
            time_centroid: Epoch::new(59_000.000_001, TimeScale::Utc),
            interval_seconds: 1.0,
            exposure_seconds: 0.8,
            parallactic_angles_rad: [0.2, 0.25],
            phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5),
            pointing_directions: SelectedPointingDirections {
                antenna1: SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499),
                antenna2: SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498),
            },
        };
        SelectedObservationSample {
            address: SelectedSampleAddress {
                measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256([1; 32])),
                physical_row: 11,
                data_description_id: 2,
                spectral_window_id: 3,
                channel_index: 7,
                frequency_centre_hz: 1_400_000_000.0,
                frequency_lower_hz: 1_399_500_000.0,
                frequency_upper_hz: 1_400_500_000.0,
                channel_width_hz: 1_000_000.0,
                frequency_frame: FrequencyFrame::Topocentric,
                polarization_id: 5,
                correlation_index: 1,
                correlation_type: CorrelationType::LinearXy,
            },
            visibility: SelectedVisibilitySample::Complex32([1.25, -0.5]),
            prediction_target: SelectedPredictionTarget::ModelData,
            channel_flag: true,
            parallel_hand_group_flag: true,
            row_flag: false,
            input_weight: 2.5,
            coordinates,
            domain_projections: one_domain_projections(&coordinates),
            metadata: SelectedSampleMetadata {
                field_id: 14,
                antenna1: 10,
                antenna2: 11,
                antenna_responses: Some(SelectedAntennaResponses {
                    antenna1: AntennaResponseClass::CasaAlma12m,
                    antenna2: AntennaResponseClass::CasaAca7m,
                    family_envelope: AntennaResponseClass::CasaAlma12m,
                }),
                feed1: 12,
                feed2: 13,
                scan_number: 15,
                state_id: -1,
                observation_id: 17,
                array_id: 18,
            },
        }
    }
}
