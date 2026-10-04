// SPDX-License-Identifier: LGPL-3.0-or-later

//! Backend-free values carried by a selected-observation sample.

use std::{mem::size_of, sync::Arc};

use smallvec::SmallVec;

use crate::{
    geometry::{Epoch, FrequencyFrame, SkyDirection, UvwCoordinateLaw},
    observation::{CorrelationType, MeasurementSetIdentity},
};

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
    aw_pointing_pixel: Option<[f64; 2]>,
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
            aw_pointing_pixel: None,
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
            aw_pointing_pixel: None,
        }
    }

    /// Attach the exact CASA chart-local baseline pointing pixel used by AW projection.
    ///
    /// The coordinate is the already averaged two-antenna pointing pixel in
    /// this chart. Raw pointing directions remain available separately for
    /// primary-beam and mosaic evaluation.
    #[must_use]
    pub fn with_aw_pointing_pixel(mut self, pixel: [f64; 2]) -> Option<Self> {
        if pixel.iter().any(|value| !value.is_finite()) {
            return None;
        }
        self.aw_pointing_pixel = Some(pixel);
        Some(self)
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

    /// Return the exact CASA chart-local baseline pointing pixel, when evaluated.
    #[must_use]
    pub const fn aw_pointing_pixel(self) -> Option<[f64; 2]> {
        self.aw_pointing_pixel
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
    /// Evaluated parallactic angles for `ANTENNA1` and `ANTENNA2`, in radians,
    /// or `None` when the compiled operator does not require feed rotation.
    ///
    /// FEED receptor-angle offsets remain instrument-response inputs and are
    /// deliberately not folded into this source-derived coordinate.
    pub parallactic_angles_rad: Option<[f64; 2]>,
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

/// Row-local geometry of an emitted selected native-channel vector.
///
/// The first two selected centres are evaluated in the requested output frame.
/// They are not reconstructed from `CHAN_WIDTH`, and retain their exact values
/// even when selected channels have gaps or descend in frequency. A restricted
/// replay additionally retains the original vector's first pair for the
/// interpolation lattice. This is a non-persistent report: selected-source
/// traversal owns its provenance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedRowSpectralGeometry {
    measurement_set: MeasurementSetIdentity,
    physical_row: u64,
    data_description_id: i32,
    spectral_window_id: u32,
    polarization_id: u32,
    field_id: i32,
    time_mjd_days_bits: u64,
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
    selected_channels: usize,
    first: (u32, f64),
    second: Option<(u32, f64)>,
    lattice_first_pair_hz: Option<[f64; 2]>,
}

impl SelectedRowSpectralGeometry {
    /// Bind a singleton or the first pair of the complete selected vector to
    /// one source row and frame-conversion context.
    ///
    /// Indices remain in canonical selected-channel order, independently of
    /// frequency direction. The caller supplies both converted centres, not a
    /// channel-width approximation. Flags never change this geometry.
    #[must_use]
    pub fn new(
        sample: SelectedObservationSampleView<'_>,
        output_frame: FrequencyFrame,
        selected_channels: usize,
        first: (u32, f64),
        second: Option<(u32, f64)>,
    ) -> Option<Self> {
        if selected_channels == 0
            || !first.1.is_finite()
            || first.1 <= 0.0
            || (selected_channels == 1) != second.is_none()
            || second.is_some_and(|second| {
                second.0 <= first.0
                    || !second.1.is_finite()
                    || second.1 <= 0.0
                    || !(second.1 - first.1).is_finite()
                    || second.1 == first.1
            })
        {
            return None;
        }
        let address = sample.address();
        Some(Self {
            measurement_set: address.measurement_set,
            physical_row: address.physical_row,
            data_description_id: address.data_description_id,
            spectral_window_id: address.spectral_window_id,
            polarization_id: address.polarization_id,
            field_id: sample.metadata().field_id,
            time_mjd_days_bits: sample.coordinates().time.mjd_days().to_bits(),
            source_frame: address.frequency_frame,
            output_frame,
            selected_channels,
            first,
            second,
            lattice_first_pair_hz: second.map(|second| [first.1, second.1]),
        })
    }

    /// Whether this descriptor belongs to the sample's row and conversion context.
    #[must_use]
    pub fn matches_sample(
        self,
        sample: SelectedObservationSampleView<'_>,
        output_frame: FrequencyFrame,
    ) -> bool {
        let address = sample.address();
        self.measurement_set == address.measurement_set
            && self.physical_row == address.physical_row
            && self.data_description_id == address.data_description_id
            && self.spectral_window_id == address.spectral_window_id
            && self.polarization_id == address.polarization_id
            && self.field_id == sample.metadata().field_id
            && self.time_mjd_days_bits == sample.coordinates().time.mjd_days().to_bits()
            && self.source_frame == address.frequency_frame
            && self.output_frame == output_frame
    }

    /// Number of native channels in the emitted vector, including flagged channels.
    #[must_use]
    pub const fn selected_channels(self) -> usize {
        self.selected_channels
    }

    /// Index and exact converted centre of the first selected channel.
    #[must_use]
    pub const fn first(self) -> (u32, f64) {
        self.first
    }

    /// Index and exact converted centre of the second selected channel, absent for a singleton.
    #[must_use]
    pub const fn second(self) -> Option<(u32, f64)> {
        self.second
    }

    /// Exact first-pair centres for row-local interpolation, absent for a singleton.
    #[must_use]
    pub fn first_pair_hz(self) -> Option<[f64; 2]> {
        self.second.map(|second| [self.first.1, second.1])
    }

    /// Preserve the complete selected vector's interpolation lattice while
    /// describing a bounded contiguous window of that vector. The local first
    /// pair and count still describe the samples that must actually arrive.
    #[must_use]
    pub fn with_lattice_first_pair_hz(mut self, pair: [f64; 2]) -> Option<Self> {
        if pair.iter().any(|value| !value.is_finite() || *value <= 0.0)
            || !(pair[1] - pair[0]).is_finite()
            || pair[0] == pair[1]
            || self.second.is_some_and(|second| {
                (second.1 - self.first.1).signum() != (pair[1] - pair[0]).signum()
            })
        {
            return None;
        }
        self.lattice_first_pair_hz = Some(pair);
        Some(self)
    }

    /// Exact first pair of the original selected vector, not the local window.
    #[must_use]
    pub const fn lattice_first_pair_hz(self) -> Option<[f64; 2]> {
        self.lattice_first_pair_hz
    }
}

/// Source-backed frame/interval evaluation for one selected spectral sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralEvaluation {
    native: SelectedSpectralInterval,
    output_frame: SelectedSpectralInterval,
    row_geometry: Option<SelectedRowSpectralGeometry>,
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
            row_geometry: None,
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

    /// Attach the source owner's exact selected-row geometry.
    ///
    /// Row-interpolating consumers require this descriptor; its absence never
    /// authorizes a channel-width or catalogue approximation.
    #[must_use]
    pub const fn with_row_geometry(mut self, geometry: SelectedRowSpectralGeometry) -> Self {
        self.row_geometry = Some(geometry);
        self
    }

    /// Return the source owner's selected-row geometry, when the envelope carries it.
    #[must_use]
    pub const fn row_geometry(self) -> Option<SelectedRowSpectralGeometry> {
        self.row_geometry
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
    pub const SCHEMA_VERSION: u32 = 6;

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
    row_spectral_geometry: Option<SelectedRowSpectralGeometry>,
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
            row_spectral_geometry: None,
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
            row_spectral_geometry: None,
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

    /// Attach the traversal-only spectral descriptor without changing the scalar value schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_row_spectral_geometry(
        mut self,
        geometry: Option<SelectedRowSpectralGeometry>,
    ) -> Self {
        self.row_spectral_geometry = geometry;
        self
    }

    /// Borrow the source-issued row geometry carried through weighted traversal.
    #[doc(hidden)]
    #[must_use]
    pub const fn row_spectral_geometry(self) -> Option<SelectedRowSpectralGeometry> {
        self.row_spectral_geometry
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(main.aw_pointing_pixel(), None);
        let exact = main
            .with_aw_pointing_pixel([256.25, 255.75])
            .expect("finite AW pointing pixel");
        assert_eq!(exact.aw_pointing_pixel(), Some([256.25, 255.75]));
        assert!(main.with_aw_pointing_pixel([f64::NAN, 0.0]).is_none());
        assert!(main.with_aw_pointing_pixel([0.0, f64::INFINITY]).is_none());
        assert!(SelectedImageDomainProjections::new([outlier, main]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier, main_second]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier]).is_some());
        assert!(SelectedImageDomainProjections::new(std::iter::empty()).is_none());
        assert!(SelectedPhaseCentreProjection::new([f64::NAN, 0.0, 0.0], 0.0).is_none());
    }

    #[test]
    fn spectral_contributions_are_finite_nonzero_values_outside_the_selected_sample_schema() {
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
        assert_eq!(
            SelectedSpectralContribution::new(0, -0.5, 1.4e9)
                .expect("cubic coefficients may be signed")
                .factor(),
            -0.5
        );
        assert!(SelectedSpectralContribution::new(0, 1.5, 1.4e9).is_some());
        assert!(SelectedSpectralContribution::new(0, 0.0, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, 1.0, f64::NAN).is_none());
        assert_eq!(
            SelectedSpectralContributions::new([None, Some(first)])
                .expect("absent sparse entries are omitted")
                .iter()
                .collect::<Vec<_>>(),
            vec![first]
        );
        assert!(SelectedSpectralContributions::new([Some(first), Some(first)]).is_none());
        assert_eq!(SelectedObservationSample::SCHEMA_VERSION, 6);
    }
}
