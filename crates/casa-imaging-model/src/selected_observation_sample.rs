// SPDX-License-Identifier: LGPL-3.0-or-later

//! Backend-free values carried by a selected-observation sample.

use std::fmt;

use crate::{
    compiled_problem::CanonicalEncoder,
    geometry::{
        Epoch, FrequencyFrame, SkyDirection, UvwCoordinateLaw, encode_sky_direction,
        frequency_frame_tag, time_scale_tag,
    },
    observation::{CorrelationType, MeasurementSetIdentity, correlation_type_tag},
};

const SELECTED_OBSERVATION_GENERATION_DOMAIN: &[u8] = b"casa-rs-selected-observation-generation";
const SELECTED_OBSERVATION_GENERATION_VERSION: u32 = 4;

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
    /// Evaluated phase direction.
    pub phase_direction: SkyDirection,
    /// Evaluated delay direction.
    pub delay_direction: SkyDirection,
    /// Evaluated per-antenna pointing directions.
    pub pointing_directions: SelectedPointingDirections,
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

/// One source-sample contribution to a compiled output spectral channel.
///
/// The factor is the owner-evaluated interpolation or channel-averaging
/// coefficient in `(0, 1]`. This reported value carries no traversal authority
/// and is deliberately outside [`SelectedObservationSample`]'s persisted
/// schema and content identity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralContribution {
    output_channel: u32,
    factor: f32,
    evaluation_frequency_hz: f64,
}

impl SelectedSpectralContribution {
    /// Construct one finite, positive, normalized contribution at its
    /// owner-evaluated frequency in the compiled output frame.
    #[must_use]
    pub fn new(output_channel: u32, factor: f32, evaluation_frequency_hz: f64) -> Option<Self> {
        (factor.is_finite()
            && factor > 0.0
            && factor <= 1.0
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
    pub const fn factor(self) -> f32 {
        self.factor
    }

    /// Return the source sample frequency evaluated in the compiled output frame.
    #[must_use]
    pub const fn evaluation_frequency_hz(self) -> f64 {
        self.evaluation_frequency_hz
    }
}

/// At most two output-channel contributions reported for one selected sample.
///
/// Entries are compact and address distinct output channels. Nearest and
/// channel-average sampling report at most one entry; linear interpolation may
/// report two. Empty coverage is represented explicitly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSpectralContributions {
    entries: [Option<SelectedSpectralContribution>; 2],
}

impl SelectedSpectralContributions {
    /// Construct a compact contribution set with distinct output channels.
    #[must_use]
    pub fn new(entries: [Option<SelectedSpectralContribution>; 2]) -> Option<Self> {
        if entries[0].is_none() && entries[1].is_some() {
            return None;
        }
        if entries[0].is_some_and(|first| {
            entries[1].is_some_and(|second| first.output_channel == second.output_channel)
        }) {
            return None;
        }
        Some(Self { entries })
    }

    /// Return an empty contribution set.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            entries: [None, None],
        }
    }

    /// Iterate through contributions in owner-reported order.
    pub fn iter(&self) -> impl Iterator<Item = SelectedSpectralContribution> + '_ {
        self.entries.iter().flatten().copied()
    }
}

/// One backend-independent selected-observation sample report.
///
/// This is a closed value schema only. Constructing it does not mint content
/// identity, prove traversal coverage, bind retained access or an execution
/// attempt, or authorize downstream weighting or publication.
#[derive(Debug, Clone, Copy, PartialEq)]
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
    /// Reported per-sample provenance.
    pub metadata: SelectedSampleMetadata,
}

impl SelectedObservationSample {
    /// Closed schema version of the selected-sample value record.
    pub const SCHEMA_VERSION: u32 = 3;
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
    sample_count: u64,
}

impl SelectedObservationGenerationEncoder {
    pub(crate) fn new() -> Self {
        let mut encoder = CanonicalEncoder::new();
        encoder.bytes(SELECTED_OBSERVATION_GENERATION_DOMAIN);
        encoder.u32(SELECTED_OBSERVATION_GENERATION_VERSION);
        Self {
            encoder,
            sample_count: 0,
        }
    }

    pub(crate) fn push(&mut self, sample: &SelectedObservationSample) {
        self.encoder.u8(0xa5);
        encode_sample(&mut self.encoder, sample);
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .expect("selected-observation sample count fits u64");
    }

    pub(crate) fn finish(mut self) -> (SelectedObservationGenerationId, u64) {
        self.encoder.u8(0xff);
        self.encoder.u64(self.sample_count);
        (
            SelectedObservationGenerationId(self.encoder.finish()),
            self.sample_count,
        )
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

fn encode_sample(encoder: &mut CanonicalEncoder, sample: &SelectedObservationSample) {
    let address = sample.address;
    encoder.i32(address.data_description_id);
    encoder.u32(address.spectral_window_id);
    encoder.u32(address.channel_index);
    encoder.f64(address.frequency_centre_hz);
    encoder.f64(address.frequency_lower_hz);
    encoder.f64(address.frequency_upper_hz);
    encoder.f64(address.channel_width_hz);
    encoder.u8(frequency_frame_tag(address.frequency_frame));
    encoder.u32(address.polarization_id);
    encoder.u32(address.correlation_index);
    encoder.u8(correlation_type_tag(address.correlation_type));

    match sample.visibility {
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
    encoder.u8(u8::from(sample.channel_flag));
    encoder.u8(u8::from(sample.parallel_hand_group_flag));
    encoder.u8(u8::from(sample.row_flag));
    encoder.f32(sample.input_weight);

    let coordinates = sample.coordinates;
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
    encoder.u8(match coordinates.uvw_law {
        UvwCoordinateLaw::PhaseTrackingCentre => 0,
    });
    encode_epoch(encoder, coordinates.time);
    encode_epoch(encoder, coordinates.time_centroid);
    encoder.f64(coordinates.interval_seconds);
    encoder.f64(coordinates.exposure_seconds);
    encode_sky_direction(encoder, coordinates.phase_direction);
    encode_sky_direction(encoder, coordinates.delay_direction);
    encode_sky_direction(encoder, coordinates.pointing_directions.antenna1);
    encode_sky_direction(encoder, coordinates.pointing_directions.antenna2);

    let metadata = sample.metadata;
    encoder.i32(metadata.field_id);
    encoder.i32(metadata.antenna1);
    encoder.i32(metadata.antenna2);
    encoder.i32(metadata.feed1);
    encoder.i32(metadata.feed2);
    encoder.i32(metadata.scan_number);
    encoder.i32(metadata.state_id);
    encoder.i32(metadata.observation_id);
    encoder.i32(metadata.array_id);
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
        "/../../resources/imaging-architecture/baselines/selected-observation-generation-v4.txt"
    ));

    const ENCODED_FIELDS: [&str; 38] = [
        "data_description_id:i32",
        "spectral_window_id:u32",
        "channel_index:u32",
        "frequency_centre_hz:f64",
        "frequency_lower_hz:f64",
        "frequency_upper_hz:f64",
        "channel_width_hz:f64",
        "frequency_frame:tag-u8",
        "polarization_id:u32",
        "correlation_index:u32",
        "correlation_type:tag-u8",
        "visibility:tag-u8-then-float32-f32-or-complex32-real-f32-imaginary-f32",
        "channel_flag:u8",
        "parallel_hand_group_flag:u8",
        "row_flag:u8",
        "input_weight:f32",
        "raw_uvw_m:f64-x-y-z",
        "density_uvw_m:f64-x-y-z",
        "transformed_uvw_m:f64-x-y-z",
        "phase_shift_m:f64",
        "uvw_law:tag-u8",
        "time:mjd-days-f64-then-scale-tag-u8",
        "time_centroid:mjd-days-f64-then-scale-tag-u8",
        "interval_seconds:f64",
        "exposure_seconds:f64",
        "phase_direction:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "delay_direction:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "pointing_antenna1:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "pointing_antenna2:frame-tag-u8-longitude-rad-f64-latitude-rad-f64",
        "field_id:i32",
        "antenna1:i32",
        "antenna2:i32",
        "feed1:i32",
        "feed2:i32",
        "scan_number:i32",
        "state_id:i32",
        "observation_id:i32",
        "array_id:i32",
    ];

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
        assert!(SelectedSpectralContribution::new(0, f32::NAN, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, -0.5, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, 1.5, 1.4e9).is_none());
        assert!(SelectedSpectralContribution::new(0, 1.0, f64::NAN).is_none());
        assert!(SelectedSpectralContributions::new([None, Some(first)]).is_none());
        assert!(SelectedSpectralContributions::new([Some(first), Some(first)]).is_none());
        assert_eq!(SelectedObservationSample::SCHEMA_VERSION, 3);

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
        let [first, second] = samples;

        let one_block = generation(&[&samples]);
        let split = generation(&[&samples[..1], &samples[1..]]);
        assert_eq!(one_block, split, "block boundaries are physical choices");
        assert_ne!(
            one_block,
            generation(&[&[second, first]]),
            "logical sample order participates in content identity"
        );
        assert_eq!(SelectedObservationGenerationId::SCHEMA_VERSION, 4);

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
            ("feed1", |s| s.metadata.feed1 += 1),
            ("feed2", |s| s.metadata.feed2 += 1),
            ("scan", |s| s.metadata.scan_number += 1),
            ("state", |s| s.metadata.state_id += 1),
            ("observation", |s| s.metadata.observation_id += 1),
            ("array", |s| s.metadata.array_id += 1),
        ];
        let baseline = generation(&[&[first]]);
        for (name, mutate) in mutations {
            let mut changed = first;
            mutate(&mut changed);
            assert_ne!(
                baseline,
                generation(&[&[changed]]),
                "{name} must participate in content identity"
            );
        }

        let mut relocated = first;
        relocated.address.measurement_set =
            MeasurementSetIdentity::new(LogicalIdentity::from_sha256([9; 32]));
        relocated.address.physical_row = 999;
        assert_eq!(
            baseline,
            generation(&[&[relocated]]),
            "external source identity and physical row are provenance only"
        );
        let mut residual_input = first;
        residual_input.prediction_target = SelectedPredictionTarget::NotRequested;
        assert_eq!(
            baseline,
            generation(&[&[residual_input]]),
            "prediction and residual consumers bind the same selected-content generation"
        );
        assert_ne!(
            baseline,
            generation(&[&[first, first]]),
            "sample multiplicity participates in content identity"
        );

        let mut negative_zero = first;
        negative_zero.coordinates.phase_shift_m = -0.0;
        let mut positive_zero = first;
        positive_zero.coordinates.phase_shift_m = 0.0;
        assert_eq!(
            generation(&[&[negative_zero]]),
            generation(&[&[positive_zero]]),
            "IEEE signed zero is canonicalized"
        );
        let mut negative_weight_zero = first;
        negative_weight_zero.input_weight = -0.0;
        let mut positive_weight_zero = first;
        positive_weight_zero.input_weight = 0.0;
        assert_eq!(
            generation(&[&[negative_weight_zero]]),
            generation(&[&[positive_weight_zero]]),
            "f32 weight signed zero is canonicalized"
        );
        let mut negative_visibility_zero = first;
        negative_visibility_zero.visibility = SelectedVisibilitySample::Float32(-0.0);
        let mut positive_visibility_zero = first;
        positive_visibility_zero.visibility = SelectedVisibilitySample::Float32(0.0);
        assert_eq!(
            generation(&[&[negative_visibility_zero]]),
            generation(&[&[positive_visibility_zero]]),
            "f32 visibility signed zero is canonicalized"
        );

        assert_eq!(
            one_block.as_bytes(),
            [
                94, 163, 119, 28, 151, 12, 53, 64, 196, 4, 192, 4, 254, 120, 214, 116, 76, 89, 227,
                128, 71, 11, 144, 158, 216, 193, 210, 121, 192, 35, 160, 205,
            ],
            "schema-4 golden ratchet"
        );
    }

    #[test]
    fn generation_fixture_pins_schema_encoder_order_and_digest() {
        assert_eq!(
            fixture_value("identity_domain"),
            "casa-rs-selected-observation-generation"
        );
        assert_eq!(fixture_value("generation_schema_version"), "4");
        assert_eq!(fixture_value("sample_marker"), "0xa5");
        assert_eq!(fixture_value("terminal_marker"), "0xff");
        assert_eq!(fixture_value("terminal_sample_count"), "u64-le");
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

    fn fixture_value(key: &str) -> &str {
        GENERATION_FIXTURE
            .lines()
            .filter_map(|line| line.split_once('='))
            .find_map(|(candidate, value)| (candidate == key).then_some(value))
            .unwrap_or_else(|| panic!("generation fixture lacks {key}"))
    }

    fn generation_fixture_samples() -> [SelectedObservationSample; 2] {
        let first = sample();
        let mut second = first;
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

    fn sample() -> SelectedObservationSample {
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
            coordinates: SelectedSampleCoordinates {
                raw_uvw_m: [12.0, -4.0, 2.0],
                density_uvw_m: [12.5, -4.25, 2.25],
                transformed_uvw_m: [11.75, -3.75, 1.5],
                phase_shift_m: 0.125,
                uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
                time: Epoch::new(59_000.0, TimeScale::Utc),
                time_centroid: Epoch::new(59_000.000_001, TimeScale::Utc),
                interval_seconds: 1.0,
                exposure_seconds: 0.8,
                phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5),
                pointing_directions: SelectedPointingDirections {
                    antenna1: SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499),
                    antenna2: SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498),
                },
            },
            metadata: SelectedSampleMetadata {
                field_id: 14,
                antenna1: 10,
                antenna2: 11,
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
