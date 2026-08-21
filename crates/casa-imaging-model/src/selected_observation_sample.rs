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
const SELECTED_OBSERVATION_GENERATION_VERSION: u32 = 2;

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
    pub const SCHEMA_VERSION: u32 = 2;
}

/// Content digest of one ordered selected-observation sample sequence.
///
/// This ID proves only the exact values supplied to [`Self::from_samples`]. It
/// does not prove commitment membership, canonical or legal order, exhaustive
/// coverage, retained source access, mutation freedom, execution-attempt
/// freshness, or traversal completion. Runtime-owned completion evidence must
/// establish those facts before downstream use.
///
/// Physical block boundaries are absent, but iteration order is identity
/// bearing. The traversal owner must therefore emit one canonical logical
/// sample order for every legal physical schedule.
///
/// There is deliberately no constructor from raw digest bytes.
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationGenerationId;
///
/// let _ = SelectedObservationGenerationId::from_sha256([0; 32]);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedObservationGenerationId([u8; 32]);

impl SelectedObservationGenerationId {
    /// Content identity schema version.
    pub const SCHEMA_VERSION: u32 = SELECTED_OBSERVATION_GENERATION_VERSION;

    /// Hash one already-ordered stream of selected-sample reports.
    ///
    /// Calling this function does not validate the public reports and does not
    /// mint authoritative traversal completion evidence.
    #[must_use]
    pub fn from_samples<'a>(
        samples: impl IntoIterator<Item = &'a SelectedObservationSample>,
    ) -> Self {
        let mut encoder = SelectedObservationGenerationEncoder::new();
        for sample in samples {
            encoder.push(sample);
        }
        encoder.finish().0
    }

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
    encoder.u8(match sample.prediction_target {
        SelectedPredictionTarget::NotRequested => 0,
        SelectedPredictionTarget::ModelData => 1,
    });
    encoder.u8(u8::from(sample.channel_flag));
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

    #[test]
    fn content_generation_is_block_independent_exhaustive_and_provenance_free() {
        let first = sample();
        let mut second = first;
        second.address.physical_row = 12;
        second.address.channel_index = 8;
        second.address.frequency_centre_hz += 1_000_000.0;
        let samples = [first, second];

        let one_block = generation(&[&samples]);
        let split = generation(&[&samples[..1], &samples[1..]]);
        assert_eq!(one_block, split, "block boundaries are physical choices");
        assert_ne!(
            one_block,
            generation(&[&[second, first]]),
            "logical sample order participates in content identity"
        );
        assert_eq!(SelectedObservationGenerationId::SCHEMA_VERSION, 2);

        let mutations: &[(&str, fn(&mut SelectedObservationSample))] = &[
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
            ("prediction target", |s| {
                s.prediction_target = SelectedPredictionTarget::NotRequested
            }),
            ("channel flag", |s| s.channel_flag = !s.channel_flag),
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
                131, 105, 54, 159, 77, 174, 149, 224, 145, 216, 150, 144, 0, 43, 151, 243, 80,
                31, 75, 231, 254, 135, 40, 44, 95, 241, 135, 140, 141, 92, 86, 58,
            ],
            "schema-2 golden ratchet"
        );
    }

    fn generation(blocks: &[&[SelectedObservationSample]]) -> SelectedObservationGenerationId {
        SelectedObservationGenerationId::from_samples(blocks.iter().flat_map(|block| block.iter()))
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
