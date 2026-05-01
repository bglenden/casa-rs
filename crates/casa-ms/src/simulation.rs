// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic MeasurementSet generation for tutorial simulation workflows.
//!
//! This module owns the first reusable MS-writing slice of the VLA simulation
//! vertical. It mirrors the CASA `simobserve` setup order at the data-model
//! level: validate a model image path, set array configuration, define the
//! spectral window and field, sample the requested observing time range, and
//! write CASA-compatible MS subtables plus uncorrupted visibility rows.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_coordinates::{Coordinate, DirectionCoordinate, Projection, ProjectionType};
use casa_imaging::{
    ImageGeometry, PrimaryBeamModel, StandardMfsModelPredictor, primary_beam_voltage_pattern,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::position::MPosition;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::column_def::{ColumnDef, ColumnKind};
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};
use crate::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};

/// Antenna configuration row for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticAntenna {
    /// Antenna name, for example `VLA01`.
    pub name: String,
    /// Station or pad name, for example `N01`.
    pub station: String,
    /// ITRF position in meters.
    pub position_m: [f64; 3],
    /// Dish diameter in meters.
    pub dish_diameter_m: f64,
}

impl SyntheticAntenna {
    /// Construct a VLA-style ground-based alt-az antenna row.
    pub fn vla(name: impl Into<String>, station: impl Into<String>, position_m: [f64; 3]) -> Self {
        Self {
            name: name.into(),
            station: station.into(),
            position_m,
            dish_diameter_m: 25.0,
        }
    }
}

/// Return the CASA Guide VLA A-configuration antenna list used by the
/// protoplanetary-disk simulation tutorial.
///
/// The values mirror CASA's packaged `vla.a.cfg` antenna-position file. Names
/// are the VLA pad labels used by CASA for this configuration.
pub fn tutorial_vla_a_antennas() -> Vec<SyntheticAntenna> {
    VLA_A_ANTENNAS
        .iter()
        .map(|antenna| {
            SyntheticAntenna::vla(
                antenna.name,
                antenna.name,
                [antenna.x_m, antenna.y_m, antenna.z_m],
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct VLaAntennaDef {
    name: &'static str,
    x_m: f64,
    y_m: f64,
    z_m: f64,
}

const VLA_A_ANTENNAS: &[VLaAntennaDef] = &[
    VLaAntennaDef {
        name: "W08",
        x_m: -1_601_614.061201,
        y_m: -5_042_001.676547,
        z_m: 3_554_652.455603,
    },
    VLaAntennaDef {
        name: "W16",
        x_m: -1_602_592.823528,
        y_m: -5_042_055.013423,
        z_m: 3_554_140.652770,
    },
    VLaAntennaDef {
        name: "W24",
        x_m: -1_604_008.701913,
        y_m: -5_042_135.835806,
        z_m: 3_553_403.666765,
    },
    VLaAntennaDef {
        name: "W32",
        x_m: -1_605_808.598184,
        y_m: -5_042_230.070459,
        z_m: 3_552_459.167358,
    },
    VLaAntennaDef {
        name: "W40",
        x_m: -1_607_962.411673,
        y_m: -5_042_338.157771,
        z_m: 3_551_324.887280,
    },
    VLaAntennaDef {
        name: "W48",
        x_m: -1_610_451.987125,
        y_m: -5_042_471.380472,
        z_m: 3_550_021.011562,
    },
    VLaAntennaDef {
        name: "W56",
        x_m: -1_613_255.373440,
        y_m: -5_042_613.052534,
        z_m: 3_548_545.864364,
    },
    VLaAntennaDef {
        name: "W64",
        x_m: -1_616_361.554136,
        y_m: -5_042_770.440739,
        z_m: 3_546_911.386423,
    },
    VLaAntennaDef {
        name: "W72",
        x_m: -1_619_757.278011,
        y_m: -5_042_937.574555,
        z_m: 3_545_120.332832,
    },
    VLaAntennaDef {
        name: "E08",
        x_m: -1_600_801.880602,
        y_m: -5_042_219.386677,
        z_m: 3_554_706.382285,
    },
    VLaAntennaDef {
        name: "E16",
        x_m: -1_599_926.059409,
        y_m: -5_042_772.992580,
        z_m: 3_554_319.742840,
    },
    VLaAntennaDef {
        name: "E24",
        x_m: -1_598_663.046405,
        y_m: -5_043_581.426755,
        z_m: 3_553_766.973356,
    },
    VLaAntennaDef {
        name: "E32",
        x_m: -1_597_053.095558,
        y_m: -5_044_604.747750,
        z_m: 3_553_058.947311,
    },
    VLaAntennaDef {
        name: "E40",
        x_m: -1_595_124.918941,
        y_m: -5_045_829.515575,
        z_m: 3_552_210.615356,
    },
    VLaAntennaDef {
        name: "E48",
        x_m: -1_592_894.065650,
        y_m: -5_047_229.198656,
        z_m: 3_551_221.180045,
    },
    VLaAntennaDef {
        name: "E56",
        x_m: -1_590_380.588363,
        y_m: -5_048_810.325262,
        z_m: 3_550_108.401088,
    },
    VLaAntennaDef {
        name: "E64",
        x_m: -1_587_600.201930,
        y_m: -5_050_575.976082,
        z_m: 3_548_885.379419,
    },
    VLaAntennaDef {
        name: "E72",
        x_m: -1_584_460.899441,
        y_m: -5_052_385.734791,
        z_m: 3_547_599.958930,
    },
    VLaAntennaDef {
        name: "N08",
        x_m: -1_601_147.885235,
        y_m: -5_041_733.855114,
        z_m: 3_555_235.914849,
    },
    VLaAntennaDef {
        name: "N16",
        x_m: -1_601_061.915919,
        y_m: -5_041_175.907706,
        z_m: 3_556_057.981979,
    },
    VLaAntennaDef {
        name: "N24",
        x_m: -1_600_929.966850,
        y_m: -5_040_316.401791,
        z_m: 3_557_330.277550,
    },
    VLaAntennaDef {
        name: "N32",
        x_m: -1_600_780.996259,
        y_m: -5_039_347.463556,
        z_m: 3_558_761.487153,
    },
    VLaAntennaDef {
        name: "N40",
        x_m: -1_600_592.692550,
        y_m: -5_038_121.380641,
        z_m: 3_560_574.803338,
    },
    VLaAntennaDef {
        name: "N48",
        x_m: -1_600_374.808396,
        y_m: -5_036_704.253012,
        z_m: 3_562_667.855946,
    },
    VLaAntennaDef {
        name: "N56",
        x_m: -1_600_128.313994,
        y_m: -5_035_104.177252,
        z_m: 3_565_024.645048,
    },
    VLaAntennaDef {
        name: "N64",
        x_m: -1_599_855.570998,
        y_m: -5_033_332.403323,
        z_m: 3_567_636.578590,
    },
    VLaAntennaDef {
        name: "N72",
        x_m: -1_599_557.838366,
        y_m: -5_031_396.391942,
        z_m: 3_570_494.716758,
    },
];

/// Single spectral-window setup for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticSpectralSetup {
    /// Spectral-window name.
    pub name: String,
    /// First channel center frequency in Hz.
    pub start_frequency_hz: f64,
    /// Channel width in Hz.
    pub channel_width_hz: f64,
    /// Number of channels.
    pub channel_count: usize,
}

impl SyntheticSpectralSetup {
    /// Return the channel-center frequencies in Hz.
    pub fn channel_frequencies_hz(&self) -> Vec<f64> {
        (0..self.channel_count)
            .map(|index| self.start_frequency_hz + index as f64 * self.channel_width_hz)
            .collect()
    }

    fn total_bandwidth_hz(&self) -> f64 {
        self.channel_width_hz.abs() * self.channel_count as f64
    }
}

/// Request for generating a synthetic MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationRequest {
    /// Existing model image path that defines the tutorial model provenance.
    pub model_image: PathBuf,
    /// Optional peak brightness scaling in Jy/pixel, matching CASA
    /// `simobserve(inbright=...)` semantics for the model image.
    #[serde(default)]
    pub model_peak_jy_per_pixel: Option<f32>,
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Replace an existing output MeasurementSet directory.
    pub overwrite: bool,
    /// Telescope name written to `OBSERVATION`.
    pub telescope_name: String,
    /// Project code written to `OBSERVATION`.
    pub project: String,
    /// Observer name written to `OBSERVATION`.
    pub observer: String,
    /// Field/source name.
    pub field_name: String,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    pub phase_center_rad: [f64; 2],
    /// Observation start time in MJD seconds UTC.
    pub start_time_mjd_seconds: f64,
    /// Requested on-source duration in seconds.
    pub duration_seconds: f64,
    /// Integration time in seconds.
    pub integration_seconds: f64,
    /// Antenna configuration.
    pub antennas: Vec<SyntheticAntenna>,
    /// Spectral-window setup.
    pub spectral_setup: SyntheticSpectralSetup,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    pub predict_model: bool,
    /// Optional deterministic corruptions applied to predicted visibility data.
    #[serde(default)]
    pub corruption: Option<SyntheticCorruptionConfig>,
}

impl SyntheticObservationRequest {
    /// Build the tutorial VLA protoplanetary-disk foundation request.
    pub fn vla_ppdisk(
        model_image: impl Into<PathBuf>,
        output_ms: impl Into<PathBuf>,
        antennas: Vec<SyntheticAntenna>,
    ) -> Self {
        Self {
            model_image: model_image.into(),
            model_peak_jy_per_pixel: Some(3.0e-5),
            output_ms: output_ms.into(),
            overwrite: false,
            telescope_name: "VLA".to_string(),
            project: "casa-rs-vla-ppdisk".to_string(),
            observer: "casa-rs".to_string(),
            field_name: "ppdisk".to_string(),
            phase_center_rad: [4.712_391_234_768_306, -0.401_423_788_703_971_4],
            start_time_mjd_seconds: 4_895_229_577.784_943,
            duration_seconds: 3_600.0,
            integration_seconds: 2.0,
            antennas,
            spectral_setup: SyntheticSpectralSetup {
                name: "Qband".to_string(),
                start_frequency_hz: 44.0e9,
                channel_width_hz: 128.0e6,
                channel_count: 1,
            },
            predict_model: true,
            corruption: None,
        }
    }
}

/// Deterministic corruption controls for tutorial-grade synthetic observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticCorruptionConfig {
    /// Seed used for deterministic random draws.
    pub seed: u64,
    /// Per-complex-component Gaussian noise standard deviation in Jy.
    #[serde(default)]
    pub noise_stddev_jy: Option<f32>,
    /// Per-antenna complex gain corruption.
    #[serde(default)]
    pub gain_phase: Option<SyntheticGainPhaseCorruption>,
}

/// Per-antenna gain and phase corruption controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticGainPhaseCorruption {
    /// Gaussian fractional amplitude standard deviation.
    pub amplitude_stddev: f32,
    /// Gaussian phase standard deviation in radians.
    pub phase_stddev_rad: f32,
}

/// Summary of a generated synthetic MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationReport {
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Model image path recorded as provenance.
    pub model_image: PathBuf,
    /// Number of antennas written.
    pub antenna_count: usize,
    /// Number of baseline rows per time sample.
    pub baseline_count: usize,
    /// Number of time samples written.
    pub time_sample_count: usize,
    /// Number of main-table rows written.
    pub main_row_count: usize,
    /// Number of channels written in the spectral window.
    pub channel_count: usize,
    /// Number of complex visibility cells with non-zero predicted model values.
    pub nonzero_visibility_count: usize,
    /// Names of corruption effects applied to `MAIN.DATA`.
    pub applied_corruptions: Vec<String>,
}

/// Generate an uncorrupted CASA-compatible synthetic MeasurementSet.
///
/// The current implementation writes structurally complete MS metadata and can
/// predict uncorrupted visibility samples from a single-plane FITS model image.
pub fn generate_synthetic_observation_ms(
    request: &SyntheticObservationRequest,
) -> MsResult<SyntheticObservationReport> {
    validate_request(request)?;

    if request.output_ms.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.output_ms).map_err(|error| {
                MsError::SyntheticObservation(format!(
                    "failed to remove existing output {}: {error}",
                    request.output_ms.display()
                ))
            })?;
        } else {
            return Err(MsError::SyntheticObservation(format!(
                "output MeasurementSet {} already exists",
                request.output_ms.display()
            )));
        }
    }

    let mut ms = MeasurementSet::create(
        &request.output_ms,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )?;

    populate_antennas(&mut ms, &request.antennas)?;
    populate_field(&mut ms, request)?;
    populate_spectral_window(&mut ms, &request.spectral_setup)?;
    populate_polarization(&mut ms)?;
    populate_data_description(&mut ms)?;
    populate_state(&mut ms)?;
    populate_processor(&mut ms)?;
    populate_feed(&mut ms, request)?;
    populate_observation(&mut ms, request)?;
    populate_history(&mut ms, request)?;

    let time_sample_count =
        time_sample_count(request.duration_seconds, request.integration_seconds);
    let baseline_count = request.antennas.len() * (request.antennas.len() - 1) / 2;
    let model = if request.predict_model {
        Some(read_fits_model_image(
            &request.model_image,
            request.model_peak_jy_per_pixel,
        )?)
    } else {
        None
    };
    let nonzero_visibility_count =
        populate_main_rows(&mut ms, request, time_sample_count, model.as_ref())?;

    ms.save()?;

    Ok(SyntheticObservationReport {
        output_ms: request.output_ms.clone(),
        model_image: request.model_image.clone(),
        antenna_count: request.antennas.len(),
        baseline_count,
        time_sample_count,
        main_row_count: baseline_count * time_sample_count,
        channel_count: request.spectral_setup.channel_count,
        nonzero_visibility_count,
        applied_corruptions: applied_corruption_names(request.corruption.as_ref()),
    })
}

fn validate_request(request: &SyntheticObservationRequest) -> MsResult<()> {
    if !request.model_image.exists() {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} does not exist",
            request.model_image.display()
        )));
    }
    if request.antennas.len() < 2 {
        return Err(MsError::SyntheticObservation(
            "at least two antennas are required for interferometric simulation".to_string(),
        ));
    }
    for antenna in &request.antennas {
        if antenna.name.trim().is_empty() {
            return Err(MsError::SyntheticObservation(
                "antenna name must not be empty".to_string(),
            ));
        }
        if antenna.station.trim().is_empty() {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} station must not be empty",
                antenna.name
            )));
        }
        if antenna.dish_diameter_m <= 0.0 || !antenna.dish_diameter_m.is_finite() {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} dish diameter must be positive",
                antenna.name
            )));
        }
        if antenna.position_m.iter().any(|value| !value.is_finite()) {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} position must be finite",
                antenna.name
            )));
        }
    }
    if request.spectral_setup.channel_count == 0 {
        return Err(MsError::SyntheticObservation(
            "spectral setup must include at least one channel".to_string(),
        ));
    }
    if request.spectral_setup.start_frequency_hz <= 0.0
        || !request.spectral_setup.start_frequency_hz.is_finite()
    {
        return Err(MsError::SyntheticObservation(
            "spectral start frequency must be positive".to_string(),
        ));
    }
    if request.spectral_setup.channel_width_hz == 0.0
        || !request.spectral_setup.channel_width_hz.is_finite()
    {
        return Err(MsError::SyntheticObservation(
            "spectral channel width must be finite and non-zero".to_string(),
        ));
    }
    if request.duration_seconds <= 0.0 || !request.duration_seconds.is_finite() {
        return Err(MsError::SyntheticObservation(
            "observation duration must be positive".to_string(),
        ));
    }
    if request.integration_seconds <= 0.0 || !request.integration_seconds.is_finite() {
        return Err(MsError::SyntheticObservation(
            "integration time must be positive".to_string(),
        ));
    }
    if request
        .phase_center_rad
        .iter()
        .any(|value| !value.is_finite())
    {
        return Err(MsError::SyntheticObservation(
            "phase center coordinates must be finite".to_string(),
        ));
    }
    if let Some(corruption) = &request.corruption {
        validate_corruption(corruption)?;
    }
    if let Some(model_peak_jy_per_pixel) = request.model_peak_jy_per_pixel {
        if !(model_peak_jy_per_pixel.is_finite() && model_peak_jy_per_pixel > 0.0) {
            return Err(MsError::SyntheticObservation(
                "model_peak_jy_per_pixel must be finite and positive".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_corruption(corruption: &SyntheticCorruptionConfig) -> MsResult<()> {
    if let Some(noise_stddev_jy) = corruption.noise_stddev_jy {
        if !(noise_stddev_jy.is_finite() && noise_stddev_jy >= 0.0) {
            return Err(MsError::SyntheticObservation(
                "noise_stddev_jy must be finite and non-negative".to_string(),
            ));
        }
    }
    if let Some(gain_phase) = &corruption.gain_phase {
        if !(gain_phase.amplitude_stddev.is_finite() && gain_phase.amplitude_stddev >= 0.0) {
            return Err(MsError::SyntheticObservation(
                "gain_phase amplitude_stddev must be finite and non-negative".to_string(),
            ));
        }
        if !(gain_phase.phase_stddev_rad.is_finite() && gain_phase.phase_stddev_rad >= 0.0) {
            return Err(MsError::SyntheticObservation(
                "gain_phase phase_stddev_rad must be finite and non-negative".to_string(),
            ));
        }
    }
    Ok(())
}

fn applied_corruption_names(corruption: Option<&SyntheticCorruptionConfig>) -> Vec<String> {
    let Some(corruption) = corruption else {
        return Vec::new();
    };
    let mut names = Vec::new();
    if corruption.noise_stddev_jy.unwrap_or(0.0) > 0.0 {
        names.push("noise".to_string());
    }
    if let Some(gain_phase) = &corruption.gain_phase {
        if gain_phase.amplitude_stddev > 0.0 || gain_phase.phase_stddev_rad > 0.0 {
            names.push("gain_phase".to_string());
        }
    }
    names
}

fn populate_antennas(ms: &mut MeasurementSet, antennas: &[SyntheticAntenna]) -> MsResult<()> {
    let mut antenna_table = ms.antenna_mut()?;
    for antenna in antennas {
        antenna_table.add_antenna(
            &antenna.name,
            &antenna.station,
            "GROUND-BASED",
            "ALT-AZ",
            antenna.position_m,
            [0.0; 3],
            antenna.dish_diameter_m,
        )?;
    }
    Ok(())
}

fn populate_field(ms: &mut MeasurementSet, request: &SyntheticObservationRequest) -> MsResult<()> {
    let direction = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], request.phase_center_rad.to_vec()).unwrap(),
    ));
    let row = row_from_defs(
        schema::field::REQUIRED_COLUMNS,
        &[
            ("NAME", s(&request.field_name)),
            ("CODE", s("")),
            ("NUM_POLY", i(0)),
            ("DELAY_DIR", direction.clone()),
            ("PHASE_DIR", direction.clone()),
            ("REFERENCE_DIR", direction),
            ("SOURCE_ID", i(-1)),
            ("TIME", f(request.start_time_mjd_seconds)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Field)?.add_row(row)?;
    Ok(())
}

fn populate_spectral_window(
    ms: &mut MeasurementSet,
    spectral_setup: &SyntheticSpectralSetup,
) -> MsResult<()> {
    let frequencies = spectral_setup.channel_frequencies_hz();
    let widths = vec![spectral_setup.channel_width_hz; spectral_setup.channel_count];
    let row = row_from_defs(
        schema::spectral_window::REQUIRED_COLUMNS,
        &[
            ("NUM_CHAN", i(spectral_setup.channel_count as i32)),
            ("NAME", s(&spectral_setup.name)),
            ("REF_FREQUENCY", f(spectral_setup.start_frequency_hz)),
            ("TOTAL_BANDWIDTH", f(spectral_setup.total_bandwidth_hz())),
            (
                "CHAN_FREQ",
                f64_array(&frequencies, vec![frequencies.len()]),
            ),
            ("CHAN_WIDTH", f64_array(&widths, vec![widths.len()])),
            ("EFFECTIVE_BW", f64_array(&widths, vec![widths.len()])),
            ("RESOLUTION", f64_array(&widths, vec![widths.len()])),
            ("MEAS_FREQ_REF", i(5)),
            (
                "NET_SIDEBAND",
                i(if spectral_setup.channel_width_hz >= 0.0 {
                    1
                } else {
                    -1
                }),
            ),
            ("FREQ_GROUP", i(0)),
            ("FREQ_GROUP_NAME", s("")),
            ("IF_CONV_CHAIN", i(0)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::SpectralWindow)?.add_row(row)?;
    Ok(())
}

fn populate_polarization(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::polarization::REQUIRED_COLUMNS,
        &[
            ("NUM_CORR", i(2)),
            ("CORR_TYPE", i32_array(&[5, 8], vec![2])),
            ("CORR_PRODUCT", i32_array(&[0, 1, 0, 1], vec![2, 2])),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Polarization)?.add_row(row)?;
    Ok(())
}

fn populate_data_description(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::data_description::REQUIRED_COLUMNS,
        &[
            ("SPECTRAL_WINDOW_ID", i(0)),
            ("POLARIZATION_ID", i(0)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::DataDescription)?.add_row(row)?;
    Ok(())
}

fn populate_state(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::state::REQUIRED_COLUMNS,
        &[
            ("CAL", f(0.0)),
            ("FLAG_ROW", b(false)),
            ("LOAD", f(0.0)),
            ("OBS_MODE", s("TARGET.ON_SOURCE")),
            ("REF", b(false)),
            ("SIG", b(true)),
            ("SUB_SCAN", i(0)),
        ],
    );
    subtable_mut(ms, SubtableId::State)?.add_row(row)?;
    Ok(())
}

fn populate_processor(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::processor::REQUIRED_COLUMNS,
        &[
            ("FLAG_ROW", b(false)),
            ("MODE_ID", i(0)),
            ("SUB_TYPE", s("SYNTHETIC")),
            ("TYPE", s("CORRELATOR")),
            ("TYPE_ID", i(0)),
        ],
    );
    subtable_mut(ms, SubtableId::Processor)?.add_row(row)?;
    Ok(())
}

fn populate_feed(ms: &mut MeasurementSet, request: &SyntheticObservationRequest) -> MsResult<()> {
    for antenna_id in 0..request.antennas.len() {
        let row = row_from_defs(
            schema::feed::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", i(antenna_id as i32)),
                ("BEAM_ID", i(-1)),
                ("BEAM_OFFSET", f64_array(&[0.0, 0.0, 0.0, 0.0], vec![2, 2])),
                ("FEED_ID", i(0)),
                ("INTERVAL", f(request.duration_seconds)),
                ("NUM_RECEPTORS", i(2)),
                (
                    "POL_RESPONSE",
                    complex_array(
                        &[
                            Complex32::new(1.0, 0.0),
                            Complex32::new(0.0, 0.0),
                            Complex32::new(0.0, 0.0),
                            Complex32::new(1.0, 0.0),
                        ],
                        vec![2, 2],
                    ),
                ),
                ("POLARIZATION_TYPE", string_array(&["R", "L"], vec![2])),
                ("POSITION", f64_array(&[0.0, 0.0, 0.0], vec![3])),
                ("RECEPTOR_ANGLE", f64_array(&[0.0, 0.0], vec![2])),
                ("SPECTRAL_WINDOW_ID", i(0)),
                ("TIME", f(request.start_time_mjd_seconds)),
            ],
        );
        subtable_mut(ms, SubtableId::Feed)?.add_row(row)?;
    }
    Ok(())
}

fn populate_observation(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
) -> MsResult<()> {
    let end_time = request.start_time_mjd_seconds + request.duration_seconds;
    let row = row_from_defs(
        schema::observation::REQUIRED_COLUMNS,
        &[
            ("FLAG_ROW", b(false)),
            (
                "LOG",
                string_array(&["generated by casa-rs synthetic observation"], vec![1]),
            ),
            ("OBSERVER", s(&request.observer)),
            ("PROJECT", s(&request.project)),
            ("RELEASE_DATE", f(0.0)),
            ("SCHEDULE", string_array(&["synthetic"], vec![1])),
            ("SCHEDULE_TYPE", s("synthetic")),
            ("TELESCOPE_NAME", s(&request.telescope_name)),
            (
                "TIME_RANGE",
                f64_array(&[request.start_time_mjd_seconds, end_time], vec![2]),
            ),
        ],
    );
    subtable_mut(ms, SubtableId::Observation)?.add_row(row)?;
    Ok(())
}

fn populate_history(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
) -> MsResult<()> {
    let message = format!(
        "generated synthetic observation from model image {}",
        request.model_image.display()
    );
    let row = row_from_defs(
        schema::history::REQUIRED_COLUMNS,
        &[
            ("APPLICATION", s("casa-rs")),
            ("APP_PARAMS", string_array(&[&message], vec![1])),
            ("CLI_COMMAND", string_array(&[], vec![0])),
            ("MESSAGE", s(&message)),
            ("OBJECT_ID", i(0)),
            ("OBSERVATION_ID", i(0)),
            ("ORIGIN", s("casa_ms::simulation")),
            ("PRIORITY", s("NORMAL")),
            ("TIME", f(request.start_time_mjd_seconds)),
        ],
    );
    subtable_mut(ms, SubtableId::History)?.add_row(row)?;
    Ok(())
}

fn populate_main_rows(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
    samples: usize,
    model: Option<&FitsModelImage>,
) -> MsResult<usize> {
    let num_corr = 2usize;
    let num_chan = request.spectral_setup.channel_count;
    let flag = bool_array(&vec![false; num_corr * num_chan], vec![num_corr, num_chan]);
    let flag_category = bool_array(
        &vec![false; num_corr * num_chan],
        vec![1, num_corr, num_chan],
    );
    let weight = f32_array(&vec![1.0; num_corr], vec![num_corr]);
    let sigma = f32_array(&vec![1.0; num_corr], vec![num_corr]);
    let main_defs = ms_main_defs(ms);
    let predictors = model
        .map(|model| {
            build_channel_predictors(model, &request.spectral_setup, request.phase_center_rad)
        })
        .transpose()?;
    let mut corruption = request
        .corruption
        .as_ref()
        .map(|config| SyntheticCorruptionState::new(config, request.antennas.len()));
    let mut nonzero_visibility_count = 0usize;

    for sample in 0..samples {
        let time =
            request.start_time_mjd_seconds + (sample as f64 + 0.5) * request.integration_seconds;
        let antenna_uvws =
            antenna_uvw_positions(&request.antennas, request.phase_center_rad, time)?;
        for antenna1 in 0..request.antennas.len() {
            for antenna2 in (antenna1 + 1)..request.antennas.len() {
                let uvw = [
                    antenna_uvws[antenna2][0] - antenna_uvws[antenna1][0],
                    antenna_uvws[antenna2][1] - antenna_uvws[antenna1][1],
                    antenna_uvws[antenna2][2] - antenna_uvws[antenna1][2],
                ];
                let mut data_values = predicted_data_values(
                    predictors.as_ref(),
                    &request.spectral_setup,
                    uvw,
                    num_corr,
                );
                if let Some(corruption) = corruption.as_mut() {
                    corruption.apply(&mut data_values, antenna1, antenna2);
                }
                nonzero_visibility_count += data_values
                    .iter()
                    .filter(|value| value.re != 0.0 || value.im != 0.0)
                    .count();
                let data = complex_array(&data_values, vec![num_corr, num_chan]);
                let row = row_from_defs(
                    &main_defs,
                    &[
                        ("ANTENNA1", i(antenna1 as i32)),
                        ("ANTENNA2", i(antenna2 as i32)),
                        ("ARRAY_ID", i(0)),
                        ("DATA_DESC_ID", i(0)),
                        ("EXPOSURE", f(request.integration_seconds)),
                        ("FEED1", i(0)),
                        ("FEED2", i(0)),
                        ("FIELD_ID", i(0)),
                        ("FLAG", flag.clone()),
                        ("FLAG_CATEGORY", flag_category.clone()),
                        ("FLAG_ROW", b(false)),
                        ("INTERVAL", f(request.integration_seconds)),
                        ("OBSERVATION_ID", i(0)),
                        ("PROCESSOR_ID", i(0)),
                        ("SCAN_NUMBER", i(1)),
                        ("SIGMA", sigma.clone()),
                        ("STATE_ID", i(0)),
                        ("TIME", f(time)),
                        ("TIME_CENTROID", f(time)),
                        ("UVW", f64_array(&uvw, vec![3])),
                        ("WEIGHT", weight.clone()),
                        ("DATA", data.clone()),
                    ],
                );
                ms.main_table_mut().add_row(row)?;
            }
        }
    }
    Ok(nonzero_visibility_count)
}

#[derive(Debug, Clone)]
struct FitsModelImage {
    pixels: Array2<f32>,
    cell_size_rad: [f64; 2],
    reference_direction_rad: Option<[f64; 2]>,
}

struct SyntheticChannelPredictor {
    predictor: StandardMfsModelPredictor,
    phase_offset_rad: [f64; 2],
}

// CASA GridFT negates u/v to compensate for an image-inversion convention in
// the degridding path. After matching that handedness and the FITS center-pixel
// offset, the tutorial model needs this residual image-x phase alignment to
// match CASA's GridFT prediction at numerical-noise scale.
const CASA_GRIDFT_IMAGE_INVERSION_PHASE_PIXELS: f64 = 0.015_122_8;

fn build_channel_predictors(
    model: &FitsModelImage,
    spectral_setup: &SyntheticSpectralSetup,
    phase_center_rad: [f64; 2],
) -> MsResult<Vec<SyntheticChannelPredictor>> {
    let geometry = ImageGeometry {
        image_shape: [model.pixels.shape()[0], model.pixels.shape()[1]],
        cell_size_rad: model.cell_size_rad,
    };
    let phase_offset_rad = casa_model_phase_offset(model, phase_center_rad);
    (0..spectral_setup.channel_count)
        .map(|channel| {
            let frequency_hz = spectral_setup.start_frequency_hz
                + channel as f64 * spectral_setup.channel_width_hz;
            let beam_corrected_pixels =
                apply_simulator_primary_beam(model, phase_center_rad, frequency_hz);
            let mut casa_oriented_pixels = Array2::<f32>::zeros(model.pixels.raw_dim());
            for x in 0..model.pixels.shape()[0] {
                for y in 0..model.pixels.shape()[1] {
                    // CASA's simulator image prediction treats positive RA
                    // offsets with the opposite image-x handedness from this
                    // crate's pure imaging gridder.
                    casa_oriented_pixels[(model.pixels.shape()[0] - 1 - x, y)] =
                        beam_corrected_pixels[(x, y)];
                }
            }
            let predictor = StandardMfsModelPredictor::new(geometry, &casa_oriented_pixels)
                .map_err(|error| {
                    MsError::SyntheticObservation(format!("model prediction setup failed: {error}"))
                })?;
            Ok(SyntheticChannelPredictor {
                predictor,
                phase_offset_rad,
            })
        })
        .collect()
}

fn apply_simulator_primary_beam(
    model: &FitsModelImage,
    phase_center_rad: [f64; 2],
    frequency_hz: f64,
) -> Array2<f32> {
    let mut pixels = model.pixels.clone();
    let Some(reference_direction_rad) = model.reference_direction_rad else {
        return pixels;
    };
    let center_ra_offset =
        circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0])
            * phase_center_rad[1].cos();
    let center_dec_offset = reference_direction_rad[1] - phase_center_rad[1];
    let x_ref = model.pixels.shape()[0] as f64 / 2.0;
    let y_ref = model.pixels.shape()[1] as f64 / 2.0;

    for x in 0..model.pixels.shape()[0] {
        for y in 0..model.pixels.shape()[1] {
            let l = center_ra_offset + (x as f64 - x_ref) * model.cell_size_rad[0];
            let m = center_dec_offset + (y as f64 - y_ref) * model.cell_size_rad[1];
            let vp = primary_beam_voltage_pattern(
                PrimaryBeamModel::Airy {
                    dish_diameter_m: 25.0,
                    blockage_diameter_m: 2.36,
                },
                (l * l + m * m).sqrt(),
                frequency_hz,
            );
            pixels[(x, y)] *= vp * vp;
        }
    }
    pixels
}

fn casa_model_phase_offset(model: &FitsModelImage, phase_center_rad: [f64; 2]) -> [f64; 2] {
    let Some(reference_direction_rad) = model.reference_direction_rad else {
        return [0.0, 0.0];
    };
    let ra_offset = circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0])
        * phase_center_rad[1].cos();
    let dec_offset = reference_direction_rad[1] - phase_center_rad[1];
    [
        ra_offset - (0.5 - CASA_GRIDFT_IMAGE_INVERSION_PHASE_PIXELS) * model.cell_size_rad[0],
        dec_offset - 0.5 * model.cell_size_rad[1],
    ]
}

fn circular_angle_delta_rad(delta: f64) -> f64 {
    (delta + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
}

fn predicted_data_values(
    predictors: Option<&Vec<SyntheticChannelPredictor>>,
    spectral_setup: &SyntheticSpectralSetup,
    uvw_m: [f64; 3],
    num_corr: usize,
) -> Vec<Complex32> {
    let mut values = vec![Complex32::new(0.0, 0.0); num_corr * spectral_setup.channel_count];
    if let Some(predictors) = predictors {
        for (channel, predictor) in predictors.iter().enumerate() {
            let frequency_hz = spectral_setup.start_frequency_hz
                + channel as f64 * spectral_setup.channel_width_hz;
            let wavelength_m = 299_792_458.0 / frequency_hz;
            let u_lambda = uvw_m[0] / wavelength_m;
            let v_lambda = uvw_m[1] / wavelength_m;
            let phase = std::f64::consts::TAU
                * (u_lambda * predictor.phase_offset_rad[0]
                    + v_lambda * predictor.phase_offset_rad[1]);
            let phase_shift = Complex32::new(phase.cos() as f32, phase.sin() as f32);
            let visibility = predictor.predictor.predict(u_lambda, v_lambda) * phase_shift;
            for corr in 0..num_corr {
                let index = corr * spectral_setup.channel_count + channel;
                values[index] = visibility;
            }
        }
    }
    values
}

struct SyntheticCorruptionState {
    noise_stddev_jy: f32,
    gains: Vec<Complex32>,
    rng: DeterministicRng,
}

impl SyntheticCorruptionState {
    fn new(config: &SyntheticCorruptionConfig, antenna_count: usize) -> Self {
        let mut rng = DeterministicRng::new(config.seed);
        let gains = (0..antenna_count)
            .map(|_| {
                if let Some(gain_phase) = &config.gain_phase {
                    let amplitude = 1.0 + rng.gaussian_f32() * gain_phase.amplitude_stddev;
                    let phase = rng.gaussian_f32() * gain_phase.phase_stddev_rad;
                    Complex32::new(amplitude * phase.cos(), amplitude * phase.sin())
                } else {
                    Complex32::new(1.0, 0.0)
                }
            })
            .collect();
        Self {
            noise_stddev_jy: config.noise_stddev_jy.unwrap_or(0.0),
            gains,
            rng,
        }
    }

    fn apply(&mut self, values: &mut [Complex32], antenna1: usize, antenna2: usize) {
        let baseline_gain = self.gains[antenna1] * self.gains[antenna2].conj();
        for value in values {
            *value *= baseline_gain;
            if self.noise_stddev_jy > 0.0 {
                value.re += self.rng.gaussian_f32() * self.noise_stddev_jy;
                value.im += self.rng.gaussian_f32() * self.noise_stddev_jy;
            }
        }
    }
}

struct DeterministicRng {
    state: u64,
    spare_gaussian: Option<f32>,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed.max(1),
            spare_gaussian: None,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_unit_f64(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        (value as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    fn gaussian_f32(&mut self) -> f32 {
        if let Some(value) = self.spare_gaussian.take() {
            return value;
        }
        let u1 = self.next_unit_f64().clamp(f64::MIN_POSITIVE, 1.0);
        let u2 = self.next_unit_f64();
        let radius = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        let z0 = radius * theta.cos();
        let z1 = radius * theta.sin();
        self.spare_gaussian = Some(z1 as f32);
        z0 as f32
    }
}

fn read_fits_model_image(
    path: &PathBuf,
    model_peak_jy_per_pixel: Option<f32>,
) -> MsResult<FitsModelImage> {
    let mut file = fs::File::open(path).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to open model image {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to read model image {}: {error}",
            path.display()
        ))
    })?;
    let (cards, data_offset) = parse_fits_header(&bytes, path)?;
    let bitpix = fits_i64(&cards, "BITPIX", path)?;
    let naxis = fits_i64(&cards, "NAXIS", path)? as usize;
    if naxis < 2 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must have at least two FITS axes",
            path.display()
        )));
    }
    let nx = fits_i64(&cards, "NAXIS1", path)? as usize;
    let ny = fits_i64(&cards, "NAXIS2", path)? as usize;
    if nx < 8 || ny < 8 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must be at least 8x8 pixels",
            path.display()
        )));
    }
    let trailing_planes = (3..=naxis)
        .map(|axis| fits_i64(&cards, &format!("NAXIS{axis}"), path).unwrap_or(1))
        .product::<i64>();
    if trailing_planes != 1 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must have one Stokes/frequency plane for this Wave 5 slice",
            path.display()
        )));
    }
    let cell_size_rad = [
        fits_axis_cell_rad(&cards, 1, path)?.abs(),
        fits_axis_cell_rad(&cards, 2, path)?.abs(),
    ];
    let reference_direction_rad = fits_center_direction_rad(&cards, nx, ny, path)?;
    let pixel_count = nx
        .checked_mul(ny)
        .ok_or_else(|| MsError::SyntheticObservation("model image shape overflows".to_string()))?;
    let bytes_per_pixel = match bitpix {
        -32 => 4usize,
        -64 => 8usize,
        other => {
            return Err(MsError::SyntheticObservation(format!(
                "model image {} uses unsupported BITPIX={other}; expected -32 or -64",
                path.display()
            )));
        }
    };
    let data_len = pixel_count * bytes_per_pixel;
    if bytes.len() < data_offset + data_len {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} is truncated before primary image data",
            path.display()
        )));
    }

    let bscale = fits_optional_f64(&cards, "BSCALE").unwrap_or(1.0);
    let bzero = fits_optional_f64(&cards, "BZERO").unwrap_or(0.0);
    let mut pixels = Array2::<f32>::zeros((nx, ny));
    let data = &bytes[data_offset..data_offset + data_len];
    for y in 0..ny {
        for x in 0..nx {
            let index = y * nx + x;
            let raw = match bitpix {
                -32 => {
                    let start = index * 4;
                    f32::from_bits(u32::from_be_bytes([
                        data[start],
                        data[start + 1],
                        data[start + 2],
                        data[start + 3],
                    ])) as f64
                }
                -64 => {
                    let start = index * 8;
                    f64::from_bits(u64::from_be_bytes([
                        data[start],
                        data[start + 1],
                        data[start + 2],
                        data[start + 3],
                        data[start + 4],
                        data[start + 5],
                        data[start + 6],
                        data[start + 7],
                    ]))
                }
                _ => unreachable!(),
            };
            pixels[(x, y)] = (raw * bscale + bzero) as f32;
        }
    }
    if let Some(target_peak) = model_peak_jy_per_pixel {
        let current_peak = pixels
            .iter()
            .copied()
            .fold(0.0f32, |peak, value| peak.max(value.abs()));
        if current_peak <= 0.0 || !current_peak.is_finite() {
            return Err(MsError::SyntheticObservation(format!(
                "model image {} cannot be scaled because its peak brightness is zero or non-finite",
                path.display()
            )));
        }
        let scale = target_peak / current_peak;
        pixels.mapv_inplace(|value| value * scale);
    }

    Ok(FitsModelImage {
        pixels,
        cell_size_rad,
        reference_direction_rad,
    })
}

fn fits_center_direction_rad(
    cards: &[String],
    nx: usize,
    ny: usize,
    path: &Path,
) -> MsResult<Option<[f64; 2]>> {
    if !(fits_value(cards, "CRVAL1").is_some()
        && fits_value(cards, "CRVAL2").is_some()
        && fits_value(cards, "CTYPE1").is_some()
        && fits_value(cards, "CTYPE2").is_some())
    {
        return Ok(None);
    }
    let projection = fits_string(cards, "CTYPE1")
        .and_then(|ctype| {
            ctype
                .rsplit_once('-')
                .map(|(_, projection)| projection.to_string())
        })
        .and_then(|projection| ProjectionType::from_name(&projection))
        .unwrap_or(ProjectionType::SIN);
    let crval = [
        fits_axis_angle_rad(cards, "CRVAL1", path)?,
        fits_axis_angle_rad(cards, "CRVAL2", path)?,
    ];
    let cdelt = [
        fits_axis_cell_rad(cards, 1, path)?,
        fits_axis_cell_rad(cards, 2, path)?,
    ];
    let crpix = [
        fits_optional_f64(cards, "CRPIX1").unwrap_or(1.0) - 1.0,
        fits_optional_f64(cards, "CRPIX2").unwrap_or(1.0) - 1.0,
    ];
    let coordinate = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(projection),
        crval,
        cdelt,
        crpix,
    );
    let center_pixel = [0.5 * nx as f64, 0.5 * ny as f64];
    let world = coordinate.to_world(&center_pixel).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to resolve model-image center direction for {}: {error}",
            path.display()
        ))
    })?;
    Ok(Some([world[0], world[1]]))
}

fn parse_fits_header(bytes: &[u8], path: &Path) -> MsResult<(Vec<String>, usize)> {
    let mut cards = Vec::new();
    for (card_index, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        let card = std::str::from_utf8(chunk).map_err(|error| {
            MsError::SyntheticObservation(format!(
                "model image {} contains non-ASCII FITS header card: {error}",
                path.display()
            ))
        })?;
        cards.push(card.to_string());
        if card.starts_with("END") {
            let header_bytes = (card_index + 1) * 80;
            let data_offset = header_bytes.div_ceil(2880) * 2880;
            return Ok((cards, data_offset));
        }
    }
    Err(MsError::SyntheticObservation(format!(
        "model image {} has no FITS END header card",
        path.display()
    )))
}

fn fits_i64(cards: &[String], key: &str, path: &Path) -> MsResult<i64> {
    fits_value(cards, key)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| {
            MsError::SyntheticObservation(format!(
                "model image {} missing integer FITS key {key}",
                path.display()
            ))
        })
}

fn fits_axis_cell_rad(cards: &[String], axis: usize, path: &Path) -> MsResult<f64> {
    let value = fits_optional_f64(cards, &format!("CDELT{axis}")).ok_or_else(|| {
        MsError::SyntheticObservation(format!(
            "model image {} missing FITS CDELT{axis}",
            path.display()
        ))
    })?;
    let unit = fits_string(cards, &format!("CUNIT{axis}")).unwrap_or_else(|| "deg".to_string());
    match unit.trim().to_ascii_lowercase().as_str() {
        "deg" | "degree" | "degrees" => Ok(value.to_radians()),
        "rad" | "radian" | "radians" => Ok(value),
        other => Err(MsError::SyntheticObservation(format!(
            "model image {} uses unsupported CUNIT{axis}={other:?}; expected deg or rad",
            path.display()
        ))),
    }
}

fn fits_axis_angle_rad(cards: &[String], key: &str, path: &Path) -> MsResult<f64> {
    let value = fits_optional_f64(cards, key).ok_or_else(|| {
        MsError::SyntheticObservation(format!("model image {} missing FITS {key}", path.display()))
    })?;
    let axis = key
        .chars()
        .last()
        .and_then(|ch| ch.to_digit(10))
        .unwrap_or(1) as usize;
    let unit = fits_string(cards, &format!("CUNIT{axis}")).unwrap_or_else(|| "deg".to_string());
    match unit.trim().to_ascii_lowercase().as_str() {
        "deg" | "degree" | "degrees" => Ok(value.to_radians()),
        "rad" | "radian" | "radians" => Ok(value),
        other => Err(MsError::SyntheticObservation(format!(
            "model image {} uses unsupported CUNIT{axis}={other:?}; expected deg or rad",
            path.display()
        ))),
    }
}

fn fits_optional_f64(cards: &[String], key: &str) -> Option<f64> {
    fits_value(cards, key).and_then(|value| value.parse::<f64>().ok())
}

fn fits_string(cards: &[String], key: &str) -> Option<String> {
    fits_value(cards, key).map(|value| value.trim().trim_matches('\'').trim().to_string())
}

fn fits_value<'a>(cards: &'a [String], key: &str) -> Option<&'a str> {
    cards.iter().find_map(|card| {
        if card.get(..8)?.trim() != key {
            return None;
        }
        let value = card.get(10..)?;
        Some(value.split('/').next().unwrap_or(value).trim())
    })
}

fn subtable_mut(ms: &mut MeasurementSet, id: SubtableId) -> MsResult<&mut casa_tables::Table> {
    ms.subtable_mut(id)
        .ok_or_else(|| MsError::MissingSubtable(id.name().to_string()))
}

fn time_sample_count(duration_seconds: f64, integration_seconds: f64) -> usize {
    (duration_seconds / integration_seconds).ceil().max(1.0) as usize
}

fn antenna_uvw_positions(
    antennas: &[SyntheticAntenna],
    phase_center_rad: [f64; 2],
    time_mjd_seconds: f64,
) -> MsResult<Vec<[f64; 3]>> {
    let phase_center = MDirection::from_angles(
        phase_center_rad[0],
        phase_center_rad[1],
        DirectionRef::J2000,
    );
    let observatory = MPosition::from_observatory_name("VLA")
        .unwrap_or_else(|| MPosition::new_itrf(-1_601_192.0, -5_041_984.0, 3_554_876.0));
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(time_mjd_seconds / 86_400.0, EpochRef::UT1))
        .with_position(observatory.clone())
        .with_direction(phase_center.clone())
        .with_bundled_eop();

    antennas
        .iter()
        .map(|antenna| {
            let obs_itrf = observatory.as_itrf();
            let ant_itrf = antenna.position_m;
            let baseline = [
                obs_itrf[0] - ant_itrf[0],
                obs_itrf[1] - ant_itrf[1],
                obs_itrf[2] - ant_itrf[2],
            ];
            let baseline_j2000 = baseline_itrf_to_j2000(baseline, &phase_center, &frame)?;
            Ok(project_j2000_baseline_to_uvw(baseline_j2000, &phase_center))
        })
        .collect()
}

fn project_j2000_baseline_to_uvw(
    baseline_j2000_m: [f64; 3],
    phase_center: &MDirection,
) -> [f64; 3] {
    let (ra, dec) = phase_center.as_angles();
    let (sin_ra, cos_ra) = ra.sin_cos();
    let (sin_dec, cos_dec) = dec.sin_cos();
    let [x, y, z] = baseline_j2000_m;

    [
        -(sin_ra * x - cos_ra * y),
        -sin_dec * cos_ra * x - sin_dec * sin_ra * y + cos_dec * z,
        cos_dec * cos_ra * x + cos_dec * sin_ra * y + sin_dec * z,
    ]
}

fn baseline_itrf_to_j2000(
    baseline_itrf_m: [f64; 3],
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let baseline_len = vector_norm(baseline_itrf_m);
    if baseline_len == 0.0 {
        return Ok([0.0, 0.0, 0.0]);
    }

    let mut unit = scale_vector(baseline_itrf_m, 1.0 / baseline_len);
    unit = itrf_to_hadec(unit, frame)?;
    unit = hadec_to_topo(unit, phase_center, frame)?;
    unit = app_to_jnat(unit, phase_center, frame)?;
    unit = jnat_to_j2000(unit, phase_center, frame)?;
    Ok(scale_vector(unit, baseline_len))
}

fn itrf_to_hadec(vector: [f64; 3], frame: &MeasFrame) -> MsResult<[f64; 3]> {
    let position = frame.position().ok_or_else(|| {
        MsError::SyntheticObservation("UVW conversion missing observatory position".to_string())
    })?;
    let lon = position.longitude_rad();
    let (s, c) = lon.sin_cos();
    let negated_y = -vector[1];
    Ok([
        c * vector[0] - s * negated_y,
        s * vector[0] + c * negated_y,
        vector[2],
    ])
}

fn hadec_to_topo(
    vector: [f64; 3],
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let last = local_apparent_sidereal_time(frame)?;
    let tdb_mjd = epoch_mjd(frame, EpochRef::TDB)?;
    let (xp, yp) = polar_motion_rad(frame, tdb_mjd);
    let mut vector = rotate(
        &polar_motion_euler(-xp, -yp, last),
        &[vector[0], -vector[1], vector[2]],
    );

    let position = frame.position().ok_or_else(|| {
        MsError::SyntheticObservation("UVW conversion missing observatory position".to_string())
    })?;
    let radius = vector_norm(position.as_itrf());
    let v_c = diurnal_aberration_factor(radius);
    let aberration_direction = spherical_to_cartesian(last, position.geocentric_latitude_rad());
    let shift = scale_vector(aberration_direction, -v_c);
    let app_phase_center = phase_center
        .convert_to(DirectionRef::APP, frame)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "UVW phase-center APP conversion failed: {error}"
            ))
        })?;
    vector = rotate_shift(vector, shift, app_phase_center.cosines());
    Ok(vector)
}

fn app_to_jnat(
    vector: [f64; 3],
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let mut vector = deapply_precession_nutation(vector, frame)?;
    let shift = inverse_aberration_shift(phase_center, frame)?;
    vector = rotate_shift(vector, shift, phase_center.cosines());
    Ok(vector)
}

fn jnat_to_j2000(
    vector: [f64; 3],
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let shift = inverse_solar_deflection_shift(phase_center, frame)?;
    Ok(rotate_shift(vector, shift, phase_center.cosines()))
}

fn deapply_precession_nutation(vector: [f64; 3], frame: &MeasFrame) -> MsResult<[f64; 3]> {
    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let nutation = sofars::pnp::nutm80(tt.0, tt.1);
    let precession = sofars::pnp::pmat76(tt.0, tt.1);
    let vector = rotate_t(&nutation, &vector);
    Ok(rotate_t(&precession, &vector))
}

fn inverse_aberration_shift(phase_center: &MDirection, frame: &MeasFrame) -> MsResult<[f64; 3]> {
    const C_AU_PER_DAY: f64 = 173.144_632_674_240_34;

    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let (_, earth_bary) = sofars::eph::epv00(tt.0, tt.1).ok_or_else(|| {
        MsError::SyntheticObservation("UVW aberration ephemeris lookup failed".to_string())
    })?;
    let velocity = scale_vector(earth_bary[1], 1.0 / C_AU_PER_DAY);
    let speed_squared = dot(velocity, velocity);
    let beta_inv = (1.0 - speed_squared).sqrt();
    let natural = phase_center.cosines();
    let dot_nv = dot(natural, velocity);
    let apparent = scale_vector(
        add_vectors(
            scale_vector(natural, beta_inv),
            scale_vector(velocity, 1.0 + dot_nv / (1.0 + beta_inv)),
        ),
        1.0 / (1.0 + dot_nv),
    );
    Ok(subtract_vectors(natural, normalize(apparent)))
}

fn inverse_solar_deflection_shift(
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let (earth_helio, _) = sofars::eph::epv00(tt.0, tt.1).ok_or_else(|| {
        MsError::SyntheticObservation("UVW solar-position ephemeris lookup failed".to_string())
    })?;
    let sun = normalize([-earth_helio[0][0], -earth_helio[0][1], -earth_helio[0][2]]);
    let source = phase_center.cosines();
    let dot_source_sun = dot(source, sun);
    let correction = scale_vector(
        subtract_vectors(sun, scale_vector(source, dot_source_sun)),
        1.974e-8 / (1.0 - dot_source_sun),
    );
    Ok(correction)
}

fn local_apparent_sidereal_time(frame: &MeasFrame) -> MsResult<f64> {
    let (ut1_a, ut1_b) = epoch_jd_pair(frame, EpochRef::UT1)?;
    let gast = sofars::erst::gst94(ut1_a, ut1_b);
    let position = frame.position().ok_or_else(|| {
        MsError::SyntheticObservation("UVW conversion missing observatory position".to_string())
    })?;
    Ok(gast + position.longitude_rad())
}

fn epoch_jd_pair(frame: &MeasFrame, refer: EpochRef) -> MsResult<(f64, f64)> {
    let epoch = frame
        .epoch()
        .ok_or_else(|| MsError::SyntheticObservation("UVW conversion missing epoch".to_string()))?;
    let converted = epoch.convert_to(refer, frame).map_err(|error| {
        MsError::SyntheticObservation(format!("UVW epoch conversion failed: {error}"))
    })?;
    Ok(converted.value().as_jd_pair())
}

fn epoch_mjd(frame: &MeasFrame, refer: EpochRef) -> MsResult<f64> {
    let epoch = frame
        .epoch()
        .ok_or_else(|| MsError::SyntheticObservation("UVW conversion missing epoch".to_string()))?;
    let converted = epoch.convert_to(refer, frame).map_err(|error| {
        MsError::SyntheticObservation(format!("UVW epoch conversion failed: {error}"))
    })?;
    Ok(converted.value().as_mjd())
}

fn polar_motion_rad(frame: &MeasFrame, epoch_mjd: f64) -> (f64, f64) {
    const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
    match frame.polar_motion_for_mjd(epoch_mjd) {
        Some((xp_arcsec, yp_arcsec)) => (xp_arcsec * ARCSEC_TO_RAD, yp_arcsec * ARCSEC_TO_RAD),
        None => (0.0, 0.0),
    }
}

fn polar_motion_euler(xp: f64, yp: f64, last: f64) -> [[f64; 3]; 3] {
    let (sx, cx) = xp.sin_cos();
    let (sy, cy) = yp.sin_cos();
    let (sl, cl) = last.sin_cos();
    let rxz = [
        [cl, -sl, 0.0],
        [cy * sl, cy * cl, -sy],
        [sy * sl, sy * cl, cy],
    ];
    [
        [
            cx * rxz[0][0] + sx * rxz[2][0],
            cx * rxz[0][1] + sx * rxz[2][1],
            cx * rxz[0][2] + sx * rxz[2][2],
        ],
        rxz[1],
        [
            -sx * rxz[0][0] + cx * rxz[2][0],
            -sx * rxz[0][1] + cx * rxz[2][1],
            -sx * rxz[0][2] + cx * rxz[2][2],
        ],
    ]
}

fn diurnal_aberration_factor(radius_m: f64) -> f64 {
    const C: f64 = 299_792_458.0;
    const SIDEREAL_RATIO: f64 = 1.002_737_909_35;
    (2.0 * std::f64::consts::PI * radius_m) / 86_400.0 * SIDEREAL_RATIO / C
}

fn rotate_shift(vector: [f64; 3], shift: [f64; 3], reference_direction: [f64; 3]) -> [f64; 3] {
    let from = normalize(reference_direction);
    let to = normalize(add_vectors(from, shift));
    let axis = cross(from, to);
    let sin_angle = vector_norm(axis);
    if sin_angle < 1.0e-18 {
        return vector;
    }
    let axis = scale_vector(axis, 1.0 / sin_angle);
    let cos_angle = dot(from, to).clamp(-1.0, 1.0);
    let parallel = scale_vector(vector, cos_angle);
    let perpendicular = scale_vector(cross(axis, vector), sin_angle);
    let axial = scale_vector(axis, dot(axis, vector) * (1.0 - cos_angle));
    add_vectors(add_vectors(parallel, perpendicular), axial)
}

fn rotate(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn rotate_t(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[1][0] * vector[1] + matrix[2][0] * vector[2],
        matrix[0][1] * vector[0] + matrix[1][1] * vector[1] + matrix[2][1] * vector[2],
        matrix[0][2] * vector[0] + matrix[1][2] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn spherical_to_cartesian(lon: f64, lat: f64) -> [f64; 3] {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
}

fn vector_norm(vector: [f64; 3]) -> f64 {
    dot(vector, vector).sqrt()
}

fn normalize(vector: [f64; 3]) -> [f64; 3] {
    let norm = vector_norm(vector);
    if norm == 0.0 {
        vector
    } else {
        scale_vector(vector, 1.0 / norm)
    }
}

fn dot(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn cross(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn add_vectors(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn subtract_vectors(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] - right[0], left[1] - right[1], left[2] - right[2]]
}

fn scale_vector(vector: [f64; 3], scale: f64) -> [f64; 3] {
    [vector[0] * scale, vector[1] * scale, vector[2] * scale]
}

fn ms_main_defs(ms: &MeasurementSet) -> Vec<ColumnDef> {
    let all_defs = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .copied()
        .collect::<Vec<_>>();
    ms.main_table()
        .schema()
        .expect("main table schema")
        .columns()
        .iter()
        .map(|column| {
            *all_defs
                .iter()
                .find(|definition| definition.name == column.name())
                .expect("known MS main column")
        })
        .collect()
}

fn row_from_defs(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields = defs
        .iter()
        .map(|definition| {
            if let Some((_, value)) = overrides.iter().find(|(name, _)| *name == definition.name) {
                RecordField::new(definition.name, value.clone())
            } else {
                RecordField::new(definition.name, default_value(definition))
            }
        })
        .collect();
    RecordValue::new(fields)
}

fn default_value(definition: &ColumnDef) -> Value {
    match definition.column_kind {
        ColumnKind::Scalar => match definition.data_type {
            casa_types::PrimitiveType::Bool => b(false),
            casa_types::PrimitiveType::Int32 => i(0),
            casa_types::PrimitiveType::Float64 => f(0.0),
            casa_types::PrimitiveType::String => s(""),
            casa_types::PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            casa_types::PrimitiveType::Complex32 => {
                Value::Scalar(ScalarValue::Complex32(Complex32::new(0.0, 0.0)))
            }
            _ => i(0),
        },
        ColumnKind::FixedArray { shape } => {
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape.to_vec()),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape.to_vec()),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape.to_vec()),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape.to_vec()),
                _ => f64_array(&vec![0.0; total], shape.to_vec()),
            }
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape),
                casa_types::PrimitiveType::Complex32 => {
                    complex_array(&vec![Complex32::new(0.0, 0.0); total], shape)
                }
                _ => f64_array(&vec![0.0; total], shape),
            }
        }
    }
}

fn s(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn i(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn f(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn b(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

fn bool_array(values: &[bool], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Bool(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn i32_array(values: &[i32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn f32_array(values: &[f32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn f64_array(values: &[f64], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn complex_array(values: &[Complex32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn string_array(values: &[&str], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(
            shape,
            values.iter().map(|value| value.to_string()).collect(),
        )
        .unwrap(),
    ))
}
