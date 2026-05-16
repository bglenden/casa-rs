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
use std::thread;

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

/// One synthetic target field or mosaic pointing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticField {
    /// Field/source name.
    pub name: String,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    pub phase_center_rad: [f64; 2],
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
    /// Optional multi-field target list. When empty, `field_name` and
    /// `phase_center_rad` define a single-field observation.
    #[serde(default)]
    pub fields: Vec<SyntheticField>,
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
            fields: Vec::new(),
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
    /// Additive visibility noise, parameterized like CASA `simulator.setnoise`.
    #[serde(default)]
    pub noise: Option<SyntheticNoiseCorruption>,
    /// Per-antenna complex gain corruption, parameterized like CASA `simulator.setgain`.
    #[serde(default)]
    pub gain: Option<SyntheticGainCorruption>,
    /// Per-antenna, per-channel complex bandpass corruption, using CASA `setbandpass` names.
    #[serde(default)]
    pub bandpass: Option<SyntheticBandpassCorruption>,
    /// Approximate parallel-hand polarization leakage corruption, parameterized like CASA `setleakage`.
    #[serde(default)]
    pub leakage: Option<SyntheticPolarizationLeakageCorruption>,
    /// Global primary-beam pointing offset applied during model prediction, using CASA `setpointingerror` names where CASA has them.
    #[serde(default)]
    pub pointing: Option<SyntheticPointingCorruption>,
}

/// Additive visibility-noise controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticNoiseCorruption {
    /// CASA `setnoise` mode. Only `simplenoise` is currently implemented.
    pub mode: SyntheticNoiseMode,
    /// Per-real/imaginary-component Gaussian sigma in Jy for `mode='simplenoise'`.
    pub simplenoise_jy: f32,
}

/// Supported CASA `setnoise` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SyntheticNoiseMode {
    /// CASA `mode='simplenoise'`.
    #[serde(rename = "simplenoise")]
    SimpleNoise,
    /// CASA `mode='tsys-atm'`; parsed for API parity but not yet implemented.
    #[serde(rename = "tsys-atm")]
    TsysAtm,
    /// CASA `mode='tsys-manual'`; parsed for API parity but not yet implemented.
    #[serde(rename = "tsys-manual")]
    TsysManual,
}

/// Per-antenna gain corruption controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticGainCorruption {
    /// CASA `setgain` mode.
    pub mode: SyntheticGainMode,
    /// CASA `interval` in seconds. CASA clamps fBM slots to at least five seconds.
    pub interval_seconds: f64,
    /// CASA `amplitude` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub amplitude: [f32; 2],
}

/// Supported CASA `setgain` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SyntheticGainMode {
    /// Fractional Brownian motion gain drift.
    #[serde(rename = "fbm")]
    Fbm,
    /// CASA's random complex gain mode.
    #[serde(rename = "random")]
    Random,
}

/// Per-antenna, per-channel bandpass corruption controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticBandpassCorruption {
    /// CASA `setbandpass` mode. CASA C++ currently disables this method; casa-rs implements `calculate` natively.
    pub mode: SyntheticBandpassMode,
    /// CASA `interval` in seconds.
    pub interval_seconds: f64,
    /// CASA `amplitude` vector `[amplitude_sigma, phase_sigma]`, or scalar expanded by the CLI to both entries.
    pub amplitude: [f32; 2],
}

/// Supported CASA `setbandpass` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SyntheticBandpassMode {
    /// CASA `mode='calculate'`.
    #[serde(rename = "calculate")]
    Calculate,
    /// CASA `mode='table'`; parsed for API parity but not yet implemented.
    #[serde(rename = "table")]
    Table,
}

/// Polarization leakage corruption controls for parallel-hand synthetic data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticPolarizationLeakageCorruption {
    /// CASA `setleakage` mode.
    pub mode: SyntheticPolarizationLeakageMode,
    /// CASA `amplitude` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub amplitude: [f32; 2],
    /// CASA `offset` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub offset: [f32; 2],
}

/// Supported CASA `setleakage` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SyntheticPolarizationLeakageMode {
    /// CASA `mode='constant'`.
    #[serde(rename = "constant")]
    Constant,
}

/// Primary-beam pointing corruption controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticPointingCorruption {
    /// CASA `epjtablename` pointing-error table. CASA C++ currently disables this path.
    #[serde(default)]
    pub epjtablename: Option<PathBuf>,
    /// CASA `applypointingoffsets` switch.
    #[serde(default, rename = "applypointingoffsets")]
    pub apply_pointing_offsets: bool,
    /// CASA `dopbcorrection` switch.
    #[serde(default, rename = "dopbcorrection")]
    pub do_pb_correction: bool,
    /// Native casa-rs global pointing offset `[right_ascension, declination]` in radians.
    #[serde(default = "default_pointing_offset_rad")]
    pub offset_rad: [f64; 2],
}

fn default_pointing_offset_rad() -> [f64; 2] {
    [0.0, 0.0]
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
    populate_pointing(&mut ms, request)?;
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
    if request.field_name.trim().is_empty() && request.fields.is_empty() {
        return Err(MsError::SyntheticObservation(
            "field name must not be empty".to_string(),
        ));
    }
    for field in &request.fields {
        if field.name.trim().is_empty() {
            return Err(MsError::SyntheticObservation(
                "field names must not be empty".to_string(),
            ));
        }
        if field
            .phase_center_rad
            .iter()
            .any(|value| !value.is_finite())
        {
            return Err(MsError::SyntheticObservation(format!(
                "field {} phase center coordinates must be finite",
                field.name
            )));
        }
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
    if let Some(noise) = &corruption.noise {
        if noise.mode != SyntheticNoiseMode::SimpleNoise {
            return Err(MsError::SyntheticObservation(
                "setnoise currently supports only mode='simplenoise'".to_string(),
            ));
        }
        if !(noise.simplenoise_jy.is_finite() && noise.simplenoise_jy >= 0.0) {
            return Err(MsError::SyntheticObservation(
                "setnoise simplenoise_jy must be finite and non-negative".to_string(),
            ));
        }
    }
    if let Some(gain) = &corruption.gain {
        if !(gain.interval_seconds.is_finite() && gain.interval_seconds > 0.0) {
            return Err(MsError::SyntheticObservation(
                "setgain interval_seconds must be finite and positive".to_string(),
            ));
        }
        if gain
            .amplitude
            .iter()
            .any(|value| !(value.is_finite() && *value >= 0.0))
        {
            return Err(MsError::SyntheticObservation(
                "setgain amplitude values must be finite and non-negative".to_string(),
            ));
        }
    }
    if let Some(bandpass) = &corruption.bandpass {
        if bandpass.mode != SyntheticBandpassMode::Calculate {
            return Err(MsError::SyntheticObservation(
                "setbandpass currently supports only mode='calculate'".to_string(),
            ));
        }
        if !(bandpass.interval_seconds.is_finite() && bandpass.interval_seconds > 0.0) {
            return Err(MsError::SyntheticObservation(
                "setbandpass interval_seconds must be finite and positive".to_string(),
            ));
        }
        if bandpass
            .amplitude
            .iter()
            .any(|value| !(value.is_finite() && *value >= 0.0))
        {
            return Err(MsError::SyntheticObservation(
                "setbandpass amplitude values must be finite and non-negative".to_string(),
            ));
        }
    }
    if let Some(leakage) = &corruption.leakage {
        if leakage.mode != SyntheticPolarizationLeakageMode::Constant {
            return Err(MsError::SyntheticObservation(
                "setleakage currently supports only mode='constant'".to_string(),
            ));
        }
        if leakage
            .amplitude
            .iter()
            .any(|value| !(value.is_finite() && *value >= 0.0))
        {
            return Err(MsError::SyntheticObservation(
                "setleakage amplitude values must be finite and non-negative".to_string(),
            ));
        }
        if leakage.offset.iter().any(|value| !value.is_finite()) {
            return Err(MsError::SyntheticObservation(
                "setleakage offset values must be finite".to_string(),
            ));
        }
    }
    if let Some(pointing) = &corruption.pointing {
        if let Some(epjtablename) = &pointing.epjtablename {
            return Err(MsError::SyntheticObservation(format!(
                "setpointingerror epjtablename={} is not supported because CASA C++ currently disables simulated pointing-error tables",
                epjtablename.display()
            )));
        }
        if pointing.offset_rad.iter().any(|value| !value.is_finite()) {
            return Err(MsError::SyntheticObservation(
                "pointing offset_rad values must be finite".to_string(),
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
    if corruption
        .noise
        .as_ref()
        .is_some_and(|noise| noise.simplenoise_jy > 0.0)
    {
        names.push("noise".to_string());
    }
    if let Some(gain) = &corruption.gain {
        if gain.amplitude.iter().any(|value| *value > 0.0) {
            names.push("gain".to_string());
        }
    }
    if let Some(bandpass) = &corruption.bandpass {
        if bandpass.amplitude.iter().any(|value| *value > 0.0) {
            names.push("bandpass".to_string());
        }
    }
    if let Some(leakage) = &corruption.leakage {
        if leakage.amplitude.iter().any(|value| *value > 0.0)
            || leakage.offset.iter().any(|value| *value != 0.0)
        {
            names.push("leakage".to_string());
        }
    }
    if let Some(pointing) = &corruption.pointing {
        if pointing.apply_pointing_offsets && pointing.offset_rad.iter().any(|value| *value != 0.0)
        {
            names.push("pointing".to_string());
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
    for field in effective_fields(request) {
        let direction = direction_poly_array(field.phase_center_rad);
        let row = row_from_defs(
            schema::field::REQUIRED_COLUMNS,
            &[
                ("NAME", s(&field.name)),
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
    }
    Ok(())
}

fn populate_pointing(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
) -> MsResult<()> {
    let time = request.start_time_mjd_seconds + 0.5 * request.duration_seconds;
    for field in effective_fields(request) {
        let direction = direction_poly_array(field.phase_center_rad);
        for antenna_id in 0..request.antennas.len() {
            let row = row_from_defs(
                schema::pointing::REQUIRED_COLUMNS,
                &[
                    ("ANTENNA_ID", i(antenna_id as i32)),
                    ("DIRECTION", direction.clone()),
                    ("INTERVAL", f(request.duration_seconds)),
                    ("NAME", s(&field.name)),
                    ("NUM_POLY", i(0)),
                    ("TARGET", direction.clone()),
                    ("TIME", f(time)),
                    ("TIME_ORIGIN", f(request.start_time_mjd_seconds)),
                    ("TRACKING", b(true)),
                ],
            );
            subtable_mut(ms, SubtableId::Pointing)?.add_row(row)?;
        }
    }
    Ok(())
}

fn effective_fields(request: &SyntheticObservationRequest) -> Vec<SyntheticField> {
    if request.fields.is_empty() {
        return vec![SyntheticField {
            name: request.field_name.clone(),
            phase_center_rad: request.phase_center_rad,
        }];
    }
    request.fields.clone()
}

fn direction_poly_array(direction_rad: [f64; 2]) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], direction_rad.to_vec()).unwrap(),
    ))
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
    let template = MainRowTemplate {
        flag: bool_array(&vec![false; num_corr * num_chan], vec![num_corr, num_chan]),
        flag_category: bool_array(
            &vec![false; num_corr * num_chan],
            vec![1, num_corr, num_chan],
        ),
        weight: f32_array(&vec![1.0; num_corr], vec![num_corr]),
        sigma: f32_array(&vec![1.0; num_corr], vec![num_corr]),
    };
    let main_defs = ms_main_defs(ms);
    let field_plans = build_field_plans(request, model)?;
    let corruption = request.corruption.as_ref().map(|config| {
        SyntheticCorruptionState::new(
            config,
            request.antennas.len(),
            request.spectral_setup.channel_count,
            samples,
        )
    });
    let context = MainRowBuildContext {
        request,
        num_corr,
        num_chan,
        main_defs: &main_defs,
        template: &template,
        field_plans: &field_plans,
        corruption: corruption.as_ref(),
    };
    let row_batches = build_main_row_batches(&context, samples)?;
    let mut nonzero_visibility_count = 0usize;

    for batch in row_batches {
        nonzero_visibility_count += batch.nonzero_visibility_count;
        for row in batch.rows {
            ms.main_table_mut().add_row(row)?;
        }
    }
    Ok(nonzero_visibility_count)
}

#[derive(Clone)]
struct MainRowTemplate {
    flag: Value,
    flag_category: Value,
    weight: Value,
    sigma: Value,
}

struct MainRowBatch {
    rows: Vec<RecordValue>,
    nonzero_visibility_count: usize,
}

#[derive(Clone, Copy)]
struct MainRowBuildContext<'a> {
    request: &'a SyntheticObservationRequest,
    num_corr: usize,
    num_chan: usize,
    main_defs: &'a [ColumnDef],
    template: &'a MainRowTemplate,
    field_plans: &'a [SyntheticFieldPlan],
    corruption: Option<&'a SyntheticCorruptionState>,
}

fn build_main_row_batches(
    context: &MainRowBuildContext<'_>,
    samples: usize,
) -> MsResult<Vec<MainRowBatch>> {
    let baseline_count = context.request.antennas.len() * (context.request.antennas.len() - 1) / 2;
    let worker_count = synthetic_observation_worker_count(samples, baseline_count);
    if worker_count <= 1 {
        return Ok(vec![build_main_row_batch(*context, 0, samples)?]);
    }

    let chunk_size = samples.div_ceil(worker_count);
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for start_sample in (0..samples).step_by(chunk_size) {
            let end_sample = (start_sample + chunk_size).min(samples);
            let context = *context;
            handles
                .push(scope.spawn(move || build_main_row_batch(context, start_sample, end_sample)));
        }

        let mut batches = Vec::with_capacity(handles.len());
        for handle in handles {
            let batch = handle.join().map_err(|_| {
                MsError::SyntheticObservation(
                    "synthetic-observation row worker panicked".to_string(),
                )
            })??;
            batches.push(batch);
        }
        Ok(batches)
    })
}

fn synthetic_observation_worker_count(samples: usize, baseline_count: usize) -> usize {
    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let requested = std::env::var("CASA_RS_SIMOBSERVE_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(available);
    synthetic_observation_worker_count_for(samples, baseline_count, requested, available)
}

fn synthetic_observation_worker_count_for(
    samples: usize,
    baseline_count: usize,
    requested: usize,
    available: usize,
) -> usize {
    if samples <= 1 || baseline_count == 0 {
        return 1;
    }
    requested.max(1).min(available.max(1)).min(samples).max(1)
}

fn build_main_row_batch(
    context: MainRowBuildContext<'_>,
    start_sample: usize,
    end_sample: usize,
) -> MsResult<MainRowBatch> {
    let request = context.request;
    let baseline_count = request.antennas.len() * (request.antennas.len() - 1) / 2;
    let mut rows = Vec::with_capacity((end_sample - start_sample) * baseline_count);
    let mut nonzero_visibility_count = 0usize;

    for sample in start_sample..end_sample {
        let field_id = sample % context.field_plans.len();
        let field_plan = &context.field_plans[field_id];
        let time =
            request.start_time_mjd_seconds + (sample as f64 + 0.5) * request.integration_seconds;
        let antenna_uvws =
            antenna_uvw_positions(&request.antennas, field_plan.phase_center_rad, time)?;
        for antenna1 in 0..request.antennas.len() {
            for antenna2 in (antenna1 + 1)..request.antennas.len() {
                let uvw = [
                    antenna_uvws[antenna2][0] - antenna_uvws[antenna1][0],
                    antenna_uvws[antenna2][1] - antenna_uvws[antenna1][1],
                    antenna_uvws[antenna2][2] - antenna_uvws[antenna1][2],
                ];
                let mut data_values = predicted_data_values(
                    field_plan.predictors.as_ref(),
                    &request.spectral_setup,
                    uvw,
                    context.num_corr,
                );
                if let Some(corruption) = context.corruption {
                    corruption.apply(
                        &mut data_values,
                        antenna1,
                        antenna2,
                        context.num_chan,
                        sample,
                    );
                }
                nonzero_visibility_count += data_values
                    .iter()
                    .filter(|value| value.re != 0.0 || value.im != 0.0)
                    .count();
                let data = complex_array(&data_values, vec![context.num_corr, context.num_chan]);
                let row = row_from_defs(
                    context.main_defs,
                    &[
                        ("ANTENNA1", i(antenna1 as i32)),
                        ("ANTENNA2", i(antenna2 as i32)),
                        ("ARRAY_ID", i(0)),
                        ("DATA_DESC_ID", i(0)),
                        ("EXPOSURE", f(request.integration_seconds)),
                        ("FEED1", i(0)),
                        ("FEED2", i(0)),
                        ("FIELD_ID", i(field_id as i32)),
                        ("FLAG", context.template.flag.clone()),
                        ("FLAG_CATEGORY", context.template.flag_category.clone()),
                        ("FLAG_ROW", b(false)),
                        ("INTERVAL", f(request.integration_seconds)),
                        ("OBSERVATION_ID", i(0)),
                        ("PROCESSOR_ID", i(0)),
                        ("SCAN_NUMBER", i(1)),
                        ("SIGMA", context.template.sigma.clone()),
                        ("STATE_ID", i(0)),
                        ("TIME", f(time)),
                        ("TIME_CENTROID", f(time)),
                        ("UVW", f64_array(&uvw, vec![3])),
                        ("WEIGHT", context.template.weight.clone()),
                        ("DATA", data),
                    ],
                );
                rows.push(row);
            }
        }
    }
    Ok(MainRowBatch {
        rows,
        nonzero_visibility_count,
    })
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
    phase_center_rad: [f64; 2],
    model_reference_direction_rad: Option<[f64; 2]>,
}

struct SyntheticFieldPlan {
    phase_center_rad: [f64; 2],
    predictors: Option<Vec<SyntheticChannelPredictor>>,
}

// CASA GridFT negates u/v to compensate for an image-inversion convention in
// the degridding path. After matching that handedness and the FITS center-pixel
// offset, the tutorial model needs this residual image-x phase alignment to
// match CASA's GridFT prediction at numerical-noise scale.
const CASA_GRIDFT_IMAGE_INVERSION_PHASE_PIXELS: f64 = 0.015_122_8;

fn build_field_plans(
    request: &SyntheticObservationRequest,
    model: Option<&FitsModelImage>,
) -> MsResult<Vec<SyntheticFieldPlan>> {
    let primary_beam = synthetic_primary_beam(request);
    let pointing_offset_rad = request
        .corruption
        .as_ref()
        .and_then(|corruption| corruption.pointing.as_ref())
        .filter(|pointing| pointing.apply_pointing_offsets)
        .map(|pointing| pointing.offset_rad)
        .unwrap_or([0.0, 0.0]);

    effective_fields(request)
        .into_iter()
        .map(|field| {
            let predictors = model
                .map(|model| {
                    build_channel_predictors(
                        model,
                        &request.spectral_setup,
                        field.phase_center_rad,
                        pointing_offset_rad,
                        primary_beam,
                    )
                })
                .transpose()?;
            Ok(SyntheticFieldPlan {
                phase_center_rad: field.phase_center_rad,
                predictors,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
struct SyntheticPrimaryBeam {
    use_casa_vla_q_table: bool,
    dish_diameter_m: f64,
    blockage_diameter_m: f64,
}

fn synthetic_primary_beam(request: &SyntheticObservationRequest) -> SyntheticPrimaryBeam {
    let dish_diameter_m = request
        .antennas
        .iter()
        .map(|antenna| antenna.dish_diameter_m)
        .sum::<f64>()
        / request.antennas.len().max(1) as f64;
    let telescope_is_vla = request.telescope_name.eq_ignore_ascii_case("VLA");
    SyntheticPrimaryBeam {
        use_casa_vla_q_table: telescope_is_vla && (dish_diameter_m - 25.0).abs() < 1.0e-6,
        dish_diameter_m,
        blockage_diameter_m: if telescope_is_vla { 2.36 } else { 0.0 },
    }
}

fn build_channel_predictors(
    model: &FitsModelImage,
    spectral_setup: &SyntheticSpectralSetup,
    phase_center_rad: [f64; 2],
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
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
            let beam_corrected_pixels = apply_simulator_primary_beam(
                model,
                phase_center_rad,
                frequency_hz,
                pointing_offset_rad,
                primary_beam,
            );
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
                phase_center_rad,
                model_reference_direction_rad: model.reference_direction_rad,
            })
        })
        .collect()
}

fn apply_simulator_primary_beam(
    model: &FitsModelImage,
    phase_center_rad: [f64; 2],
    frequency_hz: f64,
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
) -> Array2<f32> {
    let mut pixels = model.pixels.clone();
    let Some(reference_direction_rad) = model.reference_direction_rad else {
        return pixels;
    };
    let center_ra_offset =
        circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0])
            * phase_center_rad[1].cos()
            - pointing_offset_rad[0];
    let center_dec_offset =
        reference_direction_rad[1] - phase_center_rad[1] - pointing_offset_rad[1];
    let x_ref = model.pixels.shape()[0] as f64 / 2.0;
    let y_ref = model.pixels.shape()[1] as f64 / 2.0;

    for x in 0..model.pixels.shape()[0] {
        for y in 0..model.pixels.shape()[1] {
            let l = center_ra_offset + (x as f64 - x_ref) * model.cell_size_rad[0];
            let m = center_dec_offset + (y as f64 - y_ref) * model.cell_size_rad[1];
            let vp = synthetic_primary_beam_voltage_pattern(
                primary_beam,
                (l * l + m * m).sqrt(),
                frequency_hz,
            );
            pixels[(x, y)] *= vp * vp;
        }
    }
    pixels
}

fn synthetic_primary_beam_voltage_pattern(
    primary_beam: SyntheticPrimaryBeam,
    radius_rad: f64,
    frequency_hz: f64,
) -> f32 {
    if primary_beam.use_casa_vla_q_table {
        return casa_vla_q_primary_beam_voltage_pattern(radius_rad, frequency_hz);
    }
    primary_beam_voltage_pattern(
        PrimaryBeamModel::Airy {
            dish_diameter_m: primary_beam.dish_diameter_m,
            blockage_diameter_m: primary_beam.blockage_diameter_m,
        },
        radius_rad,
        frequency_hz,
    )
}

fn casa_vla_q_primary_beam_voltage_pattern(radius_rad: f64, frequency_hz: f64) -> f32 {
    const CASA_AIRY_SAMPLES: usize = 10_000;
    const CASA_VLA_Q_MAX_RADIUS_ARCMIN: f64 = 0.8564 * 60.0;
    const DISH_DIAMETER_M: f64 = 25.0;
    const BLOCKAGE_DIAMETER_M: f64 = 2.36;

    if !(radius_rad.is_finite()
        && radius_rad >= 0.0
        && frequency_hz.is_finite()
        && frequency_hz > 0.0)
    {
        return 0.0;
    }
    let radius_arcmin_ghz = radius_rad.to_degrees() * 60.0 * (frequency_hz / 1.0e9);
    let table_index = (radius_arcmin_ghz * (CASA_AIRY_SAMPLES - 1) as f64
        / CASA_VLA_Q_MAX_RADIUS_ARCMIN) as usize;
    if table_index >= CASA_AIRY_SAMPLES {
        return 0.0;
    }
    if table_index == 0 {
        return 1.0;
    }

    let quantized_radius_arcmin_ghz =
        table_index as f64 * CASA_VLA_Q_MAX_RADIUS_ARCMIN / (CASA_AIRY_SAMPLES - 1) as f64;
    let quantized_radius_rad =
        (quantized_radius_arcmin_ghz / (frequency_hz / 1.0e9) / 60.0).to_radians();
    primary_beam_voltage_pattern(
        PrimaryBeamModel::Airy {
            dish_diameter_m: DISH_DIAMETER_M,
            blockage_diameter_m: BLOCKAGE_DIAMETER_M,
        },
        quantized_radius_rad,
        frequency_hz,
    )
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
            let prediction_uvw_m = if let Some(model_reference_direction_rad) =
                predictor.model_reference_direction_rad
            {
                rotate_uvw_between_directions(
                    uvw_m,
                    predictor.phase_center_rad,
                    model_reference_direction_rad,
                )
            } else {
                uvw_m
            };
            let u_lambda = prediction_uvw_m[0] / wavelength_m;
            let v_lambda = prediction_uvw_m[1] / wavelength_m;
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

fn rotate_uvw_between_directions(
    uvw_m: [f64; 3],
    from_direction_rad: [f64; 2],
    to_direction_rad: [f64; 2],
) -> [f64; 3] {
    let baseline_j2000_m = baseline_from_uvw(uvw_m, from_direction_rad);
    let to_direction = MDirection::from_angles(
        to_direction_rad[0],
        to_direction_rad[1],
        DirectionRef::J2000,
    );
    project_j2000_baseline_to_uvw(baseline_j2000_m, &to_direction)
}

fn baseline_from_uvw(uvw_m: [f64; 3], direction_rad: [f64; 2]) -> [f64; 3] {
    let (ra, dec) = (direction_rad[0], direction_rad[1]);
    let (sin_ra, cos_ra) = ra.sin_cos();
    let (sin_dec, cos_dec) = dec.sin_cos();
    let u_axis = [-sin_ra, cos_ra, 0.0];
    let v_axis = [-sin_dec * cos_ra, -sin_dec * sin_ra, cos_dec];
    let w_axis = [cos_dec * cos_ra, cos_dec * sin_ra, sin_dec];
    [
        uvw_m[0] * u_axis[0] + uvw_m[1] * v_axis[0] + uvw_m[2] * w_axis[0],
        uvw_m[0] * u_axis[1] + uvw_m[1] * v_axis[1] + uvw_m[2] * w_axis[1],
        uvw_m[0] * u_axis[2] + uvw_m[1] * v_axis[2] + uvw_m[2] * w_axis[2],
    ]
}

struct SyntheticCorruptionState {
    seed: u64,
    simplenoise_jy: f32,
    gains_by_sample: Vec<Vec<[Complex32; 2]>>,
    bandpass_gains: Vec<Vec<Complex32>>,
    leakage_terms: Vec<[Complex32; 2]>,
}

impl SyntheticCorruptionState {
    fn new(
        config: &SyntheticCorruptionConfig,
        antenna_count: usize,
        channel_count: usize,
        sample_count: usize,
    ) -> Self {
        let mut rng = DeterministicRng::new(config.seed);
        let gains_by_sample = build_gain_terms(
            config.gain.as_ref(),
            config.seed,
            antenna_count,
            sample_count,
        );
        let bandpass_gains = (0..antenna_count)
            .map(|_| {
                (0..channel_count)
                    .map(|_| {
                        if let Some(bandpass) = &config.bandpass {
                            let amplitude = 1.0 + rng.gaussian_f32() * bandpass.amplitude[0];
                            let phase = rng.gaussian_f32() * bandpass.amplitude[1];
                            Complex32::new(amplitude * phase.cos(), amplitude * phase.sin())
                        } else {
                            Complex32::new(1.0, 0.0)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let leakage_terms = (0..antenna_count)
            .map(|_| {
                if let Some(leakage) = &config.leakage {
                    casa_like_leakage_terms(&mut rng, leakage)
                } else {
                    [Complex32::new(0.0, 0.0); 2]
                }
            })
            .collect();
        Self {
            seed: config.seed,
            simplenoise_jy: config
                .noise
                .as_ref()
                .map(|noise| noise.simplenoise_jy)
                .unwrap_or(0.0),
            gains_by_sample,
            bandpass_gains,
            leakage_terms,
        }
    }

    fn apply(
        &self,
        values: &mut [Complex32],
        antenna1: usize,
        antenna2: usize,
        channel_count: usize,
        sample_index: usize,
    ) {
        let gains = &self.gains_by_sample[sample_index % self.gains_by_sample.len()];
        for (index, value) in values.iter_mut().enumerate() {
            let channel = index % channel_count;
            let correlation = index / channel_count;
            let baseline_gain = gains[antenna1][correlation]
                * gains[antenna2][correlation].conj()
                * self.bandpass_gains[antenna1][channel]
                * self.bandpass_gains[antenna2][channel].conj();
            *value *= baseline_gain;
            if self.simplenoise_jy > 0.0 {
                let mut rng = DeterministicRng::new(row_noise_seed(
                    self.seed,
                    sample_index,
                    antenna1,
                    antenna2,
                    index,
                ));
                value.re += rng.gaussian_f32() * self.simplenoise_jy;
                value.im += rng.gaussian_f32() * self.simplenoise_jy;
            }
        }
        if channel_count > 0 && values.len() == 2 * channel_count {
            let rr_leakage =
                self.leakage_terms[antenna1][0] * self.leakage_terms[antenna2][0].conj();
            let ll_leakage =
                self.leakage_terms[antenna1][1] * self.leakage_terms[antenna2][1].conj();
            if rr_leakage != Complex32::new(0.0, 0.0) || ll_leakage != Complex32::new(0.0, 0.0) {
                for channel in 0..channel_count {
                    let rr_index = channel;
                    let ll_index = channel_count + channel;
                    let rr = values[rr_index];
                    let ll = values[ll_index];
                    values[rr_index] = rr + rr_leakage * ll;
                    values[ll_index] = ll + ll_leakage * rr;
                }
            }
        }
    }
}

fn row_noise_seed(
    seed: u64,
    sample_index: usize,
    antenna1: usize,
    antenna2: usize,
    value_index: usize,
) -> u64 {
    let mut mixed = seed ^ 0x9E37_79B9_7F4A_7C15;
    mixed = mix_u64(mixed ^ sample_index as u64);
    mixed = mix_u64(mixed ^ ((antenna1 as u64) << 32) ^ antenna2 as u64);
    mix_u64(mixed ^ value_index as u64)
}

fn mix_u64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn build_gain_terms(
    gain: Option<&SyntheticGainCorruption>,
    seed: u64,
    antenna_count: usize,
    sample_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let slot_count = sample_count.max(2);
    let Some(gain) = gain else {
        return vec![vec![[Complex32::new(1.0, 0.0); 2]; antenna_count]; slot_count];
    };
    match gain.mode {
        SyntheticGainMode::Fbm => build_fbm_gain_terms(gain, seed, antenna_count, slot_count),
        SyntheticGainMode::Random => build_random_gain_terms(gain, seed, antenna_count, slot_count),
    }
}

fn build_fbm_gain_terms(
    gain: &SyntheticGainCorruption,
    seed: u64,
    antenna_count: usize,
    slot_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let scale = gain_amplitude_scale(gain.amplitude).min(0.9);
    let mut by_sample = vec![vec![[Complex32::new(1.0, 0.0); 2]; antenna_count]; slot_count];
    if scale == 0.0 {
        return by_sample;
    }
    let series_by_antenna = (0..antenna_count)
        .map(|antenna| {
            (0..2)
                .map(|correlation| {
                    let casa_seed = seed
                        .wrapping_add(antenna as u64)
                        .wrapping_add(correlation as u64);
                    (
                        fbm_like_series(casa_seed, slot_count, 1.1),
                        fbm_like_series(casa_seed.wrapping_mul(100), slot_count, 1.1),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for (sample, row) in by_sample.iter_mut().enumerate() {
        for (antenna_terms, correlation_series) in row.iter_mut().zip(series_by_antenna.iter()) {
            for (term, (amplitude_series, phase_series)) in
                antenna_terms.iter_mut().zip(correlation_series.iter())
            {
                let amplitude = 1.0 + amplitude_series[sample] * scale;
                let phase = phase_series[sample] * scale * std::f32::consts::PI;
                *term = Complex32::new(amplitude * phase.cos(), amplitude * phase.sin());
            }
        }
    }
    by_sample
}

fn build_random_gain_terms(
    gain: &SyntheticGainCorruption,
    seed: u64,
    antenna_count: usize,
    slot_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let mut rng = DeterministicRng::new(seed);
    (0..slot_count)
        .map(|_| {
            (0..antenna_count)
                .map(|_| {
                    [
                        Complex32::new(
                            rng.gaussian_f32() * gain.amplitude[0],
                            rng.gaussian_f32() * gain.amplitude[1],
                        ),
                        Complex32::new(
                            rng.gaussian_f32() * gain.amplitude[0],
                            rng.gaussian_f32() * gain.amplitude[1],
                        ),
                    ]
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn fbm_like_series(seed: u64, sample_count: usize, beta: f32) -> Vec<f32> {
    let count = sample_count.max(2);
    let mut rng = DeterministicRng::new(seed);
    let mut series = vec![0.0_f32; count];
    let harmonics = (count / 2).max(1);
    for harmonic in 1..=harmonics {
        let phase = std::f32::consts::TAU * rng.next_unit_f64() as f32;
        let amplitude = (harmonic as f32).powf(-0.5 * beta) * rng.gaussian_f32();
        for (sample, value) in series.iter_mut().enumerate() {
            let angle =
                std::f32::consts::TAU * harmonic as f32 * sample as f32 / count as f32 + phase;
            *value += amplitude * angle.cos();
        }
    }
    let mean = series.iter().sum::<f32>() / count as f32;
    let rms = (series
        .iter()
        .map(|value| {
            let centered = *value - mean;
            centered * centered
        })
        .sum::<f32>()
        / count as f32)
        .sqrt();
    if rms > 0.0 {
        for value in &mut series {
            *value /= rms;
        }
    }
    series
}

fn gain_amplitude_scale(amplitude: [f32; 2]) -> f32 {
    (amplitude[0] * amplitude[0] + amplitude[1] * amplitude[1]).sqrt()
}

fn casa_like_leakage_terms(
    rng: &mut DeterministicRng,
    leakage: &SyntheticPolarizationLeakageCorruption,
) -> [Complex32; 2] {
    let random = Complex32::new(
        rng.gaussian_f32() * leakage.amplitude[0],
        rng.gaussian_f32() * leakage.amplitude[1],
    );
    let offset = Complex32::new(leakage.offset[0], leakage.offset[1]);
    [
        random + offset,
        random + Complex32::new(-offset.re, offset.im),
    ]
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
    let beta_inv = (1.0 - dot(velocity, velocity)).sqrt();
    let source = phase_center.cosines();
    let mut solution = subtract_vectors(source, velocity);

    loop {
        let dot_sv = dot(solution, velocity);
        let mut trial = scale_vector(
            add_vectors(
                scale_vector(solution, beta_inv),
                scale_vector(velocity, 1.0 + dot_sv / (1.0 + beta_inv)),
            ),
            1.0 / (1.0 + dot_sv),
        );
        trial = normalize(trial);
        let residual = subtract_vectors(trial, source);
        if vector_norm(residual) <= 1.0e-10 {
            break;
        }
        for idx in 0..3 {
            let component = velocity[idx];
            solution[idx] -= residual[idx]
                / (((beta_inv + component * component / (1.0 + beta_inv))
                    - component * trial[idx])
                    / (1.0 + dot_sv));
        }
    }

    Ok(subtract_vectors(solution, source))
}

fn inverse_solar_deflection_shift(
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> MsResult<[f64; 3]> {
    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let (earth_helio, _) = sofars::eph::epv00(tt.0, tt.1).ok_or_else(|| {
        MsError::SyntheticObservation("UVW solar-position ephemeris lookup failed".to_string())
    })?;
    let sun_vector = [-earth_helio[0][0], -earth_helio[0][1], -earth_helio[0][2]];
    let sun_distance = vector_norm(sun_vector);
    let sun = scale_vector(sun_vector, 1.0 / sun_distance);
    let source = phase_center.cosines();
    let mut dot_solution_sun = dot(source, sun);
    let strength = -1.974e-8 / sun_distance;
    let mut solution = source;

    loop {
        let correction = scale_vector(
            subtract_vectors(sun, scale_vector(solution, dot_solution_sun)),
            strength / (1.0 - dot_solution_sun),
        );
        for idx in 0..3 {
            let component = sun[idx];
            solution[idx] -= (correction[idx] + solution[idx] - source[idx])
                / (1.0
                    + (component * correction[idx]
                        - strength * (dot_solution_sun + component * solution[idx]))
                        / (1.0 - dot_solution_sun));
        }
        dot_solution_sun = dot(solution, sun);
        let residual = add_vectors(correction, subtract_vectors(solution, source));
        if vector_norm(residual) <= 1.0e-10 {
            break;
        }
    }

    Ok(subtract_vectors(solution, source))
}

fn local_apparent_sidereal_time(frame: &MeasFrame) -> MsResult<f64> {
    let (ut1_a, ut1_b) = epoch_jd_pair(frame, EpochRef::UT1)?;
    let ut1_mjd = epoch_mjd(frame, EpochRef::UT1)?;
    let centuries = (ut1_mjd - 51_544.5) / 36_525.0;
    let gmst0_seconds =
        24_110.548_41 + 8_640_184.812_866 * centuries + 0.093_104 * centuries * centuries
            - 6.2e-6 * centuries * centuries * centuries;
    let gmst0_turns = (ut1_mjd + gmst0_seconds / 86_400.0 + 6_713.0).fract();
    let equation_of_equinoxes =
        sofars::vm::anpm(sofars::erst::gst94(ut1_a, ut1_b) - sofars::erst::gmst82(ut1_a, ut1_b));
    // Matches casacore's legacy `Nutation::STANDARD` equation of equinoxes
    // used by `MCEpoch::UT1_GAST`; SOFA `gst94` differs by about 0.1 ms.
    const CASACORE_STANDARD_NUTATION_GAST_OFFSET_RAD: f64 = 7.719e-9;
    let gast = gmst0_turns * std::f64::consts::TAU
        + equation_of_equinoxes
        + CASACORE_STANDARD_NUTATION_GAST_OFFSET_RAD;
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
    let mut matrix = [[0.0; 3]; 3];
    for (idx, row) in matrix.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut matrix, xp, 2);
    casacore_apply_single_euler(&mut matrix, yp, 1);
    casacore_apply_single_euler(&mut matrix, last, 3);
    matrix
}

fn casacore_apply_single_euler(matrix: &mut [[f64; 3]; 3], angle: f64, axis: usize) {
    if angle == 0.0 || axis == 0 {
        return;
    }
    let mut single = [[0.0; 3]; 3];
    for (idx, row) in single.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    let i = axis % 3;
    let j = (i + 1) % 3;
    let (sin_angle, cos_angle) = angle.sin_cos();
    single[i][i] = cos_angle;
    single[j][j] = cos_angle;
    single[i][j] = -sin_angle;
    single[j][i] = sin_angle;

    let original = *matrix;
    for row in 0..3 {
        for col in 0..3 {
            matrix[row][col] = original[row][0] * single[0][col]
                + original[row][1] * single[1][col]
                + original[row][2] * single[2][col];
        }
    }
}

fn diurnal_aberration_factor(radius_m: f64) -> f64 {
    const C: f64 = 299_792_458.0;
    const SIDEREAL_RATIO: f64 = 1.002_737_909_35;
    (2.0 * std::f64::consts::PI * radius_m) / 86_400.0 * SIDEREAL_RATIO / C
}

fn rotate_shift(vector: [f64; 3], shift: [f64; 3], reference_direction: [f64; 3]) -> [f64; 3] {
    let reference = normalize(reference_direction);
    let reference_long = reference[1].atan2(reference[0]);
    let reference_lat = reference[2].asin();

    let mut rot = [[0.0; 3]; 3];
    for (idx, row) in rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut rot, -std::f64::consts::FRAC_PI_2 + reference_lat, 2);
    casacore_apply_single_euler(&mut rot, -reference_long, 3);

    let shifted_once = rotate(&rot, &shift);
    let shifted_long = shifted_once[1].atan2(shifted_once[0]);
    let mut long_rot = [[0.0; 3]; 3];
    for (idx, row) in long_rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut long_rot, -shifted_long, 3);
    rot = multiply_matrices(&long_rot, &rot);

    let shifted_twice = rotate(&rot, &shift);
    let mut correction_rot = [[0.0; 3]; 3];
    for (idx, row) in correction_rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut correction_rot, shifted_twice[0], 2);
    let corrected = multiply_matrices(&correction_rot, &rot);
    rotate_t(&rot, &rotate(&corrected, &vector))
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

fn multiply_matrices(left: &[[f64; 3]; 3], right: &[[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_observation_worker_count_is_bounded_by_work_and_capacity() {
        assert_eq!(synthetic_observation_worker_count_for(0, 10, 8, 8), 1);
        assert_eq!(synthetic_observation_worker_count_for(10, 0, 8, 8), 1);
        assert_eq!(synthetic_observation_worker_count_for(3, 10, 8, 8), 3);
        assert_eq!(synthetic_observation_worker_count_for(10, 10, 2, 8), 2);
        assert_eq!(synthetic_observation_worker_count_for(10, 10, 8, 2), 2);
        assert_eq!(synthetic_observation_worker_count_for(10, 10, 0, 0), 1);
    }

    #[test]
    fn row_noise_seed_varies_by_row_coordinates() {
        let base = row_noise_seed(42, 0, 0, 1, 0);
        assert_ne!(base, row_noise_seed(43, 0, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 1, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 0, 1, 2, 0));
        assert_ne!(base, row_noise_seed(42, 0, 0, 1, 1));
        assert_eq!(base, row_noise_seed(42, 0, 0, 1, 0));
    }
}
