// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic MeasurementSet generation for tutorial simulation workflows.
//!
//! This module owns the first reusable MS-writing slice of the VLA simulation
//! vertical. It mirrors the CASA `simobserve` setup order at the data-model
//! level: validate a model image path, set array configuration, define the
//! spectral window and field, sample the requested observing time range, and
//! write CASA-compatible MS subtables plus uncorrupted visibility rows.

use casa_coordinates::fits::{FitsHeader, from_fits_header};
use casa_coordinates::{Coordinate, CoordinateSystem, CoordinateType};
use casa_imaging::{
    ImageGeometry, PrimaryBeamModel, StandardMfsModelPredictor, primary_beam_voltage_pattern,
};
use casa_tables::{
    StreamedTiledPrimitiveColumn, StreamedTiledPrimitiveType, StreamedTiledShapeComplex32Column,
    StreamingTiledPrimitiveWriter, StreamingTiledShapeComplex32Writer,
    install_streamed_tiled_column_primitive_column, install_streamed_tiled_shape_complex32_column,
    install_streamed_tiled_shape_primitive_column,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::position::MPosition;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use libm::j1;
use ndarray::{Array2, ArrayD};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::column_def::{ColumnDef, ColumnKind};
use crate::error::{MsError, MsResult};
use crate::flagging::shadowed_antennas_from_projected_baselines;
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
    /// Wall-clock timing breakdown for the native generator.
    pub timing: SyntheticObservationTimingReport,
}

/// Wall-clock timing breakdown for one synthetic observation generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationTimingReport {
    /// Request validation time.
    pub validate_millis: u128,
    /// Existing-output removal and MeasurementSet creation time.
    pub setup_millis: u128,
    /// Static metadata subtable write time before MAIN rows.
    pub metadata_millis: u128,
    /// FITS model read and predictor preparation time.
    pub model_prepare_millis: u128,
    /// MAIN-row generation and writeback timing.
    pub main_rows: SyntheticMainRowTimingReport,
    /// MeasurementSet save time.
    pub save_millis: u128,
    /// End-to-end generation time inside the native library call.
    pub total_millis: u128,
}

/// MAIN table timing breakdown for one synthetic observation generation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticMainRowTimingReport {
    /// Number of channel prediction workers selected for each time sample.
    pub channel_prediction_workers: usize,
    /// Time spent computing UVW coordinates and row identities.
    pub uvw_and_row_setup_millis: u128,
    /// Time spent predicting model visibilities across channels.
    pub prediction_millis: u128,
    /// Time spent applying deterministic corruption/noise.
    pub corruption_millis: u128,
    /// Time spent waiting to enqueue DATA row batches to the background writer.
    #[serde(default)]
    pub data_io_enqueue_millis: u128,
    /// Time spent joining and finalizing the background DATA writer.
    #[serde(default)]
    pub data_io_finalize_millis: u128,
    /// Time spent packing DATA rows into tiled storage buffers.
    #[serde(default)]
    pub data_io_assemble_millis: u128,
    /// Time spent writing DATA tile buffers to disk.
    #[serde(default)]
    pub data_io_write_millis: u128,
    /// Bytes written through the streamed tiled MAIN column writers.
    #[serde(default)]
    pub data_io_bytes: u64,
    /// Time spent constructing MAIN rows and appending them to the table.
    pub main_write_millis: u128,
}

/// Generate an uncorrupted CASA-compatible synthetic MeasurementSet.
///
/// The current implementation writes structurally complete MS metadata and can
/// predict uncorrupted visibility samples from a FITS model image.
pub fn generate_synthetic_observation_ms(
    request: &SyntheticObservationRequest,
) -> MsResult<SyntheticObservationReport> {
    let total_started = Instant::now();
    let validate_started = Instant::now();
    validate_request(request)?;
    let validate_millis = elapsed_millis(validate_started.elapsed());

    let setup_started = Instant::now();
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
    fs::create_dir_all(&request.output_ms).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to create output MeasurementSet directory {}: {error}",
            request.output_ms.display()
        ))
    })?;
    let setup_millis = elapsed_millis(setup_started.elapsed());

    let metadata_started = Instant::now();
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
    let metadata_millis = elapsed_millis(metadata_started.elapsed());

    let time_sample_count =
        time_sample_count(request.duration_seconds, request.integration_seconds);
    let baseline_count = request.antennas.len() * (request.antennas.len() - 1) / 2;
    let model_started = Instant::now();
    let model = if request.predict_model {
        Some(read_fits_model_image(
            &request.model_image,
            request.model_peak_jy_per_pixel,
            request.spectral_setup.channel_count,
        )?)
    } else {
        None
    };
    let model_prepare_millis = elapsed_millis(model_started.elapsed());
    let mut main_column_writer = SimobserveMainColumnWriter::start(
        &request.output_ms,
        baseline_count * time_sample_count,
        2,
        request.spectral_setup.channel_count,
        &request.telescope_name,
    )?;
    let mut main_rows = populate_main_rows(
        &mut ms,
        request,
        time_sample_count,
        model.as_ref(),
        &mut main_column_writer,
    )?;
    let data_io_finalize_started = Instant::now();
    let streamed_main_columns = main_column_writer.finish()?;
    main_rows.timing.data_io_finalize_millis = elapsed_millis(data_io_finalize_started.elapsed());
    main_rows.timing.data_io_assemble_millis =
        elapsed_seconds_to_millis(streamed_main_columns.assemble_seconds());
    main_rows.timing.data_io_write_millis =
        elapsed_seconds_to_millis(streamed_main_columns.write_seconds());
    main_rows.timing.data_io_bytes = streamed_main_columns.bytes_written() as u64;

    let save_started = Instant::now();
    ms.save_assuming_valid_with_main_column_overrides(&main_rows.scalar_column_overrides)?;
    install_streamed_main_columns(ms.main_table(), &request.output_ms, streamed_main_columns)?;
    let save_millis = elapsed_millis(save_started.elapsed());

    Ok(SyntheticObservationReport {
        output_ms: request.output_ms.clone(),
        model_image: request.model_image.clone(),
        antenna_count: request.antennas.len(),
        baseline_count,
        time_sample_count,
        main_row_count: baseline_count * time_sample_count,
        channel_count: request.spectral_setup.channel_count,
        nonzero_visibility_count: main_rows.nonzero_visibility_count,
        applied_corruptions: applied_corruption_names(request.corruption.as_ref()),
        timing: SyntheticObservationTimingReport {
            validate_millis,
            setup_millis,
            metadata_millis,
            model_prepare_millis,
            main_rows: main_rows.timing,
            save_millis,
            total_millis: elapsed_millis(total_started.elapsed()),
        },
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
    main_column_writer: &mut SimobserveMainColumnWriter,
) -> MsResult<MainRowsReport> {
    let num_corr = 2usize;
    let num_chan = request.spectral_setup.channel_count;
    let channel_prediction_workers = simobserve_channel_worker_count(num_chan);
    let template = MainRowTemplate {
        flag_category: bool_array(&[], vec![0, num_corr, num_chan]),
    };
    let field_plan_started = Instant::now();
    let field_plans = build_field_plans(request, model)?;
    if trace_simobserve_setup() {
        eprintln!(
            "simobserve_setup_trace stage=field_plans fields={} total_millis={}",
            field_plans.len(),
            elapsed_millis(field_plan_started.elapsed())
        );
    }
    let corruption = request.corruption.as_ref().map(|config| {
        SyntheticCorruptionState::new(
            config,
            request.antennas.len(),
            request.spectral_setup.channel_count,
            samples,
        )
    });
    let mut nonzero_visibility_count = 0usize;
    let mut timing = MainRowTimingDurations {
        channel_prediction_workers,
        ..MainRowTimingDurations::default()
    };
    let baseline_count = request.antennas.len() * (request.antennas.len() - 1) / 2;
    let mut scalar_column_overrides =
        MainScalarColumnOverrides::with_capacity(samples * baseline_count);

    for sample in 0..samples {
        let uvw_started = Instant::now();
        let field_id = sample % field_plans.len();
        let field_plan = &field_plans[field_id];
        let time =
            request.start_time_mjd_seconds + (sample as f64 + 0.5) * request.integration_seconds;
        let antenna_uvws =
            antenna_uvw_positions(&request.antennas, field_plan.phase_center_rad, time)?;
        let mut row_specs = Vec::with_capacity(request.antennas.len() * request.antennas.len());
        for antenna1 in 0..request.antennas.len() {
            for antenna2 in (antenna1 + 1)..request.antennas.len() {
                let uvw = [
                    antenna_uvws[antenna2][0] - antenna_uvws[antenna1][0],
                    antenna_uvws[antenna2][1] - antenna_uvws[antenna1][1],
                    antenna_uvws[antenna2][2] - antenna_uvws[antenna1][2],
                ];
                row_specs.push(MainRowVisibilitySpec {
                    antenna1,
                    antenna2,
                    field_id,
                    time,
                    uvw,
                });
            }
        }
        let row_uvws = row_specs.iter().map(|spec| spec.uvw).collect::<Vec<_>>();
        let shadowed_antennas = shadowed_antennas_for_rows(&row_specs, &request.antennas);
        timing.uvw_and_row_setup += uvw_started.elapsed();

        let prediction_started = Instant::now();
        let mut data_rows = predicted_data_values_for_rows_with_workers(
            field_plan.predictors.as_deref(),
            &request.spectral_setup,
            &row_uvws,
            num_corr,
            channel_prediction_workers,
        );
        timing.prediction += prediction_started.elapsed();
        let corruption_started = Instant::now();
        nonzero_visibility_count += apply_corruption_and_count_rows_with_workers(
            corruption.as_ref(),
            &row_specs,
            &mut data_rows,
            num_chan,
            sample,
        );
        timing.corruption += corruption_started.elapsed();
        let flag_rows = row_specs
            .iter()
            .map(|spec| shadowed_antennas[spec.antenna1] || shadowed_antennas[spec.antenna2])
            .collect::<Vec<_>>();
        let uvw_rows = row_specs.iter().map(|spec| spec.uvw).collect::<Vec<_>>();
        let data_io_started = Instant::now();
        main_column_writer.send_batch(SimobserveMainColumnBatch {
            data_rows,
            flag_rows: flag_rows.clone(),
            uvw_rows,
        })?;
        timing.data_io_enqueue += data_io_started.elapsed();
        for (spec, shadowed_row) in row_specs.into_iter().zip(flag_rows.into_iter()) {
            let write_started = Instant::now();
            scalar_column_overrides.push(&spec, request.integration_seconds, shadowed_row);
            let row = RecordValue::new(vec![RecordField::new(
                "FLAG_CATEGORY",
                template.flag_category.clone(),
            )]);
            ms.main_table_mut().add_row_assuming_valid(row)?;
            timing.main_write += write_started.elapsed();
        }
    }
    Ok(MainRowsReport {
        nonzero_visibility_count,
        timing: timing.into_report(),
        scalar_column_overrides: scalar_column_overrides.into_column_overrides(),
    })
}

fn shadowed_antennas_for_rows(
    rows: &[MainRowVisibilitySpec],
    antennas: &[SyntheticAntenna],
) -> Vec<bool> {
    shadowed_antennas_from_projected_baselines(
        antennas.len(),
        rows.iter().map(|spec| {
            (
                spec.antenna1,
                spec.antenna2,
                spec.uvw,
                antennas[spec.antenna1].dish_diameter_m,
                antennas[spec.antenna2].dish_diameter_m,
            )
        }),
    )
}

fn apply_corruption_and_count_rows_with_workers(
    corruption: Option<&SyntheticCorruptionState>,
    row_specs: &[MainRowVisibilitySpec],
    data_rows: &mut [Vec<Complex32>],
    channel_count: usize,
    sample_index: usize,
) -> usize {
    let worker_count = simobserve_row_worker_count(data_rows.len(), channel_count);
    if worker_count <= 1 {
        return apply_corruption_and_count_rows(
            corruption,
            row_specs,
            data_rows,
            channel_count,
            sample_index,
        );
    }

    let chunk_size = data_rows.len().div_ceil(worker_count);
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for (spec_chunk, data_chunk) in row_specs
            .chunks(chunk_size)
            .zip(data_rows.chunks_mut(chunk_size))
        {
            handles.push(scope.spawn(move || {
                apply_corruption_and_count_rows(
                    corruption,
                    spec_chunk,
                    data_chunk,
                    channel_count,
                    sample_index,
                )
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("synthetic observation row worker should not panic")
            })
            .sum()
    })
}

fn apply_corruption_and_count_rows(
    corruption: Option<&SyntheticCorruptionState>,
    row_specs: &[MainRowVisibilitySpec],
    data_rows: &mut [Vec<Complex32>],
    channel_count: usize,
    sample_index: usize,
) -> usize {
    let mut nonzero_visibility_count = 0usize;
    for (spec, data_values) in row_specs.iter().zip(data_rows.iter_mut()) {
        if let Some(corruption) = corruption {
            corruption.apply(
                data_values,
                spec.antenna1,
                spec.antenna2,
                channel_count,
                sample_index,
            );
        }
        nonzero_visibility_count += count_nonzero_complex(data_values);
    }
    nonzero_visibility_count
}

fn count_nonzero_complex(values: &[Complex32]) -> usize {
    values
        .iter()
        .filter(|value| value.re != 0.0 || value.im != 0.0)
        .count()
}

struct MainRowsReport {
    nonzero_visibility_count: usize,
    timing: SyntheticMainRowTimingReport,
    scalar_column_overrides: HashMap<String, Vec<Option<Value>>>,
}

struct SimobserveMainColumnBatch {
    data_rows: Vec<Vec<Complex32>>,
    flag_rows: Vec<bool>,
    uvw_rows: Vec<[f64; 3]>,
}

struct StreamedSimobserveMainColumns {
    data: StreamedTiledShapeComplex32Column,
    flag: StreamedTiledPrimitiveColumn,
    uvw: StreamedTiledPrimitiveColumn,
    weight: StreamedTiledPrimitiveColumn,
    sigma: StreamedTiledPrimitiveColumn,
}

impl StreamedSimobserveMainColumns {
    fn assemble_seconds(&self) -> f64 {
        self.data.assemble_seconds()
            + self.flag.assemble_seconds()
            + self.uvw.assemble_seconds()
            + self.weight.assemble_seconds()
            + self.sigma.assemble_seconds()
    }

    fn write_seconds(&self) -> f64 {
        self.data.write_seconds()
            + self.flag.write_seconds()
            + self.uvw.write_seconds()
            + self.weight.write_seconds()
            + self.sigma.write_seconds()
    }

    fn bytes_written(&self) -> usize {
        self.data.bytes_written()
            + self.flag.bytes_written()
            + self.uvw.bytes_written()
            + self.weight.bytes_written()
            + self.sigma.bytes_written()
    }
}

struct SimobserveMainColumnWriter {
    sender: mpsc::SyncSender<SimobserveMainColumnBatch>,
    handle: thread::JoinHandle<MsResult<StreamedSimobserveMainColumns>>,
}

impl SimobserveMainColumnWriter {
    fn start(
        output_ms: &Path,
        row_count: usize,
        num_corr: usize,
        num_chan: usize,
        telescope_name: &str,
    ) -> MsResult<Self> {
        let visibility_tile_shape =
            crate::ms::casa_visibility_tile_shape(num_corr, num_chan, telescope_name);
        let weight_tile_shape = crate::ms::casa_weight_tile_shape(&visibility_tile_shape);
        let uvw_tile_shape = crate::ms::casa_uvw_tile_shape(&visibility_tile_shape);

        let data_writer = StreamingTiledShapeComplex32Writer::create(
            output_ms.join(".casa-rs.DATA.table.f.tmp"),
            row_count,
            vec![num_corr, num_chan],
            visibility_tile_shape.clone(),
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!("failed to create streamed DATA writer: {error}"))
        })?;
        let flag_writer = StreamingTiledPrimitiveWriter::create_shape(
            output_ms.join(".casa-rs.FLAG.table.f.tmp"),
            row_count,
            vec![num_corr, num_chan],
            visibility_tile_shape,
            StreamedTiledPrimitiveType::Bool,
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!("failed to create streamed FLAG writer: {error}"))
        })?;
        let uvw_writer = StreamingTiledPrimitiveWriter::create_column(
            output_ms.join(".casa-rs.UVW.table.f.tmp"),
            row_count,
            vec![3],
            uvw_tile_shape,
            StreamedTiledPrimitiveType::Float64,
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!("failed to create streamed UVW writer: {error}"))
        })?;
        let weight_writer = StreamingTiledPrimitiveWriter::create_shape(
            output_ms.join(".casa-rs.WEIGHT.table.f.tmp"),
            row_count,
            vec![num_corr],
            weight_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float32,
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to create streamed WEIGHT writer: {error}"
            ))
        })?;
        let sigma_writer = StreamingTiledPrimitiveWriter::create_shape(
            output_ms.join(".casa-rs.SIGMA.table.f.tmp"),
            row_count,
            vec![num_corr],
            weight_tile_shape,
            StreamedTiledPrimitiveType::Float32,
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to create streamed SIGMA writer: {error}"
            ))
        })?;

        let (sender, receiver) =
            mpsc::sync_channel::<SimobserveMainColumnBatch>(simobserve_io_queue_depth());
        let handle = thread::spawn(move || {
            let mut data_writer = data_writer;
            let mut flag_writer = flag_writer;
            let mut uvw_writer = uvw_writer;
            let mut weight_writer = weight_writer;
            let mut sigma_writer = sigma_writer;
            let weight_row = vec![1.0f32; num_corr];
            let sigma_row = vec![1.0f32; num_corr];
            let flag_true_row = vec![true; num_corr * num_chan];

            for batch in receiver {
                if batch.data_rows.len() != batch.flag_rows.len()
                    || batch.data_rows.len() != batch.uvw_rows.len()
                {
                    return Err(MsError::SyntheticObservation(format!(
                        "background MAIN column writer received inconsistent batch sizes: DATA={} FLAG={} UVW={}",
                        batch.data_rows.len(),
                        batch.flag_rows.len(),
                        batch.uvw_rows.len()
                    )));
                }
                for ((data_row, flag_row), uvw_row) in batch
                    .data_rows
                    .into_iter()
                    .zip(batch.flag_rows.into_iter())
                    .zip(batch.uvw_rows.into_iter())
                {
                    data_writer.push_row(&data_row).map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream DATA row into tiled storage: {error}"
                        ))
                    })?;
                    if flag_row {
                        flag_writer.push_bool_row(&flag_true_row).map_err(|error| {
                            MsError::SyntheticObservation(format!(
                                "failed to stream FLAG row into tiled storage: {error}"
                            ))
                        })?;
                    } else {
                        flag_writer.push_zero_row().map_err(|error| {
                            MsError::SyntheticObservation(format!(
                                "failed to stream empty FLAG row into tiled storage: {error}"
                            ))
                        })?;
                    }
                    uvw_writer.push_f64_row(&uvw_row).map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream UVW row into tiled storage: {error}"
                        ))
                    })?;
                    weight_writer.push_f32_row(&weight_row).map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream WEIGHT row into tiled storage: {error}"
                        ))
                    })?;
                    sigma_writer.push_f32_row(&sigma_row).map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream SIGMA row into tiled storage: {error}"
                        ))
                    })?;
                }
            }
            Ok(StreamedSimobserveMainColumns {
                data: data_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed DATA writer: {error}"
                    ))
                })?,
                flag: flag_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed FLAG writer: {error}"
                    ))
                })?,
                uvw: uvw_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed UVW writer: {error}"
                    ))
                })?,
                weight: weight_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed WEIGHT writer: {error}"
                    ))
                })?,
                sigma: sigma_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed SIGMA writer: {error}"
                    ))
                })?,
            })
        });
        Ok(Self { sender, handle })
    }

    fn send_batch(&self, batch: SimobserveMainColumnBatch) -> MsResult<()> {
        self.sender.send(batch).map_err(|error| {
            MsError::SyntheticObservation(format!(
                "background MAIN column writer stopped before accepting rows: {error}"
            ))
        })
    }

    fn finish(self) -> MsResult<StreamedSimobserveMainColumns> {
        drop(self.sender);
        self.handle.join().map_err(|_| {
            MsError::SyntheticObservation("background MAIN column writer panicked".to_string())
        })?
    }
}

fn install_streamed_main_columns(
    main: &casa_tables::Table,
    output_ms: &Path,
    streamed: StreamedSimobserveMainColumns,
) -> MsResult<()> {
    let data_seq = data_manager_sequence(main, "DATA")?;
    install_streamed_tiled_shape_complex32_column(output_ms, data_seq, "DATA", streamed.data)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to install streamed DATA column: {error}"
            ))
        })?;

    let flag_seq = data_manager_sequence(main, "FLAG")?;
    install_streamed_tiled_shape_primitive_column(output_ms, flag_seq, "FLAG", streamed.flag)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to install streamed FLAG column: {error}"
            ))
        })?;

    let uvw_seq = data_manager_sequence(main, "UVW")?;
    install_streamed_tiled_column_primitive_column(output_ms, uvw_seq, "UVW", streamed.uvw)
        .map_err(|error| {
            MsError::SyntheticObservation(format!("failed to install streamed UVW column: {error}"))
        })?;

    let weight_seq = data_manager_sequence(main, "WEIGHT")?;
    install_streamed_tiled_shape_primitive_column(output_ms, weight_seq, "WEIGHT", streamed.weight)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to install streamed WEIGHT column: {error}"
            ))
        })?;

    let sigma_seq = data_manager_sequence(main, "SIGMA")?;
    install_streamed_tiled_shape_primitive_column(output_ms, sigma_seq, "SIGMA", streamed.sigma)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to install streamed SIGMA column: {error}"
            ))
        })?;

    Ok(())
}

fn data_manager_sequence(main: &casa_tables::Table, column: &str) -> MsResult<u32> {
    crate::ms::measurement_set_main_data_manager_sequence(main, column).ok_or_else(|| {
        MsError::SyntheticObservation(format!(
            "could not resolve {column} data-manager sequence for streamed install"
        ))
    })
}

fn simobserve_io_queue_depth() -> usize {
    std::env::var("CASA_RS_SIMOBSERVE_IO_QUEUE_DEPTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

struct MainScalarColumnOverrides {
    antenna1: Vec<Option<Value>>,
    antenna2: Vec<Option<Value>>,
    array_id: Vec<Option<Value>>,
    data_desc_id: Vec<Option<Value>>,
    exposure: Vec<Option<Value>>,
    feed1: Vec<Option<Value>>,
    feed2: Vec<Option<Value>>,
    field_id: Vec<Option<Value>>,
    flag_row: Vec<Option<Value>>,
    interval: Vec<Option<Value>>,
    observation_id: Vec<Option<Value>>,
    processor_id: Vec<Option<Value>>,
    scan_number: Vec<Option<Value>>,
    state_id: Vec<Option<Value>>,
    time: Vec<Option<Value>>,
    time_centroid: Vec<Option<Value>>,
}

impl MainScalarColumnOverrides {
    fn with_capacity(row_count: usize) -> Self {
        Self {
            antenna1: Vec::with_capacity(row_count),
            antenna2: Vec::with_capacity(row_count),
            array_id: Vec::with_capacity(row_count),
            data_desc_id: Vec::with_capacity(row_count),
            exposure: Vec::with_capacity(row_count),
            feed1: Vec::with_capacity(row_count),
            feed2: Vec::with_capacity(row_count),
            field_id: Vec::with_capacity(row_count),
            flag_row: Vec::with_capacity(row_count),
            interval: Vec::with_capacity(row_count),
            observation_id: Vec::with_capacity(row_count),
            processor_id: Vec::with_capacity(row_count),
            scan_number: Vec::with_capacity(row_count),
            state_id: Vec::with_capacity(row_count),
            time: Vec::with_capacity(row_count),
            time_centroid: Vec::with_capacity(row_count),
        }
    }

    fn push(&mut self, spec: &MainRowVisibilitySpec, integration_seconds: f64, flag_row: bool) {
        self.antenna1.push(Some(i(spec.antenna1 as i32)));
        self.antenna2.push(Some(i(spec.antenna2 as i32)));
        self.array_id.push(Some(i(0)));
        self.data_desc_id.push(Some(i(0)));
        self.exposure.push(Some(f(integration_seconds)));
        self.feed1.push(Some(i(0)));
        self.feed2.push(Some(i(0)));
        self.field_id.push(Some(i(spec.field_id as i32)));
        self.flag_row.push(Some(b(flag_row)));
        self.interval.push(Some(f(integration_seconds)));
        self.observation_id.push(Some(i(0)));
        self.processor_id.push(Some(i(0)));
        self.scan_number.push(Some(i(1)));
        self.state_id.push(Some(i(0)));
        self.time.push(Some(f(spec.time)));
        self.time_centroid.push(Some(f(spec.time)));
    }

    fn into_column_overrides(self) -> HashMap<String, Vec<Option<Value>>> {
        HashMap::from([
            ("ANTENNA1".to_string(), self.antenna1),
            ("ANTENNA2".to_string(), self.antenna2),
            ("ARRAY_ID".to_string(), self.array_id),
            ("DATA_DESC_ID".to_string(), self.data_desc_id),
            ("EXPOSURE".to_string(), self.exposure),
            ("FEED1".to_string(), self.feed1),
            ("FEED2".to_string(), self.feed2),
            ("FIELD_ID".to_string(), self.field_id),
            ("FLAG_ROW".to_string(), self.flag_row),
            ("INTERVAL".to_string(), self.interval),
            ("OBSERVATION_ID".to_string(), self.observation_id),
            ("PROCESSOR_ID".to_string(), self.processor_id),
            ("SCAN_NUMBER".to_string(), self.scan_number),
            ("STATE_ID".to_string(), self.state_id),
            ("TIME".to_string(), self.time),
            ("TIME_CENTROID".to_string(), self.time_centroid),
        ])
    }
}

#[derive(Default)]
struct MainRowTimingDurations {
    channel_prediction_workers: usize,
    uvw_and_row_setup: Duration,
    prediction: Duration,
    corruption: Duration,
    data_io_enqueue: Duration,
    main_write: Duration,
}

impl MainRowTimingDurations {
    fn into_report(self) -> SyntheticMainRowTimingReport {
        SyntheticMainRowTimingReport {
            channel_prediction_workers: self.channel_prediction_workers,
            uvw_and_row_setup_millis: elapsed_millis(self.uvw_and_row_setup),
            prediction_millis: elapsed_millis(self.prediction),
            corruption_millis: elapsed_millis(self.corruption),
            data_io_enqueue_millis: elapsed_millis(self.data_io_enqueue),
            data_io_finalize_millis: 0,
            data_io_assemble_millis: 0,
            data_io_write_millis: 0,
            data_io_bytes: 0,
            main_write_millis: elapsed_millis(self.main_write),
        }
    }
}

fn elapsed_seconds_to_millis(seconds: f64) -> u128 {
    (seconds * 1000.0).round() as u128
}

#[derive(Clone)]
struct MainRowTemplate {
    flag_category: Value,
}

#[derive(Clone, Copy)]
struct MainRowVisibilitySpec {
    antenna1: usize,
    antenna2: usize,
    field_id: usize,
    time: f64,
    uvw: [f64; 3],
}

#[derive(Debug, Clone)]
struct FitsModelImage {
    pixels: Array2<f32>,
    channel_planes: Vec<Array2<f32>>,
    cell_size_rad: [f64; 2],
    direction_wcs: Option<FitsModelDirectionWcs>,
    ra_axis_increases_with_x: bool,
    reference_direction_rad: Option<[f64; 2]>,
}

impl FitsModelImage {
    fn pixels_for_channel(&self, channel: usize) -> &Array2<f32> {
        self.channel_planes.get(channel).unwrap_or(&self.pixels)
    }
}

#[derive(Debug, Clone)]
struct FitsModelDirectionWcs {
    coordinate_system: CoordinateSystem,
    coordinate_index: usize,
}

impl FitsModelDirectionWcs {
    fn coordinate(&self) -> &dyn Coordinate {
        self.coordinate_system.coordinate(self.coordinate_index)
    }
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
const CASA_GRIDFT_NEGATIVE_RA_PHASE_ALIGNMENT_PIXELS: [f64; 2] = [0.001_757_8, 0.001_115_9];

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
    let telescope_is_alma_family = request.telescope_name.eq_ignore_ascii_case("ALMA")
        || request.telescope_name.eq_ignore_ascii_case("ACA");
    let (beam_dish_diameter_m, blockage_diameter_m) =
        if telescope_is_alma_family && (dish_diameter_m - 12.0).abs() < 0.5 {
            (10.7, 0.75)
        } else if telescope_is_alma_family && (dish_diameter_m - 7.0).abs() < 0.5 {
            (6.25, 0.75)
        } else {
            (dish_diameter_m, if telescope_is_vla { 2.36 } else { 0.0 })
        };
    SyntheticPrimaryBeam {
        use_casa_vla_q_table: telescope_is_vla && (dish_diameter_m - 25.0).abs() < 1.0e-6,
        dish_diameter_m: beam_dish_diameter_m,
        blockage_diameter_m,
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
    let context = ChannelPredictorContext {
        model,
        spectral_setup,
        geometry,
        phase_center_rad,
        phase_offset_rad,
        pointing_offset_rad,
        primary_beam,
    };
    let worker_count = simobserve_channel_worker_count(spectral_setup.channel_count);
    if worker_count <= 1 || spectral_setup.channel_count <= 1 {
        return build_channel_predictor_range(&context, 0, spectral_setup.channel_count);
    }

    let chunk_size = spectral_setup.channel_count.div_ceil(worker_count);
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for start_channel in (0..spectral_setup.channel_count).step_by(chunk_size) {
            let end_channel = (start_channel + chunk_size).min(spectral_setup.channel_count);
            let worker_context = context;
            handles.push(scope.spawn(move || {
                build_channel_predictor_range(&worker_context, start_channel, end_channel)
            }));
        }

        let mut predictors = Vec::with_capacity(spectral_setup.channel_count);
        for handle in handles {
            predictors.extend(
                handle
                    .join()
                    .expect("synthetic observation predictor setup worker should not panic")?,
            );
        }
        Ok(predictors)
    })
}

#[derive(Clone, Copy)]
struct ChannelPredictorContext<'a> {
    model: &'a FitsModelImage,
    spectral_setup: &'a SyntheticSpectralSetup,
    geometry: ImageGeometry,
    phase_center_rad: [f64; 2],
    phase_offset_rad: [f64; 2],
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
}

fn build_channel_predictor_range(
    context: &ChannelPredictorContext<'_>,
    start_channel: usize,
    end_channel: usize,
) -> MsResult<Vec<SyntheticChannelPredictor>> {
    let range_started = Instant::now();
    let mut timing = ChannelPredictorBuildTiming::default();
    let mut predictors = Vec::with_capacity(end_channel - start_channel);
    for channel in start_channel..end_channel {
        let (predictor, channel_timing) = build_one_channel_predictor(context, channel)?;
        timing += channel_timing;
        predictors.push(predictor);
    }
    if trace_simobserve_setup() {
        eprintln!(
            "simobserve_setup_trace stage=channel_predictor_range start_channel={} end_channel={} channels={} total_millis={} primary_beam_millis={} orientation_millis={} fft_predictor_millis={}",
            start_channel,
            end_channel,
            end_channel - start_channel,
            elapsed_millis(range_started.elapsed()),
            elapsed_millis(timing.primary_beam),
            elapsed_millis(timing.orientation),
            elapsed_millis(timing.fft_predictor),
        );
    }
    Ok(predictors)
}

#[derive(Default, Clone, Copy)]
struct ChannelPredictorBuildTiming {
    primary_beam: Duration,
    orientation: Duration,
    fft_predictor: Duration,
}

impl std::ops::AddAssign for ChannelPredictorBuildTiming {
    fn add_assign(&mut self, rhs: Self) {
        self.primary_beam += rhs.primary_beam;
        self.orientation += rhs.orientation;
        self.fft_predictor += rhs.fft_predictor;
    }
}

fn build_one_channel_predictor(
    context: &ChannelPredictorContext<'_>,
    channel: usize,
) -> MsResult<(SyntheticChannelPredictor, ChannelPredictorBuildTiming)> {
    let mut timing = ChannelPredictorBuildTiming::default();
    let frequency_hz = context.spectral_setup.start_frequency_hz
        + channel as f64 * context.spectral_setup.channel_width_hz;
    let primary_beam_started = Instant::now();
    let beam_corrected_pixels = apply_simulator_primary_beam(
        context.model,
        context.model.pixels_for_channel(channel),
        context.phase_center_rad,
        frequency_hz,
        context.pointing_offset_rad,
        context.primary_beam,
    );
    timing.primary_beam = primary_beam_started.elapsed();
    let orientation_started = Instant::now();
    let mut casa_oriented_pixels = Array2::<f32>::zeros(beam_corrected_pixels.raw_dim());
    if context.model.ra_axis_increases_with_x {
        for x in 0..beam_corrected_pixels.shape()[0] {
            for y in 0..beam_corrected_pixels.shape()[1] {
                // CASA's simulator image prediction treats positive RA offsets
                // with the opposite image-x handedness from this crate's pure
                // imaging gridder. Conventional FITS images already have
                // CDELT1 < 0, so only positive-RA image axes need this
                // compatibility flip.
                casa_oriented_pixels[(beam_corrected_pixels.shape()[0] - 1 - x, y)] =
                    beam_corrected_pixels[(x, y)];
            }
        }
    } else {
        for x in 0..beam_corrected_pixels.shape()[0] {
            for y in 0..beam_corrected_pixels.shape()[1] {
                casa_oriented_pixels[(x, y)] = beam_corrected_pixels[(x, y)];
            }
        }
    }
    timing.orientation = orientation_started.elapsed();
    let fft_started = Instant::now();
    let predictor = StandardMfsModelPredictor::new(context.geometry, &casa_oriented_pixels)
        .map_err(|error| {
            MsError::SyntheticObservation(format!("model prediction setup failed: {error}"))
        })?;
    timing.fft_predictor = fft_started.elapsed();
    Ok((
        SyntheticChannelPredictor {
            predictor,
            phase_offset_rad: context.phase_offset_rad,
            phase_center_rad: context.phase_center_rad,
            model_reference_direction_rad: context.model.reference_direction_rad,
        },
        timing,
    ))
}

fn apply_simulator_primary_beam(
    model: &FitsModelImage,
    model_pixels: &Array2<f32>,
    phase_center_rad: [f64; 2],
    frequency_hz: f64,
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
) -> Array2<f32> {
    let mut pixels = model_pixels.clone();
    let Some(wcs) = model.direction_wcs.as_ref() else {
        return pixels;
    };
    let pointing_direction_rad = [
        phase_center_rad[0] + pointing_offset_rad[0] / phase_center_rad[1].cos(),
        phase_center_rad[1] + pointing_offset_rad[1],
    ];
    let coordinate = wcs.coordinate();
    let Ok(pointing_pixel) = coordinate.to_pixel(&pointing_direction_rad) else {
        return pixels;
    };
    let increments = coordinate.increment();
    if pointing_pixel.len() < 2 || increments.len() < 2 {
        return pixels;
    }

    for x in 0..model_pixels.shape()[0] {
        for y in 0..model_pixels.shape()[1] {
            let l = (x as f64 - pointing_pixel[0]) * increments[0];
            let m = (y as f64 - pointing_pixel[1]) * increments[1];
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

    let dimensionless_max_radius =
        CASA_VLA_Q_MAX_RADIUS_ARCMIN * 7.016 / (1.566 * 60.0) * DISH_DIAMETER_M / 24.5;
    let x = table_index as f64 * dimensionless_max_radius / (CASA_AIRY_SAMPLES - 1) as f64;
    let area_ratio = (DISH_DIAMETER_M / BLOCKAGE_DIAMETER_M).powi(2);
    let area_norm = area_ratio - 1.0;
    let length_ratio = DISH_DIAMETER_M / BLOCKAGE_DIAMETER_M;
    ((area_ratio * 2.0 * j1(x) / x - 2.0 * j1(x * length_ratio) / (x * length_ratio)) / area_norm)
        as f32
}

fn casa_model_phase_offset(model: &FitsModelImage, phase_center_rad: [f64; 2]) -> [f64; 2] {
    let Some(reference_direction_rad) = model.reference_direction_rad else {
        return [0.0, 0.0];
    };
    let ra_offset = circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0])
        * phase_center_rad[1].cos();
    let dec_offset = reference_direction_rad[1] - phase_center_rad[1];
    if model.ra_axis_increases_with_x {
        [
            ra_offset - (0.5 - CASA_GRIDFT_IMAGE_INVERSION_PHASE_PIXELS) * model.cell_size_rad[0],
            dec_offset - 0.5 * model.cell_size_rad[1],
        ]
    } else {
        // CASA GridFT uses a signed RA increment plus an internal UV negation
        // convention for conventional FITS images. A phase-residual fit against
        // CASA's full simulator path shows this leaves a sub-pixel alignment
        // offset after the larger half-pixel center alignment is accounted for.
        [
            ra_offset + CASA_GRIDFT_NEGATIVE_RA_PHASE_ALIGNMENT_PIXELS[0] * model.cell_size_rad[0],
            dec_offset + CASA_GRIDFT_NEGATIVE_RA_PHASE_ALIGNMENT_PIXELS[1] * model.cell_size_rad[1],
        ]
    }
}

fn circular_angle_delta_rad(delta: f64) -> f64 {
    (delta + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
}

fn predicted_data_values(
    predictors: Option<&[SyntheticChannelPredictor]>,
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
                let index = ms_data_index(corr, channel, num_corr);
                values[index] = visibility;
            }
        }
    }
    values
}

struct ChannelPredictionChunk {
    start_channel: usize,
    values_by_row: Vec<Vec<Complex32>>,
}

fn predicted_data_values_for_rows_with_workers(
    predictors: Option<&[SyntheticChannelPredictor]>,
    spectral_setup: &SyntheticSpectralSetup,
    row_uvws: &[[f64; 3]],
    num_corr: usize,
    worker_count: usize,
) -> Vec<Vec<Complex32>> {
    let Some(predictors) = predictors else {
        return vec![
            vec![Complex32::new(0.0, 0.0); num_corr * spectral_setup.channel_count];
            row_uvws.len()
        ];
    };
    if worker_count <= 1 {
        return row_uvws
            .iter()
            .map(|uvw| predicted_data_values(Some(predictors), spectral_setup, *uvw, num_corr))
            .collect();
    }

    let channel_count = spectral_setup.channel_count;
    let chunk_size = channel_count.div_ceil(worker_count);
    let chunks = thread::scope(|scope| {
        let mut handles = Vec::new();
        for start_channel in (0..channel_count).step_by(chunk_size) {
            let end_channel = (start_channel + chunk_size).min(channel_count);
            handles.push(scope.spawn(move || {
                predict_channel_chunk(
                    predictors,
                    spectral_setup,
                    row_uvws,
                    num_corr,
                    start_channel,
                    end_channel,
                )
            }));
        }

        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("synthetic observation prediction worker should not panic")
            })
            .collect::<Vec<_>>()
    });

    let mut values_by_row =
        vec![vec![Complex32::new(0.0, 0.0); num_corr * channel_count]; row_uvws.len()];
    for chunk in chunks {
        if chunk.values_by_row.is_empty() {
            continue;
        }
        for (row_index, row_chunk) in chunk.values_by_row.into_iter().enumerate() {
            let dst_start = chunk.start_channel * num_corr;
            let dst_end = dst_start + row_chunk.len();
            values_by_row[row_index][dst_start..dst_end].copy_from_slice(&row_chunk);
        }
    }
    values_by_row
}

fn predict_channel_chunk(
    predictors: &[SyntheticChannelPredictor],
    spectral_setup: &SyntheticSpectralSetup,
    row_uvws: &[[f64; 3]],
    num_corr: usize,
    start_channel: usize,
    end_channel: usize,
) -> ChannelPredictionChunk {
    let chunk_len = end_channel - start_channel;
    let mut values_by_row = Vec::with_capacity(row_uvws.len());
    for uvw_m in row_uvws {
        let mut row_values = vec![Complex32::new(0.0, 0.0); num_corr * chunk_len];
        for (offset, channel) in (start_channel..end_channel).enumerate() {
            let predictor = &predictors[channel];
            let visibility = predict_channel_visibility(predictor, spectral_setup, *uvw_m, channel);
            for corr in 0..num_corr {
                row_values[ms_data_index(corr, offset, num_corr)] = visibility;
            }
        }
        values_by_row.push(row_values);
    }
    ChannelPredictionChunk {
        start_channel,
        values_by_row,
    }
}

fn predict_channel_visibility(
    predictor: &SyntheticChannelPredictor,
    spectral_setup: &SyntheticSpectralSetup,
    uvw_m: [f64; 3],
    channel: usize,
) -> Complex32 {
    let frequency_hz =
        spectral_setup.start_frequency_hz + channel as f64 * spectral_setup.channel_width_hz;
    let wavelength_m = 299_792_458.0 / frequency_hz;
    let prediction_uvw_m =
        if let Some(model_reference_direction_rad) = predictor.model_reference_direction_rad {
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
        * (u_lambda * predictor.phase_offset_rad[0] + v_lambda * predictor.phase_offset_rad[1]);
    let phase_shift = Complex32::new(phase.cos() as f32, phase.sin() as f32);
    predictor.predictor.predict(u_lambda, v_lambda) * phase_shift
}

fn simobserve_channel_worker_count(channel_count: usize) -> usize {
    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let requested = std::env::var("CASA_RS_SIMOBSERVE_CHANNEL_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(available);
    let min_channels = std::env::var("CASA_RS_SIMOBSERVE_CHANNEL_PARALLEL_MIN_CHANNELS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64);
    simobserve_channel_worker_count_for(channel_count, requested, available, min_channels)
}

fn simobserve_channel_worker_count_for(
    channel_count: usize,
    requested: usize,
    available: usize,
    min_channels: usize,
) -> usize {
    if channel_count < min_channels {
        return 1;
    }
    requested
        .max(1)
        .min(available.max(1))
        .min(channel_count)
        .max(1)
}

fn trace_simobserve_setup() -> bool {
    std::env::var("CASA_RS_SIMOBSERVE_TRACE_SETUP").is_ok_and(|value| value != "0")
}

fn simobserve_row_worker_count(row_count: usize, channel_count: usize) -> usize {
    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let requested = std::env::var("CASA_RS_SIMOBSERVE_ROW_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(available);
    simobserve_row_worker_count_for(row_count, channel_count, requested, available, 64 * 1024)
}

fn simobserve_row_worker_count_for(
    row_count: usize,
    channel_count: usize,
    requested: usize,
    available: usize,
    min_values: usize,
) -> usize {
    if row_count <= 1 || row_count * channel_count < min_values {
        return 1;
    }
    requested.max(1).min(available.max(1)).min(row_count).max(1)
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
        let correlation_count = if channel_count == 0 {
            0
        } else {
            values.len() / channel_count
        };
        for (index, value) in values.iter_mut().enumerate() {
            let channel = index / correlation_count;
            let correlation = index % correlation_count;
            let logical_index = correlation * channel_count + channel;
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
                    logical_index,
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
                    let rr_index = ms_data_index(0, channel, 2);
                    let ll_index = ms_data_index(1, channel, 2);
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
    spectral_channel_count: usize,
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
    let shape = (1..=naxis)
        .map(|axis| fits_i64(&cards, &format!("NAXIS{axis}"), path).map(|value| value as usize))
        .collect::<MsResult<Vec<_>>>()?;
    if nx < 8 || ny < 8 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must be at least 8x8 pixels",
            path.display()
        )));
    }
    let channel_axis = fits_model_channel_axis(&cards, &shape, spectral_channel_count, path)?;
    let axis_cell_rad = [
        fits_axis_cell_rad(&cards, 1, path)?,
        fits_axis_cell_rad(&cards, 2, path)?,
    ];
    let direction_wcs = fits_model_direction_wcs(&cards, &shape, path)?;
    let (cell_size_rad, ra_axis_increases_with_x, reference_direction_rad) =
        if let Some(wcs) = direction_wcs.as_ref() {
            (
                fits_direction_cell_size_rad(wcs)?,
                fits_direction_ra_axis_increases_with_x(wcs, path)?,
                Some(fits_direction_center_rad(wcs, nx, ny, path)?),
            )
        } else {
            (
                [axis_cell_rad[0].abs(), axis_cell_rad[1].abs()],
                axis_cell_rad[0] > 0.0,
                None,
            )
        };
    let plane_pixel_count = nx
        .checked_mul(ny)
        .ok_or_else(|| MsError::SyntheticObservation("model image shape overflows".to_string()))?;
    let total_pixel_count = shape
        .iter()
        .try_fold(1usize, |accumulator, axis_len| {
            accumulator.checked_mul(*axis_len)
        })
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
    let data_len = total_pixel_count * bytes_per_pixel;
    if bytes.len() < data_offset + data_len {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} is truncated before primary image data",
            path.display()
        )));
    }

    let bscale = fits_optional_f64(&cards, "BSCALE").unwrap_or(1.0);
    let bzero = fits_optional_f64(&cards, "BZERO").unwrap_or(0.0);
    let data = &bytes[data_offset..data_offset + data_len];
    let channel_plane_count = channel_axis
        .map(|axis| shape[axis])
        .filter(|count| *count > 1)
        .unwrap_or(1);
    let mut channel_planes = Vec::with_capacity(channel_plane_count);
    for channel in 0..channel_plane_count {
        let mut plane = Array2::<f32>::zeros((nx, ny));
        for y in 0..ny {
            for x in 0..nx {
                let index = fits_model_flat_index(&shape, x, y, channel_axis, channel);
                plane[(x, y)] = fits_model_pixel_value(data, bitpix, index, bscale, bzero);
            }
        }
        debug_assert_eq!(plane.len(), plane_pixel_count);
        channel_planes.push(plane);
    }
    if let Some(target_peak) = model_peak_jy_per_pixel {
        let current_peak = channel_planes
            .iter()
            .flat_map(|plane| plane.iter())
            .copied()
            .fold(0.0f32, |peak, value| peak.max(value.abs()));
        if current_peak <= 0.0 || !current_peak.is_finite() {
            return Err(MsError::SyntheticObservation(format!(
                "model image {} cannot be scaled because its peak brightness is zero or non-finite",
                path.display()
            )));
        }
        let scale = target_peak / current_peak;
        for plane in &mut channel_planes {
            plane.mapv_inplace(|value| value * scale);
        }
    }
    let pixels = channel_planes
        .first()
        .cloned()
        .unwrap_or_else(|| Array2::<f32>::zeros((nx, ny)));

    Ok(FitsModelImage {
        pixels,
        channel_planes,
        cell_size_rad,
        direction_wcs,
        ra_axis_increases_with_x,
        reference_direction_rad,
    })
}

fn fits_model_channel_axis(
    cards: &[String],
    shape: &[usize],
    spectral_channel_count: usize,
    path: &Path,
) -> MsResult<Option<usize>> {
    let mut spectral_axis = None;
    for axis in 3..=shape.len() {
        if fits_axis_is_spectral(cards, axis) {
            spectral_axis = Some(axis - 1);
            break;
        }
    }
    if spectral_axis.is_none() && spectral_channel_count > 1 {
        spectral_axis = (2..shape.len()).find(|axis| shape[*axis] == spectral_channel_count);
    }

    for (axis, axis_len) in shape.iter().copied().enumerate().skip(2) {
        if Some(axis) == spectral_axis {
            if axis_len != 1 && axis_len != spectral_channel_count {
                return Err(MsError::SyntheticObservation(format!(
                    "model image {} spectral axis length {axis_len} does not match requested channel count {spectral_channel_count}",
                    path.display()
                )));
            }
        } else if axis_len != 1 && !fits_axis_is_stokes(cards, axis + 1) {
            return Err(MsError::SyntheticObservation(format!(
                "model image {} has unsupported non-spectral FITS axis {} with length {}; only singleton or STOKES axes are supported",
                path.display(),
                axis + 1,
                axis_len
            )));
        }
    }
    Ok(spectral_axis)
}

fn fits_axis_is_spectral(cards: &[String], axis: usize) -> bool {
    fits_string(cards, &format!("CTYPE{axis}"))
        .map(|ctype| {
            let upper = ctype.to_ascii_uppercase();
            upper.starts_with("FREQ")
                || upper.starts_with("VRAD")
                || upper.starts_with("VOPT")
                || upper.starts_with("VELO")
        })
        .unwrap_or(false)
}

fn fits_axis_is_stokes(cards: &[String], axis: usize) -> bool {
    fits_string(cards, &format!("CTYPE{axis}"))
        .map(|ctype| ctype.to_ascii_uppercase().starts_with("STOKES"))
        .unwrap_or(false)
}

fn fits_model_flat_index(
    shape: &[usize],
    x: usize,
    y: usize,
    channel_axis: Option<usize>,
    channel: usize,
) -> usize {
    let mut index = 0usize;
    let mut stride = 1usize;
    for (axis, axis_len) in shape.iter().copied().enumerate() {
        let axis_index = match axis {
            0 => x,
            1 => y,
            _ if Some(axis) == channel_axis => channel,
            _ => 0,
        };
        debug_assert!(axis_index < axis_len);
        index += axis_index * stride;
        stride *= axis_len;
    }
    index
}

fn fits_model_pixel_value(data: &[u8], bitpix: i64, index: usize, bscale: f64, bzero: f64) -> f32 {
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
    (raw * bscale + bzero) as f32
}

fn fits_model_direction_wcs(
    cards: &[String],
    shape: &[usize],
    path: &Path,
) -> MsResult<Option<FitsModelDirectionWcs>> {
    if !(fits_value(cards, "CRVAL1").is_some()
        && fits_value(cards, "CRVAL2").is_some()
        && fits_value(cards, "CTYPE1").is_some()
        && fits_value(cards, "CTYPE2").is_some())
    {
        return Ok(None);
    }
    let header_cards = cards.iter().map(String::as_str).collect::<Vec<_>>();
    let header = FitsHeader::from_cards(&header_cards);
    let coordinate_system = from_fits_header(&header, shape).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to parse model-image FITS WCS for {}: {error}",
            path.display()
        ))
    })?;
    let Some(coordinate_index) = coordinate_system.find_coordinate(CoordinateType::Direction)
    else {
        return Ok(None);
    };
    Ok(Some(FitsModelDirectionWcs {
        coordinate_system,
        coordinate_index,
    }))
}

fn fits_direction_cell_size_rad(wcs: &FitsModelDirectionWcs) -> MsResult<[f64; 2]> {
    let increment = wcs.coordinate().increment();
    if increment.len() < 2 {
        return Err(MsError::SyntheticObservation(
            "model image direction coordinate has fewer than two increments".to_string(),
        ));
    }
    Ok([increment[0].abs(), increment[1].abs()])
}

fn fits_direction_center_rad(
    wcs: &FitsModelDirectionWcs,
    nx: usize,
    ny: usize,
    path: &Path,
) -> MsResult<[f64; 2]> {
    let center_pixel = [0.5 * nx as f64, 0.5 * ny as f64];
    let world = wcs.coordinate().to_world(&center_pixel).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to resolve model-image center direction for {}: {error}",
            path.display()
        ))
    })?;
    Ok([world[0], world[1]])
}

fn fits_direction_ra_axis_increases_with_x(
    wcs: &FitsModelDirectionWcs,
    path: &Path,
) -> MsResult<bool> {
    let coordinate = wcs.coordinate();
    let reference_pixel = coordinate.reference_pixel();
    let reference_value = coordinate.reference_value();
    if reference_pixel.len() < 2 || reference_value.len() < 2 {
        return Err(MsError::SyntheticObservation(
            "model image direction coordinate has fewer than two axes".to_string(),
        ));
    }
    let x_step_world = coordinate
        .to_world(&[reference_pixel[0] + 1.0, reference_pixel[1]])
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to resolve model-image RA axis handedness for {}: {error}",
                path.display()
            ))
        })?;
    Ok(circular_angle_delta_rad(x_step_world[0] - reference_value[0]) > 0.0)
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

fn elapsed_millis(duration: Duration) -> u128 {
    duration.as_millis()
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

#[cfg(test)]
fn complex_array_from_ms_data_storage(
    values: &[Complex32],
    num_corr: usize,
    num_chan: usize,
) -> Value {
    use ndarray::{IxDyn, ShapeBuilder};

    let shape = IxDyn(&[num_corr, num_chan]).strides(IxDyn(&[1, num_corr]));
    Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn ms_data_index(correlation: usize, channel: usize, num_corr: usize) -> usize {
    channel * num_corr + correlation
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
    fn row_noise_seed_varies_by_row_coordinates() {
        let base = row_noise_seed(42, 0, 0, 1, 0);
        assert_ne!(base, row_noise_seed(43, 0, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 1, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 0, 1, 2, 0));
        assert_ne!(base, row_noise_seed(42, 0, 0, 1, 1));
        assert_eq!(base, row_noise_seed(42, 0, 0, 1, 0));
    }

    #[test]
    fn fits_model_cube_uses_per_channel_planes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("cube.fits");
        write_test_fits_cube(&path, 3);

        let model = read_fits_model_image(&path, None, 3).expect("read model cube");

        assert_eq!(model.channel_planes.len(), 3);
        assert_eq!(model.pixels_for_channel(0)[(2, 3)], 32.0);
        assert_eq!(model.pixels_for_channel(1)[(2, 3)], 132.0);
        assert_eq!(model.pixels_for_channel(2)[(2, 3)], 232.0);
    }

    #[test]
    fn fits_model_plane_repeats_for_all_requested_channels() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("plane.fits");
        write_test_fits_plane(&path);

        let model = read_fits_model_image(&path, None, 4).expect("read model plane");

        assert_eq!(model.channel_planes.len(), 1);
        assert_eq!(model.pixels_for_channel(0)[(2, 3)], 32.0);
        assert_eq!(model.pixels_for_channel(3)[(2, 3)], 32.0);
    }

    #[test]
    fn simobserve_channel_worker_count_is_bounded_by_frequency_work() {
        assert_eq!(simobserve_channel_worker_count_for(8, 8, 8, 64), 1);
        assert_eq!(simobserve_channel_worker_count_for(64, 8, 8, 64), 8);
        assert_eq!(simobserve_channel_worker_count_for(64, 16, 4, 64), 4);
        assert_eq!(simobserve_channel_worker_count_for(3, 16, 16, 1), 3);
        assert_eq!(simobserve_channel_worker_count_for(64, 0, 0, 64), 1);
    }

    #[test]
    fn simobserve_row_worker_count_is_bounded_by_row_work() {
        assert_eq!(simobserve_row_worker_count_for(8, 16, 8, 8, 1024), 1);
        assert_eq!(simobserve_row_worker_count_for(351, 512, 16, 10, 1024), 10);
        assert_eq!(simobserve_row_worker_count_for(351, 512, 16, 4, 1024), 4);
        assert_eq!(simobserve_row_worker_count_for(3, 512, 16, 16, 1024), 3);
        assert_eq!(simobserve_row_worker_count_for(351, 512, 0, 16, 1024), 1);
    }

    #[test]
    fn parallel_row_corruption_matches_serial_corruption() {
        let config = SyntheticCorruptionConfig {
            seed: 11,
            noise: Some(SyntheticNoiseCorruption {
                mode: SyntheticNoiseMode::SimpleNoise,
                simplenoise_jy: 0.01,
            }),
            gain: Some(SyntheticGainCorruption {
                mode: SyntheticGainMode::Fbm,
                interval_seconds: 10.0,
                amplitude: [0.03, 0.02],
            }),
            bandpass: Some(SyntheticBandpassCorruption {
                mode: SyntheticBandpassMode::Calculate,
                interval_seconds: 10.0,
                amplitude: [0.02, 0.01],
            }),
            leakage: Some(SyntheticPolarizationLeakageCorruption {
                mode: SyntheticPolarizationLeakageMode::Constant,
                amplitude: [0.02, 0.01],
                offset: [0.0, 0.0],
            }),
            pointing: None,
        };
        let corruption = SyntheticCorruptionState::new(&config, 8, 16, 5);
        let row_specs = (0..7)
            .flat_map(|antenna1| {
                ((antenna1 + 1)..8).map(move |antenna2| MainRowVisibilitySpec {
                    antenna1,
                    antenna2,
                    field_id: 0,
                    time: 0.0,
                    uvw: [antenna1 as f64, antenna2 as f64, 0.0],
                })
            })
            .collect::<Vec<_>>();
        let base_rows = row_specs
            .iter()
            .enumerate()
            .map(|(row, _)| {
                (0..32)
                    .map(|index| Complex32::new(row as f32, index as f32 * 0.25))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut serial = base_rows.clone();
        let mut parallel = base_rows;

        let serial_nonzero =
            apply_corruption_and_count_rows(Some(&corruption), &row_specs, &mut serial, 16, 3);
        let parallel_nonzero = apply_corruption_and_count_rows_with_workers(
            Some(&corruption),
            &row_specs,
            &mut parallel,
            16,
            3,
        );

        assert_eq!(serial_nonzero, parallel_nonzero);
        assert_eq!(serial, parallel);
    }

    #[test]
    fn parallel_channel_prediction_matches_serial_prediction() {
        let channel_count = 96;
        let spectral_setup = SyntheticSpectralSetup {
            name: "test".to_string(),
            start_frequency_hz: 1.0e9,
            channel_width_hz: 1.0e6,
            channel_count,
        };
        let geometry = ImageGeometry {
            image_shape: [16, 16],
            cell_size_rad: [1.0e-5, 1.0e-5],
        };
        let model = Array2::<f32>::from_shape_fn((16, 16), |(x, y)| {
            let dx = x as f32 - 7.5;
            let dy = y as f32 - 7.5;
            (-0.03 * (dx * dx + dy * dy)).exp()
        });
        let predictors = (0..channel_count)
            .map(|_| SyntheticChannelPredictor {
                predictor: StandardMfsModelPredictor::new(geometry, &model).unwrap(),
                phase_offset_rad: [1.0e-6, -2.0e-6],
                phase_center_rad: [1.0, -0.5],
                model_reference_direction_rad: None,
            })
            .collect::<Vec<_>>();
        let row_uvws = [[10.0, 20.0, 0.0], [100.0, -30.0, 5.0], [-55.0, 7.0, -3.0]];

        let serial = predicted_data_values_for_rows_with_workers(
            Some(&predictors),
            &spectral_setup,
            &row_uvws,
            2,
            1,
        );
        let parallel = predicted_data_values_for_rows_with_workers(
            Some(&predictors),
            &spectral_setup,
            &row_uvws,
            2,
            4,
        );

        assert_eq!(serial, parallel);
    }

    #[test]
    fn ms_data_array_uses_casacore_storage_order() {
        let values = vec![
            Complex32::new(10.0, 0.0),
            Complex32::new(20.0, 0.0),
            Complex32::new(11.0, 0.0),
            Complex32::new(21.0, 0.0),
            Complex32::new(12.0, 0.0),
            Complex32::new(22.0, 0.0),
        ];
        let Value::Array(ArrayValue::Complex32(array)) =
            complex_array_from_ms_data_storage(&values, 2, 3)
        else {
            panic!("expected complex DATA array");
        };

        assert_eq!(array.shape(), &[2, 3]);
        assert_eq!(array.strides(), &[1, 2]);
        assert_eq!(array[[0, 0]], Complex32::new(10.0, 0.0));
        assert_eq!(array[[1, 0]], Complex32::new(20.0, 0.0));
        assert_eq!(array[[0, 2]], Complex32::new(12.0, 0.0));
        assert_eq!(array[[1, 2]], Complex32::new(22.0, 0.0));
        assert_eq!(array.as_slice_memory_order().unwrap(), values.as_slice());
    }

    fn write_test_fits_plane(path: &Path) {
        let mut cards = test_fits_cards(&[
            ("SIMPLE", "T"),
            ("BITPIX", "-32"),
            ("NAXIS", "2"),
            ("NAXIS1", "8"),
            ("NAXIS2", "8"),
            ("CDELT1", "-0.1"),
            ("CUNIT1", "'deg'"),
            ("CDELT2", "0.1"),
            ("CUNIT2", "'deg'"),
        ]);
        let mut bytes = Vec::new();
        bytes.append(&mut cards);
        for y in 0..8 {
            for x in 0..8 {
                bytes.extend_from_slice(&((x + 10 * y) as f32).to_bits().to_be_bytes());
            }
        }
        pad_fits_block(&mut bytes);
        std::fs::write(path, bytes).expect("write test FITS plane");
    }

    fn write_test_fits_cube(path: &Path, channels: usize) {
        let channels_text = channels.to_string();
        let mut cards = test_fits_cards(&[
            ("SIMPLE", "T"),
            ("BITPIX", "-32"),
            ("NAXIS", "4"),
            ("NAXIS1", "8"),
            ("NAXIS2", "8"),
            ("NAXIS3", "1"),
            ("NAXIS4", channels_text.as_str()),
            ("CDELT1", "-0.1"),
            ("CUNIT1", "'deg'"),
            ("CDELT2", "0.1"),
            ("CUNIT2", "'deg'"),
            ("CTYPE3", "'STOKES'"),
            ("CTYPE4", "'FREQ'"),
        ]);
        let mut bytes = Vec::new();
        bytes.append(&mut cards);
        for channel in 0..channels {
            for y in 0..8 {
                for x in 0..8 {
                    let value = (100 * channel + x + 10 * y) as f32;
                    bytes.extend_from_slice(&value.to_bits().to_be_bytes());
                }
            }
        }
        pad_fits_block(&mut bytes);
        std::fs::write(path, bytes).expect("write test FITS cube");
    }

    fn test_fits_cards(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut text = String::new();
        for (key, value) in entries {
            text.push_str(
                &format!("{key:<8}= {value:>20}")
                    .chars()
                    .take(80)
                    .collect::<String>(),
            );
            let last_card_len = text.len() % 80;
            if last_card_len != 0 {
                text.push_str(&" ".repeat(80 - last_card_len));
            }
        }
        text.push_str(&format!("{:<80}", "END"));
        let mut bytes = text.into_bytes();
        pad_fits_block(&mut bytes);
        bytes
    }

    fn pad_fits_block(bytes: &mut Vec<u8>) {
        let pad = (2880 - bytes.len() % 2880) % 2880;
        bytes.extend(std::iter::repeat_n(b' ', pad));
    }

    #[test]
    fn simulator_primary_beam_is_centered_on_fits_reference_pixel() {
        let mut pixels = Array2::<f32>::zeros((5, 5));
        pixels[(1, 1)] = 1.0;
        pixels[(2, 1)] = 1.0;
        let mut coordinate_system = CoordinateSystem::new();
        coordinate_system.add_coordinate(Box::new(casa_coordinates::DirectionCoordinate::new(
            DirectionRef::J2000,
            casa_coordinates::Projection::new(casa_coordinates::ProjectionType::SIN),
            [1.25, -0.3],
            [-1.0e-3, 1.0e-3],
            [1.0, 1.0],
        )));
        let model = FitsModelImage {
            pixels,
            channel_planes: Vec::new(),
            cell_size_rad: [1.0e-3, 1.0e-3],
            direction_wcs: Some(FitsModelDirectionWcs {
                coordinate_system,
                coordinate_index: 0,
            }),
            ra_axis_increases_with_x: false,
            reference_direction_rad: Some([1.25, -0.3]),
        };
        let corrected = apply_simulator_primary_beam(
            &model,
            &model.pixels,
            [1.25, -0.3],
            43.0e9,
            [0.0, 0.0],
            SyntheticPrimaryBeam {
                use_casa_vla_q_table: true,
                dish_diameter_m: 25.0,
                blockage_diameter_m: 2.36,
            },
        );

        assert_eq!(corrected[(1, 1)], 1.0);
        assert!(
            corrected[(2, 1)] < 1.0,
            "adjacent pixels should be attenuated away from the FITS reference pixel"
        );
    }

    #[test]
    fn casa_vla_q_primary_beam_uses_common_pb_table_support() {
        let at_half_max_radius =
            casa_vla_q_primary_beam_voltage_pattern((0.5 * 0.8564_f64).to_radians(), 1.0e9);
        let at_support = casa_vla_q_primary_beam_voltage_pattern(0.8564_f64.to_radians(), 1.0e9);

        assert!((at_half_max_radius - 0.596_901_4).abs() < 1.0e-7);
        assert!((at_support - -0.017_125_657).abs() < 1.0e-8);
    }

    #[test]
    fn alma_primary_beam_uses_casa_effective_diameter_for_model_prediction() {
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            PathBuf::from("model.fits"),
            PathBuf::from("out.ms"),
            vec![SyntheticAntenna {
                name: "A000".to_string(),
                station: "A000".to_string(),
                position_m: [0.0, 0.0, 0.0],
                dish_diameter_m: 12.0,
            }],
        );
        request.telescope_name = "ALMA".to_string();

        let primary_beam = synthetic_primary_beam(&request);

        assert_eq!(primary_beam.dish_diameter_m, 10.7);
        assert_eq!(primary_beam.blockage_diameter_m, 0.75);
        assert!(!primary_beam.use_casa_vla_q_table);
    }

    #[test]
    fn shadowing_marks_nearer_antenna_and_flags_rows_involving_it() {
        let antennas = vec![
            SyntheticAntenna::vla("A0", "A0", [0.0, 0.0, 0.0]),
            SyntheticAntenna::vla("A1", "A1", [0.0, 0.0, 0.0]),
            SyntheticAntenna::vla("A2", "A2", [0.0, 0.0, 0.0]),
        ];
        let rows = vec![
            MainRowVisibilitySpec {
                antenna1: 0,
                antenna2: 1,
                field_id: 0,
                time: 0.0,
                uvw: [10.0, 0.0, 1.0],
            },
            MainRowVisibilitySpec {
                antenna1: 0,
                antenna2: 2,
                field_id: 0,
                time: 0.0,
                uvw: [100.0, 0.0, -1.0],
            },
            MainRowVisibilitySpec {
                antenna1: 1,
                antenna2: 2,
                field_id: 0,
                time: 0.0,
                uvw: [10.0, 0.0, -1.0],
            },
        ];

        assert_eq!(
            shadowed_antennas_for_rows(&rows, &antennas),
            vec![true, false, true]
        );
    }
}
