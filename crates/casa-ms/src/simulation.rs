// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic MeasurementSet generation for tutorial simulation workflows.
//!
//! This module owns the first reusable MS-writing slice of the VLA simulation
//! vertical. It mirrors the CASA `simobserve` setup order at the data-model
//! level: validate a model image path, set array configuration, define the
//! spectral window and field, sample the requested observing time range, and
//! write CASA-compatible MS subtables plus uncorrupted visibility rows.

use casa_coordinates::fits::{FitsHeader, from_fits_header};
use casa_coordinates::{
    Coordinate, CoordinateSystem, CoordinateType, DirectionCoordinate, Projection, ProjectionType,
};
use casa_imaging::{
    ImageGeometry, PrimaryBeamModel, PrimaryBeamVoltagePattern, StandardMfsModelPredictor,
};
use casa_tables::{
    ColumnOverrides, GeneratedScalarColumn, GeneratedScalarValueRun, StreamedTiledPrimitiveColumn,
    StreamedTiledPrimitiveType, StreamedTiledShapeComplex32Column, StreamingTiledPrimitiveWriter,
    StreamingTiledShapeComplex32Writer, install_streamed_tiled_column_primitive_column,
    install_streamed_tiled_shape_complex32_column, install_streamed_tiled_shape_primitive_column,
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
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::column_def::{ColumnDef, ColumnKind};
use crate::error::{MsError, MsResult};
use crate::flagging::shadowed_antennas_from_projected_baselines;
use crate::schema::{self, SubtableId};
use crate::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};

const DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD: f64 = 20.0_f64.to_radians();
const DEFAULT_SIMOBSERVE_IO_QUEUE_DEPTH: usize = 16;
const SIDEREAL_DAY_SECONDS: f64 = 86_164.090_5;

fn default_simobserve_elevation_limit_rad() -> f64 {
    DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD
}

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

    /// Return the central reference frequency in Hz.
    pub fn reference_frequency_hz(&self) -> f64 {
        self.start_frequency_hz + (self.channel_count / 2) as f64 * self.channel_width_hz
    }
}

/// Polarization/correlation layout for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticPolarizationSetup {
    /// Receptor basis used in FEED and POLARIZATION metadata.
    #[serde(default)]
    pub basis: SyntheticPolarizationBasis,
    /// Number of correlations written per visibility row. Supported values are
    /// 1, 2, and 4.
    pub correlation_count: usize,
}

impl Default for SyntheticPolarizationSetup {
    fn default() -> Self {
        Self {
            basis: SyntheticPolarizationBasis::Circular,
            correlation_count: 2,
        }
    }
}

impl SyntheticPolarizationSetup {
    /// Build a setup from the dialog-facing correlation count.
    pub fn new(basis: SyntheticPolarizationBasis, correlation_count: usize) -> MsResult<Self> {
        let setup = Self {
            basis,
            correlation_count,
        };
        setup.validate()?;
        Ok(setup)
    }

    /// Validate that the correlation layout is supported by the native writer.
    pub fn validate(&self) -> MsResult<()> {
        match self.correlation_count {
            1 | 2 | 4 => Ok(()),
            other => Err(MsError::SyntheticObservation(format!(
                "polarization correlation_count {other} is unsupported; expected 1, 2, or 4"
            ))),
        }
    }

    fn correlation_types(&self) -> Vec<i32> {
        match (self.basis, self.correlation_count) {
            (SyntheticPolarizationBasis::Circular, 1) => vec![5],
            (SyntheticPolarizationBasis::Circular, 2) => vec![5, 8],
            (SyntheticPolarizationBasis::Circular, 4) => vec![5, 6, 7, 8],
            (SyntheticPolarizationBasis::Linear, 1) => vec![9],
            (SyntheticPolarizationBasis::Linear, 2) => vec![9, 12],
            (SyntheticPolarizationBasis::Linear, 4) => vec![9, 10, 11, 12],
            _ => Vec::new(),
        }
    }

    fn correlation_products(&self) -> Vec<i32> {
        match self.correlation_count {
            1 => vec![0, 0],
            2 => vec![0, 1, 0, 1],
            4 => vec![0, 0, 1, 1, 0, 1, 0, 1],
            _ => Vec::new(),
        }
    }

    fn receptor_types(&self) -> [&'static str; 2] {
        match self.basis {
            SyntheticPolarizationBasis::Circular => ["R", "L"],
            SyntheticPolarizationBasis::Linear => ["X", "Y"],
        }
    }
}

/// Receptor basis used for synthetic polarization metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticPolarizationBasis {
    /// Circular receptors: R/L and RR/RL/LR/LL Stokes codes.
    #[default]
    Circular,
    /// Linear receptors: X/Y and XX/XY/YX/YY Stokes codes.
    Linear,
}

/// Worker selection policy for native synthetic MS generation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticWorkerPolicy {
    /// Choose worker counts from request bounds, environment, and available CPU parallelism.
    #[default]
    Auto,
    /// Use the explicit request worker counts, clamped only to the available work.
    Fixed,
}

/// Observation row topology for native synthetic MS generation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticObservationMode {
    /// Cross-correlation interferometric rows for every antenna pair.
    #[default]
    Interferometric,
    /// Single-dish total-power style autocorrelation rows.
    TotalPower,
}

/// Sky model source used by the native simulator.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyntheticSkyModel {
    /// Sampled FITS image or cube model.
    FitsImage {
        /// FITS image or cube path.
        path: PathBuf,
        /// Optional peak brightness scaling in Jy/pixel.
        #[serde(default)]
        model_peak_jy_per_pixel: Option<f32>,
        /// Optional sampled-image reference direction as `[right_ascension_rad, declination_rad]`.
        #[serde(default)]
        direction_reference_rad: Option<[f64; 2]>,
        /// Optional sampled-image cell size as absolute `[ra_cell_rad, dec_cell_rad]`.
        #[serde(default)]
        cell_size_rad: Option<[f64; 2]>,
    },
    /// Exact analytic point-source and Gaussian component model.
    AnalyticComponents {
        /// Optional JSON component-model path.
        #[serde(default)]
        path: Option<PathBuf>,
        /// Optional component-model schema version.
        #[serde(default)]
        schema_version: Option<u32>,
        /// Optional component-model name.
        #[serde(default)]
        name: Option<String>,
        /// Inline analytic components. If empty, `path` is loaded.
        #[serde(default)]
        components: Vec<SyntheticAnalyticComponent>,
    },
}

impl SyntheticSkyModel {
    /// Return the backing file path when this model has one.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::FitsImage { path, .. } => Some(path.as_path()),
            Self::AnalyticComponents { path, .. } => path.as_deref(),
        }
    }

    /// Return the stable JSON kind name for reporting.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::FitsImage { .. } => "fits_image",
            Self::AnalyticComponents { .. } => "analytic_components",
        }
    }
}

/// File-level analytic component model.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SyntheticAnalyticComponentModel {
    /// Optional schema version for component-model files.
    #[serde(default)]
    pub schema_version: Option<u32>,
    /// Optional model name for provenance.
    #[serde(default)]
    pub name: Option<String>,
    /// Analytic components in direction-cosine coordinates relative to phase center.
    pub components: Vec<SyntheticAnalyticComponent>,
}

/// One exact analytic sky component.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyntheticAnalyticComponent {
    /// Delta-function component.
    Point {
        /// Optional component name for diagnostics.
        #[serde(default)]
        name: Option<String>,
        /// Direction-cosine offset from phase center in radians.
        l_rad: f64,
        /// Direction-cosine offset from phase center in radians.
        m_rad: f64,
        /// Per-channel flux model.
        spectrum: SyntheticAnalyticSpectrum,
    },
    /// Elliptical Gaussian component parameterized by image-plane FWHM.
    Gaussian {
        /// Optional component name for diagnostics.
        #[serde(default)]
        name: Option<String>,
        /// Direction-cosine offset from phase center in radians.
        l_rad: f64,
        /// Direction-cosine offset from phase center in radians.
        m_rad: f64,
        /// Major-axis full width at half maximum in radians.
        major_fwhm_rad: f64,
        /// Minor-axis full width at half maximum in radians. Defaults to circular.
        #[serde(default)]
        minor_fwhm_rad: f64,
        /// Gaussian position angle in radians.
        #[serde(default)]
        position_angle_rad: f64,
        /// Per-channel integrated flux model.
        spectrum: SyntheticAnalyticSpectrum,
    },
}

/// Per-component spectral model for analytic components.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SyntheticAnalyticSpectrum {
    /// Continuum flux density in Jy at `reference_frequency_hz`.
    pub flux_jy: f64,
    /// Continuum spectral index.
    #[serde(default)]
    pub spectral_index: f64,
    /// Optional spectral-index reference frequency in Hz.
    #[serde(default)]
    pub reference_frequency_hz: Option<f64>,
    /// Additive Gaussian emission-line peak flux density in Jy.
    #[serde(default)]
    pub line_peak_jy: f64,
    /// Emission-line center as a fraction of the channel range.
    #[serde(default = "default_line_center_fraction")]
    pub line_center_fraction: f64,
    /// Emission-line sigma as a fraction of the channel range.
    #[serde(default = "default_line_sigma_fraction")]
    pub line_sigma_fraction: f64,
    /// Subtractive Gaussian absorption-line peak flux density in Jy.
    #[serde(default)]
    pub absorption_peak_jy: f64,
    /// Absorption-line center as a fraction of the channel range.
    #[serde(default = "default_line_center_fraction")]
    pub absorption_center_fraction: f64,
    /// Absorption-line sigma as a fraction of the channel range.
    #[serde(default = "default_line_sigma_fraction")]
    pub absorption_sigma_fraction: f64,
}

fn default_line_center_fraction() -> f64 {
    0.5
}

fn default_line_sigma_fraction() -> f64 {
    0.1
}

impl SyntheticAnalyticSpectrum {
    /// Evaluate the component flux density for one spectral channel.
    pub fn flux_for_channel(&self, spectral_setup: &SyntheticSpectralSetup, channel: usize) -> f64 {
        let reference_frequency_hz = self
            .reference_frequency_hz
            .unwrap_or_else(|| spectral_setup.reference_frequency_hz());
        let frequency_hz =
            spectral_setup.start_frequency_hz + channel as f64 * spectral_setup.channel_width_hz;
        let continuum = if reference_frequency_hz > 0.0 && frequency_hz > 0.0 {
            self.flux_jy * (frequency_hz / reference_frequency_hz).powf(self.spectral_index)
        } else {
            self.flux_jy
        };
        continuum
            + gaussian_channel_profile(
                self.line_peak_jy,
                self.line_center_fraction,
                self.line_sigma_fraction,
                spectral_setup.channel_count,
                channel,
            )
            - gaussian_channel_profile(
                self.absorption_peak_jy,
                self.absorption_center_fraction,
                self.absorption_sigma_fraction,
                spectral_setup.channel_count,
                channel,
            )
    }
}

fn gaussian_channel_profile(
    peak_jy: f64,
    center_fraction: f64,
    sigma_fraction: f64,
    channel_count: usize,
    channel: usize,
) -> f64 {
    if peak_jy == 0.0 || sigma_fraction <= 0.0 || channel_count == 0 {
        return 0.0;
    }
    let channel_fraction = if channel_count == 1 {
        0.5
    } else {
        channel as f64 / (channel_count - 1) as f64
    };
    let offset = (channel_fraction - center_fraction) / sigma_fraction;
    peak_jy * (-0.5 * offset * offset).exp()
}

/// Request for generating a synthetic MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationRequest {
    /// Existing model image path that defines the tutorial model provenance.
    pub model_image: PathBuf,
    /// Preferred sky model. When absent, `model_image` keeps the legacy FITS
    /// image behavior.
    #[serde(default)]
    pub model: Option<SyntheticSkyModel>,
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
    /// Minimum antenna elevation in radians for scheduled samples and flags.
    #[serde(default = "default_simobserve_elevation_limit_rad")]
    pub elevation_limit_rad: f64,
    /// Permit continuous tracks that include samples below the elevation limit.
    ///
    /// When false, the simulator schedules as many above-elevation transit
    /// sessions as needed to accumulate the requested on-source duration.
    #[serde(default)]
    pub allow_below_elevation_limit: bool,
    /// Antenna configuration.
    pub antennas: Vec<SyntheticAntenna>,
    /// Spectral-window setup.
    pub spectral_setup: SyntheticSpectralSetup,
    /// Polarization/correlation setup.
    #[serde(default)]
    pub polarization_setup: SyntheticPolarizationSetup,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    pub predict_model: bool,
    /// Optional deterministic corruptions applied to predicted visibility data.
    #[serde(default)]
    pub corruption: Option<SyntheticCorruptionConfig>,
    /// Worker selection policy for native row/channel parallelism.
    #[serde(default)]
    pub worker_policy: SyntheticWorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
    /// Explicit row worker count when `worker_policy` is `fixed`, or an upper
    /// bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Explicit channel prediction worker count when `worker_policy` is
    /// `fixed`, or an upper bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub channel_workers: Option<usize>,
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
            model: None,
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
            elevation_limit_rad: default_simobserve_elevation_limit_rad(),
            allow_below_elevation_limit: false,
            antennas,
            spectral_setup: SyntheticSpectralSetup {
                name: "Qband".to_string(),
                start_frequency_hz: 44.0e9,
                channel_width_hz: 128.0e6,
                channel_count: 1,
            },
            polarization_setup: SyntheticPolarizationSetup::default(),
            predict_model: true,
            corruption: None,
            worker_policy: SyntheticWorkerPolicy::Auto,
            observation_mode: SyntheticObservationMode::Interferometric,
            row_workers: None,
            channel_workers: None,
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
    /// Active sky-model kind used for prediction.
    #[serde(default)]
    pub model_kind: String,
    /// Number of antennas written.
    pub antenna_count: usize,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
    /// Number of visibility-row pairs per time sample.
    pub baseline_count: usize,
    /// Number of time samples written.
    pub time_sample_count: usize,
    /// Number of main-table rows written.
    pub main_row_count: usize,
    /// Number of channels written in the spectral window.
    pub channel_count: usize,
    /// Number of correlations written per visibility row.
    #[serde(default)]
    pub correlation_count: usize,
    /// Number of complex visibility cells with non-zero predicted model values.
    pub nonzero_visibility_count: usize,
    /// Number of MAIN rows whose `FLAG_ROW` is true.
    #[serde(default)]
    pub flagged_row_count: usize,
    /// Number of MAIN rows flagged because one or both antennas were below the
    /// elevation limit.
    #[serde(default)]
    pub elevation_flagged_row_count: usize,
    /// Number of MAIN rows flagged because one or both antennas were shadowed.
    #[serde(default)]
    pub shadow_flagged_row_count: usize,
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
    /// Wall-clock time spent inside prediction worker scopes.
    #[serde(default)]
    pub prediction_worker_wall_millis: u128,
    /// Time spent gathering channel-worker chunks into full row arrays.
    #[serde(default)]
    pub prediction_gather_millis: u128,
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
    /// Per-column timing for the streamed tiled MAIN column writers.
    #[serde(default)]
    pub data_io_columns: Vec<SyntheticMainColumnIoTimingReport>,
    /// Time spent constructing MAIN rows and appending them to the table.
    pub main_write_millis: u128,
    /// Time spent building scalar-column override vectors.
    #[serde(default)]
    pub scalar_column_millis: u128,
    /// Time spent appending placeholder MAIN rows.
    #[serde(default)]
    pub main_row_add_millis: u128,
}

/// Per-column streamed MAIN table I/O timing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticMainColumnIoTimingReport {
    /// MAIN table column name.
    pub column: String,
    /// Time spent packing rows into tiled storage buffers.
    pub assemble_millis: u128,
    /// Time spent writing tile buffers to disk.
    pub write_millis: u128,
    /// Bytes written for this streamed column.
    pub bytes_written: u64,
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

    let time_sample_count =
        time_sample_count(request.duration_seconds, request.integration_seconds);
    let sample_times = observation_sample_times(request, time_sample_count)?;

    let metadata_started = Instant::now();
    populate_antennas(&mut ms, &request.antennas)?;
    populate_field(&mut ms, request)?;
    populate_pointing(&mut ms, request, &sample_times)?;
    populate_spectral_window(&mut ms, &request.spectral_setup)?;
    populate_polarization(&mut ms, &request.polarization_setup)?;
    populate_data_description(&mut ms)?;
    populate_state(&mut ms)?;
    populate_feed(&mut ms, request, &request.polarization_setup)?;
    populate_observation(&mut ms, request, &sample_times)?;
    populate_history(&mut ms, request)?;
    let metadata_millis = elapsed_millis(metadata_started.elapsed());

    let row_pairs = observation_row_pairs(request);
    let baseline_count = row_pairs.len();
    let model_started = Instant::now();
    let model = prepare_sky_model(request)?;
    let model_prepare_millis = elapsed_millis(model_started.elapsed());
    let mut main_column_writer = SimobserveMainColumnWriter::start(
        &request.output_ms,
        baseline_count * time_sample_count,
        request.polarization_setup.correlation_count,
        request.spectral_setup.channel_count,
        &request.telescope_name,
    )?;
    let mut main_rows = populate_main_rows(
        request,
        &sample_times,
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
    main_rows.timing.data_io_columns = streamed_main_columns.column_timing_reports();

    let save_started = Instant::now();
    ms.save_assuming_valid_with_main_column_overrides(&main_rows.scalar_column_overrides)?;
    install_streamed_main_columns(ms.main_table(), &request.output_ms, streamed_main_columns)?;
    let save_millis = elapsed_millis(save_started.elapsed());

    Ok(SyntheticObservationReport {
        output_ms: request.output_ms.clone(),
        model_image: active_model_path(request)
            .unwrap_or(request.model_image.as_path())
            .to_path_buf(),
        model_kind: active_model_kind(request).to_string(),
        antenna_count: request.antennas.len(),
        observation_mode: request.observation_mode,
        baseline_count,
        time_sample_count,
        main_row_count: baseline_count * time_sample_count,
        channel_count: request.spectral_setup.channel_count,
        correlation_count: request.polarization_setup.correlation_count,
        nonzero_visibility_count: main_rows.nonzero_visibility_count,
        flagged_row_count: main_rows.flagged_row_count,
        elevation_flagged_row_count: main_rows.elevation_flagged_row_count,
        shadow_flagged_row_count: main_rows.shadow_flagged_row_count,
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

fn active_model_kind(request: &SyntheticObservationRequest) -> &'static str {
    if !request.predict_model {
        return "none";
    }
    request
        .model
        .as_ref()
        .map(SyntheticSkyModel::kind_name)
        .unwrap_or("fits_image")
}

fn active_model_path(request: &SyntheticObservationRequest) -> Option<&Path> {
    request
        .model
        .as_ref()
        .and_then(SyntheticSkyModel::path)
        .or(Some(request.model_image.as_path()))
}

fn prepare_sky_model(request: &SyntheticObservationRequest) -> MsResult<Option<PreparedSkyModel>> {
    if !request.predict_model {
        return Ok(None);
    }
    match request.model.as_ref() {
        Some(SyntheticSkyModel::FitsImage {
            path,
            model_peak_jy_per_pixel,
            direction_reference_rad,
            cell_size_rad,
        }) => Ok(Some(PreparedSkyModel::Sampled(Box::new(
            read_fits_model_image(
                path,
                (*model_peak_jy_per_pixel).or(request.model_peak_jy_per_pixel),
                *direction_reference_rad,
                *cell_size_rad,
                request.spectral_setup.channel_count,
            )?,
        )))),
        Some(SyntheticSkyModel::AnalyticComponents {
            path,
            schema_version,
            name,
            components,
        }) => {
            let model = if components.is_empty() {
                let path = path.as_ref().ok_or_else(|| {
                    MsError::SyntheticObservation(
                        "analytic component model requires either components or a path".to_string(),
                    )
                })?;
                load_analytic_component_model(path)?
            } else {
                SyntheticAnalyticComponentModel {
                    schema_version: *schema_version,
                    name: name.clone(),
                    components: components.clone(),
                }
            };
            Ok(Some(PreparedSkyModel::Analytic(
                prepare_analytic_component_model(&model)?,
            )))
        }
        None => Ok(Some(PreparedSkyModel::Sampled(Box::new(
            read_fits_model_image(
                &request.model_image,
                request.model_peak_jy_per_pixel,
                None,
                None,
                request.spectral_setup.channel_count,
            )?,
        )))),
    }
}

fn load_analytic_component_model(path: &Path) -> MsResult<SyntheticAnalyticComponentModel> {
    let json = fs::read_to_string(path).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to read analytic component model {}: {error}",
            path.display()
        ))
    })?;
    if let Ok(model) = serde_json::from_str::<SyntheticAnalyticComponentModel>(&json) {
        return Ok(model);
    }
    let sky_model = serde_json::from_str::<SyntheticSkyModel>(&json).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to parse analytic component model {}: {error}",
            path.display()
        ))
    })?;
    match sky_model {
        SyntheticSkyModel::AnalyticComponents {
            schema_version,
            name,
            components,
            ..
        } => Ok(SyntheticAnalyticComponentModel {
            schema_version,
            name,
            components,
        }),
        SyntheticSkyModel::FitsImage { .. } => Err(MsError::SyntheticObservation(format!(
            "analytic component model {} contains a FITS image model",
            path.display()
        ))),
    }
}

fn prepare_analytic_component_model(
    model: &SyntheticAnalyticComponentModel,
) -> MsResult<PreparedAnalyticSkyModel> {
    if model.components.is_empty() {
        return Err(MsError::SyntheticObservation(
            "analytic component model must include at least one component".to_string(),
        ));
    }
    let mut components = Vec::with_capacity(model.components.len());
    for component in &model.components {
        components.push(prepare_analytic_component(component)?);
    }
    Ok(PreparedAnalyticSkyModel { components })
}

fn prepare_analytic_component(
    component: &SyntheticAnalyticComponent,
) -> MsResult<PreparedAnalyticComponent> {
    match component {
        SyntheticAnalyticComponent::Point {
            l_rad,
            m_rad,
            spectrum,
            ..
        } => prepared_analytic_component(*l_rad, *m_rad, None, 0.0, 0.0, spectrum),
        SyntheticAnalyticComponent::Gaussian {
            l_rad,
            m_rad,
            major_fwhm_rad,
            minor_fwhm_rad,
            position_angle_rad,
            spectrum,
            ..
        } => {
            if *major_fwhm_rad <= 0.0 || !major_fwhm_rad.is_finite() {
                return Err(MsError::SyntheticObservation(
                    "analytic Gaussian major_fwhm_rad must be positive".to_string(),
                ));
            }
            if *minor_fwhm_rad < 0.0 || !minor_fwhm_rad.is_finite() {
                return Err(MsError::SyntheticObservation(
                    "analytic Gaussian minor_fwhm_rad must be non-negative".to_string(),
                ));
            }
            if !position_angle_rad.is_finite() {
                return Err(MsError::SyntheticObservation(
                    "analytic Gaussian position_angle_rad must be finite".to_string(),
                ));
            }
            let minor_fwhm_rad = if *minor_fwhm_rad == 0.0 {
                *major_fwhm_rad
            } else {
                *minor_fwhm_rad
            };
            let fwhm_to_sigma = 1.0 / (2.0 * (2.0_f64.ln()).sqrt());
            prepared_analytic_component(
                *l_rad,
                *m_rad,
                Some(*major_fwhm_rad * fwhm_to_sigma),
                minor_fwhm_rad * fwhm_to_sigma,
                *position_angle_rad,
                spectrum,
            )
        }
    }
}

fn prepared_analytic_component(
    l_rad: f64,
    m_rad: f64,
    major_sigma_rad: Option<f64>,
    minor_sigma_rad: f64,
    position_angle_rad: f64,
    spectrum: &SyntheticAnalyticSpectrum,
) -> MsResult<PreparedAnalyticComponent> {
    validate_direction_cosines(l_rad, m_rad)?;
    validate_analytic_spectrum(spectrum)?;
    Ok(PreparedAnalyticComponent {
        l_rad,
        m_rad,
        n_minus_one: (1.0 - l_rad * l_rad - m_rad * m_rad).sqrt() - 1.0,
        major_sigma_rad,
        minor_sigma_rad,
        position_angle_rad,
        spectrum: spectrum.clone(),
    })
}

fn validate_direction_cosines(l_rad: f64, m_rad: f64) -> MsResult<()> {
    if !l_rad.is_finite() || !m_rad.is_finite() {
        return Err(MsError::SyntheticObservation(
            "analytic component l_rad and m_rad must be finite".to_string(),
        ));
    }
    if l_rad * l_rad + m_rad * m_rad >= 1.0 {
        return Err(MsError::SyntheticObservation(
            "analytic component l_rad and m_rad must lie inside the visible hemisphere".to_string(),
        ));
    }
    Ok(())
}

fn validate_analytic_spectrum(spectrum: &SyntheticAnalyticSpectrum) -> MsResult<()> {
    let finite_values = [
        spectrum.flux_jy,
        spectrum.spectral_index,
        spectrum.line_peak_jy,
        spectrum.line_center_fraction,
        spectrum.line_sigma_fraction,
        spectrum.absorption_peak_jy,
        spectrum.absorption_center_fraction,
        spectrum.absorption_sigma_fraction,
    ];
    if finite_values.iter().any(|value| !value.is_finite()) {
        return Err(MsError::SyntheticObservation(
            "analytic spectrum values must be finite".to_string(),
        ));
    }
    if let Some(reference_frequency_hz) = spectrum.reference_frequency_hz {
        if reference_frequency_hz <= 0.0 || !reference_frequency_hz.is_finite() {
            return Err(MsError::SyntheticObservation(
                "analytic spectrum reference_frequency_hz must be positive".to_string(),
            ));
        }
    }
    if spectrum.line_peak_jy != 0.0 && spectrum.line_sigma_fraction <= 0.0 {
        return Err(MsError::SyntheticObservation(
            "analytic emission line sigma must be positive when line_peak_jy is non-zero"
                .to_string(),
        ));
    }
    if spectrum.absorption_peak_jy != 0.0 && spectrum.absorption_sigma_fraction <= 0.0 {
        return Err(MsError::SyntheticObservation(
            "analytic absorption sigma must be positive when absorption_peak_jy is non-zero"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_request(request: &SyntheticObservationRequest) -> MsResult<()> {
    if request.predict_model {
        match request.model.as_ref() {
            Some(SyntheticSkyModel::FitsImage { path, .. }) => {
                if !path.exists() {
                    return Err(MsError::SyntheticObservation(format!(
                        "model image {} does not exist",
                        path.display()
                    )));
                }
            }
            Some(SyntheticSkyModel::AnalyticComponents {
                path, components, ..
            }) => {
                if components.is_empty() {
                    let Some(path) = path else {
                        return Err(MsError::SyntheticObservation(
                            "analytic component model requires either components or a path"
                                .to_string(),
                        ));
                    };
                    if !path.exists() {
                        return Err(MsError::SyntheticObservation(format!(
                            "analytic component model {} does not exist",
                            path.display()
                        )));
                    }
                } else {
                    let model = SyntheticAnalyticComponentModel {
                        schema_version: None,
                        name: None,
                        components: components.clone(),
                    };
                    prepare_analytic_component_model(&model)?;
                }
            }
            None => {
                if !request.model_image.exists() {
                    return Err(MsError::SyntheticObservation(format!(
                        "model image {} does not exist",
                        request.model_image.display()
                    )));
                }
            }
        }
    }
    let minimum_antennas = match request.observation_mode {
        SyntheticObservationMode::Interferometric => 2,
        SyntheticObservationMode::TotalPower => 1,
    };
    if request.antennas.len() < minimum_antennas {
        return Err(MsError::SyntheticObservation(format!(
            "at least {minimum_antennas} antenna{} required for {} simulation",
            if minimum_antennas == 1 {
                " is"
            } else {
                "s are"
            },
            match request.observation_mode {
                SyntheticObservationMode::Interferometric => "interferometric",
                SyntheticObservationMode::TotalPower => "total-power",
            }
        )));
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
    if request.row_workers == Some(0) || request.channel_workers == Some(0) {
        return Err(MsError::SyntheticObservation(
            "explicit worker counts must be positive".to_string(),
        ));
    }
    request.polarization_setup.validate()?;
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
    if !request.elevation_limit_rad.is_finite()
        || request.elevation_limit_rad <= -std::f64::consts::FRAC_PI_2
        || request.elevation_limit_rad >= std::f64::consts::FRAC_PI_2
    {
        return Err(MsError::SyntheticObservation(
            "elevation limit must be finite and between -90 and +90 degrees".to_string(),
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
    sample_times: &[f64],
) -> MsResult<()> {
    if request.observation_mode == SyntheticObservationMode::TotalPower
        || !request.fields.is_empty()
    {
        let fields = effective_fields(request);
        for (sample, time) in sample_times.iter().copied().enumerate() {
            let field = &fields[sample % fields.len()];
            let direction = direction_poly_array(field.phase_center_rad);
            for antenna_id in 0..request.antennas.len() {
                let row = row_from_defs(
                    schema::pointing::REQUIRED_COLUMNS,
                    &[
                        ("ANTENNA_ID", i(antenna_id as i32)),
                        ("DIRECTION", direction.clone()),
                        ("INTERVAL", f(request.integration_seconds)),
                        ("NAME", s(&field.name)),
                        ("NUM_POLY", i(0)),
                        ("TARGET", direction.clone()),
                        ("TIME", f(time)),
                        ("TIME_ORIGIN", f(time - 0.5 * request.integration_seconds)),
                        ("TRACKING", b(true)),
                    ],
                );
                subtable_mut(ms, SubtableId::Pointing)?.add_row(row)?;
            }
        }
        return Ok(());
    }

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
            ("REF_FREQUENCY", f(spectral_setup.reference_frequency_hz())),
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

fn populate_polarization(
    ms: &mut MeasurementSet,
    polarization_setup: &SyntheticPolarizationSetup,
) -> MsResult<()> {
    let correlation_types = polarization_setup.correlation_types();
    let correlation_products = polarization_setup.correlation_products();
    let row = row_from_defs(
        schema::polarization::REQUIRED_COLUMNS,
        &[
            ("NUM_CORR", i(polarization_setup.correlation_count as i32)),
            (
                "CORR_TYPE",
                i32_array(
                    &correlation_types,
                    vec![polarization_setup.correlation_count],
                ),
            ),
            (
                "CORR_PRODUCT",
                i32_array(
                    &correlation_products,
                    vec![2, polarization_setup.correlation_count],
                ),
            ),
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

fn populate_feed(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
    polarization_setup: &SyntheticPolarizationSetup,
) -> MsResult<()> {
    let receptor_types = polarization_setup.receptor_types();
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
                ("POLARIZATION_TYPE", string_array(&receptor_types, vec![2])),
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
    sample_times: &[f64],
) -> MsResult<()> {
    let first_time = sample_times
        .first()
        .copied()
        .unwrap_or(request.start_time_mjd_seconds);
    let last_time = sample_times
        .last()
        .copied()
        .unwrap_or(request.start_time_mjd_seconds);
    let start_time = first_time - 0.5 * request.integration_seconds;
    let end_time = last_time + 0.5 * request.integration_seconds;
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
            ("TIME_RANGE", f64_array(&[start_time, end_time], vec![2])),
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
    request: &SyntheticObservationRequest,
    sample_times: &[f64],
    model: Option<&PreparedSkyModel>,
    main_column_writer: &mut SimobserveMainColumnWriter,
) -> MsResult<MainRowsReport> {
    let samples = sample_times.len();
    let num_corr = request.polarization_setup.correlation_count;
    let num_chan = request.spectral_setup.channel_count;
    let channel_prediction_workers = simobserve_channel_worker_count(request, num_chan);
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
    let row_pairs = observation_row_pairs(request);
    let baseline_count = row_pairs.len();
    let total_row_count = samples * baseline_count;
    let mut all_flag_rows = Vec::with_capacity(total_row_count);
    let observatory = simulation_observatory_position(&request.telescope_name, &request.antennas);
    let elevation_margin_rad = antenna_elevation_margin_rad(&request.antennas, &observatory);
    let uvw_production_trace = SimobserveUvwProductionTrace::from_env();
    let mut flagged_row_count = 0usize;
    let mut elevation_flagged_row_count = 0usize;
    let mut shadow_flagged_row_count = 0usize;

    for (sample, time) in sample_times.iter().copied().enumerate() {
        let uvw_started = Instant::now();
        let field_id = sample % field_plans.len();
        let field_plan = &field_plans[field_id];
        let antenna_uvws = antenna_uvw_positions(
            &request.antennas,
            field_plan.phase_center_rad,
            time,
            &observatory,
        )?;
        let mut row_specs = Vec::with_capacity(baseline_count);
        let mut row_uvws = Vec::with_capacity(baseline_count);
        for (baseline_index, pair) in row_pairs.iter().copied().enumerate() {
            let uvw = if pair.antenna1 == pair.antenna2 {
                [0.0, 0.0, 0.0]
            } else {
                [
                    antenna_uvws[pair.antenna2][0] - antenna_uvws[pair.antenna1][0],
                    antenna_uvws[pair.antenna2][1] - antenna_uvws[pair.antenna1][1],
                    antenna_uvws[pair.antenna2][2] - antenna_uvws[pair.antenna1][2],
                ]
            };
            let row_number = sample * baseline_count + baseline_index;
            if uvw_production_trace
                .as_ref()
                .is_some_and(|trace| trace.matches(pair.antenna1, pair.antenna2, time))
            {
                trace_simobserve_production_uvw(
                    row_number,
                    sample,
                    pair.antenna1,
                    pair.antenna2,
                    time,
                    field_plan.phase_center_rad,
                    &observatory,
                    &request.antennas,
                    &antenna_uvws,
                    uvw,
                );
            }
            row_specs.push(MainRowVisibilitySpec {
                antenna1: pair.antenna1,
                antenna2: pair.antenna2,
                uvw,
            });
            row_uvws.push(uvw);
        }
        let shadowed_antennas = if request.observation_mode == SyntheticObservationMode::TotalPower
        {
            vec![false; request.antennas.len()]
        } else {
            shadowed_antennas_for_rows(&row_specs, &request.antennas)
        };
        let low_elevation_antennas = antennas_below_elevation_limit(
            field_plan.phase_center_rad,
            time,
            &request.antennas,
            &observatory,
            elevation_margin_rad,
            request.elevation_limit_rad,
        )?;
        timing.uvw_and_row_setup += uvw_started.elapsed();

        let prediction_started = Instant::now();
        let prediction = predicted_data_values_for_rows_with_workers_timed(
            field_plan.predictors.as_ref(),
            &request.spectral_setup,
            &row_uvws,
            num_corr,
            channel_prediction_workers,
        );
        let mut data_rows = prediction.rows;
        timing.prediction_worker_wall += prediction.worker_wall;
        timing.prediction_gather += prediction.gather;
        timing.prediction += prediction_started.elapsed();
        let corruption_started = Instant::now();
        nonzero_visibility_count += apply_corruption_and_count_rows_with_workers(
            request,
            corruption.as_ref(),
            &row_specs,
            &mut data_rows,
            num_chan,
            sample,
        );
        timing.corruption += corruption_started.elapsed();
        let mut flag_rows = Vec::with_capacity(row_specs.len());
        for spec in &row_specs {
            let elevation_flagged =
                low_elevation_antennas[spec.antenna1] || low_elevation_antennas[spec.antenna2];
            let shadow_flagged =
                shadowed_antennas[spec.antenna1] || shadowed_antennas[spec.antenna2];
            elevation_flagged_row_count += usize::from(elevation_flagged);
            shadow_flagged_row_count += usize::from(shadow_flagged);
            flagged_row_count += usize::from(elevation_flagged || shadow_flagged);
            flag_rows.push(elevation_flagged || shadow_flagged);
        }
        let scalar_started = Instant::now();
        all_flag_rows.extend(flag_rows.iter().copied());
        timing.scalar_column += scalar_started.elapsed();
        let data_io_started = Instant::now();
        main_column_writer.send_batch(SimobserveMainColumnBatch {
            data_rows,
            flag_rows,
            uvw_rows: row_uvws,
        })?;
        timing.data_io_enqueue += data_io_started.elapsed();
    }
    let scalar_started = Instant::now();
    let scalar_column_overrides = MainScalarColumnOverrides::new(
        total_row_count,
        row_pairs,
        sample_times.to_vec(),
        field_plans.len(),
        request.integration_seconds,
        request.observation_mode,
        all_flag_rows,
    )
    .into_column_overrides()?;
    timing.scalar_column += scalar_started.elapsed();
    Ok(MainRowsReport {
        nonzero_visibility_count,
        flagged_row_count,
        elevation_flagged_row_count,
        shadow_flagged_row_count,
        timing: timing.into_report(),
        scalar_column_overrides,
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

fn antennas_below_elevation_limit(
    phase_center_rad: [f64; 2],
    time_mjd_seconds: f64,
    antennas: &[SyntheticAntenna],
    observatory: &MPosition,
    elevation_margin_rad: f64,
    elevation_limit_rad: f64,
) -> MsResult<Vec<bool>> {
    let observatory_elevation_rad =
        field_elevation_rad(phase_center_rad, time_mjd_seconds, observatory)?;
    if observatory_elevation_rad + elevation_margin_rad < elevation_limit_rad {
        return Ok(vec![true; antennas.len()]);
    }
    if observatory_elevation_rad - elevation_margin_rad > elevation_limit_rad {
        return Ok(vec![false; antennas.len()]);
    }

    antennas
        .iter()
        .map(|antenna| {
            let position = MPosition::new_itrf(
                antenna.position_m[0],
                antenna.position_m[1],
                antenna.position_m[2],
            );
            Ok(
                field_elevation_rad(phase_center_rad, time_mjd_seconds, &position)?
                    < elevation_limit_rad,
            )
        })
        .collect()
}

fn antenna_elevation_margin_rad(antennas: &[SyntheticAntenna], observatory: &MPosition) -> f64 {
    let observatory_itrf = observatory.as_itrf();
    let observatory_radius_m = vector_norm(observatory_itrf);
    if observatory_radius_m == 0.0 {
        return 0.0;
    }

    antennas
        .iter()
        .map(|antenna| {
            let offset = [
                antenna.position_m[0] - observatory_itrf[0],
                antenna.position_m[1] - observatory_itrf[1],
                antenna.position_m[2] - observatory_itrf[2],
            ];
            vector_norm(offset) / observatory_radius_m
        })
        .fold(0.0_f64, f64::max)
        + 1.0e-9
}

fn field_elevation_rad(
    phase_center_rad: [f64; 2],
    time_mjd_seconds: f64,
    observatory: &MPosition,
) -> MsResult<f64> {
    let phase_center = MDirection::from_angles(
        phase_center_rad[0],
        phase_center_rad[1],
        DirectionRef::J2000,
    );
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(time_mjd_seconds / 86_400.0, EpochRef::UT1))
        .with_position(observatory.clone())
        .with_direction(phase_center.clone())
        .with_bundled_eop();
    let azel = phase_center
        .convert_to(DirectionRef::AZEL, &frame)
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "simobserve elevation-limit direction conversion failed: {error}"
            ))
        })?;
    Ok(azel.latitude_rad())
}

fn apply_corruption_and_count_rows_with_workers(
    request: &SyntheticObservationRequest,
    corruption: Option<&SyntheticCorruptionState>,
    row_specs: &[MainRowVisibilitySpec],
    data_rows: &mut [Vec<Complex32>],
    channel_count: usize,
    sample_index: usize,
) -> usize {
    let worker_count = simobserve_row_worker_count(request, data_rows.len(), channel_count);
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
    flagged_row_count: usize,
    elevation_flagged_row_count: usize,
    shadow_flagged_row_count: usize,
    timing: SyntheticMainRowTimingReport,
    scalar_column_overrides: ColumnOverrides,
}

struct SimobserveMainColumnBatch {
    data_rows: Vec<Vec<Complex32>>,
    flag_rows: Vec<bool>,
    uvw_rows: Vec<[f64; 3]>,
}

struct StreamedSimobserveMainColumns {
    data: StreamedTiledShapeComplex32Column,
    flag: StreamedTiledPrimitiveColumn,
    flag_category: StreamedTiledPrimitiveColumn,
    uvw: StreamedTiledPrimitiveColumn,
    weight: StreamedTiledPrimitiveColumn,
    sigma: StreamedTiledPrimitiveColumn,
}

impl StreamedSimobserveMainColumns {
    fn column_timing_reports(&self) -> Vec<SyntheticMainColumnIoTimingReport> {
        vec![
            self.column_timing_report(
                "DATA",
                self.data.assemble_seconds(),
                self.data.write_seconds(),
                self.data.bytes_written(),
            ),
            self.column_timing_report(
                "FLAG",
                self.flag.assemble_seconds(),
                self.flag.write_seconds(),
                self.flag.bytes_written(),
            ),
            self.column_timing_report(
                "FLAG_CATEGORY",
                self.flag_category.assemble_seconds(),
                self.flag_category.write_seconds(),
                self.flag_category.bytes_written(),
            ),
            self.column_timing_report(
                "UVW",
                self.uvw.assemble_seconds(),
                self.uvw.write_seconds(),
                self.uvw.bytes_written(),
            ),
            self.column_timing_report(
                "WEIGHT",
                self.weight.assemble_seconds(),
                self.weight.write_seconds(),
                self.weight.bytes_written(),
            ),
            self.column_timing_report(
                "SIGMA",
                self.sigma.assemble_seconds(),
                self.sigma.write_seconds(),
                self.sigma.bytes_written(),
            ),
        ]
    }

    fn column_timing_report(
        &self,
        column: &str,
        assemble_seconds: f64,
        write_seconds: f64,
        bytes_written: usize,
    ) -> SyntheticMainColumnIoTimingReport {
        SyntheticMainColumnIoTimingReport {
            column: column.to_string(),
            assemble_millis: elapsed_seconds_to_millis(assemble_seconds),
            write_millis: elapsed_seconds_to_millis(write_seconds),
            bytes_written: bytes_written as u64,
        }
    }

    fn assemble_seconds(&self) -> f64 {
        self.data.assemble_seconds()
            + self.flag.assemble_seconds()
            + self.flag_category.assemble_seconds()
            + self.uvw.assemble_seconds()
            + self.weight.assemble_seconds()
            + self.sigma.assemble_seconds()
    }

    fn write_seconds(&self) -> f64 {
        self.data.write_seconds()
            + self.flag.write_seconds()
            + self.flag_category.write_seconds()
            + self.uvw.write_seconds()
            + self.weight.write_seconds()
            + self.sigma.write_seconds()
    }

    fn bytes_written(&self) -> usize {
        self.data.bytes_written()
            + self.flag.bytes_written()
            + self.flag_category.bytes_written()
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
        let flag_category_tile_shape = vec![
            visibility_tile_shape[0],
            visibility_tile_shape[1],
            1,
            visibility_tile_shape[2],
        ];

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
        let flag_category_writer = StreamingTiledPrimitiveWriter::create_shape(
            output_ms.join(".casa-rs.FLAG_CATEGORY.table.f.tmp"),
            row_count,
            vec![0, num_corr, num_chan],
            flag_category_tile_shape,
            StreamedTiledPrimitiveType::Bool,
            false,
        )
        .map_err(|error| {
            MsError::SyntheticObservation(format!(
                "failed to create streamed FLAG_CATEGORY writer: {error}"
            ))
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
            let mut flag_category_writer = flag_category_writer;
            let mut uvw_writer = uvw_writer;
            let mut weight_writer = weight_writer;
            let mut sigma_writer = sigma_writer;
            let weight_row = vec![1.0f32; num_corr];
            let sigma_row = vec![1.0f32; num_corr];

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
                    .zip(batch.flag_rows)
                    .zip(batch.uvw_rows)
                {
                    data_writer.push_row(&data_row).map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream DATA row into tiled storage: {error}"
                        ))
                    })?;
                    if flag_row {
                        flag_writer.push_bool_fill_row(true).map_err(|error| {
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
                    flag_category_writer.push_zero_row().map_err(|error| {
                        MsError::SyntheticObservation(format!(
                            "failed to stream empty FLAG_CATEGORY row into tiled storage: {error}"
                        ))
                    })?;
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
                flag_category: flag_category_writer.finish().map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "failed to finalize streamed FLAG_CATEGORY writer: {error}"
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

    let flag_category_seq = data_manager_sequence(main, "FLAG_CATEGORY")?;
    install_streamed_tiled_shape_primitive_column(
        output_ms,
        flag_category_seq,
        "FLAG_CATEGORY",
        streamed.flag_category,
    )
    .map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to install streamed FLAG_CATEGORY column: {error}"
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
        .unwrap_or(DEFAULT_SIMOBSERVE_IO_QUEUE_DEPTH)
}

struct MainScalarColumnOverrides {
    row_count: usize,
    baseline_count: usize,
    row_pairs: Arc<Vec<BaselinePair>>,
    sample_times: Arc<Vec<f64>>,
    field_count: usize,
    integration_seconds: f64,
    observation_mode: SyntheticObservationMode,
    flag_rows: Arc<Vec<bool>>,
}

impl MainScalarColumnOverrides {
    fn new(
        row_count: usize,
        row_pairs: Vec<BaselinePair>,
        sample_times: Vec<f64>,
        field_count: usize,
        integration_seconds: f64,
        observation_mode: SyntheticObservationMode,
        flag_rows: Vec<bool>,
    ) -> Self {
        debug_assert_eq!(row_count, row_pairs.len() * sample_times.len());
        debug_assert_eq!(row_count, flag_rows.len());
        Self {
            row_count,
            baseline_count: row_pairs.len(),
            row_pairs: Arc::new(row_pairs),
            sample_times: Arc::new(sample_times),
            field_count,
            integration_seconds,
            observation_mode,
            flag_rows: Arc::new(flag_rows),
        }
    }

    fn into_column_overrides(self) -> MsResult<ColumnOverrides> {
        let mut overrides = ColumnOverrides::for_row_count(self.row_count);
        self.insert_deferred_tiled_columns(&mut overrides);
        self.insert_baseline_columns(&mut overrides);
        self.insert_sample_columns(&mut overrides)?;
        self.insert_constant_columns(&mut overrides);
        Ok(overrides)
    }

    fn insert_deferred_tiled_columns(&self, overrides: &mut ColumnOverrides) {
        for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
            overrides.insert_deferred(column);
        }
    }

    fn insert_baseline_columns(&self, overrides: &mut ColumnOverrides) {
        let row_count = self.row_count;
        let baseline_count = self.baseline_count;
        let row_pairs = Arc::clone(&self.row_pairs);
        overrides.insert_generated_scalar(
            "ANTENNA1",
            GeneratedScalarColumn::new(row_count, move |row| {
                Some(ScalarValue::Int32(
                    row_pairs[row % baseline_count].antenna1 as i32,
                ))
            }),
        );

        let row_pairs = Arc::clone(&self.row_pairs);
        overrides.insert_generated_scalar(
            "ANTENNA2",
            GeneratedScalarColumn::new(row_count, move |row| {
                Some(ScalarValue::Int32(
                    row_pairs[row % baseline_count].antenna2 as i32,
                ))
            }),
        );
    }

    fn insert_sample_columns(&self, overrides: &mut ColumnOverrides) -> MsResult<()> {
        let row_count = self.row_count;
        let time_runs =
            self.sample_runs(|sample| Some(ScalarValue::Float64(self.sample_times[sample])));
        overrides.insert_generated_scalar(
            "TIME",
            GeneratedScalarColumn::from_scalar_runs(row_count, time_runs.clone())?,
        );

        overrides.insert_generated_scalar(
            "TIME_CENTROID",
            GeneratedScalarColumn::from_scalar_runs(row_count, time_runs)?,
        );

        let field_count = self.field_count;
        let field_runs =
            self.sample_runs(|sample| Some(ScalarValue::Int32((sample % field_count) as i32)));
        overrides.insert_generated_scalar(
            "FIELD_ID",
            GeneratedScalarColumn::from_scalar_runs(row_count, field_runs)?,
        );

        let scan_column = if self.observation_mode == SyntheticObservationMode::TotalPower {
            GeneratedScalarColumn::from_scalar_runs(
                row_count,
                self.sample_runs(|sample| Some(ScalarValue::Int32(sample as i32 + 1))),
            )?
        } else {
            GeneratedScalarColumn::constant(row_count, Some(ScalarValue::Int32(1)))
        };
        overrides.insert_generated_scalar("SCAN_NUMBER", scan_column);

        let flag_rows = Arc::clone(&self.flag_rows);
        overrides.insert_generated_scalar(
            "FLAG_ROW",
            GeneratedScalarColumn::new(row_count, move |row| {
                Some(ScalarValue::Bool(flag_rows[row]))
            }),
        );
        Ok(())
    }

    fn insert_constant_columns(&self, overrides: &mut ColumnOverrides) {
        self.insert_constant_i32(overrides, "ARRAY_ID", 0);
        self.insert_constant_i32(overrides, "DATA_DESC_ID", 0);
        self.insert_constant_i32(overrides, "FEED1", 0);
        self.insert_constant_i32(overrides, "FEED2", 0);
        self.insert_constant_i32(overrides, "OBSERVATION_ID", 0);
        self.insert_constant_i32(overrides, "PROCESSOR_ID", 0);
        self.insert_constant_i32(overrides, "STATE_ID", 0);
        self.insert_constant_f64(overrides, "EXPOSURE", self.integration_seconds);
        self.insert_constant_f64(overrides, "INTERVAL", self.integration_seconds);
    }

    fn insert_constant_i32(
        &self,
        overrides: &mut ColumnOverrides,
        column: &'static str,
        value: i32,
    ) {
        overrides.insert_generated_scalar(
            column,
            GeneratedScalarColumn::constant(self.row_count, Some(ScalarValue::Int32(value))),
        );
    }

    fn insert_constant_f64(
        &self,
        overrides: &mut ColumnOverrides,
        column: &'static str,
        value: f64,
    ) {
        overrides.insert_generated_scalar(
            column,
            GeneratedScalarColumn::constant(self.row_count, Some(ScalarValue::Float64(value))),
        );
    }

    fn sample_runs(
        &self,
        mut value_for_sample: impl FnMut(usize) -> Option<ScalarValue>,
    ) -> Vec<GeneratedScalarValueRun> {
        let sample_count = self.sample_times.len();
        let mut runs = Vec::with_capacity(sample_count);
        let mut last_value: Option<ScalarValue> = None;
        for sample in 0..sample_count {
            let value = value_for_sample(sample);
            if sample == 0 || value != last_value {
                runs.push(GeneratedScalarValueRun::new(
                    sample * self.baseline_count,
                    value.clone(),
                ));
                last_value = value;
            }
        }
        runs
    }
}

#[derive(Default)]
struct MainRowTimingDurations {
    channel_prediction_workers: usize,
    uvw_and_row_setup: Duration,
    prediction: Duration,
    prediction_worker_wall: Duration,
    prediction_gather: Duration,
    corruption: Duration,
    data_io_enqueue: Duration,
    main_write: Duration,
    scalar_column: Duration,
    main_row_add: Duration,
}

impl MainRowTimingDurations {
    fn into_report(self) -> SyntheticMainRowTimingReport {
        SyntheticMainRowTimingReport {
            channel_prediction_workers: self.channel_prediction_workers,
            uvw_and_row_setup_millis: elapsed_millis(self.uvw_and_row_setup),
            prediction_millis: elapsed_millis(self.prediction),
            prediction_worker_wall_millis: elapsed_millis(self.prediction_worker_wall),
            prediction_gather_millis: elapsed_millis(self.prediction_gather),
            corruption_millis: elapsed_millis(self.corruption),
            data_io_enqueue_millis: elapsed_millis(self.data_io_enqueue),
            data_io_finalize_millis: 0,
            data_io_assemble_millis: 0,
            data_io_write_millis: 0,
            data_io_bytes: 0,
            data_io_columns: Vec::new(),
            main_write_millis: elapsed_millis(self.main_write),
            scalar_column_millis: elapsed_millis(self.scalar_column),
            main_row_add_millis: elapsed_millis(self.main_row_add),
        }
    }
}

fn elapsed_seconds_to_millis(seconds: f64) -> u128 {
    (seconds * 1000.0).round() as u128
}

#[derive(Clone, Copy)]
struct BaselinePair {
    antenna1: usize,
    antenna2: usize,
}

fn baseline_pairs(antenna_count: usize) -> Vec<BaselinePair> {
    let baseline_count = antenna_count * antenna_count.saturating_sub(1) / 2;
    let mut pairs = Vec::with_capacity(baseline_count);
    for antenna1 in 0..antenna_count {
        for antenna2 in (antenna1 + 1)..antenna_count {
            pairs.push(BaselinePair { antenna1, antenna2 });
        }
    }
    pairs
}

fn observation_row_pairs(request: &SyntheticObservationRequest) -> Vec<BaselinePair> {
    match request.observation_mode {
        SyntheticObservationMode::Interferometric => baseline_pairs(request.antennas.len()),
        SyntheticObservationMode::TotalPower => (0..request.antennas.len())
            .map(|antenna| BaselinePair {
                antenna1: antenna,
                antenna2: antenna,
            })
            .collect(),
    }
}

#[derive(Clone, Copy)]
struct MainRowVisibilitySpec {
    antenna1: usize,
    antenna2: usize,
    uvw: [f64; 3],
}

#[derive(Debug, Clone)]
struct FitsModelImage {
    pixels: Array2<f32>,
    channel_planes: Vec<Array2<f32>>,
    cell_size_rad: [f64; 2],
    direction_increment_rad: Option<[f64; 2]>,
    direction_wcs: Option<FitsModelDirectionWcs>,
    ra_axis_increases_with_x: bool,
    reference_direction_rad: Option<[f64; 2]>,
}

impl FitsModelImage {
    fn pixels_for_channel(&self, channel: usize) -> &Array2<f32> {
        self.channel_planes.get(channel).unwrap_or(&self.pixels)
    }
}

enum PreparedSkyModel {
    Sampled(Box<FitsModelImage>),
    Analytic(PreparedAnalyticSkyModel),
}

#[derive(Debug, Clone)]
struct PreparedAnalyticSkyModel {
    components: Vec<PreparedAnalyticComponent>,
}

#[derive(Debug, Clone)]
struct PreparedAnalyticComponent {
    l_rad: f64,
    m_rad: f64,
    n_minus_one: f64,
    major_sigma_rad: Option<f64>,
    minor_sigma_rad: f64,
    position_angle_rad: f64,
    spectrum: SyntheticAnalyticSpectrum,
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
    phase_offset: ModelPhaseOffset,
    phase_center_rad: [f64; 2],
    model_reference_direction_rad: Option<[f64; 2]>,
}

enum SyntheticFieldPredictor {
    Sampled(Vec<SyntheticChannelPredictor>),
    SampledTotalPower(Vec<Complex32>),
    Analytic(AnalyticFieldPredictor),
}

struct AnalyticFieldPredictor {
    components: Vec<AnalyticFieldComponent>,
    inverse_wavelengths_m: Vec<f64>,
}

struct AnalyticFieldComponent {
    component: PreparedAnalyticComponent,
    channel_amplitudes_jy: Vec<f64>,
}

#[derive(Debug, Clone, Copy)]
struct ModelPhaseOffset {
    l_rad: f64,
    m_rad: f64,
    n_minus_one: f64,
}

impl ModelPhaseOffset {
    fn is_negligible(self) -> bool {
        self.l_rad.abs() < 1.0e-15 && self.m_rad.abs() < 1.0e-15 && self.n_minus_one.abs() < 1.0e-15
    }
}

struct SyntheticFieldPlan {
    phase_center_rad: [f64; 2],
    predictors: Option<SyntheticFieldPredictor>,
}

fn build_field_plans(
    request: &SyntheticObservationRequest,
    model: Option<&PreparedSkyModel>,
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
            let predictors = match model {
                Some(PreparedSkyModel::Sampled(model)) => {
                    if request.observation_mode == SyntheticObservationMode::TotalPower {
                        Some(SyntheticFieldPredictor::SampledTotalPower(
                            build_total_power_sampled_values(
                                model,
                                request,
                                field.phase_center_rad,
                                pointing_offset_rad,
                                primary_beam,
                            ),
                        ))
                    } else {
                        Some(SyntheticFieldPredictor::Sampled(build_channel_predictors(
                            model,
                            request,
                            field.phase_center_rad,
                            pointing_offset_rad,
                            primary_beam,
                        )?))
                    }
                }
                Some(PreparedSkyModel::Analytic(model)) => Some(SyntheticFieldPredictor::Analytic(
                    build_analytic_field_predictor(
                        model,
                        request,
                        field.phase_center_rad,
                        pointing_offset_rad,
                        primary_beam,
                    )?,
                )),
                None => None,
            };
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
        || request.telescope_name.eq_ignore_ascii_case("ACA")
        || request.telescope_name.eq_ignore_ascii_case("ALMASD");
    let (beam_dish_diameter_m, blockage_diameter_m) = if request.observation_mode
        == SyntheticObservationMode::TotalPower
        && telescope_is_alma_family
        && (dish_diameter_m - 12.0).abs() < 0.5
    {
        // CASA simobserve's ALMA single-dish path uses a slightly wider
        // effective voltage pattern than the interferometric 12m override.
        (10.86, 0.75)
    } else if telescope_is_alma_family && (dish_diameter_m - 12.0).abs() < 0.5 {
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
    request: &SyntheticObservationRequest,
    phase_center_rad: [f64; 2],
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
) -> MsResult<Vec<SyntheticChannelPredictor>> {
    let geometry = ImageGeometry {
        image_shape: [model.pixels.shape()[0], model.pixels.shape()[1]],
        cell_size_rad: model.cell_size_rad,
    };
    let phase_offset = casa_model_phase_offset(model, phase_center_rad);
    let context = ChannelPredictorContext {
        model,
        spectral_setup: &request.spectral_setup,
        geometry,
        phase_center_rad,
        phase_offset,
        pointing_offset_rad,
        primary_beam,
    };
    let worker_count =
        simobserve_channel_worker_count(request, request.spectral_setup.channel_count);
    if worker_count <= 1 || request.spectral_setup.channel_count <= 1 {
        return build_channel_predictor_range(&context, 0, request.spectral_setup.channel_count);
    }

    let chunk_size = request.spectral_setup.channel_count.div_ceil(worker_count);
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for start_channel in (0..request.spectral_setup.channel_count).step_by(chunk_size) {
            let end_channel =
                (start_channel + chunk_size).min(request.spectral_setup.channel_count);
            let worker_context = context;
            handles.push(scope.spawn(move || {
                build_channel_predictor_range(&worker_context, start_channel, end_channel)
            }));
        }

        let mut predictors = Vec::with_capacity(request.spectral_setup.channel_count);
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

fn build_total_power_sampled_values(
    model: &FitsModelImage,
    request: &SyntheticObservationRequest,
    phase_center_rad: [f64; 2],
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
) -> Vec<Complex32> {
    (0..request.spectral_setup.channel_count)
        .map(|channel| {
            let frequency_hz = request.spectral_setup.start_frequency_hz
                + channel as f64 * request.spectral_setup.channel_width_hz;
            let beam_corrected_pixels = apply_simulator_primary_beam(
                model,
                model.pixels_for_channel(channel),
                phase_center_rad,
                frequency_hz,
                pointing_offset_rad,
                primary_beam,
            );
            Complex32::new(beam_corrected_pixels.iter().sum::<f32>(), 0.0)
        })
        .collect()
}

fn build_analytic_field_predictor(
    model: &PreparedAnalyticSkyModel,
    request: &SyntheticObservationRequest,
    field_phase_center_rad: [f64; 2],
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
) -> MsResult<AnalyticFieldPredictor> {
    let mut components = Vec::with_capacity(model.components.len());
    let inverse_wavelengths_m = (0..request.spectral_setup.channel_count)
        .map(|channel| {
            let frequency_hz = request.spectral_setup.start_frequency_hz
                + channel as f64 * request.spectral_setup.channel_width_hz;
            frequency_hz / 299_792_458.0
        })
        .collect::<Vec<_>>();
    for component in &model.components {
        let shifted = analytic_component_for_field(
            component,
            request.phase_center_rad,
            field_phase_center_rad,
        )?;
        let channel_amplitudes_jy = (0..request.spectral_setup.channel_count)
            .map(|channel| {
                let frequency_hz = request.spectral_setup.start_frequency_hz
                    + channel as f64 * request.spectral_setup.channel_width_hz;
                shifted
                    .spectrum
                    .flux_for_channel(&request.spectral_setup, channel)
                    * analytic_primary_beam_taper_for_direction(
                        shifted.l_rad,
                        shifted.m_rad,
                        pointing_offset_rad,
                        primary_beam,
                        frequency_hz,
                    )
            })
            .collect();
        components.push(AnalyticFieldComponent {
            component: shifted,
            channel_amplitudes_jy,
        });
    }
    Ok(AnalyticFieldPredictor {
        components,
        inverse_wavelengths_m,
    })
}

fn analytic_component_for_field(
    component: &PreparedAnalyticComponent,
    reference_phase_center_rad: [f64; 2],
    field_phase_center_rad: [f64; 2],
) -> MsResult<PreparedAnalyticComponent> {
    let delta_ra =
        circular_angle_delta_rad(field_phase_center_rad[0] - reference_phase_center_rad[0]);
    let delta_l = delta_ra * reference_phase_center_rad[1].cos();
    let delta_m = field_phase_center_rad[1] - reference_phase_center_rad[1];
    let l_rad = component.l_rad - delta_l;
    let m_rad = component.m_rad - delta_m;
    validate_direction_cosines(l_rad, m_rad)?;
    let mut shifted = component.clone();
    shifted.l_rad = l_rad;
    shifted.m_rad = m_rad;
    shifted.n_minus_one = (1.0 - l_rad * l_rad - m_rad * m_rad).sqrt() - 1.0;
    Ok(shifted)
}

#[derive(Clone, Copy)]
struct ChannelPredictorContext<'a> {
    model: &'a FitsModelImage,
    spectral_setup: &'a SyntheticSpectralSetup,
    geometry: ImageGeometry,
    phase_center_rad: [f64; 2],
    phase_offset: ModelPhaseOffset,
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
            phase_offset: context.phase_offset,
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
    apply_simulator_primary_beam_power(
        model,
        model_pixels,
        phase_center_rad,
        frequency_hz,
        pointing_offset_rad,
        primary_beam,
        2,
    )
}

fn apply_simulator_primary_beam_power(
    model: &FitsModelImage,
    model_pixels: &Array2<f32>,
    phase_center_rad: [f64; 2],
    frequency_hz: f64,
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
    beam_power: u32,
) -> Array2<f32> {
    let mut pixels = model_pixels.clone();
    if beam_power == 0 {
        return pixels;
    }
    let Some(wcs) = model.direction_wcs.as_ref() else {
        return pixels;
    };
    let pointing_direction_rad = [
        phase_center_rad[0] + pointing_offset_rad[0] / phase_center_rad[1].cos(),
        phase_center_rad[1] + pointing_offset_rad[1],
    ];
    let coordinate = wcs.coordinate();
    let Some(raw_increments) = model.direction_increment_rad else {
        return pixels;
    };
    let imported_coordinate;
    let pb_coordinate: &dyn Coordinate =
        if let Some(reference_direction_rad) = model.reference_direction_rad {
            // CASA simobserve imports the FITS model into a casacore image and
            // recenters the direction coordinate on the image center before
            // PBMath applies the primary beam.
            imported_coordinate = DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(ProjectionType::SIN),
                reference_direction_rad,
                raw_increments,
                [
                    model_pixels.shape()[0] as f64 / 2.0,
                    model_pixels.shape()[1] as f64 / 2.0,
                ],
            )
            .with_longpole(std::f64::consts::PI)
            .with_latpole(reference_direction_rad[1]);
            &imported_coordinate
        } else {
            coordinate
        };
    let Ok(pointing_pixel) = pb_coordinate.to_pixel(&pointing_direction_rad) else {
        return pixels;
    };
    let increments = pb_coordinate.increment();
    if pointing_pixel.len() < 2 || increments.len() < 2 {
        return pixels;
    }
    let primary_beam_evaluator = (!primary_beam.use_casa_vla_q_table).then(|| {
        PrimaryBeamVoltagePattern::new(PrimaryBeamModel::Airy {
            dish_diameter_m: primary_beam.dish_diameter_m,
            blockage_diameter_m: primary_beam.blockage_diameter_m,
        })
    });

    for x in 0..model_pixels.shape()[0] {
        for y in 0..model_pixels.shape()[1] {
            let l = (x as f64 - pointing_pixel[0]) * increments[0];
            let m = (y as f64 - pointing_pixel[1]) * increments[1];
            let vp = synthetic_primary_beam_voltage_pattern(
                primary_beam,
                primary_beam_evaluator.as_ref(),
                l,
                m,
                frequency_hz,
            );
            let taper = match beam_power {
                1 => vp,
                2 => vp * vp,
                _ => vp.powi(beam_power as i32),
            };
            pixels[(x, y)] *= taper;
        }
    }
    pixels
}

fn synthetic_primary_beam_voltage_pattern(
    primary_beam: SyntheticPrimaryBeam,
    primary_beam_evaluator: Option<&PrimaryBeamVoltagePattern>,
    l_rad: f64,
    m_rad: f64,
    frequency_hz: f64,
) -> f32 {
    if primary_beam.use_casa_vla_q_table {
        let radius_rad = (l_rad * l_rad + m_rad * m_rad).sqrt();
        return casa_vla_q_primary_beam_voltage_pattern(radius_rad, frequency_hz);
    }
    primary_beam_evaluator
        .map(|evaluator| evaluator.evaluate_offsets(l_rad, m_rad, frequency_hz))
        .unwrap_or(0.0)
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

fn casa_model_phase_offset(model: &FitsModelImage, phase_center_rad: [f64; 2]) -> ModelPhaseOffset {
    let Some(reference_direction_rad) = model.reference_direction_rad else {
        return ModelPhaseOffset {
            l_rad: 0.0,
            m_rad: 0.0,
            n_minus_one: 0.0,
        };
    };
    let delta_ra = circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0]);
    let cos_delta_ra = delta_ra.cos();
    let (sin_ref_dec, cos_ref_dec) = reference_direction_rad[1].sin_cos();
    let (sin_phase_dec, cos_phase_dec) = phase_center_rad[1].sin_cos();
    let direction_l = delta_ra * phase_center_rad[1].cos();
    let direction_m = reference_direction_rad[1] - phase_center_rad[1];
    let direction_n = sin_ref_dec * sin_phase_dec + cos_ref_dec * cos_phase_dec * cos_delta_ra;
    ModelPhaseOffset {
        l_rad: direction_l,
        m_rad: direction_m,
        n_minus_one: direction_n - 1.0,
    }
}

fn circular_angle_delta_rad(delta: f64) -> f64 {
    (delta + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
}

fn predicted_data_values(
    predictor: Option<&SyntheticFieldPredictor>,
    spectral_setup: &SyntheticSpectralSetup,
    uvw_m: [f64; 3],
    num_corr: usize,
) -> Vec<Complex32> {
    let mut values = vec![Complex32::new(0.0, 0.0); num_corr * spectral_setup.channel_count];
    if let Some(predictor) = predictor {
        match predictor {
            SyntheticFieldPredictor::Sampled(predictors) => {
                let prediction_uvw_m = prediction_uvw_for_row(predictors, uvw_m);
                for (channel, predictor) in predictors.iter().enumerate() {
                    let visibility = predict_channel_visibility_preprojected(
                        predictor,
                        spectral_setup,
                        prediction_uvw_m,
                        channel,
                    );
                    for corr in 0..num_corr {
                        let index = ms_data_index(corr, channel, num_corr);
                        values[index] = visibility;
                    }
                }
            }
            SyntheticFieldPredictor::SampledTotalPower(channel_values) => {
                for (channel, visibility) in channel_values.iter().enumerate() {
                    for corr in 0..num_corr {
                        let index = ms_data_index(corr, channel, num_corr);
                        values[index] = *visibility;
                    }
                }
            }
            SyntheticFieldPredictor::Analytic(predictor) => {
                return predict_analytic_row_values(predictor, uvw_m, num_corr);
            }
        }
    }
    values
}

struct TimedPredictionRows {
    rows: Vec<Vec<Complex32>>,
    worker_wall: Duration,
    gather: Duration,
}

struct ChannelPredictionChunk {
    start_channel: usize,
    values_by_row: Vec<Vec<Complex32>>,
}

#[cfg(test)]
fn predicted_data_values_for_rows_with_workers(
    predictor: Option<&SyntheticFieldPredictor>,
    spectral_setup: &SyntheticSpectralSetup,
    row_uvws: &[[f64; 3]],
    num_corr: usize,
    worker_count: usize,
) -> Vec<Vec<Complex32>> {
    predicted_data_values_for_rows_with_workers_timed(
        predictor,
        spectral_setup,
        row_uvws,
        num_corr,
        worker_count,
    )
    .rows
}

fn predicted_data_values_for_rows_with_workers_timed(
    predictor: Option<&SyntheticFieldPredictor>,
    spectral_setup: &SyntheticSpectralSetup,
    row_uvws: &[[f64; 3]],
    num_corr: usize,
    worker_count: usize,
) -> TimedPredictionRows {
    let Some(predictor) = predictor else {
        return TimedPredictionRows {
            rows: vec![
                vec![Complex32::new(0.0, 0.0); num_corr * spectral_setup.channel_count];
                row_uvws.len()
            ],
            worker_wall: Duration::ZERO,
            gather: Duration::ZERO,
        };
    };
    if worker_count <= 1 {
        let worker_started = Instant::now();
        let rows = row_uvws
            .iter()
            .map(|uvw| predicted_data_values(Some(predictor), spectral_setup, *uvw, num_corr))
            .collect();
        return TimedPredictionRows {
            rows,
            worker_wall: worker_started.elapsed(),
            gather: Duration::ZERO,
        };
    }
    if matches!(
        predictor,
        SyntheticFieldPredictor::Analytic(_) | SyntheticFieldPredictor::SampledTotalPower(_)
    ) {
        return predicted_data_values_for_row_chunks_timed(
            predictor,
            spectral_setup,
            row_uvws,
            num_corr,
            worker_count,
        );
    }

    let channel_count = spectral_setup.channel_count;
    let chunk_size = channel_count.div_ceil(worker_count);
    let worker_started = Instant::now();
    let chunks = thread::scope(|scope| {
        let mut handles = Vec::new();
        for start_channel in (0..channel_count).step_by(chunk_size) {
            let end_channel = (start_channel + chunk_size).min(channel_count);
            handles.push(scope.spawn(move || {
                predict_channel_chunk(
                    predictor,
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
    let worker_wall = worker_started.elapsed();

    let gather_started = Instant::now();
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
    let gather = gather_started.elapsed();

    TimedPredictionRows {
        rows: values_by_row,
        worker_wall,
        gather,
    }
}

fn predicted_data_values_for_row_chunks_timed(
    predictor: &SyntheticFieldPredictor,
    spectral_setup: &SyntheticSpectralSetup,
    row_uvws: &[[f64; 3]],
    num_corr: usize,
    worker_count: usize,
) -> TimedPredictionRows {
    let chunk_size = row_uvws.len().div_ceil(worker_count);
    let worker_started = Instant::now();
    let chunks = thread::scope(|scope| {
        let mut handles = Vec::new();
        for row_chunk in row_uvws.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                row_chunk
                    .iter()
                    .map(|uvw| {
                        predicted_data_values(Some(predictor), spectral_setup, *uvw, num_corr)
                    })
                    .collect::<Vec<_>>()
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("synthetic observation row prediction worker should not panic")
            })
            .collect::<Vec<_>>()
    });
    let worker_wall = worker_started.elapsed();

    let gather_started = Instant::now();
    let mut rows = Vec::with_capacity(row_uvws.len());
    for mut chunk in chunks {
        rows.append(&mut chunk);
    }
    TimedPredictionRows {
        rows,
        worker_wall,
        gather: gather_started.elapsed(),
    }
}

fn predict_channel_chunk(
    predictor: &SyntheticFieldPredictor,
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
        match predictor {
            SyntheticFieldPredictor::Sampled(predictors) => {
                let prediction_uvw_m = prediction_uvw_for_row(predictors, *uvw_m);
                for (offset, channel) in (start_channel..end_channel).enumerate() {
                    let predictor = &predictors[channel];
                    let visibility = predict_channel_visibility_preprojected(
                        predictor,
                        spectral_setup,
                        prediction_uvw_m,
                        channel,
                    );
                    for corr in 0..num_corr {
                        row_values[ms_data_index(corr, offset, num_corr)] = visibility;
                    }
                }
            }
            SyntheticFieldPredictor::Analytic(predictor) => {
                for (offset, channel) in (start_channel..end_channel).enumerate() {
                    let visibility =
                        predict_analytic_visibility(predictor, spectral_setup, *uvw_m, channel);
                    for corr in 0..num_corr {
                        row_values[ms_data_index(corr, offset, num_corr)] = visibility;
                    }
                }
            }
            SyntheticFieldPredictor::SampledTotalPower(channel_values) => {
                for (offset, channel) in (start_channel..end_channel).enumerate() {
                    let visibility = channel_values[channel];
                    for corr in 0..num_corr {
                        row_values[ms_data_index(corr, offset, num_corr)] = visibility;
                    }
                }
            }
        }
        values_by_row.push(row_values);
    }
    ChannelPredictionChunk {
        start_channel,
        values_by_row,
    }
}

fn prediction_uvw_for_row(predictors: &[SyntheticChannelPredictor], uvw_m: [f64; 3]) -> [f64; 3] {
    let Some(first) = predictors.first() else {
        return uvw_m;
    };
    let Some(model_reference_direction_rad) = first.model_reference_direction_rad else {
        return uvw_m;
    };
    debug_assert!(predictors.iter().all(|predictor| {
        predictor.model_reference_direction_rad == first.model_reference_direction_rad
            && predictor.phase_center_rad == first.phase_center_rad
    }));
    rotate_uvw_between_directions(uvw_m, first.phase_center_rad, model_reference_direction_rad)
}

fn predict_channel_visibility_preprojected(
    predictor: &SyntheticChannelPredictor,
    spectral_setup: &SyntheticSpectralSetup,
    prediction_uvw_m: [f64; 3],
    channel: usize,
) -> Complex32 {
    let frequency_hz =
        spectral_setup.start_frequency_hz + channel as f64 * spectral_setup.channel_width_hz;
    let wavelength_m = 299_792_458.0 / frequency_hz;
    let u_lambda = prediction_uvw_m[0] / wavelength_m;
    let v_lambda = prediction_uvw_m[1] / wavelength_m;
    if predictor.phase_offset.is_negligible() {
        return predictor.predictor.predict(u_lambda, v_lambda);
    }
    let w_lambda = prediction_uvw_m[2] / wavelength_m;
    let phase = std::f64::consts::TAU
        * (u_lambda * predictor.phase_offset.l_rad + v_lambda * predictor.phase_offset.m_rad
            - w_lambda * predictor.phase_offset.n_minus_one);
    let phase_shift = Complex32::new(phase.cos() as f32, phase.sin() as f32);
    predictor.predictor.predict(u_lambda, v_lambda) * phase_shift
}

fn predict_analytic_visibility(
    predictor: &AnalyticFieldPredictor,
    _spectral_setup: &SyntheticSpectralSetup,
    uvw_m: [f64; 3],
    channel: usize,
) -> Complex32 {
    let inverse_wavelength_m = predictor
        .inverse_wavelengths_m
        .get(channel)
        .copied()
        .unwrap_or(0.0);
    let u_lambda = uvw_m[0] * inverse_wavelength_m;
    let v_lambda = uvw_m[1] * inverse_wavelength_m;
    let w_lambda = uvw_m[2] * inverse_wavelength_m;
    let mut visibility = Complex32::new(0.0, 0.0);
    for field_component in &predictor.components {
        let component = &field_component.component;
        let mut amplitude = field_component
            .channel_amplitudes_jy
            .get(channel)
            .copied()
            .unwrap_or(0.0);
        if amplitude == 0.0 {
            continue;
        }
        if let Some(major_sigma_rad) = component.major_sigma_rad {
            let (sin_pa, cos_pa) = component.position_angle_rad.sin_cos();
            let u_rot = u_lambda * cos_pa + v_lambda * sin_pa;
            let v_rot = -u_lambda * sin_pa + v_lambda * cos_pa;
            let attenuation = (-2.0
                * std::f64::consts::PI
                * std::f64::consts::PI
                * (major_sigma_rad * major_sigma_rad * u_rot * u_rot
                    + component.minor_sigma_rad * component.minor_sigma_rad * v_rot * v_rot))
                .exp();
            amplitude *= attenuation;
        }
        let phase = std::f64::consts::TAU
            * (u_lambda * component.l_rad + v_lambda * component.m_rad
                - w_lambda * component.n_minus_one);
        visibility += Complex32::new(
            (amplitude * phase.cos()) as f32,
            (amplitude * phase.sin()) as f32,
        );
    }
    visibility
}

fn predict_analytic_row_values(
    predictor: &AnalyticFieldPredictor,
    uvw_m: [f64; 3],
    num_corr: usize,
) -> Vec<Complex32> {
    let channel_count = predictor.inverse_wavelengths_m.len();
    let Some(inverse_wavelength_0) = predictor.inverse_wavelengths_m.first().copied() else {
        return Vec::new();
    };
    let inverse_wavelength_step = predictor
        .inverse_wavelengths_m
        .get(1)
        .map(|next| next - inverse_wavelength_0)
        .unwrap_or(0.0);
    let mut values = vec![Complex32::new(0.0, 0.0); num_corr * channel_count];

    for field_component in &predictor.components {
        let component = &field_component.component;
        let phase_coefficient = std::f64::consts::TAU
            * (uvw_m[0] * component.l_rad + uvw_m[1] * component.m_rad
                - uvw_m[2] * component.n_minus_one);
        let (mut phase_sin, mut phase_cos) = (phase_coefficient * inverse_wavelength_0).sin_cos();
        let (step_sin, step_cos) = (phase_coefficient * inverse_wavelength_step).sin_cos();
        let gaussian_scale = component.major_sigma_rad.map(|major_sigma_rad| {
            let (sin_pa, cos_pa) = component.position_angle_rad.sin_cos();
            let u_rot_per_inverse_wavelength = uvw_m[0] * cos_pa + uvw_m[1] * sin_pa;
            let v_rot_per_inverse_wavelength = -uvw_m[0] * sin_pa + uvw_m[1] * cos_pa;
            -2.0 * std::f64::consts::PI
                * std::f64::consts::PI
                * (major_sigma_rad
                    * major_sigma_rad
                    * u_rot_per_inverse_wavelength
                    * u_rot_per_inverse_wavelength
                    + component.minor_sigma_rad
                        * component.minor_sigma_rad
                        * v_rot_per_inverse_wavelength
                        * v_rot_per_inverse_wavelength)
        });

        for channel in 0..channel_count {
            let mut amplitude = field_component.channel_amplitudes_jy[channel];
            if let Some(gaussian_scale) = gaussian_scale {
                let inverse_wavelength_m = predictor.inverse_wavelengths_m[channel];
                amplitude *= (gaussian_scale * inverse_wavelength_m * inverse_wavelength_m).exp();
            }
            if amplitude != 0.0 {
                let value = &mut values[channel * num_corr];
                value.re += (amplitude * phase_cos) as f32;
                value.im += (amplitude * phase_sin) as f32;
            }

            let next_phase_cos = phase_cos * step_cos - phase_sin * step_sin;
            phase_sin = phase_sin * step_cos + phase_cos * step_sin;
            phase_cos = next_phase_cos;
        }
    }

    for channel in 0..channel_count {
        let row_start = channel * num_corr;
        let visibility = values[row_start];
        values[row_start + 1..row_start + num_corr].fill(visibility);
    }
    values
}

fn analytic_primary_beam_taper_for_direction(
    l_rad: f64,
    m_rad: f64,
    pointing_offset_rad: [f64; 2],
    primary_beam: SyntheticPrimaryBeam,
    frequency_hz: f64,
) -> f64 {
    let l_from_pointing = l_rad - pointing_offset_rad[0];
    let m_from_pointing = m_rad - pointing_offset_rad[1];
    let primary_beam_evaluator = (!primary_beam.use_casa_vla_q_table).then(|| {
        PrimaryBeamVoltagePattern::new(PrimaryBeamModel::Airy {
            dish_diameter_m: primary_beam.dish_diameter_m,
            blockage_diameter_m: primary_beam.blockage_diameter_m,
        })
    });
    let voltage = synthetic_primary_beam_voltage_pattern(
        primary_beam,
        primary_beam_evaluator.as_ref(),
        l_from_pointing,
        m_from_pointing,
        frequency_hz,
    ) as f64;
    voltage * voltage
}

fn simobserve_channel_worker_count(
    request: &SyntheticObservationRequest,
    channel_count: usize,
) -> usize {
    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let requested = request.channel_workers.unwrap_or_else(|| {
        std::env::var("CASA_RS_SIMOBSERVE_CHANNEL_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(available)
    });
    let min_channels = std::env::var("CASA_RS_SIMOBSERVE_CHANNEL_PARALLEL_MIN_CHANNELS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64);
    match request.worker_policy {
        SyntheticWorkerPolicy::Auto => {
            simobserve_channel_worker_count_for(channel_count, requested, available, min_channels)
        }
        SyntheticWorkerPolicy::Fixed => requested.max(1).min(channel_count.max(1)),
    }
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

fn simobserve_row_worker_count(
    request: &SyntheticObservationRequest,
    row_count: usize,
    channel_count: usize,
) -> usize {
    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let requested = request.row_workers.unwrap_or_else(|| {
        std::env::var("CASA_RS_SIMOBSERVE_ROW_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(available)
    });
    match request.worker_policy {
        SyntheticWorkerPolicy::Auto => simobserve_row_worker_count_for(
            row_count,
            channel_count,
            requested,
            available,
            64 * 1024,
        ),
        SyntheticWorkerPolicy::Fixed => requested.max(1).min(row_count.max(1)),
    }
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
        let Some(correlation_count) = values.len().checked_div(channel_count) else {
            return;
        };
        if correlation_count == 0 {
            return;
        }
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
    direction_reference_rad: Option<[f64; 2]>,
    cell_size_rad_override: Option<[f64; 2]>,
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
    let (cell_size_rad, direction_increment_rad, ra_axis_increases_with_x, reference_direction_rad) =
        if let Some(wcs) = direction_wcs.as_ref() {
            let raw_increment = wcs.coordinate().increment();
            let raw_direction_increment_rad = if raw_increment.len() >= 2 {
                Some([raw_increment[0], raw_increment[1]])
            } else {
                None
            };
            let cell_size_rad =
                cell_size_rad_override.unwrap_or(fits_direction_cell_size_rad(wcs)?);
            let direction_increment_rad = raw_direction_increment_rad.map(|increment| {
                [
                    signed_cell_increment(increment[0], cell_size_rad[0]),
                    signed_cell_increment(increment[1], cell_size_rad[1]),
                ]
            });
            (
                cell_size_rad,
                direction_increment_rad,
                fits_direction_ra_axis_increases_with_x(wcs, path)?,
                Some(
                    direction_reference_rad
                        .unwrap_or(fits_direction_center_rad(wcs, nx, ny, path)?),
                ),
            )
        } else {
            (
                cell_size_rad_override.unwrap_or([axis_cell_rad[0].abs(), axis_cell_rad[1].abs()]),
                Some([
                    signed_cell_increment(
                        axis_cell_rad[0],
                        cell_size_rad_override
                            .unwrap_or([axis_cell_rad[0].abs(), axis_cell_rad[1].abs()])[0],
                    ),
                    signed_cell_increment(
                        axis_cell_rad[1],
                        cell_size_rad_override
                            .unwrap_or([axis_cell_rad[0].abs(), axis_cell_rad[1].abs()])[1],
                    ),
                ]),
                axis_cell_rad[0] > 0.0,
                direction_reference_rad,
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
        direction_increment_rad,
        direction_wcs,
        ra_axis_increases_with_x,
        reference_direction_rad,
    })
}

fn signed_cell_increment(original_increment_rad: f64, absolute_cell_size_rad: f64) -> f64 {
    if original_increment_rad.is_sign_negative() {
        -absolute_cell_size_rad.abs()
    } else {
        absolute_cell_size_rad.abs()
    }
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

fn observation_sample_times(
    request: &SyntheticObservationRequest,
    sample_count: usize,
) -> MsResult<Vec<f64>> {
    if request.allow_below_elevation_limit {
        return Ok((0..sample_count)
            .map(|sample| {
                request.start_time_mjd_seconds + (sample as f64 + 0.5) * request.integration_seconds
            })
            .collect());
    }

    let observatory = simulation_observatory_position(&request.telescope_name, &request.antennas);
    let elevation_margin_rad = antenna_elevation_margin_rad(&request.antennas, &observatory);
    let phase_center_rad = effective_fields(request)
        .first()
        .map(|field| field.phase_center_rad)
        .unwrap_or(request.phase_center_rad);
    let first_transit = next_transit_time_mjd_seconds(
        phase_center_rad,
        request.start_time_mjd_seconds,
        &observatory,
    )?;
    let mut sample_times = Vec::with_capacity(sample_count);
    let mut transit = first_transit;

    while sample_times.len() < sample_count {
        let remaining = sample_count - sample_times.len();
        let mut offsets = above_elevation_offsets_for_transit(
            request,
            phase_center_rad,
            transit,
            &observatory,
            elevation_margin_rad,
        )?;
        if offsets.is_empty() {
            return Err(MsError::SyntheticObservation(format!(
                "target never reaches the {:.1} deg elevation limit for telescope {}; set allow_below_elevation_limit=true to generate a below-limit track",
                request.elevation_limit_rad.to_degrees(),
                request.telescope_name
            )));
        }
        if remaining < offsets.len() {
            offsets.sort_by(|left, right| {
                left.abs()
                    .partial_cmp(&right.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
            });
            offsets.truncate(remaining);
        }
        offsets.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        sample_times.extend(offsets.into_iter().map(|offset| transit + offset));
        transit += SIDEREAL_DAY_SECONDS;
    }

    Ok(sample_times)
}

fn next_transit_time_mjd_seconds(
    phase_center_rad: [f64; 2],
    reference_time_mjd_seconds: f64,
    observatory: &MPosition,
) -> MsResult<f64> {
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(
            reference_time_mjd_seconds / 86_400.0,
            EpochRef::UT1,
        ))
        .with_position(observatory.clone())
        .with_direction(MDirection::from_angles(
            phase_center_rad[0],
            phase_center_rad[1],
            DirectionRef::J2000,
        ))
        .with_bundled_eop();
    let last = local_apparent_sidereal_time(&frame)?;
    let mut delta = circular_angle_delta_rad(phase_center_rad[0] - last);
    if delta < -1.0e-12 {
        delta += std::f64::consts::TAU;
    }
    Ok(reference_time_mjd_seconds + delta / std::f64::consts::TAU * SIDEREAL_DAY_SECONDS)
}

fn above_elevation_offsets_for_transit(
    request: &SyntheticObservationRequest,
    phase_center_rad: [f64; 2],
    transit_time_mjd_seconds: f64,
    observatory: &MPosition,
    elevation_margin_rad: f64,
) -> MsResult<Vec<f64>> {
    let slots_per_sidereal_day = (SIDEREAL_DAY_SECONDS / request.integration_seconds)
        .floor()
        .max(1.0) as usize;
    let day_duration = slots_per_sidereal_day as f64 * request.integration_seconds;
    let first_offset = -0.5 * day_duration + 0.5 * request.integration_seconds;
    let mut offsets = Vec::new();
    for slot in 0..slots_per_sidereal_day {
        let offset = first_offset + slot as f64 * request.integration_seconds;
        let time = transit_time_mjd_seconds + offset;
        let elevation = field_elevation_rad(phase_center_rad, time, observatory)?;
        if elevation - elevation_margin_rad >= request.elevation_limit_rad {
            offsets.push(offset);
        }
    }
    Ok(offsets)
}

pub(crate) fn zenith_transit_phase_center_rad(
    telescope_name: &str,
    antennas: &[SyntheticAntenna],
    transit_time_mjd_seconds: f64,
) -> MsResult<[f64; 2]> {
    let observatory = simulation_observatory_position(telescope_name, antennas);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(
            transit_time_mjd_seconds / 86_400.0,
            EpochRef::UT1,
        ))
        .with_position(observatory.clone())
        .with_bundled_eop();
    Ok([
        local_apparent_sidereal_time(&frame)?.rem_euclid(std::f64::consts::TAU),
        observatory.latitude_rad(),
    ])
}

fn elapsed_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn antenna_uvw_positions(
    antennas: &[SyntheticAntenna],
    phase_center_rad: [f64; 2],
    time_mjd_seconds: f64,
    observatory: &MPosition,
) -> MsResult<Vec<[f64; 3]>> {
    let context = UvwConversionContext::new(phase_center_rad, time_mjd_seconds, observatory)?;
    let obs_itrf = observatory.as_itrf();
    antennas
        .iter()
        .map(|antenna| {
            let ant_itrf = antenna.position_m;
            let baseline = [
                obs_itrf[0] - ant_itrf[0],
                obs_itrf[1] - ant_itrf[1],
                obs_itrf[2] - ant_itrf[2],
            ];
            Ok(context.baseline_itrf_to_uvw(baseline))
        })
        .collect()
}

struct UvwConversionContext {
    phase_center: MDirection,
    observatory_longitude_sin_cos: (f64, f64),
    polar_motion_rotation: [[f64; 3]; 3],
    diurnal_aberration_rotation: [[f64; 3]; 3],
    precession_nutation_inverse: [[f64; 3]; 3],
    inverse_aberration_rotation: [[f64; 3]; 3],
    inverse_solar_deflection_rotation: [[f64; 3]; 3],
}

impl UvwConversionContext {
    fn new(
        phase_center_rad: [f64; 2],
        time_mjd_seconds: f64,
        observatory: &MPosition,
    ) -> MsResult<Self> {
        let phase_center = MDirection::from_angles(
            phase_center_rad[0],
            phase_center_rad[1],
            DirectionRef::J2000,
        );
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(time_mjd_seconds / 86_400.0, EpochRef::UT1))
            .with_position(observatory.clone())
            .with_direction(phase_center.clone())
            .with_bundled_eop();
        let longitude = observatory.longitude_rad();
        let observatory_longitude_sin_cos = longitude.sin_cos();
        let last = local_apparent_sidereal_time(&frame)?;
        let tdb_mjd = epoch_mjd(&frame, EpochRef::TDB)?;
        let (xp, yp) = polar_motion_rad(&frame, tdb_mjd);
        let polar_motion_rotation = polar_motion_euler(-xp, -yp, last);
        let radius = vector_norm(observatory.as_itrf());
        let v_c = diurnal_aberration_factor(radius);
        let aberration_direction =
            spherical_to_cartesian(last, observatory.geocentric_latitude_rad());
        let diurnal_aberration_shift = scale_vector(aberration_direction, -v_c);
        let app_phase_center =
            phase_center
                .convert_to(DirectionRef::APP, &frame)
                .map_err(|error| {
                    MsError::SyntheticObservation(format!(
                        "UVW phase-center APP conversion failed: {error}"
                    ))
                })?;
        let tt = epoch_jd_pair(&frame, EpochRef::TT)?;
        let nutation = sofars::pnp::nutm80(tt.0, tt.1);
        let precession = sofars::pnp::pmat76(tt.0, tt.1);
        let precession_nutation_inverse =
            precession_nutation_inverse_matrix(&nutation, &precession);
        let phase_center_cosines = phase_center.cosines();
        let inverse_aberration_rotation = rotate_shift_matrix(
            inverse_aberration_shift(&phase_center, &frame)?,
            phase_center_cosines,
        );
        let inverse_solar_deflection_rotation = rotate_shift_matrix(
            inverse_solar_deflection_shift(&phase_center, &frame)?,
            phase_center_cosines,
        );

        Ok(Self {
            phase_center,
            observatory_longitude_sin_cos,
            polar_motion_rotation,
            diurnal_aberration_rotation: rotate_shift_matrix(
                diurnal_aberration_shift,
                app_phase_center.cosines(),
            ),
            precession_nutation_inverse,
            inverse_aberration_rotation,
            inverse_solar_deflection_rotation,
        })
    }

    fn baseline_itrf_to_uvw(&self, baseline_itrf_m: [f64; 3]) -> [f64; 3] {
        let baseline_len = vector_norm(baseline_itrf_m);
        if baseline_len == 0.0 {
            return [0.0, 0.0, 0.0];
        }

        let mut unit = scale_vector(baseline_itrf_m, 1.0 / baseline_len);
        unit = self.itrf_to_hadec(unit);
        unit = self.hadec_to_topo(unit);
        unit = rotate(&self.precession_nutation_inverse, &unit);
        unit = rotate(&self.inverse_aberration_rotation, &unit);
        unit = rotate(&self.inverse_solar_deflection_rotation, &unit);
        project_j2000_baseline_to_uvw(scale_vector(unit, baseline_len), &self.phase_center)
    }

    fn itrf_to_hadec(&self, vector: [f64; 3]) -> [f64; 3] {
        let (s, c) = self.observatory_longitude_sin_cos;
        let negated_y = -vector[1];
        [
            c * vector[0] - s * negated_y,
            s * vector[0] + c * negated_y,
            vector[2],
        ]
    }

    fn hadec_to_topo(&self, vector: [f64; 3]) -> [f64; 3] {
        let vector = rotate(
            &self.polar_motion_rotation,
            &[vector[0], -vector[1], vector[2]],
        );
        rotate(&self.diurnal_aberration_rotation, &vector)
    }
}

fn simulation_observatory_position(
    telescope_name: &str,
    antennas: &[SyntheticAntenna],
) -> MPosition {
    observatory_position_from_name(telescope_name)
        .unwrap_or_else(|| antenna_centroid_position(antennas))
}

fn observatory_position_from_name(name: &str) -> Option<MPosition> {
    MPosition::from_observatory_name(name).or_else(|| {
        if name.eq_ignore_ascii_case("ALMASD") {
            MPosition::from_observatory_name("ALMA")
        } else {
            None
        }
    })
}

fn antenna_centroid_position(antennas: &[SyntheticAntenna]) -> MPosition {
    let count = antennas.len().max(1) as f64;
    let [x, y, z] = antennas.iter().fold([0.0_f64; 3], |mut sum, antenna| {
        sum[0] += antenna.position_m[0];
        sum[1] += antenna.position_m[1];
        sum[2] += antenna.position_m[2];
        sum
    });
    MPosition::new_itrf(x / count, y / count, z / count)
}

struct SimobserveUvwProductionTrace {
    antenna1: usize,
    antenna2: usize,
    time_mjd_seconds: f64,
    time_tolerance_seconds: f64,
}

impl SimobserveUvwProductionTrace {
    fn from_env() -> Option<Self> {
        let antenna_text = std::env::var("CASA_RS_SIMOBSERVE_TRACE_UVW_ANTENNAS").ok()?;
        let (antenna1_text, antenna2_text) = antenna_text.split_once(',')?;
        let time_text = std::env::var("CASA_RS_SIMOBSERVE_TRACE_UVW_TIME_S").ok()?;
        let time_tolerance_seconds = std::env::var("CASA_RS_SIMOBSERVE_TRACE_UVW_TIME_TOL_S")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(1.0e-6);
        Some(Self {
            antenna1: antenna1_text.trim().parse().ok()?,
            antenna2: antenna2_text.trim().parse().ok()?,
            time_mjd_seconds: time_text.trim().parse().ok()?,
            time_tolerance_seconds,
        })
    }

    fn matches(&self, antenna1: usize, antenna2: usize, time_mjd_seconds: f64) -> bool {
        self.antenna1 == antenna1
            && self.antenna2 == antenna2
            && (self.time_mjd_seconds - time_mjd_seconds).abs() <= self.time_tolerance_seconds
    }
}

#[allow(clippy::too_many_arguments)]
fn trace_simobserve_production_uvw(
    row_number: usize,
    sample: usize,
    antenna1: usize,
    antenna2: usize,
    time_mjd_seconds: f64,
    phase_center_rad: [f64; 2],
    observatory: &MPosition,
    antennas: &[SyntheticAntenna],
    antenna_uvws: &[[f64; 3]],
    row_uvw: [f64; 3],
) {
    let obs_itrf = observatory.as_itrf();
    let ant1_itrf = antennas[antenna1].position_m;
    let ant2_itrf = antennas[antenna2].position_m;
    let ant1_baseline_itrf = [
        obs_itrf[0] - ant1_itrf[0],
        obs_itrf[1] - ant1_itrf[1],
        obs_itrf[2] - ant1_itrf[2],
    ];
    let ant2_baseline_itrf = [
        obs_itrf[0] - ant2_itrf[0],
        obs_itrf[1] - ant2_itrf[1],
        obs_itrf[2] - ant2_itrf[2],
    ];
    eprintln!(
        "simobserve_uvw_production_trace row={row_number} sample={sample} ant1={antenna1} ant2={antenna2} time_s={time_mjd_seconds:.15} phase_center_rad={phase_center_rad:?} obs_itrf={obs_itrf:?} ant1_itrf={ant1_itrf:?} ant2_itrf={ant2_itrf:?} ant1_obs_minus_ant_itrf={ant1_baseline_itrf:?} ant2_obs_minus_ant_itrf={ant2_baseline_itrf:?} ant1_uvw={:?} ant2_uvw={:?} row_uvw={row_uvw:?}",
        antenna_uvws[antenna1], antenna_uvws[antenna2],
    );
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

fn rotate_shift_matrix(shift: [f64; 3], reference_direction: [f64; 3]) -> [[f64; 3]; 3] {
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
    multiply_transpose_left(&rot, &corrected)
}

fn precession_nutation_inverse_matrix(
    nutation: &[[f64; 3]; 3],
    precession: &[[f64; 3]; 3],
) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for col in 0..3 {
        let mut basis = [0.0; 3];
        basis[col] = 1.0;
        let transformed = rotate_t(precession, &rotate_t(nutation, &basis));
        for row in 0..3 {
            result[row][col] = transformed[row];
        }
    }
    result
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

fn multiply_transpose_left(left: &[[f64; 3]; 3], right: &[[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[0][row] * right[0][col]
                + left[1][row] * right[1][col]
                + left[2][row] * right[2][col];
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
    fn spectral_setup_reference_frequency_matches_casa_center_channel_convention() {
        let setup = SyntheticSpectralSetup {
            name: "spw".to_string(),
            start_frequency_hz: 8.0e9,
            channel_width_hz: 2.0e6,
            channel_count: 512,
        };

        assert_eq!(setup.channel_frequencies_hz()[0], 8.0e9);
        assert_eq!(setup.channel_frequencies_hz()[511], 9.022e9);
        assert_eq!(setup.reference_frequency_hz(), 8.512e9);
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

    fn analytic_test_spectral_setup(channel_count: usize) -> SyntheticSpectralSetup {
        SyntheticSpectralSetup {
            name: "test".to_string(),
            start_frequency_hz: 1.0e9,
            channel_width_hz: 1.0e6,
            channel_count,
        }
    }

    fn analytic_test_predictor(
        spectral_setup: &SyntheticSpectralSetup,
        components: Vec<SyntheticAnalyticComponent>,
    ) -> AnalyticFieldPredictor {
        let model = SyntheticAnalyticComponentModel {
            schema_version: Some(1),
            name: None,
            components,
        };
        let prepared = prepare_analytic_component_model(&model).expect("prepare analytic model");
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            "model.fits",
            "out.ms",
            vec![
                SyntheticAntenna::vla("A0", "A0", [0.0, 0.0, 0.0]),
                SyntheticAntenna::vla("A1", "A1", [1.0, 0.0, 0.0]),
            ],
        );
        request.spectral_setup = spectral_setup.clone();
        build_analytic_field_predictor(
            &prepared,
            &request,
            request.phase_center_rad,
            [0.0, 0.0],
            SyntheticPrimaryBeam {
                use_casa_vla_q_table: false,
                dish_diameter_m: 25.0,
                blockage_diameter_m: 0.0,
            },
        )
        .expect("build analytic predictor")
    }

    #[test]
    fn fits_model_cube_uses_per_channel_planes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("cube.fits");
        write_test_fits_cube(&path, 3);

        let model = read_fits_model_image(&path, None, None, None, 3).expect("read model cube");

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

        let model = read_fits_model_image(&path, None, None, None, 4).expect("read model plane");

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
    fn simobserve_fixed_worker_policy_honors_explicit_counts() {
        let mut request = SyntheticObservationRequest::vla_ppdisk("model.fits", "out.ms", vec![]);
        request.worker_policy = SyntheticWorkerPolicy::Fixed;
        request.channel_workers = Some(12);
        request.row_workers = Some(3);

        assert_eq!(simobserve_channel_worker_count(&request, 8), 8);
        assert_eq!(simobserve_row_worker_count(&request, 100, 1), 3);
    }

    #[test]
    fn analytic_point_source_predicts_exact_zero_baseline_flux() {
        let setup = analytic_test_spectral_setup(1);
        let predictor = analytic_test_predictor(
            &setup,
            vec![SyntheticAnalyticComponent::Point {
                name: None,
                l_rad: 0.0,
                m_rad: 0.0,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 2.5,
                    spectral_index: 0.0,
                    reference_frequency_hz: None,
                    line_peak_jy: 0.0,
                    line_center_fraction: 0.5,
                    line_sigma_fraction: 0.1,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            }],
        );

        let visibility = predict_analytic_visibility(&predictor, &setup, [0.0, 0.0, 0.0], 0);

        assert!((visibility.re - 2.5).abs() < 1.0e-6);
        assert!(visibility.im.abs() < 1.0e-6);
    }

    #[test]
    fn analytic_gaussian_visibility_falls_with_baseline() {
        let setup = analytic_test_spectral_setup(1);
        let predictor = analytic_test_predictor(
            &setup,
            vec![SyntheticAnalyticComponent::Gaussian {
                name: None,
                l_rad: 0.0,
                m_rad: 0.0,
                major_fwhm_rad: 2.0e-4,
                minor_fwhm_rad: 2.0e-4,
                position_angle_rad: 0.0,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 1.0,
                    spectral_index: 0.0,
                    reference_frequency_hz: None,
                    line_peak_jy: 0.0,
                    line_center_fraction: 0.5,
                    line_sigma_fraction: 0.1,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            }],
        );

        let zero = predict_analytic_visibility(&predictor, &setup, [0.0, 0.0, 0.0], 0);
        let long = predict_analytic_visibility(&predictor, &setup, [1_000.0, 0.0, 0.0], 0);

        assert!((zero.re - 1.0).abs() < 1.0e-6);
        assert!(long.norm() < zero.norm());
        assert!(long.re > 0.0);
    }

    #[test]
    fn analytic_component_spectrum_varies_by_channel() {
        let setup = analytic_test_spectral_setup(5);
        let predictor = analytic_test_predictor(
            &setup,
            vec![SyntheticAnalyticComponent::Point {
                name: None,
                l_rad: 0.0,
                m_rad: 0.0,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 1.0,
                    spectral_index: -0.5,
                    reference_frequency_hz: None,
                    line_peak_jy: 5.0,
                    line_center_fraction: 0.0,
                    line_sigma_fraction: 0.05,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            }],
        );

        let first = predict_analytic_visibility(&predictor, &setup, [0.0, 0.0, 0.0], 0);
        let last = predict_analytic_visibility(&predictor, &setup, [0.0, 0.0, 0.0], 4);

        assert!(first.re > last.re * 4.0);
        assert_ne!(first, last);
    }

    #[test]
    fn analytic_row_predictor_matches_scalar_channel_predictor() {
        let setup = analytic_test_spectral_setup(8);
        let spectrum = SyntheticAnalyticSpectrum {
            flux_jy: 1.0,
            spectral_index: -0.4,
            reference_frequency_hz: None,
            line_peak_jy: 0.7,
            line_center_fraction: 0.35,
            line_sigma_fraction: 0.08,
            absorption_peak_jy: 0.2,
            absorption_center_fraction: 0.7,
            absorption_sigma_fraction: 0.12,
        };
        let predictor = analytic_test_predictor(
            &setup,
            vec![
                SyntheticAnalyticComponent::Point {
                    name: None,
                    l_rad: 2.0e-5,
                    m_rad: -1.5e-5,
                    spectrum: spectrum.clone(),
                },
                SyntheticAnalyticComponent::Gaussian {
                    name: None,
                    l_rad: -3.0e-5,
                    m_rad: 2.5e-5,
                    major_fwhm_rad: 1.8e-4,
                    minor_fwhm_rad: 1.1e-4,
                    position_angle_rad: 0.4,
                    spectrum,
                },
            ],
        );
        let uvw = [810.0, -125.0, 19.0];
        let row = predict_analytic_row_values(&predictor, uvw, 4);

        for channel in 0..setup.channel_count {
            let scalar = predict_analytic_visibility(&predictor, &setup, uvw, channel);
            for corr in 0..4 {
                let value = row[ms_data_index(corr, channel, 4)];
                assert!(
                    (value - scalar).norm() < 2.0e-6,
                    "channel {channel} corr {corr}: row={value:?} scalar={scalar:?}"
                );
            }
        }
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
        let mut request = SyntheticObservationRequest::vla_ppdisk("model.fits", "out.ms", vec![]);
        request.worker_policy = SyntheticWorkerPolicy::Fixed;
        request.row_workers = Some(4);

        let serial_nonzero =
            apply_corruption_and_count_rows(Some(&corruption), &row_specs, &mut serial, 16, 3);
        let parallel_nonzero = apply_corruption_and_count_rows_with_workers(
            &request,
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
                phase_offset: ModelPhaseOffset {
                    l_rad: 1.0e-6,
                    m_rad: -2.0e-6,
                    n_minus_one: 3.0e-12,
                },
                phase_center_rad: [1.0, -0.5],
                model_reference_direction_rad: None,
            })
            .collect::<Vec<_>>();
        let predictor = SyntheticFieldPredictor::Sampled(predictors);
        let row_uvws = [[10.0, 20.0, 0.0], [100.0, -30.0, 5.0], [-55.0, 7.0, -3.0]];

        let serial = predicted_data_values_for_rows_with_workers(
            Some(&predictor),
            &spectral_setup,
            &row_uvws,
            2,
            1,
        );
        let parallel = predicted_data_values_for_rows_with_workers(
            Some(&predictor),
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
        let mut pixels = Array2::<f32>::zeros((6, 6));
        pixels[(3, 3)] = 1.0;
        pixels[(4, 3)] = 1.0;
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
            direction_increment_rad: Some([-1.0e-3, 1.0e-3]),
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

        assert_eq!(corrected[(3, 3)], 1.0);
        assert!(
            corrected[(4, 3)] < 1.0,
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

        request.telescope_name = "ALMASD".to_string();
        request.observation_mode = SyntheticObservationMode::TotalPower;
        let total_power_primary_beam = synthetic_primary_beam(&request);

        assert_eq!(total_power_primary_beam.dish_diameter_m, 10.86);
        assert_eq!(total_power_primary_beam.blockage_diameter_m, 0.75);
        assert!(!total_power_primary_beam.use_casa_vla_q_table);
    }

    #[test]
    fn simobserve_uvw_trace_when_requested() {
        let Ok(a1_text) = std::env::var("CASA_RS_SIMOBSERVE_UVW_TRACE_A1_ITRF") else {
            return;
        };
        let Ok(a2_text) = std::env::var("CASA_RS_SIMOBSERVE_UVW_TRACE_A2_ITRF") else {
            return;
        };
        let Ok(time_text) = std::env::var("CASA_RS_SIMOBSERVE_UVW_TRACE_TIME_S") else {
            return;
        };
        let a1 = parse_trace_triplet(&a1_text);
        let a2 = parse_trace_triplet(&a2_text);
        let time_s = time_text.parse::<f64>().expect("trace time seconds");
        let phase_center_rad = [-1.570_794_075_320_161_2, -0.401_423_790_643_226_1];
        let observatory = std::env::var("CASA_RS_SIMOBSERVE_UVW_TRACE_OBSERVATORY")
            .ok()
            .and_then(|name| MPosition::from_observatory_name(&name))
            .unwrap_or_else(|| {
                MPosition::new_itrf(
                    2_225_052.376_592_874_5,
                    -5_440_045.715_534_717,
                    -2_481_673.806_727_262_7,
                )
            });
        let context = UvwConversionContext::new(phase_center_rad, time_s, &observatory).unwrap();
        let a1_uvw = context.baseline_itrf_to_uvw(a1);
        let a2_uvw = context.baseline_itrf_to_uvw(a2);
        eprintln!("TRACE_A1_UVW={a1_uvw:?}");
        eprintln!("TRACE_A2_UVW={a2_uvw:?}");
        eprintln!(
            "TRACE_A2_MINUS_A1_UVW={:?}",
            [
                a2_uvw[0] - a1_uvw[0],
                a2_uvw[1] - a1_uvw[1],
                a2_uvw[2] - a1_uvw[2],
            ]
        );
    }

    #[test]
    fn simobserve_model_predictor_trace_matches_casacore_gridder_when_requested() {
        let Ok(model_path) = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_FITS") else {
            return;
        };
        let Ok(uvw_text) = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_UVW_M") else {
            return;
        };
        let uvw_m = parse_trace_triplet(&uvw_text);
        let channel = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_CHANNEL")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let channel_count = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_CHANNEL_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(channel + 1);
        let start_frequency_hz = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_START_HZ")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(230.0e9);
        let channel_width_hz = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_WIDTH_HZ")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(2.0e6);
        let frequency_hz = start_frequency_hz + channel as f64 * channel_width_hz;
        let model_path = PathBuf::from(model_path);
        let model = read_fits_model_image(&model_path, None, None, None, channel_count)
            .expect("read trace FITS model");
        let phase_center_rad = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_PHASE_CENTER_RAD")
            .ok()
            .map(|value| parse_trace_triplet(&value)[0..2].try_into().unwrap())
            .unwrap_or([4.712_391_234_768_306, -0.401_423_788_703_971_4]);
        if let Some(wcs) = model.direction_wcs.as_ref() {
            let coordinate = wcs.coordinate();
            let pointing_pixel = coordinate
                .to_pixel(&phase_center_rad)
                .expect("trace phase center pixel");
            eprintln!(
                "simobserve_model_predictor_trace_wcs reference_pixel={:?} reference_value={:?} increment={:?} phase_center_rad={phase_center_rad:?} pointing_pixel={pointing_pixel:?} reference_direction_rad={:?} ra_axis_increases_with_x={}",
                coordinate.reference_pixel(),
                coordinate.reference_value(),
                coordinate.increment(),
                model.reference_direction_rad,
                model.ra_axis_increases_with_x,
            );
        }
        let primary_beam = SyntheticPrimaryBeam {
            use_casa_vla_q_table: false,
            dish_diameter_m: std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_DISH_M")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(10.7),
            blockage_diameter_m: std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_BLOCKAGE_M")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.75),
        };
        let source_pixels = model.pixels_for_channel(channel);
        let casa_oriented_pixels = trace_oriented_pixels_for_beam_power(
            &model,
            source_pixels,
            phase_center_rad,
            frequency_hz,
            primary_beam,
            2,
        );
        let casa_oriented_moments = trace_pixels_moments(&casa_oriented_pixels);
        eprintln!(
            "simobserve_model_predictor_trace_pixels sum={:.9e} peak={:.9e} x_mean={:.9e} y_mean={:.9e} x_rms={:.9e} y_rms={:.9e}",
            casa_oriented_moments.sum,
            casa_oriented_moments.peak,
            casa_oriented_moments.x_mean(),
            casa_oriented_moments.y_mean(),
            casa_oriented_moments.x_rms(),
            casa_oriented_moments.y_rms(),
        );
        if let Ok(path) = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_DUMP_PIXELS") {
            trace_dump_pixels(&casa_oriented_pixels, &path);
        }
        let geometry = ImageGeometry {
            image_shape: [
                casa_oriented_pixels.shape()[0],
                casa_oriented_pixels.shape()[1],
            ],
            cell_size_rad: model.cell_size_rad,
        };
        let predictor = StandardMfsModelPredictor::new(geometry, &casa_oriented_pixels)
            .expect("build native predictor");
        let phase_offset = casa_model_phase_offset(&model, phase_center_rad);
        let casa_small_shift_disabled = model
            .reference_direction_rad
            .map(|reference_direction_rad| {
                let delta_ra =
                    circular_angle_delta_rad(reference_direction_rad[0] - phase_center_rad[0]);
                delta_ra.abs() < model.cell_size_rad[0].abs()
                    && (reference_direction_rad[1] - phase_center_rad[1]).abs()
                        < model.cell_size_rad[1].abs()
            })
            .unwrap_or(false);
        let prediction_uvw_m = if let Some(model_reference_direction_rad) =
            model.reference_direction_rad
        {
            rotate_uvw_between_directions(uvw_m, phase_center_rad, model_reference_direction_rad)
        } else {
            uvw_m
        };
        let wavelength_m = 299_792_458.0 / frequency_hz;
        let u_lambda = prediction_uvw_m[0] / wavelength_m;
        let v_lambda = prediction_uvw_m[1] / wavelength_m;
        let w_lambda = prediction_uvw_m[2] / wavelength_m;
        let phase = std::f64::consts::TAU
            * (u_lambda * phase_offset.l_rad + v_lambda * phase_offset.m_rad
                - w_lambda * phase_offset.n_minus_one);
        let phase_shift = Complex32::new(phase.cos() as f32, phase.sin() as f32);
        let native_gridder = predictor.predict(u_lambda, v_lambda);
        let native = native_gridder * phase_shift;
        let grid_shape = [
            trace_casa_composite_padded_len(geometry.image_shape[0], 1.3),
            trace_casa_composite_padded_len(geometry.image_shape[1], 1.3),
        ];
        let cpp = casa_test_support::gridder_interop::cpp_convolve_gridder_predict_visibility_2d(
            grid_shape,
            geometry.image_shape,
            [
                grid_shape[0] as f64 * geometry.cell_size_rad[0],
                grid_shape[1] as f64 * geometry.cell_size_rad[1],
            ],
            [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0],
            [u_lambda, -v_lambda],
            casa_oriented_pixels
                .as_slice()
                .expect("contiguous oriented model"),
        )
        .expect("casacore gridder predictor");
        let cpp_gridder = Complex32::new(cpp.re, cpp.im);
        let cpp_value = cpp_gridder * phase_shift;
        let casa_data = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_CASA_DATA")
            .ok()
            .map(|value| parse_trace_complex(&value));
        for beam_power in [0, 1, 2] {
            let pixels = trace_oriented_pixels_for_beam_power(
                &model,
                source_pixels,
                phase_center_rad,
                frequency_hz,
                primary_beam,
                beam_power,
            );
            if let Ok(prefix) = std::env::var("CASA_RS_SIMOBSERVE_MODEL_TRACE_DUMP_VARIANT_PREFIX")
            {
                trace_dump_pixels(&pixels, &format!("{prefix}-beam{beam_power}.bin"));
            }
            let trace_predictor =
                StandardMfsModelPredictor::new(geometry, &pixels).expect("build trace predictor");
            let trace_gridder = trace_predictor.predict(u_lambda, v_lambda);
            let trace_native = trace_gridder * phase_shift;
            let casa_delta = casa_data
                .map(|casa_data| (trace_native - casa_data).norm())
                .unwrap_or(f32::NAN);
            let moments = trace_pixels_moments(&pixels);
            eprintln!(
                "simobserve_model_predictor_trace_variant beam_power={beam_power} model_sum={:.9e} model_peak={:.9e} x_mean={:.9e} y_mean={:.9e} x_rms={:.9e} y_rms={:.9e} native={trace_native:?} casa_delta_abs={casa_delta:.9e}",
                moments.sum,
                moments.peak,
                moments.x_mean(),
                moments.y_mean(),
                moments.x_rms(),
                moments.y_rms(),
            );
        }
        if casa_small_shift_disabled {
            let no_shift_gridder =
                predictor.predict(uvw_m[0] / wavelength_m, uvw_m[1] / wavelength_m);
            let casa_delta = casa_data
                .map(|casa_data| (no_shift_gridder - casa_data).norm())
                .unwrap_or(f32::NAN);
            eprintln!(
                "simobserve_model_predictor_trace_casa_small_shift_disabled native_no_rotation_no_phase={no_shift_gridder:?} casa_delta_abs={casa_delta:.9e}"
            );
        }
        eprintln!(
            "simobserve_model_predictor_trace channel={channel} frequency_hz={frequency_hz:.9e} uvw_m={uvw_m:?} prediction_uvw_m={prediction_uvw_m:?} u_lambda={u_lambda:.9e} v_lambda={v_lambda:.9e} w_lambda={w_lambda:.9e} native_gridder={native_gridder:?} cpp_gridder={cpp_gridder:?} gridder_delta_abs={:.9e} phase_offset=({:.9e},{:.9e},{:.9e}) phase_rad={phase:.9e} native={native:?} cpp={cpp_value:?} final_delta_abs={:.9e}",
            (native_gridder - cpp_gridder).norm(),
            phase_offset.l_rad,
            phase_offset.m_rad,
            phase_offset.n_minus_one,
            (native - cpp_value).norm(),
        );
        assert!((native_gridder - cpp_gridder).norm() < 1.0e-3);
    }

    fn trace_oriented_pixels_for_beam_power(
        model: &FitsModelImage,
        source_pixels: &Array2<f32>,
        phase_center_rad: [f64; 2],
        frequency_hz: f64,
        primary_beam: SyntheticPrimaryBeam,
        beam_power: u32,
    ) -> Array2<f32> {
        let beam_corrected_pixels = apply_simulator_primary_beam_power(
            model,
            source_pixels,
            phase_center_rad,
            frequency_hz,
            [0.0, 0.0],
            primary_beam,
            beam_power,
        );
        let mut casa_oriented_pixels = Array2::<f32>::zeros(beam_corrected_pixels.raw_dim());
        if model.ra_axis_increases_with_x {
            for x in 0..beam_corrected_pixels.shape()[0] {
                for y in 0..beam_corrected_pixels.shape()[1] {
                    casa_oriented_pixels[(beam_corrected_pixels.shape()[0] - 1 - x, y)] =
                        beam_corrected_pixels[(x, y)];
                }
            }
        } else {
            casa_oriented_pixels.assign(&beam_corrected_pixels);
        }
        casa_oriented_pixels
    }

    struct TracePixelMoments {
        sum: f64,
        peak: f64,
        x_sum: f64,
        y_sum: f64,
        xx_sum: f64,
        yy_sum: f64,
    }

    impl TracePixelMoments {
        fn x_mean(&self) -> f64 {
            self.x_sum / self.sum
        }

        fn y_mean(&self) -> f64 {
            self.y_sum / self.sum
        }

        fn x_rms(&self) -> f64 {
            (self.xx_sum / self.sum).sqrt()
        }

        fn y_rms(&self) -> f64 {
            (self.yy_sum / self.sum).sqrt()
        }
    }

    fn trace_pixels_moments(pixels: &Array2<f32>) -> TracePixelMoments {
        let mut moments = TracePixelMoments {
            sum: 0.0,
            peak: 0.0,
            x_sum: 0.0,
            y_sum: 0.0,
            xx_sum: 0.0,
            yy_sum: 0.0,
        };
        for ((x, y), value) in pixels.indexed_iter() {
            let value = f64::from(*value);
            let x = x as f64;
            let y = y as f64;
            moments.sum += value;
            moments.peak = moments.peak.max(value.abs());
            moments.x_sum += value * x;
            moments.y_sum += value * y;
            moments.xx_sum += value * x * x;
            moments.yy_sum += value * y * y;
        }
        moments
    }

    fn trace_dump_pixels(pixels: &Array2<f32>, path: &str) {
        let mut bytes = Vec::with_capacity(pixels.len() * std::mem::size_of::<f32>());
        for value in pixels.iter() {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        std::fs::write(path, bytes).expect("write trace pixel dump");
    }

    fn parse_trace_triplet(text: &str) -> [f64; 3] {
        let values = text
            .split(',')
            .map(|part| part.trim().parse::<f64>().expect("trace triplet value"))
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 3);
        [values[0], values[1], values[2]]
    }

    fn parse_trace_complex(text: &str) -> Complex32 {
        let values = text
            .split(',')
            .map(|part| part.trim().parse::<f32>().expect("trace complex value"))
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        Complex32::new(values[0], values[1])
    }

    fn trace_casa_composite_padded_len(image_len: usize, padding_factor: f64) -> usize {
        let padded = (padding_factor * image_len as f64 - 0.5).floor() as usize;
        let mut padded = padded.max(image_len);
        if padded % 2 != 0 {
            padded += 1;
        }
        while !trace_is_casa_composite_len(padded) {
            padded += 2;
        }
        padded
    }

    fn trace_is_casa_composite_len(mut value: usize) -> bool {
        for factor in [2, 3, 5] {
            while value > 1 && value % factor == 0 {
                value /= factor;
            }
        }
        value == 1
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
                uvw: [10.0, 0.0, 1.0],
            },
            MainRowVisibilitySpec {
                antenna1: 0,
                antenna2: 2,
                uvw: [100.0, 0.0, -1.0],
            },
            MainRowVisibilitySpec {
                antenna1: 1,
                antenna2: 2,
                uvw: [10.0, 0.0, -1.0],
            },
        ];

        assert_eq!(
            shadowed_antennas_for_rows(&rows, &antennas),
            vec![true, false, true]
        );
    }

    #[test]
    fn elevation_limit_identifies_below_limit_full_track_samples() {
        let observatory = MPosition::from_observatory_name("ALMA").expect("ALMA position");
        let phase_center = [-1.570_794_075_320_161, -0.401_423_790_643_226_1];
        let start_time_mjd_seconds = 4_895_178_609.486_364;
        let first_sample = start_time_mjd_seconds + 5.0;
        let transit_sample = start_time_mjd_seconds + 43_200.0;

        assert!(
            field_elevation_rad(phase_center, first_sample, &observatory).unwrap()
                < DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD
        );
        assert!(
            field_elevation_rad(phase_center, transit_sample, &observatory).unwrap()
                > DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD
        );
    }

    #[test]
    fn default_sample_scheduler_splits_long_tracks_into_above_limit_sessions() {
        let antennas = tutorial_vla_a_antennas();
        let start_time_mjd_seconds = 59_000.25 * 86_400.0;
        let phase_center =
            zenith_transit_phase_center_rad("VLA", &antennas, start_time_mjd_seconds).unwrap();
        let mut request = SyntheticObservationRequest::vla_ppdisk("model.fits", "out.ms", antennas);
        request.telescope_name = "VLA".to_string();
        request.phase_center_rad = phase_center;
        request.start_time_mjd_seconds = start_time_mjd_seconds;
        request.duration_seconds = 20.0 * 3_600.0;
        request.integration_seconds = 600.0;

        let samples = time_sample_count(request.duration_seconds, request.integration_seconds);
        let times = observation_sample_times(&request, samples).unwrap();
        let observatory =
            simulation_observatory_position(&request.telescope_name, &request.antennas);
        let elevation_margin_rad = antenna_elevation_margin_rad(&request.antennas, &observatory);

        assert_eq!(times.len(), samples);
        assert!(times.windows(2).any(|pair| pair[1] - pair[0] > 3_600.0));
        for time in &times {
            let elevation = field_elevation_rad(phase_center, *time, &observatory).unwrap();
            assert!(
                elevation - elevation_margin_rad >= request.elevation_limit_rad,
                "scheduled sample below elevation limit: {} deg",
                elevation.to_degrees()
            );
        }

        let mut sessions = Vec::new();
        let mut start = 0usize;
        for index in 1..times.len() {
            if times[index] - times[index - 1] > 3_600.0 {
                sessions.push(&times[start..index]);
                start = index;
            }
        }
        sessions.push(&times[start..]);
        let final_session = sessions.last().expect("final session");
        let final_center = 0.5 * (final_session[0] + final_session[final_session.len() - 1]);
        let final_transit = next_transit_time_mjd_seconds(
            phase_center,
            request.start_time_mjd_seconds,
            &observatory,
        )
        .unwrap()
            + (sessions.len() - 1) as f64 * SIDEREAL_DAY_SECONDS;
        assert!(
            (final_center - final_transit).abs() <= request.integration_seconds,
            "final short session is not centered on transit"
        );
    }

    #[test]
    fn below_elevation_override_preserves_continuous_sample_times() {
        let antennas = tutorial_vla_a_antennas();
        let mut request = SyntheticObservationRequest::vla_ppdisk("model.fits", "out.ms", antennas);
        request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
        request.duration_seconds = 1_200.0;
        request.integration_seconds = 300.0;
        request.allow_below_elevation_limit = true;

        assert_eq!(
            observation_sample_times(&request, 4).unwrap(),
            vec![
                request.start_time_mjd_seconds + 150.0,
                request.start_time_mjd_seconds + 450.0,
                request.start_time_mjd_seconds + 750.0,
                request.start_time_mjd_seconds + 1_050.0,
            ]
        );
    }
}
