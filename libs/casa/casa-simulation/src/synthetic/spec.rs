// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed inputs for the developer generator and native test fixtures.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
const DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD: f64 = 20.0_f64.to_radians();
fn default_simobserve_elevation_limit_rad() -> f64 {
    DEFAULT_SIMOBSERVE_ELEVATION_LIMIT_RAD
}
/// Antenna configuration row for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Antenna {
    /// Antenna name, for example `VLA01`.
    pub name: String,
    /// Station or pad name, for example `N01`.
    pub station: String,
    /// ITRF position in meters.
    pub position_m: [f64; 3],
    /// Dish diameter in meters.
    pub dish_diameter_m: f64,
}

impl Antenna {
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
pub struct Field {
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
pub fn tutorial_vla_a_antennas() -> Vec<Antenna> {
    VLA_A_ANTENNAS
        .iter()
        .map(|antenna| {
            Antenna::vla(
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

/// One LSRK spectral window of an idealized synthetic observation.
///
/// Channel frequencies are used directly for prediction and recorded as LSRK
/// in the MeasurementSet. The simulator does not model time-dependent TOPO
/// tuning or Doppler resampling of an observed spectrum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpectralSetup {
    /// Spectral-window name.
    pub name: String,
    /// First channel center frequency in Hz, in LSRK.
    pub start_frequency_hz: f64,
    /// Channel width in Hz.
    pub channel_width_hz: f64,
    /// Number of channels.
    pub channel_count: usize,
}

impl SpectralSetup {
    /// Return the channel-center frequencies in Hz.
    pub fn channel_frequencies_hz(&self) -> Vec<f64> {
        (0..self.channel_count)
            .map(|index| self.start_frequency_hz + index as f64 * self.channel_width_hz)
            .collect()
    }

    /// Return the central reference frequency in Hz.
    pub fn reference_frequency_hz(&self) -> f64 {
        self.start_frequency_hz + (self.channel_count / 2) as f64 * self.channel_width_hz
    }
}

/// Polarization/correlation layout for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PolarizationSetup {
    /// Receptor basis used in FEED and POLARIZATION metadata.
    #[serde(default)]
    pub basis: PolarizationBasis,
    /// Number of correlations written per visibility row. Supported values are
    /// 1, 2, and 4.
    pub correlation_count: usize,
    /// Fractional source Stokes `[Q/I, U/I, V/I]` applied to the scalar sky
    /// model during synthetic visibility prediction. All zero, the model is
    /// unpolarized, predicted as CASA predicts it: the parallel hands hold
    /// Stokes I and the cross hands of four correlations are zero.
    #[serde(default)]
    pub source_stokes_fractional_quv: [f64; 3],
}

impl Default for PolarizationSetup {
    fn default() -> Self {
        Self {
            basis: PolarizationBasis::Circular,
            correlation_count: 2,
            source_stokes_fractional_quv: [0.0; 3],
        }
    }
}

impl PolarizationSetup {
    /// Build a setup from the dialog-facing correlation count.
    pub fn new(basis: PolarizationBasis, correlation_count: usize) -> super::Result<Self> {
        let setup = Self {
            basis,
            correlation_count,
            source_stokes_fractional_quv: [0.0; 3],
        };
        setup.validate()?;
        Ok(setup)
    }

    /// Validate that the correlation layout is supported by the native writer.
    pub fn validate(&self) -> super::Result<()> {
        match self.correlation_count {
            1 | 2 | 4 => {}
            other => Err(super::error(format!(
                "polarization correlation_count {other} is unsupported; expected 1, 2, or 4"
            )))?,
        }
        if self
            .source_stokes_fractional_quv
            .iter()
            .any(|value| !value.is_finite())
        {
            return Err(super::error(
                "source Stokes fractions must be finite".to_string(),
            ));
        }
        let polarized_fraction = self
            .source_stokes_fractional_quv
            .iter()
            .map(|value| value * value)
            .sum::<f64>()
            .sqrt();
        if polarized_fraction > 1.0 + 1.0e-12 {
            return Err(super::error(
                "source fractional polarization must not exceed unity".to_string(),
            ));
        }
        if polarized_fraction > 0.0
            && (self.basis != PolarizationBasis::Linear || self.correlation_count != 4)
        {
            return Err(super::error(
                "polarized synthetic prediction currently requires four linear correlations"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Receptor basis used for synthetic polarization metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolarizationBasis {
    /// Circular receptors: R/L and RR/RL/LR/LL Stokes codes.
    #[default]
    Circular,
    /// Linear receptors: X/Y and XX/XY/YX/YY Stokes codes.
    Linear,
}

/// Worker selection policy for native synthetic MS generation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPolicy {
    /// Choose worker counts from request bounds, environment, and available CPU parallelism.
    #[default]
    Auto,
    /// Use the explicit request worker counts, clamped only to the available work.
    Fixed,
}

/// Observation row topology for native synthetic MS generation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObservationMode {
    /// Cross-correlation interferometric rows for every antenna pair.
    #[default]
    Interferometric,
    /// Single-dish total-power style autocorrelation rows.
    TotalPower,
}

/// Sky model source used by the native simulator.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkyModel {
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
        components: Vec<AnalyticComponent>,
    },
}

impl SkyModel {
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
pub struct AnalyticComponentModel {
    /// Optional schema version for component-model files.
    #[serde(default)]
    pub schema_version: Option<u32>,
    /// Optional model name for provenance.
    #[serde(default)]
    pub name: Option<String>,
    /// Analytic components in direction-cosine coordinates relative to phase center.
    pub components: Vec<AnalyticComponent>,
}

/// One exact analytic sky component.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnalyticComponent {
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
        spectrum: AnalyticSpectrum,
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
        spectrum: AnalyticSpectrum,
    },
}

/// Per-component spectral model for analytic components.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AnalyticSpectrum {
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

impl AnalyticSpectrum {
    /// Evaluate the component flux density for one channel of `window`.
    ///
    /// The continuum is a single power law across every window of the
    /// observation, referenced to `reference_frequency_hz` or, when that is
    /// absent, to `observation_reference_frequency_hz` (the first window's
    /// reference frequency for a generated observation). Line and absorption
    /// profiles are placed by fraction of `window`'s own channel range.
    pub fn flux_for_channel(
        &self,
        window: &SpectralSetup,
        channel: usize,
        observation_reference_frequency_hz: f64,
    ) -> f64 {
        let reference_frequency_hz = self
            .reference_frequency_hz
            .unwrap_or(observation_reference_frequency_hz);
        let frequency_hz = window.start_frequency_hz + channel as f64 * window.channel_width_hz;
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
                window.channel_count,
                channel,
            )
            - gaussian_channel_profile(
                self.absorption_peak_jy,
                self.absorption_center_fraction,
                self.absorption_sigma_fraction,
                window.channel_count,
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
pub struct ObservationRequest {
    /// Existing model image path that defines the tutorial model provenance.
    pub model_image: PathBuf,
    /// Sky model; when absent, read the FITS image in `model_image`.
    #[serde(default)]
    pub model: Option<SkyModel>,
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
    pub fields: Vec<Field>,
    /// Observation start time in MJD seconds UTC.
    pub start_time_mjd_seconds: f64,
    /// Requested on-source duration in seconds.
    pub duration_seconds: f64,
    /// Integration time in seconds.
    pub integration_seconds: f64,
    /// Minimum antenna elevation in radians for native observing flags.
    #[serde(default = "default_simobserve_elevation_limit_rad")]
    pub elevation_limit_rad: f64,
    /// Permit continuous tracks that include samples below the elevation limit.
    ///
    /// When false, the native simulator flags below-limit samples. Requested
    /// observing times are retained; this developer helper does not reschedule.
    #[serde(default)]
    pub allow_below_elevation_limit: bool,
    /// Antenna configuration.
    pub antennas: Vec<Antenna>,
    /// Spectral windows, observed simultaneously.
    ///
    /// Window `i` writes `SPECTRAL_WINDOW` row `i` and `DATA_DESCRIPTION` row
    /// `i`, and every time sample writes one MAIN row per baseline per window
    /// with `DATA_DESC_ID = i`. A FITS model's spectral planes and bandpass
    /// corruption gains index the windows' channels concatenated in order.
    pub spectral_windows: Vec<SpectralSetup>,
    /// Polarization/correlation setup.
    #[serde(default)]
    pub polarization_setup: PolarizationSetup,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    pub predict_model: bool,
    /// Optional CASA voltage-pattern table; absent uses the native telescope default.
    #[serde(default)]
    pub vp_table: Option<PathBuf>,
    /// Optional deterministic corruptions applied to predicted visibility data.
    #[serde(default)]
    pub corruption: Option<CorruptionConfig>,
    /// Worker selection policy for native row/channel parallelism.
    #[serde(default)]
    pub worker_policy: WorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: ObservationMode,
    /// Bound on the native simulator's shared worker pool.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Another bound on the same shared pool; the smaller supplied bound wins.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

impl ObservationRequest {
    /// Build the tutorial VLA protoplanetary-disk foundation request.
    pub fn vla_ppdisk(
        model_image: impl Into<PathBuf>,
        output_ms: impl Into<PathBuf>,
        antennas: Vec<Antenna>,
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
            spectral_windows: vec![SpectralSetup {
                name: "Qband".to_string(),
                start_frequency_hz: 44.0e9,
                channel_width_hz: 128.0e6,
                channel_count: 1,
            }],
            polarization_setup: PolarizationSetup::default(),
            predict_model: true,
            vp_table: None,
            corruption: None,
            worker_policy: WorkerPolicy::Auto,
            observation_mode: ObservationMode::Interferometric,
            row_workers: None,
            channel_workers: None,
        }
    }
}

/// Deterministic corruption controls for tutorial-grade synthetic observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CorruptionConfig {
    /// Seed used for deterministic random draws.
    pub seed: u64,
    /// Additive visibility noise, parameterized like CASA `simulator.setnoise`.
    #[serde(default)]
    pub noise: Option<NoiseCorruption>,
    /// Per-antenna complex gain corruption, parameterized like CASA `simulator.setgain`.
    #[serde(default)]
    pub gain: Option<GainCorruption>,
    /// Polarization leakage, parameterized like CASA `setleakage`: CASA's
    /// linearised leakage of the cross hands with four correlations, applied
    /// before the gains; with two, an approximation on the parallel hands,
    /// applied after the gains and the noise. One correlation has no other
    /// hand to leak into, so a leakage that changes the data is refused.
    #[serde(default)]
    pub leakage: Option<PolarizationLeakageCorruption>,
}

/// Additive visibility-noise controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoiseCorruption {
    /// CASA `setnoise` mode. Only `simplenoise` is currently implemented.
    pub mode: NoiseMode,
    /// Per-real/imaginary-component Gaussian sigma in Jy for `mode='simplenoise'`.
    pub simplenoise_jy: f32,
}

/// Supported CASA `setnoise` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum NoiseMode {
    /// CASA `mode='simplenoise'`.
    #[serde(rename = "simplenoise")]
    SimpleNoise,
    /// CASA `mode='tsys-atm'`; requires the configured [`crate::Simulator`] API.
    #[serde(rename = "tsys-atm")]
    TsysAtm,
    /// CASA `mode='tsys-manual'`; requires the configured [`crate::Simulator`] API.
    #[serde(rename = "tsys-manual")]
    TsysManual,
}

/// Per-antenna gain corruption controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GainCorruption {
    /// CASA `setgain` mode.
    pub mode: GainMode,
    /// CASA `amplitude` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub amplitude: [f32; 2],
}

/// Supported CASA `setgain` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainMode {
    /// Fractional Brownian motion gain drift.
    #[serde(rename = "fbm")]
    Fbm,
    /// CASA's random complex gain mode.
    #[serde(rename = "random")]
    Random,
}

/// Polarization leakage corruption controls: with four correlations CASA's
/// linearised leakage of the cross hands, with two an approximation on the
/// parallel hands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolarizationLeakageCorruption {
    /// CASA `setleakage` mode.
    pub mode: PolarizationLeakageMode,
    /// CASA `amplitude` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub amplitude: [f32; 2],
    /// CASA `offset` vector `[real, imag]`, or scalar expanded by the CLI to both entries.
    pub offset: [f32; 2],
}

/// Supported CASA `setleakage` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum PolarizationLeakageMode {
    /// CASA `mode='constant'`.
    #[serde(rename = "constant")]
    Constant,
}
