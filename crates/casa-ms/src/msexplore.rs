// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic MeasurementSet plotting specifications and export helpers.
//!
//! `msexplore` is the next plotting layer above the curated MeasurementSet
//! metadata presets:
//!
//! - metadata summaries keep their fixed, compatibility-oriented preset catalog.
//! - `msexplore` exposes reusable plot specifications with explicit axes,
//!   selections, averaging, transforms, and export settings.
//! - Common curated raw-visibility plots are lowered into the generic
//!   `msexplore` scatter builder so new feature work lands in one place.
//!
//! The first delivery focuses on the most common MeasurementSet `plotms`
//! views backed by complex visibility samples:
//!
//! - amplitude/phase vs time
//! - amplitude/phase vs UV distance
//! - amplitude/phase vs channel/frequency
//! - real vs imaginary
//! - weight/sigma/flag vs time
//! - elevation/azimuth/hour angle/parallactic angle diagnostics
//!
//! Additional transform, multi-plot page composition, and staged flag-edit
//! fields are already modeled in the public specification types so future
//! waves can extend the implementation without redesigning the interface.

pub mod cli;
pub mod task_contract;

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casa_tables::Table;
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{MvAngle, MvTime};
use casa_types::{ArrayValue, Complex64, ScalarValue, Value};
use ndarray::{Ix1, Ix2};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::columns::{
    frequency_columns::ChanFreqColumn,
    weight_columns::{SigmaSpectrumColumn, WeightSpectrumColumn},
};
use crate::derived::engine::MsCalEngine;
use crate::listobs::{self, ListObsOptions, ListObsSummary, ListObsUvCoverage};
use crate::plot::{
    ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec, build_listobs_plot_payload_from_summary,
    build_listobs_uv_plot_payload,
};
use crate::selection::parser::parse_correlation_selector;
use crate::selection::resolve_row_indices_with_system_budget;
use crate::spectral_selection::{
    convert_frequency_to_frame, parse_rest_frequency_hz, velocity_ms_from_frequency_hz,
};
use crate::subtables::SubTable;
use crate::subtables::data_description::MsDataDescription;
use crate::subtables::polarization::MsPolarization;
use crate::{MeasurementSet, MeasurementSetSummaryOutputFormat, MsSelection};

const AVG_TIME_BUCKET_EPSILON_SECONDS: f64 = 0.002;

/// The `tracing` target of the visibility-scatter timing (`debug`).
const MSEXPLORE_TIMING: &str = "casa_ms::msexplore_timing";

#[derive(Default)]
struct VisibilityScatterTiming {
    row_grid: Duration,
    row_arrays: Duration,
    grouping: Duration,
    bin_collect: Duration,
    axis_compute: Duration,
    point_push: Duration,
}

impl VisibilityScatterTiming {
    /// Whether the logging configuration takes the timing events (`debug`,
    /// target [`MSEXPLORE_TIMING`]).
    fn enabled() -> bool {
        tracing::enabled!(target: MSEXPLORE_TIMING, tracing::Level::DEBUG)
    }

    fn checkpoint(label: &str, started: &mut Instant) {
        if Self::enabled() {
            let elapsed = started.elapsed();
            tracing::debug!(
                target: MSEXPLORE_TIMING,
                "msexplore timing: {label}={} ms",
                elapsed.as_millis()
            );
        }
        *started = Instant::now();
    }

    fn report(&self, label: &str, rows: usize, points: usize, started: Instant) {
        if !Self::enabled() {
            return;
        }
        tracing::debug!(
            target: MSEXPLORE_TIMING,
            "msexplore timing: {label} total={} ms rows={} points={} row_grid={} ms row_arrays={} ms grouping={} ms bin_collect={} ms axis_compute={} ms point_push={} ms",
            started.elapsed().as_millis(),
            rows,
            points,
            self.row_grid.as_millis(),
            self.row_arrays.as_millis(),
            self.grouping.as_millis(),
            self.bin_collect.as_millis(),
            self.axis_compute.as_millis(),
            self.point_push.as_millis()
        );
    }
}

/// Stable preset identifiers for common MeasurementSet plots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsPlotPreset {
    /// Plotms-style U against V coverage from selected visibility samples.
    UvCoverage,
    /// ANTENNA subtable layout plot.
    AntennaLayout,
    /// MAIN-table scan timeline.
    ScanTimeline,
    /// SPECTRAL_WINDOW frequency coverage.
    SpectralWindowCoverage,
    /// Vector-averaged amplitude against time.
    AmplitudeVsTime,
    /// Vector-averaged phase against time.
    PhaseVsTime,
    /// Stacked amplitude and phase against time on one two-row page.
    AmplitudePhaseVsTimeStacked,
    /// Vector-averaged amplitude against UV distance.
    AmplitudeVsUvDistance,
    /// Per-correlation WEIGHT against time.
    WeightVsTime,
    /// Per-correlation SIGMA against time.
    SigmaVsTime,
    /// Per-sample FLAG against time.
    FlagVsTime,
    /// Per-channel WEIGHT_SPECTRUM against time.
    WeightSpectrumVsTime,
    /// Per-channel SIGMA_SPECTRUM against time.
    SigmaSpectrumVsTime,
    /// Per-row FLAG_ROW against time.
    FlagRowVsTime,
    /// Elevation against time.
    ElevationVsTime,
    /// Azimuth against time.
    AzimuthVsTime,
    /// Hour angle against time.
    HourAngleVsTime,
    /// Parallactic angle against time.
    ParallacticAngleVsTime,
    /// Azimuth against elevation.
    AzimuthVsElevation,
    /// Amplitude against channel index.
    AmplitudeVsChannel,
    /// Phase against channel index.
    PhaseVsChannel,
    /// Amplitude against channel center frequency.
    AmplitudeVsFrequency,
    /// Phase against channel center frequency.
    PhaseVsFrequency,
    /// Amplitude against Doppler velocity.
    AmplitudeVsVelocity,
    /// Phase against Doppler velocity.
    PhaseVsVelocity,
    /// Real against imaginary.
    RealVsImaginary,
}

impl MsPlotPreset {
    /// All shipped presets in stable order.
    pub const ALL: [Self; 26] = [
        Self::UvCoverage,
        Self::AntennaLayout,
        Self::ScanTimeline,
        Self::SpectralWindowCoverage,
        Self::AmplitudeVsTime,
        Self::PhaseVsTime,
        Self::AmplitudePhaseVsTimeStacked,
        Self::AmplitudeVsUvDistance,
        Self::WeightVsTime,
        Self::SigmaVsTime,
        Self::FlagVsTime,
        Self::WeightSpectrumVsTime,
        Self::SigmaSpectrumVsTime,
        Self::FlagRowVsTime,
        Self::ElevationVsTime,
        Self::AzimuthVsTime,
        Self::HourAngleVsTime,
        Self::ParallacticAngleVsTime,
        Self::AzimuthVsElevation,
        Self::AmplitudeVsChannel,
        Self::PhaseVsChannel,
        Self::AmplitudeVsFrequency,
        Self::PhaseVsFrequency,
        Self::AmplitudeVsVelocity,
        Self::PhaseVsVelocity,
        Self::RealVsImaginary,
    ];

    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UvCoverage => "uv_coverage",
            Self::AntennaLayout => "antenna_layout",
            Self::ScanTimeline => "scan_timeline",
            Self::SpectralWindowCoverage => "spectral_window_coverage",
            Self::AmplitudeVsTime => "amplitude_vs_time",
            Self::PhaseVsTime => "phase_vs_time",
            Self::AmplitudePhaseVsTimeStacked => "amplitude_phase_vs_time_stacked",
            Self::AmplitudeVsUvDistance => "amplitude_vs_uv_distance",
            Self::WeightVsTime => "weight_vs_time",
            Self::SigmaVsTime => "sigma_vs_time",
            Self::FlagVsTime => "flag_vs_time",
            Self::WeightSpectrumVsTime => "weight_spectrum_vs_time",
            Self::SigmaSpectrumVsTime => "sigma_spectrum_vs_time",
            Self::FlagRowVsTime => "flagrow_vs_time",
            Self::ElevationVsTime => "elevation_vs_time",
            Self::AzimuthVsTime => "azimuth_vs_time",
            Self::HourAngleVsTime => "hour_angle_vs_time",
            Self::ParallacticAngleVsTime => "parallactic_angle_vs_time",
            Self::AzimuthVsElevation => "azimuth_vs_elevation",
            Self::AmplitudeVsChannel => "amplitude_vs_channel",
            Self::PhaseVsChannel => "phase_vs_channel",
            Self::AmplitudeVsFrequency => "amplitude_vs_frequency",
            Self::PhaseVsFrequency => "phase_vs_frequency",
            Self::AmplitudeVsVelocity => "amplitude_vs_velocity",
            Self::PhaseVsVelocity => "phase_vs_velocity",
            Self::RealVsImaginary => "real_vs_imaginary",
        }
    }

    /// Human-readable label.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::UvCoverage => "UV Coverage",
            Self::AntennaLayout => "Antenna Layout",
            Self::ScanTimeline => "Scan Timeline",
            Self::SpectralWindowCoverage => "Spectral Window Coverage",
            Self::AmplitudeVsTime => "Amplitude vs Time",
            Self::PhaseVsTime => "Phase vs Time",
            Self::AmplitudePhaseVsTimeStacked => "Amplitude / Phase vs Time (Stacked)",
            Self::AmplitudeVsUvDistance => "Amplitude vs UV Distance",
            Self::WeightVsTime => "Weight vs Time",
            Self::SigmaVsTime => "Sigma vs Time",
            Self::FlagVsTime => "Flag vs Time",
            Self::WeightSpectrumVsTime => "Weight Spectrum vs Time",
            Self::SigmaSpectrumVsTime => "Sigma Spectrum vs Time",
            Self::FlagRowVsTime => "Flag Row vs Time",
            Self::ElevationVsTime => "Elevation vs Time",
            Self::AzimuthVsTime => "Azimuth vs Time",
            Self::HourAngleVsTime => "Hour Angle vs Time",
            Self::ParallacticAngleVsTime => "Parallactic Angle vs Time",
            Self::AzimuthVsElevation => "Azimuth vs Elevation",
            Self::AmplitudeVsChannel => "Amplitude vs Channel",
            Self::PhaseVsChannel => "Phase vs Channel",
            Self::AmplitudeVsFrequency => "Amplitude vs Frequency",
            Self::PhaseVsFrequency => "Phase vs Frequency",
            Self::AmplitudeVsVelocity => "Amplitude vs Velocity",
            Self::PhaseVsVelocity => "Phase vs Velocity",
            Self::RealVsImaginary => "Real vs Imaginary",
        }
    }

    /// Parse a stable preset identifier.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "uv_coverage" | "uv" => Ok(Self::UvCoverage),
            "antenna_layout" | "antennas" => Ok(Self::AntennaLayout),
            "scan_timeline" | "scans" => Ok(Self::ScanTimeline),
            "spectral_window_coverage" | "spw_coverage" | "spws" => {
                Ok(Self::SpectralWindowCoverage)
            }
            "amplitude_vs_time" | "amp_time" => Ok(Self::AmplitudeVsTime),
            "phase_vs_time" | "phase_time" => Ok(Self::PhaseVsTime),
            "amplitude_phase_vs_time_stacked" | "amp_phase_time_stacked" => {
                Ok(Self::AmplitudePhaseVsTimeStacked)
            }
            "amplitude_vs_uv_distance" | "amplitude_vs_uvdist" | "amp_uvdist" => {
                Ok(Self::AmplitudeVsUvDistance)
            }
            "weight_vs_time" | "wt_time" => Ok(Self::WeightVsTime),
            "sigma_vs_time" | "sigma_time" => Ok(Self::SigmaVsTime),
            "flag_vs_time" | "flag_time" => Ok(Self::FlagVsTime),
            "weight_spectrum_vs_time" | "wtsp_time" => Ok(Self::WeightSpectrumVsTime),
            "sigma_spectrum_vs_time" | "sigmasp_time" => Ok(Self::SigmaSpectrumVsTime),
            "flagrow_vs_time" | "flagrow_time" => Ok(Self::FlagRowVsTime),
            "elevation_vs_time" | "elevation_time" => Ok(Self::ElevationVsTime),
            "azimuth_vs_time" | "azimuth_time" => Ok(Self::AzimuthVsTime),
            "hour_angle_vs_time" | "hourang_vs_time" | "hourang_time" => Ok(Self::HourAngleVsTime),
            "parallactic_angle_vs_time" | "parang_vs_time" | "parang_time" => {
                Ok(Self::ParallacticAngleVsTime)
            }
            "azimuth_vs_elevation" | "azimuth_elevation" => Ok(Self::AzimuthVsElevation),
            "amplitude_vs_channel" | "amp_channel" => Ok(Self::AmplitudeVsChannel),
            "phase_vs_channel" | "phase_channel" => Ok(Self::PhaseVsChannel),
            "amplitude_vs_frequency" | "amp_frequency" => Ok(Self::AmplitudeVsFrequency),
            "phase_vs_frequency" | "phase_frequency" => Ok(Self::PhaseVsFrequency),
            "amplitude_vs_velocity" | "amp_velocity" => Ok(Self::AmplitudeVsVelocity),
            "phase_vs_velocity" | "phase_velocity" => Ok(Self::PhaseVsVelocity),
            "real_vs_imaginary" | "real_vs_imag" => Ok(Self::RealVsImaginary),
            other => Err(format!("unsupported msexplore preset {other:?}")),
        }
    }

    /// Convert one legacy `listobs` plot kind into the matching preset.
    pub fn from_listobs_kind(kind: ListObsPlotKind) -> Self {
        match kind {
            ListObsPlotKind::UvCoverage => Self::UvCoverage,
            ListObsPlotKind::AntennaLayout => Self::AntennaLayout,
            ListObsPlotKind::ScanTimeline => Self::ScanTimeline,
            ListObsPlotKind::SpectralWindowCoverage => Self::SpectralWindowCoverage,
            ListObsPlotKind::AmplitudeVsTime => Self::AmplitudeVsTime,
            ListObsPlotKind::PhaseVsTime => Self::PhaseVsTime,
            ListObsPlotKind::AmplitudeVsUvDistance => Self::AmplitudeVsUvDistance,
        }
    }

    fn lowers_to_listobs_metadata(self) -> Option<ListObsPlotKind> {
        match self {
            Self::AntennaLayout => Some(ListObsPlotKind::AntennaLayout),
            Self::ScanTimeline => Some(ListObsPlotKind::ScanTimeline),
            Self::SpectralWindowCoverage => Some(ListObsPlotKind::SpectralWindowCoverage),
            _ => None,
        }
    }
}

impl fmt::Display for MsPlotPreset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported plot axes for MeasurementSet data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsAxis {
    /// Visibility amplitude.
    Amplitude,
    /// Visibility phase in degrees.
    Phase,
    /// Visibility real part.
    Real,
    /// Visibility imaginary part.
    Imaginary,
    /// MAIN.TIME in MJD seconds.
    Time,
    /// Zero-based channel index.
    Channel,
    /// Channel center frequency in Hz.
    Frequency,
    /// Velocity derived from spectral metadata.
    Velocity,
    /// UV distance `sqrt(u^2 + v^2)` in meters.
    UvDistance,
    /// U coordinate in meters.
    U,
    /// V coordinate in meters.
    V,
    /// W coordinate in meters.
    W,
    /// Scan number metadata axis.
    Scan,
    /// Field identifier metadata axis.
    Field,
    /// Spectral-window identifier metadata axis.
    SpectralWindow,
    /// Correlation slot axis.
    Correlation,
    /// Baseline metadata axis.
    Baseline,
    /// Antenna identifier metadata axis.
    Antenna,
    /// Azimuth geometry axis.
    Azimuth,
    /// Elevation geometry axis.
    Elevation,
    /// Hour-angle geometry axis.
    HourAngle,
    /// Parallactic-angle geometry axis.
    ParallacticAngle,
    /// Per-sample weight axis.
    Weight,
    /// Per-sample sigma axis.
    Sigma,
    /// Per-channel WEIGHT_SPECTRUM values, with WEIGHT fallback.
    WeightSpectrum,
    /// Per-channel SIGMA_SPECTRUM values, with SIGMA fallback.
    SigmaSpectrum,
    /// Per-sample flag axis.
    Flag,
    /// Per-row FLAG_ROW values repeated across plotted samples.
    FlagRow,
    /// Pointing right ascension for an antenna direction.
    #[serde(rename = "ant-ra")]
    AntRa,
    /// Pointing declination for an antenna direction.
    #[serde(rename = "ant-dec")]
    AntDec,
}

impl MsAxis {
    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Amplitude => "amplitude",
            Self::Phase => "phase",
            Self::Real => "real",
            Self::Imaginary => "imaginary",
            Self::Time => "time",
            Self::Channel => "channel",
            Self::Frequency => "frequency",
            Self::Velocity => "velocity",
            Self::UvDistance => "uv_distance",
            Self::U => "u",
            Self::V => "v",
            Self::W => "w",
            Self::Scan => "scan",
            Self::Field => "field",
            Self::SpectralWindow => "spectral_window",
            Self::Correlation => "correlation",
            Self::Baseline => "baseline",
            Self::Antenna => "antenna",
            Self::Azimuth => "azimuth",
            Self::Elevation => "elevation",
            Self::HourAngle => "hour_angle",
            Self::ParallacticAngle => "parallactic_angle",
            Self::Weight => "weight",
            Self::Sigma => "sigma",
            Self::WeightSpectrum => "weight_spectrum",
            Self::SigmaSpectrum => "sigma_spectrum",
            Self::Flag => "flag",
            Self::FlagRow => "flag_row",
            Self::AntRa => "ant-ra",
            Self::AntDec => "ant-dec",
        }
    }

    /// Human-readable axis label.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Amplitude => "Amplitude",
            Self::Phase => "Phase",
            Self::Real => "Real",
            Self::Imaginary => "Imaginary",
            Self::Time => "Time",
            Self::Channel => "Channel",
            Self::Frequency => "Frequency",
            Self::Velocity => "Velocity",
            Self::UvDistance => "UV Distance",
            Self::U => "U",
            Self::V => "V",
            Self::W => "W",
            Self::Scan => "Scan",
            Self::Field => "Field",
            Self::SpectralWindow => "Spectral Window",
            Self::Correlation => "Correlation",
            Self::Baseline => "Baseline",
            Self::Antenna => "Antenna",
            Self::Azimuth => "Azimuth",
            Self::Elevation => "Elevation",
            Self::HourAngle => "Hour Angle",
            Self::ParallacticAngle => "Parallactic Angle",
            Self::Weight => "Weight",
            Self::Sigma => "Sigma",
            Self::WeightSpectrum => "Weight Spectrum",
            Self::SigmaSpectrum => "Sigma Spectrum",
            Self::Flag => "Flag",
            Self::FlagRow => "Flag Row",
            Self::AntRa => "Antenna RA",
            Self::AntDec => "Antenna Dec",
        }
    }

    /// Parse one axis token accepted by the CLI.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "amp" | "amplitude" => Ok(Self::Amplitude),
            "phase" => Ok(Self::Phase),
            "real" => Ok(Self::Real),
            "imag" | "imaginary" => Ok(Self::Imaginary),
            "time" => Ok(Self::Time),
            "chan" | "channel" => Ok(Self::Channel),
            "freq" | "frequency" => Ok(Self::Frequency),
            "velocity" | "vel" => Ok(Self::Velocity),
            "uvdist" | "uv_distance" | "uv-distance" => Ok(Self::UvDistance),
            "u" => Ok(Self::U),
            "v" => Ok(Self::V),
            "w" => Ok(Self::W),
            "scan" => Ok(Self::Scan),
            "field" => Ok(Self::Field),
            "spw" | "spectral_window" | "spectral-window" => Ok(Self::SpectralWindow),
            "corr" | "correlation" => Ok(Self::Correlation),
            "baseline" => Ok(Self::Baseline),
            "antenna" => Ok(Self::Antenna),
            "azimuth" => Ok(Self::Azimuth),
            "elevation" => Ok(Self::Elevation),
            "hourang" | "hourangle" | "hour_angle" | "hour-angle" => Ok(Self::HourAngle),
            "parang" | "parallactic_angle" | "parallactic-angle" => Ok(Self::ParallacticAngle),
            "weight" => Ok(Self::Weight),
            "sigma" => Ok(Self::Sigma),
            "wtsp" | "weightspectrum" | "weight_spectrum" | "weight-spectrum" => {
                Ok(Self::WeightSpectrum)
            }
            "sigmasp" | "sigmaspectrum" | "sigma_spectrum" | "sigma-spectrum" => {
                Ok(Self::SigmaSpectrum)
            }
            "flag" => Ok(Self::Flag),
            "flagrow" | "flag_row" | "flag-row" => Ok(Self::FlagRow),
            "ant-ra" | "ant_ra" => Ok(Self::AntRa),
            "ant-dec" | "ant_dec" => Ok(Self::AntDec),
            other => Err(format!("unsupported msexplore axis {other:?}")),
        }
    }

    fn is_visibility_math(self) -> bool {
        matches!(
            self,
            Self::Amplitude | Self::Phase | Self::Real | Self::Imaginary
        )
    }

    fn uses_derived_geometry(self) -> bool {
        matches!(
            self,
            Self::Azimuth | Self::Elevation | Self::HourAngle | Self::ParallacticAngle
        )
    }

    fn uses_channel_bins(self) -> bool {
        self.is_visibility_math()
            || matches!(
                self,
                Self::Channel
                    | Self::Frequency
                    | Self::Velocity
                    | Self::WeightSpectrum
                    | Self::SigmaSpectrum
                    | Self::Flag
            )
    }

    fn uses_flag_rows(self) -> bool {
        matches!(self, Self::Flag | Self::FlagRow)
    }

    fn uses_correlation_slots(self) -> bool {
        self.is_visibility_math()
            || matches!(
                self,
                Self::Weight
                    | Self::Sigma
                    | Self::WeightSpectrum
                    | Self::SigmaSpectrum
                    | Self::Flag
            )
    }

    fn uses_spectral_coordinates(self) -> bool {
        matches!(self, Self::Frequency | Self::Velocity)
    }
}

impl fmt::Display for MsAxis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Visibility data column or derived column expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsDataColumn {
    /// MAIN.DATA.
    Data,
    /// MAIN.CORRECTED_DATA.
    Corrected,
    /// MAIN.MODEL_DATA.
    Model,
    /// Complex residual `CORRECTED_DATA - MODEL_DATA`.
    CorrectedMinusModel,
    /// Scalar residual `|CORRECTED_DATA| - |MODEL_DATA|`.
    CorrectedMinusModelScalar,
    /// Complex residual `DATA - MODEL_DATA`.
    DataMinusModel,
    /// Scalar residual `|DATA| - |MODEL_DATA|`.
    DataMinusModelScalar,
    /// Complex ratio `CORRECTED_DATA / MODEL_DATA`.
    CorrectedDivModel,
    /// Scalar ratio `|CORRECTED_DATA| / |MODEL_DATA|`.
    CorrectedDivModelScalar,
    /// Complex ratio `DATA / MODEL_DATA`.
    DataDivModel,
    /// Scalar ratio `|DATA| / |MODEL_DATA|`.
    DataDivModelScalar,
}

impl MsDataColumn {
    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Corrected => "corrected",
            Self::Model => "model",
            Self::CorrectedMinusModel => "corrected_minus_model",
            Self::CorrectedMinusModelScalar => "corrected_minus_model_scalar",
            Self::DataMinusModel => "data_minus_model",
            Self::DataMinusModelScalar => "data_minus_model_scalar",
            Self::CorrectedDivModel => "corrected_div_model",
            Self::CorrectedDivModelScalar => "corrected_div_model_scalar",
            Self::DataDivModel => "data_div_model",
            Self::DataDivModelScalar => "data_div_model_scalar",
        }
    }

    /// Parse a CLI/data-model token.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "data" => Ok(Self::Data),
            "corrected" | "corrected_data" => Ok(Self::Corrected),
            "model" | "model_data" => Ok(Self::Model),
            "corrected-model" | "corrected_minus_model" => Ok(Self::CorrectedMinusModel),
            "corrected-model_scalar" | "corrected_minus_model_scalar" => {
                Ok(Self::CorrectedMinusModelScalar)
            }
            "data-model" | "data_minus_model" => Ok(Self::DataMinusModel),
            "data-model_scalar" | "data_minus_model_scalar" => Ok(Self::DataMinusModelScalar),
            "corrected/model" | "corrected_div_model" => Ok(Self::CorrectedDivModel),
            "corrected/model_scalar" | "corrected_div_model_scalar" => {
                Ok(Self::CorrectedDivModelScalar)
            }
            "data/model" | "data_div_model" => Ok(Self::DataDivModel),
            "data/model_scalar" | "data_div_model_scalar" => Ok(Self::DataDivModelScalar),
            other => Err(format!("unsupported msexplore data column {other:?}")),
        }
    }
}

impl fmt::Display for MsDataColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Metadata axis used to group colors or series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsColorAxis {
    /// Disable metadata grouping.
    None,
    /// Group by FIELD_ID/name.
    Field,
    /// Group by SCAN_NUMBER.
    Scan,
    /// Group by spectral window.
    SpectralWindow,
    /// Group by antenna pair.
    Baseline,
    /// Group by polarization correlation.
    Correlation,
}

impl MsColorAxis {
    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Field => "field",
            Self::Scan => "scan",
            Self::SpectralWindow => "spw",
            Self::Baseline => "baseline",
            Self::Correlation => "correlation",
        }
    }

    /// Parse one grouping token.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "none" => Ok(Self::None),
            "field" => Ok(Self::Field),
            "scan" => Ok(Self::Scan),
            "spw" | "spectral_window" | "spectral-window" => Ok(Self::SpectralWindow),
            "baseline" => Ok(Self::Baseline),
            "correlation" | "corr" => Ok(Self::Correlation),
            other => Err(format!("unsupported color axis {other:?}")),
        }
    }
}

impl fmt::Display for MsColorAxis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Plot export format for `msexplore`.
///
/// The only format is the text point manifest. PNG and PDF rasterisation was
/// removed (#833); Python renders typed plot documents with Matplotlib.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsExportFormat {
    /// Text point manifest for parity tests and automation.
    Txt,
}

impl MsExportFormat {
    /// Parse an export-format token.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "txt" => Ok(Self::Txt),
            "png" | "pdf" => Err(format!(
                "msexplore {value} plot export was removed (#833); use --plot-format txt for the \
                 text point manifest, or render the typed plot document with Matplotlib from \
                 Python (casars.data)"
            )),
            other => Err(format!(
                "unsupported msexplore export format {other:?}; expected txt"
            )),
        }
    }

    /// Conventional lowercase filename extension.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Txt => "txt",
        }
    }
}

/// Averaging controls modeled after CASA `plotms`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct MsAverageSpec {
    /// Channel bin size used for channel/frequency plots.
    pub avgchannel: Option<usize>,
    /// Time averaging window in seconds.
    pub avgtime: Option<f64>,
    /// Permit averaging across scan boundaries.
    pub avgscan: bool,
    /// Permit averaging across field boundaries.
    pub avgfield: bool,
    /// Permit averaging across baselines.
    pub avgbaseline: bool,
    /// Permit averaging across antennas.
    pub avgantenna: bool,
    /// Permit averaging across spectral windows.
    pub avgspw: bool,
    /// Use scalar averaging instead of vector averaging.
    pub scalar: bool,
}

/// Transform controls modeled after CASA `plotms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsTransformSpec {
    /// Enable transform controls.
    pub transform: bool,
    /// Frequency frame for spectral transforms.
    pub freqframe: Option<String>,
    /// Rest frequency for velocity transforms.
    pub restfreq: Option<String>,
    /// Velocity definition.
    pub veldef: String,
    /// Phase-center shift.
    pub phasecenter: Option<String>,
    /// X-axis frame for pointing axes.
    pub xframe: Option<String>,
    /// X-axis interpolation for pointing axes.
    pub xinterp: Option<String>,
    /// Y-axis frame for pointing axes.
    pub yframe: Option<String>,
    /// Y-axis interpolation for pointing axes.
    pub yinterp: Option<String>,
}

impl Default for MsTransformSpec {
    fn default() -> Self {
        Self {
            transform: true,
            freqframe: None,
            restfreq: None,
            veldef: "RADIO".to_string(),
            phasecenter: None,
            xframe: None,
            xinterp: None,
            yframe: None,
            yinterp: None,
        }
    }
}

/// Page-layout controls for one plot page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsLayoutSpec {
    /// Number of grid rows.
    pub gridrows: usize,
    /// Number of grid columns.
    pub gridcols: usize,
    /// Zero-based row index for this plot.
    pub rowindex: usize,
    /// Zero-based column index for this plot.
    pub colindex: usize,
    /// Zero-based plot index on the page.
    pub plotindex: usize,
}

impl Default for MsLayoutSpec {
    fn default() -> Self {
        Self {
            gridrows: 1,
            gridcols: 1,
            rowindex: 0,
            colindex: 0,
            plotindex: 0,
        }
    }
}

/// Page export range behavior for multi-plot page composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum MsPageExportRange {
    /// Resolve bounds independently for each occupied page cell.
    #[default]
    Current,
    /// Reuse one page-wide range for all occupied cells.
    All,
}

impl MsPageExportRange {
    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::All => "all",
        }
    }
}

impl fmt::Display for MsPageExportRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Legend placement options accepted by `msexplore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub enum MsLegendPosition {
    /// Place the legend inside the plot at upper right.
    #[default]
    #[serde(rename = "upperRight")]
    UpperRight,
    /// Place the legend inside the plot at upper left.
    #[serde(rename = "upperLeft")]
    UpperLeft,
    /// Place the legend inside the plot at lower right.
    #[serde(rename = "lowerRight")]
    LowerRight,
    /// Place the legend inside the plot at lower left.
    #[serde(rename = "lowerLeft")]
    LowerLeft,
    /// Place the legend outside the plot on the right.
    #[serde(rename = "exteriorRight")]
    ExteriorRight,
    /// Place the legend outside the plot on the left.
    #[serde(rename = "exteriorLeft")]
    ExteriorLeft,
    /// Place the legend outside the plot above the chart.
    #[serde(rename = "exteriorTop")]
    ExteriorTop,
    /// Place the legend outside the plot below the chart.
    #[serde(rename = "exteriorBottom")]
    ExteriorBottom,
}

impl MsLegendPosition {
    /// Stable string form matching CASA `plotms`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UpperRight => "upperRight",
            Self::UpperLeft => "upperLeft",
            Self::LowerRight => "lowerRight",
            Self::LowerLeft => "lowerLeft",
            Self::ExteriorRight => "exteriorRight",
            Self::ExteriorLeft => "exteriorLeft",
            Self::ExteriorTop => "exteriorTop",
            Self::ExteriorBottom => "exteriorBottom",
        }
    }
}

impl fmt::Display for MsLegendPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Page header items accepted by `msexplore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum MsPageHeaderItem {
    /// MeasurementSet filename.
    #[serde(rename = "filename")]
    Filename,
    /// Y-axis column label.
    #[serde(rename = "ycolumn")]
    YColumn,
    /// Observation start date.
    #[serde(rename = "obsdate")]
    ObsDate,
    /// Observation start time.
    #[serde(rename = "obstime")]
    ObsTime,
    /// Observer name.
    #[serde(rename = "observer")]
    Observer,
    /// Project id/name.
    #[serde(rename = "projid")]
    ProjId,
    /// Telescope name.
    #[serde(rename = "telescope")]
    Telescope,
    /// Target or field name.
    #[serde(rename = "targname")]
    TargName,
    /// Target direction.
    #[serde(rename = "targdir")]
    TargDir,
}

impl MsPageHeaderItem {
    /// Stable string form matching CASA `plotms`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filename => "filename",
            Self::YColumn => "ycolumn",
            Self::ObsDate => "obsdate",
            Self::ObsTime => "obstime",
            Self::Observer => "observer",
            Self::ProjId => "projid",
            Self::Telescope => "telescope",
            Self::TargName => "targname",
            Self::TargDir => "targdir",
        }
    }
}

impl fmt::Display for MsPageHeaderItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported iteration axes for multi-panel plot pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsIterationAxis {
    /// Iterate one panel per FIELD row.
    Field,
    /// Iterate one panel per SCAN_NUMBER.
    Scan,
    /// Iterate one panel per SPECTRAL_WINDOW.
    SpectralWindow,
    /// Iterate one panel per selected correlation product.
    Correlation,
}

impl MsIterationAxis {
    /// All currently implemented iteration axes.
    pub const ALL: [Self; 4] = [
        Self::Field,
        Self::Scan,
        Self::SpectralWindow,
        Self::Correlation,
    ];

    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Field => "field",
            Self::Scan => "scan",
            Self::SpectralWindow => "spw",
            Self::Correlation => "correlation",
        }
    }

    /// Human-readable label.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Field => "Field",
            Self::Scan => "Scan",
            Self::SpectralWindow => "Spectral Window",
            Self::Correlation => "Correlation",
        }
    }

    /// Parse a CASA-style iteration-axis identifier.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "field" => Ok(Self::Field),
            "scan" => Ok(Self::Scan),
            "spw" | "spectral_window" | "spectralwindow" => Ok(Self::SpectralWindow),
            "correlation" | "corr" => Ok(Self::Correlation),
            other => Err(format!(
                "unsupported msexplore iteraxis {other:?}; expected one of {}",
                Self::ALL
                    .into_iter()
                    .map(Self::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

impl fmt::Display for MsIterationAxis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Iteration controls for multi-plot pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct MsIterationSpec {
    /// Iteration axis identifier.
    pub iteraxis: Option<MsIterationAxis>,
    /// Use self-scaled X ranges per iterated panel.
    pub xselfscale: bool,
    /// Use self-scaled Y ranges per iterated panel.
    pub yselfscale: bool,
    /// Share X axis across panels.
    pub xsharedaxis: bool,
    /// Share Y axis across panels.
    pub ysharedaxis: bool,
}

/// Presentation controls for a single plot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsPlotStyleSpec {
    /// Optional plot title override.
    pub title: Option<String>,
    /// Optional X label override.
    pub xlabel: Option<String>,
    /// Optional Y label override.
    pub ylabel: Option<String>,
    /// Optional scatter marker radius in rendered pixels.
    #[serde(default, alias = "symbolsize", alias = "marker_size")]
    pub symbol_size: Option<u32>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
    /// Legend placement.
    pub legendposition: MsLegendPosition,
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
}

impl Default for MsPlotStyleSpec {
    fn default() -> Self {
        Self {
            title: None,
            xlabel: None,
            ylabel: None,
            symbol_size: None,
            showlegend: false,
            legendposition: MsLegendPosition::UpperRight,
            showmajorgrid: false,
            showminorgrid: false,
        }
    }
}

/// One staged flag-edit request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagEditSpec {
    /// Whether the region should be flagged or unflagged.
    pub action: MsFlagAction,
    /// Inclusive numeric plot region that selects staged points.
    pub region: MsFlagRegion,
    /// Optional multi-plot page child plot index to target.
    pub plot_index: Option<usize>,
    /// Optional iterated-panel key to target within a scatter grid.
    pub panel_key: Option<String>,
    /// Extend across correlations.
    pub extcorr: bool,
    /// Extend across channels.
    pub extchannel: bool,
}

struct FlagEditPreviewContext<'a> {
    ms: &'a MeasurementSet,
    plot_title: &'a str,
    plot_index: Option<usize>,
    panel_key: Option<String>,
    panel_label: Option<String>,
    x_axis: MsAxis,
    y_axis: MsAxis,
}

/// Inclusive rectangular region used for staged flag editing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagRegion {
    /// Inclusive minimum X value.
    pub x_min: f64,
    /// Inclusive maximum X value.
    pub x_max: f64,
    /// Inclusive minimum Y value.
    pub y_min: f64,
    /// Inclusive maximum Y value.
    pub y_max: f64,
}

/// Supported flag-edit actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsFlagAction {
    /// Mark matching samples as flagged.
    Flag,
    /// Clear flags from matching samples.
    Unflag,
}

/// One plotted visibility sample selected by a staged flag edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagSampleEdit {
    /// MAIN row index.
    pub row: usize,
    /// Correlation slot within the FLAG cell.
    pub corr: usize,
    /// Channel slot within the FLAG cell.
    pub chan: usize,
    /// Flag value before applying the staged edit.
    pub old_flag: bool,
    /// Flag value after applying the staged edit.
    pub new_flag: bool,
}

/// One MAIN-row FLAG_ROW transition produced by a staged edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagRowEdit {
    /// MAIN row index.
    pub row: usize,
    /// FLAG_ROW value before applying the staged edit.
    pub old_flag_row: bool,
    /// FLAG_ROW value after applying the staged edit.
    pub new_flag_row: bool,
    /// Number of per-sample flag transitions on this row.
    pub changed_samples: usize,
}

/// Preview of a staged `msexplore` flag edit before optional writeback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagEditPreview {
    /// Plot title associated with the staged edit.
    pub plot_title: String,
    /// Optional child plot index targeted within a multi-plot page.
    pub plot_index: Option<usize>,
    /// Optional iterated-panel key targeted by this preview.
    pub panel_key: Option<String>,
    /// Optional human-readable panel label targeted by this preview.
    pub panel_label: Option<String>,
    /// X axis used for region matching.
    pub x_axis: MsAxis,
    /// Y axis used for region matching.
    pub y_axis: MsAxis,
    /// Inclusive numeric region used to select plotted samples.
    pub region: MsFlagRegion,
    /// Whether the edit flags or unflags the selected samples.
    pub action: MsFlagAction,
    /// Extend the edit across all correlations on matching rows/channels.
    pub extcorr: bool,
    /// Extend the edit across all channels on matching rows/correlations.
    pub extchannel: bool,
    /// Number of plotted points that intersect the staged region.
    pub matched_points: usize,
    /// Number of MAIN rows affected by the staged edit.
    pub affected_rows: usize,
    /// Number of unique `(row, corr, chan)` samples affected by the staged edit.
    pub affected_samples: usize,
    /// Exact per-sample flag transitions.
    pub sample_edits: Vec<MsFlagSampleEdit>,
    /// Exact per-row FLAG_ROW transitions.
    pub row_edits: Vec<MsFlagRowEdit>,
}

/// One plot request on a page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsPlotSpec {
    /// Optional stable preset identifier.
    pub preset: Option<MsPlotPreset>,
    /// X-axis selector.
    pub x_axis: MsAxis,
    /// Y-axis selectors. Up to two axes are currently supported; when two are
    /// present the second axis renders on the right side of a non-iterated plot.
    pub y_axes: Vec<MsAxis>,
    /// Visibility data column or derived expression.
    pub data_column: MsDataColumn,
    /// Metadata grouping/color axis.
    pub color_by: MsColorAxis,
    /// Averaging controls.
    pub averaging: MsAverageSpec,
    /// Transform controls.
    pub transforms: MsTransformSpec,
    /// Layout controls.
    pub layout: MsLayoutSpec,
    /// Iteration controls.
    pub iteration: MsIterationSpec,
    /// Presentation controls.
    pub style: MsPlotStyleSpec,
    /// Optional staged flag edit.
    pub flag_edit: Option<MsFlagEditSpec>,
}

impl MsPlotSpec {
    /// Build the default specification for one preset.
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub fn from_preset(preset: MsPlotPreset) -> Self {
        match preset {
            MsPlotPreset::UvCoverage => Self {
                preset: Some(preset),
                x_axis: MsAxis::U,
                y_axes: vec![MsAxis::V],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AntennaLayout
            | MsPlotPreset::ScanTimeline
            | MsPlotPreset::SpectralWindowCoverage => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudeVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::PhaseVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Phase],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudePhaseVsTimeStacked => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudeVsUvDistance => Self {
                preset: Some(preset),
                x_axis: MsAxis::UvDistance,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::WeightVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Weight],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::SigmaVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Sigma],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::FlagVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Flag],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::WeightSpectrumVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::WeightSpectrum],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::SigmaSpectrumVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::SigmaSpectrum],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::FlagRowVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::FlagRow],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::ElevationVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Elevation],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AzimuthVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::Azimuth],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::HourAngleVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::HourAngle],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::ParallacticAngleVsTime => Self {
                preset: Some(preset),
                x_axis: MsAxis::Time,
                y_axes: vec![MsAxis::ParallacticAngle],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AzimuthVsElevation => Self {
                preset: Some(preset),
                x_axis: MsAxis::Elevation,
                y_axes: vec![MsAxis::Azimuth],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudeVsChannel => Self {
                preset: Some(preset),
                x_axis: MsAxis::Channel,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::PhaseVsChannel => Self {
                preset: Some(preset),
                x_axis: MsAxis::Channel,
                y_axes: vec![MsAxis::Phase],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudeVsFrequency => Self {
                preset: Some(preset),
                x_axis: MsAxis::Frequency,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::PhaseVsFrequency => Self {
                preset: Some(preset),
                x_axis: MsAxis::Frequency,
                y_axes: vec![MsAxis::Phase],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::AmplitudeVsVelocity => Self {
                preset: Some(preset),
                x_axis: MsAxis::Velocity,
                y_axes: vec![MsAxis::Amplitude],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::PhaseVsVelocity => Self {
                preset: Some(preset),
                x_axis: MsAxis::Velocity,
                y_axes: vec![MsAxis::Phase],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::SpectralWindow,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
            MsPlotPreset::RealVsImaginary => Self {
                preset: Some(preset),
                x_axis: MsAxis::Real,
                y_axes: vec![MsAxis::Imaginary],
                data_column: MsDataColumn::Data,
                color_by: MsColorAxis::Field,
                averaging: MsAverageSpec::default(),
                transforms: MsTransformSpec::default(),
                layout: MsLayoutSpec::default(),
                iteration: MsIterationSpec::default(),
                style: MsPlotStyleSpec::default(),
                flag_edit: None,
            },
        }
    }

    /// Validate the current plot specification against the current engine.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_common()?;
        if self.layout.rowindex != 0 || self.layout.colindex != 0 || self.layout.plotindex != 0 {
            return Err(
                "msexplore per-plot rowindex/colindex/plotindex placement is reserved for multi-plot page composition".to_string(),
            );
        }
        let iterated = self.iteration.iteraxis.is_some();
        if !iterated && (self.layout.gridrows != 1 || self.layout.gridcols != 1) {
            return Err(
                "msexplore gridrows/gridcols require --iteraxis; non-iterated standalone plots still render a single panel".to_string(),
            );
        }
        Ok(())
    }

    fn validate_for_page_child(&self) -> Result<(), String> {
        self.validate_common()?;
        if self.flag_edit.is_some() {
            return Err(
                "msexplore staged flag editing currently supports standalone scatter plots only"
                    .to_string(),
            );
        }
        if self.iteration.iteraxis.is_some() {
            return Err(
                "msexplore multi-plot pages do not yet support nested iteraxis child plots"
                    .to_string(),
            );
        }
        if self.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) {
            return Err(
                "msexplore multi-plot pages do not yet support nested stacked page presets"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn validate_common(&self) -> Result<(), String> {
        if self.y_axes.is_empty() {
            return Err("msexplore requires at least one y axis per plot".to_string());
        }
        if self.y_axes.len() > 2 {
            return Err("msexplore currently supports at most two y axes per plot".to_string());
        }
        if self.y_axes.len() == 2 && self.y_axes[0] == self.y_axes[1] {
            return Err(
                "msexplore does not allow duplicate y axes with the same data column".to_string(),
            );
        }
        if self.layout.gridrows == 0 || self.layout.gridcols == 0 {
            return Err("msexplore gridrows/gridcols must be positive integers".to_string());
        }
        if self.style.symbol_size.is_some_and(|size| size == 0) {
            return Err("msexplore symbolsize must be a positive integer".to_string());
        }
        let iterated = self.iteration.iteraxis.is_some();
        if iterated && self.y_axes.len() > 1 {
            return Err(
                "msexplore multi-y plots are currently available for non-iterated plots only"
                    .to_string(),
            );
        }
        if self.iteration.xselfscale && self.iteration.xsharedaxis {
            return Err(
                "msexplore x-axis iteration scaling cannot request both self-scaled and shared axes".to_string(),
            );
        }
        if self.iteration.yselfscale && self.iteration.ysharedaxis {
            return Err(
                "msexplore y-axis iteration scaling cannot request both self-scaled and shared axes".to_string(),
            );
        }
        if let Some(flag_edit) = &self.flag_edit {
            self.validate_flag_edit(flag_edit)?;
        }
        if self.averaging.avgscan && self.averaging.avgtime.is_none() {
            return Err("msexplore avgscan requires avgtime to be set".to_string());
        }
        if self.averaging.avgfield && self.averaging.avgtime.is_none() {
            return Err("msexplore avgfield requires avgtime to be set".to_string());
        }
        if self.averaging.avgbaseline && self.averaging.avgantenna {
            return Err("msexplore avgbaseline and avgantenna are mutually exclusive".to_string());
        }
        if self.averaging.avgscan && self.iteration.iteraxis == Some(MsIterationAxis::Scan) {
            return Err(
                "msexplore cannot iterate by scan while averaging across scans".to_string(),
            );
        }
        if self.averaging.avgfield && self.iteration.iteraxis == Some(MsIterationAxis::Field) {
            return Err(
                "msexplore cannot iterate by field while averaging across fields".to_string(),
            );
        }
        if self.averaging.avgspw && self.iteration.iteraxis == Some(MsIterationAxis::SpectralWindow)
        {
            return Err(
                "msexplore cannot iterate by spectral window while averaging across spectral windows"
                    .to_string(),
            );
        }
        if self.averaging.avgscan && self.color_by == MsColorAxis::Scan {
            return Err("msexplore cannot color by scan while averaging across scans".to_string());
        }
        if self.averaging.avgfield && self.color_by == MsColorAxis::Field {
            return Err(
                "msexplore cannot color by field while averaging across fields".to_string(),
            );
        }
        if self.averaging.avgspw && self.color_by == MsColorAxis::SpectralWindow {
            return Err(
                "msexplore cannot color by spectral window while averaging across spectral windows"
                    .to_string(),
            );
        }
        if (self.averaging.avgbaseline || self.averaging.avgantenna)
            && self.color_by == MsColorAxis::Baseline
        {
            return Err(
                "msexplore cannot color by baseline while averaging across baselines or antennas"
                    .to_string(),
            );
        }
        if self.transforms.phasecenter.is_some()
            || self.transforms.xframe.is_some()
            || self.transforms.xinterp.is_some()
            || self.transforms.yframe.is_some()
            || self.transforms.yinterp.is_some()
        {
            return Err(
                "msexplore transform frame/interpolation controls are modeled but not yet implemented"
                    .to_string(),
            );
        }
        let supported_x = matches!(
            self.x_axis,
            MsAxis::Time
                | MsAxis::UvDistance
                | MsAxis::U
                | MsAxis::V
                | MsAxis::W
                | MsAxis::Channel
                | MsAxis::Frequency
                | MsAxis::Velocity
                | MsAxis::Amplitude
                | MsAxis::Phase
                | MsAxis::Real
                | MsAxis::Imaginary
                | MsAxis::Azimuth
                | MsAxis::Elevation
                | MsAxis::HourAngle
                | MsAxis::ParallacticAngle
        );
        if !supported_x {
            return Err(format!(
                "msexplore x axis {} is modeled but not implemented yet",
                self.x_axis
            ));
        }
        for y_axis in self.y_axes.iter().copied() {
            let supported_y = matches!(
                y_axis,
                MsAxis::Amplitude
                    | MsAxis::Phase
                    | MsAxis::Real
                    | MsAxis::Imaginary
                    | MsAxis::U
                    | MsAxis::V
                    | MsAxis::W
                    | MsAxis::Weight
                    | MsAxis::Sigma
                    | MsAxis::WeightSpectrum
                    | MsAxis::SigmaSpectrum
                    | MsAxis::Flag
                    | MsAxis::FlagRow
                    | MsAxis::Azimuth
                    | MsAxis::Elevation
                    | MsAxis::HourAngle
                    | MsAxis::ParallacticAngle
            );
            if !supported_y {
                return Err(format!(
                    "msexplore y axis {} is modeled but not implemented yet",
                    y_axis
                ));
            }
        }
        if self.averaging.avgchannel.is_some()
            && self
                .y_axes
                .iter()
                .copied()
                .any(|axis| matches!(axis, MsAxis::Flag))
        {
            return Err("msexplore flag plots do not yet support avgchannel binning".to_string());
        }
        Ok(())
    }

    /// Check that a staged flag edit fits this plot: a single-y scatter plot
    /// of direct, unaveraged samples, with finite region bounds and only the
    /// panel/plot keys the plot shape can resolve.
    fn validate_flag_edit(&self, flag_edit: &MsFlagEditSpec) -> Result<(), String> {
        if self.y_axes.len() > 1 {
            return Err(
                "msexplore staged flag editing currently supports single-y scatter plots only"
                    .to_string(),
            );
        }
        if self.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) {
            return Err(
                "msexplore staged flag editing currently does not support stacked page presets"
                    .to_string(),
            );
        }
        if self.averaging.avgchannel.is_some() {
            return Err(
                "msexplore staged flag editing currently requires unbinned channel samples"
                    .to_string(),
            );
        }
        if self.averaging.avgtime.is_some()
            || self.averaging.avgscan
            || self.averaging.avgfield
            || self.averaging.avgbaseline
            || self.averaging.avgantenna
            || self.averaging.avgspw
        {
            return Err(
                "msexplore staged flag editing currently requires direct sample-resolved averaging controls"
                    .to_string(),
            );
        }
        if self.x_axis.uses_derived_geometry()
            || self
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::uses_derived_geometry)
        {
            return Err(
                "msexplore staged flag editing currently requires direct sample-resolved axes rather than deduplicated geometry axes".to_string(),
            );
        }
        if !flag_edit.region.x_min.is_finite()
            || !flag_edit.region.x_max.is_finite()
            || !flag_edit.region.y_min.is_finite()
            || !flag_edit.region.y_max.is_finite()
        {
            return Err("msexplore staged flag editing requires finite region bounds".to_string());
        }
        if self.iteration.iteraxis.is_none() && flag_edit.panel_key.is_some() {
            return Err(
                "msexplore staged flag editing only accepts panel_key for iterated plots"
                    .to_string(),
            );
        }
        if self.preset != Some(MsPlotPreset::AmplitudePhaseVsTimeStacked)
            && flag_edit.plot_index.is_some()
        {
            return Err(
                "msexplore staged flag editing only accepts plot_index for page payloads"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Return the single Y axis after validation.
    pub fn y_axis(&self) -> MsAxis {
        self.y_axes[0]
    }

    /// Return the optional secondary Y axis after validation.
    pub fn secondary_y_axis(&self) -> Option<MsAxis> {
        self.y_axes.get(1).copied()
    }
}

/// Top-level `msexplore` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsExploreSpec {
    /// MeasurementSet root directory.
    pub ms_path: PathBuf,
    /// Human/machine-readable summary format.
    pub summary_format: MeasurementSetSummaryOutputFormat,
    /// Shared row-selection controls.
    pub selection: MsSelection,
    /// Page header items shown above rendered plots.
    pub header_items: Vec<MsPageHeaderItem>,
    /// Optional page title override when composing multiple plots.
    pub page_title: Option<String>,
    /// Range behavior when exporting multi-plot pages.
    pub exprange: MsPageExportRange,
    /// Hard limit on the total number of plotted points rendered by this request.
    #[serde(default = "default_max_plot_points")]
    pub max_plot_points: usize,
    /// Plot definitions on the page.
    pub plots: Vec<MsPlotSpec>,
}

impl MsExploreSpec {
    /// Validate the high-level request.
    pub fn validate(&self) -> Result<(), String> {
        if self.plots.is_empty() {
            return Err("msexplore requires at least one plot per invocation".to_string());
        }
        if self.max_plot_points == 0 {
            return Err("msexplore max_plot_points must be a positive integer".to_string());
        }
        if self.plots.len() == 1 {
            self.plots[0].validate()?;
            return Ok(());
        }

        let mut expected_grid = None;
        let mut used_plot_indices = std::collections::BTreeSet::new();
        let mut used_cells = std::collections::BTreeSet::new();
        for plot in &self.plots {
            plot.validate_for_page_child()?;
            let layout = &plot.layout;
            if layout.rowindex >= layout.gridrows || layout.colindex >= layout.gridcols {
                return Err(format!(
                    "msexplore page child plot {} is placed outside the {}x{} page grid",
                    layout.plotindex, layout.gridrows, layout.gridcols
                ));
            }
            match expected_grid {
                None => expected_grid = Some((layout.gridrows, layout.gridcols)),
                Some((gridrows, gridcols))
                    if gridrows != layout.gridrows || gridcols != layout.gridcols =>
                {
                    return Err(
                        "msexplore multi-plot page composition requires all child plots to agree on gridrows/gridcols".to_string(),
                    );
                }
                Some(_) => {}
            }
            if !used_plot_indices.insert(layout.plotindex) {
                return Err(format!(
                    "msexplore multi-plot pages require unique plotindex values; found duplicate {}",
                    layout.plotindex
                ));
            }
            used_cells.insert((layout.rowindex, layout.colindex));
        }
        if let Some((gridrows, gridcols)) = expected_grid
            && used_cells.len() > gridrows.saturating_mul(gridcols)
        {
            return Err(
                "msexplore multi-plot page composition received more occupied cells than fit in the declared grid".to_string(),
            );
        }
        if self.exprange == MsPageExportRange::All {
            let first = &self.plots[0];
            let compatible = self.plots.iter().all(|plot| {
                plot.x_axis == first.x_axis
                    && plot.y_axis() == first.y_axis()
                    && plot.secondary_y_axis() == first.secondary_y_axis()
            });
            if !compatible {
                return Err(
                    "msexplore exprange=all currently requires all page plots to share the same x/y axis configuration".to_string(),
                );
            }
        }
        Ok(())
    }
}

/// Default hard cap on rendered points per `msexplore` request.
pub const DEFAULT_MAX_PLOT_POINTS: usize = 10_000_000;

fn default_max_plot_points() -> usize {
    DEFAULT_MAX_PLOT_POINTS
}

/// Typed plot payload prepared for one render/export step.
#[derive(Debug, Clone, PartialEq)]
pub enum MsPlotPayload {
    /// Reused `listobs` payload for metadata-oriented presets.
    ListObs(ListObsPlotPayload),
    /// Generic scatter payload for raw visibility plots.
    Scatter(MsScatterPlotPayload),
    /// Iterated multi-panel scatter payload for raw visibility plots.
    ScatterGrid(MsScatterGridPayload),
    /// Multi-plot scatter page payload for stacked/common paired views.
    ScatterPage(MsScatterPagePayload),
}

impl MsPlotPayload {}

/// Scatter payload for one raw MeasurementSet plot.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPlotPayload {
    /// Plot title used for export naming.
    pub title: String,
    /// X axis kind.
    pub x_axis: MsAxis,
    /// Y axis kind.
    pub y_axis: MsAxis,
    /// Optional right-side Y axis kind.
    pub secondary_y_axis: Option<MsAxis>,
    /// X axis label.
    pub x_label: String,
    /// Y axis label.
    pub y_label: String,
    /// Optional right-side Y axis label.
    pub secondary_y_label: Option<String>,
    /// Optional fixed X axis bounds.
    pub fixed_x_bounds: Option<(f64, f64)>,
    /// Optional fixed Y axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
    /// Optional fixed right-side Y axis bounds.
    pub secondary_fixed_y_bounds: Option<(f64, f64)>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
    /// Legend placement.
    pub legend_position: MsLegendPosition,
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
    /// Optional scatter marker radius in rendered pixels.
    pub symbol_size_px: Option<u32>,
    /// Grouped scatter series.
    pub series: Vec<MsScatterSeries>,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Human-readable summary.
    pub summary: String,
}

/// One grouped scatter series.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterSeries {
    /// Stable series label.
    pub label: String,
    /// Stable group key used for palette selection.
    pub color_group: String,
    /// Y axis rendered by this series.
    pub y_axis: MsAxis,
    /// Plot points as `(x, y)`.
    pub points: Vec<(f64, f64)>,
    /// Provenance for each plotted point in `points`.
    pub provenance: Vec<MsScatterPointRef>,
}

/// One plotted point's originating visibility sample range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsScatterPointRef {
    /// MAIN row index.
    pub row: usize,
    /// Correlation slot within the row FLAG/DATA cell.
    pub corr: usize,
    /// Inclusive channel start for this plotted point.
    pub chan_start: usize,
    /// Exclusive channel end for this plotted point.
    pub chan_end: usize,
}

/// One iterated scatter panel within a multi-panel page.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPanelPayload {
    /// Stable panel key derived from the iteraxis value.
    pub key: String,
    /// Human-readable panel label.
    pub label: String,
    /// Grouped scatter series within the panel.
    pub series: Vec<MsScatterSeries>,
    /// Human-readable summary for the panel.
    pub summary: String,
}

/// Multi-panel scatter payload produced by iterated `msexplore` plots.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterGridPayload {
    /// Page title used for export naming.
    pub title: String,
    /// X axis kind.
    pub x_axis: MsAxis,
    /// Y axis kind.
    pub y_axis: MsAxis,
    /// X axis label.
    pub x_label: String,
    /// Y axis label.
    pub y_label: String,
    /// Optional fixed X axis bounds.
    pub fixed_x_bounds: Option<(f64, f64)>,
    /// Optional fixed Y axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
    /// Legend placement.
    pub legend_position: MsLegendPosition,
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
    /// Optional scatter marker radius in rendered pixels.
    pub symbol_size_px: Option<u32>,
    /// Iteration axis used to build the page.
    pub iteraxis: MsIterationAxis,
    /// Resolved page grid row count.
    pub gridrows: usize,
    /// Resolved page grid column count.
    pub gridcols: usize,
    /// Share X bounds across panels.
    pub share_x_bounds: bool,
    /// Share Y bounds across panels.
    pub share_y_bounds: bool,
    /// Ordered panel payloads.
    pub panels: Vec<MsScatterPanelPayload>,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Human-readable page summary.
    pub summary: String,
}

/// One scatter subplot placed on a multi-plot page.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPageItemPayload {
    /// Zero-based plot index on the page.
    pub plotindex: usize,
    /// Zero-based row placement.
    pub rowindex: usize,
    /// Zero-based column placement.
    pub colindex: usize,
    /// Child scatter plot payload.
    pub plot: MsScatterPlotPayload,
}

/// Multi-plot page payload composed from non-iterated scatter plots.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPagePayload {
    /// Page title used for export naming.
    pub title: String,
    /// Page export range behavior.
    pub exprange: MsPageExportRange,
    /// Resolved page grid row count.
    pub gridrows: usize,
    /// Resolved page grid column count.
    pub gridcols: usize,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Ordered child plots placed on the page.
    pub items: Vec<MsScatterPageItemPayload>,
    /// Human-readable page summary.
    pub summary: String,
}

#[derive(Debug, Clone)]
struct PointBudget {
    max_plot_points: Option<usize>,
    rendered_points: usize,
    allow_predecimation: bool,
}

impl PointBudget {
    fn unlimited() -> Self {
        Self {
            max_plot_points: None,
            rendered_points: 0,
            allow_predecimation: false,
        }
    }

    fn limited(max_plot_points: usize, allow_predecimation: bool) -> Self {
        Self {
            max_plot_points: Some(max_plot_points),
            rendered_points: 0,
            allow_predecimation,
        }
    }

    fn record_points(&mut self, additional_points: usize, context: &str) -> Result<(), String> {
        self.rendered_points = self
            .rendered_points
            .checked_add(additional_points)
            .ok_or_else(|| format!("{context} exceeded the supported plotted-point range"))?;
        Ok(())
    }
}

fn allocate_series_point_quotas(lengths: &[usize], budget: usize) -> Vec<usize> {
    let total_points = lengths.iter().sum::<usize>();
    if total_points <= budget {
        return lengths.to_vec();
    }
    if budget == 0 {
        return vec![0; lengths.len()];
    }

    let mut quotas = vec![0usize; lengths.len()];
    let mut remainders = Vec::new();
    let mut assigned = 0usize;
    for (index, &length) in lengths.iter().enumerate() {
        if length == 0 {
            continue;
        }
        let scaled = length.saturating_mul(budget);
        let base = scaled / total_points;
        let remainder = scaled % total_points;
        quotas[index] = base;
        assigned += base;
        remainders.push((index, remainder, length));
    }

    remainders.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.2.cmp(&left.2)));
    let mut remaining = budget.saturating_sub(assigned);
    for (index, _, _) in remainders {
        if remaining == 0 {
            break;
        }
        quotas[index] += 1;
        remaining -= 1;
    }

    quotas
}

fn sampled_indices(length: usize, take: usize) -> Vec<usize> {
    if take >= length {
        return (0..length).collect();
    }
    if take == 0 {
        return Vec::new();
    }
    if take == 1 {
        return vec![length / 2];
    }

    (0..take)
        .map(|slot| slot.saturating_mul(length.saturating_sub(1)) / (take - 1))
        .collect()
}

fn compact_scatter_series(
    series: Vec<MsScatterSeries>,
    max_plot_points: usize,
) -> (Vec<MsScatterSeries>, usize, usize) {
    let lengths = series
        .iter()
        .map(|entry| entry.points.len())
        .collect::<Vec<_>>();
    let total_points = lengths.iter().sum::<usize>();
    if total_points <= max_plot_points {
        return (series, total_points, total_points);
    }

    let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
    let mut kept_points = 0usize;
    let compacted = series
        .into_iter()
        .zip(quotas)
        .filter_map(|(entry, quota)| {
            if quota == 0 || entry.points.is_empty() {
                return None;
            }
            let indices = sampled_indices(entry.points.len(), quota);
            kept_points += indices.len();
            let points = indices.iter().map(|&index| entry.points[index]).collect();
            let provenance = indices
                .iter()
                .map(|&index| entry.provenance[index].clone())
                .collect();
            Some(MsScatterSeries {
                label: entry.label,
                color_group: entry.color_group,
                y_axis: entry.y_axis,
                points,
                provenance,
            })
        })
        .collect();

    (compacted, total_points, kept_points)
}

fn compact_payload_to_budget(
    payload: MsPlotPayload,
    point_budget: &PointBudget,
) -> Result<MsPlotPayload, String> {
    let Some(max_plot_points) = point_budget.max_plot_points else {
        return Ok(payload);
    };
    if point_budget.rendered_points <= max_plot_points {
        return Ok(payload);
    }

    let warning = format!(
        "Downsampled plot from {} to at most {} points using evenly spaced per-series decimation.",
        point_budget.rendered_points, max_plot_points
    );

    let payload = match payload {
        MsPlotPayload::ListObs(payload) => MsPlotPayload::ListObs(payload),
        MsPlotPayload::Scatter(mut payload) => {
            let (series, original_points, kept_points) =
                compact_scatter_series(payload.series, max_plot_points);
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable points after point-budget decimation",
                    payload.title
                ));
            }
            payload.series = series;
            payload.header_lines.push(warning.clone());
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, original_points, kept_points
            );
            MsPlotPayload::Scatter(payload)
        }
        MsPlotPayload::ScatterGrid(mut payload) => {
            let total_points = payload
                .panels
                .iter()
                .map(|panel| {
                    panel
                        .series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>()
                })
                .sum::<usize>();
            let lengths = payload
                .panels
                .iter()
                .flat_map(|panel| panel.series.iter().map(|series| series.points.len()))
                .collect::<Vec<_>>();
            let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
            let mut quota_iter = quotas.into_iter();
            let mut kept_points = 0usize;
            let mut kept_panels = Vec::new();
            for panel in std::mem::take(&mut payload.panels) {
                let mut kept_series = Vec::new();
                for series in panel.series {
                    let quota = quota_iter.next().unwrap_or(0);
                    let (mut compacted, _, kept) = compact_scatter_series(vec![series], quota);
                    kept_points += kept;
                    kept_series.append(&mut compacted);
                }
                if !kept_series.is_empty() {
                    let panel_point_count = kept_series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>();
                    kept_panels.push(MsScatterPanelPayload {
                        key: panel.key,
                        label: panel.label,
                        summary: format!(
                            "{} Decimated points to {}.",
                            panel.summary, panel_point_count
                        ),
                        series: kept_series,
                    });
                }
            }
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable panels after point-budget decimation",
                    payload.title
                ));
            }
            payload.panels = kept_panels;
            payload.header_lines.push(warning.clone());
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, total_points, kept_points
            );
            MsPlotPayload::ScatterGrid(payload)
        }
        MsPlotPayload::ScatterPage(mut payload) => {
            let total_points = payload
                .items
                .iter()
                .map(|item| {
                    item.plot
                        .series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>()
                })
                .sum::<usize>();
            let lengths = payload
                .items
                .iter()
                .flat_map(|item| item.plot.series.iter().map(|series| series.points.len()))
                .collect::<Vec<_>>();
            let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
            let mut quota_iter = quotas.into_iter();
            let mut kept_points = 0usize;
            let mut kept_items = Vec::new();
            for mut item in std::mem::take(&mut payload.items) {
                let mut kept_series = Vec::new();
                for series in item.plot.series {
                    let quota = quota_iter.next().unwrap_or(0);
                    let (mut compacted, _, kept) = compact_scatter_series(vec![series], quota);
                    kept_points += kept;
                    kept_series.append(&mut compacted);
                }
                if !kept_series.is_empty() {
                    item.plot.series = kept_series;
                    item.plot.summary = format!(
                        "{} Decimated points to {}.",
                        item.plot.summary,
                        item.plot
                            .series
                            .iter()
                            .map(|series| series.points.len())
                            .sum::<usize>()
                    );
                    kept_items.push(item);
                }
            }
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable plots after point-budget decimation",
                    payload.title
                ));
            }
            payload.items = kept_items;
            payload.header_lines.push(warning);
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, total_points, kept_points
            );
            MsPlotPayload::ScatterPage(payload)
        }
    };

    Ok(payload)
}

fn build_msexplore_plot_payload_validated(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    if spec.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) {
        return build_stacked_amplitude_phase_time_page(ms, selection, spec, point_budget);
    }
    if let Some(listobs_kind) = spec
        .preset
        .and_then(MsPlotPreset::lowers_to_listobs_metadata)
    {
        let listobs_options = selection.to_summary_options();
        return match listobs_kind {
            ListObsPlotKind::UvCoverage => {
                let coverage = ListObsUvCoverage::from_ms_with_options(ms, &listobs_options)
                    .map_err(|error| error.to_string())?;
                build_listobs_uv_plot_payload(&coverage, &ListObsPlotSpec::new(listobs_kind))
                    .map(MsPlotPayload::ListObs)
            }
            _ => {
                let summary = ListObsSummary::from_ms_with_options(ms, &listobs_options)
                    .map_err(|error| error.to_string())?;
                build_listobs_plot_payload_from_summary(
                    &summary,
                    &ListObsPlotSpec::new(listobs_kind),
                )
                .map(MsPlotPayload::ListObs)
            }
        };
    }

    build_generic_visibility_scatter(ms, selection, spec, point_budget)
}

fn build_msexplore_payload_internal(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    allow_decimation: bool,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    let mut point_budget = PointBudget::limited(spec.max_plot_points, allow_decimation);
    let header_lines = resolve_page_header_lines(ms, spec)?;
    if spec.plots.len() == 1 {
        let mut payload = build_msexplore_plot_payload_validated(
            ms,
            &spec.selection,
            &spec.plots[0],
            &mut point_budget,
        )?;
        apply_page_header_lines(&mut payload, header_lines);
        return if allow_decimation {
            compact_payload_to_budget(payload, &point_budget)
        } else if point_budget.rendered_points > spec.max_plot_points {
            Err(format!(
                "{} would render more than {}; narrow the selection, add averaging, or raise --max-points",
                spec.plots[0]
                    .preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("Requested plot"),
                spec.max_plot_points
            ))
        } else {
            Ok(payload)
        };
    }

    let (gridrows, gridcols) = {
        let layout = &spec.plots[0].layout;
        (layout.gridrows, layout.gridcols)
    };
    let mut items = Vec::with_capacity(spec.plots.len());
    for plot in &spec.plots {
        let payload =
            build_msexplore_plot_payload_validated(ms, &spec.selection, plot, &mut point_budget)?;
        let scatter = match payload {
            MsPlotPayload::Scatter(payload) => payload,
            MsPlotPayload::ListObs(_) => {
                return Err(
                    "msexplore multi-plot pages currently support raw-visibility scatter plots only"
                        .to_string(),
                );
            }
            MsPlotPayload::ScatterGrid(_) => {
                return Err(
                    "msexplore multi-plot pages do not yet support iterated child grids"
                        .to_string(),
                );
            }
            MsPlotPayload::ScatterPage(_) => {
                return Err(
                    "msexplore multi-plot pages do not yet support nested page payloads"
                        .to_string(),
                );
            }
        };
        items.push(MsScatterPageItemPayload {
            plotindex: plot.layout.plotindex,
            rowindex: plot.layout.rowindex,
            colindex: plot.layout.colindex,
            plot: scatter,
        });
    }
    items.sort_by_key(|item| item.plotindex);
    validate_page_overplots(&items)?;
    let title = spec
        .page_title
        .clone()
        .unwrap_or_else(|| "MeasurementSet Multi-Plot Page".to_string());

    let payload = MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title,
        exprange: spec.exprange,
        gridrows,
        gridcols,
        header_lines,
        summary: format!(
            "MeasurementSet multi-plot page. Plots={} Occupied cells={} Grid={}x{} Exprange={}",
            items.len(),
            items
                .iter()
                .map(|item| (item.rowindex, item.colindex))
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            gridrows,
            gridcols,
            spec.exprange
        ),
        items,
    });
    if allow_decimation {
        compact_payload_to_budget(payload, &point_budget)
    } else if point_budget.rendered_points > spec.max_plot_points {
        Err(format!(
            "MeasurementSet multi-plot page would render more than {}; narrow the selection, add averaging, or raise --max-points",
            spec.max_plot_points
        ))
    } else {
        Ok(payload)
    }
}

/// Rejects a page whose plots share a grid cell but not its axes.
///
/// Plots in one cell are overplotted on one set of axes, so they must agree
/// on the x, y and secondary-y axes, on any fixed axis bounds, and on the
/// legend position. `items` must be sorted by `plotindex`; the first plot in
/// each cell sets the cell's axes.
fn validate_page_overplots(items: &[MsScatterPageItemPayload]) -> Result<(), String> {
    let mut first_in_cell = std::collections::BTreeMap::new();
    for item in items {
        let cell = (item.rowindex, item.colindex);
        let first = *first_in_cell.entry(cell).or_insert(&item.plot);
        let (rowindex, colindex) = cell;
        if item.plot.x_axis != first.x_axis
            || item.plot.y_axis != first.y_axis
            || item.plot.secondary_y_axis != first.secondary_y_axis
        {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible axes; overplots require matching x/y axis configurations"
            ));
        }
        if item.plot.fixed_x_bounds != first.fixed_x_bounds
            || item.plot.fixed_y_bounds != first.fixed_y_bounds
            || item.plot.secondary_fixed_y_bounds != first.secondary_fixed_y_bounds
        {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible fixed axis bounds"
            ));
        }
        if item.plot.legend_position != first.legend_position {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible legend positions"
            ));
        }
    }
    Ok(())
}

/// Build a plot payload from a full `msexplore` request.
pub fn build_msexplore_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<MsPlotPayload, String> {
    build_msexplore_payload_internal(ms, spec, true)
}

/// Open a MeasurementSet path from a full `msexplore` request and build the
/// requested plot payload.
pub fn build_msexplore_payload_from_spec(spec: &MsExploreSpec) -> Result<MsPlotPayload, String> {
    let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| {
        if spec.ms_path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                spec.ms_path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", spec.ms_path.display())
        }
    })?;
    build_msexplore_payload(&ms, spec)
}

/// Preview a staged flag edit against one standalone `msexplore` scatter plot.
///
/// The returned preview resolves the plot region to exact `(row, corr, chan)`
/// samples before any writeback occurs.
pub fn preview_msexplore_flag_edit(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsFlagEditPreview, String> {
    spec.validate()?;
    let flag_edit = spec
        .flag_edit
        .as_ref()
        .ok_or_else(|| "msexplore flag-edit preview requires MsPlotSpec.flag_edit".to_string())?;
    let mut point_budget = PointBudget::unlimited();
    let payload = build_msexplore_plot_payload_validated(ms, selection, spec, &mut point_budget)?;
    preview_msexplore_flag_edit_from_payload(ms, payload, flag_edit)
}

/// Preview a staged flag edit against a full `msexplore` request, including
/// stacked presets and generic multi-plot page payloads.
pub fn preview_msexplore_flag_edit_for_request(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    spec.validate()?;
    validate_flag_edit_request(flag_edit)?;
    let payload = build_msexplore_payload_internal(ms, spec, false)?;
    preview_msexplore_flag_edit_from_payload(ms, payload, flag_edit)
}

/// Apply a staged flag edit resolved from a full `msexplore` request and return
/// the exact preview that was committed.
pub fn apply_msexplore_flag_edit_for_request(
    ms: &mut MeasurementSet,
    spec: &MsExploreSpec,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    let preview = preview_msexplore_flag_edit_for_request(ms, spec, flag_edit)?;
    apply_msexplore_flag_edit_preview(ms, preview)
}

fn preview_msexplore_flag_edit_from_payload(
    ms: &MeasurementSet,
    payload: MsPlotPayload,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    match payload {
        MsPlotPayload::Scatter(payload) => {
            if flag_edit.panel_key.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts panel_key for iterated scatter grids"
                        .to_string(),
                );
            }
            if flag_edit.plot_index.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts plot_index for multi-plot page payloads"
                        .to_string(),
                );
            }
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &payload.title,
                    plot_index: None,
                    panel_key: None,
                    panel_label: None,
                    x_axis: payload.x_axis,
                    y_axis: payload.y_axis,
                },
                &payload.series,
                flag_edit,
            )
        }
        MsPlotPayload::ListObs(_) => Err(
            "msexplore staged flag editing currently supports raw-visibility scatter plots only"
                .to_string(),
        ),
        MsPlotPayload::ScatterGrid(payload) => {
            if flag_edit.plot_index.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts plot_index for multi-plot page payloads"
                        .to_string(),
                );
            }
            let panel = match flag_edit.panel_key.as_deref() {
                Some(key) => payload
                    .panels
                    .iter()
                    .find(|panel| panel.key == key)
                    .ok_or_else(|| {
                        format!(
                            "msexplore flag-edit panel_key {:?} was not found in iterated payload; available panels: {}",
                            key,
                            payload
                                .panels
                                .iter()
                                .map(|panel| panel.key.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })?,
                None if payload.panels.len() == 1 => &payload.panels[0],
                None => {
                    return Err(format!(
                        "msexplore staged flag editing requires panel_key for iterated plots with multiple panels; available panels: {}",
                        payload
                            .panels
                            .iter()
                            .map(|panel| panel.key.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            };
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &payload.title,
                    plot_index: None,
                    panel_key: Some(panel.key.clone()),
                    panel_label: Some(panel.label.clone()),
                    x_axis: payload.x_axis,
                    y_axis: payload.y_axis,
                },
                &panel.series,
                flag_edit,
            )
        }
        MsPlotPayload::ScatterPage(payload) => {
            if flag_edit.panel_key.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts panel_key for iterated scatter grids"
                        .to_string(),
                );
            }
            let item = match flag_edit.plot_index {
                Some(plot_index) => payload
                    .items
                    .iter()
                    .find(|item| item.plotindex == plot_index)
                    .ok_or_else(|| {
                        format!(
                            "msexplore flag-edit plot_index {} was not found in page payload; available plot indices: {}",
                            plot_index,
                            payload
                                .items
                                .iter()
                                .map(|item| item.plotindex.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })?,
                None if payload.items.len() == 1 => &payload.items[0],
                None => {
                    return Err(format!(
                        "msexplore staged flag editing requires plot_index for page payloads with multiple plots; available plot indices: {}",
                        payload
                            .items
                            .iter()
                            .map(|item| item.plotindex.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            };
            if item.plot.secondary_y_axis.is_some() {
                return Err(
                    "msexplore staged flag editing currently supports single-y child plots only"
                        .to_string(),
                );
            }
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &item.plot.title,
                    plot_index: Some(item.plotindex),
                    panel_key: None,
                    panel_label: None,
                    x_axis: item.plot.x_axis,
                    y_axis: item.plot.y_axis,
                },
                &item.plot.series,
                flag_edit,
            )
        }
    }
}

fn validate_flag_edit_request(flag_edit: &MsFlagEditSpec) -> Result<(), String> {
    if !flag_edit.region.x_min.is_finite()
        || !flag_edit.region.x_max.is_finite()
        || !flag_edit.region.y_min.is_finite()
        || !flag_edit.region.y_max.is_finite()
    {
        return Err("msexplore staged flag editing requires finite region bounds".to_string());
    }
    if flag_edit.plot_index.is_some() && flag_edit.panel_key.is_some() {
        return Err(
            "msexplore staged flag editing accepts either plot_index or panel_key, not both"
                .to_string(),
        );
    }
    Ok(())
}

/// Apply a staged flag edit to MAIN `FLAG` / `FLAG_ROW` and return the exact
/// preview that was committed.
pub fn apply_msexplore_flag_edit(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsFlagEditPreview, String> {
    let preview = preview_msexplore_flag_edit(ms, selection, spec)?;
    apply_msexplore_flag_edit_preview(ms, preview)
}

fn apply_msexplore_flag_edit_preview(
    ms: &mut MeasurementSet,
    preview: MsFlagEditPreview,
) -> Result<MsFlagEditPreview, String> {
    let mut row_updates = std::collections::BTreeMap::<usize, (ndarray::Array2<bool>, bool)>::new();
    for row_edit in &preview.row_edits {
        row_updates.insert(
            row_edit.row,
            (clone_flag_matrix(ms, row_edit.row)?, row_edit.new_flag_row),
        );
    }
    for sample in &preview.sample_edits {
        let (matrix, _) = row_updates
            .get_mut(&sample.row)
            .ok_or_else(|| format!("flag edit lost planned row {}", sample.row))?;
        matrix[(sample.corr, sample.chan)] = sample.new_flag;
    }
    for (row, (matrix, flag_row)) in row_updates {
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG")
            .map_err(|error| error.to_string())?
            .set(Value::Array(ArrayValue::Bool(matrix.into_dyn())))
            .map_err(|error| error.to_string())?;
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG_ROW")
            .map_err(|error| error.to_string())?
            .set(Value::Scalar(ScalarValue::Bool(flag_row)))
            .map_err(|error| error.to_string())?;
    }
    Ok(preview)
}

fn apply_page_header_lines(payload: &mut MsPlotPayload, header_lines: Vec<String>) {
    if header_lines.is_empty() {
        return;
    }
    match payload {
        MsPlotPayload::Scatter(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ScatterGrid(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ScatterPage(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ListObs(_) => {}
    }
}

fn build_flag_edit_preview_from_series(
    context: &FlagEditPreviewContext<'_>,
    series: &[MsScatterSeries],
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    let mut matched_points = 0usize;
    let mut selected_samples = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut row_shapes = std::collections::BTreeMap::<usize, (usize, usize)>::new();
    let region = normalized_flag_region(&flag_edit.region);

    for series in series {
        if series.points.len() != series.provenance.len() {
            return Err(format!(
                "msexplore scatter series {:?} has {} points but {} provenance entries",
                series.label,
                series.points.len(),
                series.provenance.len()
            ));
        }
        for ((x, y), point_ref) in series.points.iter().zip(series.provenance.iter()) {
            if !flag_region_contains(&region, *x, *y) {
                continue;
            }
            matched_points += 1;
            let (corr_count, chan_count) = *row_shapes
                .entry(point_ref.row)
                .or_insert(clone_flag_matrix(context.ms, point_ref.row)?.dim());
            let corr_start = if flag_edit.extcorr { 0 } else { point_ref.corr };
            let corr_end = if flag_edit.extcorr {
                corr_count
            } else {
                point_ref.corr + 1
            };
            let chan_start = if flag_edit.extchannel {
                0
            } else {
                point_ref.chan_start
            };
            let chan_end = if flag_edit.extchannel {
                chan_count
            } else {
                point_ref.chan_end
            };
            for corr in corr_start..corr_end {
                for chan in chan_start..chan_end {
                    selected_samples.insert((point_ref.row, corr, chan));
                }
            }
        }
    }

    let mut sample_edits = Vec::<MsFlagSampleEdit>::new();
    let mut row_edits = Vec::<MsFlagRowEdit>::new();
    let mut samples_by_row = std::collections::BTreeMap::<usize, Vec<(usize, usize)>>::new();
    for (row, corr, chan) in selected_samples {
        samples_by_row.entry(row).or_default().push((corr, chan));
    }

    let flag_row = context.ms.flag_row_column();
    for (row, samples) in samples_by_row {
        let old_flag_row = flag_row.get(row).map_err(|error| error.to_string())?;
        let old_matrix = clone_flag_matrix(context.ms, row)?;
        let mut new_matrix = old_matrix.clone();
        let mut changed_samples = 0usize;
        for (corr, chan) in samples {
            let old_flag = old_matrix[(corr, chan)];
            let new_flag = match flag_edit.action {
                MsFlagAction::Flag => true,
                MsFlagAction::Unflag => false,
            };
            if old_flag != new_flag {
                new_matrix[(corr, chan)] = new_flag;
                sample_edits.push(MsFlagSampleEdit {
                    row,
                    corr,
                    chan,
                    old_flag,
                    new_flag,
                });
                changed_samples += 1;
            }
        }
        let new_flag_row = new_matrix.iter().all(|value| *value);
        if changed_samples > 0 || old_flag_row != new_flag_row {
            row_edits.push(MsFlagRowEdit {
                row,
                old_flag_row,
                new_flag_row,
                changed_samples,
            });
        }
    }

    Ok(MsFlagEditPreview {
        plot_title: context.plot_title.to_string(),
        plot_index: context.plot_index,
        panel_key: context.panel_key.clone(),
        panel_label: context.panel_label.clone(),
        x_axis: context.x_axis,
        y_axis: context.y_axis,
        region,
        action: flag_edit.action,
        extcorr: flag_edit.extcorr,
        extchannel: flag_edit.extchannel,
        matched_points,
        affected_rows: row_edits.len(),
        affected_samples: sample_edits.len(),
        sample_edits,
        row_edits,
    })
}

fn normalized_flag_region(region: &MsFlagRegion) -> MsFlagRegion {
    MsFlagRegion {
        x_min: region.x_min.min(region.x_max),
        x_max: region.x_min.max(region.x_max),
        y_min: region.y_min.min(region.y_max),
        y_max: region.y_min.max(region.y_max),
    }
}

fn flag_region_contains(region: &MsFlagRegion, x: f64, y: f64) -> bool {
    x >= region.x_min && x <= region.x_max && y >= region.y_min && y <= region.y_max
}

fn clone_flag_matrix(ms: &MeasurementSet, row: usize) -> Result<ndarray::Array2<bool>, String> {
    match ms
        .flag_column()
        .get(row)
        .map_err(|error| error.to_string())?
    {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .map(|matrix| matrix.to_owned())
            .map_err(|_| {
                "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
            }),
        other => Err(format!(
            "msexplore requires BOOL flag cells, found {:?}",
            other.primitive_type()
        )),
    }
}

fn resolve_page_header_lines(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<Vec<String>, String> {
    if spec.header_items.is_empty() {
        return Ok(Vec::new());
    }
    let summary = ListObsSummary::from_ms_with_options(ms, &spec.selection.to_summary_options())
        .map_err(|error| error.to_string())?;
    let segments = spec
        .header_items
        .iter()
        .filter_map(|item| resolve_page_header_segment(*item, spec, &summary))
        .collect::<Vec<_>>();
    Ok(wrap_header_segments(&segments))
}

fn resolve_page_header_segment(
    item: MsPageHeaderItem,
    spec: &MsExploreSpec,
    summary: &ListObsSummary,
) -> Option<String> {
    match item {
        MsPageHeaderItem::Filename => spec
            .ms_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("Filename: {name}")),
        MsPageHeaderItem::YColumn => {
            let y_columns = collect_page_y_column_labels(spec);
            (!y_columns.is_empty()).then(|| format!("Y Column: {}", y_columns.join(", ")))
        }
        MsPageHeaderItem::ObsDate => measurement_set_start_time(summary)
            .map(|time| format!("Obs Date: {}", MvTime::from_mjd_seconds(time).format_dmy(1))),
        MsPageHeaderItem::ObsTime => measurement_set_start_time(summary).map(|time| {
            format!(
                "Obs Time: {}",
                MvTime::from_mjd_seconds(time).format_time(1)
            )
        }),
        MsPageHeaderItem::Observer => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.observer.as_str()),
        )
        .map(|value| format!("Observer: {value}")),
        MsPageHeaderItem::ProjId => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.project.as_str()),
        )
        .map(|value| format!("Project: {value}")),
        MsPageHeaderItem::Telescope => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.telescope_name.as_str()),
        )
        .map(|value| format!("Telescope: {value}")),
        MsPageHeaderItem::TargName => summary
            .fields
            .first()
            .map(|field| field.name.clone())
            .or_else(|| summary.sources.first().map(|source| source.name.clone()))
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("Target: {value}")),
        MsPageHeaderItem::TargDir => summary.fields.first().map(|field| {
            let ra = MvAngle::from_radians(field.phase_direction_radians[0]).format_time(6);
            let dec = MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5);
            match field.direction_reference.as_deref() {
                Some(reference) if !reference.trim().is_empty() => {
                    format!("Target Dir: {ra} {dec} {reference}")
                }
                _ => format!("Target Dir: {ra} {dec}"),
            }
        }),
    }
}

fn collect_page_y_column_labels(spec: &MsExploreSpec) -> Vec<String> {
    let mut labels = Vec::<String>::new();
    for plot in &spec.plots {
        let plot_labels =
            if let Some(label) = plot.style.ylabel.as_ref().filter(|label| !label.is_empty()) {
                vec![label.clone()]
            } else {
                plot.y_axes
                    .iter()
                    .map(|axis| axis.display_name().to_string())
                    .collect::<Vec<_>>()
            };
        for label in plot_labels {
            if !labels.iter().any(|existing| existing == &label) {
                labels.push(label);
            }
        }
    }
    labels
}

fn wrap_header_segments(segments: &[String]) -> Vec<String> {
    const MAX_HEADER_LINE_CHARS: usize = 96;
    let mut lines = Vec::<String>::new();
    let mut current = String::new();
    for segment in segments {
        if current.is_empty() {
            current.push_str(segment);
            continue;
        }
        let candidate_len = current.len() + 3 + segment.len();
        if candidate_len > MAX_HEADER_LINE_CHARS {
            lines.push(current);
            current = segment.clone();
        } else {
            current.push_str("   ");
            current.push_str(segment);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

fn measurement_set_start_time(summary: &ListObsSummary) -> Option<f64> {
    summary.measurement_set.start_mjd_seconds.or_else(|| {
        summary
            .observations
            .iter()
            .find_map(|observation| observation.start_mjd_seconds)
    })
}

fn first_non_empty<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    values
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

/// Build a generic plot payload from one open MeasurementSet.
pub fn build_msexplore_plot_payload(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    let mut point_budget = PointBudget::unlimited();
    build_msexplore_plot_payload_validated(ms, selection, spec, &mut point_budget)
}

/// Open a MeasurementSet path and build the requested plot payload.
pub fn build_msexplore_plot_payload_from_path(
    path: &Path,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    let ms = MeasurementSet::open(path).map_err(|error| {
        if path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", path.display())
        }
    })?;
    build_msexplore_plot_payload(&ms, selection, spec)
}

/// Write one plot payload's text point manifest (`--plot-format txt`).
///
/// The manifest is a tab-separated list of the plotted points with `#`
/// header lines naming the axes, layout and series; the CASA-parity
/// comparator reads it. PNG and PDF rasterisation was removed (#833):
/// Python renders the typed plot document with Matplotlib instead.
pub fn export_msexplore_plot(payload: &MsPlotPayload, output_path: &Path) -> Result<(), String> {
    let manifest = match payload {
        MsPlotPayload::ListObs(payload) => render_listobs_manifest(payload)?,
        MsPlotPayload::Scatter(_)
        | MsPlotPayload::ScatterGrid(_)
        | MsPlotPayload::ScatterPage(_) => render_scatter_manifest(payload)?,
    };
    std::fs::write(output_path, manifest).map_err(|error| error.to_string())
}

fn render_scatter_manifest(payload: &MsPlotPayload) -> Result<String, String> {
    match payload {
        MsPlotPayload::Scatter(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
            out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
            out.push_str(&format!("# legendposition={}\n", payload.legend_position));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            if let Some(secondary_y_axis) = payload.secondary_y_axis {
                out.push_str(&format!(
                    "# secondary_y_axis={}\n",
                    secondary_y_axis.as_str()
                ));
                out.push_str("series_key\tseries_label\ty_axis\tx\ty\n");
            } else {
                out.push_str("series_key\tseries_label\tx\ty\n");
            }
            for series in &payload.series {
                for (x, y) in &series.points {
                    if payload.secondary_y_axis.is_some() {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            series.color_group,
                            series.label,
                            series.y_axis.as_str(),
                            x,
                            y
                        ));
                    } else {
                        out.push_str(&format!(
                            "{}\t{}\t{:.12}\t{:.12}\n",
                            series.color_group, series.label, x, y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ScatterGrid(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
            out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
            out.push_str(&format!("# iteraxis={}\n", payload.iteraxis.as_str()));
            out.push_str(&format!("# gridrows={}\n", payload.gridrows));
            out.push_str(&format!("# gridcols={}\n", payload.gridcols));
            out.push_str(&format!("# legendposition={}\n", payload.legend_position));
            out.push_str(&format!("# share_x_bounds={}\n", payload.share_x_bounds));
            out.push_str(&format!("# share_y_bounds={}\n", payload.share_y_bounds));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            out.push_str("panel_key\tpanel_label\tseries_key\tseries_label\tx\ty\n");
            for panel in &payload.panels {
                for series in &panel.series {
                    for (x, y) in &series.points {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            panel.key, panel.label, series.color_group, series.label, x, y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ScatterPage(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# exprange={}\n", payload.exprange));
            out.push_str(&format!("# gridrows={}\n", payload.gridrows));
            out.push_str(&format!("# gridcols={}\n", payload.gridcols));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            out.push_str(
                "plotindex\trowindex\tcolindex\tplot_title\tx_axis\ty_axis\tseries_key\tseries_label\tx\ty\n",
            );
            for item in &payload.items {
                for series in &item.plot.series {
                    for (x, y) in &series.points {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            item.plotindex,
                            item.rowindex,
                            item.colindex,
                            item.plot.title,
                            item.plot.x_axis.as_str(),
                            series.y_axis.as_str(),
                            series.color_group,
                            series.label,
                            x,
                            y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ListObs(_) => {
            Err("text manifest export requires a scatter payload".to_string())
        }
    }
}

fn render_listobs_manifest(payload: &ListObsPlotPayload) -> Result<String, String> {
    match payload {
        ListObsPlotPayload::UvCoverage(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str("# payload=listobs_uv_coverage\n");
            out.push_str("# title=UV Coverage\n");
            out.push_str("# x_axis=u_lambda\n");
            out.push_str("# y_axis=v_lambda\n");
            out.push_str(&format!("# mirror={}\n", payload.mirror));
            out.push_str(&format!(
                "# axis_extent_lambda={:.12}\n",
                payload.axis_extent_lambda
            ));
            out.push_str(&format!("# tracks={}\n", payload.tracks.len()));
            out.push_str(&format!("# summary={}\n", payload.summary));
            out.push_str("series_key\tseries_label\tmirrored\tx\ty\n");
            for (track_index, track) in payload.tracks.iter().enumerate() {
                let series_key = format!("track-{track_index}");
                for (x, y) in &track.points {
                    out.push_str(&format!(
                        "{}\t{}\tfalse\t{:.12}\t{:.12}\n",
                        series_key, track.label, x, y
                    ));
                    if payload.mirror {
                        out.push_str(&format!(
                            "{}\t{}\ttrue\t{:.12}\t{:.12}\n",
                            series_key, track.label, -x, -y
                        ));
                    }
                }
            }
            Ok(out)
        }
        _ => Err("text export is currently available for UV coverage and raw msexplore scatter plots only".to_string()),
    }
}

#[derive(Debug, Default)]
struct ScatterPanelAccumulator {
    label: String,
    series: std::collections::BTreeMap<String, MsScatterSeries>,
    contributing_rows: usize,
    contributing_points: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum AveragingBaselineToken {
    Baseline(i32, i32),
    Antenna(i32),
    Averaged,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TimeAverageScopeKey {
    field_id: Option<i32>,
    scan_number: Option<i32>,
    spw_id: Option<i32>,
    baseline: AveragingBaselineToken,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SharedTimeAverageScopeKey {
    field_id: Option<i32>,
    scan_number: Option<i32>,
    spw_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum AveragingTimeKey {
    Exact(u64),
    Bucket(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AveragingPointKey {
    time_key: AveragingTimeKey,
    field_id: Option<i32>,
    scan_number: Option<i32>,
    spw_id: Option<i32>,
    baseline: AveragingBaselineToken,
    corr_index: usize,
    chan_start: usize,
    chan_end: usize,
    chan_ordinal: usize,
}

struct AveragingPointKeyInputs<'a> {
    field_id: i32,
    scan_number: i32,
    spw_id: i32,
    baseline: AveragingBaselineToken,
    corr_index: usize,
    row_time_value: f64,
    bin: &'a ChannelBin,
}

#[derive(Debug, Default)]
struct AveragedPointAccumulator {
    visibility_samples: Vec<Complex64>,
    visibility_sample_weights: Vec<f64>,
    numeric_axis_samples: Vec<(MsAxis, Vec<f64>)>,
    time_interval_samples: Vec<f64>,
    has_flag_samples: bool,
    any_flag_sample: bool,
    has_flag_row_samples: bool,
    any_flag_row: bool,
    representative: Option<MsScatterPointRef>,
}

#[derive(Debug)]
struct AveragedSeriesAccumulator {
    label: String,
    color_group: String,
    y_axis: MsAxis,
    points: std::collections::BTreeMap<AveragingPointKey, AveragedPointAccumulator>,
}

impl Default for AveragedSeriesAccumulator {
    fn default() -> Self {
        Self {
            label: String::new(),
            color_group: String::new(),
            y_axis: MsAxis::Amplitude,
            points: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct AveragedPanelAccumulator {
    label: String,
    series: std::collections::BTreeMap<String, AveragedSeriesAccumulator>,
    contributing_rows: std::collections::BTreeSet<usize>,
}

struct VisibilityPredecimationContext<'a> {
    data_description: &'a MsDataDescription<'a>,
    spectral_window: &'a crate::subtables::spectral_window::MsSpectralWindow<'a>,
    polarization: &'a MsPolarization<'a>,
    requested_corr_codes: Option<&'a [i32]>,
}

fn predecimate_visibility_rows(
    ms: &MeasurementSet,
    row_numbers: Vec<usize>,
    spec: &MsPlotSpec,
    point_budget: &PointBudget,
    context: VisibilityPredecimationContext<'_>,
) -> Result<Vec<usize>, String> {
    if !point_budget.allow_predecimation || row_numbers.len() <= 1 {
        return Ok(row_numbers);
    }
    let Some(max_plot_points) = point_budget.max_plot_points else {
        return Ok(row_numbers);
    };
    let Some(first_row) = row_numbers.first().copied() else {
        return Ok(row_numbers);
    };

    let ddid = selected_i32_values(ms.main_table(), "DATA_DESC_ID", &[first_row])?
        .into_iter()
        .next()
        .ok_or_else(|| format!("msexplore requires DATA_DESC_ID data for row {first_row}"))?;
    if ddid < 0 || (ddid as usize) >= context.data_description.row_count() {
        return Ok(row_numbers);
    }
    let ddid = ddid as usize;
    let spw_id = context
        .data_description
        .spectral_window_id(ddid)
        .map_err(|error| error.to_string())?;
    let chan_count = if spw_id >= 0 && (spw_id as usize) < context.spectral_window.row_count() {
        let num_chan = context
            .spectral_window
            .num_chan(spw_id as usize)
            .map_err(|error| error.to_string())?;
        usize::try_from(num_chan).unwrap_or(0)
    } else {
        0
    };
    if chan_count == 0 {
        return Ok(row_numbers);
    }
    let pol_id = context
        .data_description
        .polarization_id(ddid)
        .map_err(|error| error.to_string())?;
    let (corr_count, corr_types) =
        if pol_id >= 0 && (pol_id as usize) < context.polarization.row_count() {
            let pol_id = pol_id as usize;
            let corr_types = context
                .polarization
                .corr_type(pol_id)
                .map_err(|error| error.to_string())?;
            let num_corr = context
                .polarization
                .num_corr(pol_id)
                .map_err(|error| error.to_string())?;
            let corr_count = usize::try_from(num_corr)
                .ok()
                .filter(|count| *count > 0)
                .unwrap_or(corr_types.len());
            (corr_count, corr_types)
        } else {
            (0, Vec::new())
        };
    if corr_count == 0 {
        return Ok(row_numbers);
    }
    let selected_correlations =
        select_correlation_slots(corr_count, &corr_types, context.requested_corr_codes);
    if selected_correlations.is_empty() {
        return Ok(row_numbers);
    }
    let correlation_required = spec.x_axis.uses_correlation_slots()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_correlation_slots)
        || matches!(spec.color_by, MsColorAxis::Correlation)
        || matches!(spec.iteration.iteraxis, Some(MsIterationAxis::Correlation));
    let corr_points_per_row = if correlation_required {
        selected_correlations.len()
    } else {
        1
    };
    let channel_points_per_corr = plot_channel_bins(chan_count, spec)?.len();
    let y_points_per_bin = spec.y_axes.len().max(1);
    let estimated_points_per_row = corr_points_per_row
        .saturating_mul(channel_points_per_corr)
        .saturating_mul(y_points_per_bin);
    if estimated_points_per_row == 0 {
        return Ok(row_numbers);
    }

    let estimated_points = row_numbers.len().saturating_mul(estimated_points_per_row);
    if estimated_points <= max_plot_points {
        return Ok(row_numbers);
    }
    let target_rows = (max_plot_points / estimated_points_per_row).clamp(1, row_numbers.len());
    let sampled = sampled_indices(row_numbers.len(), target_rows)
        .into_iter()
        .map(|index| row_numbers[index])
        .collect();
    Ok(sampled)
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn build_generic_visibility_scatter(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    let timing_enabled = VisibilityScatterTiming::enabled();
    let scatter_started = Instant::now();
    let mut checkpoint_started = Instant::now();
    let mut scatter_timing = VisibilityScatterTiming::default();
    if uses_extended_averaging(spec) {
        return build_generic_visibility_scatter_with_averaging(ms, selection, spec, point_budget);
    }

    let listobs_options = selection.to_summary_options();
    let row_numbers = resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?;
    let selected_row_count = row_numbers.len();
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("resolve_selected_rows", &mut checkpoint_started);
    }

    let needs_visibility_grid = spec.x_axis.is_visibility_math()
        || spec.y_axes.iter().copied().any(MsAxis::is_visibility_math);
    let needs_spectral_coordinates = spec.x_axis.uses_spectral_coordinates()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_spectral_coordinates);
    let needs_flag_grid = needs_visibility_grid
        || matches!(spec.x_axis, MsAxis::Flag)
        || spec.y_axes.iter().copied().any(|axis| {
            matches!(
                axis,
                MsAxis::Flag | MsAxis::WeightSpectrum | MsAxis::SigmaSpectrum
            )
        });
    let needs_weight_values = matches!(spec.x_axis, MsAxis::Weight | MsAxis::WeightSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::Weight | MsAxis::WeightSpectrum));
    let needs_sigma_values = matches!(spec.x_axis, MsAxis::Sigma | MsAxis::SigmaSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::Sigma | MsAxis::SigmaSpectrum));
    let needs_uvw_values = matches!(
        spec.x_axis,
        MsAxis::UvDistance | MsAxis::U | MsAxis::V | MsAxis::W
    ) || spec
        .y_axes
        .iter()
        .copied()
        .any(|axis| matches!(axis, MsAxis::UvDistance | MsAxis::U | MsAxis::V | MsAxis::W));
    let requested_freqframe = parse_frequency_frame(spec.transforms.freqframe.as_deref())?;
    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("open_metadata_subtables", &mut checkpoint_started);
    }

    let requested_corr_codes = selection
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let row_numbers = predecimate_visibility_rows(
        ms,
        row_numbers,
        spec,
        point_budget,
        VisibilityPredecimationContext {
            data_description: &data_description,
            spectral_window: &spectral_window,
            polarization: &polarization,
            requested_corr_codes: requested_corr_codes.as_deref(),
        },
    )?;
    let sampled_row_count = row_numbers.len();
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("predecimate_rows", &mut checkpoint_started);
    }
    let row_sampling_summary = if sampled_row_count < selected_row_count {
        format!(" Sampled rows={sampled_row_count}/{selected_row_count}.")
    } else {
        String::new()
    };

    let data_source = if needs_visibility_grid {
        Some(PreparedSelectedDataSource::new(
            ms,
            spec.data_column,
            &row_numbers,
        )?)
    } else {
        None
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_visibility_data", &mut checkpoint_started);
    }
    let selected_flags = if needs_flag_grid {
        Some(SelectedArrayColumn::load(
            ms.main_table(),
            "FLAG",
            &row_numbers,
        )?)
    } else {
        None
    };
    let selected_weights = if needs_weight_values {
        Some(SelectedArrayColumn::load(
            ms.main_table(),
            "WEIGHT",
            &row_numbers,
        )?)
    } else {
        None
    };
    let selected_sigmas = if needs_sigma_values {
        Some(SelectedArrayColumn::load(
            ms.main_table(),
            "SIGMA",
            &row_numbers,
        )?)
    } else {
        None
    };
    let selected_flag_rows = selected_bool_values(ms.main_table(), "FLAG_ROW", &row_numbers)?;
    let selected_data_desc_ids =
        selected_i32_values(ms.main_table(), "DATA_DESC_ID", &row_numbers)?;
    let selected_field_ids = selected_i32_values(ms.main_table(), "FIELD_ID", &row_numbers)?;
    let selected_scan_numbers = selected_i32_values(ms.main_table(), "SCAN_NUMBER", &row_numbers)?;
    let selected_antenna1 = selected_i32_values(ms.main_table(), "ANTENNA1", &row_numbers)?;
    let selected_antenna2 = selected_i32_values(ms.main_table(), "ANTENNA2", &row_numbers)?;
    let selected_times = selected_f64_values(ms.main_table(), "TIME", &row_numbers)?;
    let selected_uvw = if needs_uvw_values {
        selected_uvw_values(ms.main_table(), "UVW", &row_numbers)?
    } else {
        Vec::new()
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_aux_columns", &mut checkpoint_started);
    }
    let derived_engine = if spec.x_axis.uses_derived_geometry()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_derived_geometry)
        || (needs_spectral_coordinates && requested_freqframe.is_some())
    {
        Some(MsCalEngine::new(ms).map_err(|error| error.to_string())?)
    } else {
        None
    };
    let geometry_dedup_required = spec.x_axis.uses_derived_geometry()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_derived_geometry);
    let mut geometry_samples_seen =
        std::collections::BTreeSet::<(String, String, String, u64, i32)>::new();
    let chan_freq = if needs_spectral_coordinates {
        Some(ChanFreqColumn::new(spectral_window.table()))
    } else {
        None
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("prepare_context", &mut checkpoint_started);
    }

    let iteraxis = spec.iteration.iteraxis;
    let mut panel_order = Vec::<String>::new();
    let mut panels = std::collections::BTreeMap::<String, ScatterPanelAccumulator>::new();
    let mut contributing_rows = 0usize;
    let mut contributing_points = 0usize;
    let include_row_flagged_points =
        spec.x_axis.uses_flag_rows() || spec.y_axes.iter().copied().any(MsAxis::uses_flag_rows);
    let needs_weight_spectrum = matches!(spec.x_axis, MsAxis::WeightSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::WeightSpectrum));
    let needs_sigma_spectrum = matches!(spec.x_axis, MsAxis::SigmaSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::SigmaSpectrum));
    let selected_weight_spectrum = if needs_weight_spectrum {
        WeightSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "WEIGHT_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    let selected_sigma_spectrum = if needs_sigma_spectrum {
        SigmaSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "SIGMA_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_optional_spectra", &mut checkpoint_started);
    }

    for (row_slot, row) in row_numbers.iter().copied().enumerate() {
        let flag_row_value = selected_flag_rows[row_slot];
        if flag_row_value && !include_row_flagged_points {
            continue;
        }

        let ddid = selected_data_desc_ids[row_slot];
        if ddid < 0 || (ddid as usize) >= data_description.row_count() {
            continue;
        }
        let ddid = ddid as usize;
        let spw_id = data_description
            .spectral_window_id(ddid)
            .map_err(|error| error.to_string())?;
        let pol_id = data_description
            .polarization_id(ddid)
            .map_err(|error| error.to_string())?;
        let corr_types = if pol_id >= 0 && (pol_id as usize) < polarization.row_count() {
            polarization
                .corr_type(pol_id as usize)
                .map_err(|error| error.to_string())?
        } else {
            Vec::new()
        };

        let row_grid_started = timing_enabled.then(Instant::now);
        let grid = data_source
            .as_ref()
            .map(|source| source.row(row_slot))
            .transpose()?;
        if let Some(started) = row_grid_started {
            scatter_timing.row_grid += started.elapsed();
        }
        let expected_corr_count = corr_types.len();
        let spw_row = usize::try_from(spw_id)
            .map_err(|_| format!("invalid DATA_DESCRIPTION SPECTRAL_WINDOW_ID {spw_id}"))?;
        let expected_chan_count = spectral_window
            .num_chan(spw_row)
            .map_err(|error| error.to_string())
            .and_then(|count| {
                usize::try_from(count)
                    .map_err(|_| format!("invalid SPECTRAL_WINDOW NUM_CHAN {count}"))
            })?;
        let (corr_count, chan_count) = grid
            .as_ref()
            .map(|grid| (grid.corr_count, grid.chan_count))
            .unwrap_or((expected_corr_count, expected_chan_count));
        let flags = selected_flags
            .as_ref()
            .map(|column| match column.get(row_slot)? {
                ArrayValue::Bool(values) => {
                    values.view().into_dimensionality::<Ix2>().map_err(|_| {
                        "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
                    })
                }
                other => Err(format!(
                    "msexplore requires BOOL flag cells, found {:?}",
                    other.primitive_type()
                )),
            })
            .transpose()?;
        if let Some(flags) = flags.as_ref()
            && flags.shape() != [corr_count, chan_count]
        {
            return Err(format!(
                "visibility flag shape {:?} does not match data shape [{}, {}]",
                flags.shape(),
                corr_count,
                chan_count
            ));
        }
        let row_arrays_started = timing_enabled.then(Instant::now);
        let weight_values = selected_weights
            .as_ref()
            .map(|column| float_axis_values(column.get(row_slot)?, corr_count, "WEIGHT"))
            .transpose()?
            .unwrap_or_else(|| vec![0.0; corr_count]);
        let sigma_values = selected_sigmas
            .as_ref()
            .map(|column| float_axis_values(column.get(row_slot)?, corr_count, "SIGMA"))
            .transpose()?
            .unwrap_or_else(|| vec![0.0; corr_count]);
        let weight_spectrum_grid = if needs_weight_spectrum {
            Some(
                selected_weight_spectrum
                    .as_ref()
                    .map(|column| float_grid_from_array(column.get(row_slot)?, "WEIGHT_SPECTRUM"))
                    .transpose()?
                    .unwrap_or_else(|| scalar_values_to_grid(&weight_values, chan_count)),
            )
        } else {
            None
        };
        let sigma_spectrum_grid = if needs_sigma_spectrum {
            Some(
                selected_sigma_spectrum
                    .as_ref()
                    .map(|column| float_grid_from_array(column.get(row_slot)?, "SIGMA_SPECTRUM"))
                    .transpose()?
                    .unwrap_or_else(|| scalar_values_to_grid(&sigma_values, chan_count)),
            )
        } else {
            None
        };
        for (column_name, grid) in [
            ("WEIGHT_SPECTRUM", weight_spectrum_grid.as_ref()),
            ("SIGMA_SPECTRUM", sigma_spectrum_grid.as_ref()),
        ] {
            if let Some(grid) = grid
                && (grid.corr_count != corr_count || grid.chan_count != chan_count)
            {
                return Err(format!(
                    "{column_name} shape [{}, {}] does not match data shape [{corr_count}, {chan_count}]",
                    grid.corr_count, grid.chan_count
                ));
            }
        }
        if let Some(started) = row_arrays_started {
            scatter_timing.row_arrays += started.elapsed();
        }

        let selected_correlations =
            select_correlation_slots(corr_count, &corr_types, requested_corr_codes.as_deref());
        if selected_correlations.is_empty() {
            continue;
        }
        let correlation_required = spec.x_axis.uses_correlation_slots()
            || spec
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::uses_correlation_slots)
            || matches!(spec.color_by, MsColorAxis::Correlation)
            || matches!(iteraxis, Some(MsIterationAxis::Correlation));
        let selected_correlations = if correlation_required {
            selected_correlations
        } else {
            vec![selected_correlations[0].clone()]
        };

        let channel_bins = plot_channel_bins(chan_count, spec)?;

        let field_id_value = selected_field_ids[row_slot];
        let scan_number_value = selected_scan_numbers[row_slot];
        let antenna1_value = selected_antenna1[row_slot];
        let antenna2_value = selected_antenna2[row_slot];
        let row_time_value = selected_times[row_slot];
        let row_uvw_value = if needs_uvw_values {
            selected_uvw[row_slot]
        } else {
            [0.0, 0.0, 0.0]
        };
        let spectral_context = resolve_spectral_context(
            spec,
            spw_id,
            chan_count,
            field_id_value,
            row_time_value,
            chan_freq.as_ref(),
            &spectral_window,
            derived_engine.as_ref(),
        )?;

        let mut row_contributed = false;
        let mut row_panels = std::collections::BTreeSet::<String>::new();
        for (corr_index, corr_label) in &selected_correlations {
            let grouping_started = timing_enabled.then(Instant::now);
            let row_weight = weight_values[*corr_index];
            let row_sigma = sigma_values[*corr_index];
            let (group_key, group_label) = visibility_group(
                spec.color_by,
                field_id_value,
                &field,
                spw_id,
                &spectral_window,
                scan_number_value,
                antenna1_value,
                antenna2_value,
                Some(corr_label),
            );
            let (panel_key, panel_label) = iteration_group(
                iteraxis,
                field_id_value,
                &field,
                spw_id,
                &spectral_window,
                scan_number_value,
                Some(corr_label),
            );
            if !panels.contains_key(&panel_key) {
                panel_order.push(panel_key.clone());
                panels.insert(
                    panel_key.clone(),
                    ScatterPanelAccumulator {
                        label: panel_label.clone(),
                        ..Default::default()
                    },
                );
            }
            let series_identity = spec
                .y_axes
                .iter()
                .copied()
                .map(|y_axis| {
                    let (series_key, series_label, color_group) = scatter_series_identity(
                        y_axis,
                        spec.y_axes.len() > 1,
                        &group_key,
                        &group_label,
                    );
                    (y_axis, series_key, series_label, color_group)
                })
                .collect::<Vec<_>>();
            if let Some(started) = grouping_started {
                scatter_timing.grouping += started.elapsed();
            }
            let mut samples = Vec::<Complex64>::new();
            let mut flag_samples = Vec::<bool>::new();
            let mut weight_spectrum_samples = Vec::<f64>::new();
            let mut sigma_spectrum_samples = Vec::<f64>::new();
            for bin in &channel_bins {
                let bin_collect_started = timing_enabled.then(Instant::now);
                if let Some(grid) = grid.as_ref() {
                    let flags = flags
                        .as_ref()
                        .expect("visibility grid loads matching flag grid");
                    collect_bin_samples_into(
                        grid,
                        flags,
                        &[*corr_index],
                        bin.start,
                        bin.end,
                        &mut samples,
                    );
                } else {
                    samples.clear();
                }
                if let Some(flags) = flags.as_ref() {
                    collect_bin_flags_into(
                        flags,
                        *corr_index,
                        bin.start,
                        bin.end,
                        &mut flag_samples,
                    );
                } else {
                    flag_samples.clear();
                }
                if let Some(grid) = weight_spectrum_grid.as_ref() {
                    let flags = flags
                        .as_ref()
                        .expect("weight spectrum loads matching flag grid");
                    collect_bin_float_samples_into(
                        grid,
                        flags,
                        *corr_index,
                        bin.start,
                        bin.end,
                        &mut weight_spectrum_samples,
                    );
                } else {
                    weight_spectrum_samples.clear();
                }
                if let Some(grid) = sigma_spectrum_grid.as_ref() {
                    let flags = flags
                        .as_ref()
                        .expect("sigma spectrum loads matching flag grid");
                    collect_bin_float_samples_into(
                        grid,
                        flags,
                        *corr_index,
                        bin.start,
                        bin.end,
                        &mut sigma_spectrum_samples,
                    );
                } else {
                    sigma_spectrum_samples.clear();
                }
                if let Some(started) = bin_collect_started {
                    scatter_timing.bin_collect += started.elapsed();
                }
                let axis_started = timing_enabled.then(Instant::now);
                let Some(x_value) = compute_axis_value(
                    spec.x_axis,
                    &samples,
                    &flag_samples,
                    &weight_spectrum_samples,
                    &sigma_spectrum_samples,
                    flag_row_value,
                    row_weight,
                    row_sigma,
                    field_id_value,
                    antenna1_value,
                    spec.averaging.scalar,
                    bin,
                    spectral_context.as_ref(),
                    row_time_value,
                    row_uvw_value,
                    derived_engine.as_ref(),
                )?
                else {
                    continue;
                };
                let panel = panels
                    .get_mut(&panel_key)
                    .expect("panel inserted before mutation");
                for (y_axis, series_key, series_label, color_group) in &series_identity {
                    let Some(y_value) = compute_axis_value(
                        *y_axis,
                        &samples,
                        &flag_samples,
                        &weight_spectrum_samples,
                        &sigma_spectrum_samples,
                        flag_row_value,
                        row_weight,
                        row_sigma,
                        field_id_value,
                        antenna1_value,
                        spec.averaging.scalar,
                        bin,
                        spectral_context.as_ref(),
                        row_time_value,
                        row_uvw_value,
                        derived_engine.as_ref(),
                    )?
                    else {
                        continue;
                    };
                    if geometry_dedup_required
                        && !geometry_samples_seen.insert((
                            panel_key.clone(),
                            series_key.clone(),
                            y_axis.as_str().to_string(),
                            row_time_value.to_bits(),
                            field_id_value,
                        ))
                    {
                        continue;
                    }
                    let series =
                        panel
                            .series
                            .entry(series_key.clone())
                            .or_insert_with(|| MsScatterSeries {
                                label: series_label.clone(),
                                color_group: color_group.clone(),
                                y_axis: *y_axis,
                                points: Vec::new(),
                                provenance: Vec::new(),
                            });
                    let push_started = timing_enabled.then(Instant::now);
                    series.points.push((x_value, y_value));
                    series.provenance.push(MsScatterPointRef {
                        row,
                        corr: *corr_index,
                        chan_start: bin.start,
                        chan_end: bin.end,
                    });
                    if let Some(started) = push_started {
                        scatter_timing.point_push += started.elapsed();
                    }
                    point_budget.record_points(
                        1,
                        spec.preset
                            .map(MsPlotPreset::display_name)
                            .unwrap_or("Requested plot"),
                    )?;
                    panel.contributing_points += 1;
                    contributing_points += 1;
                    row_contributed = true;
                    row_panels.insert(panel_key.clone());
                }
                if let Some(started) = axis_started {
                    scatter_timing.axis_compute += started.elapsed();
                }
            }
        }

        for panel_key in row_panels {
            if let Some(panel) = panels.get_mut(&panel_key) {
                panel.contributing_rows += 1;
            }
        }
        if row_contributed {
            contributing_rows += 1;
        }
    }
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("expand_rows", &mut checkpoint_started);
    }

    if contributing_points == 0 {
        return Err(format!(
            "{} produced no unflagged visibility points for the current selection",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("Requested plot")
        ));
    }

    let title = spec
        .style
        .title
        .clone()
        .or_else(|| spec.preset.map(|preset| preset.display_name().to_string()))
        .unwrap_or_else(|| {
            format!(
                "{} vs {}",
                spec.y_axis().display_name(),
                spec.x_axis.display_name()
            )
        });
    let x_label = spec
        .style
        .xlabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.x_axis));
    let y_label = spec
        .style
        .ylabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.y_axis()));
    let secondary_y_label = spec.secondary_y_axis().map(axis_label);
    let fixed_x_bounds = fixed_bounds(spec.x_axis);
    let fixed_y_bounds = fixed_bounds(spec.y_axis());
    let secondary_fixed_y_bounds = spec.secondary_y_axis().and_then(fixed_bounds);
    let mut panels = panel_order
        .into_iter()
        .filter_map(|panel_key| {
            let panel = panels.remove(&panel_key)?;
            let mut series = panel.series.into_values().collect::<Vec<_>>();
            for entry in &mut series {
                entry
                    .points
                    .sort_by(|left, right| left.0.total_cmp(&right.0));
            }
            Some(MsScatterPanelPayload {
                key: panel_key,
                summary: format!(
                    "{}. Rows={} Points={} Data column={}",
                    panel.label,
                    panel.contributing_rows,
                    panel.contributing_points,
                    spec.data_column
                ),
                label: panel.label,
                series,
            })
        })
        .collect::<Vec<_>>();
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("sort_and_pack_series", &mut checkpoint_started);
        scatter_timing.report(
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            scatter_started,
        );
    }
    if let Some(iteraxis) = iteraxis {
        let (gridrows, gridcols) =
            resolve_iterated_grid(panels.len(), spec.layout.gridrows, spec.layout.gridcols)?;
        let share_x_bounds =
            share_axis_bounds(spec.iteration.xselfscale, spec.iteration.xsharedaxis);
        let share_y_bounds =
            share_axis_bounds(spec.iteration.yselfscale, spec.iteration.ysharedaxis);
        return Ok(MsPlotPayload::ScatterGrid(MsScatterGridPayload {
            title,
            x_axis: spec.x_axis,
            y_axis: spec.y_axis(),
            x_label,
            y_label,
            fixed_x_bounds,
            fixed_y_bounds,
            showlegend: spec.style.showlegend,
            legend_position: spec.style.legendposition,
            showmajorgrid: spec.style.showmajorgrid,
            showminorgrid: spec.style.showminorgrid,
            symbol_size_px: spec.style.symbol_size,
            iteraxis,
            gridrows,
            gridcols,
            share_x_bounds,
            share_y_bounds,
            header_lines: Vec::new(),
            summary: format!(
                "{}. Panels={} Rows={} Points={} Data column={}.{}",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("MeasurementSet iterated plot"),
                panels.len(),
                contributing_rows,
                contributing_points,
                spec.data_column,
                row_sampling_summary
            ),
            panels,
        }));
    }
    let panel = panels
        .pop()
        .ok_or_else(|| "msexplore scatter payload lost its only panel".to_string())?;
    Ok(MsPlotPayload::Scatter(MsScatterPlotPayload {
        title,
        x_axis: spec.x_axis,
        y_axis: spec.y_axis(),
        secondary_y_axis: spec.secondary_y_axis(),
        x_label,
        y_label,
        secondary_y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        secondary_fixed_y_bounds,
        showlegend: spec.style.showlegend,
        legend_position: spec.style.legendposition,
        showmajorgrid: spec.style.showmajorgrid,
        showminorgrid: spec.style.showminorgrid,
        symbol_size_px: spec.style.symbol_size,
        header_lines: Vec::new(),
        summary: format!(
            "{}. Rows={} Points={} Data column={}.{}",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            spec.data_column,
            row_sampling_summary
        ),
        series: panel.series,
    }))
}

fn uses_extended_averaging(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgtime.is_some()
        || spec.averaging.avgscan
        || spec.averaging.avgfield
        || spec.averaging.avgbaseline
        || spec.averaging.avgantenna
        || spec.averaging.avgspw
}

fn averaging_baseline_tokens(
    antenna1: i32,
    antenna2: i32,
    spec: &MsPlotSpec,
) -> Vec<AveragingBaselineToken> {
    if spec.averaging.avgantenna {
        vec![
            AveragingBaselineToken::Antenna(antenna1),
            AveragingBaselineToken::Antenna(antenna2),
        ]
    } else if spec.averaging.avgbaseline {
        vec![AveragingBaselineToken::Averaged]
    } else {
        vec![AveragingBaselineToken::Baseline(antenna1, antenna2)]
    }
}

fn build_time_average_scope_key(
    field_id: i32,
    scan_number: i32,
    spw_id: i32,
    baseline: AveragingBaselineToken,
    spec: &MsPlotSpec,
) -> TimeAverageScopeKey {
    TimeAverageScopeKey {
        field_id: (!spec.averaging.avgfield).then_some(field_id),
        scan_number: (!spec.averaging.avgscan).then_some(scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(spw_id),
        baseline,
    }
}

fn build_shared_time_average_scope_key(
    field_id: i32,
    scan_number: i32,
    spw_id: i32,
    spec: &MsPlotSpec,
) -> SharedTimeAverageScopeKey {
    SharedTimeAverageScopeKey {
        field_id: (!spec.averaging.avgfield).then_some(field_id),
        scan_number: (!spec.averaging.avgscan).then_some(scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(spw_id),
    }
}

fn build_averaging_point_key(
    inputs: AveragingPointKeyInputs<'_>,
    spec: &MsPlotSpec,
    time_scope_origins: &std::collections::BTreeMap<TimeAverageScopeKey, f64>,
) -> AveragingPointKey {
    let time_key = if let Some(avgtime) = spec.averaging.avgtime {
        let scope = build_time_average_scope_key(
            inputs.field_id,
            inputs.scan_number,
            inputs.spw_id,
            inputs.baseline.clone(),
            spec,
        );
        let origin = time_scope_origins
            .get(&scope)
            .copied()
            .unwrap_or(inputs.row_time_value);
        let bucket = ((inputs.row_time_value - origin + AVG_TIME_BUCKET_EPSILON_SECONDS) / avgtime)
            .floor() as i64;
        AveragingTimeKey::Bucket(bucket)
    } else {
        AveragingTimeKey::Exact(inputs.row_time_value.to_bits())
    };
    AveragingPointKey {
        time_key,
        field_id: (!spec.averaging.avgfield).then_some(inputs.field_id),
        scan_number: (!spec.averaging.avgscan).then_some(inputs.scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(inputs.spw_id),
        baseline: inputs.baseline,
        corr_index: inputs.corr_index,
        chan_start: inputs.bin.start,
        chan_end: inputs.bin.end,
        chan_ordinal: inputs.bin.ordinal,
    }
}

fn push_numeric_axis_sample(accumulator: &mut AveragedPointAccumulator, axis: MsAxis, value: f64) {
    if let Some((_, samples)) = accumulator
        .numeric_axis_samples
        .iter_mut()
        .find(|(candidate, _)| *candidate == axis)
    {
        samples.push(value);
    } else {
        accumulator.numeric_axis_samples.push((axis, vec![value]));
    }
}

fn push_time_interval_sample(accumulator: &mut AveragedPointAccumulator, interval_seconds: f64) {
    accumulator.time_interval_samples.push(interval_seconds);
}

fn transform_visibility_samples_for_baseline_token(
    samples: &[Complex64],
    baseline_token: &AveragingBaselineToken,
    antenna1: i32,
    antenna2: i32,
) -> Vec<Complex64> {
    match baseline_token {
        AveragingBaselineToken::Antenna(target) if antenna1 != antenna2 && *target == antenna2 => {
            samples.iter().map(|value| value.conj()).collect()
        }
        _ => samples.to_vec(),
    }
}

fn finalize_averaged_axis_value(
    axis: MsAxis,
    accumulator: &AveragedPointAccumulator,
    scalar_average: bool,
    avgtime_enabled: bool,
) -> Option<f64> {
    if axis.is_visibility_math() {
        return compute_weighted_visibility_math(
            axis,
            &accumulator.visibility_samples,
            &accumulator.visibility_sample_weights,
            scalar_average,
        );
    }
    match axis {
        MsAxis::Time => {
            let samples = accumulator
                .numeric_axis_samples
                .iter()
                .find(|(candidate, _)| *candidate == axis)
                .map(|(_, samples)| samples)?;
            if avgtime_enabled {
                if samples.len() == 1 {
                    return Some(
                        samples[0]
                            - average_float_samples(&accumulator.time_interval_samples)? / 2.0,
                    );
                }
                let min_time = samples.iter().copied().min_by(f64::total_cmp)?;
                let max_time = samples.iter().copied().max_by(f64::total_cmp)?;
                return Some(min_time + (max_time - min_time) / 2.0);
            }
            average_float_samples(samples)
        }
        MsAxis::Flag => accumulator
            .has_flag_samples
            .then_some(if accumulator.any_flag_sample {
                1.0
            } else {
                0.0
            }),
        MsAxis::FlagRow => accumulator
            .has_flag_row_samples
            .then_some(if accumulator.any_flag_row { 1.0 } else { 0.0 }),
        _ => accumulator
            .numeric_axis_samples
            .iter()
            .find(|(candidate, _)| *candidate == axis)
            .and_then(|(_, samples)| average_float_samples(samples)),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn build_generic_visibility_scatter_with_averaging(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    let listobs_options = selection.to_summary_options();
    let row_numbers = resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?;

    let needs_visibility_grid = spec.x_axis.is_visibility_math()
        || spec.y_axes.iter().copied().any(MsAxis::is_visibility_math);
    let needs_spectral_coordinates = spec.x_axis.uses_spectral_coordinates()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_spectral_coordinates);
    let requested_freqframe = parse_frequency_frame(spec.transforms.freqframe.as_deref())?;
    let data_source = if needs_visibility_grid {
        Some(PreparedSelectedDataSource::new(
            ms,
            spec.data_column,
            &row_numbers,
        )?)
    } else {
        None
    };
    let selected_flags = SelectedArrayColumn::load(ms.main_table(), "FLAG", &row_numbers)?;
    let selected_weights = SelectedArrayColumn::load(ms.main_table(), "WEIGHT", &row_numbers)?;
    let selected_sigmas = SelectedArrayColumn::load(ms.main_table(), "SIGMA", &row_numbers)?;
    let selected_flag_rows = selected_bool_values(ms.main_table(), "FLAG_ROW", &row_numbers)?;
    let selected_intervals = selected_f64_values(ms.main_table(), "INTERVAL", &row_numbers)?;
    let selected_data_desc_ids =
        selected_i32_values(ms.main_table(), "DATA_DESC_ID", &row_numbers)?;
    let selected_field_ids = selected_i32_values(ms.main_table(), "FIELD_ID", &row_numbers)?;
    let selected_scan_numbers = selected_i32_values(ms.main_table(), "SCAN_NUMBER", &row_numbers)?;
    let selected_antenna1 = selected_i32_values(ms.main_table(), "ANTENNA1", &row_numbers)?;
    let selected_antenna2 = selected_i32_values(ms.main_table(), "ANTENNA2", &row_numbers)?;
    let selected_times = selected_f64_values(ms.main_table(), "TIME", &row_numbers)?;
    let selected_uvw = selected_uvw_values(ms.main_table(), "UVW", &row_numbers)?;
    let derived_engine = if spec.x_axis.uses_derived_geometry()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_derived_geometry)
        || (needs_spectral_coordinates && requested_freqframe.is_some())
    {
        Some(MsCalEngine::new(ms).map_err(|error| error.to_string())?)
    } else {
        None
    };
    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    let chan_freq = if needs_spectral_coordinates {
        Some(ChanFreqColumn::new(spectral_window.table()))
    } else {
        None
    };

    let requested_corr_codes = selection
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let iteraxis = spec.iteration.iteraxis;
    let include_row_flagged_points =
        spec.x_axis.uses_flag_rows() || spec.y_axes.iter().copied().any(MsAxis::uses_flag_rows);
    let needs_weight_spectrum = matches!(spec.x_axis, MsAxis::WeightSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::WeightSpectrum));
    let needs_sigma_spectrum = matches!(spec.x_axis, MsAxis::SigmaSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::SigmaSpectrum));
    let selected_weight_spectrum = if needs_weight_spectrum {
        WeightSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "WEIGHT_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    let selected_sigma_spectrum = if needs_sigma_spectrum {
        SigmaSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "SIGMA_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    let use_shared_time_scope_midpoint = spec.averaging.avgtime.is_some()
        && (spec.averaging.avgfield || spec.averaging.avgscan || spec.averaging.avgspw);

    let mut time_scope_origins = std::collections::BTreeMap::<TimeAverageScopeKey, f64>::new();
    let mut shared_time_scope_bounds =
        std::collections::BTreeMap::<SharedTimeAverageScopeKey, (f64, f64)>::new();
    if spec.averaging.avgtime.is_some() {
        for (row_slot, _row) in row_numbers.iter().copied().enumerate() {
            let flag_row_value = selected_flag_rows[row_slot];
            if flag_row_value && !include_row_flagged_points {
                continue;
            }
            let ddid = selected_data_desc_ids[row_slot];
            if ddid < 0 || (ddid as usize) >= data_description.row_count() {
                continue;
            }
            let ddid = ddid as usize;
            let spw_id = data_description
                .spectral_window_id(ddid)
                .map_err(|error| error.to_string())?;
            let field_id_value = selected_field_ids[row_slot];
            let scan_number_value = selected_scan_numbers[row_slot];
            let antenna1_value = selected_antenna1[row_slot];
            let antenna2_value = selected_antenna2[row_slot];
            let row_time_value = selected_times[row_slot];
            if use_shared_time_scope_midpoint {
                let shared_scope = build_shared_time_average_scope_key(
                    field_id_value,
                    scan_number_value,
                    spw_id,
                    spec,
                );
                shared_time_scope_bounds
                    .entry(shared_scope)
                    .and_modify(|(min_time, max_time)| {
                        *min_time = min_time.min(row_time_value);
                        *max_time = max_time.max(row_time_value);
                    })
                    .or_insert((row_time_value, row_time_value));
            }
            for baseline_token in averaging_baseline_tokens(antenna1_value, antenna2_value, spec) {
                let scope = build_time_average_scope_key(
                    field_id_value,
                    scan_number_value,
                    spw_id,
                    baseline_token,
                    spec,
                );
                time_scope_origins
                    .entry(scope)
                    .and_modify(|origin| {
                        if row_time_value < *origin {
                            *origin = row_time_value;
                        }
                    })
                    .or_insert(row_time_value);
            }
        }
    }

    let mut panel_order = Vec::<String>::new();
    let mut panels = std::collections::BTreeMap::<String, AveragedPanelAccumulator>::new();
    let mut contributing_rows = std::collections::BTreeSet::<usize>::new();

    for (row_slot, row) in row_numbers.iter().copied().enumerate() {
        let flag_row_value = selected_flag_rows[row_slot];
        if flag_row_value && !include_row_flagged_points {
            continue;
        }

        let ddid = selected_data_desc_ids[row_slot];
        if ddid < 0 || (ddid as usize) >= data_description.row_count() {
            continue;
        }
        let ddid = ddid as usize;
        let spw_id = data_description
            .spectral_window_id(ddid)
            .map_err(|error| error.to_string())?;
        let pol_id = data_description
            .polarization_id(ddid)
            .map_err(|error| error.to_string())?;
        let corr_types = if pol_id >= 0 && (pol_id as usize) < polarization.row_count() {
            polarization
                .corr_type(pol_id as usize)
                .map_err(|error| error.to_string())?
        } else {
            Vec::new()
        };

        let flags = match selected_flags.get(row_slot)? {
            ArrayValue::Bool(values) => {
                values.view().into_dimensionality::<Ix2>().map_err(|_| {
                    "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
                })?
            }
            other => {
                return Err(format!(
                    "msexplore requires BOOL flag cells, found {:?}",
                    other.primitive_type()
                ));
            }
        };
        let grid = data_source
            .as_ref()
            .map(|source| source.row(row_slot))
            .transpose()?;
        let (corr_count, chan_count) = grid
            .as_ref()
            .map(|grid| (grid.corr_count, grid.chan_count))
            .unwrap_or((flags.nrows(), flags.ncols()));
        if flags.shape() != [corr_count, chan_count] {
            return Err(format!(
                "visibility flag shape {:?} does not match data shape [{}, {}]",
                flags.shape(),
                corr_count,
                chan_count
            ));
        }
        let weight_values =
            float_axis_values(selected_weights.get(row_slot)?, corr_count, "WEIGHT")?;
        let sigma_values = float_axis_values(selected_sigmas.get(row_slot)?, corr_count, "SIGMA")?;
        let weight_spectrum_grid = if needs_weight_spectrum {
            Some(
                selected_weight_spectrum
                    .as_ref()
                    .map(|column| float_grid_from_array(column.get(row_slot)?, "WEIGHT_SPECTRUM"))
                    .transpose()?
                    .unwrap_or_else(|| scalar_values_to_grid(&weight_values, chan_count)),
            )
        } else {
            None
        };
        let sigma_spectrum_grid = if needs_sigma_spectrum {
            Some(
                selected_sigma_spectrum
                    .as_ref()
                    .map(|column| float_grid_from_array(column.get(row_slot)?, "SIGMA_SPECTRUM"))
                    .transpose()?
                    .unwrap_or_else(|| scalar_values_to_grid(&sigma_values, chan_count)),
            )
        } else {
            None
        };
        for (column_name, grid) in [
            ("WEIGHT_SPECTRUM", weight_spectrum_grid.as_ref()),
            ("SIGMA_SPECTRUM", sigma_spectrum_grid.as_ref()),
        ] {
            if let Some(grid) = grid
                && (grid.corr_count != corr_count || grid.chan_count != chan_count)
            {
                return Err(format!(
                    "{column_name} shape [{}, {}] does not match data shape [{corr_count}, {chan_count}]",
                    grid.corr_count, grid.chan_count
                ));
            }
        }

        let selected_correlations =
            select_correlation_slots(corr_count, &corr_types, requested_corr_codes.as_deref());
        if selected_correlations.is_empty() {
            continue;
        }
        let correlation_required = spec.x_axis.uses_correlation_slots()
            || spec
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::uses_correlation_slots)
            || matches!(spec.color_by, MsColorAxis::Correlation)
            || matches!(iteraxis, Some(MsIterationAxis::Correlation));
        let selected_correlations = if correlation_required {
            selected_correlations
        } else {
            vec![selected_correlations[0].clone()]
        };

        let channel_bins = plot_channel_bins(chan_count, spec)?;

        let field_id_value = selected_field_ids[row_slot];
        let scan_number_value = selected_scan_numbers[row_slot];
        let antenna1_value = selected_antenna1[row_slot];
        let antenna2_value = selected_antenna2[row_slot];
        let row_time_value = selected_times[row_slot];
        let row_uvw_value = selected_uvw[row_slot];
        let row_interval_value = selected_intervals[row_slot];
        let shared_time_scope_value = if use_shared_time_scope_midpoint {
            let shared_scope = build_shared_time_average_scope_key(
                field_id_value,
                scan_number_value,
                spw_id,
                spec,
            );
            shared_time_scope_bounds
                .get(&shared_scope)
                .map(|(min_time, max_time)| min_time + (max_time - min_time) / 2.0)
        } else {
            None
        };
        let spectral_context = resolve_spectral_context(
            spec,
            spw_id,
            chan_count,
            field_id_value,
            row_time_value,
            chan_freq.as_ref(),
            &spectral_window,
            derived_engine.as_ref(),
        )?;

        let baseline_tokens = averaging_baseline_tokens(antenna1_value, antenna2_value, spec);
        let mut row_panels = std::collections::BTreeSet::<String>::new();
        for (corr_index, corr_label) in &selected_correlations {
            let row_weight = weight_values[*corr_index];
            let row_sigma = sigma_values[*corr_index];
            let mut samples = Vec::<Complex64>::new();
            let mut flag_samples = Vec::<bool>::new();
            let mut visibility_weight_samples = Vec::<f64>::new();
            let mut weight_spectrum_samples = Vec::<f64>::new();
            let mut sigma_spectrum_samples = Vec::<f64>::new();
            for baseline_token in &baseline_tokens {
                for bin in &channel_bins {
                    if let Some(grid) = grid.as_ref() {
                        collect_bin_samples_into(
                            grid,
                            &flags,
                            &[*corr_index],
                            bin.start,
                            bin.end,
                            &mut samples,
                        );
                    } else {
                        samples.clear();
                    }
                    collect_bin_flags_into(
                        &flags,
                        *corr_index,
                        bin.start,
                        bin.end,
                        &mut flag_samples,
                    );
                    visibility_weight_samples.clear();
                    visibility_weight_samples.resize(samples.len(), row_weight);
                    if let Some(grid) = weight_spectrum_grid.as_ref() {
                        collect_bin_float_samples_into(
                            grid,
                            &flags,
                            *corr_index,
                            bin.start,
                            bin.end,
                            &mut weight_spectrum_samples,
                        );
                    } else {
                        weight_spectrum_samples.clear();
                    }
                    if let Some(grid) = sigma_spectrum_grid.as_ref() {
                        collect_bin_float_samples_into(
                            grid,
                            &flags,
                            *corr_index,
                            bin.start,
                            bin.end,
                            &mut sigma_spectrum_samples,
                        );
                    } else {
                        sigma_spectrum_samples.clear();
                    }
                    let (group_key, group_label) = visibility_group(
                        spec.color_by,
                        field_id_value,
                        &field,
                        spw_id,
                        &spectral_window,
                        scan_number_value,
                        antenna1_value,
                        antenna2_value,
                        Some(corr_label),
                    );
                    let (panel_key, panel_label) = iteration_group(
                        iteraxis,
                        field_id_value,
                        &field,
                        spw_id,
                        &spectral_window,
                        scan_number_value,
                        Some(corr_label),
                    );
                    if !panels.contains_key(&panel_key) {
                        panel_order.push(panel_key.clone());
                        panels.insert(
                            panel_key.clone(),
                            AveragedPanelAccumulator {
                                label: panel_label.clone(),
                                ..Default::default()
                            },
                        );
                    }
                    let point_key = build_averaging_point_key(
                        AveragingPointKeyInputs {
                            field_id: field_id_value,
                            scan_number: scan_number_value,
                            spw_id,
                            baseline: baseline_token.clone(),
                            corr_index: *corr_index,
                            row_time_value,
                            bin,
                        },
                        spec,
                        &time_scope_origins,
                    );
                    let panel = panels
                        .get_mut(&panel_key)
                        .expect("panel inserted before mutation");
                    let mut panel_used = false;
                    for y_axis in spec.y_axes.iter().copied() {
                        let (series_key, series_label, color_group) = scatter_series_identity(
                            y_axis,
                            spec.y_axes.len() > 1,
                            &group_key,
                            &group_label,
                        );
                        let series = panel.series.entry(series_key).or_insert_with(|| {
                            AveragedSeriesAccumulator {
                                label: series_label,
                                color_group,
                                y_axis,
                                ..Default::default()
                            }
                        });
                        let accumulator = series.points.entry(point_key.clone()).or_default();
                        if accumulator.representative.is_none() {
                            accumulator.representative = Some(MsScatterPointRef {
                                row,
                                corr: *corr_index,
                                chan_start: bin.start,
                                chan_end: bin.end,
                            });
                        }

                        let mut series_used = false;
                        if (spec.x_axis.is_visibility_math() || y_axis.is_visibility_math())
                            && !samples.is_empty()
                        {
                            let transformed_samples =
                                transform_visibility_samples_for_baseline_token(
                                    &samples,
                                    baseline_token,
                                    antenna1_value,
                                    antenna2_value,
                                );
                            accumulator
                                .visibility_sample_weights
                                .extend(visibility_weight_samples.iter().copied());
                            accumulator.visibility_samples.extend(transformed_samples);
                            series_used = true;
                        }
                        match spec.x_axis {
                            MsAxis::Flag => {
                                accumulator.has_flag_samples = true;
                                accumulator.any_flag_sample |=
                                    flag_samples.iter().any(|flag| *flag);
                                series_used = true;
                            }
                            MsAxis::FlagRow => {
                                accumulator.has_flag_row_samples = true;
                                accumulator.any_flag_row |= flag_row_value;
                                series_used = true;
                            }
                            axis if !axis.is_visibility_math() => {
                                if let Some(value) = compute_axis_value(
                                    axis,
                                    &samples,
                                    &flag_samples,
                                    &weight_spectrum_samples,
                                    &sigma_spectrum_samples,
                                    flag_row_value,
                                    row_weight,
                                    row_sigma,
                                    field_id_value,
                                    antenna1_value,
                                    spec.averaging.scalar,
                                    bin,
                                    spectral_context.as_ref(),
                                    row_time_value,
                                    row_uvw_value,
                                    derived_engine.as_ref(),
                                )? {
                                    let value = if axis == MsAxis::Time {
                                        shared_time_scope_value.unwrap_or(value)
                                    } else {
                                        value
                                    };
                                    push_numeric_axis_sample(accumulator, axis, value);
                                    if axis == MsAxis::Time {
                                        push_time_interval_sample(accumulator, row_interval_value);
                                    }
                                    series_used = true;
                                }
                            }
                            _ => {}
                        }
                        match y_axis {
                            MsAxis::Flag => {
                                accumulator.has_flag_samples = true;
                                accumulator.any_flag_sample |=
                                    flag_samples.iter().any(|flag| *flag);
                                series_used = true;
                            }
                            MsAxis::FlagRow => {
                                accumulator.has_flag_row_samples = true;
                                accumulator.any_flag_row |= flag_row_value;
                                series_used = true;
                            }
                            axis if !axis.is_visibility_math() => {
                                if let Some(value) = compute_axis_value(
                                    axis,
                                    &samples,
                                    &flag_samples,
                                    &weight_spectrum_samples,
                                    &sigma_spectrum_samples,
                                    flag_row_value,
                                    row_weight,
                                    row_sigma,
                                    field_id_value,
                                    antenna1_value,
                                    spec.averaging.scalar,
                                    bin,
                                    spectral_context.as_ref(),
                                    row_time_value,
                                    row_uvw_value,
                                    derived_engine.as_ref(),
                                )? {
                                    let value = if axis == MsAxis::Time {
                                        shared_time_scope_value.unwrap_or(value)
                                    } else {
                                        value
                                    };
                                    push_numeric_axis_sample(accumulator, axis, value);
                                    if axis == MsAxis::Time {
                                        push_time_interval_sample(accumulator, row_interval_value);
                                    }
                                    series_used = true;
                                }
                            }
                            _ => {}
                        }
                        if series_used {
                            panel_used = true;
                        }
                    }
                    if panel_used {
                        row_panels.insert(panel_key.clone());
                    }
                }
            }
        }

        for panel_key in row_panels {
            if let Some(panel) = panels.get_mut(&panel_key) {
                panel.contributing_rows.insert(row);
            }
            contributing_rows.insert(row);
        }
    }

    let title = spec
        .style
        .title
        .clone()
        .or_else(|| spec.preset.map(|preset| preset.display_name().to_string()))
        .unwrap_or_else(|| {
            format!(
                "{} vs {}",
                spec.y_axis().display_name(),
                spec.x_axis.display_name()
            )
        });
    let x_label = spec
        .style
        .xlabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.x_axis));
    let y_label = spec
        .style
        .ylabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.y_axis()));
    let secondary_y_label = spec.secondary_y_axis().map(axis_label);
    let fixed_x_bounds = fixed_bounds(spec.x_axis);
    let fixed_y_bounds = fixed_bounds(spec.y_axis());
    let secondary_fixed_y_bounds = spec.secondary_y_axis().and_then(fixed_bounds);

    let mut contributing_points = 0usize;
    let mut finalized_panels = Vec::new();
    for panel_key in panel_order {
        let Some(panel) = panels.remove(&panel_key) else {
            continue;
        };
        let mut series_entries = Vec::new();
        for series in panel.series.into_values() {
            let mut output = MsScatterSeries {
                label: series.label,
                color_group: series.color_group,
                y_axis: series.y_axis,
                points: Vec::new(),
                provenance: Vec::new(),
            };
            for accumulator in series.points.into_values() {
                let Some(representative) = accumulator.representative.clone() else {
                    continue;
                };
                let Some(x_value) = finalize_averaged_axis_value(
                    spec.x_axis,
                    &accumulator,
                    spec.averaging.scalar,
                    spec.averaging.avgtime.is_some(),
                ) else {
                    continue;
                };
                let Some(y_value) = finalize_averaged_axis_value(
                    series.y_axis,
                    &accumulator,
                    spec.averaging.scalar,
                    spec.averaging.avgtime.is_some(),
                ) else {
                    continue;
                };
                output.points.push((x_value, y_value));
                output.provenance.push(representative);
                point_budget.record_points(
                    1,
                    spec.preset
                        .map(MsPlotPreset::display_name)
                        .unwrap_or("Requested plot"),
                )?;
            }
            output
                .points
                .sort_by(|left, right| left.0.total_cmp(&right.0));
            if !output.points.is_empty() {
                series_entries.push(output);
            }
        }
        if series_entries.is_empty() {
            continue;
        }
        let panel_point_count = series_entries
            .iter()
            .map(|entry| entry.points.len())
            .sum::<usize>();
        contributing_points += panel_point_count;
        finalized_panels.push(MsScatterPanelPayload {
            key: panel_key,
            summary: format!(
                "{}. Rows={} Points={} Data column={}",
                panel.label,
                panel.contributing_rows.len(),
                panel_point_count,
                spec.data_column
            ),
            label: panel.label,
            series: series_entries,
        });
    }

    if contributing_points == 0 {
        return Err(format!(
            "{} produced no unflagged visibility points for the current selection",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("Requested plot")
        ));
    }

    if let Some(iteraxis) = iteraxis {
        let (gridrows, gridcols) = resolve_iterated_grid(
            finalized_panels.len(),
            spec.layout.gridrows,
            spec.layout.gridcols,
        )?;
        let share_x_bounds =
            share_axis_bounds(spec.iteration.xselfscale, spec.iteration.xsharedaxis);
        let share_y_bounds =
            share_axis_bounds(spec.iteration.yselfscale, spec.iteration.ysharedaxis);
        return Ok(MsPlotPayload::ScatterGrid(MsScatterGridPayload {
            title,
            x_axis: spec.x_axis,
            y_axis: spec.y_axis(),
            x_label,
            y_label,
            fixed_x_bounds,
            fixed_y_bounds,
            showlegend: spec.style.showlegend,
            legend_position: spec.style.legendposition,
            showmajorgrid: spec.style.showmajorgrid,
            showminorgrid: spec.style.showminorgrid,
            symbol_size_px: spec.style.symbol_size,
            iteraxis,
            gridrows,
            gridcols,
            share_x_bounds,
            share_y_bounds,
            header_lines: Vec::new(),
            summary: format!(
                "{}. Panels={} Rows={} Points={} Data column={}",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("MeasurementSet iterated plot"),
                finalized_panels.len(),
                contributing_rows.len(),
                contributing_points,
                spec.data_column
            ),
            panels: finalized_panels,
        }));
    }

    let panel = finalized_panels
        .pop()
        .ok_or_else(|| "msexplore scatter payload lost its only panel".to_string())?;
    Ok(MsPlotPayload::Scatter(MsScatterPlotPayload {
        title,
        x_axis: spec.x_axis,
        y_axis: spec.y_axis(),
        secondary_y_axis: spec.secondary_y_axis(),
        x_label,
        y_label,
        secondary_y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        secondary_fixed_y_bounds,
        showlegend: spec.style.showlegend,
        legend_position: spec.style.legendposition,
        showmajorgrid: spec.style.showmajorgrid,
        showminorgrid: spec.style.showminorgrid,
        symbol_size_px: spec.style.symbol_size,
        header_lines: Vec::new(),
        summary: format!(
            "{}. Rows={} Points={} Data column={}",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows.len(),
            contributing_points,
            spec.data_column
        ),
        series: panel.series,
    }))
}

fn build_stacked_amplitude_phase_time_page(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    if spec.iteration.iteraxis.is_some() {
        return Err(
            "msexplore stacked paired presets do not yet support nested iteraxis pages".to_string(),
        );
    }
    if spec.secondary_y_axis().is_some() {
        return Err(
            "msexplore stacked paired presets already define their subplot pairing".to_string(),
        );
    }
    if spec.layout.gridrows != 1
        || spec.layout.gridcols != 1
        || spec.layout.rowindex != 0
        || spec.layout.colindex != 0
        || spec.layout.plotindex != 0
    {
        return Err(
            "msexplore stacked paired presets currently manage their own fixed two-row page layout"
                .to_string(),
        );
    }

    let page_title = spec.style.title.clone().unwrap_or_else(|| {
        MsPlotPreset::AmplitudePhaseVsTimeStacked
            .display_name()
            .to_string()
    });
    let amplitude = build_stacked_page_child(
        ms,
        selection,
        spec,
        MsPlotPreset::AmplitudeVsTime,
        MsAxis::Amplitude,
        "Amplitude vs Time",
        point_budget,
    )?;
    let phase = build_stacked_page_child(
        ms,
        selection,
        spec,
        MsPlotPreset::PhaseVsTime,
        MsAxis::Phase,
        "Phase vs Time",
        point_budget,
    )?;

    Ok(MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title: page_title,
        exprange: MsPageExportRange::Current,
        gridrows: 2,
        gridcols: 1,
        header_lines: Vec::new(),
        summary: format!(
            "{}. Plots=2 Data column={}",
            MsPlotPreset::AmplitudePhaseVsTimeStacked.display_name(),
            spec.data_column
        ),
        items: vec![
            MsScatterPageItemPayload {
                plotindex: 0,
                rowindex: 0,
                colindex: 0,
                plot: amplitude,
            },
            MsScatterPageItemPayload {
                plotindex: 1,
                rowindex: 1,
                colindex: 0,
                plot: phase,
            },
        ],
    }))
}

fn build_stacked_page_child(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    preset: MsPlotPreset,
    y_axis: MsAxis,
    title: &str,
    point_budget: &mut PointBudget,
) -> Result<MsScatterPlotPayload, String> {
    let mut child = MsPlotSpec::from_preset(preset);
    child.data_column = spec.data_column;
    child.color_by = spec.color_by;
    child.averaging = spec.averaging.clone();
    child.transforms = spec.transforms.clone();
    child.style = spec.style.clone();
    child.style.title = Some(title.to_string());
    child.style.ylabel = None;
    child.y_axes = vec![y_axis];
    match build_generic_visibility_scatter(ms, selection, &child, point_budget)? {
        MsPlotPayload::Scatter(payload) => Ok(payload),
        _ => Err("internal error: stacked subplot lowered to a non-scatter payload".to_string()),
    }
}

fn resolve_selected_rows_with_msselect(
    ms: &MeasurementSet,
    selection: &MsSelection,
    _listobs_options: &ListObsOptions,
) -> Result<Vec<usize>, String> {
    if !selection.selectdata {
        return Ok((0..ms.row_count()).collect());
    }
    resolve_row_indices_with_system_budget(ms, selection).map_err(|error| error.to_string())
}

#[derive(Debug, Clone, Copy)]
struct ChannelBin {
    start: usize,
    end: usize,
    ordinal: usize,
}

fn channel_bins(chan_count: usize, avgchannel: Option<usize>) -> Result<Vec<ChannelBin>, String> {
    if chan_count == 0 {
        return Ok(Vec::new());
    }
    let width = avgchannel.unwrap_or(1);
    if width == 0 {
        return Err("avgchannel must be a positive integer".to_string());
    }
    let mut bins = Vec::new();
    let mut ordinal = 0usize;
    let mut start = 0usize;
    while start < chan_count {
        let end = start + width;
        if end > chan_count {
            if avgchannel.is_some() {
                if start == 0 {
                    bins.push(ChannelBin {
                        start,
                        end: chan_count,
                        ordinal,
                    });
                }
                break;
            }
            bins.push(ChannelBin {
                start,
                end: chan_count,
                ordinal,
            });
            break;
        }
        bins.push(ChannelBin {
            start,
            end,
            ordinal,
        });
        start = end;
        ordinal += 1;
    }
    Ok(bins)
}

fn plot_channel_bins(chan_count: usize, spec: &MsPlotSpec) -> Result<Vec<ChannelBin>, String> {
    if spec.x_axis.uses_channel_bins() || spec.y_axes.iter().copied().any(MsAxis::uses_channel_bins)
    {
        channel_bins(chan_count, spec.averaging.avgchannel)
    } else if chan_count == 0 {
        Ok(Vec::new())
    } else {
        Ok(vec![ChannelBin {
            start: 0,
            end: chan_count,
            ordinal: 0,
        }])
    }
}

fn fixed_bounds(axis: MsAxis) -> Option<(f64, f64)> {
    matches!(axis, MsAxis::Phase).then_some((-180.0, 180.0))
}

fn axis_label(axis: MsAxis) -> String {
    match axis {
        MsAxis::Time => "Time (MJD seconds)".to_string(),
        MsAxis::UvDistance => "UV Distance (m)".to_string(),
        MsAxis::U => "U (m)".to_string(),
        MsAxis::V => "V (m)".to_string(),
        MsAxis::W => "W (m)".to_string(),
        MsAxis::Channel => "Channel".to_string(),
        MsAxis::Frequency => "Frequency (Hz)".to_string(),
        MsAxis::Velocity => "Velocity (km/s)".to_string(),
        MsAxis::Azimuth => "Azimuth (deg)".to_string(),
        MsAxis::Elevation => "Elevation (deg)".to_string(),
        MsAxis::HourAngle => "Hour Angle (hours)".to_string(),
        MsAxis::ParallacticAngle => "Parallactic Angle (deg)".to_string(),
        MsAxis::Amplitude => "Amplitude".to_string(),
        MsAxis::Phase => "Phase (deg)".to_string(),
        MsAxis::Real => "Real".to_string(),
        MsAxis::Imaginary => "Imaginary".to_string(),
        MsAxis::Weight => "Weight".to_string(),
        MsAxis::Sigma => "Sigma".to_string(),
        MsAxis::WeightSpectrum => "Weight Spectrum".to_string(),
        MsAxis::SigmaSpectrum => "Sigma Spectrum".to_string(),
        MsAxis::Flag => "Flag".to_string(),
        MsAxis::FlagRow => "Flag Row".to_string(),
        _ => axis.display_name().to_string(),
    }
}

fn float_axis_values(
    array: &ArrayValue,
    expected_len: usize,
    column_name: &str,
) -> Result<Vec<f64>, String> {
    let values = match array {
        ArrayValue::Float32(values) => values
            .view()
            .into_dimensionality::<Ix1>()
            .map_err(|_| format!("msexplore expects {column_name} cells with shape [num_corr]"))?
            .iter()
            .map(|value| *value as f64)
            .collect::<Vec<_>>(),
        ArrayValue::Float64(values) => values
            .view()
            .into_dimensionality::<Ix1>()
            .map_err(|_| format!("msexplore expects {column_name} cells with shape [num_corr]"))?
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        other => {
            return Err(format!(
                "msexplore requires FLOAT {column_name} cells, found {:?}",
                other.primitive_type()
            ));
        }
    };
    if values.len() != expected_len {
        return Err(format!(
            "{column_name} shape [{}] does not match correlation count {expected_len}",
            values.len()
        ));
    }
    Ok(values)
}

#[derive(Debug, Clone)]
struct SpectralContext {
    channel_frequencies_hz: Vec<f64>,
    rest_frequency_hz: f64,
    doppler_ref: DopplerRef,
}

fn parse_frequency_frame(value: Option<&str>) -> Result<Option<FrequencyRef>, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<FrequencyRef>()
                .map_err(|error| error.to_string())
        })
        .transpose()
}

fn parse_velocity_definition(value: &str) -> Result<DopplerRef, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "RADIO" => Ok(DopplerRef::RADIO),
        "OPTICAL" | "Z" => Ok(DopplerRef::Z),
        "TRUE" | "RELATIVISTIC" | "BETA" => Ok(DopplerRef::BETA),
        "RATIO" => Ok(DopplerRef::RATIO),
        "GAMMA" => Ok(DopplerRef::GAMMA),
        other => Err(format!(
            "unsupported msexplore velocity definition {other:?}"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_spectral_context(
    spec: &MsPlotSpec,
    spw_id: i32,
    chan_count: usize,
    field_id_value: i32,
    row_time_value: f64,
    chan_freq: Option<&ChanFreqColumn<'_>>,
    spectral_window: &crate::subtables::MsSpectralWindow<'_>,
    derived_engine: Option<&MsCalEngine>,
) -> Result<Option<SpectralContext>, String> {
    let needs_spectral_coordinates = spec.x_axis.uses_spectral_coordinates()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_spectral_coordinates);
    if !needs_spectral_coordinates {
        return Ok(None);
    }

    let spw_index = usize::try_from(spw_id)
        .map_err(|_| format!("invalid DATA_DESCRIPTION::SPECTRAL_WINDOW_ID {spw_id}"))?;
    let chan_freq = chan_freq.ok_or_else(|| {
        "internal error: missing channel-frequency column for spectral axis".to_string()
    })?;
    let source_channel_frequencies = chan_freq
        .get_frequencies(spw_index)
        .map_err(|error| error.to_string())?;
    if source_channel_frequencies.len() != chan_count {
        return Err(format!(
            "SPECTRAL_WINDOW row {spw_id} reported {} channel frequencies for {} data channels",
            source_channel_frequencies.len(),
            chan_count
        ));
    }

    let source_ref = FrequencyRef::from_casacore_code(
        spectral_window
            .meas_freq_ref(spw_index)
            .map_err(|error| error.to_string())?,
    )
    .ok_or_else(|| format!("unsupported MEAS_FREQ_REF code for SPECTRAL_WINDOW row {spw_id}"))?;
    let target_ref =
        parse_frequency_frame(spec.transforms.freqframe.as_deref())?.unwrap_or(source_ref);
    let channel_frequencies_hz = if target_ref == source_ref {
        source_channel_frequencies
            .iter()
            .map(MFrequency::hz)
            .collect::<Vec<_>>()
    } else {
        let derived_engine = derived_engine.ok_or_else(|| {
            "internal error: missing derived engine for spectral-frame conversion".to_string()
        })?;
        let field_id = usize::try_from(field_id_value)
            .map_err(|_| format!("invalid FIELD_ID {field_id_value} for spectral transform"))?;
        source_channel_frequencies
            .iter()
            .map(|frequency| {
                convert_frequency_to_frame(
                    source_ref,
                    target_ref,
                    frequency.hz(),
                    row_time_value,
                    field_id,
                    derived_engine,
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let center_frequency_hz =
        channel_frequencies_hz.iter().copied().sum::<f64>() / channel_frequencies_hz.len() as f64;
    let rest_frequency_hz = spec
        .transforms
        .restfreq
        .as_deref()
        .map(parse_rest_frequency_hz)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or(center_frequency_hz);
    let doppler_ref = parse_velocity_definition(&spec.transforms.veldef)?;
    Ok(Some(SpectralContext {
        channel_frequencies_hz,
        rest_frequency_hz,
        doppler_ref,
    }))
}

#[allow(clippy::too_many_arguments)]
fn compute_axis_value(
    axis: MsAxis,
    samples: &[Complex64],
    flag_samples: &[bool],
    weight_spectrum_samples: &[f64],
    sigma_spectrum_samples: &[f64],
    flag_row_value: bool,
    row_weight: f64,
    row_sigma: f64,
    field_id_value: i32,
    antenna1_value: i32,
    scalar_average: bool,
    channel_bin: &ChannelBin,
    spectral_context: Option<&SpectralContext>,
    row_time_value: f64,
    row_uvw_value: [f64; 3],
    geometry_engine: Option<&MsCalEngine>,
) -> Result<Option<f64>, String> {
    if axis.is_visibility_math() {
        return Ok(compute_visibility_math(axis, samples, scalar_average));
    }
    match axis {
        MsAxis::Time => Ok(Some(row_time_value)),
        MsAxis::UvDistance => {
            let [u, v, _w] = row_uvw_value;
            Ok(Some((u * u + v * v).sqrt()))
        }
        MsAxis::U => {
            let [u, _v, _w] = row_uvw_value;
            Ok(Some(u))
        }
        MsAxis::V => {
            let [_u, v, _w] = row_uvw_value;
            Ok(Some(v))
        }
        MsAxis::W => {
            let [_u, _v, w] = row_uvw_value;
            Ok(Some(w))
        }
        MsAxis::Channel => {
            if channel_bin.end.saturating_sub(channel_bin.start) == 1 {
                Ok(Some(channel_bin.start as f64))
            } else {
                Ok(Some(channel_bin.ordinal as f64))
            }
        }
        MsAxis::Frequency => {
            let Some(spectral_context) = spectral_context else {
                return Ok(None);
            };
            if spectral_context.channel_frequencies_hz.is_empty()
                || channel_bin.end > spectral_context.channel_frequencies_hz.len()
            {
                return Ok(None);
            }
            let bin = &spectral_context.channel_frequencies_hz[channel_bin.start..channel_bin.end];
            Ok(Some(bin.iter().copied().sum::<f64>() / bin.len() as f64))
        }
        MsAxis::Velocity => {
            let Some(spectral_context) = spectral_context else {
                return Ok(None);
            };
            if spectral_context.channel_frequencies_hz.is_empty()
                || channel_bin.end > spectral_context.channel_frequencies_hz.len()
            {
                return Ok(None);
            }
            let bin = &spectral_context.channel_frequencies_hz[channel_bin.start..channel_bin.end];
            let mean_frequency_hz = bin.iter().copied().sum::<f64>() / bin.len() as f64;
            Ok(Some(
                velocity_ms_from_frequency_hz(
                    mean_frequency_hz,
                    spectral_context.rest_frequency_hz,
                    spectral_context.doppler_ref,
                )
                .map_err(|error| error.to_string())?
                    / 1000.0,
            ))
        }
        MsAxis::Weight => Ok(Some(row_weight)),
        MsAxis::Sigma => Ok(Some(row_sigma)),
        MsAxis::WeightSpectrum => Ok(average_float_samples(weight_spectrum_samples)),
        MsAxis::SigmaSpectrum => Ok(average_float_samples(sigma_spectrum_samples)),
        MsAxis::Flag => Ok(compute_flag_value(flag_samples)),
        MsAxis::FlagRow => Ok(Some(if flag_row_value { 1.0 } else { 0.0 })),
        MsAxis::Azimuth => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for azimuth axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for azimuth axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for azimuth axis"))?;
            geometry_engine
                .azel(row_time_value, field_id, antenna_id)
                .map(|(azimuth, _elevation)| Some(normalize_signed_degrees(azimuth.to_degrees())))
                .map_err(|error| error.to_string())
        }
        MsAxis::Elevation => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for elevation axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for elevation axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for elevation axis"))?;
            geometry_engine
                .azel(row_time_value, field_id, antenna_id)
                .map(|(_azimuth, elevation)| Some(elevation.to_degrees()))
                .map_err(|error| error.to_string())
        }
        MsAxis::HourAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for hour angle axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for hour angle axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for hour angle axis"))?;
            geometry_engine
                .hour_angle(row_time_value, field_id, antenna_id)
                .map(|hour_angle| Some(hour_angle * 12.0 / std::f64::consts::PI))
                .map_err(|error| error.to_string())
        }
        MsAxis::ParallacticAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for parallactic angle axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value).map_err(|_| {
                format!("invalid FIELD_ID {field_id_value} for parallactic angle axis")
            })?;
            let antenna_id = usize::try_from(antenna1_value).map_err(|_| {
                format!("invalid ANTENNA1 {antenna1_value} for parallactic angle axis")
            })?;
            geometry_engine
                .parallactic_angle(row_time_value, field_id, antenna_id)
                .map(|parallactic_angle| Some(parallactic_angle.to_degrees()))
                .map_err(|error| error.to_string())
        }
        _ => Ok(None),
    }
}

fn average_float_samples(samples: &[f64]) -> Option<f64> {
    if samples.is_empty() {
        None
    } else {
        Some(samples.iter().copied().sum::<f64>() / samples.len() as f64)
    }
}

fn normalize_signed_degrees(angle_degrees: f64) -> f64 {
    let wrapped = (angle_degrees + 180.0).rem_euclid(360.0) - 180.0;
    if wrapped == -180.0 { 180.0 } else { wrapped }
}

fn compute_flag_value(flags: &[bool]) -> Option<f64> {
    if flags.is_empty() {
        None
    } else if flags.iter().any(|flag| *flag) {
        Some(1.0)
    } else {
        Some(0.0)
    }
}

fn compute_visibility_math(
    axis: MsAxis,
    samples: &[Complex64],
    scalar_average: bool,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if scalar_average {
        match axis {
            MsAxis::Amplitude => {
                Some(samples.iter().map(|value| value.norm()).sum::<f64>() / samples.len() as f64)
            }
            MsAxis::Phase => {
                let (sin_sum, cos_sum) =
                    samples
                        .iter()
                        .fold((0.0, 0.0), |(sin_sum, cos_sum), value| {
                            let angle = value.arg();
                            (sin_sum + angle.sin(), cos_sum + angle.cos())
                        });
                Some(sin_sum.atan2(cos_sum).to_degrees())
            }
            MsAxis::Real => {
                Some(samples.iter().map(|value| value.re).sum::<f64>() / samples.len() as f64)
            }
            MsAxis::Imaginary => {
                Some(samples.iter().map(|value| value.im).sum::<f64>() / samples.len() as f64)
            }
            _ => None,
        }
    } else {
        let average = samples.iter().copied().sum::<Complex64>() / samples.len() as f64;
        match axis {
            MsAxis::Amplitude => Some(average.norm()),
            MsAxis::Phase => Some(average.arg().to_degrees()),
            MsAxis::Real => Some(average.re),
            MsAxis::Imaginary => Some(average.im),
            _ => None,
        }
    }
}

fn compute_weighted_visibility_math(
    axis: MsAxis,
    samples: &[Complex64],
    weights: &[f64],
    scalar_average: bool,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if weights.len() != samples.len() || weights.is_empty() {
        return compute_visibility_math(axis, samples, scalar_average);
    }
    let weight_sum = weights.iter().sum::<f64>();
    if weight_sum <= 0.0 {
        return compute_visibility_math(axis, samples, scalar_average);
    }
    if scalar_average {
        match axis {
            MsAxis::Amplitude => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.norm() * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            MsAxis::Phase => {
                let (sin_sum, cos_sum) = samples.iter().zip(weights.iter()).fold(
                    (0.0, 0.0),
                    |(sin_sum, cos_sum), (value, weight)| {
                        let angle = value.arg();
                        (
                            sin_sum + angle.sin() * *weight,
                            cos_sum + angle.cos() * *weight,
                        )
                    },
                );
                Some(sin_sum.atan2(cos_sum).to_degrees())
            }
            MsAxis::Real => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.re * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            MsAxis::Imaginary => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.im * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            _ => None,
        }
    } else {
        let average = samples
            .iter()
            .zip(weights.iter())
            .map(|(value, weight)| *value * *weight)
            .sum::<Complex64>()
            / weight_sum;
        match axis {
            MsAxis::Amplitude => Some(average.norm()),
            MsAxis::Phase => Some(average.arg().to_degrees()),
            MsAxis::Real => Some(average.re),
            MsAxis::Imaginary => Some(average.im),
            _ => None,
        }
    }
}

fn collect_bin_samples_into(
    grid: &ComplexGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_indices: &[usize],
    chan_start: usize,
    chan_end: usize,
    samples: &mut Vec<Complex64>,
) {
    samples.clear();
    for &corr_index in corr_indices {
        if corr_index >= grid.corr_count {
            continue;
        }
        for chan_index in chan_start..chan_end {
            if chan_index >= grid.chan_count || flags[(corr_index, chan_index)] {
                continue;
            }
            let value = grid.values[corr_index * grid.chan_count + chan_index];
            if value.re.is_finite() && value.im.is_finite() {
                samples.push(value);
            }
        }
    }
}

fn collect_bin_flags_into(
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    chan_start: usize,
    chan_end: usize,
    out: &mut Vec<bool>,
) {
    out.clear();
    if corr_index >= flags.nrows() {
        return;
    }
    for chan_index in chan_start..chan_end {
        if chan_index < flags.ncols() {
            out.push(flags[(corr_index, chan_index)]);
        }
    }
}

fn collect_bin_float_samples_into(
    grid: &FloatGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    chan_start: usize,
    chan_end: usize,
    samples: &mut Vec<f64>,
) {
    samples.clear();
    if corr_index >= grid.corr_count {
        return;
    }
    for chan_index in chan_start..chan_end {
        if chan_index >= grid.chan_count
            || chan_index >= flags.ncols()
            || flags[(corr_index, chan_index)]
        {
            continue;
        }
        let value = grid.values[corr_index * grid.chan_count + chan_index];
        if value.is_finite() {
            samples.push(value);
        }
    }
}

fn select_correlation_slots(
    corr_count: usize,
    corr_types: &[i32],
    requested_corr_codes: Option<&[i32]>,
) -> Vec<(usize, String)> {
    let requested_corr_codes = requested_corr_codes.unwrap_or(&[]);
    let mut slots = Vec::new();
    for corr_index in 0..corr_count {
        let corr_label = corr_types
            .get(corr_index)
            .map(|code| listobs::stokes_name(*code).to_string())
            .unwrap_or_else(|| format!("corr-{corr_index}"));
        let corr_code = corr_types.get(corr_index).copied();
        if requested_corr_codes.is_empty()
            || corr_code.is_some_and(|code| requested_corr_codes.contains(&code))
        {
            slots.push((corr_index, corr_label));
        }
    }
    slots
}

#[allow(clippy::too_many_arguments)]
fn visibility_group(
    color_by: MsColorAxis,
    field_id: i32,
    field: &crate::subtables::MsField<'_>,
    spw_id: i32,
    spectral_window: &crate::subtables::MsSpectralWindow<'_>,
    scan_number: i32,
    antenna1: i32,
    antenna2: i32,
    correlation_label: Option<&str>,
) -> (String, String) {
    match color_by {
        MsColorAxis::None => ("all".to_string(), "All data".to_string()),
        MsColorAxis::Field => {
            let field_name = if field_id >= 0 && (field_id as usize) < field.row_count() {
                field
                    .name(field_id as usize)
                    .unwrap_or_else(|_| format!("FIELD {field_id}"))
            } else {
                format!("FIELD {field_id}")
            };
            (format!("field-{field_id}"), field_name)
        }
        MsColorAxis::Scan => (format!("scan-{scan_number}"), format!("Scan {scan_number}")),
        MsColorAxis::SpectralWindow => {
            let spw_name = if spw_id >= 0 && (spw_id as usize) < spectral_window.row_count() {
                spectral_window
                    .name(spw_id as usize)
                    .unwrap_or_else(|_| format!("SPW {spw_id}"))
            } else {
                format!("SPW {spw_id}")
            };
            (format!("spw-{spw_id}"), spw_name)
        }
        MsColorAxis::Baseline => {
            let label = format!("a{antenna1}-a{antenna2}");
            (format!("baseline-{label}"), label)
        }
        MsColorAxis::Correlation => {
            let label = correlation_label.unwrap_or("corr").to_string();
            (format!("corr-{label}"), label)
        }
    }
}

fn iteration_group(
    iteraxis: Option<MsIterationAxis>,
    field_id: i32,
    field: &crate::subtables::MsField<'_>,
    spw_id: i32,
    spectral_window: &crate::subtables::MsSpectralWindow<'_>,
    scan_number: i32,
    correlation_label: Option<&str>,
) -> (String, String) {
    match iteraxis {
        None => ("all".to_string(), "All data".to_string()),
        Some(MsIterationAxis::Field) => {
            let field_name = if field_id >= 0 && (field_id as usize) < field.row_count() {
                field
                    .name(field_id as usize)
                    .unwrap_or_else(|_| format!("Field {field_id}"))
            } else {
                format!("Field {field_id}")
            };
            (format!("field-{field_id}"), field_name)
        }
        Some(MsIterationAxis::Scan) => {
            (format!("scan-{scan_number}"), format!("Scan {scan_number}"))
        }
        Some(MsIterationAxis::SpectralWindow) => {
            let spw_name = if spw_id >= 0 && (spw_id as usize) < spectral_window.row_count() {
                spectral_window
                    .name(spw_id as usize)
                    .unwrap_or_else(|_| format!("SPW {spw_id}"))
            } else {
                format!("SPW {spw_id}")
            };
            (format!("spw-{spw_id}"), spw_name)
        }
        Some(MsIterationAxis::Correlation) => {
            let label = correlation_label.unwrap_or("corr").to_string();
            (format!("corr-{label}"), label)
        }
    }
}

fn scatter_series_identity(
    y_axis: MsAxis,
    multi_y: bool,
    base_key: &str,
    base_label: &str,
) -> (String, String, String) {
    if !multi_y {
        return (
            base_key.to_string(),
            base_label.to_string(),
            base_key.to_string(),
        );
    }
    let series_key = format!("{}::{base_key}", y_axis.as_str());
    let series_label = if base_key == "all" {
        y_axis.display_name().to_string()
    } else {
        format!("{} · {base_label}", y_axis.display_name())
    };
    (series_key.clone(), series_label, series_key)
}

fn resolve_iterated_grid(
    panel_count: usize,
    requested_rows: usize,
    requested_cols: usize,
) -> Result<(usize, usize), String> {
    if panel_count == 0 {
        return Err("msexplore iteration produced no populated panels".to_string());
    }
    let rows = requested_rows.max(1);
    let cols = requested_cols.max(1);
    if rows == 1 && cols == 1 {
        let auto_cols = (panel_count as f64).sqrt().ceil().max(1.0) as usize;
        let auto_rows = panel_count.div_ceil(auto_cols);
        return Ok((auto_rows, auto_cols));
    }
    if rows == 1 {
        return Ok((panel_count.div_ceil(cols), cols));
    }
    if cols == 1 {
        return Ok((rows, panel_count.div_ceil(rows)));
    }
    if rows * cols < panel_count {
        return Err(format!(
            "msexplore gridrows={rows} and gridcols={cols} cannot hold {panel_count} iterated panels"
        ));
    }
    Ok((rows, cols))
}

fn share_axis_bounds(self_scale: bool, shared_axis: bool) -> bool {
    shared_axis || !self_scale
}

#[derive(Debug, Clone)]
struct ComplexGrid {
    corr_count: usize,
    chan_count: usize,
    values: Vec<Complex64>,
}

#[derive(Debug, Clone)]
struct FloatGrid {
    corr_count: usize,
    chan_count: usize,
    values: Vec<f64>,
}

fn selected_bool_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<bool>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Bool(value) => Ok(value),
        other => Err(format!(
            "msexplore requires BOOL {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_i32_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<i32>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Int32(value) => Ok(value),
        other => Err(format!(
            "msexplore requires INT {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_f64_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<f64>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Float64(value) => Ok(value),
        other => Err(format!(
            "msexplore requires DOUBLE {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_scalar_values<T>(
    table: &Table,
    column: &'static str,
    rows: &[usize],
    convert: impl Fn(ScalarValue) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .scalar_cells_owned_for_rows(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            value
                .ok_or_else(|| format!("msexplore requires {column} data for row {row}"))
                .and_then(&convert)
        })
        .collect()
}

fn selected_uvw_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<[f64; 3]>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .array_cells_owned(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            let value =
                value.ok_or_else(|| format!("msexplore requires {column} data for row {row}"))?;
            match value {
                ArrayValue::Float64(values) => {
                    let slice = values.as_slice().ok_or_else(|| {
                        format!("msexplore requires contiguous {column} f64[3] cells")
                    })?;
                    if slice.len() != 3 {
                        return Err(format!(
                            "msexplore requires {column} cells with shape [3], found [{}]",
                            slice.len()
                        ));
                    }
                    Ok([slice[0], slice[1], slice[2]])
                }
                other => Err(format!(
                    "msexplore requires DOUBLE array {column} cells, found {:?}",
                    other.primitive_type()
                )),
            }
        })
        .collect()
}

struct SelectedArrayColumn {
    column: &'static str,
    values: Vec<Option<ArrayValue>>,
}

impl SelectedArrayColumn {
    fn load(table: &Table, column: &'static str, row_indices: &[usize]) -> Result<Self, String> {
        let values = table
            .column_accessor(column)
            .map_err(|error| error.to_string())?
            .array_cells_owned(row_indices)
            .map_err(|error| error.to_string())?;
        Ok(Self { column, values })
    }

    fn get(&self, row_slot: usize) -> Result<&ArrayValue, String> {
        self.values
            .get(row_slot)
            .and_then(|value| value.as_ref())
            .ok_or_else(|| {
                format!(
                    "msexplore requires {} data for selected row slot {}",
                    self.column, row_slot
                )
            })
    }
}

enum PreparedSelectedDataSource {
    Single(SelectedArrayColumn),
    Difference {
        left: SelectedArrayColumn,
        right: SelectedArrayColumn,
        scalar: bool,
    },
    Ratio {
        numerator: SelectedArrayColumn,
        denominator: SelectedArrayColumn,
        scalar: bool,
    },
}

impl PreparedSelectedDataSource {
    fn new(
        ms: &MeasurementSet,
        column: MsDataColumn,
        row_indices: &[usize],
    ) -> Result<Self, String> {
        match column {
            MsDataColumn::Data => Ok(Self::Single(SelectedArrayColumn::load(
                ms.main_table(),
                "DATA",
                row_indices,
            )?)),
            MsDataColumn::Corrected => Ok(Self::Single(SelectedArrayColumn::load(
                ms.main_table(),
                "CORRECTED_DATA",
                row_indices,
            )?)),
            MsDataColumn::Model => Ok(Self::Single(SelectedArrayColumn::load(
                ms.main_table(),
                "MODEL_DATA",
                row_indices,
            )?)),
            MsDataColumn::CorrectedMinusModel => Ok(Self::Difference {
                left: SelectedArrayColumn::load(ms.main_table(), "CORRECTED_DATA", row_indices)?,
                right: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: false,
            }),
            MsDataColumn::CorrectedMinusModelScalar => Ok(Self::Difference {
                left: SelectedArrayColumn::load(ms.main_table(), "CORRECTED_DATA", row_indices)?,
                right: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: true,
            }),
            MsDataColumn::DataMinusModel => Ok(Self::Difference {
                left: SelectedArrayColumn::load(ms.main_table(), "DATA", row_indices)?,
                right: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: false,
            }),
            MsDataColumn::DataMinusModelScalar => Ok(Self::Difference {
                left: SelectedArrayColumn::load(ms.main_table(), "DATA", row_indices)?,
                right: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: true,
            }),
            MsDataColumn::CorrectedDivModel => Ok(Self::Ratio {
                numerator: SelectedArrayColumn::load(
                    ms.main_table(),
                    "CORRECTED_DATA",
                    row_indices,
                )?,
                denominator: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: false,
            }),
            MsDataColumn::CorrectedDivModelScalar => Ok(Self::Ratio {
                numerator: SelectedArrayColumn::load(
                    ms.main_table(),
                    "CORRECTED_DATA",
                    row_indices,
                )?,
                denominator: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: true,
            }),
            MsDataColumn::DataDivModel => Ok(Self::Ratio {
                numerator: SelectedArrayColumn::load(ms.main_table(), "DATA", row_indices)?,
                denominator: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: false,
            }),
            MsDataColumn::DataDivModelScalar => Ok(Self::Ratio {
                numerator: SelectedArrayColumn::load(ms.main_table(), "DATA", row_indices)?,
                denominator: SelectedArrayColumn::load(ms.main_table(), "MODEL_DATA", row_indices)?,
                scalar: true,
            }),
        }
    }

    fn row(&self, row_slot: usize) -> Result<ComplexGrid, String> {
        match self {
            Self::Single(column) => complex_grid_from_array(column.get(row_slot)?),
            Self::Difference {
                left,
                right,
                scalar,
            } => {
                let left = complex_grid_from_array(left.get(row_slot)?)?;
                let right = complex_grid_from_array(right.get(row_slot)?)?;
                combine_grids(left, right, |left, right| {
                    if *scalar {
                        Complex64::new(left.norm() - right.norm(), 0.0)
                    } else {
                        left - right
                    }
                })
            }
            Self::Ratio {
                numerator,
                denominator,
                scalar,
            } => {
                let numerator = complex_grid_from_array(numerator.get(row_slot)?)?;
                let denominator = complex_grid_from_array(denominator.get(row_slot)?)?;
                combine_grids(numerator, denominator, |left, right| {
                    if *scalar {
                        if right.norm() == 0.0 {
                            Complex64::new(f64::NAN, f64::NAN)
                        } else {
                            Complex64::new(left.norm() / right.norm(), 0.0)
                        }
                    } else if right == Complex64::new(0.0, 0.0) {
                        Complex64::new(f64::NAN, f64::NAN)
                    } else {
                        left / right
                    }
                })
            }
        }
    }
}

fn complex_grid_from_array(array: &ArrayValue) -> Result<ComplexGrid, String> {
    match array {
        ArrayValue::Complex32(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values
                    .iter()
                    .map(|value| Complex64::new(value.re as f64, value.im as f64))
                    .collect(),
            })
        }
        ArrayValue::Complex64(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().copied().collect(),
            })
        }
        other => Err(format!(
            "msexplore requires complex visibility data, found {:?}",
            other.primitive_type()
        )),
    }
}

fn float_grid_from_array(array: &ArrayValue, column_name: &str) -> Result<FloatGrid, String> {
    match array {
        ArrayValue::Float32(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                format!("msexplore expects {column_name} cells with shape [num_corr, num_chan]")
            })?;
            Ok(FloatGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().map(|value| *value as f64).collect(),
            })
        }
        ArrayValue::Float64(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                format!("msexplore expects {column_name} cells with shape [num_corr, num_chan]")
            })?;
            Ok(FloatGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().copied().collect(),
            })
        }
        other => Err(format!(
            "msexplore requires FLOAT {column_name} data, found {:?}",
            other.primitive_type()
        )),
    }
}

fn scalar_values_to_grid(values: &[f64], chan_count: usize) -> FloatGrid {
    FloatGrid {
        corr_count: values.len(),
        chan_count,
        values: values
            .iter()
            .flat_map(|value| std::iter::repeat_n(*value, chan_count))
            .collect(),
    }
}

fn combine_grids<F>(
    left: ComplexGrid,
    right: ComplexGrid,
    mut combine: F,
) -> Result<ComplexGrid, String>
where
    F: FnMut(Complex64, Complex64) -> Complex64,
{
    if left.corr_count != right.corr_count || left.chan_count != right.chan_count {
        return Err(format!(
            "msexplore data columns have mismatched shapes [{}, {}] and [{}, {}]",
            left.corr_count, left.chan_count, right.corr_count, right.chan_count
        ));
    }
    Ok(ComplexGrid {
        corr_count: left.corr_count,
        chan_count: left.chan_count,
        values: left
            .values
            .into_iter()
            .zip(right.values)
            .map(|(left, right)| combine(left, right))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listobs::{
        AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo,
        ObservationSummary, PolarizationSummary, SourceSummary, SpectralWindowSummary,
    };
    use crate::plot::UvCoveragePlotPayload;
    use crate::test_helpers::default_value;
    use crate::{MeasurementSetBuilder, OptionalMainColumn};
    use casa_types::{RecordField, RecordValue};
    use ndarray::ArrayD;
    use num_complex::Complex32;
    use tempfile::tempdir;

    fn scatter_point_ref(row: usize) -> MsScatterPointRef {
        MsScatterPointRef {
            row,
            corr: 0,
            chan_start: 0,
            chan_end: 1,
        }
    }

    fn scatter_series(
        label: &str,
        color_group: &str,
        y_axis: MsAxis,
        points: &[(f64, f64)],
        row_base: usize,
    ) -> MsScatterSeries {
        MsScatterSeries {
            label: label.to_string(),
            color_group: color_group.to_string(),
            y_axis,
            points: points.to_vec(),
            provenance: (0..points.len())
                .map(|offset| scatter_point_ref(row_base + offset))
                .collect(),
        }
    }

    fn scatter_plot_payload(
        title: &str,
        secondary_y_axis: Option<MsAxis>,
        legend_position: MsLegendPosition,
        series: Vec<MsScatterSeries>,
    ) -> MsScatterPlotPayload {
        MsScatterPlotPayload {
            title: title.to_string(),
            x_axis: MsAxis::Time,
            y_axis: MsAxis::Amplitude,
            secondary_y_axis,
            x_label: "Time".to_string(),
            y_label: "Amplitude".to_string(),
            secondary_y_label: secondary_y_axis.map(|axis| axis.display_name().to_string()),
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            secondary_fixed_y_bounds: None,
            showlegend: true,
            legend_position,
            showmajorgrid: true,
            showminorgrid: true,
            symbol_size_px: None,
            series,
            header_lines: vec!["Synthetic header".to_string()],
            summary: "Synthetic scatter payload".to_string(),
        }
    }

    #[test]
    fn prepared_selected_data_source_preserves_selected_row_order() {
        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .expect("create in-memory MS");
        let schema = ms.main_table().schema().expect("main schema").clone();

        for amplitude in [10.0_f32, 20.0_f32, 30.0_f32] {
            let data = ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    vec![1, 2],
                    vec![
                        Complex32::new(amplitude, 0.0),
                        Complex32::new(amplitude + 1.0, 0.0),
                    ],
                )
                .expect("data shape"),
            );
            let fields = schema
                .columns()
                .iter()
                .map(|column| {
                    if column.name() == "DATA" {
                        RecordField::new("DATA", Value::Array(data.clone()))
                    } else {
                        RecordField::new(column.name(), default_value(column.name()))
                    }
                })
                .collect::<Vec<_>>();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .expect("add main row");
        }

        let source =
            PreparedSelectedDataSource::new(&ms, MsDataColumn::Data, &[2, 0]).expect("prefetch");
        let first = source.row(0).expect("selected row 0");
        let second = source.row(1).expect("selected row 1");

        assert_eq!(first.corr_count, 1);
        assert_eq!(first.chan_count, 2);
        assert_eq!(first.values[0], Complex64::new(30.0, 0.0));
        assert_eq!(first.values[1], Complex64::new(31.0, 0.0));
        assert_eq!(second.values[0], Complex64::new(10.0, 0.0));
        assert_eq!(second.values[1], Complex64::new(11.0, 0.0));
    }

    #[test]
    fn preset_enum_surfaces_round_trip_and_cover_aliases() {
        for preset in MsPlotPreset::ALL {
            assert_eq!(MsPlotPreset::parse(preset.as_str()).unwrap(), preset);
            assert_eq!(preset.to_string(), preset.as_str());
            assert!(!preset.display_name().is_empty());
        }

        let alias_cases = [
            ("uv", MsPlotPreset::UvCoverage),
            ("antennas", MsPlotPreset::AntennaLayout),
            ("scans", MsPlotPreset::ScanTimeline),
            ("spw_coverage", MsPlotPreset::SpectralWindowCoverage),
            ("spws", MsPlotPreset::SpectralWindowCoverage),
            ("amp_time", MsPlotPreset::AmplitudeVsTime),
            ("phase_time", MsPlotPreset::PhaseVsTime),
            (
                "amp_phase_time_stacked",
                MsPlotPreset::AmplitudePhaseVsTimeStacked,
            ),
            ("amplitude_vs_uvdist", MsPlotPreset::AmplitudeVsUvDistance),
            ("amp_uvdist", MsPlotPreset::AmplitudeVsUvDistance),
            ("wt_time", MsPlotPreset::WeightVsTime),
            ("sigma_time", MsPlotPreset::SigmaVsTime),
            ("flag_time", MsPlotPreset::FlagVsTime),
            ("wtsp_time", MsPlotPreset::WeightSpectrumVsTime),
            ("sigmasp_time", MsPlotPreset::SigmaSpectrumVsTime),
            ("flagrow_time", MsPlotPreset::FlagRowVsTime),
            ("elevation_time", MsPlotPreset::ElevationVsTime),
            ("azimuth_time", MsPlotPreset::AzimuthVsTime),
            ("hourang_vs_time", MsPlotPreset::HourAngleVsTime),
            ("hourang_time", MsPlotPreset::HourAngleVsTime),
            ("parang_vs_time", MsPlotPreset::ParallacticAngleVsTime),
            ("parang_time", MsPlotPreset::ParallacticAngleVsTime),
            ("azimuth_elevation", MsPlotPreset::AzimuthVsElevation),
            ("amp_channel", MsPlotPreset::AmplitudeVsChannel),
            ("phase_channel", MsPlotPreset::PhaseVsChannel),
            ("amp_frequency", MsPlotPreset::AmplitudeVsFrequency),
            ("phase_frequency", MsPlotPreset::PhaseVsFrequency),
            ("amp_velocity", MsPlotPreset::AmplitudeVsVelocity),
            ("phase_velocity", MsPlotPreset::PhaseVsVelocity),
            ("real_vs_imag", MsPlotPreset::RealVsImaginary),
        ];
        for (alias, expected) in alias_cases {
            assert_eq!(MsPlotPreset::parse(alias).unwrap(), expected);
        }
        assert!(
            MsPlotPreset::parse("nope")
                .unwrap_err()
                .contains("unsupported")
        );

        let metadata_pairs = [
            (ListObsPlotKind::UvCoverage, MsPlotPreset::UvCoverage),
            (ListObsPlotKind::AntennaLayout, MsPlotPreset::AntennaLayout),
            (ListObsPlotKind::ScanTimeline, MsPlotPreset::ScanTimeline),
            (
                ListObsPlotKind::SpectralWindowCoverage,
                MsPlotPreset::SpectralWindowCoverage,
            ),
            (
                ListObsPlotKind::AmplitudeVsTime,
                MsPlotPreset::AmplitudeVsTime,
            ),
            (ListObsPlotKind::PhaseVsTime, MsPlotPreset::PhaseVsTime),
            (
                ListObsPlotKind::AmplitudeVsUvDistance,
                MsPlotPreset::AmplitudeVsUvDistance,
            ),
        ];
        for (kind, preset) in metadata_pairs {
            assert_eq!(MsPlotPreset::from_listobs_kind(kind), preset);
        }
        assert_eq!(MsPlotPreset::UvCoverage.lowers_to_listobs_metadata(), None);
        assert_eq!(
            MsPlotPreset::AmplitudeVsFrequency.lowers_to_listobs_metadata(),
            None
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    fn axis_and_related_enums_cover_aliases_and_helper_flags() {
        let axis_cases = [
            (
                MsAxis::Amplitude,
                &["amp", "amplitude"][..],
                "Amplitude",
                true,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::Phase,
                &["phase"][..],
                "Phase",
                true,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::Real,
                &["real"][..],
                "Real",
                true,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::Imaginary,
                &["imag", "imaginary"][..],
                "Imaginary",
                true,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::Time,
                &["time"][..],
                "Time",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Channel,
                &["chan", "channel"][..],
                "Channel",
                false,
                false,
                true,
                false,
                false,
                false,
            ),
            (
                MsAxis::Frequency,
                &["freq", "frequency"][..],
                "Frequency",
                false,
                false,
                true,
                false,
                false,
                true,
            ),
            (
                MsAxis::Velocity,
                &["velocity", "vel"][..],
                "Velocity",
                false,
                false,
                true,
                false,
                false,
                true,
            ),
            (
                MsAxis::UvDistance,
                &["uvdist", "uv_distance", "uv-distance"][..],
                "UV Distance",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::U,
                &["u"][..],
                "U",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::V,
                &["v"][..],
                "V",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::W,
                &["w"][..],
                "W",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Scan,
                &["scan"][..],
                "Scan",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Field,
                &["field"][..],
                "Field",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::SpectralWindow,
                &["spw", "spectral_window", "spectral-window"][..],
                "Spectral Window",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Correlation,
                &["corr", "correlation"][..],
                "Correlation",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Baseline,
                &["baseline"][..],
                "Baseline",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Antenna,
                &["antenna"][..],
                "Antenna",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Azimuth,
                &["azimuth"][..],
                "Azimuth",
                false,
                true,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Elevation,
                &["elevation"][..],
                "Elevation",
                false,
                true,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::HourAngle,
                &["hourang", "hourangle", "hour_angle", "hour-angle"][..],
                "Hour Angle",
                false,
                true,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::ParallacticAngle,
                &["parang", "parallactic_angle", "parallactic-angle"][..],
                "Parallactic Angle",
                false,
                true,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::Weight,
                &["weight"][..],
                "Weight",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
            (
                MsAxis::Sigma,
                &["sigma"][..],
                "Sigma",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
            (
                MsAxis::WeightSpectrum,
                &[
                    "wtsp",
                    "weightspectrum",
                    "weight_spectrum",
                    "weight-spectrum",
                ][..],
                "Weight Spectrum",
                false,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::SigmaSpectrum,
                &[
                    "sigmasp",
                    "sigmaspectrum",
                    "sigma_spectrum",
                    "sigma-spectrum",
                ][..],
                "Sigma Spectrum",
                false,
                false,
                true,
                false,
                true,
                false,
            ),
            (
                MsAxis::Flag,
                &["flag"][..],
                "Flag",
                false,
                false,
                true,
                true,
                true,
                false,
            ),
            (
                MsAxis::FlagRow,
                &["flagrow", "flag_row", "flag-row"][..],
                "Flag Row",
                false,
                false,
                false,
                true,
                false,
                false,
            ),
            (
                MsAxis::AntRa,
                &["ant-ra", "ant_ra"][..],
                "Antenna RA",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
            (
                MsAxis::AntDec,
                &["ant-dec", "ant_dec"][..],
                "Antenna Dec",
                false,
                false,
                false,
                false,
                false,
                false,
            ),
        ];
        for (
            axis,
            aliases,
            display_name,
            visibility_math,
            derived_geometry,
            channel_bins,
            flag_rows,
            correlation_slots,
            spectral_coordinates,
        ) in axis_cases
        {
            assert_eq!(axis.to_string(), axis.as_str());
            assert_eq!(axis.display_name(), display_name);
            assert_eq!(axis.is_visibility_math(), visibility_math);
            assert_eq!(axis.uses_derived_geometry(), derived_geometry);
            assert_eq!(axis.uses_channel_bins(), channel_bins);
            assert_eq!(axis.uses_flag_rows(), flag_rows);
            assert_eq!(axis.uses_correlation_slots(), correlation_slots);
            assert_eq!(axis.uses_spectral_coordinates(), spectral_coordinates);
            for alias in aliases {
                assert_eq!(MsAxis::parse(alias).unwrap(), axis);
            }
        }
        assert!(MsAxis::parse("bogus").unwrap_err().contains("unsupported"));

        let data_columns = [
            (MsDataColumn::Data, &["data"][..]),
            (
                MsDataColumn::Corrected,
                &["corrected", "corrected_data"][..],
            ),
            (MsDataColumn::Model, &["model", "model_data"][..]),
            (
                MsDataColumn::CorrectedMinusModel,
                &["corrected-model", "corrected_minus_model"][..],
            ),
            (
                MsDataColumn::CorrectedMinusModelScalar,
                &["corrected-model_scalar", "corrected_minus_model_scalar"][..],
            ),
            (
                MsDataColumn::DataMinusModel,
                &["data-model", "data_minus_model"][..],
            ),
            (
                MsDataColumn::DataMinusModelScalar,
                &["data-model_scalar", "data_minus_model_scalar"][..],
            ),
            (
                MsDataColumn::CorrectedDivModel,
                &["corrected/model", "corrected_div_model"][..],
            ),
            (
                MsDataColumn::CorrectedDivModelScalar,
                &["corrected/model_scalar", "corrected_div_model_scalar"][..],
            ),
            (
                MsDataColumn::DataDivModel,
                &["data/model", "data_div_model"][..],
            ),
            (
                MsDataColumn::DataDivModelScalar,
                &["data/model_scalar", "data_div_model_scalar"][..],
            ),
        ];
        for (column, aliases) in data_columns {
            assert_eq!(column.to_string(), column.as_str());
            for alias in aliases {
                assert_eq!(MsDataColumn::parse(alias).unwrap(), column);
            }
        }
        assert!(
            MsDataColumn::parse("bad")
                .unwrap_err()
                .contains("unsupported")
        );

        let color_axes = [
            (MsColorAxis::None, &["none"][..]),
            (MsColorAxis::Field, &["field"][..]),
            (MsColorAxis::Scan, &["scan"][..]),
            (
                MsColorAxis::SpectralWindow,
                &["spw", "spectral_window", "spectral-window"][..],
            ),
            (MsColorAxis::Baseline, &["baseline"][..]),
            (MsColorAxis::Correlation, &["correlation", "corr"][..]),
        ];
        for (axis, aliases) in color_axes {
            assert_eq!(axis.to_string(), axis.as_str());
            for alias in aliases {
                assert_eq!(MsColorAxis::parse(alias).unwrap(), axis);
            }
        }
        assert!(
            MsColorAxis::parse("bad")
                .unwrap_err()
                .contains("unsupported")
        );

        assert_eq!(MsExportFormat::parse("txt").unwrap(), MsExportFormat::Txt);
        assert_eq!(MsExportFormat::Txt.extension(), "txt");
        for removed in ["png", "pdf"] {
            let error = MsExportFormat::parse(removed).unwrap_err();
            assert!(error.contains("removed (#833)"), "{error}");
            assert!(error.contains("--plot-format txt"), "{error}");
        }
        assert!(
            MsExportFormat::parse("jpg")
                .unwrap_err()
                .contains("unsupported")
        );
    }

    #[test]
    fn selection_and_page_enums_preserve_stable_strings() {
        let selection = MsSelection {
            selectdata: false,
            field: Some("0".to_string()),
            spw: Some("1".to_string()),
            antenna: Some("ea01".to_string()),
            scan: Some("5".to_string()),
            observation: Some("2".to_string()),
            array: Some("1".to_string()),
            timerange: Some("09:00:00~10:00:00".to_string()),
            uvrange: Some(">100m".to_string()),
            correlation: Some("RR".to_string()),
            intent: Some("CALIBRATE".to_string()),
            data_description: None,
            state: None,
            msselect: Some("DATA_DESC_ID==0".to_string()),
            feed: Some("0".to_string()),
        };
        let summary = selection.to_summary_options();
        assert!(!summary.selectdata);
        assert_eq!(summary.field.as_deref(), Some("0"));
        assert_eq!(summary.spw.as_deref(), Some("1"));
        assert_eq!(summary.antenna.as_deref(), Some("ea01"));
        assert_eq!(summary.scan.as_deref(), Some("5"));
        assert_eq!(summary.observation.as_deref(), Some("2"));
        assert_eq!(summary.array.as_deref(), Some("1"));
        assert_eq!(summary.timerange.as_deref(), Some("09:00:00~10:00:00"));
        assert_eq!(summary.uvrange.as_deref(), Some(">100m"));
        assert_eq!(summary.correlation.as_deref(), Some("RR"));
        assert_eq!(summary.intent.as_deref(), Some("CALIBRATE"));
        assert_eq!(summary.msselect.as_deref(), Some("DATA_DESC_ID==0"));
        assert_eq!(summary.feed.as_deref(), Some("0"));
        assert!(!summary.listunfl);
        assert_eq!(summary.cachesize_mb, None);

        for (range, raw) in [
            (MsPageExportRange::Current, "current"),
            (MsPageExportRange::All, "all"),
        ] {
            assert_eq!(range.as_str(), raw);
            assert_eq!(range.to_string(), raw);
        }

        for position in [
            MsLegendPosition::UpperRight,
            MsLegendPosition::UpperLeft,
            MsLegendPosition::LowerRight,
            MsLegendPosition::LowerLeft,
            MsLegendPosition::ExteriorRight,
            MsLegendPosition::ExteriorLeft,
            MsLegendPosition::ExteriorTop,
            MsLegendPosition::ExteriorBottom,
        ] {
            assert_eq!(
                serde_json::from_str::<MsLegendPosition>(&format!("\"{}\"", position.as_str()))
                    .unwrap(),
                position
            );
            assert_eq!(position.to_string(), position.as_str());
        }

        for item in [
            MsPageHeaderItem::Filename,
            MsPageHeaderItem::YColumn,
            MsPageHeaderItem::ObsDate,
            MsPageHeaderItem::ObsTime,
            MsPageHeaderItem::Observer,
            MsPageHeaderItem::ProjId,
            MsPageHeaderItem::Telescope,
            MsPageHeaderItem::TargName,
            MsPageHeaderItem::TargDir,
        ] {
            assert_eq!(item.to_string(), item.as_str());
        }

        for axis in MsIterationAxis::ALL {
            assert_eq!(MsIterationAxis::parse(axis.as_str()).unwrap(), axis);
            assert_eq!(axis.to_string(), axis.as_str());
            assert!(!axis.display_name().is_empty());
        }
        assert_eq!(
            MsIterationAxis::parse("spectralwindow").unwrap(),
            MsIterationAxis::SpectralWindow
        );
        assert_eq!(
            MsIterationAxis::parse("corr").unwrap(),
            MsIterationAxis::Correlation
        );
        assert!(
            MsIterationAxis::parse("bad")
                .unwrap_err()
                .contains("unsupported")
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    fn from_preset_and_validation_cover_supported_and_rejected_shapes() {
        for preset in MsPlotPreset::ALL {
            let spec = MsPlotSpec::from_preset(preset);
            assert_eq!(spec.preset, Some(preset));
            assert_eq!(spec.layout, MsLayoutSpec::default());
            assert_eq!(spec.iteration, MsIterationSpec::default());
            assert_eq!(spec.style, MsPlotStyleSpec::default());
            assert!(spec.flag_edit.is_none());
        }

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.y_axes.clear();
        assert!(spec.validate().unwrap_err().contains("at least one y axis"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.y_axes = vec![MsAxis::Amplitude, MsAxis::Phase, MsAxis::Weight];
        assert!(spec.validate().unwrap_err().contains("at most two y axes"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.y_axes = vec![MsAxis::Amplitude, MsAxis::Amplitude];
        assert!(spec.validate().unwrap_err().contains("duplicate y axes"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.layout.gridrows = 0;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("must be positive integers")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.iteration.iteraxis = Some(MsIterationAxis::Field);
        spec.y_axes.push(MsAxis::Phase);
        assert!(spec.validate().unwrap_err().contains("multi-y plots"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.iteration.xselfscale = true;
        spec.iteration.xsharedaxis = true;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("x-axis iteration scaling")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.iteration.yselfscale = true;
        spec.iteration.ysharedaxis = true;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("y-axis iteration scaling")
        );

        let finite_flag_edit = MsFlagEditSpec {
            action: MsFlagAction::Flag,
            region: MsFlagRegion {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            },
            plot_index: None,
            panel_key: None,
            extcorr: false,
            extchannel: false,
        };

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.flag_edit = Some(MsFlagEditSpec {
            region: MsFlagRegion {
                x_min: f64::NAN,
                ..finite_flag_edit.region.clone()
            },
            ..finite_flag_edit.clone()
        });
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("finite region bounds")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.flag_edit = Some(MsFlagEditSpec {
            panel_key: Some("field=0".to_string()),
            ..finite_flag_edit.clone()
        });
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("only accepts panel_key")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.flag_edit = Some(MsFlagEditSpec {
            plot_index: Some(0),
            ..finite_flag_edit.clone()
        });
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("only accepts plot_index")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgscan = true;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("avgscan requires avgtime")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgfield = true;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("avgfield requires avgtime")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgbaseline = true;
        spec.averaging.avgantenna = true;
        assert!(spec.validate().unwrap_err().contains("mutually exclusive"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgtime = Some(30.0);
        spec.averaging.avgscan = true;
        spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
        assert!(spec.validate().unwrap_err().contains("iterate by scan"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgtime = Some(30.0);
        spec.averaging.avgfield = true;
        spec.color_by = MsColorAxis::Field;
        assert!(spec.validate().unwrap_err().contains("color by field"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsChannel);
        spec.averaging.avgspw = true;
        spec.color_by = MsColorAxis::SpectralWindow;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("color by spectral window")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.transforms.phasecenter = Some("J2000 1rad 2rad".to_string());
        assert!(spec.validate().unwrap_err().contains("not yet implemented"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.x_axis = MsAxis::Field;
        assert!(spec.validate().unwrap_err().contains("x axis field"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.y_axes = vec![MsAxis::Channel];
        assert!(spec.validate().unwrap_err().contains("y axis channel"));

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.averaging.avgchannel = Some(8);
        spec.validate()
            .expect("CASA plotms accepts avgchannel for visibility amplitude plots");

        let bins = channel_bins(384, Some(10_000)).expect("high avgchannel");
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].start, 0);
        assert_eq!(bins[0].end, 384);

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::UvCoverage);
        spec.averaging.avgchannel = Some(10_000);
        spec.validate()
            .expect("CASA plotms accepts avgchannel for UV coverage");

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::FlagVsTime);
        spec.x_axis = MsAxis::Channel;
        spec.averaging.avgchannel = Some(8);
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("flag plots do not yet support avgchannel")
        );

        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.layout.rowindex = 1;
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("reserved for multi-plot page composition")
        );

        let request = MsExploreSpec {
            ms_path: PathBuf::from("demo.ms"),
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: None,
            exprange: MsPageExportRange::Current,
            max_plot_points: 10,
            plots: Vec::new(),
        };
        assert!(
            request
                .validate()
                .unwrap_err()
                .contains("at least one plot")
        );

        let request = MsExploreSpec {
            max_plot_points: 0,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
            ..request
        };
        assert!(request.validate().unwrap_err().contains("positive integer"));

        let mut first = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
        first.layout.gridrows = 1;
        first.layout.gridcols = 2;
        first.layout.rowindex = 0;
        first.layout.colindex = 0;
        first.layout.plotindex = 0;
        let mut second = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
        second.layout.gridrows = 2;
        second.layout.gridcols = 2;
        second.layout.rowindex = 0;
        second.layout.colindex = 1;
        second.layout.plotindex = 1;
        let request = MsExploreSpec {
            ms_path: PathBuf::from("demo.ms"),
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: None,
            exprange: MsPageExportRange::Current,
            max_plot_points: 10,
            plots: vec![first, second],
        };
        assert!(
            request
                .validate()
                .unwrap_err()
                .contains("agree on gridrows/gridcols")
        );

        let mut first = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
        first.layout.gridrows = 1;
        first.layout.gridcols = 2;
        first.layout.rowindex = 0;
        first.layout.colindex = 0;
        first.layout.plotindex = 0;
        let mut second = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsFrequency);
        second.layout.gridrows = 1;
        second.layout.gridcols = 2;
        second.layout.rowindex = 0;
        second.layout.colindex = 1;
        second.layout.plotindex = 1;
        let request = MsExploreSpec {
            ms_path: PathBuf::from("demo.ms"),
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: None,
            exprange: MsPageExportRange::All,
            max_plot_points: 10,
            plots: vec![first, second],
        };
        assert!(
            request
                .validate()
                .unwrap_err()
                .contains("share the same x/y axis configuration")
        );
    }

    #[test]
    fn compact_scatter_series_evenly_decimates_points() {
        let series = vec![MsScatterSeries {
            label: "Amplitude".to_string(),
            color_group: "all".to_string(),
            y_axis: MsAxis::Amplitude,
            points: (0..5).map(|index| (index as f64, index as f64)).collect(),
            provenance: (0..5)
                .map(|index| MsScatterPointRef {
                    row: index,
                    corr: 0,
                    chan_start: index,
                    chan_end: index + 1,
                })
                .collect(),
        }];

        let (compacted, original_points, kept_points) = compact_scatter_series(series, 3);
        assert_eq!(original_points, 5);
        assert_eq!(kept_points, 3);
        assert_eq!(compacted.len(), 1);
        assert_eq!(
            compacted[0].points,
            vec![(0.0, 0.0), (2.0, 2.0), (4.0, 4.0)]
        );
        assert_eq!(
            compacted[0]
                .provenance
                .iter()
                .map(|point| point.row)
                .collect::<Vec<_>>(),
            vec![0, 2, 4]
        );
    }

    #[test]
    fn compact_payload_to_budget_downsamples_scatter_payload_and_warns() {
        let payload = MsPlotPayload::Scatter(MsScatterPlotPayload {
            title: "Dense plot".to_string(),
            x_axis: MsAxis::Time,
            y_axis: MsAxis::Amplitude,
            secondary_y_axis: None,
            x_label: "Time".to_string(),
            y_label: "Amplitude".to_string(),
            secondary_y_label: None,
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            secondary_fixed_y_bounds: None,
            showlegend: true,
            legend_position: MsLegendPosition::UpperRight,
            showmajorgrid: true,
            showminorgrid: false,
            symbol_size_px: None,
            series: vec![
                MsScatterSeries {
                    label: "A".to_string(),
                    color_group: "a".to_string(),
                    y_axis: MsAxis::Amplitude,
                    points: (0..5).map(|index| (index as f64, index as f64)).collect(),
                    provenance: (0..5)
                        .map(|index| MsScatterPointRef {
                            row: index,
                            corr: 0,
                            chan_start: index,
                            chan_end: index + 1,
                        })
                        .collect(),
                },
                MsScatterSeries {
                    label: "B".to_string(),
                    color_group: "b".to_string(),
                    y_axis: MsAxis::Amplitude,
                    points: (5..10).map(|index| (index as f64, index as f64)).collect(),
                    provenance: (5..10)
                        .map(|index| MsScatterPointRef {
                            row: index,
                            corr: 0,
                            chan_start: index,
                            chan_end: index + 1,
                        })
                        .collect(),
                },
            ],
            header_lines: Vec::new(),
            summary: "Dense plot summary.".to_string(),
        });
        let point_budget = PointBudget {
            max_plot_points: Some(6),
            rendered_points: 10,
            allow_predecimation: false,
        };

        let compacted = compact_payload_to_budget(payload, &point_budget).unwrap();
        let MsPlotPayload::Scatter(compacted) = compacted else {
            panic!("expected scatter payload");
        };
        assert_eq!(
            compacted
                .series
                .iter()
                .map(|series| series.points.len())
                .sum::<usize>(),
            6
        );
        assert_eq!(compacted.header_lines.len(), 1);
        assert!(compacted.header_lines[0].contains("Downsampled plot"));
        assert!(compacted.summary.contains("Decimated points from 10 to 6"));
    }

    fn synthetic_summary() -> ListObsSummary {
        ListObsSummary {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set: MeasurementSetInfo {
                path: Some("fixture.ms".to_string()),
                ms_version: Some(2.0),
                row_count: 4,
                observation_count: 1,
                field_count: 2,
                spectral_window_count: 2,
                polarization_count: 1,
                data_description_count: 2,
                source_count: 1,
                antenna_count: 2,
                antenna_table_count: 2,
                time_reference: Some("UTC".to_string()),
                start_mjd_seconds: None,
                end_mjd_seconds: Some(180.0),
                total_elapsed_seconds: Some(80.0),
            },
            observations: vec![ObservationSummary {
                observation_id: 0,
                telescope_name: "VLA".to_string(),
                observer: "  ".to_string(),
                project: "Project-42".to_string(),
                release_date_mjd_seconds: 0.0,
                start_mjd_seconds: Some(100.0),
                end_mjd_seconds: Some(180.0),
            }],
            scans: vec![],
            fields: vec![
                FieldSummary {
                    field_id: 0,
                    name: "FIELD_A".to_string(),
                    code: "".to_string(),
                    source_id: 0,
                    row_count: 2,
                    unflagged_row_count: None,
                    time_mjd_seconds: 100.0,
                    direction_reference: Some("J2000".to_string()),
                    phase_direction_radians: [0.0, 0.0],
                },
                FieldSummary {
                    field_id: 1,
                    name: "FIELD_B".to_string(),
                    code: "".to_string(),
                    source_id: 0,
                    row_count: 2,
                    unflagged_row_count: None,
                    time_mjd_seconds: 140.0,
                    direction_reference: Some("J2000".to_string()),
                    phase_direction_radians: [0.1, 0.2],
                },
            ],
            polarization_setups: vec![PolarizationSummary {
                polarization_id: 0,
                num_correlations: 2,
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            }],
            data_descriptions: vec![
                DataDescriptionSummary {
                    data_description_id: 0,
                    spectral_window_id: 0,
                    polarization_id: 0,
                    flagged: false,
                },
                DataDescriptionSummary {
                    data_description_id: 1,
                    spectral_window_id: 1,
                    polarization_id: 0,
                    flagged: false,
                },
            ],
            spectral_windows: vec![
                SpectralWindowSummary {
                    spectral_window_id: 0,
                    name: "LOW".to_string(),
                    num_channels: 16,
                    frame: Some("TOPO".to_string()),
                    first_channel_frequency_hz: 1.0e9,
                    channel_width_hz: 1.0e6,
                    reference_frequency_hz: 1.0e9,
                    center_frequency_hz: 1.008e9,
                    min_frequency_hz: 0.9995e9,
                    max_frequency_hz: 1.0165e9,
                    total_bandwidth_hz: 16.0e6,
                    data_description_ids: vec![0],
                    polarization_ids: vec![0],
                    correlation_types: vec!["XX".to_string(), "YY".to_string()],
                },
                SpectralWindowSummary {
                    spectral_window_id: 1,
                    name: "HIGH".to_string(),
                    num_channels: 32,
                    frame: Some("TOPO".to_string()),
                    first_channel_frequency_hz: 1.2e9,
                    channel_width_hz: 5.0e5,
                    reference_frequency_hz: 1.2e9,
                    center_frequency_hz: 1.208e9,
                    min_frequency_hz: 1.19975e9,
                    max_frequency_hz: 1.21625e9,
                    total_bandwidth_hz: 16.0e6,
                    data_description_ids: vec![1],
                    polarization_ids: vec![0],
                    correlation_types: vec!["XX".to_string(), "YY".to_string()],
                },
            ],
            sources: vec![SourceSummary {
                source_id: 0,
                name: "SOURCE_A".to_string(),
                code: "".to_string(),
                spectral_window_id: -1,
                calibration_group: 0,
                num_lines: 0,
                rest_frequency_hz: None,
                system_velocity_m_s: None,
                time_mjd_seconds: 100.0,
                direction_radians: [0.0, 0.0],
            }],
            antennas: vec![
                AntennaSummary {
                    antenna_id: 0,
                    name: "ea01".to_string(),
                    station: "A01".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 0.0,
                    latitude_radians: 0.0,
                    offset_from_observatory_m: [0.0, 0.0, 0.0],
                    position_m: [1000.0, 2000.0, 0.0],
                },
                AntennaSummary {
                    antenna_id: 1,
                    name: "ea02".to_string(),
                    station: "A02".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 0.0,
                    latitude_radians: 0.0,
                    offset_from_observatory_m: [120.0, 40.0, 0.0],
                    position_m: [1120.0, 2040.0, 0.0],
                },
            ],
        }
    }

    #[test]
    fn compact_payload_to_budget_handles_grid_page_passthrough_and_empty_results() {
        let original =
            MsPlotPayload::ListObs(ListObsPlotPayload::UvCoverage(UvCoveragePlotPayload {
                draw_points: true,
                mirror: true,
                axis_extent_lambda: 0.0,
                tracks: vec![],
                summary: "uv".to_string(),
            }));
        assert_eq!(
            compact_payload_to_budget(original.clone(), &PointBudget::unlimited()).unwrap(),
            original
        );

        let grid_payload = MsPlotPayload::ScatterGrid(MsScatterGridPayload {
            title: "Dense grid".to_string(),
            x_axis: MsAxis::Time,
            y_axis: MsAxis::Amplitude,
            x_label: "Time".to_string(),
            y_label: "Amplitude".to_string(),
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            showlegend: true,
            legend_position: MsLegendPosition::ExteriorBottom,
            showmajorgrid: true,
            showminorgrid: false,
            symbol_size_px: None,
            iteraxis: MsIterationAxis::Field,
            gridrows: 1,
            gridcols: 2,
            share_x_bounds: true,
            share_y_bounds: true,
            panels: vec![
                MsScatterPanelPayload {
                    key: "field=0".to_string(),
                    label: "Field 0".to_string(),
                    summary: "Field 0 summary.".to_string(),
                    series: vec![
                        scatter_series(
                            "A0",
                            "a0",
                            MsAxis::Amplitude,
                            &[(0.0, 1.0), (1.0, 2.0), (2.0, 3.0)],
                            0,
                        ),
                        scatter_series(
                            "A1",
                            "a1",
                            MsAxis::Amplitude,
                            &[(0.0, 1.5), (1.0, 2.5), (2.0, 3.5)],
                            10,
                        ),
                    ],
                },
                MsScatterPanelPayload {
                    key: "field=1".to_string(),
                    label: "Field 1".to_string(),
                    summary: "Field 1 summary.".to_string(),
                    series: vec![scatter_series(
                        "B0",
                        "b0",
                        MsAxis::Amplitude,
                        &[(0.0, 4.0), (1.0, 5.0), (2.0, 6.0)],
                        20,
                    )],
                },
            ],
            header_lines: vec![],
            summary: "Dense grid summary.".to_string(),
        });
        let compacted_grid = compact_payload_to_budget(
            grid_payload,
            &PointBudget {
                max_plot_points: Some(4),
                rendered_points: 9,
                allow_predecimation: false,
            },
        )
        .unwrap();
        let MsPlotPayload::ScatterGrid(compacted_grid) = compacted_grid else {
            panic!("expected scatter grid payload");
        };
        assert_eq!(compacted_grid.panels.len(), 2);
        assert!(compacted_grid.header_lines[0].contains("Downsampled plot"));
        assert!(
            compacted_grid
                .summary
                .contains("Decimated points from 9 to 4")
        );

        let page_payload = MsPlotPayload::ScatterPage(MsScatterPagePayload {
            title: "Dense page".to_string(),
            exprange: MsPageExportRange::All,
            gridrows: 1,
            gridcols: 2,
            header_lines: vec![],
            items: vec![
                MsScatterPageItemPayload {
                    plotindex: 0,
                    rowindex: 0,
                    colindex: 0,
                    plot: scatter_plot_payload(
                        "Left",
                        None,
                        MsLegendPosition::UpperRight,
                        vec![scatter_series(
                            "L",
                            "left",
                            MsAxis::Amplitude,
                            &[(0.0, 1.0), (1.0, 1.5), (2.0, 2.0)],
                            30,
                        )],
                    ),
                },
                MsScatterPageItemPayload {
                    plotindex: 1,
                    rowindex: 0,
                    colindex: 1,
                    plot: scatter_plot_payload(
                        "Right",
                        None,
                        MsLegendPosition::UpperRight,
                        vec![scatter_series(
                            "R",
                            "right",
                            MsAxis::Amplitude,
                            &[(0.0, 3.0), (1.0, 3.5), (2.0, 4.0)],
                            40,
                        )],
                    ),
                },
            ],
            summary: "Dense page summary.".to_string(),
        });
        let compacted_page = compact_payload_to_budget(
            page_payload,
            &PointBudget {
                max_plot_points: Some(3),
                rendered_points: 6,
                allow_predecimation: false,
            },
        )
        .unwrap();
        let MsPlotPayload::ScatterPage(compacted_page) = compacted_page else {
            panic!("expected scatter page payload");
        };
        assert_eq!(compacted_page.items.len(), 2);
        assert!(compacted_page.header_lines[0].contains("Downsampled plot"));
        assert!(
            compacted_page
                .summary
                .contains("Decimated points from 6 to 3")
        );

        let empty_grid = MsPlotPayload::ScatterGrid(MsScatterGridPayload {
            title: "Empty".to_string(),
            x_axis: MsAxis::Time,
            y_axis: MsAxis::Amplitude,
            x_label: "Time".to_string(),
            y_label: "Amplitude".to_string(),
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            showlegend: false,
            legend_position: MsLegendPosition::UpperRight,
            showmajorgrid: true,
            showminorgrid: false,
            symbol_size_px: None,
            iteraxis: MsIterationAxis::Field,
            gridrows: 1,
            gridcols: 1,
            share_x_bounds: true,
            share_y_bounds: true,
            panels: vec![MsScatterPanelPayload {
                key: "field=0".to_string(),
                label: "Field 0".to_string(),
                summary: "Empty panel".to_string(),
                series: vec![scatter_series("Empty", "empty", MsAxis::Amplitude, &[], 50)],
            }],
            header_lines: vec![],
            summary: "Empty grid".to_string(),
        });
        assert!(
            compact_payload_to_budget(
                empty_grid,
                &PointBudget {
                    max_plot_points: Some(1),
                    rendered_points: 2,
                    allow_predecimation: false,
                },
            )
            .unwrap_err()
            .contains("produced no drawable panels")
        );
    }

    #[test]
    fn page_header_resolution_uses_summary_fallbacks_and_wrapping() {
        let summary = synthetic_summary();
        assert_eq!(measurement_set_start_time(&summary), Some(100.0));
        assert_eq!(first_non_empty([" ", "", "Observer"]), Some("Observer"));
        assert_eq!(first_non_empty([" ", "   "]), None);

        let spec = MsExploreSpec {
            ms_path: std::path::PathBuf::from("fixture.ms"),
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: vec![
                MsPageHeaderItem::Filename,
                MsPageHeaderItem::ObsDate,
                MsPageHeaderItem::ObsTime,
                MsPageHeaderItem::Observer,
                MsPageHeaderItem::ProjId,
                MsPageHeaderItem::Telescope,
                MsPageHeaderItem::TargName,
                MsPageHeaderItem::TargDir,
                MsPageHeaderItem::YColumn,
            ],
            page_title: None,
            exprange: MsPageExportRange::Current,
            max_plot_points: 10,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        };

        let lines = [
            resolve_page_header_segment(MsPageHeaderItem::Filename, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::ObsDate, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::ObsTime, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::Observer, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::ProjId, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::Telescope, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::TargName, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::TargDir, &spec, &summary),
            resolve_page_header_segment(MsPageHeaderItem::YColumn, &spec, &summary),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let joined = lines.join(" | ");
        assert!(joined.contains("fixture.ms"));
        assert!(joined.contains("Obs Date:"));
        assert!(joined.contains("Obs Time:"));
        assert!(joined.contains("Project-42"));
        assert!(joined.contains("VLA"));
        assert!(joined.contains("FIELD_A"));
        assert!(joined.contains("J2000"));
        assert!(joined.contains("Amplitude"));
        assert_eq!(
            wrap_header_segments(&["x".repeat(40), "y".repeat(40), "z".repeat(40),]).len(),
            2
        );
    }

    #[test]
    fn prepared_selected_data_source_and_grid_helpers_cover_ratio_and_errors() {
        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::CorrectedData)
                .with_main_column(OptionalMainColumn::ModelData),
        )
        .expect("create in-memory MS");
        let schema = ms.main_table().schema().expect("main schema").clone();
        let make_complex = |values: &[(f32, f32)]| {
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    vec![1, values.len()],
                    values
                        .iter()
                        .map(|(re, im)| Complex32::new(*re, *im))
                        .collect(),
                )
                .expect("complex shape"),
            )
        };

        for (data, corrected, model) in [
            (
                make_complex(&[(10.0, 0.0), (20.0, 0.0)]),
                make_complex(&[(12.0, 0.0), (24.0, 0.0)]),
                make_complex(&[(2.0, 0.0), (4.0, 0.0)]),
            ),
            (
                make_complex(&[(5.0, 0.0), (15.0, 0.0)]),
                make_complex(&[(8.0, 0.0), (0.0, 0.0)]),
                make_complex(&[(4.0, 0.0), (0.0, 0.0)]),
            ),
        ] {
            let fields = schema
                .columns()
                .iter()
                .map(|column| {
                    let value = match column.name() {
                        "DATA" => Value::Array(data.clone()),
                        "CORRECTED_DATA" => Value::Array(corrected.clone()),
                        "MODEL_DATA" => Value::Array(model.clone()),
                        _ => default_value(column.name()),
                    };
                    RecordField::new(column.name(), value)
                })
                .collect::<Vec<_>>();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .expect("add main row");
        }

        let difference =
            PreparedSelectedDataSource::new(&ms, MsDataColumn::CorrectedMinusModel, &[0, 1])
                .unwrap();
        assert_eq!(
            difference.row(0).unwrap().values[0],
            Complex64::new(10.0, 0.0)
        );

        let scalar_ratio =
            PreparedSelectedDataSource::new(&ms, MsDataColumn::CorrectedDivModelScalar, &[0, 1])
                .unwrap();
        let scalar_ratio_row = scalar_ratio.row(0).unwrap();
        assert_eq!(scalar_ratio_row.values[0], Complex64::new(6.0, 0.0));
        assert!(scalar_ratio.row(1).unwrap().values[1].re.is_nan());

        let direct_ratio =
            PreparedSelectedDataSource::new(&ms, MsDataColumn::DataDivModel, &[0]).unwrap();
        assert_eq!(
            direct_ratio.row(0).unwrap().values[1],
            Complex64::new(5.0, 0.0)
        );

        let wrong_shape = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2],
                vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
            )
            .unwrap(),
        );
        assert!(
            complex_grid_from_array(&wrong_shape)
                .unwrap_err()
                .contains("expects complex visibility cells")
        );

        let float_grid = float_grid_from_array(
            &ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![1, 2], vec![1.0_f32, 2.0_f32]).unwrap(),
            ),
            "WEIGHT_SPECTRUM",
        )
        .unwrap();
        assert_eq!(float_grid.values, vec![1.0, 2.0]);
        assert_eq!(
            scalar_values_to_grid(&[3.0, 4.0], 2).values,
            vec![3.0, 3.0, 4.0, 4.0]
        );
        assert!(
            combine_grids(
                ComplexGrid {
                    corr_count: 1,
                    chan_count: 2,
                    values: vec![Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)],
                },
                ComplexGrid {
                    corr_count: 2,
                    chan_count: 1,
                    values: vec![Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)],
                },
                |left, right| left + right,
            )
            .unwrap_err()
            .contains("mismatched shapes")
        );
    }

    #[test]
    fn listobs_visibility_seam_rejects_metadata_presets() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new())
            .expect("create in-memory MS");
        assert!(
            crate::plot_visibility::build_listobs_visibility_plot_payload(
                &ms,
                &ListObsOptions::default(),
                &ListObsPlotSpec::new(ListObsPlotKind::UvCoverage),
            )
            .unwrap_err()
            .contains("not a raw visibility")
        );
    }

    #[test]
    fn scatter_manifests_cover_dual_axis_and_grid_payloads() {
        let dual_axis = scatter_plot_payload(
            "Dual Axis Scatter",
            Some(MsAxis::Phase),
            MsLegendPosition::ExteriorRight,
            vec![
                scatter_series(
                    "Amplitude A",
                    "amp-a",
                    MsAxis::Amplitude,
                    &[(4_304_481_700.0, 1.0), (4_304_481_760.0, 2.5)],
                    0,
                ),
                scatter_series(
                    "Amplitude B",
                    "amp-b",
                    MsAxis::Amplitude,
                    &[(4_304_481_700.0, 0.5), (4_304_481_760.0, 1.8)],
                    10,
                ),
                scatter_series(
                    "Phase",
                    "phase",
                    MsAxis::Phase,
                    &[(4_304_481_700.0, -30.0), (4_304_481_760.0, 45.0)],
                    20,
                ),
            ],
        );
        let manifest = render_scatter_manifest(&MsPlotPayload::Scatter(dual_axis))
            .expect("dual-axis scatter manifest");
        assert!(manifest.contains("# secondary_y_axis=phase"));
        assert!(manifest.contains("series_key\tseries_label\ty_axis\tx\ty\n"));
        assert!(manifest.contains("phase\tPhase\tphase\t"));
        assert!(manifest.contains("amp-b\tAmplitude B\tamplitude\t"));

        let grid = MsScatterGridPayload {
            title: "Grid Scatter".to_string(),
            x_axis: MsAxis::Channel,
            y_axis: MsAxis::Amplitude,
            x_label: "Channel".to_string(),
            y_label: "Amplitude".to_string(),
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            showlegend: true,
            legend_position: MsLegendPosition::ExteriorBottom,
            showmajorgrid: true,
            showminorgrid: false,
            symbol_size_px: None,
            iteraxis: MsIterationAxis::Field,
            gridrows: 1,
            gridcols: 2,
            share_x_bounds: true,
            share_y_bounds: true,
            panels: vec![
                MsScatterPanelPayload {
                    key: "field=0".to_string(),
                    label: "Field 0".to_string(),
                    series: vec![
                        scatter_series(
                            "Field 0 RR",
                            "rr",
                            MsAxis::Amplitude,
                            &[(0.0, 1.0), (1.0, 1.5), (2.0, 2.0)],
                            30,
                        ),
                        scatter_series(
                            "Field 0 LL",
                            "ll",
                            MsAxis::Amplitude,
                            &[(0.0, 0.8), (1.0, 1.1), (2.0, 1.4)],
                            40,
                        ),
                    ],
                    summary: "Field 0".to_string(),
                },
                MsScatterPanelPayload {
                    key: "field=1".to_string(),
                    label: "Field 1".to_string(),
                    series: vec![
                        scatter_series(
                            "Field 1 RR",
                            "rr",
                            MsAxis::Amplitude,
                            &[(0.0, 2.5), (1.0, 2.0), (2.0, 1.7)],
                            50,
                        ),
                        scatter_series(
                            "Field 1 LL",
                            "ll",
                            MsAxis::Amplitude,
                            &[(0.0, 2.2), (1.0, 1.8), (2.0, 1.3)],
                            60,
                        ),
                    ],
                    summary: "Field 1".to_string(),
                },
            ],
            header_lines: vec!["Grid header".to_string()],
            summary: "Grid summary".to_string(),
        };
        let manifest = render_scatter_manifest(&MsPlotPayload::ScatterGrid(grid))
            .expect("grid scatter manifest");
        assert!(manifest.contains("# iteraxis=field"));
        assert!(manifest.contains("# gridcols=2"));
        assert!(manifest.contains("# header_line=Grid header"));
        assert!(manifest.contains("panel_key\tpanel_label\tseries_key\tseries_label\tx\ty\n"));
        assert!(manifest.contains("field=1\tField 1\tll\tField 1 LL\t"));
    }

    #[test]
    fn scatter_page_exports_text_manifest_for_every_item() {
        let shared_series = vec![scatter_series(
            "Amplitude",
            "amp",
            MsAxis::Amplitude,
            &[(0.0, 1.0), (1.0, 1.5), (2.0, 1.75)],
            70,
        )];
        let phase_series = vec![scatter_series(
            "Phase",
            "phase",
            MsAxis::Phase,
            &[(0.0, -15.0), (1.0, 5.0), (2.0, 30.0)],
            80,
        )];
        let page = MsScatterPagePayload {
            title: "Composite Page".to_string(),
            exprange: MsPageExportRange::All,
            gridrows: 1,
            gridcols: 2,
            header_lines: vec!["Composite header".to_string()],
            items: vec![
                MsScatterPageItemPayload {
                    plotindex: 0,
                    rowindex: 0,
                    colindex: 0,
                    plot: scatter_plot_payload(
                        "Amplitude",
                        None,
                        MsLegendPosition::ExteriorTop,
                        shared_series.clone(),
                    ),
                },
                MsScatterPageItemPayload {
                    plotindex: 1,
                    rowindex: 0,
                    colindex: 0,
                    plot: scatter_plot_payload(
                        "Phase",
                        None,
                        MsLegendPosition::ExteriorTop,
                        phase_series.clone(),
                    ),
                },
                MsScatterPageItemPayload {
                    plotindex: 2,
                    rowindex: 0,
                    colindex: 1,
                    plot: scatter_plot_payload(
                        "Amplitude Detail",
                        None,
                        MsLegendPosition::UpperRight,
                        vec![scatter_series(
                            "Amplitude Detail",
                            "detail",
                            MsAxis::Amplitude,
                            &[(0.0, 0.4), (1.0, 0.6), (2.0, 1.2)],
                            90,
                        )],
                    ),
                },
            ],
            summary: "Composite summary".to_string(),
        };

        let output_dir = tempdir().expect("tempdir");
        let txt_path = output_dir.path().join("page.txt");
        let payload = MsPlotPayload::ScatterPage(page.clone());
        export_msexplore_plot(&payload, &txt_path).expect("export scatter manifest");

        let manifest = std::fs::read_to_string(&txt_path).expect("read manifest");
        assert!(manifest.contains("# msexplore-manifest-v1"));
        assert!(manifest.contains("Composite Page"));
        assert!(manifest.contains("# header_line=Composite header"));
        assert!(manifest.contains("1\t0\t0\tPhase\t"));
        assert!(manifest.contains("2\t0\t1\tAmplitude Detail\t"));
    }

    fn overplot_item(
        plotindex: usize,
        colindex: usize,
        plot: MsScatterPlotPayload,
    ) -> MsScatterPageItemPayload {
        MsScatterPageItemPayload {
            plotindex,
            rowindex: 0,
            colindex,
            plot,
        }
    }

    #[test]
    fn page_overplots_must_share_axes_bounds_and_legend() {
        let amplitude = || {
            scatter_plot_payload(
                "Amplitude",
                None,
                MsLegendPosition::ExteriorRight,
                vec![scatter_series(
                    "Amplitude",
                    "amp",
                    MsAxis::Amplitude,
                    &[(0.0, 1.0), (1.0, 1.5)],
                    100,
                )],
            )
        };
        let by_channel = MsScatterPlotPayload {
            x_axis: MsAxis::Channel,
            x_label: "Channel".to_string(),
            ..amplitude()
        };

        let matching = [
            overplot_item(0, 0, amplitude()),
            overplot_item(1, 0, amplitude()),
        ];
        validate_page_overplots(&matching).expect("matching overplot");
        let side_by_side = [
            overplot_item(0, 0, amplitude()),
            overplot_item(1, 1, by_channel.clone()),
        ];
        validate_page_overplots(&side_by_side).expect("different cells may differ");

        let mixed_axes = [
            overplot_item(0, 0, amplitude()),
            overplot_item(1, 0, by_channel),
        ];
        let error = validate_page_overplots(&mixed_axes).expect_err("mixed axes");
        assert!(
            error.contains("cell (0, 0) mixes incompatible axes"),
            "{error}"
        );

        let bounded = MsScatterPlotPayload {
            fixed_y_bounds: Some((0.0, 2.0)),
            ..amplitude()
        };
        let mixed_bounds = [
            overplot_item(0, 0, amplitude()),
            overplot_item(1, 0, bounded),
        ];
        let error = validate_page_overplots(&mixed_bounds).expect_err("mixed bounds");
        assert!(error.contains("incompatible fixed axis bounds"), "{error}");

        let legend = MsScatterPlotPayload {
            legend_position: MsLegendPosition::UpperRight,
            ..amplitude()
        };
        let mixed_legends = [
            overplot_item(0, 0, amplitude()),
            overplot_item(1, 0, legend),
        ];
        let error = validate_page_overplots(&mixed_legends).expect_err("mixed legends");
        assert!(error.contains("incompatible legend positions"), "{error}");
    }
}
