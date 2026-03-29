// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic MeasurementSet plotting specifications and export helpers.
//!
//! `msexplore` is the next plotting layer above the curated `listobs` presets:
//!
//! - `listobs` keeps its fixed, compatibility-oriented preset catalog.
//! - `msexplore` exposes reusable plot specifications with explicit axes,
//!   selections, averaging, transforms, and export settings.
//! - Common `listobs` raw-visibility plots are lowered into the generic
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

use std::fmt;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frame::MeasFrame;
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::quanta::{Quantity, Unit};
use casacore_types::{ArrayValue, Complex64};
use image::{DynamicImage, ImageFormat, RgbImage};
use ndarray::{Ix1, Ix2};
use plotters::prelude::*;
use printpdf::{Mm, Op, PdfDocument, PdfPage, PdfSaveOptions, Pt, RawImage, XObjectTransform};
use serde::{Deserialize, Serialize};

use crate::MeasurementSet;
use crate::columns::{
    frequency_columns::ChanFreqColumn,
    main_ids,
    time_columns::TimeColumn,
    uvw_column::UvwColumn,
    weight_columns::{SigmaSpectrumColumn, WeightSpectrumColumn},
};
use crate::derived::engine::MsCalEngine;
use crate::listobs::{self, ListObsOptions, ListObsSummary, ListObsUvCoverage};
use crate::plot::{
    ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotPayload, ListObsPlotRenderStyle,
    ListObsPlotSpec, ListObsPlotTheme, VisibilityScatterPlotPayload, VisibilityScatterSeries,
    build_listobs_plot_payload_from_summary, build_listobs_uv_plot_payload, export_listobs_plot,
};
use crate::schema::main_table::VisibilityDataColumn;
use crate::subtables::SubTable;

const EXPORT_DPI: f32 = 72.0;
const SPEED_OF_LIGHT_KM_S: f64 = 299_792.458;

/// Stable preset identifiers for common MeasurementSet plots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsPlotPreset {
    /// UV coverage from grouped UVW samples.
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
            Self::UvCoverage => Some(ListObsPlotKind::UvCoverage),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsExportFormat {
    /// Raster PNG export.
    Png,
    /// Raster-backed single-page PDF export.
    Pdf,
    /// Text manifest export for parity tests and automation.
    Txt,
}

impl MsExportFormat {
    /// Parse an export-format token.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "png" => Ok(Self::Png),
            "pdf" => Ok(Self::Pdf),
            "txt" => Ok(Self::Txt),
            other => Err(format!("unsupported msexplore export format {other:?}")),
        }
    }

    /// Conventional lowercase filename extension.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Pdf => "pdf",
            Self::Txt => "txt",
        }
    }
}

/// Structured selection controls shared by CLI and library callers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsSelectionSpec {
    /// Enable the structured selectors below.
    pub selectdata: bool,
    /// FIELD selection expression.
    pub field: Option<String>,
    /// SPW selection expression.
    pub spw: Option<String>,
    /// TIMERANGE selection expression.
    pub timerange: Option<String>,
    /// UVRANGE selection expression.
    pub uvrange: Option<String>,
    /// ANTENNA selection expression.
    pub antenna: Option<String>,
    /// SCAN selection expression.
    pub scan: Option<String>,
    /// CORRELATION selection expression.
    pub correlation: Option<String>,
    /// ARRAY selection expression.
    pub array: Option<String>,
    /// OBSERVATION selection expression.
    pub observation: Option<String>,
    /// INTENT selection expression.
    pub intent: Option<String>,
    /// FEED selection expression.
    pub feed: Option<String>,
    /// Raw TaQL/MSSelection expression.
    pub msselect: Option<String>,
}

impl Default for MsSelectionSpec {
    fn default() -> Self {
        Self {
            selectdata: true,
            field: None,
            spw: None,
            timerange: None,
            uvrange: None,
            antenna: None,
            scan: None,
            correlation: None,
            array: None,
            observation: None,
            intent: None,
            feed: None,
            msselect: None,
        }
    }
}

impl MsSelectionSpec {
    /// Convert the generic selection spec into the shared `listobs` option set.
    pub fn to_listobs_options(&self) -> ListObsOptions {
        ListObsOptions {
            verbose: true,
            selectdata: self.selectdata,
            field: self.field.clone(),
            spw: self.spw.clone(),
            antenna: self.antenna.clone(),
            scan: self.scan.clone(),
            observation: self.observation.clone(),
            array: self.array.clone(),
            timerange: self.timerange.clone(),
            uvrange: self.uvrange.clone(),
            correlation: self.correlation.clone(),
            intent: self.intent.clone(),
            msselect: self.msselect.clone(),
            feed: self.feed.clone(),
            listunfl: false,
            cachesize_mb: None,
        }
    }
}

/// Averaging controls modeled after CASA `plotms`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl Default for MsAverageSpec {
    fn default() -> Self {
        Self {
            avgchannel: None,
            avgtime: None,
            avgscan: false,
            avgfield: false,
            avgbaseline: false,
            avgantenna: false,
            avgspw: false,
            scalar: false,
        }
    }
}

/// Transform controls modeled after CASA `plotms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
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

/// Supported iteration axes for multi-panel plot pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Default for MsIterationSpec {
    fn default() -> Self {
        Self {
            iteraxis: None,
            xselfscale: false,
            yselfscale: false,
            xsharedaxis: false,
            ysharedaxis: false,
        }
    }
}

/// Presentation controls for a single plot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsPlotStyleSpec {
    /// Optional plot title override.
    pub title: Option<String>,
    /// Optional X label override.
    pub xlabel: Option<String>,
    /// Optional Y label override.
    pub ylabel: Option<String>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
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
            showlegend: false,
            showmajorgrid: false,
            showminorgrid: false,
        }
    }
}

/// One staged flag-edit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsFlagEditSpec {
    /// Whether the region should be flagged or unflagged.
    pub action: MsFlagAction,
    /// Extend across correlations.
    pub extcorr: bool,
    /// Extend across channels.
    pub extchannel: bool,
}

/// Supported flag-edit actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsFlagAction {
    /// Mark matching samples as flagged.
    Flag,
    /// Clear flags from matching samples.
    Unflag,
}

/// One plot request on a page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub fn from_preset(preset: MsPlotPreset) -> Self {
        match preset {
            MsPlotPreset::UvCoverage
            | MsPlotPreset::AntennaLayout
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
        if self.flag_edit.is_some() {
            return Err(
                "msexplore staged flag editing is modeled but not yet implemented".to_string(),
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
                "msexplore currently supports avgchannel and scalar/vector averaging; other averaging controls are reserved for future waves".to_string(),
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
            && !matches!(
                self.x_axis,
                MsAxis::Channel | MsAxis::Frequency | MsAxis::Velocity
            )
        {
            return Err(
                "msexplore avgchannel currently requires xaxis=channel, xaxis=frequency, or xaxis=velocity"
                    .to_string(),
            );
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsExploreSpec {
    /// MeasurementSet root directory.
    pub ms_path: PathBuf,
    /// Human/machine-readable summary format.
    pub summary_format: crate::listobs::ListObsOutputFormat,
    /// Shared row-selection controls.
    pub selection: MsSelectionSpec,
    /// Optional page title override when composing multiple plots.
    pub page_title: Option<String>,
    /// Range behavior when exporting multi-plot pages.
    pub exprange: MsPageExportRange,
    /// Plot definitions on the page.
    pub plots: Vec<MsPlotSpec>,
}

impl MsExploreSpec {
    /// Validate the high-level request.
    pub fn validate(&self) -> Result<(), String> {
        if self.plots.is_empty() {
            return Err("msexplore requires at least one plot per invocation".to_string());
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
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
    /// Grouped scatter series.
    pub series: Vec<MsScatterSeries>,
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
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
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
    /// Ordered child plots placed on the page.
    pub items: Vec<MsScatterPageItemPayload>,
    /// Human-readable page summary.
    pub summary: String,
}

fn build_msexplore_plot_payload_validated(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    if spec.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) {
        return build_stacked_amplitude_phase_time_page(ms, selection, spec);
    }
    if let Some(listobs_kind) = spec
        .preset
        .and_then(MsPlotPreset::lowers_to_listobs_metadata)
    {
        let listobs_options = selection.to_listobs_options();
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

    build_generic_visibility_scatter(ms, selection, spec)
}

/// Build a plot payload from a full `msexplore` request.
pub fn build_msexplore_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    if spec.plots.len() == 1 {
        return build_msexplore_plot_payload_validated(ms, &spec.selection, &spec.plots[0]);
    }

    let (gridrows, gridcols) = {
        let layout = &spec.plots[0].layout;
        (layout.gridrows, layout.gridcols)
    };
    let mut items = Vec::with_capacity(spec.plots.len());
    for plot in &spec.plots {
        let payload = build_msexplore_plot_payload_validated(ms, &spec.selection, plot)?;
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
    let title = spec
        .page_title
        .clone()
        .unwrap_or_else(|| "MeasurementSet Multi-Plot Page".to_string());

    Ok(MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title,
        exprange: spec.exprange,
        gridrows,
        gridcols,
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
    }))
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

/// Build a generic plot payload from one open MeasurementSet.
pub fn build_msexplore_plot_payload(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    build_msexplore_plot_payload_validated(ms, selection, spec)
}

/// Open a MeasurementSet path and build the requested plot payload.
pub fn build_msexplore_plot_payload_from_path(
    path: &Path,
    selection: &MsSelectionSpec,
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

/// Lower one `listobs` raw-visibility preset into the generic scatter engine.
pub(crate) fn build_listobs_compat_visibility_payload(
    ms: &MeasurementSet,
    options: &ListObsOptions,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let preset = MsPlotPreset::from_listobs_kind(spec.kind);
    let color_by = MsColorAxis::parse(spec.option("color_by").unwrap_or(match spec.kind {
        ListObsPlotKind::AmplitudeVsUvDistance => "spw",
        _ => "field",
    }))?;
    let data_column = MsDataColumn::parse(spec.option("data_column").unwrap_or("data"))?;
    let plot_spec = MsPlotSpec {
        preset: Some(preset),
        x_axis: MsPlotSpec::from_preset(preset).x_axis,
        y_axes: MsPlotSpec::from_preset(preset).y_axes,
        data_column,
        color_by,
        averaging: MsAverageSpec::default(),
        transforms: MsTransformSpec::default(),
        layout: MsLayoutSpec::default(),
        iteration: MsIterationSpec::default(),
        style: MsPlotStyleSpec::default(),
        flag_edit: None,
    };
    let selection = MsSelectionSpec {
        selectdata: options.selectdata,
        field: options.field.clone(),
        spw: options.spw.clone(),
        timerange: options.timerange.clone(),
        uvrange: options.uvrange.clone(),
        antenna: options.antenna.clone(),
        scan: options.scan.clone(),
        correlation: options.correlation.clone(),
        array: options.array.clone(),
        observation: options.observation.clone(),
        intent: options.intent.clone(),
        feed: options.feed.clone(),
        msselect: options.msselect.clone(),
    };
    match build_msexplore_plot_payload(ms, &selection, &plot_spec)? {
        MsPlotPayload::Scatter(payload) => Ok(ListObsPlotPayload::VisibilityScatter(
            VisibilityScatterPlotPayload {
                kind: spec.kind,
                x_label: payload.x_label,
                y_label: payload.y_label,
                fixed_y_bounds: payload.fixed_y_bounds,
                series: payload
                    .series
                    .into_iter()
                    .map(|series| VisibilityScatterSeries {
                        label: series.label,
                        color_group: series.color_group,
                        points: series.points,
                    })
                    .collect(),
                summary: payload.summary,
            },
        )),
        MsPlotPayload::ListObs(_)
        | MsPlotPayload::ScatterGrid(_)
        | MsPlotPayload::ScatterPage(_) => {
            Err("internal error: raw listobs preset lowered to a metadata payload".to_string())
        }
    }
}

/// Render one plot payload into an in-memory bitmap.
pub fn render_msexplore_plot_image(
    payload: &MsPlotPayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    match payload {
        MsPlotPayload::ListObs(payload) => {
            crate::plot::render_listobs_plot_image(payload, theme, width, height)
        }
        MsPlotPayload::Scatter(payload) => render_scatter_image(payload, theme, width, height),
        MsPlotPayload::ScatterGrid(payload) => {
            render_scatter_grid_image(payload, theme, width, height)
        }
        MsPlotPayload::ScatterPage(payload) => {
            render_scatter_page_image(payload, theme, width, height)
        }
    }
}

/// Export one plot payload as `png`, `pdf`, or `txt`.
pub fn export_msexplore_plot(
    payload: &MsPlotPayload,
    theme: ListObsPlotTheme,
    output_path: &Path,
    format: MsExportFormat,
    width: u32,
    height: u32,
) -> Result<(), String> {
    match (payload, format) {
        (MsPlotPayload::ListObs(payload), MsExportFormat::Png) => export_listobs_plot(
            payload,
            theme,
            output_path,
            ListObsPlotExportFormat::Png,
            width,
            height,
        ),
        (MsPlotPayload::ListObs(payload), MsExportFormat::Pdf) => export_listobs_plot(
            payload,
            theme,
            output_path,
            ListObsPlotExportFormat::Pdf,
            width,
            height,
        ),
        (MsPlotPayload::ListObs(_), MsExportFormat::Txt) => Err(
            "text export is currently available for raw msexplore scatter plots only".to_string(),
        ),
        (MsPlotPayload::Scatter(_), MsExportFormat::Txt)
        | (MsPlotPayload::ScatterGrid(_), MsExportFormat::Txt)
        | (MsPlotPayload::ScatterPage(_), MsExportFormat::Txt) => {
            std::fs::write(output_path, render_scatter_manifest(payload)?)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::Scatter(payload), MsExportFormat::Png) => {
            render_scatter_image(payload, theme, width, height)?
                .save_with_format(output_path, ImageFormat::Png)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::ScatterGrid(payload), MsExportFormat::Png) => {
            render_scatter_grid_image(payload, theme, width, height)?
                .save_with_format(output_path, ImageFormat::Png)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::ScatterPage(payload), MsExportFormat::Png) => {
            render_scatter_page_image(payload, theme, width, height)?
                .save_with_format(output_path, ImageFormat::Png)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::Scatter(payload), MsExportFormat::Pdf) => {
            let image = render_scatter_image(payload, theme, width, height)?;
            export_scatter_pdf(&image, output_path, &payload.title)
        }
        (MsPlotPayload::ScatterGrid(payload), MsExportFormat::Pdf) => {
            let image = render_scatter_grid_image(payload, theme, width, height)?;
            export_scatter_pdf(&image, output_path, &payload.title)
        }
        (MsPlotPayload::ScatterPage(payload), MsExportFormat::Pdf) => {
            let image = render_scatter_page_image(payload, theme, width, height)?;
            export_scatter_pdf(&image, output_path, &payload.title)
        }
    }
}

fn render_scatter_manifest(payload: &MsPlotPayload) -> Result<String, String> {
    match payload {
        MsPlotPayload::Scatter(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
            out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
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
            out.push_str(&format!("# share_x_bounds={}\n", payload.share_x_bounds));
            out.push_str(&format!("# share_y_bounds={}\n", payload.share_y_bounds));
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

#[derive(Debug, Default)]
struct ScatterPanelAccumulator {
    label: String,
    series: std::collections::BTreeMap<String, MsScatterSeries>,
    contributing_rows: usize,
    contributing_points: usize,
}

fn build_generic_visibility_scatter(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    let listobs_options = selection.to_listobs_options();
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
        Some(PreparedDataSource::new(ms, spec.data_column)?)
    } else {
        None
    };
    let flag = ms.flag_column();
    let flag_row = ms.flag_row_column();
    let weight = ms.weight_column();
    let sigma = ms.sigma_column();
    let weight_spectrum = WeightSpectrumColumn::new(ms.main_table()).ok();
    let sigma_spectrum = SigmaSpectrumColumn::new(ms.main_table()).ok();
    let time = TimeColumn::new(ms.main_table());
    let uvw = UvwColumn::new(ms.main_table());
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
    let field_id = main_ids::field_id(ms.main_table());
    let scan_number = main_ids::scan_number(ms.main_table());
    let data_desc_id = main_ids::data_desc_id(ms.main_table());
    let antenna1 = main_ids::antenna1(ms.main_table());
    let antenna2 = main_ids::antenna2(ms.main_table());

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
        .map(listobs::parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

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

    for row in row_numbers {
        let flag_row_value = flag_row.get(row).map_err(|error| error.to_string())?;
        if flag_row_value && !include_row_flagged_points {
            continue;
        }

        let ddid = data_desc_id.get(row).map_err(|error| error.to_string())?;
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

        let flags = match flag.get(row).map_err(|error| error.to_string())? {
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
            .map(|source| source.row(row))
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
        let weight_values = float_axis_values(
            weight.get(row).map_err(|error| error.to_string())?,
            corr_count,
            "WEIGHT",
        )?;
        let sigma_values = float_axis_values(
            sigma.get(row).map_err(|error| error.to_string())?,
            corr_count,
            "SIGMA",
        )?;
        let weight_spectrum_grid = if needs_weight_spectrum {
            Some(
                weight_spectrum
                    .as_ref()
                    .map(|column| {
                        float_grid_from_array(
                            column.get(row).map_err(|error| error.to_string())?,
                            "WEIGHT_SPECTRUM",
                        )
                    })
                    .transpose()?
                    .unwrap_or_else(|| scalar_values_to_grid(&weight_values, chan_count)),
            )
        } else {
            None
        };
        let sigma_spectrum_grid = if needs_sigma_spectrum {
            Some(
                sigma_spectrum
                    .as_ref()
                    .map(|column| {
                        float_grid_from_array(
                            column.get(row).map_err(|error| error.to_string())?,
                            "SIGMA_SPECTRUM",
                        )
                    })
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
            if let Some(grid) = grid {
                if grid.corr_count != corr_count || grid.chan_count != chan_count {
                    return Err(format!(
                        "{column_name} shape [{}, {}] does not match data shape [{corr_count}, {chan_count}]",
                        grid.corr_count, grid.chan_count
                    ));
                }
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

        let field_id_value = field_id.get(row).map_err(|error| error.to_string())?;
        let scan_number_value = scan_number.get(row).map_err(|error| error.to_string())?;
        let antenna1_value = antenna1.get(row).map_err(|error| error.to_string())?;
        let antenna2_value = antenna2.get(row).map_err(|error| error.to_string())?;
        let row_time_value = time
            .get_mjd_seconds(row)
            .map_err(|error| error.to_string())?;
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
            let row_weight = weight_values[*corr_index];
            let row_sigma = sigma_values[*corr_index];
            for bin in &channel_bins {
                let samples = grid
                    .as_ref()
                    .map(|grid| {
                        collect_bin_samples(grid, &flags, &[*corr_index], bin.start, bin.end)
                    })
                    .unwrap_or_default();
                let flag_samples = collect_bin_flags(&flags, *corr_index, bin.start, bin.end);
                let weight_spectrum_samples = weight_spectrum_grid
                    .as_ref()
                    .map(|grid| {
                        collect_bin_float_samples(grid, &flags, *corr_index, bin.start, bin.end)
                    })
                    .unwrap_or_default();
                let sigma_spectrum_samples = sigma_spectrum_grid
                    .as_ref()
                    .map(|grid| {
                        collect_bin_float_samples(grid, &flags, *corr_index, bin.start, bin.end)
                    })
                    .unwrap_or_default();
                let Some(x_value) = compute_axis_value(
                    spec.x_axis,
                    row,
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
                    &time,
                    &uvw,
                    derived_engine.as_ref(),
                )?
                else {
                    continue;
                };
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
                let panel = panels
                    .get_mut(&panel_key)
                    .expect("panel inserted before mutation");
                for y_axis in spec.y_axes.iter().copied() {
                    let Some(y_value) = compute_axis_value(
                        y_axis,
                        row,
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
                        &time,
                        &uvw,
                        derived_engine.as_ref(),
                    )?
                    else {
                        continue;
                    };
                    let (series_key, series_label, color_group) = scatter_series_identity(
                        y_axis,
                        spec.y_axes.len() > 1,
                        &group_key,
                        &group_label,
                    );
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
                    panel
                        .series
                        .entry(series_key)
                        .or_insert_with(|| MsScatterSeries {
                            label: series_label,
                            color_group,
                            y_axis,
                            points: Vec::new(),
                        })
                        .points
                        .push((x_value, y_value));
                    panel.contributing_points += 1;
                    contributing_points += 1;
                    row_contributed = true;
                    row_panels.insert(panel_key.clone());
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
            showmajorgrid: spec.style.showmajorgrid,
            showminorgrid: spec.style.showminorgrid,
            iteraxis,
            gridrows,
            gridcols,
            share_x_bounds,
            share_y_bounds,
            summary: format!(
                "{}. Panels={} Rows={} Points={} Data column={}",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("MeasurementSet iterated plot"),
                panels.len(),
                contributing_rows,
                contributing_points,
                spec.data_column
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
        showmajorgrid: spec.style.showmajorgrid,
        showminorgrid: spec.style.showminorgrid,
        summary: format!(
            "{}. Rows={} Points={} Data column={}",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            spec.data_column
        ),
        series: panel.series,
    }))
}

fn build_stacked_amplitude_phase_time_page(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
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
    )?;
    let phase = build_stacked_page_child(
        ms,
        selection,
        spec,
        MsPlotPreset::PhaseVsTime,
        MsAxis::Phase,
        "Phase vs Time",
    )?;

    Ok(MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title: page_title,
        exprange: MsPageExportRange::Current,
        gridrows: 2,
        gridcols: 1,
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
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
    preset: MsPlotPreset,
    y_axis: MsAxis,
    title: &str,
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
    match build_generic_visibility_scatter(ms, selection, &child)? {
        MsPlotPayload::Scatter(payload) => Ok(payload),
        _ => Err("internal error: stacked subplot lowered to a non-scatter payload".to_string()),
    }
}

fn resolve_selected_rows_with_msselect(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    listobs_options: &ListObsOptions,
) -> Result<Vec<usize>, String> {
    if !selection.selectdata
        || (!listobs_options.has_selection()
            && selection
                .msselect
                .as_deref()
                .is_none_or(|value| value.trim().is_empty()))
    {
        return Ok((0..ms.row_count()).collect());
    }

    let selected = if selection
        .msselect
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        let selection_builder = listobs::selection_from_options(ms, listobs_options)
            .map_err(|error| error.to_string())?;
        selection_builder
            .apply(ms)
            .map_err(|error| error.to_string())?
    } else {
        listobs::resolve_selected_rows(ms, listobs_options)
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| (0..ms.row_count()).collect())
    };
    Ok(selected)
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

fn parse_rest_frequency_hz(value: &str) -> Result<f64, String> {
    let quantity = value
        .trim()
        .parse::<Quantity>()
        .map_err(|error| format!("invalid restfreq {value:?}: {error}"))?;
    if quantity.unit().name().is_empty() {
        return Ok(quantity.value() * 1.0e6);
    }
    let hz = quantity
        .get_value_in(&Unit::new("Hz").expect("Hz is a valid unit"))
        .map_err(|error| format!("invalid restfreq {value:?}: {error}"))?;
    if hz <= 0.0 || !hz.is_finite() {
        return Err(format!(
            "restfreq must resolve to a positive finite Hz value, got {hz}"
        ));
    }
    Ok(hz)
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
        let frame = derived_engine
            .spectral_frame_observatory(row_time_value, field_id)
            .map_err(|error| error.to_string())?;
        source_channel_frequencies
            .iter()
            .map(|frequency| {
                frequency
                    .convert_to(target_ref, &frame)
                    .map(|value| value.hz())
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
        .transpose()?
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
    row: usize,
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
    time: &TimeColumn<'_>,
    uvw: &UvwColumn<'_>,
    geometry_engine: Option<&MsCalEngine>,
) -> Result<Option<f64>, String> {
    if axis.is_visibility_math() {
        return Ok(compute_visibility_math(axis, samples, scalar_average));
    }
    match axis {
        MsAxis::Time => time
            .get_mjd_seconds(row)
            .map(Some)
            .map_err(|error| error.to_string()),
        MsAxis::UvDistance => {
            let [u, v, _w] = uvw.get(row).map_err(|error| error.to_string())?;
            Ok(Some((u * u + v * v).sqrt()))
        }
        MsAxis::U => {
            let [u, _v, _w] = uvw.get(row).map_err(|error| error.to_string())?;
            Ok(Some(u))
        }
        MsAxis::V => {
            let [_u, v, _w] = uvw.get(row).map_err(|error| error.to_string())?;
            Ok(Some(v))
        }
        MsAxis::W => {
            let [_u, _v, w] = uvw.get(row).map_err(|error| error.to_string())?;
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
            let doppler = MDoppler::new(
                mean_frequency_hz / spectral_context.rest_frequency_hz,
                DopplerRef::RATIO,
            )
            .convert_to(spectral_context.doppler_ref, &MeasFrame::new())
            .map_err(|error| error.to_string())?;
            Ok(Some(doppler.value() * SPEED_OF_LIGHT_KM_S))
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
            let time_value = time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for azimuth axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for azimuth axis"))?;
            geometry_engine
                .azel(time_value, field_id, antenna_id)
                .map(|(azimuth, _elevation)| Some(normalize_signed_degrees(azimuth.to_degrees())))
                .map_err(|error| error.to_string())
        }
        MsAxis::Elevation => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for elevation axis".to_string()
            })?;
            let time_value = time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for elevation axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for elevation axis"))?;
            geometry_engine
                .azel(time_value, field_id, antenna_id)
                .map(|(_azimuth, elevation)| Some(elevation.to_degrees()))
                .map_err(|error| error.to_string())
        }
        MsAxis::HourAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for hour angle axis".to_string()
            })?;
            let time_value = time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for hour angle axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for hour angle axis"))?;
            geometry_engine
                .hour_angle(time_value, field_id, antenna_id)
                .map(|hour_angle| Some(hour_angle * 12.0 / std::f64::consts::PI))
                .map_err(|error| error.to_string())
        }
        MsAxis::ParallacticAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for parallactic angle axis".to_string()
            })?;
            let time_value = time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?;
            let field_id = usize::try_from(field_id_value).map_err(|_| {
                format!("invalid FIELD_ID {field_id_value} for parallactic angle axis")
            })?;
            let antenna_id = usize::try_from(antenna1_value).map_err(|_| {
                format!("invalid ANTENNA1 {antenna1_value} for parallactic angle axis")
            })?;
            geometry_engine
                .parallactic_angle(time_value, field_id, antenna_id)
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

fn collect_bin_samples(
    grid: &ComplexGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_indices: &[usize],
    chan_start: usize,
    chan_end: usize,
) -> Vec<Complex64> {
    let mut samples = Vec::new();
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
    samples
}

fn collect_bin_flags(
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    chan_start: usize,
    chan_end: usize,
) -> Vec<bool> {
    if corr_index >= flags.nrows() {
        return Vec::new();
    }
    (chan_start..chan_end)
        .filter(|chan_index| *chan_index < flags.ncols())
        .map(|chan_index| flags[(corr_index, chan_index)])
        .collect()
}

fn collect_bin_float_samples(
    grid: &FloatGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    chan_start: usize,
    chan_end: usize,
) -> Vec<f64> {
    if corr_index >= grid.corr_count {
        return Vec::new();
    }
    let mut samples = Vec::new();
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
    samples
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

enum PreparedDataSource<'a> {
    Single(crate::columns::data_columns::DataColumn<'a>),
    Difference {
        left: crate::columns::data_columns::DataColumn<'a>,
        right: crate::columns::data_columns::DataColumn<'a>,
        scalar: bool,
    },
    Ratio {
        numerator: crate::columns::data_columns::DataColumn<'a>,
        denominator: crate::columns::data_columns::DataColumn<'a>,
        scalar: bool,
    },
}

impl<'a> PreparedDataSource<'a> {
    fn new(ms: &'a MeasurementSet, column: MsDataColumn) -> Result<Self, String> {
        match column {
            MsDataColumn::Data => Ok(Self::Single(
                ms.data_column(VisibilityDataColumn::Data)
                    .map_err(|error| error.to_string())?,
            )),
            MsDataColumn::Corrected => Ok(Self::Single(
                ms.data_column(VisibilityDataColumn::CorrectedData)
                    .map_err(|error| error.to_string())?,
            )),
            MsDataColumn::Model => Ok(Self::Single(
                ms.data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
            )),
            MsDataColumn::CorrectedMinusModel => Ok(Self::Difference {
                left: ms
                    .data_column(VisibilityDataColumn::CorrectedData)
                    .map_err(|error| error.to_string())?,
                right: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: false,
            }),
            MsDataColumn::CorrectedMinusModelScalar => Ok(Self::Difference {
                left: ms
                    .data_column(VisibilityDataColumn::CorrectedData)
                    .map_err(|error| error.to_string())?,
                right: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: true,
            }),
            MsDataColumn::DataMinusModel => Ok(Self::Difference {
                left: ms
                    .data_column(VisibilityDataColumn::Data)
                    .map_err(|error| error.to_string())?,
                right: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: false,
            }),
            MsDataColumn::DataMinusModelScalar => Ok(Self::Difference {
                left: ms
                    .data_column(VisibilityDataColumn::Data)
                    .map_err(|error| error.to_string())?,
                right: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: true,
            }),
            MsDataColumn::CorrectedDivModel => Ok(Self::Ratio {
                numerator: ms
                    .data_column(VisibilityDataColumn::CorrectedData)
                    .map_err(|error| error.to_string())?,
                denominator: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: false,
            }),
            MsDataColumn::CorrectedDivModelScalar => Ok(Self::Ratio {
                numerator: ms
                    .data_column(VisibilityDataColumn::CorrectedData)
                    .map_err(|error| error.to_string())?,
                denominator: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: true,
            }),
            MsDataColumn::DataDivModel => Ok(Self::Ratio {
                numerator: ms
                    .data_column(VisibilityDataColumn::Data)
                    .map_err(|error| error.to_string())?,
                denominator: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: false,
            }),
            MsDataColumn::DataDivModelScalar => Ok(Self::Ratio {
                numerator: ms
                    .data_column(VisibilityDataColumn::Data)
                    .map_err(|error| error.to_string())?,
                denominator: ms
                    .data_column(VisibilityDataColumn::ModelData)
                    .map_err(|error| error.to_string())?,
                scalar: true,
            }),
        }
    }

    fn row(&self, row: usize) -> Result<ComplexGrid, String> {
        match self {
            Self::Single(column) => {
                complex_grid_from_array(column.get(row).map_err(|error| error.to_string())?)
            }
            Self::Difference {
                left,
                right,
                scalar,
            } => {
                let left =
                    complex_grid_from_array(left.get(row).map_err(|error| error.to_string())?)?;
                let right =
                    complex_grid_from_array(right.get(row).map_err(|error| error.to_string())?)?;
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
                let numerator = complex_grid_from_array(
                    numerator.get(row).map_err(|error| error.to_string())?,
                )?;
                let denominator = complex_grid_from_array(
                    denominator.get(row).map_err(|error| error.to_string())?,
                )?;
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
            .flat_map(|value| std::iter::repeat(*value).take(chan_count))
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

fn render_scatter_image(
    payload: &MsScatterPlotPayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    render_scatter_panel(
        &root,
        payload.x_axis,
        payload.y_axis,
        payload.secondary_y_axis,
        &payload.x_label,
        &payload.y_label,
        payload.secondary_y_label.as_deref(),
        payload.fixed_x_bounds,
        payload.fixed_y_bounds,
        payload.secondary_fixed_y_bounds,
        &payload.series,
        None,
        theme,
        style,
        payload.showlegend,
        payload.showmajorgrid,
        payload.showminorgrid,
        None,
    )?;
    root.present().map_err(|error| error.to_string())?;
    drop(root);
    let image = RgbImage::from_raw(width, height, buffer)
        .ok_or_else(|| "failed to assemble rendered plot image".to_string())?;
    Ok(DynamicImage::ImageRgb8(image))
}

fn render_scatter_grid_image(
    payload: &MsScatterGridPayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    let titled = root
        .titled(
            &payload.title,
            ("sans-serif", style.axis_desc_font_px().saturating_add(2))
                .into_font()
                .color(&rgb(theme.label)),
        )
        .map_err(|error| error.to_string())?;
    let areas = titled.split_evenly((payload.gridrows, payload.gridcols));
    let global_bounds = if payload.share_x_bounds || payload.share_y_bounds {
        Some(scatter_bounds(
            payload.panels.iter().flat_map(|panel| {
                panel
                    .series
                    .iter()
                    .flat_map(|series| series.points.iter().copied())
            }),
            payload.fixed_x_bounds,
            payload.fixed_y_bounds,
        )?)
    } else {
        None
    };
    for (area, panel) in areas.iter().zip(payload.panels.iter()) {
        let local_bounds = scatter_bounds(
            panel
                .series
                .iter()
                .flat_map(|series| series.points.iter().copied()),
            payload.fixed_x_bounds,
            payload.fixed_y_bounds,
        )?;
        let resolved_bounds = match global_bounds {
            Some((global_min_x, global_max_x, global_min_y, global_max_y)) => Some((
                if payload.share_x_bounds {
                    global_min_x
                } else {
                    local_bounds.0
                },
                if payload.share_x_bounds {
                    global_max_x
                } else {
                    local_bounds.1
                },
                if payload.share_y_bounds {
                    global_min_y
                } else {
                    local_bounds.2
                },
                if payload.share_y_bounds {
                    global_max_y
                } else {
                    local_bounds.3
                },
            )),
            None => Some(local_bounds),
        };
        render_scatter_panel(
            area,
            payload.x_axis,
            payload.y_axis,
            None,
            &payload.x_label,
            &payload.y_label,
            None,
            payload.fixed_x_bounds,
            payload.fixed_y_bounds,
            None,
            &panel.series,
            Some(&panel.label),
            theme,
            style,
            payload.showlegend,
            payload.showmajorgrid,
            payload.showminorgrid,
            resolved_bounds,
        )?;
    }
    root.present().map_err(|error| error.to_string())?;
    drop(areas);
    drop(titled);
    drop(root);
    let image = RgbImage::from_raw(width, height, buffer)
        .ok_or_else(|| "failed to assemble rendered plot image".to_string())?;
    Ok(DynamicImage::ImageRgb8(image))
}

#[derive(Debug, Clone)]
struct ScatterPageCellRender {
    rowindex: usize,
    colindex: usize,
    x_axis: MsAxis,
    y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    x_label: String,
    y_label: String,
    secondary_y_label: Option<String>,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    secondary_fixed_y_bounds: Option<(f64, f64)>,
    showlegend: bool,
    showmajorgrid: bool,
    showminorgrid: bool,
    title: String,
    series: Vec<MsScatterSeries>,
}

fn resolve_scatter_page_cells(
    payload: &MsScatterPagePayload,
) -> Result<Vec<ScatterPageCellRender>, String> {
    let mut cells =
        std::collections::BTreeMap::<(usize, usize), Vec<&MsScatterPageItemPayload>>::new();
    for item in &payload.items {
        cells
            .entry((item.rowindex, item.colindex))
            .or_default()
            .push(item);
    }

    let mut resolved = Vec::with_capacity(cells.len());
    for ((rowindex, colindex), mut items) in cells {
        items.sort_by_key(|item| item.plotindex);
        let first = &items[0].plot;
        for item in items.iter().skip(1) {
            if item.plot.x_axis != first.x_axis
                || item.plot.y_axis != first.y_axis
                || item.plot.secondary_y_axis != first.secondary_y_axis
            {
                return Err(format!(
                    "scatter page cell ({rowindex}, {colindex}) mixes incompatible axes; overplots currently require matching x/y axis configurations"
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
        }

        let overplotted = items.len() > 1;
        let mut title_parts = Vec::<String>::new();
        let mut series = Vec::<MsScatterSeries>::new();
        let showlegend = items.iter().any(|item| item.plot.showlegend);
        let showmajorgrid = items.iter().any(|item| item.plot.showmajorgrid);
        let showminorgrid = items.iter().any(|item| item.plot.showminorgrid);

        for item in items {
            let item_title = if item.plot.title.trim().is_empty() {
                format!("Plot {}", item.plotindex)
            } else {
                item.plot.title.clone()
            };
            if !title_parts.iter().any(|part| part == &item_title) {
                title_parts.push(item_title.clone());
            }
            for child_series in &item.plot.series {
                let (label, color_group) = if overplotted {
                    (
                        format!("{} · {}", item_title, child_series.label),
                        format!(
                            "plot{}::{}::{}",
                            item.plotindex, item_title, child_series.color_group
                        ),
                    )
                } else {
                    (child_series.label.clone(), child_series.color_group.clone())
                };
                series.push(MsScatterSeries {
                    label,
                    color_group,
                    y_axis: child_series.y_axis,
                    points: child_series.points.clone(),
                });
            }
        }

        resolved.push(ScatterPageCellRender {
            rowindex,
            colindex,
            x_axis: first.x_axis,
            y_axis: first.y_axis,
            secondary_y_axis: first.secondary_y_axis,
            x_label: first.x_label.clone(),
            y_label: first.y_label.clone(),
            secondary_y_label: first.secondary_y_label.clone(),
            fixed_x_bounds: first.fixed_x_bounds,
            fixed_y_bounds: first.fixed_y_bounds,
            secondary_fixed_y_bounds: first.secondary_fixed_y_bounds,
            showlegend,
            showmajorgrid,
            showminorgrid,
            title: title_parts.join(", "),
            series,
        });
    }
    resolved.sort_by_key(|cell| (cell.rowindex, cell.colindex));
    Ok(resolved)
}

fn render_scatter_page_image(
    payload: &MsScatterPagePayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    let titled = root
        .titled(
            &payload.title,
            ("sans-serif", style.axis_desc_font_px().saturating_add(2))
                .into_font()
                .color(&rgb(theme.label)),
        )
        .map_err(|error| error.to_string())?;
    let areas = titled.split_evenly((payload.gridrows, payload.gridcols));
    let cells = resolve_scatter_page_cells(payload)?;
    let global_bounds = if payload.exprange == MsPageExportRange::All {
        Some(scatter_bounds(
            cells.iter().flat_map(|cell| {
                cell.series
                    .iter()
                    .flat_map(|series| series.points.iter().copied())
            }),
            None,
            None,
        )?)
    } else {
        None
    };
    for cell in &cells {
        if cell.rowindex >= payload.gridrows || cell.colindex >= payload.gridcols {
            return Err(format!(
                "scatter page cell ({}, {}) is placed outside the {}x{} page grid",
                cell.rowindex, cell.colindex, payload.gridrows, payload.gridcols
            ));
        }
        let area_index = cell.rowindex * payload.gridcols + cell.colindex;
        let area = areas.get(area_index).ok_or_else(|| {
            format!(
                "scatter page cell ({}, {}) could not resolve drawing area {}",
                cell.rowindex, cell.colindex, area_index
            )
        })?;
        let resolved_bounds = match global_bounds {
            Some((min_x, max_x, min_y, max_y)) => Some((min_x, max_x, min_y, max_y)),
            None => None,
        };
        render_scatter_panel(
            area,
            cell.x_axis,
            cell.y_axis,
            cell.secondary_y_axis,
            &cell.x_label,
            &cell.y_label,
            cell.secondary_y_label.as_deref(),
            cell.fixed_x_bounds,
            cell.fixed_y_bounds,
            cell.secondary_fixed_y_bounds,
            &cell.series,
            Some(&cell.title),
            theme,
            style,
            cell.showlegend,
            cell.showmajorgrid,
            cell.showminorgrid,
            resolved_bounds,
        )?;
    }
    root.present().map_err(|error| error.to_string())?;
    drop(areas);
    drop(titled);
    drop(root);
    let image = RgbImage::from_raw(width, height, buffer)
        .ok_or_else(|| "failed to assemble rendered plot image".to_string())?;
    Ok(DynamicImage::ImageRgb8(image))
}

fn export_scatter_pdf(image: &DynamicImage, output_path: &Path, title: &str) -> Result<(), String> {
    let mut png_bytes = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut png_bytes), ImageFormat::Png)
        .map_err(|error| error.to_string())?;
    let raw = RawImage::decode_from_bytes(&png_bytes, &mut Vec::new())
        .map_err(|error| error.to_string())?;

    let page_width_mm = Mm((image.width() as f32) * 25.4 / EXPORT_DPI);
    let page_height_mm = Mm((image.height() as f32) * 25.4 / EXPORT_DPI);

    let mut document = PdfDocument::new(title);
    let image_id = document.add_image(&raw);
    let page = PdfPage::new(
        page_width_mm,
        page_height_mm,
        vec![Op::UseXobject {
            id: image_id,
            transform: XObjectTransform {
                translate_x: Some(Pt(0.0)),
                translate_y: Some(Pt(0.0)),
                rotate: None,
                scale_x: None,
                scale_y: None,
                dpi: Some(EXPORT_DPI),
            },
        }],
    );
    let bytes = document
        .with_pages(vec![page])
        .save(&PdfSaveOptions::default(), &mut Vec::new());
    std::fs::write(output_path, bytes).map_err(|error| error.to_string())
}

fn render_scatter_panel(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    x_axis: MsAxis,
    y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    x_label: &str,
    y_label: &str,
    secondary_y_label: Option<&str>,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    secondary_fixed_y_bounds: Option<(f64, f64)>,
    series: &[MsScatterSeries],
    panel_title: Option<&str>,
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
    showlegend: bool,
    showmajorgrid: bool,
    showminorgrid: bool,
    bounds_override: Option<(f64, f64, f64, f64)>,
) -> Result<(), String> {
    let mut chart_builder = ChartBuilder::on(root);
    chart_builder
        .margin(style.margin_px())
        .x_label_area_size(style.label_area_px())
        .y_label_area_size(style.wide_y_label_area_px());
    if secondary_y_axis.is_some() {
        chart_builder.right_y_label_area_size(style.wide_y_label_area_px());
    }
    if let Some(panel_title) = panel_title {
        chart_builder.caption(panel_title, ("sans-serif", style.axis_desc_font_px()));
    }
    let point_radius = style.point_radius_px().saturating_sub(1).max(3);
    if let Some(secondary_y_axis) = secondary_y_axis {
        let primary_axis_label_color =
            scatter_axis_color(y_axis, y_axis, Some(secondary_y_axis), theme);
        let secondary_axis_label_color =
            scatter_axis_color(secondary_y_axis, y_axis, Some(secondary_y_axis), theme);
        let primary_series = series
            .iter()
            .filter(|series| series.y_axis == y_axis)
            .collect::<Vec<_>>();
        let secondary_series = series
            .iter()
            .filter(|series| series.y_axis == secondary_y_axis)
            .collect::<Vec<_>>();
        let (min_x, max_x) = axis_bounds(
            series
                .iter()
                .flat_map(|series| series.points.iter().map(|(x, _)| *x)),
            fixed_x_bounds,
        )?;
        let (min_y, max_y) = axis_bounds(
            primary_series
                .iter()
                .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
            fixed_y_bounds,
        )?;
        let (secondary_min_y, secondary_max_y) = axis_bounds(
            secondary_series
                .iter()
                .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
            secondary_fixed_y_bounds,
        )?;
        let x_offset = if x_axis == MsAxis::Time {
            scan_timeline_axis_offset(min_x, max_x)
        } else {
            0.0
        };
        let rendered_x_label = if x_offset == 0.0 {
            x_label.to_string()
        } else {
            format!("Time (MJD seconds - {:.0})", x_offset)
        };
        let x_span = (max_x - min_x).abs();
        let y_span = (max_y - min_y).abs();
        let secondary_y_span = (secondary_max_y - secondary_min_y).abs();
        let x_range = (min_x - x_offset)..(max_x - x_offset);
        let x_label_formatter = |value: &f64| format_numeric_tick(*value, x_span);
        let y_label_formatter = |value: &f64| format_numeric_tick(*value, y_span);
        let secondary_y_label_formatter =
            |value: &f64| format_numeric_tick(*value, secondary_y_span);

        let mut chart = chart_builder
            .build_cartesian_2d(x_range.clone(), min_y..max_y)
            .map_err(|error| error.to_string())?
            .set_secondary_coord(x_range, secondary_min_y..secondary_max_y);

        let mut mesh = chart.configure_mesh();
        mesh.x_desc(&rendered_x_label)
            .y_desc(y_label)
            .axis_desc_style(
                ("sans-serif", style.axis_desc_font_px())
                    .into_font()
                    .color(&rgb(theme.axis)),
            )
            .axis_style(rgb(theme.axis))
            .x_label_style(
                ("sans-serif", style.axis_label_font_px())
                    .into_font()
                    .color(&rgb(theme.label)),
            )
            .y_label_style(
                ("sans-serif", style.axis_label_font_px())
                    .into_font()
                    .color(&primary_axis_label_color),
            )
            .x_labels(6)
            .y_labels(6)
            .x_label_formatter(&x_label_formatter)
            .y_label_formatter(&y_label_formatter);
        if !showmajorgrid && !showminorgrid {
            mesh.disable_mesh();
        } else {
            mesh.bold_line_style(if showmajorgrid {
                ShapeStyle::from(&rgb(theme.grid))
            } else {
                ShapeStyle::from(&TRANSPARENT)
            })
            .light_line_style(if showminorgrid {
                rgb(theme.grid).mix(0.55)
            } else {
                TRANSPARENT
            });
        }
        mesh.draw().map_err(|error| error.to_string())?;

        let mut secondary_axes = chart.configure_secondary_axes();
        secondary_axes
            .y_desc(secondary_y_label.unwrap_or_else(|| secondary_y_axis.display_name()))
            .axis_desc_style(
                ("sans-serif", style.axis_desc_font_px())
                    .into_font()
                    .color(&secondary_axis_label_color),
            )
            .axis_style(secondary_axis_label_color)
            .label_style(
                ("sans-serif", style.axis_label_font_px())
                    .into_font()
                    .color(&secondary_axis_label_color),
            )
            .y_label_formatter(&secondary_y_label_formatter)
            .draw()
            .map_err(|error| error.to_string())?;

        for series in secondary_series {
            let render_style = scatter_series_style(series, y_axis, Some(secondary_y_axis), theme);
            let drawn = chart
                .draw_secondary_series(PointSeries::of_element(
                    series
                        .points
                        .iter()
                        .map(|(x, y)| (*x - x_offset, *y))
                        .collect::<Vec<_>>(),
                    point_radius,
                    render_style.shape_style(),
                    &|coord, size, draw_style| {
                        EmptyElement::at(coord)
                            + Rectangle::new([(-size, -size), (size, size)], draw_style)
                    },
                ))
                .map_err(|error| error.to_string())?;
            if showlegend {
                let legend_color = render_style.color;
                drawn.label(series.label.clone()).legend(move |(x, y)| {
                    Rectangle::new(
                        [
                            (x - point_radius, y - point_radius),
                            (x + point_radius, y + point_radius),
                        ],
                        ShapeStyle::from(&legend_color).stroke_width(2),
                    )
                });
            }
        }
        for series in primary_series {
            let render_style = scatter_series_style(series, y_axis, Some(secondary_y_axis), theme);
            let drawn = chart
                .draw_series(PointSeries::of_element(
                    series
                        .points
                        .iter()
                        .map(|(x, y)| (*x - x_offset, *y))
                        .collect::<Vec<_>>(),
                    point_radius,
                    render_style.shape_style(),
                    &|coord, size, draw_style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, draw_style)
                    },
                ))
                .map_err(|error| error.to_string())?;
            if showlegend {
                let legend_color = render_style.color;
                drawn
                    .label(series.label.clone())
                    .legend(move |(x, y)| Circle::new((x, y), point_radius, legend_color.filled()));
            }
        }
        if showlegend && series.len() > 1 {
            chart
                .configure_series_labels()
                .background_style(rgb(theme.background).mix(0.92))
                .border_style(rgb(theme.axis))
                .label_font(("sans-serif", style.axis_label_font_px()))
                .draw()
                .map_err(|error| error.to_string())?;
        }
        return Ok(());
    }

    let (min_x, max_x, min_y, max_y) = match bounds_override {
        Some(bounds) => bounds,
        None => scatter_bounds(
            series
                .iter()
                .flat_map(|series| series.points.iter().copied()),
            fixed_x_bounds,
            fixed_y_bounds,
        )?,
    };
    let x_offset = if x_axis == MsAxis::Time {
        scan_timeline_axis_offset(min_x, max_x)
    } else {
        0.0
    };
    let rendered_x_label = if x_offset == 0.0 {
        x_label.to_string()
    } else {
        format!("Time (MJD seconds - {:.0})", x_offset)
    };
    let x_span = (max_x - min_x).abs();
    let y_span = (max_y - min_y).abs();
    let x_label_formatter = |value: &f64| format_numeric_tick(*value, x_span);
    let y_label_formatter = |value: &f64| format_numeric_tick(*value, y_span);

    let mut chart = chart_builder
        .build_cartesian_2d((min_x - x_offset)..(max_x - x_offset), min_y..max_y)
        .map_err(|error| error.to_string())?;
    let mut mesh = chart.configure_mesh();
    mesh.x_desc(&rendered_x_label)
        .y_desc(y_label)
        .axis_desc_style(
            ("sans-serif", style.axis_desc_font_px())
                .into_font()
                .color(&rgb(theme.axis)),
        )
        .axis_style(rgb(theme.axis))
        .label_style(
            ("sans-serif", style.axis_label_font_px())
                .into_font()
                .color(&rgb(theme.label)),
        )
        .x_labels(6)
        .y_labels(6)
        .x_label_formatter(&x_label_formatter)
        .y_label_formatter(&y_label_formatter);
    if !showmajorgrid && !showminorgrid {
        mesh.disable_mesh();
    } else {
        mesh.bold_line_style(if showmajorgrid {
            ShapeStyle::from(&rgb(theme.grid))
        } else {
            ShapeStyle::from(&TRANSPARENT)
        })
        .light_line_style(if showminorgrid {
            rgb(theme.grid).mix(0.55)
        } else {
            TRANSPARENT
        });
    }
    mesh.draw().map_err(|error| error.to_string())?;

    for series in series {
        let render_style = scatter_series_style(series, y_axis, None, theme);
        let drawn = chart
            .draw_series(PointSeries::of_element(
                series
                    .points
                    .iter()
                    .map(|(x, y)| (*x - x_offset, *y))
                    .collect::<Vec<_>>(),
                point_radius,
                render_style.shape_style(),
                &|coord, size, draw_style| {
                    EmptyElement::at(coord) + Circle::new((0, 0), size, draw_style)
                },
            ))
            .map_err(|error| error.to_string())?;
        if showlegend {
            let legend_color = render_style.color;
            drawn
                .label(series.label.clone())
                .legend(move |(x, y)| Circle::new((x, y), point_radius, legend_color.filled()));
        }
    }
    if showlegend && series.len() > 1 {
        chart
            .configure_series_labels()
            .background_style(rgb(theme.background).mix(0.92))
            .border_style(rgb(theme.axis))
            .label_font(("sans-serif", style.axis_label_font_px()))
            .draw()
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn scatter_bounds<I>(
    points: I,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
) -> Result<(f64, f64, f64, f64), String>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    let (mut min_x, mut max_x, mut min_y, mut max_y) =
        bounds(points).ok_or_else(|| "scatter plot has no finite points".to_string())?;
    if let Some((fixed_min, fixed_max)) = fixed_x_bounds {
        min_x = fixed_min;
        max_x = fixed_max;
    } else {
        (min_x, max_x) = padded_range(min_x, max_x);
    }
    if let Some((fixed_min, fixed_max)) = fixed_y_bounds {
        min_y = fixed_min;
        max_y = fixed_max;
    } else {
        (min_y, max_y) = padded_range(min_y, max_y);
    }
    Ok((min_x, max_x, min_y, max_y))
}

fn axis_bounds<I>(values: I, fixed_bounds: Option<(f64, f64)>) -> Result<(f64, f64), String>
where
    I: IntoIterator<Item = f64>,
{
    let mut iter = values.into_iter().filter(|value| value.is_finite());
    let first = iter
        .next()
        .ok_or_else(|| "scatter plot has no finite points".to_string())?;
    let (mut min_value, mut max_value) = (first, first);
    for value in iter {
        min_value = min_value.min(value);
        max_value = max_value.max(value);
    }
    Ok(match fixed_bounds {
        Some(bounds) => bounds,
        None => padded_range(min_value, max_value),
    })
}

fn bounds<I>(points: I) -> Option<(f64, f64, f64, f64)>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    let mut iter = points
        .into_iter()
        .filter(|(x, y)| x.is_finite() && y.is_finite());
    let (first_x, first_y) = iter.next()?;
    let mut min_x = first_x;
    let mut max_x = first_x;
    let mut min_y = first_y;
    let mut max_y = first_y;
    for (x, y) in iter {
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }
    Some((min_x, max_x, min_y, max_y))
}

fn padded_range(min: f64, max: f64) -> (f64, f64) {
    let span = (max - min).abs();
    let padding = if span == 0.0 {
        min.abs().max(1.0) * 0.05
    } else {
        span * 0.05
    };
    (min - padding, max + padding)
}

fn scan_timeline_axis_offset(min: f64, max: f64) -> f64 {
    ((min + max) / 2.0 / 10.0).round() * 10.0
}

fn format_numeric_tick(value: f64, span: f64) -> String {
    let decimals = if span >= 1000.0 {
        0
    } else if span >= 10.0 {
        1
    } else if span >= 1.0 {
        2
    } else {
        4
    };
    format!("{value:.decimals$}")
}

fn palette_color_with_offset(group: &str, theme: ListObsPlotTheme, offset: usize) -> RGBColor {
    if group == "all" {
        return rgb(theme.accents[offset % theme.accents.len()]);
    }
    let mut hash = 0u64;
    for byte in group.as_bytes() {
        hash = hash.wrapping_mul(109).wrapping_add(u64::from(*byte));
    }
    rgb(theme.accents[((hash as usize) + offset) % theme.accents.len()])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScatterMarker {
    FilledCircle,
    HollowSquare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScatterSeriesRenderStyle {
    color: RGBColor,
    marker: ScatterMarker,
}

impl ScatterSeriesRenderStyle {
    fn shape_style(self) -> ShapeStyle {
        match self.marker {
            ScatterMarker::FilledCircle => self.color.filled(),
            ScatterMarker::HollowSquare => ShapeStyle::from(&self.color).stroke_width(2),
        }
    }
}

fn scatter_axis_color(
    axis: MsAxis,
    primary_y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    theme: ListObsPlotTheme,
) -> RGBColor {
    let offset =
        if secondary_y_axis.is_some_and(|secondary| axis == secondary && axis != primary_y_axis) {
            1
        } else {
            0
        };
    palette_color_with_offset("all", theme, offset)
}

fn scatter_series_style(
    series: &MsScatterSeries,
    primary_y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    theme: ListObsPlotTheme,
) -> ScatterSeriesRenderStyle {
    let offset = if secondary_y_axis
        .is_some_and(|secondary| series.y_axis == secondary && secondary != primary_y_axis)
    {
        1
    } else {
        0
    };
    let marker = if offset == 0 {
        ScatterMarker::FilledCircle
    } else {
        ScatterMarker::HollowSquare
    };
    ScatterSeriesRenderStyle {
        color: palette_color_with_offset(&series.color_group, theme, offset),
        marker,
    }
}

fn rgb(color: [u8; 3]) -> RGBColor {
    RGBColor(color[0], color[1], color[2])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dual_axis_series_styles_use_distinct_markers_and_colors() {
        let theme = ListObsPlotTheme::light();
        let primary = MsScatterSeries {
            label: "Amplitude".to_string(),
            color_group: "all".to_string(),
            y_axis: MsAxis::Amplitude,
            points: vec![(0.0, 1.0)],
        };
        let secondary = MsScatterSeries {
            label: "Phase".to_string(),
            color_group: "all".to_string(),
            y_axis: MsAxis::Phase,
            points: vec![(0.0, 2.0)],
        };

        let primary_style =
            scatter_series_style(&primary, MsAxis::Amplitude, Some(MsAxis::Phase), theme);
        let secondary_style =
            scatter_series_style(&secondary, MsAxis::Amplitude, Some(MsAxis::Phase), theme);

        assert_eq!(primary_style.marker, ScatterMarker::FilledCircle);
        assert_eq!(secondary_style.marker, ScatterMarker::HollowSquare);
        assert_ne!(primary_style.color, secondary_style.color);
    }
}

impl ListObsPlotRenderStyle {
    fn margin_px(&self) -> u32 {
        self.margin_px
    }

    fn label_area_px(&self) -> u32 {
        self.label_area_px
    }

    fn wide_y_label_area_px(&self) -> u32 {
        self.wide_y_label_area_px
    }

    fn axis_desc_font_px(&self) -> i32 {
        self.axis_desc_font_px
    }

    fn axis_label_font_px(&self) -> i32 {
        self.axis_label_font_px
    }

    fn point_radius_px(&self) -> i32 {
        self.point_radius_px
    }
}
