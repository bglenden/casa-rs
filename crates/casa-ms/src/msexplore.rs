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

use std::fmt;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{MvAngle, MvTime, Quantity, Unit};
use casa_types::{ArrayValue, Complex64, ScalarValue, Value};
use image::{DynamicImage, ImageFormat, RgbImage};
use ndarray::{Ix1, Ix2};
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use printpdf::{Mm, Op, PdfDocument, PdfPage, PdfSaveOptions, Pt, RawImage, XObjectTransform};
use serde::{Deserialize, Serialize};

use crate::columns::{
    exposure_interval::IntervalColumn,
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
use crate::{MeasurementSet, MeasurementSetSummaryOptions, MeasurementSetSummaryOutputFormat};

const EXPORT_DPI: f32 = 72.0;
const SPEED_OF_LIGHT_KM_S: f64 = 299_792.458;
const AVG_TIME_BUCKET_EPSILON_SECONDS: f64 = 0.002;
type BitmapArea<'a> = DrawingArea<BitMapBackend<'a>, plotters::coord::Shift>;

struct ScatterPanelAxes<'a> {
    x_axis: MsAxis,
    y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    x_label: &'a str,
    y_label: &'a str,
    secondary_y_label: Option<&'a str>,
}

struct ScatterPanelBounds {
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    secondary_fixed_y_bounds: Option<(f64, f64)>,
    bounds_override: Option<(f64, f64, f64, f64)>,
}

struct ScatterPanelPresentation<'a> {
    panel_title: Option<&'a str>,
    showlegend: bool,
    legend_position: MsLegendPosition,
    showmajorgrid: bool,
    showminorgrid: bool,
}

struct ScatterPanelRenderContext<'a> {
    axes: ScatterPanelAxes<'a>,
    bounds: ScatterPanelBounds,
    series: &'a [MsScatterSeries],
    presentation: ScatterPanelPresentation<'a>,
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
}

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
    /// Convert the generic selection spec into the shared summary option set.
    pub fn to_summary_options(&self) -> MeasurementSetSummaryOptions {
        MeasurementSetSummaryOptions {
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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

/// Legend placement options accepted by `msexplore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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

    fn is_exterior(self) -> bool {
        matches!(
            self,
            Self::ExteriorRight | Self::ExteriorLeft | Self::ExteriorTop | Self::ExteriorBottom
        )
    }
}

impl fmt::Display for MsLegendPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Page header items accepted by `msexplore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
            showlegend: false,
            legendposition: MsLegendPosition::UpperRight,
            showmajorgrid: false,
            showminorgrid: false,
        }
    }
}

/// One staged flag-edit request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsFlagAction {
    /// Mark matching samples as flagged.
    Flag,
    /// Clear flags from matching samples.
    Unflag,
}

/// One plotted visibility sample selected by a staged flag edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
                return Err(
                    "msexplore staged flag editing requires finite region bounds".to_string(),
                );
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
    pub summary_format: MeasurementSetSummaryOutputFormat,
    /// Shared row-selection controls.
    pub selection: MsSelectionSpec,
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
}

impl PointBudget {
    fn unlimited() -> Self {
        Self {
            max_plot_points: None,
            rendered_points: 0,
        }
    }

    fn limited(max_plot_points: usize) -> Self {
        Self {
            max_plot_points: Some(max_plot_points),
            rendered_points: 0,
        }
    }

    fn record_points(&mut self, additional_points: usize, context: &str) -> Result<(), String> {
        let next_points = self
            .rendered_points
            .checked_add(additional_points)
            .ok_or_else(|| format!("{context} exceeded the supported plotted-point range"))?;
        if let Some(max_plot_points) = self.max_plot_points
            && next_points > max_plot_points
        {
            return Err(format!(
                "{context} would render more than {max_plot_points} points; narrow the selection, add averaging, or raise --max-points"
            ));
        }
        self.rendered_points = next_points;
        Ok(())
    }
}

fn build_msexplore_plot_payload_validated(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
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

/// Build a plot payload from a full `msexplore` request.
pub fn build_msexplore_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    let mut point_budget = PointBudget::limited(spec.max_plot_points);
    let header_lines = resolve_page_header_lines(ms, spec)?;
    if spec.plots.len() == 1 {
        let mut payload = build_msexplore_plot_payload_validated(
            ms,
            &spec.selection,
            &spec.plots[0],
            &mut point_budget,
        )?;
        apply_page_header_lines(&mut payload, header_lines);
        return Ok(payload);
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
    let title = spec
        .page_title
        .clone()
        .unwrap_or_else(|| "MeasurementSet Multi-Plot Page".to_string());

    Ok(MsPlotPayload::ScatterPage(MsScatterPagePayload {
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

/// Preview a staged flag edit against one standalone `msexplore` scatter plot.
///
/// The returned preview resolves the plot region to exact `(row, corr, chan)`
/// samples before any writeback occurs.
pub fn preview_msexplore_flag_edit(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
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
    let payload = build_msexplore_payload(ms, spec)?;
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
    selection: &MsSelectionSpec,
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
            .set_cell(
                row,
                "FLAG",
                Value::Array(ArrayValue::Bool(matrix.into_dyn())),
            )
            .map_err(|error| error.to_string())?;
        ms.main_table_mut()
            .set_cell(row, "FLAG_ROW", Value::Scalar(ScalarValue::Bool(flag_row)))
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
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    let mut point_budget = PointBudget::unlimited();
    build_msexplore_plot_payload_validated(ms, selection, spec, &mut point_budget)
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

fn build_generic_visibility_scatter(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    if uses_extended_averaging(spec) {
        return build_generic_visibility_scatter_with_averaging(ms, selection, spec, point_budget);
    }

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
            let mut samples = Vec::<Complex64>::new();
            let mut flag_samples = Vec::<bool>::new();
            let mut weight_spectrum_samples = Vec::<f64>::new();
            let mut sigma_spectrum_samples = Vec::<f64>::new();
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
                collect_bin_flags_into(&flags, *corr_index, bin.start, bin.end, &mut flag_samples);
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
                let panel = panels
                    .get_mut(&panel_key)
                    .expect("panel inserted before mutation");
                for (y_axis, series_key, series_label, color_group) in &series_identity {
                    let Some(y_value) = compute_axis_value(
                        *y_axis,
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
                    series.points.push((x_value, y_value));
                    series.provenance.push(MsScatterPointRef {
                        row,
                        corr: *corr_index,
                        chan_start: bin.start,
                        chan_end: bin.end,
                    });
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
            legend_position: spec.style.legendposition,
            showmajorgrid: spec.style.showmajorgrid,
            showminorgrid: spec.style.showminorgrid,
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
        legend_position: spec.style.legendposition,
        showmajorgrid: spec.style.showmajorgrid,
        showminorgrid: spec.style.showminorgrid,
        header_lines: Vec::new(),
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

fn build_generic_visibility_scatter_with_averaging(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
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
        Some(PreparedDataSource::new(ms, spec.data_column)?)
    } else {
        None
    };
    let flag = ms.flag_column();
    let flag_row = ms.flag_row_column();
    let weight = ms.weight_column();
    let sigma = ms.sigma_column();
    let interval = IntervalColumn::new(ms.main_table());
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
    let use_shared_time_scope_midpoint = spec.averaging.avgtime.is_some()
        && (spec.averaging.avgfield || spec.averaging.avgscan || spec.averaging.avgspw);

    let mut time_scope_origins = std::collections::BTreeMap::<TimeAverageScopeKey, f64>::new();
    let mut shared_time_scope_bounds =
        std::collections::BTreeMap::<SharedTimeAverageScopeKey, (f64, f64)>::new();
    if spec.averaging.avgtime.is_some() {
        for &row in &row_numbers {
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
            let field_id_value = field_id.get(row).map_err(|error| error.to_string())?;
            let scan_number_value = scan_number.get(row).map_err(|error| error.to_string())?;
            let antenna1_value = antenna1.get(row).map_err(|error| error.to_string())?;
            let antenna2_value = antenna2.get(row).map_err(|error| error.to_string())?;
            let row_time_value = time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?;
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

        let field_id_value = field_id.get(row).map_err(|error| error.to_string())?;
        let scan_number_value = scan_number.get(row).map_err(|error| error.to_string())?;
        let antenna1_value = antenna1.get(row).map_err(|error| error.to_string())?;
        let antenna2_value = antenna2.get(row).map_err(|error| error.to_string())?;
        let row_time_value = time
            .get_mjd_seconds(row)
            .map_err(|error| error.to_string())?;
        let row_interval_value = interval.get(row).map_err(|error| error.to_string())?;
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
    selection: &MsSelectionSpec,
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
    selection: &MsSelectionSpec,
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

#[derive(Debug, Clone)]
struct ScatterLegendEntry {
    label: String,
    render_style: ScatterSeriesRenderStyle,
}

fn reserve_scatter_canvas<'a>(
    root: &BitmapArea<'a>,
    title: &str,
    header_lines: &[String],
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> Result<BitmapArea<'a>, String> {
    let mut body = root.clone();
    if !header_lines.is_empty() {
        let line_height = u32::try_from(style.axis_label_font_px())
            .unwrap_or(0)
            .saturating_add(6);
        let header_height = (header_lines.len() as u32)
            .saturating_mul(line_height)
            .saturating_add(10);
        let (header_area, remaining) = body.split_vertically(header_height);
        draw_scatter_header_lines(&header_area, header_lines, theme, style)?;
        body = remaining;
    }
    if !title.trim().is_empty() {
        let title_height = u32::try_from(style.axis_desc_font_px())
            .unwrap_or(0)
            .saturating_add(14);
        let (title_area, remaining) = body.split_vertically(title_height);
        draw_scatter_title(&title_area, title, theme, style)?;
        body = remaining;
    }
    Ok(body)
}

fn draw_scatter_header_lines(
    area: &BitmapArea<'_>,
    header_lines: &[String],
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> Result<(), String> {
    let font = ("sans-serif", style.axis_label_font_px())
        .into_font()
        .color(&rgb(theme.label));
    let line_height = style.axis_label_font_px().saturating_add(6);
    for (index, line) in header_lines.iter().enumerate() {
        area.draw(&Text::new(
            line.clone(),
            (14, 8 + (index as i32) * line_height),
            font.clone(),
        ))
        .map_err(|error| error.to_string())?;
    }
    let (width, height) = area.dim_in_pixel();
    if width > 1 && height > 1 {
        area.draw(&PathElement::new(
            vec![(0, height as i32 - 1), (width as i32, height as i32 - 1)],
            ShapeStyle::from(&rgb(theme.grid)).stroke_width(1),
        ))
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn draw_scatter_title(
    area: &BitmapArea<'_>,
    title: &str,
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> Result<(), String> {
    let (width, height) = area.dim_in_pixel();
    let font = ("sans-serif", style.axis_desc_font_px().saturating_add(2))
        .into_font()
        .color(&rgb(theme.label))
        .pos(Pos::new(HPos::Center, VPos::Center));
    area.draw(&Text::new(
        title.to_string(),
        (width as i32 / 2, height as i32 / 2),
        font,
    ))
    .map_err(|error| error.to_string())
}

fn collect_scatter_legend_entries(
    series: &[MsScatterSeries],
    primary_y_axis: MsAxis,
    secondary_y_axis: Option<MsAxis>,
    theme: ListObsPlotTheme,
) -> Vec<ScatterLegendEntry> {
    series
        .iter()
        .map(|series| ScatterLegendEntry {
            label: series.label.clone(),
            render_style: scatter_series_style(series, primary_y_axis, secondary_y_axis, theme),
        })
        .collect()
}

fn split_scatter_panel_for_legend<'a>(
    root: &BitmapArea<'a>,
    showlegend: bool,
    legend_position: MsLegendPosition,
    legend_count: usize,
    style: ListObsPlotRenderStyle,
) -> (BitmapArea<'a>, Option<BitmapArea<'a>>) {
    if !showlegend || legend_count <= 1 || !legend_position.is_exterior() {
        return (root.clone(), None);
    }

    let (width, height) = root.dim_in_pixel();
    match legend_position {
        MsLegendPosition::ExteriorRight => {
            let legend_width = (width.saturating_mul(26) / 100).clamp(160, 360);
            let split_at = width.saturating_sub(legend_width.max(1));
            let (plot_area, legend_area) = root.split_horizontally(split_at.max(1));
            (plot_area, Some(legend_area))
        }
        MsLegendPosition::ExteriorLeft => {
            let legend_width = (width.saturating_mul(26) / 100).clamp(160, 360);
            let (legend_area, plot_area) = root.split_horizontally(legend_width.max(1));
            (plot_area, Some(legend_area))
        }
        MsLegendPosition::ExteriorTop => {
            let legend_height = u32::try_from(style.axis_label_font_px())
                .unwrap_or(0)
                .saturating_add(10)
                .saturating_mul(2)
                .clamp(42, 96);
            let (legend_area, plot_area) = root.split_vertically(legend_height.max(1));
            (plot_area, Some(legend_area))
        }
        MsLegendPosition::ExteriorBottom => {
            let legend_height = u32::try_from(style.axis_label_font_px())
                .unwrap_or(0)
                .saturating_add(10)
                .saturating_mul(2)
                .clamp(42, 96);
            let split_at = height.saturating_sub(legend_height.max(1));
            let (plot_area, legend_area) = root.split_vertically(split_at.max(1));
            (plot_area, Some(legend_area))
        }
        _ => (root.clone(), None),
    }
}

fn render_external_scatter_legend(
    area: &BitmapArea<'_>,
    legend_entries: &[ScatterLegendEntry],
    legend_position: MsLegendPosition,
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> Result<(), String> {
    let (width, height) = area.dim_in_pixel();
    let font = external_scatter_legend_text_style(theme, style);
    let marker_radius = style.point_radius_px().saturating_sub(1).max(3);
    let line_height = style.axis_label_font_px().saturating_add(8);
    let padding = 12i32;

    let draw_marker = |area: &BitmapArea<'_>,
                       x: i32,
                       y: i32,
                       marker: ScatterMarker,
                       color: RGBColor|
     -> Result<(), String> {
        match marker {
            ScatterMarker::FilledCircle => area
                .draw(&Circle::new((x, y), marker_radius, color.filled()))
                .map_err(|error| error.to_string()),
            ScatterMarker::HollowSquare => area
                .draw(&Rectangle::new(
                    [
                        (x - marker_radius, y - marker_radius),
                        (x + marker_radius, y + marker_radius),
                    ],
                    ShapeStyle::from(&color).stroke_width(2),
                ))
                .map_err(|error| error.to_string()),
        }
    };

    match legend_position {
        MsLegendPosition::ExteriorLeft | MsLegendPosition::ExteriorRight => {
            for (index, entry) in legend_entries.iter().enumerate() {
                let baseline = padding + (index as i32) * line_height;
                if baseline > height as i32 - padding {
                    break;
                }
                draw_marker(
                    area,
                    padding + marker_radius,
                    baseline,
                    entry.render_style.marker,
                    entry.render_style.color,
                )?;
                area.draw(&Text::new(
                    entry.label.clone(),
                    (padding + marker_radius * 2 + 8, baseline - marker_radius),
                    font.clone(),
                ))
                .map_err(|error| error.to_string())?;
            }
        }
        MsLegendPosition::ExteriorTop | MsLegendPosition::ExteriorBottom => {
            let mut x = padding;
            let mut y = padding + marker_radius;
            for entry in legend_entries {
                let estimated_width = (entry.label.len() as i32)
                    .saturating_mul(style.axis_label_font_px() / 2)
                    .saturating_add(marker_radius * 2 + 24);
                if x + estimated_width > width as i32 - padding {
                    x = padding;
                    y += line_height;
                    if y > height as i32 - padding {
                        break;
                    }
                }
                draw_marker(
                    area,
                    x + marker_radius,
                    y,
                    entry.render_style.marker,
                    entry.render_style.color,
                )?;
                area.draw(&Text::new(
                    entry.label.clone(),
                    (x + marker_radius * 2 + 8, y - marker_radius),
                    font.clone(),
                ))
                .map_err(|error| error.to_string())?;
                x += estimated_width;
            }
        }
        _ => {}
    }

    if width > 1 && height > 1 {
        area.draw(&Rectangle::new(
            [(0, 0), (width as i32 - 1, height as i32 - 1)],
            ShapeStyle::from(&rgb(theme.grid)).stroke_width(1),
        ))
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn render_scatter_image(
    payload: &MsScatterPlotPayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    #[cfg(not(target_os = "macos"))]
    crate::plot::ensure_non_macos_plot_font()?;

    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    let plot_root =
        reserve_scatter_canvas(&root, &payload.title, &payload.header_lines, theme, style)?;
    render_scatter_panel(
        &plot_root,
        ScatterPanelRenderContext {
            axes: ScatterPanelAxes {
                x_axis: payload.x_axis,
                y_axis: payload.y_axis,
                secondary_y_axis: payload.secondary_y_axis,
                x_label: &payload.x_label,
                y_label: &payload.y_label,
                secondary_y_label: payload.secondary_y_label.as_deref(),
            },
            bounds: ScatterPanelBounds {
                fixed_x_bounds: payload.fixed_x_bounds,
                fixed_y_bounds: payload.fixed_y_bounds,
                secondary_fixed_y_bounds: payload.secondary_fixed_y_bounds,
                bounds_override: None,
            },
            series: &payload.series,
            presentation: ScatterPanelPresentation {
                panel_title: None,
                showlegend: payload.showlegend,
                legend_position: payload.legend_position,
                showmajorgrid: payload.showmajorgrid,
                showminorgrid: payload.showminorgrid,
            },
            theme,
            style,
        },
    )?;
    drop(plot_root);
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
    #[cfg(not(target_os = "macos"))]
    crate::plot::ensure_non_macos_plot_font()?;

    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    let titled =
        reserve_scatter_canvas(&root, &payload.title, &payload.header_lines, theme, style)?;
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
            ScatterPanelRenderContext {
                axes: ScatterPanelAxes {
                    x_axis: payload.x_axis,
                    y_axis: payload.y_axis,
                    secondary_y_axis: None,
                    x_label: &payload.x_label,
                    y_label: &payload.y_label,
                    secondary_y_label: None,
                },
                bounds: ScatterPanelBounds {
                    fixed_x_bounds: payload.fixed_x_bounds,
                    fixed_y_bounds: payload.fixed_y_bounds,
                    secondary_fixed_y_bounds: None,
                    bounds_override: resolved_bounds,
                },
                series: &panel.series,
                presentation: ScatterPanelPresentation {
                    panel_title: Some(&panel.label),
                    showlegend: payload.showlegend,
                    legend_position: payload.legend_position,
                    showmajorgrid: payload.showmajorgrid,
                    showminorgrid: payload.showminorgrid,
                },
                theme,
                style,
            },
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
    legend_position: MsLegendPosition,
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
        let legend_position = first.legend_position;
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
            if item.plot.legend_position != legend_position {
                return Err(format!(
                    "scatter page cell ({rowindex}, {colindex}) mixes incompatible legend positions"
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
            let child_series_count = item.plot.series.len();
            if !title_parts.iter().any(|part| part == &item_title) {
                title_parts.push(item_title.clone());
            }
            for child_series in &item.plot.series {
                let (label, color_group) = if overplotted {
                    let label = if child_series_count == 1 {
                        item_title.clone()
                    } else {
                        format!("{} · {}", item_title, child_series.label)
                    };
                    (
                        label,
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
                    provenance: child_series.provenance.clone(),
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
            legend_position,
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
    #[cfg(not(target_os = "macos"))]
    crate::plot::ensure_non_macos_plot_font()?;

    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let style = ListObsPlotRenderStyle::for_bitmap_size(width, height);
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;
    let titled =
        reserve_scatter_canvas(&root, &payload.title, &payload.header_lines, theme, style)?;
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
        render_scatter_panel(
            area,
            ScatterPanelRenderContext {
                axes: ScatterPanelAxes {
                    x_axis: cell.x_axis,
                    y_axis: cell.y_axis,
                    secondary_y_axis: cell.secondary_y_axis,
                    x_label: &cell.x_label,
                    y_label: &cell.y_label,
                    secondary_y_label: cell.secondary_y_label.as_deref(),
                },
                bounds: ScatterPanelBounds {
                    fixed_x_bounds: cell.fixed_x_bounds,
                    fixed_y_bounds: cell.fixed_y_bounds,
                    secondary_fixed_y_bounds: cell.secondary_fixed_y_bounds,
                    bounds_override: global_bounds,
                },
                series: &cell.series,
                presentation: ScatterPanelPresentation {
                    panel_title: Some(&cell.title),
                    showlegend: cell.showlegend,
                    legend_position: cell.legend_position,
                    showmajorgrid: cell.showmajorgrid,
                    showminorgrid: cell.showminorgrid,
                },
                theme,
                style,
            },
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
    root: &BitmapArea<'_>,
    context: ScatterPanelRenderContext<'_>,
) -> Result<(), String> {
    let ScatterPanelRenderContext {
        axes,
        bounds,
        series,
        presentation,
        theme,
        style,
    } = context;
    let ScatterPanelAxes {
        x_axis,
        y_axis,
        secondary_y_axis,
        x_label,
        y_label,
        secondary_y_label,
    } = axes;
    let ScatterPanelBounds {
        fixed_x_bounds,
        fixed_y_bounds,
        secondary_fixed_y_bounds,
        bounds_override,
    } = bounds;
    let ScatterPanelPresentation {
        panel_title,
        showlegend,
        legend_position,
        showmajorgrid,
        showminorgrid,
    } = presentation;

    let legend_entries = collect_scatter_legend_entries(series, y_axis, secondary_y_axis, theme);
    let (chart_root, legend_area) = split_scatter_panel_for_legend(
        root,
        showlegend,
        legend_position,
        legend_entries.len(),
        style,
    );
    let mut chart_builder = ChartBuilder::on(&chart_root);
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
                    series.points.iter().map(|(x, y)| (*x - x_offset, *y)),
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
                    series.points.iter().map(|(x, y)| (*x - x_offset, *y)),
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
            if legend_position.is_exterior() {
                if let Some(legend_area) = legend_area.as_ref() {
                    render_external_scatter_legend(
                        legend_area,
                        &legend_entries,
                        legend_position,
                        theme,
                        style,
                    )?;
                }
            } else {
                chart
                    .configure_series_labels()
                    .position(match legend_position {
                        MsLegendPosition::UpperLeft => SeriesLabelPosition::UpperLeft,
                        MsLegendPosition::LowerRight => SeriesLabelPosition::LowerRight,
                        MsLegendPosition::LowerLeft => SeriesLabelPosition::LowerLeft,
                        _ => SeriesLabelPosition::UpperRight,
                    })
                    .background_style(rgb(theme.background).mix(0.92))
                    .border_style(rgb(theme.axis))
                    .label_font(internal_scatter_legend_text_style(theme, style))
                    .draw()
                    .map_err(|error| error.to_string())?;
            }
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
                series.points.iter().map(|(x, y)| (*x - x_offset, *y)),
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
        if legend_position.is_exterior() {
            if let Some(legend_area) = legend_area.as_ref() {
                render_external_scatter_legend(
                    legend_area,
                    &legend_entries,
                    legend_position,
                    theme,
                    style,
                )?;
            }
        } else {
            chart
                .configure_series_labels()
                .position(match legend_position {
                    MsLegendPosition::UpperLeft => SeriesLabelPosition::UpperLeft,
                    MsLegendPosition::LowerRight => SeriesLabelPosition::LowerRight,
                    MsLegendPosition::LowerLeft => SeriesLabelPosition::LowerLeft,
                    _ => SeriesLabelPosition::UpperRight,
                })
                .background_style(rgb(theme.background).mix(0.92))
                .border_style(rgb(theme.axis))
                .label_font(internal_scatter_legend_text_style(theme, style))
                .draw()
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn external_scatter_legend_text_style(
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> TextStyle<'static> {
    ("sans-serif", style.axis_label_font_px())
        .into_font()
        .color(&rgb(theme.label))
}

fn internal_scatter_legend_text_style(
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> TextStyle<'static> {
    ("sans-serif", style.axis_label_font_px())
        .into_font()
        .color(&rgb(theme.axis))
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
        min.abs().max(max.abs()).max(1.0) * 1e-6
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

#[cfg(test)]
mod tests {
    use super::*;
    use plotters::style::Color;

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
        assert_eq!(
            MsPlotPreset::UvCoverage.lowers_to_listobs_metadata(),
            Some(ListObsPlotKind::UvCoverage)
        );
        assert_eq!(
            MsPlotPreset::AmplitudeVsFrequency.lowers_to_listobs_metadata(),
            None
        );
    }

    #[test]
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

        for (format, raw, extension) in [
            (MsExportFormat::Png, "png", "png"),
            (MsExportFormat::Pdf, "pdf", "pdf"),
            (MsExportFormat::Txt, "txt", "txt"),
        ] {
            assert_eq!(MsExportFormat::parse(raw).unwrap(), format);
            assert_eq!(format.extension(), extension);
        }
        assert!(
            MsExportFormat::parse("jpg")
                .unwrap_err()
                .contains("unsupported")
        );
    }

    #[test]
    fn selection_and_page_enums_preserve_stable_strings() {
        let selection = MsSelectionSpec {
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

        for (position, is_exterior) in [
            (MsLegendPosition::UpperRight, false),
            (MsLegendPosition::UpperLeft, false),
            (MsLegendPosition::LowerRight, false),
            (MsLegendPosition::LowerLeft, false),
            (MsLegendPosition::ExteriorRight, true),
            (MsLegendPosition::ExteriorLeft, true),
            (MsLegendPosition::ExteriorTop, true),
            (MsLegendPosition::ExteriorBottom, true),
        ] {
            assert_eq!(position.is_exterior(), is_exterior);
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
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("avgchannel currently requires")
        );

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
            selection: MsSelectionSpec::default(),
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
            selection: MsSelectionSpec::default(),
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
            selection: MsSelectionSpec::default(),
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
    fn dual_axis_series_styles_use_distinct_markers_and_colors() {
        let theme = ListObsPlotTheme::light();
        let primary = MsScatterSeries {
            label: "Amplitude".to_string(),
            color_group: "all".to_string(),
            y_axis: MsAxis::Amplitude,
            points: vec![(0.0, 1.0)],
            provenance: vec![MsScatterPointRef {
                row: 0,
                corr: 0,
                chan_start: 0,
                chan_end: 1,
            }],
        };
        let secondary = MsScatterSeries {
            label: "Phase".to_string(),
            color_group: "all".to_string(),
            y_axis: MsAxis::Phase,
            points: vec![(0.0, 2.0)],
            provenance: vec![MsScatterPointRef {
                row: 0,
                corr: 0,
                chan_start: 0,
                chan_end: 1,
            }],
        };

        let primary_style =
            scatter_series_style(&primary, MsAxis::Amplitude, Some(MsAxis::Phase), theme);
        let secondary_style =
            scatter_series_style(&secondary, MsAxis::Amplitude, Some(MsAxis::Phase), theme);

        assert_eq!(primary_style.marker, ScatterMarker::FilledCircle);
        assert_eq!(secondary_style.marker, ScatterMarker::HollowSquare);
        assert_ne!(primary_style.color, secondary_style.color);
    }

    #[test]
    fn internal_legend_text_style_uses_theme_axis_color() {
        let theme = ListObsPlotTheme::dark();
        let style = ListObsPlotRenderStyle::for_bitmap_size(1200, 800);

        let text_style = internal_scatter_legend_text_style(theme, style);

        assert_eq!(text_style.color.rgb, rgb(theme.axis).to_backend_color().rgb);
    }

    #[test]
    fn zero_span_large_time_axis_uses_local_padding() {
        let value = 4_304_481_700.0;
        let (min, max) = padded_range(value, value);

        assert!(min < value);
        assert!(max > value);
        assert!(
            (max - min) < 20_000.0,
            "unexpectedly wide zero-span padding: {}",
            max - min
        );
    }
}
