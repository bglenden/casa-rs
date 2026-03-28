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
//!
//! Additional layout, transform, and staged flag-edit fields are already
//! modeled in the public specification types so future waves can extend the
//! implementation without redesigning the interface.

pub mod cli;

use std::fmt;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use casacore_types::{ArrayValue, Complex64};
use image::{DynamicImage, ImageFormat, RgbImage};
use ndarray::Ix2;
use plotters::prelude::*;
use printpdf::{Mm, Op, PdfDocument, PdfPage, PdfSaveOptions, Pt, RawImage, XObjectTransform};
use serde::{Deserialize, Serialize};

use crate::MeasurementSet;
use crate::columns::{main_ids, time_columns::TimeColumn, uvw_column::UvwColumn};
use crate::listobs::{self, ListObsOptions, ListObsSummary, ListObsUvCoverage};
use crate::plot::{
    ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotPayload, ListObsPlotRenderStyle,
    ListObsPlotSpec, ListObsPlotTheme, VisibilityScatterPlotPayload, VisibilityScatterSeries,
    build_listobs_plot_payload_from_summary, build_listobs_uv_plot_payload, export_listobs_plot,
};
use crate::schema::main_table::VisibilityDataColumn;

const EXPORT_DPI: f32 = 72.0;

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
    /// Vector-averaged amplitude against UV distance.
    AmplitudeVsUvDistance,
    /// Amplitude against channel index.
    AmplitudeVsChannel,
    /// Phase against channel index.
    PhaseVsChannel,
    /// Amplitude against channel center frequency.
    AmplitudeVsFrequency,
    /// Phase against channel center frequency.
    PhaseVsFrequency,
    /// Real against imaginary.
    RealVsImaginary,
}

impl MsPlotPreset {
    /// All shipped presets in stable order.
    pub const ALL: [Self; 12] = [
        Self::UvCoverage,
        Self::AntennaLayout,
        Self::ScanTimeline,
        Self::SpectralWindowCoverage,
        Self::AmplitudeVsTime,
        Self::PhaseVsTime,
        Self::AmplitudeVsUvDistance,
        Self::AmplitudeVsChannel,
        Self::PhaseVsChannel,
        Self::AmplitudeVsFrequency,
        Self::PhaseVsFrequency,
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
            Self::AmplitudeVsUvDistance => "amplitude_vs_uv_distance",
            Self::AmplitudeVsChannel => "amplitude_vs_channel",
            Self::PhaseVsChannel => "phase_vs_channel",
            Self::AmplitudeVsFrequency => "amplitude_vs_frequency",
            Self::PhaseVsFrequency => "phase_vs_frequency",
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
            Self::AmplitudeVsUvDistance => "Amplitude vs UV Distance",
            Self::AmplitudeVsChannel => "Amplitude vs Channel",
            Self::PhaseVsChannel => "Phase vs Channel",
            Self::AmplitudeVsFrequency => "Amplitude vs Frequency",
            Self::PhaseVsFrequency => "Phase vs Frequency",
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
            "amplitude_vs_uv_distance" | "amplitude_vs_uvdist" | "amp_uvdist" => {
                Ok(Self::AmplitudeVsUvDistance)
            }
            "amplitude_vs_channel" | "amp_channel" => Ok(Self::AmplitudeVsChannel),
            "phase_vs_channel" | "phase_channel" => Ok(Self::PhaseVsChannel),
            "amplitude_vs_frequency" | "amp_frequency" => Ok(Self::AmplitudeVsFrequency),
            "phase_vs_frequency" | "phase_frequency" => Ok(Self::PhaseVsFrequency),
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
    /// Per-sample flag axis.
    Flag,
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
            Self::Flag => "flag",
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
            Self::Flag => "Flag",
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
            "hourangle" | "hour_angle" | "hour-angle" => Ok(Self::HourAngle),
            "parang" | "parallactic_angle" | "parallactic-angle" => Ok(Self::ParallacticAngle),
            "weight" => Ok(Self::Weight),
            "sigma" => Ok(Self::Sigma),
            "flag" => Ok(Self::Flag),
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

/// Iteration controls for multi-plot pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsIterationSpec {
    /// Iteration axis identifier.
    pub iteraxis: Option<String>,
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
    /// Y-axis selectors. The first wave supports exactly one Y axis.
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

    /// Validate the current plot specification against the first-wave engine.
    pub fn validate(&self) -> Result<(), String> {
        if self.y_axes.len() != 1 {
            return Err("msexplore currently supports exactly one y axis per plot".to_string());
        }
        if self.layout.gridrows != 1
            || self.layout.gridcols != 1
            || self.layout.rowindex != 0
            || self.layout.colindex != 0
            || self.layout.plotindex != 0
        {
            return Err(
                "msexplore page grids are modeled, but the first wave only supports a single plot per page".to_string(),
            );
        }
        if self.iteration.iteraxis.is_some()
            || self.iteration.xselfscale
            || self.iteration.yselfscale
            || self.iteration.xsharedaxis
            || self.iteration.ysharedaxis
        {
            return Err(
                "msexplore iteration controls are not implemented in the first wave".to_string(),
            );
        }
        if self.flag_edit.is_some() {
            return Err(
                "msexplore staged flag editing is modeled but not yet implemented in the first wave".to_string(),
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
        if matches!(self.x_axis, MsAxis::Velocity)
            || self
                .y_axes
                .iter()
                .any(|axis| matches!(axis, MsAxis::Velocity))
        {
            return Err(
                "msexplore velocity axes require transform support that is not implemented yet"
                    .to_string(),
            );
        }
        if self.transforms.phasecenter.is_some()
            || self.transforms.freqframe.is_some()
            || self.transforms.restfreq.is_some()
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
                | MsAxis::Channel
                | MsAxis::Frequency
                | MsAxis::Amplitude
                | MsAxis::Phase
                | MsAxis::Real
                | MsAxis::Imaginary
        );
        let y_axis = self.y_axes[0];
        let supported_y = matches!(
            y_axis,
            MsAxis::Amplitude | MsAxis::Phase | MsAxis::Real | MsAxis::Imaginary
        );
        if !supported_x {
            return Err(format!(
                "msexplore x axis {} is modeled but not implemented in the first wave",
                self.x_axis
            ));
        }
        if !supported_y {
            return Err(format!(
                "msexplore y axis {} is modeled but not implemented in the first wave",
                y_axis
            ));
        }
        if self.averaging.avgchannel.is_some()
            && !matches!(self.x_axis, MsAxis::Channel | MsAxis::Frequency)
        {
            return Err(
                "msexplore avgchannel currently requires xaxis=channel or xaxis=frequency"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Return the single Y axis after validation.
    pub fn y_axis(&self) -> MsAxis {
        self.y_axes[0]
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
    /// Plot definitions on the page.
    pub plots: Vec<MsPlotSpec>,
}

impl MsExploreSpec {
    /// Validate the high-level request.
    pub fn validate(&self) -> Result<(), String> {
        if self.plots.len() > 1 {
            return Err(
                "msexplore currently supports one plot per invocation; multi-plot pages are reserved for a future wave".to_string(),
            );
        }
        for plot in &self.plots {
            plot.validate()?;
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
    /// X axis label.
    pub x_label: String,
    /// Y axis label.
    pub y_label: String,
    /// Optional fixed X axis bounds.
    pub fixed_x_bounds: Option<(f64, f64)>,
    /// Optional fixed Y axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
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
    /// Plot points as `(x, y)`.
    pub points: Vec<(f64, f64)>,
}

/// Build a generic plot payload from one open MeasurementSet.
pub fn build_msexplore_plot_payload(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
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

    build_generic_visibility_scatter(ms, selection, spec).map(MsPlotPayload::Scatter)
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
        MsPlotPayload::ListObs(_) => {
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
        (MsPlotPayload::Scatter(_), MsExportFormat::Txt) => {
            std::fs::write(output_path, render_scatter_manifest(payload)?)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::Scatter(payload), MsExportFormat::Png) => {
            render_scatter_image(payload, theme, width, height)?
                .save_with_format(output_path, ImageFormat::Png)
                .map_err(|error| error.to_string())
        }
        (MsPlotPayload::Scatter(payload), MsExportFormat::Pdf) => {
            let image = render_scatter_image(payload, theme, width, height)?;
            export_scatter_pdf(&image, output_path, &payload.title)
        }
    }
}

fn render_scatter_manifest(payload: &MsPlotPayload) -> Result<String, String> {
    let MsPlotPayload::Scatter(payload) = payload else {
        return Err("text manifest export requires a scatter payload".to_string());
    };
    let mut out = String::new();
    out.push_str("# msexplore-manifest-v1\n");
    out.push_str(&format!("# title={}\n", payload.title));
    out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
    out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
    out.push_str("series_key\tseries_label\tx\ty\n");
    for series in &payload.series {
        for (x, y) in &series.points {
            out.push_str(&format!(
                "{}\t{}\t{:.12}\t{:.12}\n",
                series.color_group, series.label, x, y
            ));
        }
    }
    Ok(out)
}

fn build_generic_visibility_scatter(
    ms: &MeasurementSet,
    selection: &MsSelectionSpec,
    spec: &MsPlotSpec,
) -> Result<MsScatterPlotPayload, String> {
    let listobs_options = selection.to_listobs_options();
    let row_numbers = resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?;

    let data_source = PreparedDataSource::new(ms, spec.data_column)?;
    let flag = ms.flag_column();
    let flag_row = ms.flag_row_column();
    let time = TimeColumn::new(ms.main_table());
    let uvw = UvwColumn::new(ms.main_table());
    let field_id = main_ids::field_id(ms.main_table());
    let scan_number = main_ids::scan_number(ms.main_table());
    let data_desc_id = main_ids::data_desc_id(ms.main_table());
    let antenna1 = main_ids::antenna1(ms.main_table());
    let antenna2 = main_ids::antenna2(ms.main_table());

    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;

    let requested_corr_codes = selection
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(listobs::parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let mut series = std::collections::BTreeMap::<String, MsScatterSeries>::new();
    let mut contributing_rows = 0usize;
    let mut contributing_points = 0usize;

    for row in row_numbers {
        if flag_row.get(row).map_err(|error| error.to_string())? {
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

        let grid = data_source.row(row)?;
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
        if flags.shape() != [grid.corr_count, grid.chan_count] {
            return Err(format!(
                "visibility flag shape {:?} does not match data shape [{}, {}]",
                flags.shape(),
                grid.corr_count,
                grid.chan_count
            ));
        }

        let selected_correlations = select_correlation_slots(
            grid.corr_count,
            &corr_types,
            requested_corr_codes.as_deref(),
        );
        if selected_correlations.is_empty() {
            continue;
        }

        let freq_axis = matches!(spec.x_axis, MsAxis::Frequency);
        let channel_bins = channel_bins(grid.chan_count, spec.averaging.avgchannel)?;
        let channel_frequencies = if freq_axis {
            spectral_window
                .chan_freq(spw_id as usize)
                .map_err(|error| error.to_string())?
        } else {
            Vec::new()
        };
        if freq_axis && channel_frequencies.len() != grid.chan_count {
            return Err(format!(
                "SPECTRAL_WINDOW row {spw_id} reported {} channel frequencies for {} data channels",
                channel_frequencies.len(),
                grid.chan_count
            ));
        }

        let field_id_value = field_id.get(row).map_err(|error| error.to_string())?;
        let scan_number_value = scan_number.get(row).map_err(|error| error.to_string())?;
        let antenna1_value = antenna1.get(row).map_err(|error| error.to_string())?;
        let antenna2_value = antenna2.get(row).map_err(|error| error.to_string())?;

        let mut row_contributed = false;
        for (corr_index, corr_label) in &selected_correlations {
            for bin in &channel_bins {
                let samples =
                    collect_bin_samples(&grid, &flags, &[*corr_index], bin.start, bin.end);
                if samples.is_empty() {
                    continue;
                }
                let Some((x_value, y_value)) =
                    compute_xy_values(spec, row, &samples, bin, &channel_frequencies, &time, &uvw)?
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
                series
                    .entry(group_key.clone())
                    .or_insert_with(|| MsScatterSeries {
                        label: group_label,
                        color_group: group_key,
                        points: Vec::new(),
                    })
                    .points
                    .push((x_value, y_value));
                contributing_points += 1;
                row_contributed = true;
            }
        }

        if row_contributed {
            contributing_rows += 1;
        }
    }

    let mut series = series.into_values().collect::<Vec<_>>();
    for entry in &mut series {
        entry
            .points
            .sort_by(|left, right| left.0.total_cmp(&right.0));
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
    Ok(MsScatterPlotPayload {
        title,
        x_axis: spec.x_axis,
        y_axis: spec.y_axis(),
        x_label: spec
            .style
            .xlabel
            .clone()
            .unwrap_or_else(|| axis_label(spec.x_axis)),
        y_label: spec
            .style
            .ylabel
            .clone()
            .unwrap_or_else(|| axis_label(spec.y_axis())),
        fixed_x_bounds: fixed_bounds(spec.x_axis),
        fixed_y_bounds: fixed_bounds(spec.y_axis()),
        summary: format!(
            "{}. Rows={} Points={} Data column={}",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            spec.data_column
        ),
        series,
    })
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

fn fixed_bounds(axis: MsAxis) -> Option<(f64, f64)> {
    matches!(axis, MsAxis::Phase).then_some((-180.0, 180.0))
}

fn axis_label(axis: MsAxis) -> String {
    match axis {
        MsAxis::Time => "Time (MJD seconds)".to_string(),
        MsAxis::UvDistance => "UV Distance (m)".to_string(),
        MsAxis::Channel => "Channel".to_string(),
        MsAxis::Frequency => "Frequency (Hz)".to_string(),
        MsAxis::Amplitude => "Amplitude".to_string(),
        MsAxis::Phase => "Phase (deg)".to_string(),
        MsAxis::Real => "Real".to_string(),
        MsAxis::Imaginary => "Imaginary".to_string(),
        _ => axis.display_name().to_string(),
    }
}

fn compute_xy_values(
    spec: &MsPlotSpec,
    row: usize,
    samples: &[Complex64],
    channel_bin: &ChannelBin,
    channel_frequencies: &[f64],
    time: &TimeColumn<'_>,
    uvw: &UvwColumn<'_>,
) -> Result<Option<(f64, f64)>, String> {
    let x_value = compute_axis_value(
        spec.x_axis,
        row,
        samples,
        spec.averaging.scalar,
        channel_bin,
        channel_frequencies,
        time,
        uvw,
    )?;
    let y_value = compute_axis_value(
        spec.y_axis(),
        row,
        samples,
        spec.averaging.scalar,
        channel_bin,
        channel_frequencies,
        time,
        uvw,
    )?;
    Ok(match (x_value, y_value) {
        (Some(x), Some(y)) if x.is_finite() && y.is_finite() => Some((x, y)),
        _ => None,
    })
}

#[allow(clippy::too_many_arguments)]
fn compute_axis_value(
    axis: MsAxis,
    row: usize,
    samples: &[Complex64],
    scalar_average: bool,
    channel_bin: &ChannelBin,
    channel_frequencies: &[f64],
    time: &TimeColumn<'_>,
    uvw: &UvwColumn<'_>,
) -> Result<Option<f64>, String> {
    if samples.is_empty() {
        return Ok(None);
    }
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
        MsAxis::Channel => {
            if channel_bin.end.saturating_sub(channel_bin.start) == 1 {
                Ok(Some(channel_bin.start as f64))
            } else {
                Ok(Some(channel_bin.ordinal as f64))
            }
        }
        MsAxis::Frequency => {
            if channel_frequencies.is_empty() || channel_bin.end > channel_frequencies.len() {
                return Ok(None);
            }
            let bin = &channel_frequencies[channel_bin.start..channel_bin.end];
            Ok(Some(bin.iter().copied().sum::<f64>() / bin.len() as f64))
        }
        _ => Ok(None),
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

#[derive(Debug, Clone)]
struct ComplexGrid {
    corr_count: usize,
    chan_count: usize,
    values: Vec<Complex64>,
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
    render_scatter_plot(&root, payload, theme, style)?;
    root.present().map_err(|error| error.to_string())?;
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

fn render_scatter_plot(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    payload: &MsScatterPlotPayload,
    theme: ListObsPlotTheme,
    style: ListObsPlotRenderStyle,
) -> Result<(), String> {
    let (mut min_x, mut max_x, mut min_y, mut max_y) = bounds(
        payload
            .series
            .iter()
            .flat_map(|series| series.points.iter().copied()),
    )
    .ok_or_else(|| "scatter plot has no finite points".to_string())?;
    if let Some((fixed_min, fixed_max)) = payload.fixed_x_bounds {
        min_x = fixed_min;
        max_x = fixed_max;
    } else {
        (min_x, max_x) = padded_range(min_x, max_x);
    }
    if let Some((fixed_min, fixed_max)) = payload.fixed_y_bounds {
        min_y = fixed_min;
        max_y = fixed_max;
    } else {
        (min_y, max_y) = padded_range(min_y, max_y);
    }

    let x_offset = if payload.x_axis == MsAxis::Time {
        scan_timeline_axis_offset(min_x, max_x)
    } else {
        0.0
    };
    let x_label = if x_offset == 0.0 {
        payload.x_label.clone()
    } else {
        format!("Time (MJD seconds - {:.0})", x_offset)
    };
    let x_span = (max_x - min_x).abs();
    let y_span = (max_y - min_y).abs();

    let mut chart = ChartBuilder::on(root)
        .margin(style.margin_px())
        .x_label_area_size(style.label_area_px())
        .y_label_area_size(style.wide_y_label_area_px())
        .build_cartesian_2d((min_x - x_offset)..(max_x - x_offset), min_y..max_y)
        .map_err(|error| error.to_string())?;
    chart
        .configure_mesh()
        .x_desc(&x_label)
        .y_desc(&payload.y_label)
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
        .light_line_style(rgb(theme.grid).mix(0.55))
        .bold_line_style(rgb(theme.grid))
        .x_labels(6)
        .y_labels(6)
        .x_label_formatter(&|value| format_numeric_tick(*value, x_span))
        .y_label_formatter(&|value| format_numeric_tick(*value, y_span))
        .draw()
        .map_err(|error| error.to_string())?;

    let point_radius = style.point_radius_px().saturating_sub(1).max(3);
    for series in &payload.series {
        let color = palette_color(&series.color_group, theme);
        chart
            .draw_series(PointSeries::of_element(
                series
                    .points
                    .iter()
                    .map(|(x, y)| (*x - x_offset, *y))
                    .collect::<Vec<_>>(),
                point_radius,
                color.filled(),
                &|coord, size, draw_style| {
                    EmptyElement::at(coord) + Circle::new((0, 0), size, draw_style)
                },
            ))
            .map_err(|error| error.to_string())?;
    }

    Ok(())
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

fn palette_color(group: &str, theme: ListObsPlotTheme) -> RGBColor {
    if group == "all" {
        return rgb(theme.accents[0]);
    }
    let mut hash = 0u64;
    for byte in group.as_bytes() {
        hash = hash.wrapping_mul(109).wrapping_add(u64::from(*byte));
    }
    rgb(theme.accents[(hash as usize) % theme.accents.len()])
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
