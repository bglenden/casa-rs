// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
    /// Parses a supported CASA iteration-axis token.
    pub fn parse(token: &str) -> Option<Self> {
        use MsPageHeaderItem::*;
        [
            Filename, YColumn, ObsDate, ObsTime, Observer, ProjId, Telescope, TargName, TargDir,
        ]
        .into_iter()
        .find(|item| item.as_str() == token)
    }

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
    /// Iterate one panel per selected antenna, including either baseline endpoint.
    Antenna,
    /// Iterate one panel per selected antenna pair.
    Baseline,
    /// Iterate one panel per selected integration epoch.
    Time,
}

impl MsIterationAxis {
    /// Reports whether this iteration dimension supports averaged ordinates.
    pub fn supports_averaged_ordinates(self) -> bool {
        matches!(self, Self::Correlation | Self::Scan | Self::Field)
    }

    /// Reports whether this iteration dimension is keyed by row metadata.
    pub fn groups_by_row(self) -> bool {
        matches!(self, Self::Antenna | Self::Baseline | Self::Time)
    }

    /// All currently implemented iteration axes.
    pub const ALL: [Self; 7] = [
        Self::Field,
        Self::Scan,
        Self::SpectralWindow,
        Self::Correlation,
        Self::Antenna,
        Self::Baseline,
        Self::Time,
    ];

    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Field => "field",
            Self::Scan => "scan",
            Self::SpectralWindow => "spw",
            Self::Correlation => "correlation",
            Self::Antenna => "antenna",
            Self::Baseline => "baseline",
            Self::Time => "time",
        }
    }

    /// Human-readable label.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Field => "Field",
            Self::Scan => "Scan",
            Self::SpectralWindow => "Spectral Window",
            Self::Correlation => "Correlation",
            Self::Antenna => "Antenna",
            Self::Baseline => "Baseline",
            Self::Time => "Time",
        }
    }

    /// Parse a CASA-style iteration-axis identifier.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "field" => Ok(Self::Field),
            "scan" => Ok(Self::Scan),
            "spw" | "spectral_window" | "spectralwindow" => Ok(Self::SpectralWindow),
            "correlation" | "corr" => Ok(Self::Correlation),
            "antenna" => Ok(Self::Antenna),
            "baseline" => Ok(Self::Baseline),
            "time" => Ok(Self::Time),
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
        self.validate_with_plotms(false, false, false)
    }

    /// Validates the plot specification together with the PlotMS-specific axis and averaging
    /// options.
    pub fn validate_with_plotms(
        &self,
        independent_ordinates: bool,
        pointing: bool,
        iterated_ordinates: bool,
    ) -> Result<(), String> {
        let iterated = self.iteration.iteraxis.is_some();
        self.validate_common(independent_ordinates, pointing, iterated_ordinates)?;
        if self.layout.rowindex != 0 || self.layout.colindex != 0 || self.layout.plotindex != 0 {
            return Err(
                "msexplore per-plot rowindex/colindex/plotindex placement is reserved for multi-plot page composition".to_string(),
            );
        }
        if iterated && self.y_axes.len() > 1 && !iterated_ordinates {
            return Err(
                "msexplore multi-y plots are currently available for non-iterated plots only"
                    .to_string(),
            );
        }
        if !iterated && (self.layout.gridrows != 1 || self.layout.gridcols != 1) {
            return Err(
                "msexplore gridrows/gridcols require --iteraxis; non-iterated standalone plots still render a single panel".to_string(),
            );
        }
        Ok(())
    }

    fn validate_for_page_child(&self) -> Result<(), String> {
        self.validate_common(false, false, false)?;
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

    fn validate_common(
        &self,
        independent_ordinates: bool,
        pointing: bool,
        plotms_iteration: bool,
    ) -> Result<(), String> {
        if self.y_axes.is_empty() {
            return Err("msexplore requires at least one y axis per plot".to_string());
        }
        if self.y_axes.len() > 2 && !independent_ordinates {
            return Err("msexplore currently supports at most two y axes per plot".to_string());
        }
        if self.y_axes.len() == 2 && self.y_axes[0] == self.y_axes[1] && !independent_ordinates {
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
        let plotms_shared_axes = plotms_iteration && self.iteration.iteraxis.is_some();
        if !plotms_shared_axes && self.iteration.xselfscale && self.iteration.xsharedaxis {
            return Err(
                "msexplore x-axis iteration scaling cannot request both self-scaled and shared axes".to_string(),
            );
        }
        if !plotms_shared_axes && self.iteration.yselfscale && self.iteration.ysharedaxis {
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
        self.validate_iteration_averaging(plotms_iteration)?;
        // Independent PlotMS ordinates color the averaged groups using their anchors.
        if !independent_ordinates && self.averaging.avgscan && self.color_by == MsColorAxis::Scan {
            return Err("msexplore cannot color by scan while averaging across scans".to_string());
        }
        if !independent_ordinates && self.averaging.avgfield && self.color_by == MsColorAxis::Field
        {
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
        self.validate_interval_and_wavelength_axes()?;
        if uses_extended_averaging(self)
            && !plotms_columns::uses_time_statistics(self)
            && (self.x_axis == MsAxis::WeightedAmplitude
                || self.y_axes.contains(&MsAxis::WeightedAmplitude))
        {
            return Err("This PlotMS weighted amplitude averaging is pending (#1032)".into());
        }
        plotms_row_axes::validate(self)?;
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
        if !supported_x
            && !self.x_axis.is_interval_or_wavelength()
            && !(self.x_axis.is_weight_axis() && self.x_axis.uses_channel_bins())
            && !self.x_axis.is_row_identifier()
            && !(pointing && self.x_axis.is_pointing())
        {
            return Err(format!(
                "msexplore x axis {} is modeled but not implemented yet",
                self.x_axis
            ));
        }
        for y_axis in self.y_axes.iter().copied() {
            if y_axis == MsAxis::UvDistance {
                if self.x_axis.uses_uvw()
                    || (self.x_axis.is_visibility_math()
                        && (!uses_extended_averaging(self)
                            || plotms_columns::uses_weighted_time_uvw(self)))
                {
                    continue;
                }
                return Err(
                    "This PlotMS UV distance ordinate combination is pending (#1032)".into(),
                );
            }
            let supported_y = matches!(
                y_axis,
                MsAxis::Amplitude
                    | MsAxis::WeightedAmplitude
                    | MsAxis::Phase
                    | MsAxis::Real
                    | MsAxis::Imaginary
                    | MsAxis::Channel
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
            if !supported_y
                && !y_axis.is_interval_or_wavelength()
                && !y_axis.is_row_identifier()
                && !y_axis.is_antenna_horizontal()
                && !y_axis.is_ephemeris()
                && !(self.y_axes.len() == 1
                    && plotms_row_axes::metadata_coordinate_pair(self.x_axis, y_axis).is_some())
                && !(pointing && (y_axis.is_pointing() || y_axis == MsAxis::Time))
                && !(plotms_iteration && y_axis == MsAxis::Time && self.averaging.avgtime.is_none())
            {
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

    fn validate_iteration_averaging(&self, plotms_iteration: bool) -> Result<(), String> {
        match self.iteration.iteraxis {
            Some(axis) if axis.groups_by_row() && uses_extended_averaging(self) => {
                Err("averaged PlotMS antenna, baseline or time iteration is pending (#1032)".into())
            }
            Some(MsIterationAxis::Scan) if self.averaging.avgscan => {
                Err("msexplore cannot iterate by scan while averaging across scans".into())
            }
            Some(MsIterationAxis::Field) if self.averaging.avgfield => {
                Err("msexplore cannot iterate by field while averaging across fields".into())
            }
            Some(MsIterationAxis::SpectralWindow)
                if self.averaging.avgspw
                    && (!plotms_iteration || self.averaging.avgtime.is_some()) =>
            {
                Err(
                    "msexplore cannot iterate by spectral window while averaging across spectral windows"
                        .into(),
                )
            }
            _ => Ok(()),
        }
    }

    fn validate_interval_and_wavelength_axes(&self) -> Result<(), String> {
        let requested = self.x_axis.is_interval_or_wavelength()
            || self
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::is_interval_or_wavelength);
        if !requested {
            return Ok(());
        }
        if (uses_extended_averaging(self)
            && !plotms_columns::uses_averaged_geometry_coordinates(self))
            || (self.averaging.avgchannel.is_some_and(|width| width > 1)
                && !self.has_uvw_coordinate_axes())
        {
            return Err(
                "averaged PlotMS interval and wavelength coordinates are pending (#1032)".into(),
            );
        }
        if self.transforms.freqframe.is_some()
            && !std::iter::once(self.x_axis)
                .chain(self.y_axes.iter().copied())
                .any(MsAxis::uses_wavelengths)
        {
            return Err("PlotMS reporting-frame transforms for interval and wavelength plots are pending (#1032)".into());
        }
        if self.y_axes.iter().copied().any(|y| {
            let uvw_pair = self.x_axis.uses_uvw() && y.uses_uvw();
            !uvw_pair
                && ((self.x_axis.is_interval_or_wavelength() && !y.is_visibility_math())
                    || (y.is_interval_or_wavelength() && !self.x_axis.is_visibility_math()))
        }) {
            return Err("PlotMS interval and wavelength plots without a visibility axis are pending (#1032)".into());
        }
        Ok(())
    }

    pub(super) fn has_uvw_coordinate_axes(&self) -> bool {
        self.x_axis.uses_uvw() && self.y_axes.iter().copied().all(MsAxis::uses_uvw)
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
        self.validate_with_plotms(false, false, false)
    }

    /// Validates the iteration or page request together with the supplied PlotMS plot settings.
    pub fn validate_with_plotms(
        &self,
        independent_ordinates: bool,
        pointing: bool,
        iterated_ordinates: bool,
    ) -> Result<(), String> {
        if self.plots.is_empty() {
            return Err("msexplore requires at least one plot per invocation".to_string());
        }
        if self.max_plot_points == 0 {
            return Err("msexplore max_plot_points must be a positive integer".to_string());
        }
        if self.plots.len() == 1 {
            self.plots[0].validate_with_plotms(
                independent_ordinates,
                pointing,
                iterated_ordinates,
            )?;
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

pub(super) fn default_max_plot_points() -> usize {
    DEFAULT_MAX_PLOT_POINTS
}
