// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Short PlotMS display name for `axis`.
pub fn axis_name(axis: MsAxis) -> &'static str {
    match axis {
        MsAxis::Amplitude => "Amp",
        MsAxis::Phase => "Phase",
        MsAxis::Real => "Real",
        MsAxis::Imaginary => "Imag",
        MsAxis::Time => "Time",
        MsAxis::Interval => "Interval",
        MsAxis::Channel => "Channel",
        MsAxis::Frequency => "Frequency",
        MsAxis::Velocity => "Velocity",
        MsAxis::RadialVelocity | MsAxis::EphemerisDistance => axis.display_name(),
        MsAxis::Scan => "Scan",
        MsAxis::Field => "Field",
        MsAxis::SpectralWindow => "Spw",
        MsAxis::Correlation => "Corr",
        MsAxis::Baseline => "Baseline",
        MsAxis::Row
        | MsAxis::Antenna1
        | MsAxis::Antenna2
        | MsAxis::Observation
        | MsAxis::Intent
        | MsAxis::Feed1
        | MsAxis::Feed2 => axis.display_name(),
        MsAxis::Antenna => "Antenna",
        MsAxis::UvDistance => "UVdist",
        MsAxis::U => "U",
        MsAxis::V => "V",
        MsAxis::W => "W",
        MsAxis::UvWave => "UVwave",
        MsAxis::UWave => "Uwave",
        MsAxis::VWave => "Vwave",
        MsAxis::WWave => "Wwave",
        MsAxis::Azimuth => "Azimuth",
        MsAxis::Elevation => "Elevation",
        MsAxis::HourAngle => "HourAngle",
        MsAxis::ParallacticAngle => "ParAngle",
        MsAxis::AntennaAzimuth | MsAxis::AntennaElevation | MsAxis::AntennaParallacticAngle => {
            axis.display_name()
        }
        MsAxis::Weight => "Wt",
        MsAxis::Sigma => "Sigma",
        MsAxis::WeightSpectrum => "WtSp",
        MsAxis::WeightedAmplitude => "Wt*Amp",
        MsAxis::SigmaSpectrum => "SigmaSp",
        MsAxis::Flag => "Flag",
        MsAxis::FlagRow => "FlagRow",
        MsAxis::AntRa => "Ant-Ra",
        MsAxis::AntDec => "Ant-Dec",
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

    pub(super) fn lowers_to_listobs_metadata(self) -> Option<ListObsPlotKind> {
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
    /// Integration interval in seconds.
    Interval,
    /// Zero-based channel index.
    Channel,
    /// Channel center frequency in Hz.
    Frequency,
    /// Velocity derived from spectral metadata.
    Velocity,
    /// Radial velocity in km/s from the selected FIELD's attached ephemeris.
    RadialVelocity,
    /// Distance in kilometres from the attached ephemeris's reference point.
    #[serde(rename = "rho")]
    EphemerisDistance,
    /// UV distance `sqrt(u^2 + v^2)` in meters.
    UvDistance,
    /// U coordinate in meters.
    U,
    /// V coordinate in meters.
    V,
    /// W coordinate in meters.
    W,
    /// UV distance in wavelengths at the selected channel frequency.
    #[serde(rename = "uvwave")]
    UvWave,
    /// U coordinate in wavelengths at the selected channel frequency.
    #[serde(rename = "uwave")]
    UWave,
    /// V coordinate in wavelengths at the selected channel frequency.
    #[serde(rename = "vwave")]
    VWave,
    /// W coordinate in wavelengths at the selected channel frequency.
    #[serde(rename = "wwave")]
    WWave,
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
    /// Row number in the selected MeasurementSet.
    Row,
    /// First antenna identifier on the selected baseline.
    Antenna1,
    /// Second antenna identifier on the selected baseline.
    Antenna2,
    /// Observation identifier of the selected visibility buffer.
    Observation,
    /// Identifier of the selected visibility buffer's distinct observing mode.
    Intent,
    /// First feed identifier of the selected visibility buffer.
    Feed1,
    /// Second feed identifier of the selected visibility buffer.
    Feed2,
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
    /// Azimuth in degrees at each antenna position.
    #[serde(rename = "ant-azimuth")]
    AntennaAzimuth,
    /// Elevation in degrees at each antenna position.
    #[serde(rename = "ant-elevation")]
    AntennaElevation,
    /// Feed parallactic angle in degrees including the first receptor angle.
    #[serde(rename = "ant-parangle")]
    AntennaParallacticAngle,
    /// Per-sample weight axis.
    Weight,
    /// Visibility amplitude multiplied by the buffer's correlation weight.
    WeightedAmplitude,
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
            Self::Interval => "interval",
            Self::Channel => "channel",
            Self::Frequency => "frequency",
            Self::Velocity => "velocity",
            Self::RadialVelocity => "radial_velocity",
            Self::EphemerisDistance => "rho",
            Self::UvDistance => "uv_distance",
            Self::U => "u",
            Self::V => "v",
            Self::W => "w",
            Self::UvWave => "uvwave",
            Self::UWave => "uwave",
            Self::VWave => "vwave",
            Self::WWave => "wwave",
            Self::Scan => "scan",
            Self::Field => "field",
            Self::SpectralWindow => "spectral_window",
            Self::Correlation => "correlation",
            Self::Baseline => "baseline",
            Self::Row => "row",
            Self::Antenna1 => "antenna1",
            Self::Antenna2 => "antenna2",
            Self::Observation => "observation",
            Self::Intent => "intent",
            Self::Feed1 => "feed1",
            Self::Feed2 => "feed2",
            Self::Antenna => "antenna",
            Self::Azimuth => "azimuth",
            Self::Elevation => "elevation",
            Self::HourAngle => "hour_angle",
            Self::ParallacticAngle => "parallactic_angle",
            Self::AntennaAzimuth => "ant-azimuth",
            Self::AntennaElevation => "ant-elevation",
            Self::AntennaParallacticAngle => "ant-parangle",
            Self::Weight => "weight",
            Self::WeightedAmplitude => "weighted_amplitude",
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
            Self::Interval => "Interval",
            Self::Channel => "Channel",
            Self::Frequency => "Frequency",
            Self::Velocity => "Velocity",
            Self::RadialVelocity => "Radial Velocity",
            Self::EphemerisDistance => "Distance (rho)",
            Self::UvDistance => "UV Distance",
            Self::U => "U",
            Self::V => "V",
            Self::W => "W",
            Self::UvWave => "UVwave",
            Self::UWave => "Uwave",
            Self::VWave => "Vwave",
            Self::WWave => "Wwave",
            Self::Scan => "Scan",
            Self::Field => "Field",
            Self::SpectralWindow => "Spectral Window",
            Self::Correlation => "Correlation",
            Self::Baseline => "Baseline",
            Self::Row => "Row",
            Self::Antenna1 => "Antenna1",
            Self::Antenna2 => "Antenna2",
            Self::Observation => "Observation",
            Self::Intent => "Intent",
            Self::Feed1 => "Feed1",
            Self::Feed2 => "Feed2",
            Self::Antenna => "Antenna",
            Self::Azimuth => "Azimuth",
            Self::Elevation => "Elevation",
            Self::HourAngle => "Hour Angle",
            Self::ParallacticAngle => "Parallactic Angle",
            Self::AntennaAzimuth => "Ant-Azimuth",
            Self::AntennaElevation => "Ant-Elevation",
            Self::AntennaParallacticAngle => "Ant-ParAngle",
            Self::Weight => "Weight",
            Self::WeightedAmplitude => "Wt*Amp",
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
            "interval" | "time_interval" | "timeinterval" | "timeint" => Ok(Self::Interval),
            "chan" | "channel" => Ok(Self::Channel),
            "freq" | "frequency" => Ok(Self::Frequency),
            "velocity" | "vel" => Ok(Self::Velocity),
            "radial velocity" | "radialvelocity" | "radial_velocity" => Ok(Self::RadialVelocity),
            "distance (rho)" | "distance" | "rho" => Ok(Self::EphemerisDistance),
            "uvdist" | "uv_distance" | "uv-distance" => Ok(Self::UvDistance),
            "u" => Ok(Self::U),
            "v" => Ok(Self::V),
            "w" => Ok(Self::W),
            "uvwave" | "uvdist_l" | "uvdistl" => Ok(Self::UvWave),
            "uwave" => Ok(Self::UWave),
            "vwave" => Ok(Self::VWave),
            "wwave" => Ok(Self::WWave),
            "scan" => Ok(Self::Scan),
            "field" => Ok(Self::Field),
            "spw" | "spectral_window" | "spectral-window" => Ok(Self::SpectralWindow),
            "corr" | "correlation" => Ok(Self::Correlation),
            "baseline" => Ok(Self::Baseline),
            "row" => Ok(Self::Row),
            "ant1" | "antenna1" => Ok(Self::Antenna1),
            "ant2" | "antenna2" => Ok(Self::Antenna2),
            "observation" => Ok(Self::Observation),
            "intent" => Ok(Self::Intent),
            "feed1" => Ok(Self::Feed1),
            "feed2" => Ok(Self::Feed2),
            "antenna" => Ok(Self::Antenna),
            "azimuth" => Ok(Self::Azimuth),
            "elevation" => Ok(Self::Elevation),
            "hourang" | "hourangle" | "hour_angle" | "hour-angle" => Ok(Self::HourAngle),
            "parang" | "parangle" | "parallactic_angle" | "parallactic-angle" => {
                Ok(Self::ParallacticAngle)
            }
            "ant-azimuth" => Ok(Self::AntennaAzimuth),
            "ant-elevation" => Ok(Self::AntennaElevation),
            "ant-parang" | "ant-parangle" | "ant-parallacticangle" => {
                Ok(Self::AntennaParallacticAngle)
            }
            "wt" | "weight" => Ok(Self::Weight),
            "wt*amp" | "weighted_amplitude" => Ok(Self::WeightedAmplitude),
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

    pub fn is_visibility_math(self) -> bool {
        matches!(
            self,
            Self::Amplitude | Self::WeightedAmplitude | Self::Phase | Self::Real | Self::Imaginary
        )
    }

    pub fn is_weight_axis(self) -> bool {
        matches!(
            self,
            Self::Weight
                | Self::WeightedAmplitude
                | Self::WeightSpectrum
                | Self::Sigma
                | Self::SigmaSpectrum
        )
    }

    pub fn is_row_identifier(self) -> bool {
        matches!(
            self,
            Self::Row
                | Self::Antenna1
                | Self::Antenna2
                | Self::Observation
                | Self::Intent
                | Self::Feed1
                | Self::Feed2
        )
    }

    pub fn is_pointing(self) -> bool {
        matches!(self, Self::AntRa | Self::AntDec)
    }

    pub fn uses_derived_geometry(self) -> bool {
        matches!(
            self,
            Self::Azimuth | Self::Elevation | Self::HourAngle | Self::ParallacticAngle
        )
    }

    pub fn is_antenna_horizontal(self) -> bool {
        matches!(
            self,
            Self::AntennaAzimuth | Self::AntennaElevation | Self::AntennaParallacticAngle
        )
    }

    pub fn is_ephemeris(self) -> bool {
        matches!(self, Self::RadialVelocity | Self::EphemerisDistance)
    }

    pub(super) fn uses_channel_bins(self) -> bool {
        self.is_visibility_math()
            || self.uses_wavelengths()
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

    pub(super) fn uses_flag_rows(self) -> bool {
        matches!(self, Self::Flag | Self::FlagRow)
    }

    pub(super) fn uses_correlation_slots(self) -> bool {
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

    pub(super) fn uses_spectral_coordinates(self) -> bool {
        matches!(self, Self::Frequency | Self::Velocity) || self.uses_wavelengths()
    }

    pub(super) fn uses_wavelengths(self) -> bool {
        matches!(self, Self::UvWave | Self::UWave | Self::VWave | Self::WWave)
    }

    pub fn is_interval_or_wavelength(self) -> bool {
        self == Self::Interval || self.uses_wavelengths()
    }

    pub fn uses_uvw(self) -> bool {
        self.uses_uvw_metres() || self.uses_wavelengths()
    }

    pub fn is_uv_plane_axis(self) -> bool {
        matches!(self, Self::U | Self::V | Self::UWave | Self::VWave)
    }

    pub fn uses_uvw_metres(self) -> bool {
        matches!(self, Self::UvDistance | Self::U | Self::V | Self::W)
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
    /// MAIN.FLOAT_DATA for signed observed amplitude.
    Float,
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
    pub(super) fn needs_model(self) -> bool {
        self == Self::Model || self.is_derived()
    }

    pub fn is_corrected_model_expression(self) -> bool {
        matches!(
            self,
            Self::CorrectedMinusModel
                | Self::CorrectedMinusModelScalar
                | Self::CorrectedDivModel
                | Self::CorrectedDivModelScalar
        )
    }

    pub fn is_derived(self) -> bool {
        matches!(
            self,
            Self::DataMinusModel
                | Self::CorrectedMinusModel
                | Self::DataMinusModelScalar
                | Self::CorrectedMinusModelScalar
                | Self::DataDivModel
                | Self::CorrectedDivModel
                | Self::DataDivModelScalar
                | Self::CorrectedDivModelScalar
        )
    }

    pub fn has_unbounded_phase(self) -> bool {
        matches!(
            self,
            Self::DataMinusModelScalar
                | Self::CorrectedMinusModelScalar
                | Self::DataDivModelScalar
                | Self::CorrectedDivModelScalar
        )
    }

    /// Stable machine-readable identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Float => "float",
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
            "float" | "float_data" => Ok(Self::Float),
            "corrected" | "corrected_data" => Ok(Self::Corrected),
            "model" | "model_data" => Ok(Self::Model),
            "residual" | "corrected-model" | "corrected-model_vector" | "corrected_minus_model" => {
                Ok(Self::CorrectedMinusModel)
            }
            "corrected-model_scalar" | "corrected_minus_model_scalar" => {
                Ok(Self::CorrectedMinusModelScalar)
            }
            "data-model" | "data-model_vector" | "data_minus_model" => Ok(Self::DataMinusModel),
            "data-model_scalar" | "data_minus_model_scalar" => Ok(Self::DataMinusModelScalar),
            "corrected/model" | "corrected/model_vector" | "corrected_div_model" => {
                Ok(Self::CorrectedDivModel)
            }
            "corrected/model_scalar" | "corrected_div_model_scalar" => {
                Ok(Self::CorrectedDivModelScalar)
            }
            "data/model" | "data/model_vector" | "data_div_model" => Ok(Self::DataDivModel),
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

/// PlotMS axis label naming `axis` and its data column when applicable.
pub fn column_axis_name(axis: MsAxis, column: MsDataColumn) -> String {
    let name = axis_name(axis);
    if axis.is_visibility_math() && column != MsDataColumn::Data {
        let label = match column {
            MsDataColumn::DataMinusModel => "data-model (vector)",
            MsDataColumn::CorrectedMinusModel => "corrected-model (vector)",
            MsDataColumn::DataMinusModelScalar => "data-model (scalar)",
            MsDataColumn::CorrectedMinusModelScalar => "corrected-model (scalar)",
            MsDataColumn::DataDivModel => "data/model (vector)",
            MsDataColumn::CorrectedDivModel => "corrected/model (vector)",
            MsDataColumn::DataDivModelScalar => "data/model (scalar)",
            MsDataColumn::CorrectedDivModelScalar => "corrected/model (scalar)",
            other => other.as_str(),
        };
        format!("{name}:{label}")
    } else {
        name.to_owned()
    }
}
