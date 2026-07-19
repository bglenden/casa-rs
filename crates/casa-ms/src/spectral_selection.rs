// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style spectral-axis and channel-selection helpers for imaging.
//!
//! CASA handles generic row selectors near `MSSelection`, while spectral cube
//! axis construction is delegated lower to `SubMS`, `MSTransformRegridder`,
//! and synthesis utility code. This module mirrors that split on the Rust
//! side: it owns reusable channel-range resolution and output cube-axis setup
//! that should not live in application code.

use std::str::FromStr;

use casa_types::measures::direction::MDirection;
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::doppler::MDoppler;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};

use crate::derived::engine::MsCalEngine;
use crate::error::{MsError, MsResult};
use crate::selection::syntax::ChannelSelection;

/// Spectral interpolation policy for cube imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CubeInterpolation {
    /// Nearest-neighbour interpolation.
    Nearest,
    /// Linear interpolation.
    #[default]
    Linear,
    /// Cubic interpolation.
    Cubic,
}

/// CASA spectral-cube mode for the shared cube-axis builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CubeSpecMode {
    /// CASA `specmode='cube'`: build the output axis in the requested frame and
    /// apply runtime frequency-frame conversion while assigning rows.
    #[default]
    Cube,
    /// CASA `specmode='cubedata'`: keep the output axis in the native data
    /// frame and skip runtime frequency-frame conversion.
    Cubedata,
}

/// Typed cube-axis value corresponding to CASA `start` / `width`.
#[derive(Debug, Clone, PartialEq)]
pub enum CubeAxisValue {
    /// Channel-number selection in the source SPW.
    Channel(i32),
    /// Frequency-like quantity in Hz.
    FrequencyHz {
        /// Frequency value in Hz.
        hz: f64,
        /// Explicit frame carried by a CASA measure.
        frame: Option<FrequencyRef>,
    },
    /// Velocity-like quantity in m/s.
    VelocityMs {
        /// Velocity value in m/s.
        ///
        /// CASA measure-valued `start` / `width` inputs such as
        /// `me.radialvelocity(...)` and many `me.todoppler(...)` results surface
        /// here, because the casatools layer hands them around as velocity-like
        /// quantities in `m/s` rather than as dimensionless algebraic doppler
        /// parameters.
        ms: f64,
        /// Explicit frame carried by a CASA measure.
        frame: Option<FrequencyRef>,
    },
    /// Doppler value in the given convention.
    Doppler {
        /// Dimensionless algebraic doppler value in the given convention.
        ///
        /// This variant is for callers that already have the convention's
        /// normalized scalar. It is not the usual representation of CASA
        /// `me.todoppler(...)` records in cube-axis task inputs; those are more
        /// commonly handled as [`VelocityMs`](Self::VelocityMs).
        value: f64,
        /// Doppler convention.
        convention: DopplerRef,
    },
}

impl CubeAxisValue {
    /// Parse a simple CASA-style cube-axis value.
    ///
    /// Supported forms currently include plain integers (channel numbers),
    /// frequency quantities such as `1.1GHz`, and velocity quantities such as
    /// `11991.7km/s`.
    pub fn parse(text: &str, veltype: DopplerRef) -> MsResult<Self> {
        let text = text.trim();
        if text.is_empty() {
            return Err(MsError::VersionError(
                "cube axis value was empty".to_string(),
            ));
        }
        if let Ok(channel) = text.parse::<i32>() {
            return Ok(Self::Channel(channel));
        }

        let quantity = Quantity::from_str(text).map_err(|error| {
            MsError::VersionError(format!("parse cube axis value {text:?}: {error}"))
        })?;
        let hz = Unit::new("Hz").expect("built-in Hz unit must parse");
        let velocity = Unit::new("m/s").expect("built-in velocity unit must parse");
        let dimensionless = Unit::dimensionless();
        if quantity.unit().conformant(&hz) {
            return Ok(Self::FrequencyHz {
                hz: quantity.get_value_in(&hz).map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube axis frequency {text:?} to Hz: {error}"
                    ))
                })?,
                frame: None,
            });
        }
        if quantity.unit().conformant(&velocity) {
            return Ok(Self::VelocityMs {
                ms: quantity.get_value_in(&velocity).map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube axis velocity {text:?} to m/s: {error}"
                    ))
                })?,
                frame: None,
            });
        }
        if quantity.unit().conformant(&dimensionless) {
            return Ok(Self::Doppler {
                value: quantity.value(),
                convention: veltype,
            });
        }

        Err(MsError::VersionError(format!(
            "cube axis value {text:?} is neither a channel id, frequency, velocity, nor dimensionless doppler value"
        )))
    }
}

/// Parse a CASA-style rest frequency quantity into Hz.
///
/// This accepts explicit frequency units such as `1.25GHz`. Bare numeric values
/// follow CASA task semantics and are interpreted as MHz.
pub fn parse_rest_frequency_hz(value: &str) -> MsResult<f64> {
    let quantity = value.trim().parse::<Quantity>().map_err(|error| {
        MsError::VersionError(format!("invalid rest frequency {value:?}: {error}"))
    })?;
    let hz = if quantity.unit().name().is_empty() {
        quantity.value() * 1.0e6
    } else {
        quantity
            .get_value_in(&Unit::new("Hz").expect("built-in Hz unit must parse"))
            .map_err(|error| {
                MsError::VersionError(format!("convert rest frequency {value:?} to Hz: {error}"))
            })?
    };
    if hz <= 0.0 || !hz.is_finite() {
        return Err(MsError::VersionError(format!(
            "rest frequency must resolve to a positive finite Hz value, got {hz}"
        )));
    }
    Ok(hz)
}

/// CASA-style cube-axis construction options.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeAxisConfig {
    /// CASA spectral-cube mode.
    pub specmode: CubeSpecMode,
    /// Output frequency frame for the image spectral axis.
    pub outframe: FrequencyRef,
    /// Velocity convention used by velocity-like start/width values.
    pub veltype: DopplerRef,
    /// Output-axis interpolation policy.
    pub interpolation: CubeInterpolation,
    /// Rest frequency in Hz for velocity and Doppler axes.
    pub rest_frequency_hz: Option<f64>,
    /// Optional cube-axis start value.
    pub start: Option<CubeAxisValue>,
    /// Optional cube-axis width value.
    pub width: Option<CubeAxisValue>,
}

impl Default for CubeAxisConfig {
    fn default() -> Self {
        Self {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::LSRK,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: None,
            start: None,
            width: None,
        }
    }
}

/// Resolved contiguous channel selection for one spectral window.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedChannelSelection {
    /// Concrete source-channel indices in the SPW.
    pub indices: Vec<usize>,
    /// Frequencies for the selected channels.
    pub frequencies_hz: Vec<f64>,
}

/// Cube spectral-axis setup shared between the MeasurementSet adapter and the
/// pure imaging core.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeSpectralSetup {
    /// Source spectral frame from the SPW definition.
    pub source_freq_ref: FrequencyRef,
    /// Output spectral frame for the cube axis.
    pub output_freq_ref: FrequencyRef,
    /// Spectral interpolation policy for mapping source samples into cube planes.
    pub interpolation: CubeInterpolation,
    /// Whether interpolation should use native source-channel frequencies
    /// rather than source frequencies converted into the output frame.
    pub interpolation_uses_native_source_frequencies: bool,
    /// Reference row time used to define the output cube spectral frame.
    pub output_frame_reference_time_mjd_sec: f64,
    /// Field id used to define the output cube spectral frame.
    pub output_frame_field_id: usize,
    /// Output cube channel frequencies in `output_freq_ref`.
    pub output_channel_frequencies_hz: Vec<f64>,
    /// Output cube channel widths in `output_freq_ref`.
    pub output_channel_widths_hz: Vec<f64>,
}

/// One source-spectral contribution into an output cube plane.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CubeChannelContribution {
    /// Source channel contributing to the output plane.
    pub source_channel: usize,
    /// Native source-channel frequency in Hz used for wavelength scaling.
    pub source_frequency_hz: f64,
    /// Linear contribution factor in `[0, 1]`.
    pub factor: f32,
}

/// One CASA `FTMachine::interpolateFrequencyTogrid` spectral sample and the
/// output cube plane it grids into.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeGridChannelContributions {
    /// Output cube plane selected by the channel map.
    pub output_channel: usize,
    /// Frequency in the interpolation frame used for this gridded sample.
    pub grid_frequency_hz: f64,
    /// Source-channel contributions into this gridded sample.
    pub contributions: Vec<CubeChannelContribution>,
}

/// All spectral interpolation products needed for one prepared cube row.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeRowSpectralContributions {
    /// Source-channel contributions into each output cube plane for this row.
    pub output_channel_contributions: Vec<Vec<CubeChannelContribution>>,
    /// CASA `FTMachine::channelMap`-style nearest output plane for each
    /// selected source channel.
    pub source_channel_output_map: Vec<Option<usize>>,
    /// Source-channel map through CASA's padded Briggs-cube weight grid.
    ///
    /// `BriggsCubeWeightor` expands the spectral density cube by two channels
    /// on either side and shifts its reference pixel by two channels. This map
    /// keeps CASA's padded density-plane index rather than collapsing back to
    /// the final image-plane index.
    pub padded_source_channel_output_map: Vec<Option<usize>>,
    /// CASA `BriggsCubeWeightor` density-grid spectral samples and their
    /// padded density-plane map.
    pub padded_grid_channel_contributions: Vec<CubeGridChannelContributions>,
    /// CASA `FTMachine::interpolateFrequencyTogrid`-style grid samples and
    /// their output channel map.
    pub grid_channel_contributions: Vec<CubeGridChannelContributions>,
    /// Output-model channel contributions used while degridding each selected
    /// source channel for this row.
    pub source_channel_model_contributions: Vec<Vec<CubeChannelContribution>>,
}

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

fn ensure_supported_interpolation(interpolation: CubeInterpolation) -> MsResult<()> {
    match interpolation {
        CubeInterpolation::Nearest | CubeInterpolation::Linear => Ok(()),
        CubeInterpolation::Cubic => Err(MsError::VersionError(
            "cube interpolation 'cubic' is not implemented; use 'nearest' or 'linear'".to_string(),
        )),
    }
}

fn channel_edges_from_centers(all_source_frequencies_hz: &[f64]) -> MsResult<Vec<f64>> {
    if all_source_frequencies_hz.is_empty() {
        return Err(MsError::VersionError(
            "cannot build cube channel axis from an empty SPW".to_string(),
        ));
    }
    if all_source_frequencies_hz.len() == 1 {
        let center = all_source_frequencies_hz[0];
        return Ok(vec![center - 0.5, center + 0.5]);
    }

    let mut edges = Vec::with_capacity(all_source_frequencies_hz.len() + 1);
    let first_step = all_source_frequencies_hz[1] - all_source_frequencies_hz[0];
    edges.push(all_source_frequencies_hz[0] - first_step / 2.0);
    for pair in all_source_frequencies_hz.windows(2) {
        edges.push(0.5 * (pair[0] + pair[1]));
    }
    let last = all_source_frequencies_hz.len() - 1;
    let last_step = all_source_frequencies_hz[last] - all_source_frequencies_hz[last - 1];
    edges.push(all_source_frequencies_hz[last] + last_step / 2.0);
    Ok(edges)
}

fn channel_widths_from_centers(channel_frequencies_hz: &[f64]) -> MsResult<Vec<f64>> {
    let edges = channel_edges_from_centers(channel_frequencies_hz)?;
    Ok(edges
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).abs())
        .collect())
}

fn channel_mode_single_output_width_hz(
    source_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    start_channel: i32,
    width_channels: i32,
) -> MsResult<f64> {
    if width_channels == 0 {
        return Err(MsError::VersionError(
            "cube channel width must not be zero".to_string(),
        ));
    }
    let nchannels = i32::try_from(source_frequencies_hz.len()).map_err(|_| {
        MsError::VersionError("spectral window channel count exceeds i32".to_string())
    })?;
    if source_channel_widths_hz.len() != source_frequencies_hz.len() {
        return Err(MsError::VersionError(format!(
            "channel-mode cube axis requires matching frequency/width arrays, got {} and {}",
            source_frequencies_hz.len(),
            source_channel_widths_hz.len()
        )));
    }
    let (low, high) = if width_channels > 0 {
        let low = start_channel;
        let high = low + width_channels;
        (low, high)
    } else {
        let high = start_channel + 1;
        let low = high + width_channels;
        (low, high)
    };
    if low < 0 || high < 0 || low >= nchannels || high > nchannels || low >= high {
        return Err(MsError::VersionError(format!(
            "cube channel axis bin [{low}, {high}) is outside the SPW with {nchannels} channels"
        )));
    }
    let selected = usize::try_from(low).expect("validated low bound")
        ..usize::try_from(high).expect("validated high bound");
    let low_edge_hz = selected
        .clone()
        .map(|index| source_frequencies_hz[index] - source_channel_widths_hz[index].abs() / 2.0)
        .reduce(f64::min)
        .expect("validated non-empty channel range");
    let high_edge_hz = selected
        .map(|index| source_frequencies_hz[index] + source_channel_widths_hz[index].abs() / 2.0)
        .reduce(f64::max)
        .expect("validated non-empty channel range");
    Ok((high_edge_hz - low_edge_hz).abs())
}

fn channel_mode_output_centers(
    all_source_frequencies_hz: &[f64],
    all_source_channel_widths_hz: &[f64],
    start_channel: i32,
    width_channels: i32,
    nchan: usize,
) -> MsResult<Vec<f64>> {
    if width_channels == 0 {
        return Err(MsError::VersionError(
            "cube channel width must not be zero".to_string(),
        ));
    }
    let nchannels = i32::try_from(all_source_frequencies_hz.len()).map_err(|_| {
        MsError::VersionError("spectral window channel count exceeds i32".to_string())
    })?;
    if all_source_channel_widths_hz.len() != all_source_frequencies_hz.len() {
        return Err(MsError::VersionError(format!(
            "channel-mode cube axis requires matching frequency/width arrays, got {} and {}",
            all_source_frequencies_hz.len(),
            all_source_channel_widths_hz.len()
        )));
    }
    if all_source_frequencies_hz.first() <= all_source_frequencies_hz.last() {
        return channel_mode_output_centers_simple(
            all_source_frequencies_hz,
            all_source_channel_widths_hz,
            start_channel,
            width_channels,
            nchan,
        );
    }

    let mut trans_freq_hz = all_source_frequencies_hz.to_vec();
    let mut trans_width_hz = all_source_channel_widths_hz
        .iter()
        .map(|width_hz| width_hz.abs())
        .collect::<Vec<_>>();
    let mut start = start_channel;
    let mut start_is_end = width_channels < 0;

    if trans_freq_hz.first() > trans_freq_hz.last() {
        trans_freq_hz.reverse();
        trans_width_hz.reverse();
        start = nchannels - 1 - start;
        start_is_end = !start_is_end;
    }

    if start < 0 || start >= nchannels {
        return Err(MsError::VersionError(format!(
            "cube channel start {start_channel} is outside the SPW with {nchannels} channels"
        )));
    }

    let requested_nchan = i32::try_from(nchan)
        .map_err(|_| MsError::VersionError("cube channel count exceeds i32".to_string()))?;
    let mut bandwidth_channels = requested_nchan;
    let mut center_channel = start;
    if start_is_end {
        center_channel -= bandwidth_channels / 2;
    } else {
        center_channel += bandwidth_channels / 2;
    }

    if center_channel - bandwidth_channels / 2 < 0 {
        bandwidth_channels = 2 * center_channel + 1;
    }
    if nchannels < center_channel + bandwidth_channels / 2 {
        bandwidth_channels = 2 * (nchannels - center_channel);
    }

    let requested_width_channels = width_channels.abs();
    let channel_width = if requested_width_channels < 1 {
        1
    } else if requested_width_channels > bandwidth_channels {
        bandwidth_channels
    } else {
        requested_width_channels
    };
    bandwidth_channels = requested_nchan * channel_width;

    let bandwidth_lower_end_channel = center_channel - bandwidth_channels / 2;
    let bandwidth_upper_end_channel = bandwidth_lower_end_channel + bandwidth_channels - 1;

    if channel_width == bandwidth_channels {
        let low = usize::try_from(bandwidth_lower_end_channel)
            .expect("validated lower single-channel bound");
        let high = usize::try_from(bandwidth_upper_end_channel)
            .expect("validated upper single-channel bound");
        return Ok(vec![
            0.5 * ((trans_freq_hz[low] - trans_width_hz[low] / 2.0)
                + (trans_freq_hz[high] + trans_width_hz[high] / 2.0)),
        ]);
    }

    let mut lower_bounds_hz = Vec::new();
    let mut upper_bounds_hz = Vec::new();
    let channel_ratio = bandwidth_channels / channel_width;
    let start_channel_index = if channel_ratio % 2 != 0 {
        center_channel - channel_width / 2
    } else {
        center_channel
    };

    let mut lower_indices_up = Vec::new();
    let mut upper_indices_up = Vec::new();
    let mut index = start_channel_index;
    while index <= bandwidth_upper_end_channel {
        lower_indices_up.push(index);
        upper_indices_up.push((index + channel_width - 1).min(bandwidth_upper_end_channel));
        index += channel_width;
    }

    let mut lower_indices_down = Vec::new();
    let mut upper_indices_down = Vec::new();
    let mut index = start_channel_index - 1;
    while index >= bandwidth_lower_end_channel {
        upper_indices_down.push(index);
        lower_indices_down.push((index - channel_width + 1).max(bandwidth_lower_end_channel));
        index -= channel_width;
    }

    for reverse_index in (0..lower_indices_down.len()).rev() {
        let low = usize::try_from(lower_indices_down[reverse_index])
            .expect("validated lower descending bound");
        let high = usize::try_from(upper_indices_down[reverse_index])
            .expect("validated upper descending bound");
        lower_bounds_hz.push(trans_freq_hz[low] - trans_width_hz[low] / 2.0);
        upper_bounds_hz.push(trans_freq_hz[high] + trans_width_hz[high] / 2.0);
    }
    for index in 0..lower_indices_up.len() {
        let low =
            usize::try_from(lower_indices_up[index]).expect("validated lower ascending bound");
        let high =
            usize::try_from(upper_indices_up[index]).expect("validated upper ascending bound");
        lower_bounds_hz.push(trans_freq_hz[low] - trans_width_hz[low] / 2.0);
        upper_bounds_hz.push(trans_freq_hz[high] + trans_width_hz[high] / 2.0);
    }

    Ok(lower_bounds_hz
        .into_iter()
        .zip(upper_bounds_hz)
        .map(|(low_hz, high_hz)| 0.5 * (low_hz + high_hz))
        .collect())
}

fn casa_regridding_input_widths(
    source_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
) -> MsResult<Vec<f64>> {
    if source_frequencies_hz.len() != source_channel_widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "CASA regridding widths require matching frequency/width arrays, got {} and {}",
            source_frequencies_hz.len(),
            source_channel_widths_hz.len()
        )));
    }
    match source_frequencies_hz.len() {
        0 => Err(MsError::VersionError(
            "cannot build CASA regridding widths from an empty SPW".to_string(),
        )),
        _ => Ok(source_channel_widths_hz
            .iter()
            .map(|width_hz| width_hz.abs())
            .collect()),
    }
}

fn casa_transformed_channel_mode_output_centers(
    transformed_source_frequencies_hz: &[f64],
    transformed_source_channel_widths_hz: &[f64],
    start_channel: i32,
    width_channels: i32,
    nchan: usize,
) -> MsResult<Vec<f64>> {
    if width_channels == 0 {
        return Err(MsError::VersionError(
            "cube channel width must not be zero".to_string(),
        ));
    }
    if transformed_source_frequencies_hz.len() != transformed_source_channel_widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "CASA transformed cube axis requires matching frequency/width arrays, got {} and {}",
            transformed_source_frequencies_hz.len(),
            transformed_source_channel_widths_hz.len()
        )));
    }
    if transformed_source_frequencies_hz.is_empty() {
        return Err(MsError::VersionError(
            "cannot build CASA transformed cube axis from an empty SPW".to_string(),
        ));
    }

    let nchannels = i32::try_from(transformed_source_frequencies_hz.len()).map_err(|_| {
        MsError::VersionError("spectral window channel count exceeds i32".to_string())
    })?;
    let mut transformed_frequencies_hz = transformed_source_frequencies_hz.to_vec();
    let mut transformed_widths_hz = transformed_source_channel_widths_hz
        .iter()
        .map(|width_hz| width_hz.abs())
        .collect::<Vec<_>>();
    let mut first_channel = start_channel;
    let mut start_is_end = width_channels < 0;
    let descending = transformed_frequencies_hz.first() > transformed_frequencies_hz.last();

    if descending {
        transformed_frequencies_hz.reverse();
        transformed_widths_hz.reverse();
        first_channel = nchannels - 1 - first_channel;
        start_is_end = !start_is_end;
    }

    if first_channel < 0 || first_channel >= nchannels {
        return Err(MsError::VersionError(format!(
            "cube channel start {start_channel} is outside the SPW with {nchannels} channels"
        )));
    }

    let output_channel_width_hz =
        transformed_widths_hz[0] * f64::from(width_channels.unsigned_abs());
    let output_bandwidth_hz = output_channel_width_hz * nchan as f64;
    let first_channel = usize::try_from(first_channel).expect("validated channel index");
    let start_edge_hz = if start_is_end {
        transformed_frequencies_hz[first_channel] + transformed_widths_hz[first_channel] / 2.0
            - output_bandwidth_hz
    } else {
        transformed_frequencies_hz[first_channel] - transformed_widths_hz[first_channel] / 2.0
    };
    let mut centers_hz = (0..nchan)
        .map(|index| start_edge_hz + (index as f64 + 0.5) * output_channel_width_hz)
        .collect::<Vec<_>>();
    if descending ^ (width_channels < 0) {
        centers_hz.reverse();
    }
    Ok(centers_hz)
}

fn channel_mode_output_centers_simple(
    all_source_frequencies_hz: &[f64],
    all_source_channel_widths_hz: &[f64],
    start_channel: i32,
    width_channels: i32,
    nchan: usize,
) -> MsResult<Vec<f64>> {
    let nchannels = i32::try_from(all_source_frequencies_hz.len()).map_err(|_| {
        MsError::VersionError("spectral window channel count exceeds i32".to_string())
    })?;
    let mut centers = Vec::with_capacity(nchan);
    for offset in 0..nchan {
        let offset = i32::try_from(offset)
            .map_err(|_| MsError::VersionError("cube channel count exceeds i32".to_string()))?;
        let (low, high) = if width_channels > 0 {
            let low = start_channel + offset * width_channels;
            let high = low + width_channels;
            (low, high)
        } else {
            let high = start_channel + 1 + offset * width_channels;
            let low = high + width_channels;
            (low, high)
        };
        if low < 0 || high < 0 || low >= nchannels || high > nchannels || low >= high {
            return Err(MsError::VersionError(format!(
                "cube channel axis bin [{low}, {high}) is outside the SPW with {nchannels} channels"
            )));
        }
        let selected = usize::try_from(low).expect("validated low bound")
            ..usize::try_from(high).expect("validated high bound");
        let low_edge_hz = selected
            .clone()
            .map(|index| {
                all_source_frequencies_hz[index] - all_source_channel_widths_hz[index].abs() / 2.0
            })
            .reduce(f64::min)
            .expect("validated non-empty channel range");
        let high_edge_hz = selected
            .map(|index| {
                all_source_frequencies_hz[index] + all_source_channel_widths_hz[index].abs() / 2.0
            })
            .reduce(f64::max)
            .expect("validated non-empty channel range");
        centers.push(0.5 * (low_edge_hz + high_edge_hz));
    }
    Ok(centers)
}

/// Resolve a simple contiguous source-channel selection.
pub fn resolve_contiguous_channel_selection(
    all_frequencies_hz: &[f64],
    start: Option<usize>,
    count: Option<usize>,
) -> MsResult<ResolvedChannelSelection> {
    let start = start.unwrap_or(0);
    if start >= all_frequencies_hz.len() {
        return Err(MsError::VersionError(format!(
            "channel start {start} is outside spectral window with {} channels",
            all_frequencies_hz.len()
        )));
    }
    let count = count.unwrap_or(all_frequencies_hz.len() - start);
    if count == 0 || start + count > all_frequencies_hz.len() {
        return Err(MsError::VersionError(format!(
            "channel selection [{start}, {}) is outside spectral window with {} channels",
            start + count,
            all_frequencies_hz.len()
        )));
    }
    Ok(ResolvedChannelSelection {
        indices: (start..start + count).collect(),
        frequencies_hz: all_frequencies_hz[start..start + count].to_vec(),
    })
}

/// Resolve an explicit CASA channel selector such as `4~9;12~14` or `0~10^2`.
pub fn resolve_channel_selector_selection(
    all_frequencies_hz: &[f64],
    selector: &ChannelSelection,
) -> MsResult<ResolvedChannelSelection> {
    let indices = selector.indices(all_frequencies_hz.len())?;
    if indices.is_empty() {
        return Err(MsError::VersionError(
            "channel selector resolved to zero channels".to_string(),
        ));
    }
    Ok(ResolvedChannelSelection {
        frequencies_hz: indices
            .iter()
            .map(|&index| all_frequencies_hz[index])
            .collect(),
        indices,
    })
}

impl CubeSpectralSetup {
    /// Build a CASA-style output cube axis.
    #[allow(clippy::too_many_arguments)]
    pub fn for_casa_cube_axis(
        source_freq_ref: FrequencyRef,
        all_source_frequencies_hz: &[f64],
        all_source_channel_widths_hz: &[f64],
        nchan: usize,
        axis_config: &CubeAxisConfig,
        reference_row_time_mjd_sec: f64,
        field_id: usize,
        phase_center_direction: Option<MDirection>,
        time_bounds_mjd_sec: [f64; 2],
        derived_engine: &MsCalEngine,
    ) -> MsResult<(Self, ResolvedChannelSelection)> {
        ensure_supported_interpolation(axis_config.interpolation)?;
        let channel_mode = axis_config.start.is_none() && axis_config.width.is_none()
            || matches!(axis_config.start, Some(CubeAxisValue::Channel(_)))
                && axis_config.width.is_none()
            || matches!(
                (&axis_config.start, &axis_config.width),
                (
                    Some(CubeAxisValue::Channel(_)) | None,
                    Some(CubeAxisValue::Channel(_)) | None
                )
            );
        if channel_mode {
            return Self::for_casa_cube_channel_axis(
                source_freq_ref,
                all_source_frequencies_hz,
                all_source_channel_widths_hz,
                nchan,
                axis_config,
                reference_row_time_mjd_sec,
                field_id,
                phase_center_direction,
                time_bounds_mjd_sec,
                derived_engine,
            );
        }

        if nchan == 0 {
            return Err(MsError::VersionError(
                "cube output channel count must be positive".to_string(),
            ));
        }

        let output_freq_ref = effective_output_frequency_ref(source_freq_ref, axis_config)?;
        let frame = if matches!(axis_config.specmode, CubeSpecMode::Cubedata)
            || source_freq_ref == output_freq_ref
        {
            None
        } else {
            Some(
                spectral_frame_observatory(
                    derived_engine,
                    reference_row_time_mjd_sec,
                    field_id,
                    phase_center_direction.clone(),
                )
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "build spectral frame for field {field_id}: {error}"
                    ))
                })?,
            )
        };
        let source_frequencies_in_output_frame =
            if matches!(axis_config.specmode, CubeSpecMode::Cubedata) {
                all_source_frequencies_hz.to_vec()
            } else {
                all_source_frequencies_hz
                    .iter()
                    .copied()
                    .map(|frequency_hz| {
                        convert_frequency_to_frame_with_frame(
                            source_freq_ref,
                            output_freq_ref,
                            frequency_hz,
                            frame.as_ref(),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?
            };
        if uses_optical_velocity_axis(axis_config) {
            let output_channel_frequencies_hz = optical_velocity_output_centers(
                &source_frequencies_in_output_frame,
                all_source_channel_widths_hz,
                nchan,
                axis_config,
            )?;
            let setup = Self {
                source_freq_ref,
                output_freq_ref,
                interpolation: axis_config.interpolation,
                interpolation_uses_native_source_frequencies: uses_native_source_interpolation(
                    axis_config,
                ),
                output_frame_reference_time_mjd_sec: reference_row_time_mjd_sec,
                output_frame_field_id: field_id,
                output_channel_widths_hz: channel_widths_from_centers(
                    &output_channel_frequencies_hz,
                )?,
                output_channel_frequencies_hz,
            };
            let support = cube_source_channel_support(
                all_source_frequencies_hz,
                all_source_channel_widths_hz,
                &setup,
                time_bounds_mjd_sec,
                field_id,
                derived_engine,
            )?;
            return Ok((setup, support));
        }
        let native_spacing_hz = infer_native_spacing_hz(&source_frequencies_in_output_frame)?;

        let delta_hz = match axis_config.width.as_ref() {
            Some(value) => cube_axis_delta_hz_from_value(
                value,
                axis_config.rest_frequency_hz,
                axis_config.veltype,
            )?,
            None => match axis_config.start.as_ref() {
                Some(CubeAxisValue::FrequencyHz { .. }) => native_spacing_hz.abs(),
                Some(CubeAxisValue::VelocityMs { .. }) | Some(CubeAxisValue::Doppler { .. }) => {
                    -native_spacing_hz.abs()
                }
                _ => native_spacing_hz,
            },
        };
        if delta_hz == 0.0 {
            return Err(MsError::VersionError(
                "cube output frequency increment must not be zero".to_string(),
            ));
        }

        let start_frequency_hz = match axis_config.start.as_ref() {
            Some(value) => cube_axis_frequency_hz_from_value(
                value,
                output_freq_ref,
                axis_config.rest_frequency_hz,
                axis_config.veltype,
                reference_row_time_mjd_sec,
                field_id,
                derived_engine,
            )?,
            None => {
                default_generic_axis_start_hz(&source_frequencies_in_output_frame, delta_hz, nchan)?
            }
        };

        let start_frequency_hz = snap_default_frequency_like_start_to_source_grid(
            axis_config,
            &source_frequencies_in_output_frame,
            start_frequency_hz,
        );

        let output_channel_frequencies_hz = (0..nchan)
            .map(|index| start_frequency_hz + index as f64 * delta_hz)
            .collect::<Vec<_>>();
        let setup = Self {
            source_freq_ref,
            output_freq_ref,
            interpolation: axis_config.interpolation,
            interpolation_uses_native_source_frequencies: uses_native_source_interpolation(
                axis_config,
            ),
            output_frame_reference_time_mjd_sec: reference_row_time_mjd_sec,
            output_frame_field_id: field_id,
            output_channel_widths_hz: channel_widths_from_centers(&output_channel_frequencies_hz)?,
            output_channel_frequencies_hz,
        };
        let support = cube_source_channel_support(
            all_source_frequencies_hz,
            all_source_channel_widths_hz,
            &setup,
            time_bounds_mjd_sec,
            field_id,
            derived_engine,
        )?;
        Ok((setup, support))
    }

    /// Build a CASA-style output cube axis in channel mode.
    ///
    /// This models the source-backed `tclean` cube tests that specify
    /// `start`/`width` as channel numbers. Output channels are defined on the
    /// full SPW channel axis; any SPW channel selector affects which input data
    /// are available, not the output channel locations themselves.
    #[allow(clippy::too_many_arguments)]
    pub fn for_casa_cube_channel_axis(
        source_freq_ref: FrequencyRef,
        all_source_frequencies_hz: &[f64],
        all_source_channel_widths_hz: &[f64],
        nchan: usize,
        axis_config: &CubeAxisConfig,
        reference_row_time_mjd_sec: f64,
        field_id: usize,
        phase_center_direction: Option<MDirection>,
        time_bounds_mjd_sec: [f64; 2],
        derived_engine: &MsCalEngine,
    ) -> MsResult<(Self, ResolvedChannelSelection)> {
        ensure_supported_interpolation(axis_config.interpolation)?;
        if nchan == 0 {
            return Err(MsError::VersionError(
                "cube output channel count must be positive".to_string(),
            ));
        }
        let start_channel = match axis_config.start.as_ref() {
            None => 0,
            Some(CubeAxisValue::Channel(channel)) => *channel,
            Some(other) => {
                return Err(MsError::VersionError(format!(
                    "channel-mode cube axis currently expects a channel-valued start, found {other:?}"
                )));
            }
        };
        let width_channels = match axis_config.width.as_ref() {
            None => 1,
            Some(CubeAxisValue::Channel(channel_width)) => *channel_width,
            Some(other) => {
                return Err(MsError::VersionError(format!(
                    "channel-mode cube axis currently expects a channel-valued width, found {other:?}"
                )));
            }
        };
        if width_channels == 0 {
            return Err(MsError::VersionError(
                "cube channel width must not be zero".to_string(),
            ));
        }
        let output_freq_ref = match axis_config.specmode {
            CubeSpecMode::Cube => axis_config.outframe,
            CubeSpecMode::Cubedata => source_freq_ref,
        };
        let (output_channel_frequencies_hz, output_channel_widths_hz) =
            if matches!(axis_config.specmode, CubeSpecMode::Cubedata) {
                let output_channel_frequencies_hz = channel_mode_output_centers(
                    all_source_frequencies_hz,
                    all_source_channel_widths_hz,
                    start_channel,
                    width_channels,
                    nchan,
                )?;
                let output_channel_widths_hz = if nchan == 1 {
                    vec![channel_mode_single_output_width_hz(
                        all_source_frequencies_hz,
                        all_source_channel_widths_hz,
                        start_channel,
                        width_channels,
                    )?]
                } else {
                    channel_widths_from_centers(&output_channel_frequencies_hz)?
                };
                (output_channel_frequencies_hz, output_channel_widths_hz)
            } else {
                let frame = spectral_frame_observatory(
                    derived_engine,
                    reference_row_time_mjd_sec,
                    field_id,
                    phase_center_direction,
                )
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "build spectral frame for field {field_id}: {error}"
                    ))
                })?;
                let source_frequencies_in_output_frame = all_source_frequencies_hz
                    .iter()
                    .copied()
                    .map(|frequency_hz| {
                        convert_frequency_to_frame_with_frame(
                            source_freq_ref,
                            output_freq_ref,
                            frequency_hz,
                            Some(&frame),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                let regridding_input_widths_hz = casa_regridding_input_widths(
                    all_source_frequencies_hz,
                    all_source_channel_widths_hz,
                )?;
                let source_channel_widths_in_output_frame =
                    convert_channel_widths_to_frame_with_frame(
                        source_freq_ref,
                        output_freq_ref,
                        all_source_frequencies_hz,
                        &regridding_input_widths_hz,
                        &frame,
                    )?;
                let output_channel_frequencies_hz = casa_transformed_channel_mode_output_centers(
                    &source_frequencies_in_output_frame,
                    &source_channel_widths_in_output_frame,
                    start_channel,
                    width_channels,
                    nchan,
                )?;
                let output_channel_widths_hz = if nchan == 1 {
                    vec![channel_mode_single_output_width_hz(
                        &source_frequencies_in_output_frame,
                        &source_channel_widths_in_output_frame,
                        start_channel,
                        width_channels,
                    )?]
                } else {
                    channel_widths_from_centers(&output_channel_frequencies_hz)?
                };
                (output_channel_frequencies_hz, output_channel_widths_hz)
            };
        let setup = Self {
            source_freq_ref,
            output_freq_ref,
            interpolation: axis_config.interpolation,
            interpolation_uses_native_source_frequencies: uses_native_source_interpolation(
                axis_config,
            ),
            output_frame_reference_time_mjd_sec: reference_row_time_mjd_sec,
            output_frame_field_id: field_id,
            output_channel_widths_hz,
            output_channel_frequencies_hz,
        };
        let support = cube_source_channel_support(
            all_source_frequencies_hz,
            all_source_channel_widths_hz,
            &setup,
            time_bounds_mjd_sec,
            field_id,
            derived_engine,
        )?;
        Ok((setup, support))
    }

    /// Build all per-row spectral interpolation products needed by cube
    /// preparation and major-cycle prediction.
    pub fn row_spectral_contributions(
        &self,
        source_frequencies_hz: &[f64],
        source_channel_widths_hz: &[f64],
        row_time_mjd_sec: f64,
        row_field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<CubeRowSpectralContributions> {
        ensure_supported_interpolation(self.interpolation)?;
        if source_channel_widths_hz.len() != source_frequencies_hz.len() {
            return Err(MsError::VersionError(format!(
                "row cube interpolation requires matching frequency/width arrays, got {} and {}",
                source_frequencies_hz.len(),
                source_channel_widths_hz.len()
            )));
        }
        let (source_spectral_frame, output_spectral_frame) =
            self.row_and_output_spectral_frames(row_time_mjd_sec, row_field_id, derived_engine)?;
        let source_frequencies_for_interpolation =
            if self.interpolation_uses_native_source_frequencies {
                source_frequencies_hz.to_vec()
            } else {
                source_frequencies_hz
                    .iter()
                    .copied()
                    .map(|source_frequency_hz| {
                        convert_frequency_to_frame_with_frames(
                            self.source_freq_ref,
                            self.output_freq_ref,
                            source_frequency_hz,
                            source_spectral_frame.as_ref(),
                            output_spectral_frame.as_ref(),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?
            };
        let source_channel_widths_for_interpolation = if self.interpolation
            == CubeInterpolation::Nearest
            || self.interpolation_uses_native_source_frequencies
        {
            source_channel_widths_hz.to_vec()
        } else {
            match (
                source_spectral_frame.as_ref(),
                output_spectral_frame.as_ref(),
            ) {
                (Some(source_frame), Some(output_frame)) => {
                    convert_channel_widths_to_frame_with_frames(
                        self.source_freq_ref,
                        self.output_freq_ref,
                        source_frequencies_hz,
                        source_channel_widths_hz,
                        source_frame,
                        output_frame,
                    )?
                }
                (None, None) => source_channel_widths_hz.to_vec(),
                _ => unreachable!("source and output spectral frames are paired"),
            }
        };
        let output_frequencies_for_interpolation =
            if self.interpolation_uses_native_source_frequencies {
                match (
                    source_spectral_frame.as_ref(),
                    output_spectral_frame.as_ref(),
                ) {
                    (Some(source_frame), Some(output_frame)) => self
                        .output_channel_frequencies_hz
                        .iter()
                        .copied()
                        .map(|output_frequency_hz| {
                            convert_frequency_to_frame_with_frames(
                                self.output_freq_ref,
                                self.source_freq_ref,
                                output_frequency_hz,
                                Some(output_frame),
                                Some(source_frame),
                            )
                        })
                        .collect::<MsResult<Vec<_>>>()?,
                    (None, None) => self.output_channel_frequencies_hz.clone(),
                    _ => unreachable!("source and output spectral frames are paired"),
                }
            } else {
                self.output_channel_frequencies_hz.clone()
            };
        let output_channel_widths_for_interpolation =
            if self.interpolation_uses_native_source_frequencies {
                match (
                    source_spectral_frame.as_ref(),
                    output_spectral_frame.as_ref(),
                ) {
                    (Some(source_frame), Some(output_frame)) => {
                        convert_channel_widths_to_frame_with_frames(
                            self.output_freq_ref,
                            self.source_freq_ref,
                            &self.output_channel_frequencies_hz,
                            &self.output_channel_widths_hz,
                            output_frame,
                            source_frame,
                        )?
                    }
                    (None, None) => self.output_channel_widths_hz.clone(),
                    _ => unreachable!("source and output spectral frames are paired"),
                }
            } else {
                self.output_channel_widths_hz.clone()
            };
        let output_channel_contributions = build_output_channel_contributions(
            source_frequencies_hz,
            &source_frequencies_for_interpolation,
            &source_channel_widths_for_interpolation,
            &output_frequencies_for_interpolation,
            self.interpolation,
        );
        let source_channel_output_map = build_source_channel_output_map(
            &source_frequencies_for_interpolation,
            &output_frequencies_for_interpolation,
            &output_channel_widths_for_interpolation,
        );
        let padded_output_frequencies_for_interpolation =
            build_briggs_cube_weight_output_frequencies(
                &output_frequencies_for_interpolation,
                &output_channel_widths_for_interpolation,
            );
        let padded_source_channel_output_map = build_source_channel_output_map(
            &source_frequencies_for_interpolation,
            &padded_output_frequencies_for_interpolation,
            &[],
        );
        let grid_channel_contributions = build_grid_channel_contributions(
            source_frequencies_hz,
            &source_frequencies_for_interpolation,
            &source_channel_widths_for_interpolation,
            &output_frequencies_for_interpolation,
            &output_channel_widths_for_interpolation,
            self.interpolation,
        );
        let padded_grid_channel_contributions = build_grid_channel_contributions(
            source_frequencies_hz,
            &source_frequencies_for_interpolation,
            &source_channel_widths_for_interpolation,
            &padded_output_frequencies_for_interpolation,
            &[],
            self.interpolation,
        );
        let source_channel_model_contributions = source_frequencies_for_interpolation
            .into_iter()
            .map(|source_frequency_hz| match self.interpolation {
                CubeInterpolation::Nearest => nearest_channel_index(
                    &output_frequencies_for_interpolation,
                    source_frequency_hz,
                )
                .map(|index| {
                    vec![CubeChannelContribution {
                        source_channel: index,
                        source_frequency_hz: self.output_channel_frequencies_hz[index],
                        factor: 1.0,
                    }]
                })
                .unwrap_or_default(),
                CubeInterpolation::Linear | CubeInterpolation::Cubic => {
                    linear_channel_model_contributions(
                        &self.output_channel_frequencies_hz,
                        &output_frequencies_for_interpolation,
                        &output_channel_widths_for_interpolation,
                        source_frequency_hz,
                    )
                }
            })
            .collect();
        Ok(CubeRowSpectralContributions {
            output_channel_contributions,
            source_channel_output_map,
            padded_source_channel_output_map,
            padded_grid_channel_contributions,
            grid_channel_contributions,
            source_channel_model_contributions,
        })
    }

    /// Convert selected source-channel frequencies into the frame used for
    /// row-local spectral interpolation.
    pub fn row_source_frequencies_for_interpolation(
        &self,
        source_frequencies_hz: &[f64],
        row_time_mjd_sec: f64,
        row_field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Vec<f64>> {
        if self.interpolation_uses_native_source_frequencies {
            return Ok(source_frequencies_hz.to_vec());
        }
        let (source_spectral_frame, output_spectral_frame) =
            self.row_and_output_spectral_frames(row_time_mjd_sec, row_field_id, derived_engine)?;
        source_frequencies_hz
            .iter()
            .copied()
            .map(|source_frequency_hz| {
                convert_frequency_to_frame_with_frames(
                    self.source_freq_ref,
                    self.output_freq_ref,
                    source_frequency_hz,
                    source_spectral_frame.as_ref(),
                    output_spectral_frame.as_ref(),
                )
            })
            .collect()
    }

    /// Build output-channel interpolation contributions for one row, reusing
    /// the per-row spectral frame conversion state.
    pub fn row_output_channel_contributions_batch(
        &self,
        source_frequencies_hz: &[f64],
        source_channel_widths_hz: &[f64],
        row_time_mjd_sec: f64,
        row_field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Vec<Vec<CubeChannelContribution>>> {
        self.row_spectral_contributions(
            source_frequencies_hz,
            source_channel_widths_hz,
            row_time_mjd_sec,
            row_field_id,
            derived_engine,
        )
        .map(|contributions| contributions.output_channel_contributions)
    }

    /// Build per-source-channel model interpolation contributions for one row.
    ///
    /// Each returned entry corresponds to one selected source channel and
    /// contains the output-model channels that CASA-style cube degridding would
    /// interpolate between when predicting at that source-channel frequency.
    pub fn row_source_channel_model_contributions_batch(
        &self,
        source_frequencies_hz: &[f64],
        source_channel_widths_hz: &[f64],
        row_time_mjd_sec: f64,
        row_field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Vec<Vec<CubeChannelContribution>>> {
        self.row_spectral_contributions(
            source_frequencies_hz,
            source_channel_widths_hz,
            row_time_mjd_sec,
            row_field_id,
            derived_engine,
        )
        .map(|contributions| contributions.source_channel_model_contributions)
    }

    fn row_and_output_spectral_frames(
        &self,
        row_time_mjd_sec: f64,
        row_field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<(Option<MeasFrame>, Option<MeasFrame>)> {
        if self.source_freq_ref == self.output_freq_ref {
            return Ok((None, None));
        }
        let source_spectral_frame = derived_engine
            .spectral_frame_observatory(row_time_mjd_sec, row_field_id)
            .map_err(|error| {
                MsError::VersionError(format!(
                    "build source spectral frame for field {row_field_id}: {error}"
                ))
            })?;
        let output_spectral_frame = derived_engine
            .spectral_frame_observatory(
                self.output_frame_reference_time_mjd_sec,
                self.output_frame_field_id,
            )
            .map_err(|error| {
                MsError::VersionError(format!(
                    "build output spectral frame for field {}: {error}",
                    self.output_frame_field_id
                ))
            })?;
        Ok((Some(source_spectral_frame), Some(output_spectral_frame)))
    }
}

fn uses_optical_velocity_axis(axis_config: &CubeAxisConfig) -> bool {
    axis_config.veltype == DopplerRef::Z
        && matches!(
            axis_config.start,
            Some(CubeAxisValue::VelocityMs { .. }) | Some(CubeAxisValue::Doppler { .. })
        )
        && matches!(
            axis_config.width,
            None | Some(CubeAxisValue::VelocityMs { .. }) | Some(CubeAxisValue::Doppler { .. })
        )
}

fn uses_native_source_interpolation(axis_config: &CubeAxisConfig) -> bool {
    axis_config.start.is_none()
        && matches!(
            axis_config.width,
            Some(CubeAxisValue::VelocityMs { ms, .. }) if ms < 0.0
        )
        || axis_config.start.is_none()
            && matches!(
                axis_config.width,
                Some(CubeAxisValue::Doppler { value, .. }) if value < 0.0
            )
}

fn spectral_frame_observatory(
    derived_engine: &MsCalEngine,
    time_mjd_sec: f64,
    fallback_field_id: usize,
    phase_center_direction: Option<MDirection>,
) -> MsResult<MeasFrame> {
    if let Some(direction) = phase_center_direction {
        derived_engine.spectral_frame_observatory_direction(time_mjd_sec, direction)
    } else {
        derived_engine.spectral_frame_observatory(time_mjd_sec, fallback_field_id)
    }
}

fn effective_output_frequency_ref(
    source_freq_ref: FrequencyRef,
    axis_config: &CubeAxisConfig,
) -> MsResult<FrequencyRef> {
    if matches!(axis_config.specmode, CubeSpecMode::Cubedata) {
        return Ok(source_freq_ref);
    }
    let start_frame = cube_axis_explicit_frame(axis_config.start.as_ref());
    let width_frame = cube_axis_explicit_frame(axis_config.width.as_ref());
    match (start_frame, width_frame) {
        (Some(left), Some(right)) if left != right => Err(MsError::VersionError(format!(
            "cube axis start frame {left} and width frame {right} disagree"
        ))),
        (Some(frame), _) | (_, Some(frame)) => Ok(frame),
        (None, None) => Ok(axis_config.outframe),
    }
}

fn cube_axis_explicit_frame(axis_value: Option<&CubeAxisValue>) -> Option<FrequencyRef> {
    match axis_value {
        Some(CubeAxisValue::FrequencyHz { frame, .. })
        | Some(CubeAxisValue::VelocityMs { frame, .. }) => *frame,
        Some(CubeAxisValue::Channel(_)) | Some(CubeAxisValue::Doppler { .. }) | None => None,
    }
}

fn infer_native_spacing_hz(channel_frequencies_hz: &[f64]) -> MsResult<f64> {
    channel_frequencies_hz
        .windows(2)
        .next()
        .map(|pair| pair[1] - pair[0])
        .ok_or_else(|| {
            MsError::VersionError(
                "cannot infer native spectral spacing from fewer than two channels".to_string(),
            )
        })
}

fn cube_axis_frequency_hz_from_value(
    axis_value: &CubeAxisValue,
    output_freq_ref: FrequencyRef,
    rest_frequency_hz: Option<f64>,
    veltype: DopplerRef,
    row_time_mjd_sec: f64,
    field_id: usize,
    derived_engine: &MsCalEngine,
) -> MsResult<f64> {
    match axis_value {
        CubeAxisValue::Channel(channel) => Err(MsError::VersionError(format!(
            "expected frequency-like cube axis value, found channel {channel}"
        ))),
        CubeAxisValue::FrequencyHz { hz, frame } => {
            let source_freq_ref = frame.unwrap_or(output_freq_ref);
            convert_frequency_to_frame(
                source_freq_ref,
                output_freq_ref,
                *hz,
                row_time_mjd_sec,
                field_id,
                derived_engine,
            )
        }
        CubeAxisValue::VelocityMs { ms, frame } => {
            let rest_frequency_hz = required_rest_frequency_hz(rest_frequency_hz, axis_value)?;
            let doppler = MDoppler::new(ms / SPEED_OF_LIGHT_M_S, veltype)
                .convert_to(DopplerRef::RATIO, &MeasFrame::new())
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube-axis velocity {ms} m/s to frequency ratio: {error}"
                    ))
                })?;
            let frequency_hz = rest_frequency_hz * doppler.value();
            let source_freq_ref = frame.unwrap_or(output_freq_ref);
            convert_frequency_to_frame(
                source_freq_ref,
                output_freq_ref,
                frequency_hz,
                row_time_mjd_sec,
                field_id,
                derived_engine,
            )
        }
        CubeAxisValue::Doppler { value, convention } => {
            let rest_frequency_hz = required_rest_frequency_hz(rest_frequency_hz, axis_value)?;
            let doppler = MDoppler::new(*value, *convention)
                .convert_to(DopplerRef::RATIO, &MeasFrame::new())
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube-axis doppler value {value} to frequency ratio: {error}"
                    ))
                })?;
            Ok(rest_frequency_hz * doppler.value())
        }
    }
}

fn cube_axis_delta_hz_from_value(
    axis_value: &CubeAxisValue,
    rest_frequency_hz: Option<f64>,
    veltype: DopplerRef,
) -> MsResult<f64> {
    match axis_value {
        CubeAxisValue::Channel(channel_width) => Err(MsError::VersionError(format!(
            "expected frequency-like cube increment, found channel width {channel_width}"
        ))),
        CubeAxisValue::FrequencyHz { hz, .. } => Ok(*hz),
        CubeAxisValue::VelocityMs { ms, .. } => {
            let rest_frequency_hz = required_rest_frequency_hz(rest_frequency_hz, axis_value)?;
            let doppler = MDoppler::new(ms / SPEED_OF_LIGHT_M_S, veltype)
                .convert_to(DopplerRef::RATIO, &MeasFrame::new())
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube-axis velocity width {ms} m/s to frequency ratio: {error}"
                    ))
                })?;
            Ok(rest_frequency_hz * (doppler.value() - 1.0))
        }
        CubeAxisValue::Doppler { value, convention } => {
            let rest_frequency_hz = required_rest_frequency_hz(rest_frequency_hz, axis_value)?;
            let doppler = MDoppler::new(*value, *convention)
                .convert_to(DopplerRef::RATIO, &MeasFrame::new())
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube-axis doppler width {value} to frequency ratio: {error}"
                    ))
                })?;
            Ok(rest_frequency_hz * (doppler.value() - 1.0))
        }
    }
}

fn frequency_hz_from_vopt_ms(optical_velocity_ms: f64, rest_frequency_hz: f64) -> f64 {
    rest_frequency_hz / (1.0 + optical_velocity_ms / SPEED_OF_LIGHT_M_S)
}

fn optical_velocity_ms_from_frequency_hz(frequency_hz: f64, rest_frequency_hz: f64) -> f64 {
    SPEED_OF_LIGHT_M_S * (rest_frequency_hz / frequency_hz - 1.0)
}

fn optical_velocity_ms_from_axis_value(
    axis_value: &CubeAxisValue,
    rest_frequency_hz: f64,
) -> MsResult<f64> {
    match axis_value {
        CubeAxisValue::VelocityMs { ms, .. } => Ok(*ms),
        CubeAxisValue::Doppler { value, convention } => {
            let doppler = MDoppler::new(*value, *convention)
                .convert_to(DopplerRef::RATIO, &MeasFrame::new())
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "convert cube-axis optical doppler {value} to ratio: {error}"
                    ))
                })?;
            Ok(optical_velocity_ms_from_frequency_hz(
                rest_frequency_hz * doppler.value(),
                rest_frequency_hz,
            ))
        }
        other => Err(MsError::VersionError(format!(
            "expected optical velocity-like cube axis value, found {other:?}"
        ))),
    }
}

fn optical_velocity_output_centers(
    source_frequencies_in_output_frame_hz: &[f64],
    source_channel_widths_hz: &[f64],
    nchan: usize,
    axis_config: &CubeAxisConfig,
) -> MsResult<Vec<f64>> {
    if nchan == 0 {
        return Err(MsError::VersionError(
            "cube output channel count must be positive".to_string(),
        ));
    }
    if source_frequencies_in_output_frame_hz.is_empty()
        || source_channel_widths_hz.len() != source_frequencies_in_output_frame_hz.len()
    {
        return Err(MsError::VersionError(
            "optical velocity axis requires non-empty source frequencies and matching widths"
                .to_string(),
        ));
    }
    let rest_frequency_hz =
        required_rest_frequency_hz(axis_config.rest_frequency_hz, &axis_config.start)?;
    let mut source_frequencies_hz = source_frequencies_in_output_frame_hz.to_vec();
    let mut source_widths_hz = source_channel_widths_hz.to_vec();
    let mut reverse_output = false;
    if source_frequencies_hz.len() > 1 && source_frequencies_hz[1] < source_frequencies_hz[0] {
        source_frequencies_hz.reverse();
        source_widths_hz.reverse();
        reverse_output = true;
    }

    let width_ms = match axis_config.width.as_ref() {
        Some(value @ (CubeAxisValue::VelocityMs { .. } | CubeAxisValue::Doppler { .. })) => {
            optical_velocity_ms_from_axis_value(value, rest_frequency_hz)?.abs()
        }
        Some(other) => {
            return Err(MsError::VersionError(format!(
                "optical velocity cube width expects a velocity-like value, found {other:?}"
            )));
        }
        None => {
            let first_center_hz = source_frequencies_hz[0];
            let first_width_hz = source_widths_hz[0];
            let upper_edge = optical_velocity_ms_from_frequency_hz(
                first_center_hz - first_width_hz,
                rest_frequency_hz,
            );
            let lower_edge = optical_velocity_ms_from_frequency_hz(
                first_center_hz + first_width_hz,
                rest_frequency_hz,
            );
            (upper_edge - lower_edge).abs()
        }
    };
    if !(width_ms.is_finite() && width_ms > 0.0) {
        return Err(MsError::VersionError(
            "optical velocity cube width must be positive".to_string(),
        ));
    }

    let start_is_end = match axis_config.width.as_ref() {
        Some(CubeAxisValue::VelocityMs { ms, .. }) => *ms >= 0.0,
        Some(CubeAxisValue::Doppler { value, .. }) => *value >= 0.0,
        None => true,
        Some(other) => {
            return Err(MsError::VersionError(format!(
                "optical velocity cube width expects a velocity-like value, found {other:?}"
            )));
        }
    };

    let center_is_start = axis_config.start.is_some();
    let mut regrid_center_hz;
    let regrid_center_velocity_ms;
    if let Some(start_value) = axis_config.start.as_ref() {
        let mut start_velocity_ms =
            optical_velocity_ms_from_axis_value(start_value, rest_frequency_hz)?;
        if center_is_start {
            if start_is_end {
                start_velocity_ms -= width_ms / 2.0;
            } else {
                start_velocity_ms += width_ms / 2.0;
            }
        }
        regrid_center_hz = frequency_hz_from_vopt_ms(start_velocity_ms, rest_frequency_hz);
        regrid_center_velocity_ms = start_velocity_ms;
    } else {
        regrid_center_hz = (source_frequencies_hz[0] - source_widths_hz[0]
            + source_frequencies_hz[source_frequencies_hz.len() - 1]
            + source_widths_hz[source_widths_hz.len() - 1])
            / 2.0;
        regrid_center_velocity_ms =
            optical_velocity_ms_from_frequency_hz(regrid_center_hz, rest_frequency_hz);
    }

    let regrid_bandwidth_hz = {
        let div_by_two = if center_is_start { 1.0 } else { 0.5 };
        let bw_edge_hz = if center_is_start && !start_is_end {
            frequency_hz_from_vopt_ms(
                regrid_center_velocity_ms - nchan as f64 * width_ms * div_by_two,
                rest_frequency_hz,
            )
        } else {
            frequency_hz_from_vopt_ms(
                regrid_center_velocity_ms + nchan as f64 * width_ms * div_by_two,
                rest_frequency_hz,
            )
        };
        (bw_edge_hz - regrid_center_hz).abs() / div_by_two
    };
    if center_is_start {
        let center_velocity_ms = if start_is_end {
            regrid_center_velocity_ms + nchan as f64 * width_ms / 2.0
        } else {
            regrid_center_velocity_ms - nchan as f64 * width_ms / 2.0
        };
        regrid_center_hz = frequency_hz_from_vopt_ms(center_velocity_ms, rest_frequency_hz);
    }

    let channel_upper_edge_hz = frequency_hz_from_vopt_ms(
        regrid_center_velocity_ms - width_ms / 2.0,
        rest_frequency_hz,
    );
    let regrid_channel_width_hz = 2.0
        * (channel_upper_edge_hz
            - frequency_hz_from_vopt_ms(regrid_center_velocity_ms, rest_frequency_hz));

    let lower_source_edge_hz = source_frequencies_hz[0] - source_widths_hz[0] / 2.0;
    let upper_source_edge_hz = source_frequencies_hz[source_frequencies_hz.len() - 1]
        + source_widths_hz[source_widths_hz.len() - 1] / 2.0;

    let mut the_regrid_center_hz = regrid_center_hz;
    if (the_regrid_center_hz - upper_source_edge_hz) > 1.0 {
        the_regrid_center_hz = upper_source_edge_hz;
    } else if the_regrid_center_hz < lower_source_edge_hz {
        the_regrid_center_hz = lower_source_edge_hz;
    }

    let mut the_regrid_bandwidth_hz = regrid_bandwidth_hz;
    let range_tolerance_hz = source_widths_hz[0];
    if (the_regrid_center_hz + the_regrid_bandwidth_hz / 2.0) - upper_source_edge_hz
        > range_tolerance_hz
    {
        the_regrid_bandwidth_hz = (upper_source_edge_hz - the_regrid_center_hz)
            .abs()
            .min((the_regrid_center_hz - lower_source_edge_hz).abs())
            * 2.0;
    }
    if (the_regrid_center_hz - the_regrid_bandwidth_hz / 2.0) - lower_source_edge_hz
        < -range_tolerance_hz
    {
        the_regrid_bandwidth_hz = (upper_source_edge_hz - the_regrid_center_hz)
            .abs()
            .min((the_regrid_center_hz - lower_source_edge_hz).abs())
            * 2.0;
    }

    let central_channel_width_hz = regrid_channel_width_hz;

    let mut lo_fb_up = Vec::new();
    let mut hi_fb_up = Vec::new();
    let mut lo_fb_down = Vec::new();
    let mut hi_fb_down = Vec::new();
    let edge_tolerance_hz = central_channel_width_hz.abs() * 0.01;
    let upper_end_hz = the_regrid_center_hz + the_regrid_bandwidth_hz / 2.0;
    let lower_end_hz = the_regrid_center_hz - the_regrid_bandwidth_hz / 2.0;
    let upper_end_velocity_ms =
        optical_velocity_ms_from_frequency_hz(upper_end_hz, rest_frequency_hz);
    let lower_end_velocity_ms =
        optical_velocity_ms_from_frequency_hz(lower_end_hz, rest_frequency_hz);
    let tnum_chan =
        ((the_regrid_bandwidth_hz + edge_tolerance_hz) / central_channel_width_hz).floor();
    if (tnum_chan as i64) % 2 != 0 {
        lo_fb_up.push(the_regrid_center_hz - central_channel_width_hz / 2.0);
        hi_fb_up.push(the_regrid_center_hz + central_channel_width_hz / 2.0);
        lo_fb_down.push(the_regrid_center_hz - central_channel_width_hz / 2.0);
        hi_fb_down.push(the_regrid_center_hz + central_channel_width_hz / 2.0);
    } else {
        lo_fb_up.push(the_regrid_center_hz);
        hi_fb_up.push(the_regrid_center_hz + central_channel_width_hz);
        lo_fb_down.push(the_regrid_center_hz);
        hi_fb_down.push(the_regrid_center_hz + central_channel_width_hz);
    }

    let velocity_channel_width_ms =
        optical_velocity_ms_from_frequency_hz(lo_fb_up[0], rest_frequency_hz)
            - optical_velocity_ms_from_frequency_hz(hi_fb_up[0], rest_frequency_hz);
    let mut lower_velocity_ms =
        optical_velocity_ms_from_frequency_hz(*hi_fb_up.last().unwrap(), rest_frequency_hz);
    let mut upper_velocity_ms = lower_velocity_ms - velocity_channel_width_ms;
    while upper_end_velocity_ms - upper_velocity_ms < velocity_channel_width_ms / 10.0 {
        let upper_frequency_hz = frequency_hz_from_vopt_ms(upper_velocity_ms, rest_frequency_hz);
        if upper_frequency_hz <= upper_end_hz + edge_tolerance_hz {
            lo_fb_up.push(*hi_fb_up.last().unwrap());
            hi_fb_up.push(upper_frequency_hz);
        } else if upper_frequency_hz < upper_end_hz + edge_tolerance_hz {
            lo_fb_up.push(*hi_fb_up.last().unwrap());
            hi_fb_up.push(upper_end_hz);
            break;
        } else {
            break;
        }
        lower_velocity_ms =
            optical_velocity_ms_from_frequency_hz(*hi_fb_up.last().unwrap(), rest_frequency_hz);
        upper_velocity_ms = lower_velocity_ms - velocity_channel_width_ms;
    }

    let mut upper_velocity_ms =
        optical_velocity_ms_from_frequency_hz(*lo_fb_down.last().unwrap(), rest_frequency_hz);
    let mut lower_velocity_ms = upper_velocity_ms + velocity_channel_width_ms;
    while lower_velocity_ms - lower_end_velocity_ms < velocity_channel_width_ms / 10.0 {
        let lower_frequency_hz = frequency_hz_from_vopt_ms(lower_velocity_ms, rest_frequency_hz);
        if lower_frequency_hz >= lower_end_hz - edge_tolerance_hz {
            hi_fb_down.push(*lo_fb_down.last().unwrap());
            lo_fb_down.push(lower_frequency_hz);
        } else if lower_frequency_hz > lower_end_hz - edge_tolerance_hz {
            hi_fb_down.push(*lo_fb_down.last().unwrap());
            lo_fb_down.push(lower_end_hz);
            break;
        } else {
            break;
        }
        upper_velocity_ms =
            optical_velocity_ms_from_frequency_hz(*lo_fb_down.last().unwrap(), rest_frequency_hz);
        lower_velocity_ms = upper_velocity_ms + velocity_channel_width_ms;
    }

    let mut centers = Vec::with_capacity(lo_fb_down.len() + lo_fb_up.len());
    for index in (0..lo_fb_down.len()).rev() {
        centers.push((lo_fb_down[index] + hi_fb_down[index]) / 2.0);
    }
    centers.extend(
        lo_fb_up
            .iter()
            .zip(hi_fb_up.iter())
            .enumerate()
            .filter(|(index, _)| *index > 0)
            .map(|(_, (low_hz, high_hz))| (low_hz + high_hz) / 2.0),
    );
    if reverse_output {
        centers.reverse();
    }
    Ok(centers)
}

fn required_rest_frequency_hz(
    rest_frequency_hz: Option<f64>,
    value: &impl std::fmt::Debug,
) -> MsResult<f64> {
    rest_frequency_hz.ok_or_else(|| {
        MsError::VersionError(format!(
            "cube axis value {value:?} requires a rest frequency"
        ))
    })
}

fn default_generic_axis_start_hz(
    source_frequencies_in_output_frame: &[f64],
    delta_hz: f64,
    nchan: usize,
) -> MsResult<f64> {
    let edges = channel_edges_from_centers(source_frequencies_in_output_frame)?;
    let low_edge_hz = edges.iter().copied().reduce(f64::min).ok_or_else(|| {
        MsError::VersionError(
            "cannot choose a cube-axis start from an empty spectral window".to_string(),
        )
    })?;
    if nchan == 0 {
        return Err(MsError::VersionError(
            "cube output channel count must be positive".to_string(),
        ));
    }
    let first_center_hz = if delta_hz >= 0.0 {
        low_edge_hz + 0.5 * delta_hz
    } else {
        low_edge_hz - (nchan as f64 - 0.5) * delta_hz
    };
    Ok(first_center_hz)
}

fn snap_default_frequency_like_start_to_source_grid(
    axis_config: &CubeAxisConfig,
    source_frequencies_in_output_frame: &[f64],
    start_frequency_hz: f64,
) -> f64 {
    if axis_config.width.is_some() {
        return start_frequency_hz;
    }
    if matches!(axis_config.start, None | Some(CubeAxisValue::Channel(_))) {
        return start_frequency_hz;
    }
    nearest_channel_index(source_frequencies_in_output_frame, start_frequency_hz)
        .map(|index| source_frequencies_in_output_frame[index])
        .unwrap_or(start_frequency_hz)
}

/// Convert one scalar frequency between spectral frames using the MeasurementSet
/// observatory frame for the given row time and field.
pub fn convert_frequency_to_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    row_time_mjd_sec: f64,
    field_id: usize,
    derived_engine: &MsCalEngine,
) -> MsResult<f64> {
    if source_freq_ref == target_freq_ref {
        return Ok(frequency_hz);
    }
    let frame = derived_engine
        .spectral_frame_observatory(row_time_mjd_sec, field_id)
        .map_err(|error| {
            MsError::VersionError(format!(
                "build spectral frame for field {field_id}: {error}"
            ))
        })?;
    convert_frequency_to_frame_with_frame(
        source_freq_ref,
        target_freq_ref,
        frequency_hz,
        Some(&frame),
    )
}

/// Convert one scalar frequency between spectral frames using a caller-provided
/// measures frame.
///
/// If the source and target references are identical, the input frequency is
/// returned unchanged and the frame is ignored.
pub fn convert_frequency_to_frame_with_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    frame: Option<&MeasFrame>,
) -> MsResult<f64> {
    convert_frequency_to_frame_with_frames(
        source_freq_ref,
        target_freq_ref,
        frequency_hz,
        frame,
        frame,
    )
}

fn convert_frequency_to_frame_with_frames(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    source_frame: Option<&MeasFrame>,
    target_frame: Option<&MeasFrame>,
) -> MsResult<f64> {
    if source_freq_ref == target_freq_ref {
        return Ok(frequency_hz);
    }
    let source_frame = source_frame.ok_or_else(|| {
        MsError::VersionError(format!(
            "source frame required for cross-frame frequency conversion from {source_freq_ref} to {target_freq_ref}"
        ))
    })?;
    let target_frame = target_frame.ok_or_else(|| {
        MsError::VersionError(format!(
            "target frame required for cross-frame frequency conversion from {source_freq_ref} to {target_freq_ref}"
        ))
    })?;
    let mut current_frequency_hz = frequency_hz;
    let mut current_ref = source_freq_ref;
    for next_ref in direct_frequency_hop_path(source_freq_ref, target_freq_ref)? {
        let hop_frame = if direct_frequency_hop_uses_target_frame(current_ref, next_ref) {
            target_frame
        } else {
            source_frame
        };
        current_frequency_hz = MFrequency::new(current_frequency_hz, current_ref)
            .convert_to(next_ref, hop_frame)
            .map(|frequency| frequency.hz())
            .map_err(|error| {
                MsError::VersionError(format!(
                    "convert frequency {current_frequency_hz} Hz from {current_ref} to {next_ref}: {error}"
                ))
            })?;
        current_ref = next_ref;
    }
    Ok(current_frequency_hz)
}

/// Convert one scalar frequency into a doppler velocity in m/s using the given
/// rest frequency and velocity definition.
pub fn velocity_ms_from_frequency_hz(
    frequency_hz: f64,
    rest_frequency_hz: f64,
    doppler_ref: DopplerRef,
) -> MsResult<f64> {
    MDoppler::new(frequency_hz / rest_frequency_hz, DopplerRef::RATIO)
        .convert_to(doppler_ref, &MeasFrame::new())
        .map(|doppler| doppler.value() * SPEED_OF_LIGHT_M_S)
        .map_err(|error| {
            MsError::VersionError(format!(
                "convert frequency {frequency_hz} Hz to {doppler_ref} velocity: {error}"
            ))
        })
}

fn convert_channel_widths_to_frame_with_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frame: &MeasFrame,
) -> MsResult<Vec<f64>> {
    convert_channel_widths_to_frame_with_frames(
        source_freq_ref,
        target_freq_ref,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frame,
        frame,
    )
}

fn convert_channel_widths_to_frame_with_frames(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    source_frame: &MeasFrame,
    target_frame: &MeasFrame,
) -> MsResult<Vec<f64>> {
    if source_channel_frequencies_hz.len() != source_channel_widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "frequency/width arrays must match for frame conversion, got {} and {}",
            source_channel_frequencies_hz.len(),
            source_channel_widths_hz.len()
        )));
    }
    if source_freq_ref == target_freq_ref {
        return Ok(source_channel_widths_hz
            .iter()
            .map(|width_hz| width_hz.abs())
            .collect());
    }
    source_channel_frequencies_hz
        .iter()
        .copied()
        .zip(source_channel_widths_hz.iter().copied())
        .map(|(frequency_hz, width_hz)| {
            let half_width_hz = width_hz.abs() / 2.0;
            let upper_hz = convert_frequency_to_frame_with_frames(
                source_freq_ref,
                target_freq_ref,
                frequency_hz + half_width_hz,
                Some(source_frame),
                Some(target_frame),
            )?;
            let lower_hz = convert_frequency_to_frame_with_frames(
                source_freq_ref,
                target_freq_ref,
                frequency_hz - half_width_hz,
                Some(source_frame),
                Some(target_frame),
            )?;
            Ok((upper_hz - lower_hz).abs())
        })
        .collect()
}

const DIRECT_FREQUENCY_HOP_EDGES: &[(FrequencyRef, FrequencyRef)] = &[
    (FrequencyRef::REST, FrequencyRef::LSRK),
    (FrequencyRef::LSRK, FrequencyRef::BARY),
    (FrequencyRef::LSRD, FrequencyRef::BARY),
    (FrequencyRef::LSRD, FrequencyRef::GALACTO),
    (FrequencyRef::BARY, FrequencyRef::LGROUP),
    (FrequencyRef::BARY, FrequencyRef::CMB),
    (FrequencyRef::BARY, FrequencyRef::GEO),
    (FrequencyRef::GEO, FrequencyRef::TOPO),
];

fn direct_frequency_hop_path(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
) -> MsResult<Vec<FrequencyRef>> {
    use std::collections::{HashMap, HashSet, VecDeque};

    if source_freq_ref == target_freq_ref {
        return Ok(Vec::new());
    }

    let mut queue = VecDeque::from([source_freq_ref]);
    let mut visited = HashSet::from([source_freq_ref]);
    let mut parent = HashMap::<FrequencyRef, FrequencyRef>::new();

    while let Some(current) = queue.pop_front() {
        for &(left, right) in DIRECT_FREQUENCY_HOP_EDGES {
            let next = if left == current {
                right
            } else if right == current {
                left
            } else {
                continue;
            };
            if !visited.insert(next) {
                continue;
            }
            parent.insert(next, current);
            if next == target_freq_ref {
                let mut path = Vec::new();
                let mut cursor = target_freq_ref;
                while cursor != source_freq_ref {
                    path.push(cursor);
                    cursor = *parent
                        .get(&cursor)
                        .expect("BFS parent must exist for visited node");
                }
                path.reverse();
                return Ok(path);
            }
            queue.push_back(next);
        }
    }

    Err(MsError::VersionError(format!(
        "no direct-hop route found from {source_freq_ref} to {target_freq_ref}"
    )))
}

fn direct_frequency_hop_uses_target_frame(from: FrequencyRef, to: FrequencyRef) -> bool {
    matches!(
        (from, to),
        (FrequencyRef::LSRK, FrequencyRef::REST)
            | (FrequencyRef::BARY, FrequencyRef::LSRK)
            | (FrequencyRef::BARY, FrequencyRef::LSRD)
            | (FrequencyRef::LSRD, FrequencyRef::GALACTO)
            | (FrequencyRef::BARY, FrequencyRef::LGROUP)
            | (FrequencyRef::BARY, FrequencyRef::CMB)
            | (FrequencyRef::BARY, FrequencyRef::GEO)
            | (FrequencyRef::GEO, FrequencyRef::TOPO)
    )
}

fn nearest_channel_index(channel_frequencies_hz: &[f64], frequency_hz: f64) -> Option<usize> {
    let (best_index, best_diff) = channel_frequencies_hz
        .iter()
        .enumerate()
        .map(|(index, channel_frequency_hz)| (index, (channel_frequency_hz - frequency_hz).abs()))
        .min_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    let max_allowed_diff = if channel_frequencies_hz.len() <= 1 {
        f64::INFINITY
    } else {
        let lower_spacing = best_index
            .checked_sub(1)
            .map(|index| (channel_frequencies_hz[best_index] - channel_frequencies_hz[index]).abs())
            .unwrap_or(f64::INFINITY);
        let upper_spacing = channel_frequencies_hz
            .get(best_index + 1)
            .map(|frequency| (frequency - channel_frequencies_hz[best_index]).abs())
            .unwrap_or(f64::INFINITY);
        0.5 * lower_spacing.min(upper_spacing)
    };
    if best_diff <= max_allowed_diff {
        Some(best_index)
    } else {
        None
    }
}

fn spectral_interpolation_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    match interpolation {
        CubeInterpolation::Nearest => {
            nearest_channel_index(source_channel_frequencies_hz, frequency_hz)
                .map(|index| {
                    vec![CubeChannelContribution {
                        source_channel: index,
                        source_frequency_hz: native_source_channel_frequencies_hz[index],
                        factor: 1.0,
                    }]
                })
                .unwrap_or_default()
        }
        CubeInterpolation::Linear | CubeInterpolation::Cubic => linear_channel_contributions(
            native_source_channel_frequencies_hz,
            source_channel_frequencies_hz,
            source_channel_widths_hz,
            frequency_hz,
        ),
    }
}

fn build_output_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_frequencies_for_interpolation: &[f64],
    source_channel_widths_for_interpolation: &[f64],
    output_frequencies_for_interpolation: &[f64],
    interpolation: CubeInterpolation,
) -> Vec<Vec<CubeChannelContribution>> {
    output_frequencies_for_interpolation
        .iter()
        .copied()
        .map(|output_frequency_hz| {
            spectral_interpolation_contributions(
                native_source_channel_frequencies_hz,
                source_frequencies_for_interpolation,
                source_channel_widths_for_interpolation,
                interpolation,
                output_frequency_hz,
            )
        })
        .collect()
}

fn build_source_channel_output_map(
    source_channel_frequencies_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
) -> Vec<Option<usize>> {
    if output_channel_frequencies_hz.is_empty() {
        return source_channel_frequencies_hz.iter().map(|_| None).collect();
    }
    if output_channel_frequencies_hz.len() == 1 {
        let center_hz = output_channel_frequencies_hz[0];
        let half_width_hz = output_channel_widths_hz
            .first()
            .copied()
            .map(|width| 0.5 * width.abs())
            .unwrap_or(f64::INFINITY);
        return source_channel_frequencies_hz
            .iter()
            .map(|frequency_hz| {
                (frequency_hz.is_finite()
                    && center_hz.is_finite()
                    && (*frequency_hz - center_hz).abs() <= half_width_hz)
                    .then_some(0)
            })
            .collect();
    }
    let first_hz = output_channel_frequencies_hz[0];
    let increment_hz = output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0];
    if !(first_hz.is_finite() && increment_hz.is_finite() && increment_hz != 0.0) {
        return source_channel_frequencies_hz.iter().map(|_| None).collect();
    }
    let nchan = output_channel_frequencies_hz.len() as isize;
    source_channel_frequencies_hz
        .iter()
        .copied()
        .map(|frequency_hz| {
            if !frequency_hz.is_finite() {
                return None;
            }
            let pixel = ((frequency_hz - first_hz) / increment_hz).round() as isize;
            (pixel >= 0 && pixel < nchan).then_some(pixel as usize)
        })
        .collect()
}

fn build_briggs_cube_weight_output_frequencies(
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
) -> Vec<f64> {
    if output_channel_frequencies_hz.is_empty() {
        return Vec::new();
    }
    let increment_hz = if output_channel_frequencies_hz.len() >= 2 {
        output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0]
    } else {
        output_channel_widths_hz.first().copied().unwrap_or(0.0)
    };
    if !(increment_hz.is_finite() && increment_hz != 0.0) {
        return output_channel_frequencies_hz.to_vec();
    }
    let mut padded_frequencies_hz = Vec::with_capacity(output_channel_frequencies_hz.len() + 4);
    padded_frequencies_hz.push(output_channel_frequencies_hz[0] - 2.0 * increment_hz);
    padded_frequencies_hz.push(output_channel_frequencies_hz[0] - increment_hz);
    padded_frequencies_hz.extend_from_slice(output_channel_frequencies_hz);
    padded_frequencies_hz.push(
        output_channel_frequencies_hz[output_channel_frequencies_hz.len() - 1] + increment_hz,
    );
    padded_frequencies_hz.push(
        output_channel_frequencies_hz[output_channel_frequencies_hz.len() - 1] + 2.0 * increment_hz,
    );
    padded_frequencies_hz
}

fn build_grid_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
) -> Vec<CubeGridChannelContributions> {
    let (grid_frequencies_hz, channel_map) = casa_grid_frequency_channel_map(
        source_channel_frequencies_hz,
        output_channel_frequencies_hz,
        output_channel_widths_hz,
        interpolation,
    );
    if interpolation == CubeInterpolation::Nearest
        && grid_frequencies_hz.len() == source_channel_frequencies_hz.len()
    {
        return grid_frequencies_hz
            .into_iter()
            .zip(channel_map)
            .enumerate()
            .filter_map(|(source_channel, (grid_frequency_hz, output_channel))| {
                let output_channel = output_channel?;
                let source_frequency_hz = native_source_channel_frequencies_hz
                    .get(source_channel)
                    .copied()?;
                Some(CubeGridChannelContributions {
                    output_channel,
                    grid_frequency_hz,
                    contributions: vec![CubeChannelContribution {
                        source_channel,
                        source_frequency_hz,
                        factor: 1.0,
                    }],
                })
            })
            .collect();
    }
    grid_frequencies_hz
        .into_iter()
        .zip(channel_map)
        .filter_map(|(grid_frequency_hz, output_channel)| {
            let output_channel = output_channel?;
            let contributions = spectral_interpolation_contributions(
                native_source_channel_frequencies_hz,
                source_channel_frequencies_hz,
                source_channel_widths_hz,
                interpolation,
                grid_frequency_hz,
            );
            (!contributions.is_empty()).then_some(CubeGridChannelContributions {
                output_channel,
                grid_frequency_hz,
                contributions,
            })
        })
        .collect()
}

fn casa_grid_frequency_channel_map(
    source_channel_frequencies_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
) -> (Vec<f64>, Vec<Option<usize>>) {
    if output_channel_frequencies_hz.is_empty() {
        return (Vec::new(), Vec::new());
    }
    if output_channel_frequencies_hz.len() == 1
        || interpolation == CubeInterpolation::Nearest
        || source_channel_frequencies_hz.len() == 1
    {
        let channel_map = build_source_channel_output_map(
            source_channel_frequencies_hz,
            output_channel_frequencies_hz,
            output_channel_widths_hz,
        );
        return (source_channel_frequencies_hz.to_vec(), channel_map);
    }

    let image_increment_hz = output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0];
    let source_increment_hz = source_channel_frequencies_hz[1] - source_channel_frequencies_hz[0];
    if !(image_increment_hz.is_finite()
        && source_increment_hz.is_finite()
        && image_increment_hz != 0.0
        && source_increment_hz != 0.0)
    {
        return (
            output_channel_frequencies_hz.to_vec(),
            (0..output_channel_frequencies_hz.len()).map(Some).collect(),
        );
    }

    let width = image_increment_hz.abs() / source_increment_hz.abs();
    let direct_width_limit = match interpolation {
        CubeInterpolation::Linear => 2.0,
        CubeInterpolation::Cubic => 4.0,
        CubeInterpolation::Nearest => 1.0,
    };
    if width <= direct_width_limit {
        return (
            output_channel_frequencies_hz.to_vec(),
            (0..output_channel_frequencies_hz.len()).map(Some).collect(),
        );
    }

    let min_vis_hz = source_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max_vis_hz = source_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let mut min_image_hz = output_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let mut max_image_hz = output_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let image_width_hz = image_increment_hz.abs();
    if !((min_image_hz - image_width_hz / 2.0) <= max_vis_hz
        && (max_image_hz + image_width_hz / 2.0) >= min_vis_hz)
    {
        return (
            output_channel_frequencies_hz.to_vec(),
            output_channel_frequencies_hz.iter().map(|_| None).collect(),
        );
    }

    if min_image_hz < min_vis_hz {
        if let Some(value) =
            nearest_bracketed_output_frequency(output_channel_frequencies_hz, min_vis_hz)
        {
            min_image_hz = value;
        }
    }
    if max_image_hz > max_vis_hz {
        if let Some(value) =
            nearest_bracketed_output_frequency(output_channel_frequencies_hz, max_vis_hz)
        {
            max_image_hz = value;
        }
    }

    let interp_width_hz = (image_width_hz / width.floor()).copysign(source_increment_hz);
    let ninterp = (((max_image_hz - min_image_hz).abs() + image_width_hz) / interp_width_hz.abs())
        .ceil() as usize
        + 2;
    let mut first = if interp_width_hz > 0.0 {
        min_image_hz
    } else {
        max_image_hz
    };
    if interpolation == CubeInterpolation::Linear {
        first -= interp_width_hz;
    } else if interpolation == CubeInterpolation::Cubic {
        first -= 2.0 * interp_width_hz;
    }
    let start_edge_hz = image_width_hz / 2.0 - interp_width_hz.abs() / 2.0;
    if interp_width_hz > 0.0 {
        first -= start_edge_hz;
    } else {
        first += start_edge_hz;
    }
    let grid_frequencies_hz = (0..ninterp)
        .map(|index| first + index as f64 * interp_width_hz)
        .collect::<Vec<_>>();
    let half_image_width_hz = image_width_hz / 2.0;
    let channel_map = grid_frequencies_hz
        .iter()
        .copied()
        .map(|frequency_hz| {
            output_channel_frequencies_hz.iter().position(|output_hz| {
                frequency_hz >= output_hz - half_image_width_hz
                    && frequency_hz < output_hz + half_image_width_hz
            })
        })
        .collect();
    (grid_frequencies_hz, channel_map)
}

fn nearest_bracketed_output_frequency(
    output_channel_frequencies_hz: &[f64],
    frequency_hz: f64,
) -> Option<f64> {
    output_channel_frequencies_hz
        .iter()
        .copied()
        .min_by(|left, right| {
            (left - frequency_hz)
                .abs()
                .partial_cmp(&(right - frequency_hz).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn linear_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    linear_channel_contributions_impl(
        native_source_channel_frequencies_hz,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frequency_hz,
        false,
    )
}

fn linear_channel_model_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    linear_channel_contributions_impl(
        native_source_channel_frequencies_hz,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frequency_hz,
        true,
    )
}

fn linear_channel_contributions_impl(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
    clip_to_edge_channel: bool,
) -> Vec<CubeChannelContribution> {
    let nchan = source_channel_frequencies_hz.len();
    if nchan == 0 {
        return Vec::new();
    }
    if native_source_channel_frequencies_hz.len() != nchan {
        return Vec::new();
    }
    if source_channel_widths_hz.len() != nchan {
        return Vec::new();
    }
    if nchan == 1 {
        let low_edge_hz =
            source_channel_frequencies_hz[0] - source_channel_widths_hz[0].abs() / 2.0;
        let high_edge_hz =
            source_channel_frequencies_hz[0] + source_channel_widths_hz[0].abs() / 2.0;
        if frequency_hz < low_edge_hz || frequency_hz > high_edge_hz {
            return Vec::new();
        }
        return Some(0)
            .map(|index| {
                vec![CubeChannelContribution {
                    source_channel: index,
                    source_frequency_hz: native_source_channel_frequencies_hz[index],
                    factor: 1.0,
                }]
            })
            .unwrap_or_default();
    }

    let min_spacing_hz = source_channel_frequencies_hz
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).abs())
        .filter(|spacing| spacing.is_finite() && *spacing > 0.0)
        .fold(f64::INFINITY, f64::min);
    let center_tolerance_hz = if min_spacing_hz.is_finite() {
        min_spacing_hz * 1.0e-12
    } else {
        f64::EPSILON
    };

    if let Some(index) = source_channel_frequencies_hz
        .iter()
        .position(|center| (*center - frequency_hz).abs() <= center_tolerance_hz)
    {
        return vec![CubeChannelContribution {
            source_channel: index,
            source_frequency_hz: native_source_channel_frequencies_hz[index],
            factor: 1.0,
        }];
    }

    let ascending = source_channel_frequencies_hz[nchan - 1] >= source_channel_frequencies_hz[0];
    let first_center_hz = source_channel_frequencies_hz[0];
    let last_index = nchan - 1;
    let last_center_hz = source_channel_frequencies_hz[last_index];
    if clip_to_edge_channel {
        if ascending {
            let first_outer_edge_hz = first_center_hz - source_channel_widths_hz[0].abs() / 2.0;
            let last_outer_edge_hz =
                last_center_hz + source_channel_widths_hz[last_index].abs() / 2.0;
            if first_outer_edge_hz <= frequency_hz && frequency_hz < first_center_hz {
                return vec![CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: native_source_channel_frequencies_hz[0],
                    factor: 1.0,
                }];
            }
            if last_center_hz < frequency_hz && frequency_hz <= last_outer_edge_hz {
                return vec![CubeChannelContribution {
                    source_channel: last_index,
                    source_frequency_hz: native_source_channel_frequencies_hz[last_index],
                    factor: 1.0,
                }];
            }
        } else {
            let first_outer_edge_hz = first_center_hz + source_channel_widths_hz[0].abs() / 2.0;
            let last_outer_edge_hz =
                last_center_hz - source_channel_widths_hz[last_index].abs() / 2.0;
            if first_center_hz < frequency_hz && frequency_hz <= first_outer_edge_hz {
                return vec![CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: native_source_channel_frequencies_hz[0],
                    factor: 1.0,
                }];
            }
            if last_outer_edge_hz <= frequency_hz && frequency_hz < last_center_hz {
                return vec![CubeChannelContribution {
                    source_channel: last_index,
                    source_frequency_hz: native_source_channel_frequencies_hz[last_index],
                    factor: 1.0,
                }];
            }
        }
    }

    let interval = if ascending {
        source_channel_frequencies_hz
            .windows(2)
            .position(|pair| pair[0] <= frequency_hz && frequency_hz <= pair[1])
    } else {
        source_channel_frequencies_hz
            .windows(2)
            .position(|pair| pair[1] <= frequency_hz && frequency_hz <= pair[0])
    };
    let Some(lower_index) = interval else {
        return Vec::new();
    };

    let upper_index = lower_index + 1;
    let lower_freq = source_channel_frequencies_hz[lower_index];
    let upper_freq = source_channel_frequencies_hz[upper_index];
    let span = upper_freq - lower_freq;
    if !span.is_finite() || span == 0.0 {
        return vec![CubeChannelContribution {
            source_channel: lower_index,
            source_frequency_hz: native_source_channel_frequencies_hz[lower_index],
            factor: 1.0,
        }];
    }

    let upper_factor = ((frequency_hz - lower_freq) / span) as f32;
    let lower_factor = 1.0 - upper_factor;
    let mut contributions = Vec::with_capacity(2);
    if lower_factor > 0.0 {
        contributions.push(CubeChannelContribution {
            source_channel: lower_index,
            source_frequency_hz: native_source_channel_frequencies_hz[lower_index],
            factor: lower_factor,
        });
    }
    if upper_factor > 0.0 {
        contributions.push(CubeChannelContribution {
            source_channel: upper_index,
            source_frequency_hz: native_source_channel_frequencies_hz[upper_index],
            factor: upper_factor,
        });
    }
    contributions
}

fn cube_source_channel_support(
    all_source_frequencies_hz: &[f64],
    all_source_channel_widths_hz: &[f64],
    cube_setup: &CubeSpectralSetup,
    time_bounds_mjd_sec: [f64; 2],
    field_id: usize,
    derived_engine: &MsCalEngine,
) -> MsResult<ResolvedChannelSelection> {
    let mut first_index = None::<usize>;
    let mut last_index = None::<usize>;
    for row_time_mjd_sec in [time_bounds_mjd_sec[0], time_bounds_mjd_sec[1]] {
        let contributions = cube_setup.row_spectral_contributions(
            all_source_frequencies_hz,
            all_source_channel_widths_hz,
            row_time_mjd_sec,
            field_id,
            derived_engine,
        )?;
        for source_index in contributions
            .output_channel_contributions
            .into_iter()
            .flatten()
            .chain(
                contributions
                    .grid_channel_contributions
                    .into_iter()
                    .flat_map(|grid| grid.contributions),
            )
            .chain(
                contributions
                    .padded_grid_channel_contributions
                    .into_iter()
                    .flat_map(|grid| grid.contributions),
            )
            .map(|contribution| contribution.source_channel)
        {
            first_index =
                Some(first_index.map_or(source_index, |current| current.min(source_index)));
            last_index = Some(last_index.map_or(source_index, |current| current.max(source_index)));
        }
    }
    let mut first_index = first_index.ok_or_else(|| {
        MsError::VersionError(
            "cube channel selection produced no supporting source channels".to_string(),
        )
    })?;
    let mut last_index = last_index.expect("first_index implies last_index");
    first_index = first_index.saturating_sub(1);
    last_index = (last_index + 2).min(all_source_frequencies_hz.len().saturating_sub(1));
    Ok(ResolvedChannelSelection {
        indices: (first_index..=last_index).collect(),
        frequencies_hz: all_source_frequencies_hz[first_index..=last_index].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> MsCalEngine {
        let observatory = casa_types::measures::position::MPosition::new_itrf(
            -1_601_185.4,
            -5_041_977.5,
            3_554_875.9,
        );
        let direction = casa_types::measures::direction::MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_4,
            casa_types::measures::direction::DirectionRef::J2000,
        );
        MsCalEngine::from_parts(
            vec![observatory.clone()],
            vec![direction],
            observatory,
            casa_test_support::deterministic_measures_provider(),
        )
    }

    #[test]
    fn parse_cube_axis_value_channel() {
        assert_eq!(
            CubeAxisValue::parse("5", DopplerRef::RADIO).unwrap(),
            CubeAxisValue::Channel(5)
        );
    }

    #[test]
    fn parse_cube_axis_value_frequency() {
        assert_eq!(
            CubeAxisValue::parse("1.1GHz", DopplerRef::RADIO).unwrap(),
            CubeAxisValue::FrequencyHz {
                hz: 1.1e9,
                frame: None,
            }
        );
    }

    #[test]
    fn parse_cube_axis_value_velocity() {
        assert_eq!(
            CubeAxisValue::parse("11991.7km/s", DopplerRef::RADIO).unwrap(),
            CubeAxisValue::VelocityMs {
                ms: 11_991_700.0,
                frame: None,
            }
        );
    }

    #[test]
    fn parse_cube_axis_value_dimensionless_doppler_and_empty_input() {
        assert_eq!(
            CubeAxisValue::parse("0.25", DopplerRef::RADIO).unwrap(),
            CubeAxisValue::Doppler {
                value: 0.25,
                convention: DopplerRef::RADIO,
            }
        );
        assert!(CubeAxisValue::parse("", DopplerRef::RADIO).is_err());
    }

    #[test]
    fn channel_edges_and_inferred_spacing_cover_boundary_cases() {
        assert!(channel_edges_from_centers(&[]).is_err());
        assert_eq!(channel_edges_from_centers(&[5.0]).unwrap(), vec![4.5, 5.5]);
        assert_eq!(
            channel_edges_from_centers(&[10.0, 20.0, 30.0]).unwrap(),
            vec![5.0, 15.0, 25.0, 35.0]
        );
        assert_eq!(infer_native_spacing_hz(&[10.0, 15.0, 25.0]).unwrap(), 5.0);
        assert!(infer_native_spacing_hz(&[10.0]).is_err());
    }

    #[test]
    fn axis_mode_helpers_cover_native_interpolation_and_explicit_frames() {
        let mut axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::TOPO,
            veltype: DopplerRef::Z,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 1.0,
                frame: Some(FrequencyRef::BARY),
            }),
            width: Some(CubeAxisValue::VelocityMs {
                ms: -2.0,
                frame: Some(FrequencyRef::BARY),
            }),
        };
        assert!(uses_optical_velocity_axis(&axis_config));
        let native_axis_config = CubeAxisConfig {
            start: None,
            width: Some(CubeAxisValue::VelocityMs {
                ms: -2.0,
                frame: None,
            }),
            ..axis_config.clone()
        };
        assert!(uses_native_source_interpolation(&native_axis_config));
        assert_eq!(
            effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
            FrequencyRef::BARY
        );
        assert_eq!(
            cube_axis_explicit_frame(axis_config.start.as_ref()),
            Some(FrequencyRef::BARY)
        );
        axis_config.width = Some(CubeAxisValue::FrequencyHz {
            hz: 1.0,
            frame: Some(FrequencyRef::LSRK),
        });
        let error = effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap_err();
        assert!(error.to_string().contains("disagree"));
        axis_config.specmode = CubeSpecMode::Cubedata;
        assert_eq!(
            effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
            FrequencyRef::TOPO
        );
    }

    #[test]
    fn frequency_and_velocity_helpers_cover_error_and_identity_paths() {
        assert!(matches!(
            cube_axis_delta_hz_from_value(&CubeAxisValue::Channel(1), Some(1.25e9), DopplerRef::RADIO),
            Err(MsError::VersionError(message)) if message.contains("channel width")
        ));
        assert_eq!(
            cube_axis_delta_hz_from_value(
                &CubeAxisValue::FrequencyHz {
                    hz: 25.0,
                    frame: None,
                },
                None,
                DopplerRef::RADIO
            )
            .unwrap(),
            25.0
        );
        assert_eq!(
            optical_velocity_ms_from_axis_value(
                &CubeAxisValue::VelocityMs {
                    ms: 12.5,
                    frame: None,
                },
                1.25e9
            )
            .unwrap(),
            12.5
        );
        assert!(matches!(
            optical_velocity_ms_from_axis_value(&CubeAxisValue::Channel(2), 1.25e9),
            Err(MsError::VersionError(message)) if message.contains("optical velocity-like")
        ));
        assert!(frequency_hz_from_vopt_ms(0.0, 1.25e9) - 1.25e9 < 1e-6);
        assert!((optical_velocity_ms_from_frequency_hz(1.25e9, 1.25e9)).abs() < 1e-6);
    }

    #[test]
    fn output_selection_helpers_cover_nearest_and_interpolation_cases() {
        assert_eq!(nearest_channel_index(&[], 1.0), None);
        assert_eq!(nearest_channel_index(&[10.0], 12.0), Some(0));
        assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 19.0), Some(1));
        assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 100.0), None);

        let nearest = spectral_interpolation_contributions(
            &[10.0, 20.0, 30.0],
            &[10.0, 20.0, 30.0],
            &[10.0, 10.0, 10.0],
            CubeInterpolation::Nearest,
            19.0,
        );
        assert_eq!(
            nearest,
            vec![CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 20.0,
                factor: 1.0,
            }]
        );

        let linear = spectral_interpolation_contributions(
            &[10.0, 20.0, 30.0],
            &[10.0, 20.0, 30.0],
            &[10.0, 10.0, 10.0],
            CubeInterpolation::Linear,
            15.0,
        );
        assert_eq!(linear.len(), 2);

        let single = linear_channel_contributions(&[10.0], &[10.0], &[4.0], 11.0);
        assert_eq!(single.len(), 1);
    }

    #[test]
    fn casa_grid_frequency_channel_map_uses_casa_intermediate_grid_thresholds() {
        let source = (0..12).map(|index| index as f64 * 10.0).collect::<Vec<_>>();
        let linear_direct = vec![5.0, 20.0, 35.0];
        let (grid, channel_map) = casa_grid_frequency_channel_map(
            &source,
            &linear_direct,
            &[],
            CubeInterpolation::Linear,
        );
        assert_eq!(grid, linear_direct);
        assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);

        let linear_threshold = vec![0.0, 20.0, 40.0];
        let (grid, channel_map) = casa_grid_frequency_channel_map(
            &source,
            &linear_threshold,
            &[],
            CubeInterpolation::Linear,
        );
        assert_eq!(grid, linear_threshold);
        assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);

        let linear_wide = vec![0.0, 25.0, 50.0];
        let (grid, channel_map) =
            casa_grid_frequency_channel_map(&source, &linear_wide, &[], CubeInterpolation::Linear);
        assert_ne!(grid, linear_wide);
        assert!(grid.len() > linear_wide.len());
        assert!(channel_map.iter().any(Option::is_some));

        let cubic_direct = vec![0.0, 40.0, 80.0];
        let (grid, channel_map) =
            casa_grid_frequency_channel_map(&source, &cubic_direct, &[], CubeInterpolation::Cubic);
        assert_eq!(grid, cubic_direct);
        assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn parse_rest_frequency_supports_bare_mhz_and_frequency_units() {
        assert_eq!(
            parse_rest_frequency_hz("1420.405752").unwrap(),
            1.420405752e9
        );
        assert_eq!(parse_rest_frequency_hz("1.25GHz").unwrap(), 1.25e9);
    }

    #[test]
    fn velocity_ms_from_frequency_matches_radio_definition() {
        let velocity_ms = velocity_ms_from_frequency_hz(1.2e9, 1.25e9, DopplerRef::RADIO).unwrap();
        assert!((velocity_ms - 11_991_698.32).abs() < 1.0);
    }

    #[test]
    fn resolve_contiguous_channel_selection_defaults_to_full_spw() {
        let resolved = resolve_contiguous_channel_selection(&[1.0, 2.0, 3.0], None, None).unwrap();
        assert_eq!(
            resolved,
            ResolvedChannelSelection {
                indices: vec![0, 1, 2],
                frequencies_hz: vec![1.0, 2.0, 3.0],
            }
        );
    }

    #[test]
    fn resolve_contiguous_channel_selection_checks_bounds() {
        let error =
            resolve_contiguous_channel_selection(&[1.0, 2.0, 3.0], Some(2), Some(2)).unwrap_err();
        assert!(error.to_string().contains("outside spectral window"));
    }

    #[test]
    fn resolve_channel_selector_selection_supports_gaps_and_stride() {
        let selector = ChannelSelection {
            segments: vec![
                crate::selection::syntax::ChannelSelectionSegment {
                    start: 4,
                    end: 9,
                    stride: 1,
                },
                crate::selection::syntax::ChannelSelectionSegment {
                    start: 12,
                    end: 14,
                    stride: 1,
                },
                crate::selection::syntax::ChannelSelectionSegment {
                    start: 18,
                    end: 22,
                    stride: 2,
                },
            ],
        };
        let all = (0..24).map(|channel| channel as f64).collect::<Vec<_>>();
        let resolved = resolve_channel_selector_selection(&all, &selector).unwrap();
        assert_eq!(
            resolved.indices,
            vec![4, 5, 6, 7, 8, 9, 12, 13, 14, 18, 20, 22]
        );
        assert_eq!(
            resolved.frequencies_hz,
            vec![
                4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 12.0, 13.0, 14.0, 18.0, 20.0, 22.0
            ]
        );
    }

    #[test]
    fn linear_contributions_split_between_adjacent_ascending_channels() {
        let contributions = linear_channel_contributions(
            &[10.0, 20.0, 30.0],
            &[10.0, 20.0, 30.0],
            &[10.0, 10.0, 10.0],
            15.0,
        );
        assert_eq!(
            contributions,
            vec![
                CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: 10.0,
                    factor: 0.5,
                },
                CubeChannelContribution {
                    source_channel: 1,
                    source_frequency_hz: 20.0,
                    factor: 0.5,
                },
            ]
        );
    }

    #[test]
    fn linear_contributions_split_between_adjacent_descending_channels() {
        let contributions = linear_channel_contributions(
            &[30.0, 20.0, 10.0],
            &[30.0, 20.0, 10.0],
            &[10.0, 10.0, 10.0],
            15.0,
        );
        assert_eq!(
            contributions,
            vec![
                CubeChannelContribution {
                    source_channel: 1,
                    source_frequency_hz: 20.0,
                    factor: 0.5,
                },
                CubeChannelContribution {
                    source_channel: 2,
                    source_frequency_hz: 10.0,
                    factor: 0.5,
                },
            ]
        );
    }

    #[test]
    fn output_channel_contributions_use_transformed_query_frequencies() {
        let contributions = build_output_channel_contributions(
            &[100.0, 200.0, 300.0],
            &[100.0, 200.0, 300.0],
            &[100.0, 100.0, 100.0],
            &[100.0, 200.0, 300.0],
            CubeInterpolation::Linear,
        );
        assert_eq!(
            contributions,
            vec![
                vec![CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: 100.0,
                    factor: 1.0,
                }],
                vec![CubeChannelContribution {
                    source_channel: 1,
                    source_frequency_hz: 200.0,
                    factor: 1.0,
                }],
                vec![CubeChannelContribution {
                    source_channel: 2,
                    source_frequency_hz: 300.0,
                    factor: 1.0,
                }],
            ]
        );
    }

    #[test]
    fn linear_contributions_clip_below_lowest_source_channel_edge() {
        let contributions = linear_channel_contributions(
            &[1.0e9, 1.05e9, 1.10e9],
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            0.974_988_750_387e9,
        );
        assert!(contributions.is_empty());
    }

    #[test]
    fn linear_contributions_do_not_extrapolate_below_first_channel_center() {
        let contributions = linear_channel_contributions(
            &[1.0e9, 1.05e9, 1.10e9],
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            0.999_988_750_387e9,
        );
        assert!(contributions.is_empty());
    }

    #[test]
    fn linear_contributions_tolerate_sub_hz_roundoff_at_low_edge() {
        let contributions = linear_channel_contributions(
            &[
                999_988_750.387_0,
                1_049_988_187.881_7,
                1_099_987_625.376_4,
                1_149_987_062.871_2,
                1_199_986_500.365_9,
            ],
            &[
                999_988_750.387_0,
                1_049_988_187.881_7,
                1_099_987_625.376_4,
                1_149_987_062.871_2,
                1_199_986_500.365_9,
            ],
            &[
                49_999_437.494_7,
                49_999_437.494_7,
                49_999_437.494_7,
                49_999_437.494_7,
                49_999_437.494_7,
            ],
            999_988_749.894_262_6,
        );
        assert!(contributions.is_empty());
    }

    #[test]
    fn linear_model_contributions_clip_below_first_channel_center_within_edge() {
        let contributions = linear_channel_model_contributions(
            &[1.0e9, 1.05e9, 1.10e9],
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            0.999_988_750_387e9,
        );
        assert_eq!(
            contributions,
            vec![CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 1.0e9,
                factor: 1.0,
            }]
        );
    }

    #[test]
    fn linear_model_contributions_clip_above_highest_channel_center_within_edge() {
        let contributions = linear_channel_model_contributions(
            &[1.0e9, 1.05e9, 1.10e9],
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            1.100_011_249_613e9,
        );
        assert_eq!(
            contributions,
            vec![CubeChannelContribution {
                source_channel: 2,
                source_frequency_hz: 1.10e9,
                factor: 1.0,
            }]
        );
    }

    #[test]
    fn channel_mode_output_centers_shift_for_multi_channel_width() {
        let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
        let widths = vec![50.0e6; all.len()];
        let centers = channel_mode_output_centers(&all, &widths, 0, 2, 3).unwrap();
        assert_eq!(centers, vec![1.025e9, 1.125e9, 1.225e9]);
    }

    #[test]
    fn channel_mode_output_centers_support_negative_width() {
        let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
        let widths = vec![50.0e6; all.len()];
        let centers = channel_mode_output_centers(&all, &widths, 9, -2, 2).unwrap();
        assert_eq!(centers, vec![1.425e9, 1.325e9]);
    }

    #[test]
    fn channel_mode_single_output_width_uses_selected_channel_bin() {
        let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
        let widths = vec![50.0e6; all.len()];
        assert_eq!(
            channel_mode_single_output_width_hz(&all, &widths, 0, 1).unwrap(),
            50.0e6
        );
        assert_eq!(
            channel_mode_single_output_width_hz(&all, &widths, 0, 2).unwrap(),
            100.0e6
        );
        assert_eq!(
            channel_mode_single_output_width_hz(&all, &widths, 9, -2).unwrap(),
            100.0e6
        );
    }

    #[test]
    fn casa_transformed_channel_mode_output_centers_use_uniform_output_spacing() {
        let centers = casa_transformed_channel_mode_output_centers(
            &[10.0, 11.0, 12.001, 13.003],
            &[1.0, 1.001, 1.002, 1.003],
            1,
            1,
            3,
        )
        .unwrap();
        assert_eq!(centers, vec![10.9995, 11.9995, 12.9995]);
    }

    #[test]
    fn casa_transformed_channel_mode_output_centers_support_negative_width() {
        let centers = casa_transformed_channel_mode_output_centers(
            &[1.0e9, 1.05e9, 1.10e9, 1.15e9, 1.20e9],
            &[5.0e7; 5],
            3,
            -1,
            3,
        )
        .unwrap();
        assert_eq!(centers, vec![1.15e9, 1.10e9, 1.05e9]);
    }

    #[test]
    fn casa_regridding_input_widths_preserve_single_channel_width() {
        let widths_hz = casa_regridding_input_widths(&[1.420_405_752e9], &[2.5e6]).unwrap();
        assert_eq!(widths_hz, vec![2.5e6]);
    }

    #[test]
    fn casa_regridding_input_widths_preserve_measured_channel_widths() {
        let widths_hz = casa_regridding_input_widths(
            &[1.0e9, 1.000_001e9, 1.000_002e9],
            &[900.0, -950.0, 975.0],
        )
        .unwrap();
        assert_eq!(widths_hz, vec![900.0, 950.0, 975.0]);
    }

    #[test]
    fn default_generic_axis_start_uses_first_output_bin_center_for_positive_width() {
        let start_hz = default_generic_axis_start_hz(&[1.0e9, 1.05e9, 1.10e9], 0.1e9, 2).unwrap();
        assert!((start_hz - 1.025e9).abs() < 1.0);
    }

    #[test]
    fn default_generic_axis_start_uses_first_output_bin_center_for_negative_width() {
        let start_hz = default_generic_axis_start_hz(&[1.0e9, 1.05e9, 1.10e9], -0.1e9, 2).unwrap();
        assert!((start_hz - 1.125e9).abs() < 1.0);
    }

    #[test]
    fn default_generic_axis_start_anchors_descending_axes_to_low_frequency_edge() {
        let source_hz = (0..20)
            .map(|index| 1.0e9 + index as f64 * 50.0e6)
            .collect::<Vec<_>>();
        let start_hz = default_generic_axis_start_hz(&source_hz, -50.0e6, 10).unwrap();
        assert!((start_hz - 1.45e9).abs() < 1.0);
    }

    #[test]
    fn default_width_frequency_like_start_snaps_to_nearest_source_channel_center() {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::TOPO,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 11_991_700.0,
                frame: None,
            }),
            width: None,
        };
        let snapped_hz = snap_default_frequency_like_start_to_source_grid(
            &axis_config,
            &[1.0e9, 1.05e9, 1.10e9, 1.15e9, 1.20e9],
            1_199_999_992.995_154,
        );
        assert_eq!(snapped_hz, 1.20e9);
    }

    #[test]
    fn explicit_width_frequency_like_start_does_not_snap_to_source_grid() {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::TOPO,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 11_991_700.0,
                frame: None,
            }),
            width: Some(CubeAxisValue::FrequencyHz {
                hz: -5.0e7,
                frame: None,
            }),
        };
        let raw_hz = 1_199_999_992.995_154;
        let snapped_hz = snap_default_frequency_like_start_to_source_grid(
            &axis_config,
            &[1.0e9, 1.05e9, 1.10e9, 1.15e9, 1.20e9],
            raw_hz,
        );
        assert_eq!(snapped_hz, raw_hz);
    }

    #[test]
    fn explicit_measure_frame_overrides_outframe() {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::TOPO,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 11_977_600.0,
                frame: Some(FrequencyRef::BARY),
            }),
            width: None,
        };
        assert_eq!(
            effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
            FrequencyRef::BARY
        );
    }

    #[test]
    fn conflicting_measure_frames_are_rejected() {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cube,
            outframe: FrequencyRef::LSRK,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 11_977_600.0,
                frame: Some(FrequencyRef::BARY),
            }),
            width: Some(CubeAxisValue::VelocityMs {
                ms: 11_991_700.0,
                frame: Some(FrequencyRef::TOPO),
            }),
        };
        let error = effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap_err();
        assert!(error.to_string().contains("disagree"));
    }

    #[test]
    fn cubedata_mode_uses_native_source_frequency_frame() {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cubedata,
            outframe: FrequencyRef::LSRK,
            veltype: DopplerRef::RADIO,
            interpolation: CubeInterpolation::Linear,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: 11_977_600.0,
                frame: Some(FrequencyRef::BARY),
            }),
            width: None,
        };
        assert_eq!(
            effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
            FrequencyRef::TOPO
        );
    }

    #[test]
    fn cubic_interpolation_is_rejected_until_implemented() {
        let error = ensure_supported_interpolation(CubeInterpolation::Cubic).unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn asymmetric_frame_conversion_matches_manual_direct_hops() {
        let measures = crate::open_measures_runtime().expect("test measures runtime");
        let source_frame = MeasFrame::new()
            .with_measures(measures.clone())
            .with_epoch(casa_types::measures::epoch::MEpoch::from_mjd(
                59_000.0,
                casa_types::measures::epoch::EpochRef::UTC,
            ))
            .with_position(casa_types::measures::position::MPosition::new_itrf(
                -1_601_185.4,
                -5_041_977.5,
                3_554_875.9,
            ))
            .with_direction(casa_types::measures::direction::MDirection::from_angles(
                1.0,
                0.5,
                casa_types::measures::direction::DirectionRef::J2000,
            ));
        let target_frame = MeasFrame::new()
            .with_measures(measures)
            .with_epoch(casa_types::measures::epoch::MEpoch::from_mjd(
                59_001.0,
                casa_types::measures::epoch::EpochRef::UTC,
            ))
            .with_position(casa_types::measures::position::MPosition::new_itrf(
                -1_601_185.4,
                -5_041_977.5,
                3_554_875.9,
            ))
            .with_direction(casa_types::measures::direction::MDirection::from_angles(
                1.1,
                0.55,
                casa_types::measures::direction::DirectionRef::J2000,
            ));
        let frequency_hz = 6.667_582_296e9;

        let converted = convert_frequency_to_frame_with_frames(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            frequency_hz,
            Some(&source_frame),
            Some(&target_frame),
        )
        .unwrap();

        let manual = MFrequency::new(frequency_hz, FrequencyRef::TOPO)
            .convert_to(FrequencyRef::GEO, &source_frame)
            .unwrap()
            .convert_to(FrequencyRef::BARY, &source_frame)
            .unwrap()
            .convert_to(FrequencyRef::LSRK, &target_frame)
            .unwrap()
            .hz();

        let source_only = convert_frequency_to_frame_with_frame(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            frequency_hz,
            Some(&source_frame),
        )
        .unwrap();

        assert!((converted - manual).abs() < 1.0e-6);
        assert!((converted - source_only).abs() > 1.0e-6);
    }

    #[test]
    fn cross_frame_frequency_conversion_requires_structured_frame_context() {
        let missing_source = convert_frequency_to_frame_with_frames(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            6.667_582_296e9,
            None,
            Some(&MeasFrame::new()),
        )
        .unwrap_err();
        assert!(
            missing_source
                .to_string()
                .contains("source frame required for cross-frame frequency conversion")
        );

        let missing_target = convert_frequency_to_frame_with_frames(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            6.667_582_296e9,
            Some(&MeasFrame::new()),
            None,
        )
        .unwrap_err();
        assert!(
            missing_target
                .to_string()
                .contains("target frame required for cross-frame frequency conversion")
        );

        assert_eq!(
            convert_frequency_to_frame_with_frames(
                FrequencyRef::LSRK,
                FrequencyRef::LSRK,
                6.667_582_296e9,
                None,
                None,
            )
            .unwrap(),
            6.667_582_296e9
        );
    }

    #[test]
    fn parse_cube_axis_value_rejects_empty_and_unknown_units() {
        let empty = CubeAxisValue::parse("   ", DopplerRef::RADIO).unwrap_err();
        assert!(empty.to_string().contains("empty"));

        let invalid = CubeAxisValue::parse("1kg", DopplerRef::RADIO).unwrap_err();
        assert!(invalid.to_string().contains("neither a channel id"));
    }

    #[test]
    fn channel_mode_output_centers_reject_zero_width_and_mismatched_arrays() {
        let all = vec![1.0e9, 1.05e9, 1.10e9];
        let zero_width = channel_mode_output_centers(&all, &[5.0e7; 3], 0, 0, 1).unwrap_err();
        assert!(zero_width.to_string().contains("must not be zero"));

        let mismatched = channel_mode_output_centers(&all, &[5.0e7; 2], 0, 1, 1).unwrap_err();
        assert!(
            mismatched
                .to_string()
                .contains("matching frequency/width arrays")
        );
    }

    #[test]
    fn for_casa_cube_axis_rejects_zero_output_channels() {
        let engine = test_engine();
        let error = CubeSpectralSetup::for_casa_cube_axis(
            FrequencyRef::TOPO,
            &[1.0e9, 1.05e9],
            &[5.0e7, 5.0e7],
            0,
            &CubeAxisConfig::default(),
            59_000.0 * 86_400.0,
            0,
            None,
            [59_000.0 * 86_400.0, 59_000.1 * 86_400.0],
            &engine,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be positive"));
    }

    #[test]
    fn for_casa_cube_channel_axis_keeps_single_output_channel_width() {
        let engine = test_engine();
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cubedata,
            start: Some(CubeAxisValue::Channel(0)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        };
        let (setup, _support) = CubeSpectralSetup::for_casa_cube_axis(
            FrequencyRef::TOPO,
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            1,
            &axis_config,
            59_000.0 * 86_400.0,
            0,
            None,
            [59_000.0 * 86_400.0, 59_000.1 * 86_400.0],
            &engine,
        )
        .unwrap();
        assert_eq!(setup.output_channel_frequencies_hz, vec![1.0e9]);
        assert_eq!(setup.output_channel_widths_hz, vec![5.0e7]);
    }

    #[test]
    fn for_casa_cube_channel_axis_rejects_frequency_like_channel_inputs() {
        let engine = test_engine();
        let axis_config = CubeAxisConfig {
            start: Some(CubeAxisValue::FrequencyHz {
                hz: 1.0e9,
                frame: None,
            }),
            ..CubeAxisConfig::default()
        };
        let error = CubeSpectralSetup::for_casa_cube_channel_axis(
            FrequencyRef::TOPO,
            &[1.0e9, 1.05e9],
            &[5.0e7, 5.0e7],
            1,
            &axis_config,
            59_000.0 * 86_400.0,
            0,
            None,
            [59_000.0 * 86_400.0, 59_000.1 * 86_400.0],
            &engine,
        )
        .unwrap_err();
        assert!(error.to_string().contains("channel-valued start"));
    }

    #[test]
    fn row_output_channel_contributions_batch_rejects_width_mismatch() {
        let setup = CubeSpectralSetup {
            source_freq_ref: FrequencyRef::TOPO,
            output_freq_ref: FrequencyRef::TOPO,
            interpolation: CubeInterpolation::Linear,
            interpolation_uses_native_source_frequencies: false,
            output_frame_reference_time_mjd_sec: 59_000.0 * 86_400.0,
            output_frame_field_id: 0,
            output_channel_frequencies_hz: vec![1.0e9],
            output_channel_widths_hz: vec![5.0e7],
        };
        let engine = test_engine();
        let error = setup
            .row_output_channel_contributions_batch(&[1.0e9], &[], 59_000.0 * 86_400.0, 0, &engine)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("matching frequency/width arrays")
        );
    }

    #[test]
    fn row_output_channel_contributions_batch_uses_native_interpolation_when_requested() {
        let setup = CubeSpectralSetup {
            source_freq_ref: FrequencyRef::TOPO,
            output_freq_ref: FrequencyRef::LSRK,
            interpolation: CubeInterpolation::Nearest,
            interpolation_uses_native_source_frequencies: true,
            output_frame_reference_time_mjd_sec: 59_000.0 * 86_400.0,
            output_frame_field_id: 0,
            output_channel_frequencies_hz: vec![1.05e9],
            output_channel_widths_hz: vec![5.0e7],
        };
        let engine = test_engine();
        let contributions = setup
            .row_output_channel_contributions_batch(
                &[1.0e9, 1.05e9, 1.10e9],
                &[5.0e7, 5.0e7, 5.0e7],
                59_000.0 * 86_400.0,
                0,
                &engine,
            )
            .unwrap();
        assert_eq!(contributions[0].len(), 1);
        assert_eq!(contributions[0][0].source_channel, 1);
        assert_eq!(contributions[0][0].source_frequency_hz, 1.05e9);
    }

    #[test]
    fn nearest_channel_index_requires_points_to_stay_within_half_spacing() {
        assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 25.0), Some(1));
        assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 35.1), None);
    }

    #[test]
    fn spectral_interpolation_contributions_cover_nearest_and_cubic_paths() {
        let nearest = spectral_interpolation_contributions(
            &[10.0, 20.0, 30.0],
            &[10.0, 20.0, 30.0],
            &[10.0, 10.0, 10.0],
            CubeInterpolation::Nearest,
            19.0,
        );
        assert_eq!(nearest[0].source_channel, 1);

        let cubic = spectral_interpolation_contributions(
            &[10.0, 20.0, 30.0],
            &[10.0, 20.0, 30.0],
            &[10.0, 10.0, 10.0],
            CubeInterpolation::Cubic,
            15.0,
        );
        assert_eq!(cubic.len(), 2);
        assert!((cubic[0].factor + cubic[1].factor - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn linear_channel_contributions_cover_single_channel_and_invalid_shapes() {
        let inside = linear_channel_contributions(&[42.0], &[42.0], &[4.0], 43.0);
        assert_eq!(inside.len(), 1);
        assert_eq!(inside[0].source_channel, 0);

        let outside = linear_channel_contributions(&[42.0], &[42.0], &[4.0], 45.0);
        assert!(outside.is_empty());

        let invalid = linear_channel_contributions(&[42.0], &[42.0, 43.0], &[4.0, 4.0], 42.0);
        assert!(invalid.is_empty());
    }

    #[test]
    fn optical_velocity_output_centers_require_velocity_width_and_rest_frequency() {
        let axis_config = CubeAxisConfig {
            veltype: DopplerRef::Z,
            start: Some(CubeAxisValue::VelocityMs {
                ms: 10_000.0,
                frame: None,
            }),
            width: Some(CubeAxisValue::FrequencyHz {
                hz: 1.0e6,
                frame: None,
            }),
            rest_frequency_hz: Some(1.42e9),
            ..CubeAxisConfig::default()
        };
        let error = optical_velocity_output_centers(
            &[1.42e9, 1.41e9, 1.40e9],
            &[1.0e6, 1.0e6, 1.0e6],
            3,
            &axis_config,
        )
        .unwrap_err();
        assert!(error.to_string().contains("velocity-like value"));

        let missing_rest = required_rest_frequency_hz(None, &axis_config.start).unwrap_err();
        assert!(
            missing_rest
                .to_string()
                .contains("requires a rest frequency")
        );
    }

    #[test]
    fn optical_velocity_output_centers_support_descending_axes() {
        let axis_config = CubeAxisConfig {
            veltype: DopplerRef::Z,
            start: Some(CubeAxisValue::VelocityMs {
                ms: 15_000.0,
                frame: None,
            }),
            width: Some(CubeAxisValue::VelocityMs {
                ms: -5_000.0,
                frame: None,
            }),
            rest_frequency_hz: Some(1.42e9),
            ..CubeAxisConfig::default()
        };
        let centers = optical_velocity_output_centers(
            &[1.42e9, 1.419e9, 1.418e9, 1.417e9],
            &[1.0e6, 1.0e6, 1.0e6, 1.0e6],
            3,
            &axis_config,
        )
        .unwrap();
        assert_eq!(centers.len(), 3);
        assert!(centers.windows(2).all(|pair| pair[0] > pair[1]));
    }
}
