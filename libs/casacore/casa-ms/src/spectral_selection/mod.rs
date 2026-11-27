// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style spectral-axis and channel-selection helpers for imaging.
//!
//! CASA handles generic row selectors near `MSSelection`, while spectral cube
//! axis construction is delegated lower to `SubMS`, `MSTransformRegridder`,
//! and synthesis utility code. This module mirrors that split on the Rust
//! side: it owns reusable channel-range resolution and output cube-axis setup
//! that should not live in application code.

use std::str::FromStr;

use casa_core::measures::PreparedFrequencyConversion;
use casa_core::measures::direction::MDirection;
use casa_core::measures::doppler::DopplerRef;
use casa_core::measures::doppler::MDoppler;
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::quanta::{Quantity, Unit};

use crate::derived::engine::MsCalEngine;
use crate::error::{MsError, MsResult};
use crate::selection::syntax::ChannelSelection;

mod combine;
mod default_start;
mod regrid;

pub use combine::combine_spectral_windows;
pub use combine::{CombinedChannel, combine_windows};
pub use default_start::{casa_default_start_channels, casa_selected_band_hz};
pub use regrid::LegacyRegridQuantity;
pub use regrid::{
    CasaChannelFrequencies, RegridQuantity, RegridRequest, casa_data_channel_frequencies,
    centered_numeric_frequencies,
};

mod axis;
mod contributions;
mod frames;

pub use axis::resolve_channel_selector_selection;
pub use axis::resolve_contiguous_channel_selection;
use axis::*;
use contributions::*;
pub use frames::convert_frequency_to_frame;
pub use frames::convert_frequency_to_frame_with_frame;
pub(crate) use frames::convert_frequency_to_frame_with_frames;
pub use frames::velocity_ms_from_frequency_hz;
use frames::*;

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
    /// The band tclean's `spw` selection spans in the data's frame
    /// ([`casa_selected_band_hz`]; CASA's `datafstart` and `datafend`).
    /// With `start` unset, CASA lays the grid over the data channels within
    /// it when it begins above their lowest channel centre, so that the
    /// cube starts at the first selected channel
    /// (`buildCoordinateSystemCore`); `None` keeps every data channel.
    pub selected_band_hz: Option<[f64; 2]>,
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
            selected_band_hz: None,
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
    /// CASA's stored frequency table, which may be shorter than the cube.
    pub output_channel_table_hz: Option<Vec<f64>>,
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

impl CubeSpectralSetup {
    /// Build a CASA-style output cube axis over the data channels
    /// `data_frequencies_hz` and `data_channel_widths_hz` in
    /// `source_freq_ref`: one window's `CHAN_FREQ` and `CHAN_WIDTH`, or
    /// several windows' merged by [`combine_spectral_windows`], as
    /// `SynthesisParamsImage::buildCoordinateSystemCore` takes them.
    ///
    /// A `start` and `width` that are channel numbers (or unset) take
    /// [`Self::for_casa_cube_channel_axis`]. A frequency or velocity `start`
    /// or `width` takes CASA's real-number regridding,
    /// `MSTransformRegridder::calcChanFreqs` as
    /// `SynthesisParamsImage::getImFreq` calls it: the data channels are
    /// converted to the output frame, the requested grid is laid over them
    /// with its band centre kept within their edges, and the image axis
    /// starts at the first resulting channel centre and steps by the
    /// difference of the first two (`buildCoordinateSystemCore`). `start`
    /// is not snapped to a data channel; it moves only when that band
    /// centre falls outside the data.
    ///
    /// With `start` unset, CASA lays either grid over the data channels
    /// within `axis_config.selected_band_hz` when that band begins above
    /// their lowest channel centre, so the cube starts at the first
    /// selected channel. `nchan` counts the output channels; `None`
    /// (tclean's `nchan=-1`) takes as many output channels as fit from the
    /// start, or the band's lower edge, to the band's far edge
    /// (`regridChanBounds` with `nchan < 0`). Which channels of each
    /// window the axis reads is [`Self::source_channel_support`].
    #[allow(clippy::too_many_arguments)]
    pub fn for_casa_cube_axis(
        source_freq_ref: FrequencyRef,
        data_frequencies_hz: &[f64],
        data_channel_widths_hz: &[f64],
        nchan: Option<usize>,
        axis_config: &CubeAxisConfig,
        reference_row_time_mjd_sec: f64,
        field_id: usize,
        phase_center_direction: Option<MDirection>,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Self> {
        let channel_mode = matches!(
            (&axis_config.start, &axis_config.width),
            (
                Some(CubeAxisValue::Channel(_)) | None,
                Some(CubeAxisValue::Channel(_)) | None
            )
        );
        // For an unset start CASA lays the grid over the selected band.
        let cut = match (&axis_config.start, axis_config.selected_band_hz) {
            (None, Some(band_hz)) => default_start::casa_default_start_channels(
                data_frequencies_hz,
                data_channel_widths_hz,
                band_hz,
            ),
            _ => None,
        };
        let (band_frequencies_hz, band_widths_hz) = match cut {
            Some(cut) => (
                &data_frequencies_hz[cut.clone()],
                &data_channel_widths_hz[cut],
            ),
            None => (data_frequencies_hz, data_channel_widths_hz),
        };
        if channel_mode {
            return Self::for_casa_cube_channel_axis(
                source_freq_ref,
                band_frequencies_hz,
                band_widths_hz,
                nchan,
                axis_config,
                reference_row_time_mjd_sec,
                field_id,
                phase_center_direction,
                derived_engine,
            );
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
                    phase_center_direction,
                )
                .map_err(|error| {
                    MsError::VersionError(format!(
                        "build spectral frame for field {field_id}: {error}"
                    ))
                })?,
            )
        };
        // `calcChanFreqs` regrids the data channels converted to the output
        // frame (`transNewXin`) with widths measured there
        // (`transCHAN_WIDTH`); cubedata and an unchanged frame keep them.
        let regridding_widths_hz =
            casa_regridding_input_widths(band_frequencies_hz, band_widths_hz)?;
        let (source_frequencies_hz, source_widths_hz) = match frame.as_ref() {
            None => (band_frequencies_hz.to_vec(), regridding_widths_hz),
            Some(frame) => (
                band_frequencies_hz
                    .iter()
                    .map(|&frequency_hz| {
                        convert_frequency_to_frame_with_frame(
                            source_freq_ref,
                            output_freq_ref,
                            frequency_hz,
                            Some(frame),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?,
                convert_channel_widths_to_frame_with_frame(
                    source_freq_ref,
                    output_freq_ref,
                    band_frequencies_hz,
                    &regridding_widths_hz,
                    frame,
                )?,
            ),
        };
        let request = casa_regrid_request(axis_config, nchan)?;
        let channels =
            regrid::casa_channel_frequencies(&request, &source_frequencies_hz, &source_widths_hz)?;
        let tabulated_optical = request.quantity == regrid::RegridQuantity::OpticalVelocity;
        let output_channel_frequencies_hz =
            casa_regridded_image_axis_hz(&channels, &request, axis_config.veltype);
        Ok(Self {
            source_freq_ref,
            output_freq_ref,
            interpolation: axis_config.interpolation,
            interpolation_uses_native_source_frequencies: uses_native_source_interpolation(
                axis_config,
            ),
            output_frame_reference_time_mjd_sec: reference_row_time_mjd_sec,
            output_frame_field_id: field_id,
            output_channel_widths_hz: if output_channel_frequencies_hz.len() == 1 {
                vec![channels.steps_hz[0].abs()]
            } else {
                channel_widths_from_centers(&output_channel_frequencies_hz)?
            },
            output_channel_frequencies_hz,
            output_channel_table_hz: tabulated_optical.then_some(channels.centres_hz),
        })
    }

    /// Build a CASA-style output cube axis in channel mode over the data
    /// channels `data_frequencies_hz` and `data_channel_widths_hz`.
    ///
    /// This models the source-backed `tclean` cube tests that specify
    /// `start`/`width` as channel numbers. Output channels are defined on
    /// the data channels; any channel selector affects which input data are
    /// available, not the output channel locations themselves. With
    /// `nchan` `None` the axis holds as many channels of the output width
    /// as fit from the start channel's edge to the data's far edge.
    #[allow(clippy::too_many_arguments)]
    pub fn for_casa_cube_channel_axis(
        source_freq_ref: FrequencyRef,
        data_frequencies_hz: &[f64],
        data_channel_widths_hz: &[f64],
        nchan: Option<usize>,
        axis_config: &CubeAxisConfig,
        reference_row_time_mjd_sec: f64,
        field_id: usize,
        phase_center_direction: Option<MDirection>,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Self> {
        if nchan == Some(0) {
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
        let regridding_input_widths_hz =
            casa_regridding_input_widths(data_frequencies_hz, data_channel_widths_hz)?;
        // CASA `calcChanFreqs` skips the frame transform for cubedata and
        // builds the bounds with the same `regridChanBounds` code.
        let (output_freq_ref, frequencies_hz, widths_hz) = match axis_config.specmode {
            CubeSpecMode::Cubedata => (
                source_freq_ref,
                data_frequencies_hz.to_vec(),
                regridding_input_widths_hz,
            ),
            CubeSpecMode::Cube => {
                let output_freq_ref = axis_config.outframe;
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
                let frequencies_hz = data_frequencies_hz
                    .iter()
                    .map(|&frequency_hz| {
                        convert_frequency_to_frame_with_frame(
                            source_freq_ref,
                            output_freq_ref,
                            frequency_hz,
                            Some(&frame),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                let widths_hz = convert_channel_widths_to_frame_with_frame(
                    source_freq_ref,
                    output_freq_ref,
                    data_frequencies_hz,
                    &regridding_input_widths_hz,
                    &frame,
                )?;
                (output_freq_ref, frequencies_hz, widths_hz)
            }
        };
        let output_channel_frequencies_hz = casa_image_spectral_axis_hz(
            casa_transformed_channel_mode_output_centers(
                &frequencies_hz,
                &widths_hz,
                start_channel,
                width_channels,
                nchan,
            )?,
            axis_config.veltype,
        );
        let output_channel_widths_hz = if output_channel_frequencies_hz.len() == 1 {
            vec![channel_mode_single_output_width_hz(
                &frequencies_hz,
                &widths_hz,
                start_channel,
                width_channels,
            )?]
        } else {
            channel_widths_from_centers(&output_channel_frequencies_hz)?
        };
        Ok(Self {
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
            output_channel_table_hz: None,
        })
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
        // CASA FTMachine bypasses interpolation for a single output plane.
        let interpolation = match self.interpolation {
            CubeInterpolation::Cubic if self.output_channel_frequencies_hz.len() == 1 => {
                CubeInterpolation::Nearest
            }
            interpolation => interpolation,
        };
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
        let source_channel_widths_for_interpolation = if interpolation == CubeInterpolation::Nearest
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
            interpolation,
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
            interpolation,
        );
        let padded_grid_channel_contributions = build_grid_channel_contributions(
            source_frequencies_hz,
            &source_frequencies_for_interpolation,
            &source_channel_widths_for_interpolation,
            &padded_output_frequencies_for_interpolation,
            &[],
            interpolation,
        );
        let source_channel_model_contributions = source_frequencies_for_interpolation
            .into_iter()
            .map(|source_frequency_hz| match interpolation {
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

impl CubeSpectralSetup {
    /// The channels of one window, `window_frequencies_hz` and
    /// `window_channel_widths_hz` in its own order, whose samples reach a
    /// plane of this axis (or of its padded Briggs density axis) at either
    /// end of `time_bounds_mjd_sec` towards `field_id`, widened by one
    /// channel below and two above: the channels CASA reads of the window
    /// when `SynthesisImager::tuneSelectData` replaces the selection by the
    /// image's band. Empty when none reaches the axis.
    ///
    /// The window and `field_id` are `derived_engine`'s MeasurementSet's,
    /// which may be another than the one the axis was made from (CASA's
    /// tclean images several MeasurementSets on one's axis), so the output
    /// frame is taken at this axis's reference time towards that field.
    ///
    /// # Errors
    ///
    /// A frame conversion that fails, or widths that do not match the
    /// frequencies.
    pub fn source_channel_support(
        &self,
        window_frequencies_hz: &[f64],
        window_channel_widths_hz: &[f64],
        time_bounds_mjd_sec: [f64; 2],
        field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<ResolvedChannelSelection> {
        let setup = Self {
            output_frame_field_id: field_id,
            ..self.clone()
        };
        let mut first_index = None::<usize>;
        let mut last_index = None::<usize>;
        for row_time_mjd_sec in [time_bounds_mjd_sec[0], time_bounds_mjd_sec[1]] {
            let contributions = setup.row_spectral_contributions(
                window_frequencies_hz,
                window_channel_widths_hz,
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
                last_index =
                    Some(last_index.map_or(source_index, |current| current.max(source_index)));
            }
        }
        let Some(mut first_index) = first_index else {
            return Ok(ResolvedChannelSelection {
                indices: Vec::new(),
                frequencies_hz: Vec::new(),
            });
        };
        let mut last_index = last_index.expect("first_index implies last_index");
        first_index = first_index.saturating_sub(1);
        last_index = (last_index + 2).min(window_frequencies_hz.len().saturating_sub(1));
        Ok(ResolvedChannelSelection {
            indices: (first_index..=last_index).collect(),
            frequencies_hz: window_frequencies_hz[first_index..=last_index].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests;
