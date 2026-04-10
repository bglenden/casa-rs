// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style spectral-axis and channel-selection helpers for imaging.
//!
//! CASA handles generic row selectors near `MSSelection`, while spectral cube
//! axis construction is delegated lower to `SubMS`, `MSTransformRegridder`,
//! and synthesis utility code. This module mirrors that split on the Rust
//! side: it owns reusable channel-range resolution and output cube-axis setup
//! that should not live in application code.

use std::str::FromStr;

use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::doppler::MDoppler;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};

use crate::derived::engine::MsCalEngine;
use crate::error::{MsError, MsResult};
use crate::selection_syntax::ChannelSelection;

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
    /// Output cube channel frequencies in `output_freq_ref`.
    pub output_channel_frequencies_hz: Vec<f64>,
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

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

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
        time_bounds_mjd_sec: [f64; 2],
        derived_engine: &MsCalEngine,
    ) -> MsResult<(Self, ResolvedChannelSelection)> {
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
                derived_engine
                    .spectral_frame_observatory(reference_row_time_mjd_sec, field_id)
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
        time_bounds_mjd_sec: [f64; 2],
        derived_engine: &MsCalEngine,
    ) -> MsResult<(Self, ResolvedChannelSelection)> {
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
        let output_channel_frequencies_hz =
            if matches!(axis_config.specmode, CubeSpecMode::Cubedata) {
                channel_mode_output_centers(
                    all_source_frequencies_hz,
                    all_source_channel_widths_hz,
                    start_channel,
                    width_channels,
                    nchan,
                )?
            } else {
                let frame = derived_engine
                    .spectral_frame_observatory(reference_row_time_mjd_sec, field_id)
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
                let source_channel_widths_in_output_frame =
                    convert_channel_widths_to_frame_with_frame(
                        source_freq_ref,
                        output_freq_ref,
                        all_source_frequencies_hz,
                        all_source_channel_widths_hz,
                        &frame,
                    )?;
                channel_mode_output_centers(
                    &source_frequencies_in_output_frame,
                    &source_channel_widths_in_output_frame,
                    start_channel,
                    width_channels,
                    nchan,
                )?
            };
        let setup = Self {
            source_freq_ref,
            output_freq_ref,
            interpolation: axis_config.interpolation,
            interpolation_uses_native_source_frequencies: uses_native_source_interpolation(
                axis_config,
            ),
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

    /// Build output-channel interpolation contributions for one row, reusing
    /// the per-row spectral frame conversion state.
    pub fn row_output_channel_contributions_batch(
        &self,
        source_frequencies_hz: &[f64],
        source_channel_widths_hz: &[f64],
        row_time_mjd_sec: f64,
        field_id: usize,
        derived_engine: &MsCalEngine,
    ) -> MsResult<Vec<Vec<CubeChannelContribution>>> {
        if source_channel_widths_hz.len() != source_frequencies_hz.len() {
            return Err(MsError::VersionError(format!(
                "row cube interpolation requires matching frequency/width arrays, got {} and {}",
                source_frequencies_hz.len(),
                source_channel_widths_hz.len()
            )));
        }
        let source_frequencies_for_interpolation =
            if self.interpolation_uses_native_source_frequencies {
                source_frequencies_hz.to_vec()
            } else {
                let spectral_frame = if self.source_freq_ref == self.output_freq_ref {
                    None
                } else {
                    Some(
                        derived_engine
                            .spectral_frame_observatory(row_time_mjd_sec, field_id)
                            .map_err(|error| {
                                MsError::VersionError(format!(
                                    "build spectral frame for field {field_id}: {error}"
                                ))
                            })?,
                    )
                };
                source_frequencies_hz
                    .iter()
                    .copied()
                    .map(|source_frequency_hz| {
                        convert_frequency_to_frame_with_frame(
                            self.source_freq_ref,
                            self.output_freq_ref,
                            source_frequency_hz,
                            spectral_frame.as_ref(),
                        )
                    })
                    .collect::<MsResult<Vec<_>>>()?
            };
        let source_channel_widths_for_interpolation =
            if self.interpolation_uses_native_source_frequencies {
                source_channel_widths_hz.to_vec()
            } else {
                let spectral_frame = if self.source_freq_ref == self.output_freq_ref {
                    None
                } else {
                    Some(
                        derived_engine
                            .spectral_frame_observatory(row_time_mjd_sec, field_id)
                            .map_err(|error| {
                                MsError::VersionError(format!(
                                    "build spectral frame for field {field_id}: {error}"
                                ))
                            })?,
                    )
                };
                match spectral_frame.as_ref() {
                    Some(frame) => convert_channel_widths_to_frame_with_frame(
                        self.source_freq_ref,
                        self.output_freq_ref,
                        source_frequencies_hz,
                        source_channel_widths_hz,
                        frame,
                    )?,
                    None => source_channel_widths_hz.to_vec(),
                }
            };
        Ok(self
            .output_channel_frequencies_hz
            .iter()
            .copied()
            .map(|output_frequency_hz| {
                spectral_interpolation_contributions(
                    source_frequencies_hz,
                    &source_frequencies_for_interpolation,
                    &source_channel_widths_for_interpolation,
                    self.interpolation,
                    output_frequency_hz,
                )
            })
            .collect())
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

fn convert_frequency_to_frame(
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

fn convert_frequency_to_frame_with_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    frame: Option<&MeasFrame>,
) -> MsResult<f64> {
    if source_freq_ref == target_freq_ref {
        return Ok(frequency_hz);
    }
    let frame = frame.expect("frame required for cross-frame frequency conversion");
    MFrequency::new(frequency_hz, source_freq_ref)
        .convert_to(target_freq_ref, frame)
        .map(|frequency| frequency.hz())
        .map_err(|error| {
            MsError::VersionError(format!(
                "convert frequency {frequency_hz} Hz from {source_freq_ref} to {target_freq_ref}: {error}"
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
            let upper_hz = convert_frequency_to_frame_with_frame(
                source_freq_ref,
                target_freq_ref,
                frequency_hz + half_width_hz,
                Some(frame),
            )?;
            let lower_hz = convert_frequency_to_frame_with_frame(
                source_freq_ref,
                target_freq_ref,
                frequency_hz - half_width_hz,
                Some(frame),
            )?;
            Ok((upper_hz - lower_hz).abs())
        })
        .collect()
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

fn linear_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
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
        let contributions = cube_setup.row_output_channel_contributions_batch(
            all_source_frequencies_hz,
            all_source_channel_widths_hz,
            row_time_mjd_sec,
            field_id,
            derived_engine,
        )?;
        for source_index in contributions
            .into_iter()
            .flatten()
            .map(|contribution| contribution.source_channel)
        {
            first_index =
                Some(first_index.map_or(source_index, |current| current.min(source_index)));
            last_index = Some(last_index.map_or(source_index, |current| current.max(source_index)));
        }
    }
    let first_index = first_index.ok_or_else(|| {
        MsError::VersionError(
            "cube channel selection produced no supporting source channels".to_string(),
        )
    })?;
    let last_index = last_index.expect("first_index implies last_index");
    Ok(ResolvedChannelSelection {
        indices: (first_index..=last_index).collect(),
        frequencies_hz: all_source_frequencies_hz[first_index..=last_index].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
                crate::selection_syntax::ChannelSelectionSegment {
                    start: 4,
                    end: 9,
                    stride: 1,
                },
                crate::selection_syntax::ChannelSelectionSegment {
                    start: 12,
                    end: 14,
                    stride: 1,
                },
                crate::selection_syntax::ChannelSelectionSegment {
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
}
