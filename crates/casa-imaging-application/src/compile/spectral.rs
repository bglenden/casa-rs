// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image's spectral axis: one constant-basis continuum plane, or a
//! cube's channel-local output axis in its output or the moving source's
//! rest frame (CASA `SpectralImagingMode`, `CubeSpectralSetup`).

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{
    DirectionCoordinateSpec, DopplerConvention, Epoch, FrequencyFrame, ItrfPosition,
    ReconstructionBasis, RestFrequency, SpectralCoordinateSpec, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, TimeScale,
};
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeInterpolation, CubeSpectralSetup, MeasurementSet,
    SubtableId, spectral_selection::CubeSpecMode,
};
use casa_types::ArrayValue;
use casa_types::measures::{
    direction::MDirection, doppler::DopplerRef, epoch::EpochRef, frame::MeasFrame,
    frequency::FrequencyRef,
};

use super::boxed;
use super::selection::{
    SourceSpectralWindow, Survey, WindowChannels, explicit_spw_channels, one_window,
};
use crate::{ApplicationError, ImagingRequest, SpecMode};

/// The compiled spectral axis and the source channels it reads.
pub(super) struct PreparedSpectralAxis {
    pub(super) selected_source_channels: WindowChannels,
    pub(super) source_frame: FrequencyFrame,
    pub(super) output_frequency_reference: FrequencyRef,
    pub(super) output_frame: FrequencyFrame,
    pub(super) anchor: SpectralFrameAnchor,
    pub(super) wcs: SpectralWcs,
    pub(super) rest_frequency: RestFrequency,
    /// The rest frequency the image header records.
    pub(super) image_rest_frequency_hz: f64,
    pub(super) doppler: DopplerConvention,
    pub(super) sampling: SpectralSamplingLaw,
    pub(super) basis: ReconstructionBasis,
    pub(super) output_channels: usize,
    /// Centre of the first output channel.
    pub(super) reference_frequency_hz: f64,
    /// Channel increment; a continuum plane's full band.
    pub(super) increment_hz: f64,
}

impl PreparedSpectralAxis {
    /// The model's spectral coordinate of this axis.
    pub(super) fn coordinate(&self) -> SpectralCoordinateSpec {
        SpectralCoordinateSpec::new(
            self.source_frame,
            self.output_frame,
            self.anchor,
            self.wcs.clone(),
            self.rest_frequency,
            self.doppler,
        )
    }
}

/// Where and when a frame conversion is evaluated.
pub(super) struct FrameContext<'a> {
    pub(super) anchor_time_mjd_seconds: f64,
    pub(super) time_bounds_mjd_seconds: [f64; 2],
    pub(super) field_id: usize,
    pub(super) phase: MDirection,
    pub(super) direction: DirectionCoordinateSpec,
    pub(super) engine: &'a casa_ms::derived::engine::MsCalEngine,
}

/// The output axis of one spectral mode, before its frame anchor.
struct AxisLaw {
    selected_source_channels: WindowChannels,
    output_frequency_reference: FrequencyRef,
    reference_frequency_hz: f64,
    increment_hz: f64,
    output_channels: usize,
    rest_frequency: RestFrequency,
    image_rest_frequency_hz: f64,
    doppler: DopplerConvention,
    sampling: SpectralSamplingLaw,
    basis: ReconstructionBasis,
}

/// The request's spectral axis over the surveyed selection.
pub(super) fn prepare_axis(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    survey: &Survey,
    frame: &FrameContext<'_>,
    moving_rest_frame: Option<&MeasFrame>,
) -> Result<PreparedSpectralAxis, ApplicationError> {
    let source_reference = survey.source_frequency_reference;
    let law = match request.specmode {
        SpecMode::Mfs => continuum_law(survey)?,
        SpecMode::Cube | SpecMode::Cubedata => cube_law(request, ms, survey, frame)?,
        SpecMode::Cubesource => {
            let frame_with_velocity = moving_rest_frame.ok_or_else(|| {
                boxed("source-frame cube imaging requires an ephemeris radial velocity")
            })?;
            source_frame_cube_law(request, ms, survey, frame, frame_with_velocity)?
        }
    };
    let source_frame = imaging_frequency_frame(source_reference)?;
    let output_frame = imaging_frequency_frame(law.output_frequency_reference)?;
    Ok(PreparedSpectralAxis {
        selected_source_channels: law.selected_source_channels,
        source_frame,
        output_frequency_reference: law.output_frequency_reference,
        output_frame,
        anchor: spectral_frame_anchor(source_frame, output_frame, frame)?,
        wcs: SpectralWcs::Linear {
            channels: law.output_channels,
            reference_pixel: 0.0,
            reference_frequency_hz: law.reference_frequency_hz,
            increment_hz: law.increment_hz,
        },
        rest_frequency: law.rest_frequency,
        image_rest_frequency_hz: law.image_rest_frequency_hz,
        doppler: law.doppler,
        sampling: law.sampling,
        basis: law.basis,
        output_channels: law.output_channels,
        reference_frequency_hz: law.reference_frequency_hz,
        increment_hz: law.increment_hz,
    })
}

/// One LSRK plane over the selected band's envelope.
fn continuum_law(survey: &Survey) -> Result<AxisLaw, ApplicationError> {
    let (channels, envelope) = survey
        .continuum
        .as_ref()
        .ok_or_else(|| boxed("continuum imaging requires the selected spectral envelope"))?;
    let [lower_hz, upper_hz] = envelope.edges_hz();
    let reference_frequency_hz = envelope.midpoint_hz();
    Ok(AxisLaw {
        selected_source_channels: channels.clone(),
        output_frequency_reference: FrequencyRef::LSRK,
        reference_frequency_hz,
        increment_hz: upper_hz - lower_hz,
        output_channels: 1,
        rest_frequency: RestFrequency::NotApplicable,
        image_rest_frequency_hz: reference_frequency_hz,
        doppler: DopplerConvention::NotApplicable,
        sampling: SpectralSamplingLaw::IDENTITY,
        basis: ReconstructionBasis::Constant,
    })
}

/// A cube in its output frame (`cube`) or the data's (`cubedata`).
fn cube_law(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    survey: &Survey,
    frame: &FrameContext<'_>,
) -> Result<AxisLaw, ApplicationError> {
    let window = one_window(&survey.spectral_windows, "native cube imaging")?;
    let axis = cube_axis(request)?;
    let output_channels = request.channel_count.unwrap_or(window.frequencies_hz.len());
    let (setup, support) = cube_setup(survey, window, output_channels, &axis, frame)?;
    let mut selected = support.indices;
    let explicit = explicit_spw_channels(request, window.spw_id, &window.frequencies_hz)?
        .map(|channels| channels.into_iter().collect::<BTreeSet<_>>());
    if let Some(explicit) = &explicit {
        selected.retain(|channel| explicit.contains(channel));
    }
    if selected.is_empty() {
        return Err(boxed(
            "cube axis and SPW selector have no common source channels",
        ));
    }
    let (reference_frequency_hz, increment_hz) = first_and_increment(&setup, output_channels, 1.0)?;
    let source_rest_hz = source_rest_frequency(ms, frame.field_id, &survey.spectral_windows)?;
    let (resolved_rest_hz, image_rest_frequency_hz) =
        cube_rest_frequency_hz(request.restfreq, source_rest_hz, window, explicit.as_ref());
    let (rest_frequency, doppler) = match resolved_rest_hz {
        None => (
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
        Some(hertz) => (
            RestFrequency::Line { hertz },
            doppler_convention(request.veltype)?,
        ),
    };
    Ok(AxisLaw {
        selected_source_channels: BTreeMap::from([(window.spw_id, selected)]),
        output_frequency_reference: setup.output_freq_ref,
        reference_frequency_hz,
        increment_hz,
        output_channels,
        rest_frequency,
        image_rest_frequency_hz,
        doppler,
        sampling: sampling_law(setup.interpolation),
        basis: ReconstructionBasis::ChannelLocal {
            channels: output_channels,
        },
    })
}

/// A cube in the rest frame of a moving source: the data-frame axis
/// scaled into the source's rest frame (`cubesource`).
fn source_frame_cube_law(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    survey: &Survey,
    frame: &FrameContext<'_>,
    moving_rest_frame: &MeasFrame,
) -> Result<AxisLaw, ApplicationError> {
    let window = one_window(&survey.spectral_windows, "source-frame cube imaging")?;
    let source_reference = survey.source_frequency_reference;
    let mut axis = cube_axis(request)?;
    axis.specmode = CubeSpecMode::Cubedata;
    axis.outframe = source_reference;
    let output_channels = request.channel_count.unwrap_or(window.frequencies_hz.len());
    let (setup, support) = cube_setup(survey, window, output_channels, &axis, frame)?;
    let factor = casa_ms::convert_frequency_to_frame_with_frame(
        source_reference,
        FrequencyRef::REST,
        1.0,
        Some(moving_rest_frame),
    )?;
    let (reference_frequency_hz, increment_hz) =
        first_and_increment(&setup, output_channels, factor)?;
    let rest_frequency_hz = request
        .restfreq
        .or(source_rest_frequency(
            ms,
            frame.field_id,
            &survey.spectral_windows,
        )?)
        .ok_or_else(|| boxed("source-frame cube imaging requires REST_FREQUENCY metadata"))?;
    Ok(AxisLaw {
        selected_source_channels: BTreeMap::from([(window.spw_id, support.indices)]),
        output_frequency_reference: FrequencyRef::REST,
        reference_frequency_hz,
        increment_hz,
        output_channels,
        rest_frequency: RestFrequency::Line {
            hertz: rest_frequency_hz,
        },
        image_rest_frequency_hz: rest_frequency_hz,
        doppler: doppler_convention(request.veltype)?,
        sampling: sampling_law(request.interpolation),
        basis: ReconstructionBasis::ChannelLocal {
            channels: output_channels,
        },
    })
}

/// CASA's cube axis of the request: `start` (else `channel_start`) and
/// `width` in the request's velocity convention.
fn cube_axis(request: &ImagingRequest) -> Result<CubeAxisConfig, ApplicationError> {
    let start = match (&request.start, request.channel_start) {
        (Some(start), _) => Some(CubeAxisValue::parse(start, request.veltype)?),
        (None, Some(channel)) => Some(CubeAxisValue::Channel(
            i32::try_from(channel).map_err(|_| boxed("cube channel start exceeds i32"))?,
        )),
        (None, None) => None,
    };
    let width = request
        .width
        .as_deref()
        .map(|width| CubeAxisValue::parse(width, request.veltype))
        .transpose()?;
    Ok(CubeAxisConfig {
        specmode: if request.specmode == SpecMode::Cubedata {
            CubeSpecMode::Cubedata
        } else {
            CubeSpecMode::Cube
        },
        outframe: request.outframe,
        veltype: request.veltype,
        interpolation: request.interpolation,
        rest_frequency_hz: request.restfreq,
        start,
        width,
    })
}

fn cube_setup(
    survey: &Survey,
    window: &SourceSpectralWindow,
    output_channels: usize,
    axis: &CubeAxisConfig,
    frame: &FrameContext<'_>,
) -> Result<(CubeSpectralSetup, casa_ms::ResolvedChannelSelection), ApplicationError> {
    Ok(CubeSpectralSetup::for_casa_cube_axis(
        survey.source_frequency_reference,
        &window.frequencies_hz,
        &window.channel_widths_hz,
        output_channels,
        axis,
        frame.anchor_time_mjd_seconds,
        frame.field_id,
        Some(frame.phase.clone()),
        frame.time_bounds_mjd_seconds,
        frame.engine,
    )?)
}

/// The first output channel's centre and the channel increment, scaled by
/// `factor`; a one-channel cube's increment is its width.
fn first_and_increment(
    setup: &CubeSpectralSetup,
    output_channels: usize,
    factor: f64,
) -> Result<(f64, f64), ApplicationError> {
    let first_hz = setup.output_channel_frequencies_hz[0];
    let increment_hz = if output_channels > 1 {
        setup.output_channel_frequencies_hz[1] - first_hz
    } else {
        setup.output_channel_widths_hz[0]
    } * factor;
    if !increment_hz.is_finite() || increment_hz == 0.0 {
        return Err(boxed(
            "cube output frequency increment must be finite and non-zero",
        ));
    }
    Ok((first_hz * factor, increment_hz))
}

/// The model's Doppler convention of CASA `veltype`.
fn doppler_convention(veltype: DopplerRef) -> Result<DopplerConvention, ApplicationError> {
    match veltype {
        DopplerRef::RADIO => Ok(DopplerConvention::Radio),
        DopplerRef::Z => Ok(DopplerConvention::Optical),
        DopplerRef::BETA => Ok(DopplerConvention::Relativistic),
        DopplerRef::RATIO | DopplerRef::GAMMA => {
            Err(boxed("cube Doppler convention is not supported"))
        }
    }
}

const fn sampling_law(interpolation: CubeInterpolation) -> SpectralSamplingLaw {
    match interpolation {
        CubeInterpolation::Nearest => SpectralSamplingLaw::NEAREST,
        CubeInterpolation::Linear => SpectralSamplingLaw::LINEAR,
        CubeInterpolation::Cubic => SpectralSamplingLaw::CUBIC,
    }
}

fn spectral_frame_anchor(
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
    frame: &FrameContext<'_>,
) -> Result<SpectralFrameAnchor, ApplicationError> {
    if source_frame == output_frame {
        return Ok(SpectralFrameAnchor::NotApplicable);
    }
    let [x_metres, y_metres, z_metres] = frame.engine.observatory_position().as_itrf();
    Ok(SpectralFrameAnchor::Conversion {
        epoch: Epoch::new(
            frame.anchor_time_mjd_seconds / 86_400.0,
            imaging_time_scale(frame.engine.time_reference())?,
        ),
        direction: frame.direction.reference_direction(),
        observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
    })
}

fn imaging_frequency_frame(reference: FrequencyRef) -> Result<FrequencyFrame, ApplicationError> {
    match reference {
        FrequencyRef::REST => Ok(FrequencyFrame::Rest),
        FrequencyRef::TOPO => Ok(FrequencyFrame::Topocentric),
        FrequencyRef::BARY => Ok(FrequencyFrame::Barycentric),
        FrequencyRef::LSRK => Ok(FrequencyFrame::Lsrk),
        _ => Err(boxed(format!(
            "native imaging does not support the frequency frame {reference}"
        ))),
    }
}

pub(super) fn imaging_time_scale(reference: EpochRef) -> Result<TimeScale, ApplicationError> {
    match reference {
        EpochRef::UTC => Ok(TimeScale::Utc),
        EpochRef::TAI => Ok(TimeScale::Tai),
        EpochRef::TT => Ok(TimeScale::Tt),
        EpochRef::TDB => Ok(TimeScale::Tdb),
        _ => Err(boxed(format!(
            "native imaging does not support the MeasurementSet epoch reference {reference}"
        ))),
    }
}

/// The `REST_FREQUENCY` the `SOURCE` table gives the field over the
/// selected windows, if any.
fn source_rest_frequency(
    measurement_set: &MeasurementSet,
    field_id: usize,
    spectral_windows: &[SourceSpectralWindow],
) -> Result<Option<f64>, ApplicationError> {
    let source_id = measurement_set.field()?.source_id(field_id)?;
    if source_id < 0 || measurement_set.subtable(SubtableId::Source).is_none() {
        return Ok(None);
    }
    let source = measurement_set.source()?;
    let selected = spectral_windows
        .iter()
        .map(|window| i32::try_from(window.spw_id))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut resolved = None;
    for row in 0..source.row_count() {
        if source.i32(row, "SOURCE_ID")? != source_id {
            continue;
        }
        let spectral_window_id = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        if spectral_window_id >= 0 && !selected.contains(&spectral_window_id) {
            continue;
        }
        let Some(ArrayValue::Float64(values)) = source.optional_array(row, "REST_FREQUENCY")?
        else {
            continue;
        };
        let Some(value) = values.first().copied().filter(|value| *value > 0.0) else {
            continue;
        };
        if resolved.is_some_and(|prior: f64| prior.to_bits() != value.to_bits()) {
            return Err(boxed(
                "selected SOURCE rows disagree on REST_FREQUENCY metadata",
            ));
        }
        resolved = Some(value);
    }
    Ok(resolved)
}

/// CASA's precedence for a cube's rest frequency: the request's, else the
/// source's; the header records the selected band's centre when neither
/// exists.
fn cube_rest_frequency_hz(
    explicit_hz: Option<f64>,
    source_hz: Option<f64>,
    spectral_window: &SourceSpectralWindow,
    selected_channels: Option<&BTreeSet<usize>>,
) -> (Option<f64>, f64) {
    let resolved = explicit_hz.or(source_hz);
    let image_hz = resolved.unwrap_or_else(|| {
        let (lower, upper) = spectral_window
            .frequencies_hz
            .iter()
            .zip(&spectral_window.channel_widths_hz)
            .enumerate()
            .filter(|(index, _)| selected_channels.is_none_or(|channels| channels.contains(index)))
            .map(|(_, (&centre, &width))| (centre - width.abs() / 2.0, centre + width.abs() / 2.0))
            .fold(
                (f64::INFINITY, f64::NEG_INFINITY),
                |(lower, upper), (lo, hi)| (lower.min(lo), upper.max(hi)),
            );
        lower + (upper - lower) / 2.0
    });
    (resolved, image_hz)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cube_rest_frequency_follows_casa_precedence() {
        let window = SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::LSRK,
            frequencies_hz: vec![44.0e9, 76.704e9, 109.408e9],
            channel_widths_hz: vec![1.0; 3],
        };
        assert_eq!(
            cube_rest_frequency_hz(Some(115.0e9), Some(110.0e9), &window, None),
            (Some(115.0e9), 115.0e9)
        );
        assert_eq!(
            cube_rest_frequency_hz(None, Some(110.0e9), &window, None),
            (Some(110.0e9), 110.0e9)
        );
        assert_eq!(
            cube_rest_frequency_hz(None, None, &window, None),
            (None, 76.704e9)
        );
    }
}
