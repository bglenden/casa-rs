// SPDX-License-Identifier: LGPL-3.0-or-later

//! The frequency conversions compile evaluates at the epochs of the
//! selected samples ([`Epoch`]): a continuum run's spectral envelope and a
//! cube's density padding.
//!
//! A cube's samples are converted towards their field's direction at
//! their time, as CASA's visibility iterator converts them; a field that
//! follows an ephemeris is converted towards its direction at the time, not
//! its `PHASE_DIR`, which is then an offset. A continuum envelope converts
//! towards each field's reference direction instead, as CASA's
//! `MSUtil::getFreqRangeInSpw` does ([`continuum_direction`]).

use std::collections::BTreeMap;

use casa_ms::derived::engine::MsCalEngine;
use casa_observation::Catalogue;
use casa_types::measures::PreparedFrequencyConversion;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequencyConverter};

use super::PrepareError;
use super::selection::{SourceSpectralWindow, WindowChannels};
use super::survey::{Epoch, Survey};

/// The selected channels' edges in the output frame, over every epoch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct SpectralEnvelope {
    edges_hz: [f64; 2],
}

impl SpectralEnvelope {
    /// The lower and upper selected-channel edges in the output frame.
    pub(super) const fn edges_hz(self) -> [f64; 2] {
        self.edges_hz
    }

    /// CASA's MFS and Taylor reference frequency: the midpoint of the
    /// edges.
    pub(super) fn midpoint_hz(self) -> f64 {
        (self.edges_hz[0] + self.edges_hz[1]) / 2.0
    }
}

/// The observatory frame towards `epoch`'s field at its time: a moving
/// field's direction then, else a fixed field's stored phase centre, in its
/// stored frame, as the deleted MAIN walk read `PHASE_DIR`.
fn epoch_frame(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    epoch: Epoch,
) -> Result<MeasFrame, PrepareError> {
    let field = usize::try_from(epoch.field_id).expect("a surveyed field is a FIELD row");
    let direction = if engine.field_moves(field) {
        engine.observation_direction_j2000(epoch.time_mjd_seconds, field)?
    } else {
        catalogue.fields[field].phase_centre.clone()
    };
    Ok(engine.spectral_frame_observatory_direction(epoch.time_mjd_seconds, direction)?)
}

/// The direction a continuum envelope converts field `field_id`'s samples
/// towards: `MSUtil::getFreqRangeInSpw` asks `phaseDirMeas(field)` with no
/// time, which for a field following an ephemeris is its geocentric
/// direction at its reference epoch (MSv2 `FIELD.TIME`; the field's first
/// selected time `first_time_mjd_seconds` on a store without one), in the
/// ephemeris frame and including `PHASE_DIR`'s offset
/// ([`MsCalEngine::moving_field_reference_direction`]); a fixed field's is
/// its stored phase centre. Only the conversion epoch follows the samples.
fn continuum_direction(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    field_id: i32,
    first_time_mjd_seconds: f64,
) -> Result<MDirection, PrepareError> {
    let field = usize::try_from(field_id).expect("a surveyed field is a FIELD row");
    Ok(
        match engine.moving_field_reference_direction(field, first_time_mjd_seconds)? {
            Some(direction) => direction,
            None => catalogue.fields[field].phase_centre.clone(),
        },
    )
}

/// The lower and upper edges of `channels` of `window`: each channel's
/// centre less and plus half its width.
fn selected_edges(
    window: &SourceSpectralWindow,
    channels: &[usize],
) -> Result<[f64; 2], PrepareError> {
    let invalid = || PrepareError::SpectralWindowCoordinates {
        spw_id: window.spw_id,
    };
    let mut edges = [f64::INFINITY, f64::NEG_INFINITY];
    for &channel in channels {
        let (Some(&centre), Some(&width)) = (
            window.frequencies_hz.get(channel),
            window.channel_widths_hz.get(channel),
        ) else {
            return Err(invalid());
        };
        if !(centre.is_finite() && width.is_finite() && width != 0.0) {
            return Err(invalid());
        }
        edges[0] = edges[0].min(centre - width.abs() / 2.0);
        edges[1] = edges[1].max(centre + width.abs() / 2.0);
    }
    if edges[0].is_finite() && edges[1] > edges[0] {
        Ok(edges)
    } else {
        Err(invalid())
    }
}

/// The envelope in frame `output` of the `selected` channels of `windows`
/// over each window's epochs in `survey`: LSRK, where CASA grids an MFS
/// image's samples, or the image's `outframe`, which its header records.
///
/// It follows CASA's `MSUtil::getFreqRangeInSpw` (`fromEdge`), which
/// `SynthesisUtilMethods::buildCoordinateSystem` uses for the MFS spectral
/// axis: each window converts its selected channel edges, low edge first,
/// at each epoch of its samples in time order, through **one**
/// `MFrequency::Convert`, whose aberration and dUT1 are anchored at the
/// first epoch and extrapolated across 0.04 days
/// ([`MFrequencyConverter`]); that moves the far end of a long track by
/// about 1e-12. CASA keys its epochs by `TIME·(DATA_DESC_ID+1)`, each
/// standing for the earliest row with the key; a window's samples at one
/// time towards one field are one epoch here. CASA takes `REST` edges, and
/// edges already in a frame other than `TOPO` that is the output's, as
/// they are.
pub(super) fn continuum_envelope(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
    selected: &WindowChannels,
    output: FrequencyRef,
) -> Result<SpectralEnvelope, PrepareError> {
    let mut edges_hz = [f64::INFINITY, f64::NEG_INFINITY];
    // Each field's direction, once: only the conversion epoch varies.
    let mut directions = BTreeMap::<i32, MDirection>::new();
    let mut fold = |hz: f64| {
        edges_hz[0] = edges_hz[0].min(hz);
        edges_hz[1] = edges_hz[1].max(hz);
    };
    for window in &survey.spectral_windows {
        let channels = selected
            .get(&window.spw_id)
            .expect("the continuum selects channels of every selected window");
        let [lower_hz, upper_hz] = selected_edges(window, channels)?;
        let source = window.frequency_reference;
        if source == FrequencyRef::REST || (source == output && source != FrequencyRef::TOPO) {
            fold(lower_hz);
            fold(upper_hz);
            continue;
        }
        let mut converter = MFrequencyConverter::new(source, output);
        for epoch in survey.window_epochs(window.spw_id) {
            let direction = match directions.get(&epoch.field_id) {
                Some(direction) => direction.clone(),
                None => {
                    // Only a moving field without its own reference epoch
                    // (an MSv4's) reads its first selected time; finding it
                    // scans every window's epochs, so fixed fields skip it.
                    let field =
                        usize::try_from(epoch.field_id).expect("a surveyed field is a FIELD row");
                    let first = if engine.field_moves(field) {
                        survey
                            .first_time_of(epoch.field_id)
                            .unwrap_or(epoch.time_mjd_seconds)
                    } else {
                        epoch.time_mjd_seconds
                    };
                    let direction =
                        continuum_direction((catalogue, engine), epoch.field_id, first)?;
                    directions.insert(epoch.field_id, direction.clone());
                    direction
                }
            };
            let frame =
                engine.spectral_frame_observatory_direction(epoch.time_mjd_seconds, direction)?;
            fold(converter.convert_hz(lower_hz, &frame)?.hz());
            fold(converter.convert_hz(upper_hz, &frame)?.hz());
        }
    }
    if !(edges_hz[0].is_finite() && edges_hz[1].is_finite() && edges_hz[1] > edges_hz[0]) {
        return Err(PrepareError::SpectralEnvelope { edges_hz });
    }
    Ok(SpectralEnvelope { edges_hz })
}

/// Each selected window's factor from its frame to LSRK at the first
/// visibility buffer CASA's tclean iterates of it, by window number.
///
/// `SynthesisImagerVi2::createVisSet` iterates by `ARRAY_ID`,
/// `DATA_DESC_ID`, `FIELD_ID` and `TIME`, so a window's first buffer is
/// its least selected field at that field's earliest selected time
/// ([`Epoch`]; one array). `FTMachine::matchChannel` converts the buffer's
/// frequencies to LSRK there, towards the field, whatever frame the image
/// records, and `HetArrayConvFunc::findConvFunction` evaluates a mosaic's
/// beams at them once for the window. A window in `REST`, or already in
/// LSRK, keeps its frequencies (factor 1), as `matchChannel` keeps a rest
/// frame's.
pub(super) fn first_buffer_lsrk_factors(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
) -> Result<BTreeMap<usize, f64>, PrepareError> {
    let mut factors = BTreeMap::new();
    for window in &survey.spectral_windows {
        let source = window.frequency_reference;
        let first = survey
            .window_epochs(window.spw_id)
            .iter()
            .copied()
            .min_by(|a, b| {
                a.field_id
                    .cmp(&b.field_id)
                    .then(a.time_mjd_seconds.total_cmp(&b.time_mjd_seconds))
            });
        let factor = match first {
            Some(epoch) if !matches!(source, FrequencyRef::REST | FrequencyRef::LSRK) => {
                let frame = epoch_frame((catalogue, engine), epoch)?;
                PreparedFrequencyConversion::new(
                    source,
                    FrequencyRef::LSRK,
                    Some(&frame),
                    Some(&frame),
                )?
                .convert_hz(1.0)
            }
            _ => 1.0,
        };
        factors.insert(window.spw_id, factor);
    }
    Ok(factors)
}

/// A cube's output axis: its frame, the centres of its first and last
/// channels, their count and the image's channel increment.
pub(super) struct OutputAxis {
    pub(super) frequency_reference: FrequencyRef,
    pub(super) centres_hz: [f64; 2],
    pub(super) channels: usize,
    pub(super) increment_hz: f64,
}

/// CASA's nominal Briggs-cube density padding, per side
/// (`BriggsCubeWeightor::estimateSwingChanPad`, half its `swingpad`).
///
/// CASA walks its visibility buffers in `DATA_DESC_ID`, then `FIELD_ID`
/// order (one array; windows here in window order) and takes a window's
/// range whenever the field changes from the buffer before, so with one
/// field only the first window counts. For each such (window, field), the
/// field's output interval is converted back to the window's frame at the
/// epochs of its samples, then matched against the whole window with
/// CASA's strict channel-edge overlap; a window it misses adds nothing.
/// The output interval is widened by half its mean channel step,
/// `|last − first| / max(nchan − 1, 1)`, which is zero for one channel.
/// The padding is the largest swing of the matched ranges' ends plus the
/// largest offset between the windows' lowest channels, in the image's
/// channel increment rounded up, plus `max(min(4, nchan/10), 1)`, so it is
/// at least one plane even when the frames agree. The selected channels'
/// interpolation support does not redefine this nominal density domain.
pub(super) fn cube_density_padding(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
    output: &OutputAxis,
) -> Result<usize, PrepareError> {
    for window in &survey.spectral_windows {
        if window.frequencies_hz.is_empty()
            || window.frequencies_hz.len() != window.channel_widths_hz.len()
            || window
                .frequencies_hz
                .iter()
                .any(|frequency| !frequency.is_finite() || *frequency <= 0.0)
            || window
                .channel_widths_hz
                .iter()
                .any(|width| !width.is_finite() || *width == 0.0)
        {
            return Err(PrepareError::SpectralWindowCoordinates {
                spw_id: window.spw_id,
            });
        }
    }
    let extra_padding = (output.channels / 10).clamp(1, 4);
    if survey.source_frequency_reference == output.frequency_reference {
        return Ok(extra_padding);
    }
    let [first, last] = output.centres_hz;
    let step = (last - first).abs() / output.channels.saturating_sub(1).max(1) as f64;
    let increment = output.increment_hz.abs();
    if !(step.is_finite() && increment.is_finite() && increment > 0.0) {
        return Err(PrepareError::CubeIncrement {
            increment_hz: output.increment_hz,
        });
    }
    let interval = [first.min(last), first.max(last)];
    let mut lower_centres = [f64::INFINITY, f64::NEG_INFINITY];
    let mut upper_centres = [f64::INFINITY, f64::NEG_INFINITY];
    let mut lowest_channels = [f64::INFINITY, f64::NEG_INFINITY];
    let mut previous_field = None;
    for window in &survey.spectral_windows {
        let mut fields = survey
            .window_epochs(window.spw_id)
            .iter()
            .map(|epoch| epoch.field_id)
            .collect::<Vec<_>>();
        fields.sort_unstable();
        fields.dedup();
        for field in fields {
            if previous_field.replace(field) == Some(field) {
                continue;
            }
            let Some([a, b]) = matched_range(
                (catalogue, engine),
                survey,
                (window, field),
                output.frequency_reference,
                interval,
                step,
            )?
            else {
                continue;
            };
            lower_centres[0] = lower_centres[0].min(a.min(b));
            lower_centres[1] = lower_centres[1].max(a.min(b));
            upper_centres[0] = upper_centres[0].min(a.max(b));
            upper_centres[1] = upper_centres[1].max(a.max(b));
            let lowest = window
                .frequencies_hz
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            lowest_channels[0] = lowest_channels[0].min(lowest);
            lowest_channels[1] = lowest_channels[1].max(lowest);
        }
    }
    if !lower_centres[0].is_finite() {
        return Ok(extra_padding);
    }
    let swing = (lower_centres[1] - lower_centres[0]).max(upper_centres[1] - upper_centres[0]);
    let first_channel_shift = lowest_channels[1] - lowest_channels[0];
    let swing_channels = ((swing + first_channel_shift) / increment).ceil();
    if !(swing_channels.is_finite() && swing_channels < usize::MAX as f64) {
        return Err(PrepareError::CubeIncrement {
            increment_hz: increment,
        });
    }
    Ok(swing_channels as usize + extra_padding)
}

/// The centres of the first and last channels of `window` that the output
/// `interval` in frame `output_reference`, converted back to the window's
/// frame at field `field`'s epochs in it and widened by half a `step`,
/// overlaps (`MSUtil::getSpwInFreqRange`), or `None` when it overlaps
/// none.
fn matched_range(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
    (window, field): (&SourceSpectralWindow, i32),
    output_reference: FrequencyRef,
    interval: [f64; 2],
    step: f64,
) -> Result<Option<[f64; 2]>, PrepareError> {
    let native_min = window
        .frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let native_max = window
        .frequencies_hz
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let mut native = [f64::INFINITY, f64::NEG_INFINITY];
    for epoch in survey
        .window_epochs(window.spw_id)
        .iter()
        .filter(|epoch| epoch.field_id == field)
    {
        let frame = epoch_frame((catalogue, engine), *epoch)?;
        let conversion = PreparedFrequencyConversion::new(
            output_reference,
            window.frequency_reference,
            Some(&frame),
            Some(&frame),
        )?;
        let a = conversion.convert_hz(interval[0]);
        let b = conversion.convert_hz(interval[1]);
        native[0] = native[0].min(a.min(b));
        native[1] = native[1].max(a.max(b));
    }
    let lower = native[0] - 0.5 * step;
    let upper = native[1] + 0.5 * step;
    let partial =
        (lower > native_min && lower < native_max) || (upper > native_min && upper < native_max);
    let full = lower < native_min && upper > native_max;
    if !partial && !full {
        return Ok(None);
    }
    let mut matched = window
        .frequencies_hz
        .iter()
        .zip(&window.channel_widths_hz)
        .enumerate()
        .filter(|(_, (centre, width))| {
            full || (**centre + 0.5 * width.abs() > lower && **centre - 0.5 * width.abs() < upper)
        })
        .map(|(channel, _)| channel);
    let Some(first) = matched.next() else {
        return Ok(None);
    };
    let last = matched.next_back().unwrap_or(first);
    Ok(Some([
        window.frequencies_hz[first],
        window.frequencies_hz[last],
    ]))
}

#[cfg(test)]
mod tests {
    use casa_types::measures::frequency::FrequencyRef;

    use super::selected_edges;
    use crate::compile::selection::SourceSpectralWindow;

    #[test]
    fn the_envelope_takes_the_exact_edges_of_nonuniform_selected_channels() {
        let window = SourceSpectralWindow {
            spw_id: 7,
            frequency_reference: FrequencyRef::TOPO,
            frequencies_hz: vec![100.0, 113.0, 151.0, 220.0],
            channel_widths_hz: vec![8.0, -10.0, 14.0, 20.0],
        };
        let [lower, upper] = selected_edges(&window, &[1, 3]).expect("nonuniform selected edges");
        assert_eq!(lower, 108.0);
        assert_eq!(upper, 230.0);
        assert_eq!(
            (lower + upper) / 2.0,
            169.0,
            "the Taylor reference is the selected edge-envelope midpoint, not the mean channel centre"
        );
    }
}
