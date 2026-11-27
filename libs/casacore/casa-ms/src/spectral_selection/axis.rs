// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn channel_edges_from_centers(all_source_frequencies_hz: &[f64]) -> MsResult<Vec<f64>> {
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

pub(super) fn channel_widths_from_centers(channel_frequencies_hz: &[f64]) -> MsResult<Vec<f64>> {
    let edges = channel_edges_from_centers(channel_frequencies_hz)?;
    Ok(edges
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).abs())
        .collect())
}

pub(super) fn channel_mode_single_output_width_hz(
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

pub(super) fn casa_regridding_input_widths(
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

/// CASA's channel-mode `chanFreq` vector: the regridder's output channel
/// centres, ordered as `SynthesisParamsImage::getImFreq` leaves them after
/// its final reversal.
///
/// This is a step-for-step port of `MSTransformRegridder::regridChanBounds`
/// and the `(lo + hi)/2` centres of `MSTransformRegridder::calcChanFreqs`.
/// These are not the frequencies CASA grids on:
/// [`casa_image_spectral_axis_hz`] turns them into the image spectral axis.
/// An unset `nchan` (`-1`) counts the output channels as `regridChanBounds`
/// does when only the start is set: the span from the start channel's edge
/// to the band's far edge in whole output widths, with a 1% tolerance.
pub(super) fn casa_transformed_channel_mode_output_centers(
    transformed_source_frequencies_hz: &[f64],
    transformed_source_channel_widths_hz: &[f64],
    start_channel: i32,
    width_channels: i32,
    nchan: Option<usize>,
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
    let first_channel = usize::try_from(first_channel).expect("validated channel index");
    let nchan = match nchan {
        Some(nchan) => nchan,
        None => {
            let last = transformed_frequencies_hz.len() - 1;
            let span_hz = if start_is_end {
                transformed_frequencies_hz[first_channel]
                    + transformed_widths_hz[first_channel] / 2.0
                    - (transformed_frequencies_hz[0] - transformed_widths_hz[0] / 2.0)
            } else {
                transformed_frequencies_hz[last] + transformed_widths_hz[last] / 2.0
                    - (transformed_frequencies_hz[first_channel]
                        - transformed_widths_hz[first_channel] / 2.0)
            };
            let count =
                ((span_hz + output_channel_width_hz * 0.01) / output_channel_width_hz).floor();
            if !(count >= 1.0 && count < u32::MAX as f64) {
                return Err(MsError::VersionError(format!(
                    "no output channel of {output_channel_width_hz} Hz fits from cube channel \
                     start {start_channel}"
                )));
            }
            count as usize
        }
    };
    let output_bandwidth_hz = output_channel_width_hz * nchan as f64;
    // CASA `MSTransformRegridder::regridChanBounds` ("freq" regridding)
    // moves the start edge to the band centre, steps the channel bounds out
    // from the centre channel by repeated subtraction and addition of the
    // width, and `calcChanFreqs` takes each centre as `(lo + hi)/2`. The
    // rounding of the first two centres fixes the image axis, and so
    // whether an output channel lies exactly on a transformed source
    // channel, which `FTMachine::interpolateFrequencyTogrid` keeps and a
    // frequency one ulp below it discards.
    let centre_hz = if start_is_end {
        transformed_frequencies_hz[first_channel] + transformed_widths_hz[first_channel] / 2.0
            - output_bandwidth_hz / 2.0
    } else {
        transformed_frequencies_hz[first_channel] - transformed_widths_hz[first_channel] / 2.0
            + output_bandwidth_hz / 2.0
    };
    // An even channel count puts the band centre on the lower edge of the
    // centre channel, an odd one on its centre.
    let centre_channel = if nchan % 2 == 1 {
        (
            centre_hz - output_channel_width_hz / 2.0,
            centre_hz + output_channel_width_hz / 2.0,
        )
    } else {
        (centre_hz, centre_hz + output_channel_width_hz)
    };
    let below = nchan / 2;
    let mut bounds_hz = vec![centre_channel; nchan];
    for index in (0..below).rev() {
        let high_hz = bounds_hz[index + 1].0;
        bounds_hz[index] = (high_hz - output_channel_width_hz, high_hz);
    }
    for index in below + 1..nchan {
        let low_hz = bounds_hz[index - 1].1;
        bounds_hz[index] = (low_hz, low_hz + output_channel_width_hz);
    }
    let mut centers_hz = bounds_hz
        .iter()
        .map(|(low_hz, high_hz)| (low_hz + high_hz) / 2.0)
        .collect::<Vec<_>>();
    if descending ^ (width_channels < 0) {
        centers_hz.reverse();
    }
    Ok(centers_hz)
}

/// The image spectral axis CASA builds from its `chanFreq` vector and grids
/// on.
///
/// `SynthesisParamsImage::buildCoordinateSystemCore` makes a linear
/// `SpectralCoordinate` with `crval = chanFreq[0]`,
/// `cdelt = chanFreq[1] − chanFreq[0]` and the reference pixel 0 set by
/// `getImFreq`, and `FTMachine::initMaps` takes plane `k` at
/// `toWorld(k)`. Only the first two entries of `chan_freqs_hz` survive; the
/// rest of the axis is [`linear_spectral_axis_hz`], which can differ from
/// the remaining `chanFreq` entries by a few ulps.
///
/// For a non-linear velocity convention (`OPTICAL`/`Z`, `BETA`/`RELATIVISTIC`
/// or `GAMMA`) `buildCoordinateSystemCore` makes a tabular coordinate from
/// `chanFreq` instead, which reproduces the table at integer pixels up to the
/// rounding of its channel corrector; the table is returned unchanged.
pub(super) fn casa_image_spectral_axis_hz(
    chan_freqs_hz: Vec<f64>,
    veltype: DopplerRef,
) -> Vec<f64> {
    if casa_tabular_convention(veltype) || chan_freqs_hz.len() < 2 {
        return chan_freqs_hz;
    }
    let reference_hz = chan_freqs_hz[0];
    linear_spectral_axis_hz(
        reference_hz,
        chan_freqs_hz[1] - reference_hz,
        chan_freqs_hz.len(),
    )
}

/// Whether `buildCoordinateSystemCore` makes a tabular spectral coordinate
/// for `veltype`: the non-linear conventions `OPTICAL`/`Z`,
/// `BETA`/`RELATIVISTIC` and `GAMMA`.
pub(super) fn casa_tabular_convention(veltype: DopplerRef) -> bool {
    matches!(
        veltype,
        DopplerRef::Z | DopplerRef::BETA | DopplerRef::GAMMA
    )
}

/// CASA's frequency- or velocity-mode grid for `axis_config`
/// (`SynthesisParamsImage::findSpecMode` and
/// `MSTransformRegridder::convertGridPars`).
///
/// A velocity `start` or `width` makes a velocity grid, spaced in optical
/// velocity for `veltype='optical'` and in radio velocity otherwise, since
/// `convertGridPars` turns every other convention into radio; a frequency
/// one makes a frequency grid. Like `SynthesisParamsImage`, this refuses a
/// `start` and `width` of different kinds. A dimensionless Doppler value is
/// taken as the velocity it stands for in the grid's convention.
pub(super) fn casa_regrid_request(
    axis_config: &CubeAxisConfig,
    nchan: Option<usize>,
) -> MsResult<regrid::RegridRequest> {
    let kind = |value: &Option<CubeAxisValue>| match value {
        None => None,
        Some(CubeAxisValue::Channel(_)) => Some("channel"),
        Some(CubeAxisValue::FrequencyHz { .. }) => Some("frequency"),
        Some(CubeAxisValue::VelocityMs { .. } | CubeAxisValue::Doppler { .. }) => Some("velocity"),
    };
    let quantity = match (kind(&axis_config.start), kind(&axis_config.width)) {
        (Some(start), Some(width)) if start != width => {
            return Err(MsError::VersionError(format!(
                "Cannot mix start and width with different unit types: a {start} start and a \
                 {width} width"
            )));
        }
        (Some("frequency"), _) | (_, Some("frequency")) => regrid::RegridQuantity::Frequency,
        (Some("velocity"), _) | (_, Some("velocity")) if axis_config.veltype == DopplerRef::Z => {
            regrid::RegridQuantity::OpticalVelocity
        }
        (Some("velocity"), _) | (_, Some("velocity")) => regrid::RegridQuantity::RadioVelocity,
        _ => {
            return Err(MsError::VersionError(
                "a frequency or velocity cube axis needs a frequency or velocity start or width"
                    .to_string(),
            ));
        }
    };
    let convention = match quantity {
        regrid::RegridQuantity::OpticalVelocity => DopplerRef::Z,
        _ => DopplerRef::RADIO,
    };
    let in_grid_unit = |value: &CubeAxisValue| match value {
        CubeAxisValue::FrequencyHz { hz, .. } => Ok(*hz),
        CubeAxisValue::VelocityMs { ms, .. } => Ok(*ms),
        CubeAxisValue::Doppler {
            value,
            convention: from,
        } => MDoppler::new(*value, *from)
            .convert_to(convention, &MeasFrame::new())
            .map(|doppler| doppler.value() * SPEED_OF_LIGHT_M_S)
            .map_err(|error| {
                MsError::VersionError(format!(
                    "convert cube-axis doppler value {value} to {convention}: {error}"
                ))
            }),
        CubeAxisValue::Channel(channel) => Err(MsError::VersionError(format!(
            "expected a frequency or velocity cube axis value, found channel {channel}"
        ))),
    };
    let rest_frequency_hz = if quantity == regrid::RegridQuantity::Frequency {
        0.0
    } else {
        required_rest_frequency_hz(axis_config.rest_frequency_hz, &axis_config.start)?
    };
    Ok(regrid::RegridRequest {
        quantity,
        start: axis_config.start.as_ref().map(in_grid_unit).transpose()?,
        width: axis_config.width.as_ref().map(in_grid_unit).transpose()?,
        nchan,
        rest_frequency_hz,
    })
}

/// The `nchan` planes of the image spectral axis
/// `SynthesisParamsImage::buildCoordinateSystemCore` makes from a
/// frequency- or velocity-mode `chanFreq` vector; `None` (tclean's
/// `nchan=-1`) takes as many planes as `chanFreq` holds (`getImFreq`).
///
/// For a linear convention it is a linear coordinate with
/// `crval = chanFreq[0]` and `cdelt = chanFreq[1] − chanFreq[0]`
/// (`chanFreqStep[0]` for a single channel), however many channels the
/// regridder made. For a non-linear convention
/// ([`casa_tabular_convention`]) CASA makes a tabular coordinate over
/// `chanFreq`, whose header records `chanFreq[0]` and the table's mean step
/// `(chanFreq[n−1] − chanFreq[0])/(n − 1)`. The optical-velocity grid
/// retains each centre and extrapolates additional planes by the last
/// table interval. The other conventions use a uniform frequency grid.
pub(super) fn casa_regridded_image_axis_hz(
    channels: &regrid::CasaChannelFrequencies,
    request: &regrid::RegridRequest,
    veltype: DopplerRef,
) -> Vec<f64> {
    let centres_hz = &channels.centres_hz;
    let nchan = request.nchan.unwrap_or(centres_hz.len());
    let first_hz = centres_hz[0];
    let last = centres_hz.len() - 1;
    if request.quantity == regrid::RegridQuantity::OpticalVelocity && last > 0 {
        let step = centres_hz[last] - centres_hz[last - 1];
        return (0..nchan)
            .map(|pixel| {
                if pixel <= last {
                    centres_hz[pixel]
                } else {
                    centres_hz[last] + (pixel - last) as f64 * step
                }
            })
            .collect();
    }
    let increment_hz = if last == 0 {
        channels.steps_hz[0]
    } else if casa_tabular_convention(veltype) {
        (centres_hz[last] - first_hz) / last as f64
    } else {
        centres_hz[1] - first_hz
    };
    linear_spectral_axis_hz(first_hz, increment_hz, nchan)
}

/// Frequencies of `nchan` planes on a linear spectral axis whose reference
/// pixel is 0.
///
/// This is casacore `SpectralCoordinate::toWorld` for a linear `FREQ` axis
/// in Hz: wcslib's `linp2x` forms `cdelt·(k − crpix)` and `spcx2s` adds
/// `crval`, so plane `k` lies at `reference_hz + k·increment_hz` rounded
/// after the product and after the sum.
pub(super) fn linear_spectral_axis_hz(
    reference_hz: f64,
    increment_hz: f64,
    nchan: usize,
) -> Vec<f64> {
    (0..nchan)
        .map(|pixel| reference_hz + pixel as f64 * increment_hz)
        .collect()
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
