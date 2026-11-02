// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's output channels for a cube whose `start` or `width` is a
//! frequency or a velocity.
//!
//! This is a step-for-step port of the real-number path of
//! `MSTransformRegridder::calcChanFreqs` (casatools
//! `mstransform/MSTransform/MSTransformRegridder.cc`, CASA 6.7.5): the
//! frequency and velocity modes of `convertGridPars` (~line 943), the
//! `"freq"`, `"vrad"` and `"vopt"` branches of `regridChanBounds`
//! (~lines 1190–2740) and the `(lo + hi)/2` channel centres of
//! `calcChanFreqs` (~line 721), followed by the reordering
//! `SynthesisParamsImage::getImFreq` applies (`SynthesisUtilMethods.cc`
//! ~line 3520).
//!
//! The rule keeps `start` exact rather than snapping it to a data channel:
//! the regridder only moves the grid when its band centre (frequency mode:
//! its starting edge) falls outside the data's edges, where
//! `regridChanBounds` resets it to the nearer edge ("Keep center in
//! limits"). An optical-velocity grid may also be truncated to the data's
//! band, so it can hold fewer channels than `nchan`.
//!
//! casacore evaluates these steps in `lDouble`, which is `double` on the
//! arm64 hosts the CASA oracle runs on; the port evaluates them in `f64`
//! in the same order.

use super::SPEED_OF_LIGHT_M_S as C;
use crate::error::{MsError, MsResult};

/// `convertGridPars`'s `t_cstart` for an unset `start`.
const UNSET_START: f64 = -9e99;

/// The quantity in which CASA spaces the output channels equally
/// (`convertGridPars`'s `t_regridQuantity`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegridQuantity {
    /// `"freq"`: equal frequency steps; `start` and `width` in Hz.
    Frequency,
    /// `"vrad"`: equal radio-velocity steps; `start` and `width` in m/s.
    RadioVelocity,
    /// `"vopt"`: equal optical-velocity steps; `start` and `width` in m/s.
    OpticalVelocity,
}

impl RegridQuantity {
    const fn is_velocity(self) -> bool {
        matches!(self, Self::RadioVelocity | Self::OpticalVelocity)
    }
}

/// A frequency- or velocity-mode cube grid as tclean asks for it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct RegridRequest {
    /// Quantity the channels are equally spaced in.
    pub(super) quantity: RegridQuantity,
    /// Centre of the first channel (Hz or m/s); `None` for `start=''`.
    pub(super) start: Option<f64>,
    /// Signed channel width (Hz or m/s); `None` for `width=''`.
    pub(super) width: Option<f64>,
    /// Requested number of output channels (`nchan`), at least one.
    pub(super) nchan: usize,
    /// Rest frequency in Hz; required for a velocity quantity.
    pub(super) rest_frequency_hz: f64,
}

impl RegridRequest {
    /// `convertGridPars`'s `t_startIsEnd`: whether `start` names the
    /// highest-frequency channel. A negative frequency width, and a
    /// non-negative or unset velocity width, put `start` at the high end.
    fn start_is_end(&self) -> bool {
        match self.quantity {
            RegridQuantity::Frequency => self.width.is_some_and(|width| width < 0.0),
            _ => self.width.is_none_or(|width| width >= 0.0),
        }
    }

    /// `getImFreq`'s `descendingfreq`: whether the image axis runs down in
    /// frequency. It reads the sign of the `width` text: a frequency width
    /// starting with `-`, or a velocity width not starting with `-`.
    fn descending_axis(&self) -> bool {
        let negative = self.width.is_some_and(|width| width.is_sign_negative());
        match self.quantity {
            RegridQuantity::Frequency => negative,
            _ => !negative,
        }
    }

    /// `regridChanBounds`'s `regridChanWidth`: `|width|`, or `-1` unset.
    fn channel_width(&self) -> f64 {
        self.width.map_or(-1.0, f64::abs)
    }
}

/// CASA's `chanFreq` and `chanFreqStep` vectors as `getImFreq` leaves
/// them for `buildCoordinateSystemCore`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CasaChannelFrequencies {
    /// Output channel centres in Hz (`chanFreq`); the image axis starts at
    /// the first.
    pub(super) centres_hz: Vec<f64>,
    /// Output channel widths in Hz (`chanFreqStep`, `hi − lo`), negated
    /// when `getImFreq` reverses `chanFreq`; a one-channel image's
    /// increment.
    pub(super) steps_hz: Vec<f64>,
}

/// CASA's output channels for `request` over the data channels
/// `frequencies_hz` with widths `widths_hz`, both already in the output
/// frame (`calcChanFreqs`'s `transNewXin` and `transCHAN_WIDTH`).
pub(super) fn casa_channel_frequencies(
    request: &RegridRequest,
    frequencies_hz: &[f64],
    widths_hz: &[f64],
) -> MsResult<CasaChannelFrequencies> {
    let bounds = regrid_chan_bounds(request, frequencies_hz, widths_hz)?;
    let mut centres_hz = bounds
        .iter()
        .map(|(low_hz, high_hz)| (low_hz + high_hz) / 2.0)
        .collect::<Vec<_>>();
    let mut steps_hz = bounds
        .iter()
        .map(|(low_hz, high_hz)| high_hz - low_hz)
        .collect::<Vec<_>>();
    // `getImFreq` reverses `chanFreq` when the order it wants differs from
    // the order the regridder returned, and negates `chanFreqStep`.
    let descending_out = centres_hz.first() > centres_hz.last();
    if request.descending_axis() != descending_out {
        centres_hz.reverse();
        steps_hz.iter_mut().for_each(|step_hz| *step_hz = -*step_hz);
    }
    Ok(CasaChannelFrequencies {
        centres_hz,
        steps_hz,
    })
}

/// The data channels `regridChanBounds` works on, ascending in frequency.
struct Band {
    frequencies_hz: Vec<f64>,
    widths_hz: Vec<f64>,
}

impl Band {
    fn first(&self) -> (f64, f64) {
        (self.frequencies_hz[0], self.widths_hz[0])
    }

    fn last(&self) -> (f64, f64) {
        let last = self.frequencies_hz.len() - 1;
        (self.frequencies_hz[last], self.widths_hz[last])
    }

    /// `transNewXin[0] - transCHAN_WIDTH[0] / 2.`
    fn lower_edge_hz(&self) -> f64 {
        let (frequency_hz, width_hz) = self.first();
        frequency_hz - width_hz / 2.0
    }

    /// `transNewXin[n - 1] + transCHAN_WIDTH[n - 1] / 2.`
    fn upper_edge_hz(&self) -> f64 {
        let (frequency_hz, width_hz) = self.last();
        frequency_hz + width_hz / 2.0
    }
}

/// `regridChanBounds`'s grid once converted to frequency.
#[derive(Debug, Clone, Copy)]
struct FrequencyGrid {
    /// `regridCenterF`; negative when unset.
    centre_hz: f64,
    /// `regridBandwidthF`; non-positive when unset.
    bandwidth_hz: f64,
    /// `regridChanWidthF`; non-positive when unset.
    channel_width_hz: f64,
    /// `centerIsStart`: `centre_hz` is still the grid's starting edge.
    centre_is_start: bool,
    /// `startIsEnd`.
    start_is_end: bool,
    /// `nchan`, which the optical-velocity branch spends (sets to 0).
    nchan: usize,
    /// `regridCenter` after its half-channel shift, in the request's unit.
    start: f64,
}

/// The `(lo, hi)` bounds of CASA's output channels, in the data's order
/// (`regridChanBounds`'s real-number path; `regridBandwidth` is unset and
/// `nchan` positive for every tclean grid, and those branches are left
/// out).
fn regrid_chan_bounds(
    request: &RegridRequest,
    frequencies_hz: &[f64],
    widths_hz: &[f64],
) -> MsResult<Vec<(f64, f64)>> {
    let (band, descending) = ascending_band(frequencies_hz, widths_hz)?;
    if request.nchan == 0 {
        return Err(MsError::VersionError(
            "cube output channel count must be positive".to_string(),
        ));
    }
    let rest_hz = request.rest_frequency_hz;
    if request.quantity.is_velocity() && !(rest_hz > 0.0 && rest_hz <= 1e30) {
        return Err(MsError::VersionError(format!(
            "a velocity cube axis needs a valid rest frequency, got {rest_hz} Hz"
        )));
    }
    let grid = match request.quantity {
        RegridQuantity::Frequency => frequency_grid(request, &band),
        RegridQuantity::RadioVelocity => radio_velocity_grid(request, &band),
        RegridQuantity::OpticalVelocity => optical_velocity_grid(request, &band),
    };
    let (centre_hz, bandwidth_hz) = possible_centre_and_bandwidth(request.quantity, &grid, &band);
    let (central_width_hz, velocity_width) =
        central_channel_width(request, &grid, &band, bandwidth_hz)?;
    if !(central_width_hz.is_finite() && central_width_hz > 0.0) {
        return Err(MsError::VersionError(format!(
            "cube output channel width must be positive, got {central_width_hz} Hz"
        )));
    }
    let span = Span {
        centre_hz,
        bandwidth_hz,
        central_width_hz,
    };
    let (up, down) = match request.quantity {
        RegridQuantity::Frequency => frequency_bounds(&span),
        RegridQuantity::RadioVelocity => {
            velocity_bounds(&span, velocity_width, &RADIO_VELOCITY, rest_hz)?
        }
        RegridQuantity::OpticalVelocity => {
            velocity_bounds(&span, velocity_width, &OPTICAL_VELOCITY, rest_hz)?
        }
    };
    // The central channel opens both lists.
    let mut bounds = down
        .into_iter()
        .rev()
        .chain(up.into_iter().skip(1))
        .collect::<Vec<_>>();
    if descending {
        bounds.reverse();
    }
    Ok(bounds)
}

/// The data channels in ascending order and whether they were descending;
/// `regridChanBounds` refuses channels in neither order.
fn ascending_band(frequencies_hz: &[f64], widths_hz: &[f64]) -> MsResult<(Band, bool)> {
    if frequencies_hz.is_empty() || frequencies_hz.len() != widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "CASA cube regridding needs matching non-empty frequency/width arrays, got {} and {}",
            frequencies_hz.len(),
            widths_hz.len()
        )));
    }
    let mut descending = false;
    for pair in frequencies_hz.windows(2) {
        if pair[1] < pair[0] {
            descending = true;
        } else if descending {
            return Err(MsError::VersionError(
                "Channel frequencies are neither in ascending nor in descending order. \
                 Cannot process."
                    .to_string(),
            ));
        }
    }
    let mut band = Band {
        frequencies_hz: frequencies_hz.to_vec(),
        widths_hz: widths_hz.iter().map(|width_hz| width_hz.abs()).collect(),
    };
    if descending {
        band.frequencies_hz.reverse();
        band.widths_hz.reverse();
    }
    Ok((band, descending))
}

/// The `"freq"` branch: `start` moves from the first channel's centre to
/// its edge (`centerIsStart` is always true for a tclean grid).
fn frequency_grid(request: &RegridRequest, band: &Band) -> FrequencyGrid {
    let start_is_end = request.start_is_end();
    let channel_width = request.channel_width();
    let mut start = request.start.unwrap_or(UNSET_START);
    let half_width = if channel_width > 0.0 {
        channel_width
    } else {
        band.first().1
    } / 2.0;
    if start_is_end {
        start += half_width;
    } else {
        start -= half_width;
    }
    FrequencyGrid {
        centre_hz: start,
        bandwidth_hz: -1.0,
        channel_width_hz: channel_width,
        centre_is_start: true,
        start_is_end,
        nchan: request.nchan,
        start,
    }
}

/// The `"vrad"` branch: a radio-velocity start and width become a band
/// centre and channel width in frequency.
fn radio_velocity_grid(request: &RegridRequest, band: &Band) -> FrequencyGrid {
    let rest_hz = request.rest_frequency_hz;
    let start_is_end = request.start_is_end();
    let channel_width = request.channel_width();
    let mut start = request.start.unwrap_or(UNSET_START);
    let mut centre_is_start = true;
    let (mut centre_hz, centre_velocity) = if start > -C {
        let (frequency_hz, width_hz) = band.first();
        let half_width = if channel_width > 0.0 {
            channel_width
        } else {
            vrad(frequency_hz - width_hz / 2.0, rest_hz)
                - vrad(frequency_hz + width_hz / 2.0, rest_hz)
        } / 2.0;
        if start_is_end {
            start -= half_width;
        } else {
            start += half_width;
        }
        (freq_from_vrad(start, rest_hz), start)
    } else {
        centre_is_start = false;
        let centre_hz = (band.first().0 + band.last().0) / 2.0;
        (centre_hz, vrad(centre_hz, rest_hz))
    };
    let channel_width_hz = if channel_width > 0.0 {
        2.0 * (freq_from_vrad(centre_velocity - channel_width / 2.0, rest_hz) - centre_hz)
    } else {
        band.first().1
    };
    let bandwidth_hz = request.nchan as f64 * channel_width_hz;
    if centre_is_start {
        if start_is_end {
            centre_hz -= bandwidth_hz / 2.0;
        } else {
            centre_hz += bandwidth_hz / 2.0;
        }
        centre_is_start = false;
    }
    FrequencyGrid {
        centre_hz,
        bandwidth_hz,
        channel_width_hz,
        centre_is_start,
        start_is_end,
        nchan: request.nchan,
        start,
    }
}

/// The `"vopt"` branch: an optical-velocity start and width become a band
/// centre, bandwidth and channel width in frequency, and `nchan` is spent.
fn optical_velocity_grid(request: &RegridRequest, band: &Band) -> FrequencyGrid {
    let rest_hz = request.rest_frequency_hz;
    let start_is_end = request.start_is_end();
    let channel_width = request.channel_width();
    let mut start = request.start.unwrap_or(UNSET_START);
    let mut centre_is_start = true;
    let (frequency_hz, width_hz) = band.first();
    let (mut centre_hz, mut centre_velocity) = if start > -C {
        let half_width = if channel_width > 0.0 {
            channel_width
        } else {
            vopt(frequency_hz - width_hz / 2.0, rest_hz)
                - vopt(frequency_hz + width_hz / 2.0, rest_hz)
        } / 2.0;
        if start_is_end {
            start -= half_width;
        } else {
            start += half_width;
        }
        (freq_from_vopt(start, rest_hz), start)
    } else {
        centre_is_start = false;
        let (last_hz, last_width_hz) = band.last();
        let centre_hz = (frequency_hz - width_hz + last_hz + last_width_hz) / 2.0;
        (centre_hz, vopt(centre_hz, rest_hz))
    };
    let nchan = request.nchan as f64;
    let divide_by_two = if centre_is_start { 1.0 } else { 0.5 };
    let velocity_width = if channel_width > 0.0 {
        channel_width
    } else {
        (vopt(frequency_hz - width_hz, rest_hz) - vopt(frequency_hz + width_hz, rest_hz)).abs()
    };
    // "Start is end in velocity".
    let bandwidth_upper_end_hz = if centre_is_start && !start_is_end {
        freq_from_vopt(
            centre_velocity - nchan * velocity_width * divide_by_two,
            rest_hz,
        )
    } else {
        freq_from_vopt(
            centre_velocity + nchan * velocity_width * divide_by_two,
            rest_hz,
        )
    };
    let bandwidth_hz = (bandwidth_upper_end_hz - centre_hz).abs() / divide_by_two;
    if centre_is_start {
        if start_is_end {
            centre_velocity += nchan * velocity_width / 2.0;
        } else {
            centre_velocity -= nchan * velocity_width / 2.0;
        }
        centre_hz = freq_from_vopt(centre_velocity, rest_hz);
        centre_is_start = false;
    }
    let channel_width_hz = if channel_width > 0.0 {
        2.0 * (freq_from_vopt(centre_velocity - channel_width / 2.0, rest_hz)
            - freq_from_vopt(centre_velocity, rest_hz))
    } else {
        -1.0
    };
    FrequencyGrid {
        centre_hz,
        bandwidth_hz,
        channel_width_hz,
        centre_is_start,
        start_is_end,
        nchan: 0,
        start,
    }
}

/// `theRegridCenterF` and `theRegridBWF`: the band centre kept within the
/// data's edges and the bandwidth the grid can have.
fn possible_centre_and_bandwidth(
    quantity: RegridQuantity,
    grid: &FrequencyGrid,
    band: &Band,
) -> (f64, f64) {
    let (first_hz, first_width_hz) = band.first();
    let (last_hz, last_width_hz) = band.last();
    let mut centre_is_start = grid.centre_is_start;
    let mut centre_hz = if grid.centre_hz < 0.0 {
        centre_is_start = false;
        (first_hz - first_width_hz / 2.0 + last_hz + last_width_hz / 2.0) / 2.0
    } else if grid.centre_hz - band.upper_edge_hz() > 1.0 {
        // "Keep center in limits", with a 1 Hz tolerance above.
        band.upper_edge_hz()
    } else if grid.centre_hz < band.lower_edge_hz() {
        band.lower_edge_hz()
    } else {
        grid.centre_hz
    };
    let shift_start_to_centre = |centre_hz: &mut f64, bandwidth_hz: f64| {
        if grid.start_is_end {
            *centre_hz -= bandwidth_hz / 2.0;
        } else {
            *centre_hz += bandwidth_hz / 2.0;
        }
    };
    if grid.bandwidth_hz <= 0.0 || grid.nchan != 0 {
        let mut bandwidth_hz = last_hz - first_hz + first_width_hz / 2.0 + last_width_hz / 2.0;
        if grid.nchan != 0 {
            let nchan = grid.nchan as f64;
            bandwidth_hz = if grid.channel_width_hz <= 0.0 {
                first_width_hz * nchan
            } else {
                grid.channel_width_hz * nchan
            };
            // "Center was not set by user but calculated".
            if grid.centre_hz <= 0.0 || grid.start < -C {
                centre_hz = first_hz - first_width_hz / 2.0 + bandwidth_hz / 2.0;
                centre_is_start = false;
            }
        }
        if centre_is_start {
            shift_start_to_centre(&mut centre_hz, bandwidth_hz);
        }
        return (centre_hz, bandwidth_hz);
    }
    let mut bandwidth_hz = grid.bandwidth_hz;
    if centre_is_start {
        shift_start_to_centre(&mut centre_hz, bandwidth_hz);
    }
    // The centre of an optical-velocity grid is its centre in wavelength.
    let range_tolerance_hz = if quantity == RegridQuantity::OpticalVelocity {
        first_width_hz
    } else {
        1.0
    };
    // CASA's truncation measures the lower margin from `transNewXin[0]`
    // plus (not minus) half its width, and its fallback adds the last
    // channel's width where it means its frequency; both are kept.
    let truncate = |centre_hz: &mut f64, bandwidth_hz: &mut f64| {
        *bandwidth_hz = f64::min(
            (last_hz + last_width_hz / 2.0 - *centre_hz).abs(),
            (*centre_hz - first_hz + first_width_hz / 2.0).abs(),
        ) * 2.0;
        if *bandwidth_hz < first_width_hz {
            *centre_hz =
                (first_hz + last_width_hz + last_width_hz / 2.0 - first_width_hz / 2.0) / 2.0;
            *bandwidth_hz = last_width_hz - first_hz + last_width_hz / 2.0 + first_width_hz / 2.0;
        }
    };
    if (centre_hz + bandwidth_hz / 2.0) - band.upper_edge_hz() > range_tolerance_hz {
        truncate(&mut centre_hz, &mut bandwidth_hz);
    }
    if (centre_hz - bandwidth_hz / 2.0) - band.lower_edge_hz() < -range_tolerance_hz {
        truncate(&mut centre_hz, &mut bandwidth_hz);
    }
    (centre_hz, bandwidth_hz)
}

/// `theCentralChanWidthF` and `theChanWidthX`: the frequency width of the
/// central output channel and, when the requested width had to be checked
/// against the data's narrowest channel, that width in the request's unit
/// (`-1` otherwise). A width narrower than every data channel by more
/// than 1 Hz is refused, as CASA refuses it.
fn central_channel_width(
    request: &RegridRequest,
    grid: &FrequencyGrid,
    band: &Band,
    bandwidth_hz: f64,
) -> MsResult<(f64, f64)> {
    let first_width_hz = band.first().1;
    if grid.channel_width_hz <= 0.0 {
        // `nchan != 0 || centerIsStartC`, and a tclean grid always starts
        // with `centerIsStart`: the first data channel's width.
        return Ok((first_width_hz, -1.0));
    }
    let width_hz = grid.channel_width_hz;
    if width_hz > bandwidth_hz {
        return Ok((bandwidth_hz, -1.0));
    }
    if width_hz >= first_width_hz {
        return Ok((width_hz, -1.0));
    }
    let smallest_hz =
        band.widths_hz.iter().copied().fold(
            1e30,
            |smallest, width| if width < smallest { width } else { smallest },
        );
    if width_hz < smallest_hz - 1.0 {
        return Err(MsError::VersionError(format!(
            "Requested new channel width ({width_hz} Hz) is smaller than smallest original \
             channel width which is {smallest_hz} Hz"
        )));
    }
    Ok((width_hz, request.channel_width()))
}

/// The band the output channels fill.
struct Span {
    centre_hz: f64,
    bandwidth_hz: f64,
    central_width_hz: f64,
}

impl Span {
    fn upper_end_hz(&self) -> f64 {
        self.centre_hz + self.bandwidth_hz / 2.0
    }

    fn lower_end_hz(&self) -> f64 {
        self.centre_hz - self.bandwidth_hz / 2.0
    }

    /// `edgeTolerance`.
    fn tolerance_hz(&self) -> f64 {
        self.central_width_hz * 0.01
    }

    /// The central output channel: centred on the band centre when the
    /// bandwidth holds an odd number of channels, starting there otherwise.
    fn central_channel(&self) -> (f64, f64) {
        let channels = ((self.bandwidth_hz + self.tolerance_hz()) / self.central_width_hz).floor();
        let width_hz = self.central_width_hz;
        if (channels as i64) % 2 != 0 {
            (
                self.centre_hz - width_hz / 2.0,
                self.centre_hz + width_hz / 2.0,
            )
        } else {
            (self.centre_hz, self.centre_hz + width_hz)
        }
    }
}

/// Channel bounds stepping up from the central channel and down from it;
/// each list opens with the central channel.
type SteppedBounds = (Vec<(f64, f64)>, Vec<(f64, f64)>);

/// The `"freq"` bounds: contiguous channels of the central width.
fn frequency_bounds(span: &Span) -> SteppedBounds {
    let width_hz = span.central_width_hz;
    let tolerance_hz = span.tolerance_hz();
    let upper_end_hz = span.upper_end_hz();
    let lower_end_hz = span.lower_end_hz();
    let central = span.central_channel();
    let mut up = vec![central];
    let mut down = vec![central];
    let mut high_hz = central.1;
    while high_hz < upper_end_hz + tolerance_hz {
        let next_hz = high_hz + width_hz;
        if next_hz > upper_end_hz + tolerance_hz {
            break;
        }
        up.push((high_hz, next_hz));
        high_hz = next_hz;
    }
    let mut low_hz = central.0;
    while low_hz > lower_end_hz - tolerance_hz {
        let next_hz = low_hz - width_hz;
        if next_hz < lower_end_hz - tolerance_hz {
            break;
        }
        down.push((next_hz, low_hz));
        low_hz = next_hz;
    }
    (up, down)
}

/// A velocity convention's `regridChanBounds` stepping: its conversions
/// and its two loop conditions, which CASA writes differently for radio
/// and optical velocity.
struct VelocityStepping {
    to_velocity: fn(f64, f64) -> f64,
    to_frequency: fn(f64, f64) -> f64,
    /// Whether the channel ending at velocity `edge` still lies within the
    /// band's upper end at `end`, for velocity width `width`.
    continue_up: fn(end: f64, edge: f64, width: f64) -> bool,
    /// The same for the band's lower end, stepping down.
    continue_down: fn(end: f64, edge: f64, width: f64) -> bool,
}

/// `while (upperEndV - theChanWidthX / 10. < velHi)` and
/// `while (velLo < lowerEndV + theChanWidthX / 10.)`.
const RADIO_VELOCITY: VelocityStepping = VelocityStepping {
    to_velocity: vrad,
    to_frequency: freq_from_vrad,
    continue_up: |end, edge, width| end - width / 10.0 < edge,
    continue_down: |end, edge, width| edge < end + width / 10.0,
};

/// `while (upperEndV - velHi < theChanWidthX / 10.)` and
/// `while (velLo - lowerEndV < theChanWidthX / 10.)`.
const OPTICAL_VELOCITY: VelocityStepping = VelocityStepping {
    to_velocity: vopt,
    to_frequency: freq_from_vopt,
    continue_up: |end, edge, width| end - edge < width / 10.0,
    continue_down: |end, edge, width| edge - end < width / 10.0,
};

/// The `"vrad"` and `"vopt"` bounds: contiguous channels of equal
/// velocity width, `velocity_width` when given (`theChanWidthX`), else
/// the central channel's.
fn velocity_bounds(
    span: &Span,
    velocity_width: f64,
    stepping: &VelocityStepping,
    rest_hz: f64,
) -> MsResult<SteppedBounds> {
    let velocity = |frequency_hz| (stepping.to_velocity)(frequency_hz, rest_hz);
    let frequency = |velocity| (stepping.to_frequency)(velocity, rest_hz);
    let tolerance_hz = span.tolerance_hz();
    let upper_end_hz = span.upper_end_hz();
    let lower_end_hz = span.lower_end_hz();
    let upper_end_velocity = velocity(upper_end_hz);
    let lower_end_velocity = velocity(lower_end_hz);
    let central = span.central_channel();
    // "Cannot use original channel width in velocity units".
    let width = if velocity_width < 0.0 {
        velocity(central.0) - velocity(central.1)
    } else {
        velocity_width
    };
    if !(width.is_finite() && width > 0.0) {
        return Err(MsError::VersionError(format!(
            "cube output channel velocity width must be positive, got {width} m/s"
        )));
    }
    // Velocity falls as frequency rises. CASA's `else if` branch after
    // each test repeats it negated and never runs.
    let mut up = vec![central];
    let mut high_velocity = velocity(central.1) - width;
    while (stepping.continue_up)(upper_end_velocity, high_velocity, width) {
        let high_hz = frequency(high_velocity);
        if high_hz > upper_end_hz + tolerance_hz {
            break;
        }
        let low_hz = up.last().expect("the central channel opens the list").1;
        up.push((low_hz, high_hz));
        high_velocity = velocity(high_hz) - width;
    }
    let mut down = vec![central];
    let mut low_velocity = velocity(central.0) + width;
    while (stepping.continue_down)(lower_end_velocity, low_velocity, width) {
        let low_hz = frequency(low_velocity);
        if low_hz < lower_end_hz - tolerance_hz {
            break;
        }
        let high_hz = down.last().expect("the central channel opens the list").0;
        down.push((low_hz, high_hz));
        low_velocity = velocity(low_hz) + width;
    }
    Ok((up, down))
}

/// `MSTransformRegridder::vrad`.
fn vrad(frequency_hz: f64, rest_hz: f64) -> f64 {
    C * (1.0 - frequency_hz / rest_hz)
}

/// `MSTransformRegridder::freq_from_vrad`.
fn freq_from_vrad(velocity: f64, rest_hz: f64) -> f64 {
    rest_hz * (1.0 - velocity / C)
}

/// `MSTransformRegridder::vopt`.
fn vopt(frequency_hz: f64, rest_hz: f64) -> f64 {
    C * (rest_hz / frequency_hz - 1.0)
}

/// `MSTransformRegridder::freq_from_vopt`.
fn freq_from_vopt(velocity: f64, rest_hz: f64) -> f64 {
    rest_hz / (1.0 + velocity / C)
}

#[cfg(test)]
#[path = "regrid_tests.rs"]
mod tests;
