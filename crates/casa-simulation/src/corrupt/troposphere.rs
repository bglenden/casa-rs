// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.settrop`: the phase a turbulent screen of water vapour adds above
//! each antenna, channel by channel (CASA's `AtmosCorruptor` in its
//! `individual` and `screen` modes, a `Tf Jones` term; #841 Phase 4d).
//!
//! # What CASA computes
//!
//! Measured against CASA 6.7.6 (`settrop` probes of 64-channel windows over
//! the 183 GHz and 325 GHz water lines, #841 Phase 4d): `Simulator::settrop`
//! adds a `Tf Jones` term whose value for antenna `a` in channel `c` at a
//! time in slot `s` is `exp(i·δ_a(s)·φ(c))`, where
//! - `φ(c)` is ATM's zenith wet phase delay for the mean water column
//!   `pwv`, radians, at a site `settrop` fixes (5000 m, 560 hPa, 270 K,
//!   20 %, a 2 km water scale height), whatever `setnoise` set;
//! - `δ_a(s)` is the fractional excess of water vapour over `pwv` above the
//!   antenna in slot `s`: fractional Brownian motion of spectral exponent
//!   `beta`, scaled to a root-mean-square of `deltapwv` (at most 1) about
//!   its mean, the mean kept. The slots are even, 10 s wide (or `simint`,
//!   at least 0.1 s), spanning the integrations observed so far, and a
//!   solution takes the slot nearest its time (`CalCorruptor::setEvenSlots`
//!   and `setCurrTime`). In `individual` mode each antenna has a series of
//!   its own; in `screen` mode one two-dimensional screen of pixels half the
//!   smallest dish across is blown along the array's east axis at
//!   `windspeed`, and an antenna reads the pixel it is over, the screen
//!   moved on by `floor(s·Δt·v/pixel)` pixels in slot `s`
//!   (`AtmosCorruptor::cphase`).
//!
//! The term is phase only, so an autocorrelation is unchanged, and it has a
//! solution per integration, window and antenna, as CASA's table has.
//! The screen sits at the zenith: CASA neither scales the phase by the
//! airmass nor moves an antenna's pixel with the direction it observes
//! (its own `RI TODO`s), and casa-rs follows it; nor does it interpolate
//! between pixels.
//!
//! # Where casa-rs departs
//!
//! Recorded in `docs/CASA (C++) bugs.md`:
//! - **The wet path.** CASA's `φ` is ATM's *dispersive* wet phase delay
//!   alone, the part of the water vapour's refraction its lines add. The
//!   path a column of water vapour adds is mostly its non-dispersive part
//!   (`ℒ_V ≃ 6.3 w`, Thompson, Moran & Swenson 3rd ed. eq. 13.19, p. 702):
//!   at `settrop`'s site the whole wet delay is 8.5 to 940 times the
//!   dispersive part between 345 and 43 GHz. casa-rs takes both: `φ` is the
//!   whole wet phase delay.
//! - **The channels' frequencies.** As for `tsys-atm` noise
//!   ([`super::tsys_atm`]), CASA evaluates ATM half a band below each
//!   channel, and reads past the end of a later one-channel window's map;
//!   casa-rs evaluates it at each channel's CHAN_FREQ.
//! - **The screen.** CASA's two-dimensional spectrum has amplitudes
//!   `|(i, j)|^−(β+½)/2` in index units, on unwrapped indices (so the
//!   negative wavenumbers of the full axis get the smallest amplitudes) and
//!   on a screen whose two axes differ, and it writes past the end of its
//!   spectrum. casa-rs draws `|k|^−(β+1)/2` on the wavenumbers themselves
//!   (cycles per pixel, wrapped), the spectrum of a two-dimensional
//!   fractional Brownian motion whose one-dimensional cuts have the
//!   spectral exponent `beta`, as `individual` mode's series do.
//! - **The series.** As for `setgain(mode='fbm')` ([`super::jones`]), CASA
//!   reads a series' last two slots from past the end of its transform and
//!   normalises with them; casa-rs transforms every slot.
//! - As for `setgain`, every antenna is simulated where CASA's
//!   autocorrelation rows reset theirs to 1, and with `simint` longer than
//!   an integration each solution is stamped with its own integration's
//!   time, where CASA stamps it halfway between its interval's first
//!   integration and its own and so applies a slot from earlier in the
//!   interval.
//!
//! The draws are casa-rs's (Philox streams of their own purpose byte), so
//! realisations are compared with CASA's statistically.

use casa_atmosphere::{AbsorptionProfile, ChannelTotals};
use casa_fft::RealFft2;
use num_complex::Complex32;

use super::jones::{Slots, normal_pair, uniform};

/// The purpose byte of tropospheric screens' streams.
const PURPOSE_TROPOSPHERE_SCREEN: u32 = 0x05;

/// The Earth's radius CASA projects antenna positions with, metres.
const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Pixels CASA leaves around the antennas on each screen axis.
const SCREEN_BORDER_PIXELS: f64 = 2.0;

/// The margin CASA gives the screen along the wind, beyond what the wind
/// blows over the track.
const BLOW_MARGIN: f64 = 1.05;

/// The longest screen CASA makes for its width: a screen more than this
/// many times as long as wide is widened.
const LONGEST_ASPECT: usize = 5;

/// A tropospheric term's phases: each solution's fractional excess of water
/// vapour above each antenna, and each window's wet phase delay per channel
/// for the mean water column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TroposphericPhase {
    /// `[block · antennas + antenna]`: δ, the excess over `pwv` as a
    /// fraction of it.
    excess: Vec<f32>,
    /// `[window]`: each channel's zenith wet phase delay for `pwv`, radians;
    /// `None` for a window defined after the term was set.
    delays: Vec<Option<Vec<f64>>>,
}

impl TroposphericPhase {
    /// The phases of `excess` (`[block · antennas + antenna]`) through
    /// `delays` (`[window]`, radians per channel).
    pub(crate) fn new(excess: Vec<f32>, delays: Vec<Option<Vec<f64>>>) -> Self {
        Self { excess, delays }
    }

    /// The heap the phases keep.
    pub(crate) fn bytes(&self) -> u64 {
        (self.excess.capacity() * size_of::<f32>()
            + self.delays.capacity() * size_of::<Option<Vec<f64>>>()
            + self
                .delays
                .iter()
                .flatten()
                .map(|delays| delays.capacity() * size_of::<f64>())
                .sum::<usize>()) as u64
    }

    /// The heap phases of `blocks` blocks of `antennas` antennas keep for
    /// windows of `channels` channels each.
    pub(crate) fn bytes_for(blocks: usize, antennas: usize, channels: &[usize]) -> u64 {
        (blocks * antennas * size_of::<f32>()
            + channels.len() * size_of::<Option<Vec<f64>>>()
            + channels.iter().sum::<usize>() * size_of::<f64>()) as u64
    }

    /// Spectral window `window`'s channels, when the term has its delays.
    pub(crate) fn channels(&self, window: usize) -> Option<usize> {
        self.delays.get(window)?.as_ref().map(Vec::len)
    }

    /// Antenna `antenna`'s value in each channel of `window` in block
    /// `block` of `antennas` antennas, `exp(i·δ·φ(c))`, into `phasors`.
    pub(crate) fn phasors(
        &self,
        (window, block): (usize, usize),
        (antenna, antennas): (usize, usize),
        phasors: &mut [Complex32],
    ) {
        let excess = f64::from(self.excess[block * antennas + antenna]);
        let delays = self.delays[window]
            .as_ref()
            .expect("a tropospheric term has the delays of every window it has solutions in");
        for (phasor, &delay) in phasors.iter_mut().zip(delays) {
            let (sin, cos) = (excess * delay).sin_cos();
            *phasor = Complex32::new(cos as f32, sin as f32);
        }
    }
}

/// Each channel's zenith wet phase delay, radians, of `profile`'s channels
/// for a water column of `water_column_m`: the dispersive part (the lines)
/// and the non-dispersive part (the continuum) together.
pub(crate) fn wet_delays(
    profile: &AbsorptionProfile<ChannelTotals>,
    water_column_m: f64,
) -> Vec<f64> {
    (0..profile.channels().len())
        .map(|channel| {
            (profile.dispersive_h2o_phase_delay_deg(channel, water_column_m)
                + profile.non_dispersive_h2o_phase_delay_deg(channel, water_column_m))
            .to_radians()
        })
        .collect()
}

/// `individual` mode's excess: for each of `antennas` antennas a series of
/// fractional Brownian motion of spectral exponent `beta` over `slots`,
/// scaled to a root-mean-square of `scale` about its mean, the mean kept
/// (`AtmosCorruptor::initialize(Seed, Beta, scale, rxtype)`), drawn from
/// `seed`'s streams. `[slot · antennas + antenna]`.
pub(crate) fn antenna_series(
    seed: i32,
    (beta, scale): (f64, f64),
    slots: usize,
    antennas: usize,
) -> Vec<f32> {
    let key = [seed as u32, PURPOSE_TROPOSPHERE_SCREEN << 24];
    let mut fft = super::jones::ScreenFft::new(slots);
    let mut excess = vec![0.0; slots * antennas];
    for antenna in 0..antennas {
        let series = fft.normalised_fbm(key, [antenna as u32, 0, 0, 0], (beta, scale));
        for (slot, value) in series.into_iter().enumerate() {
            excess[slot * antennas + antenna] = value as f32;
        }
    }
    excess
}

/// Where a `screen` mode screen lies over the array, as CASA lays it
/// (`AtmosCorruptor::initialize(…, antcols)`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScreenLayout {
    /// The pixel's side, metres: half the smallest dish.
    pub(crate) pixel_m: f64,
    /// `[rows, columns]`: north by east.
    pub(crate) shape: [usize; 2],
    /// Each antenna's pixel, (row, column), before the wind.
    pub(crate) antennas: Vec<[usize; 2]>,
}

impl ScreenLayout {
    /// The screen over antennas at `positions_itrf_m` with dishes of
    /// `diameters_m`, long enough for a wind of `windspeed_m_s` over a
    /// track of `track_s`: CASA projects each position's longitude and
    /// latitude onto a sphere of the Earth's radius, east scaled by the
    /// cosine of the mean latitude, from the westmost and southmost
    /// antennas, with two pixels' border and the wind's run along the east
    /// axis, 5 % more; a screen more than five times as long as wide is
    /// widened to a fifth of its length.
    pub(crate) fn new(
        positions_itrf_m: &[[f64; 3]],
        diameters_m: &[f64],
        (windspeed_m_s, track_s): (f64, f64),
    ) -> Self {
        let pixel_m = 0.5 * diameters_m.iter().copied().fold(f64::INFINITY, f64::min);
        let north: Vec<f64> = positions_itrf_m
            .iter()
            .map(|[x, y, z]| z.atan2(x.hypot(*y)) * EARTH_RADIUS_M)
            .collect();
        let mean_latitude = north.iter().sum::<f64>() / north.len().max(1) as f64 / EARTH_RADIUS_M;
        let east: Vec<f64> = positions_itrf_m
            .iter()
            .map(|[x, y, _]| y.atan2(*x) * EARTH_RADIUS_M * mean_latitude.cos())
            .collect();
        let from_least = |values: &[f64]| {
            let least = values.iter().copied().fold(f64::INFINITY, f64::min);
            values
                .iter()
                .map(|value| (value - least) / pixel_m)
                .collect::<Vec<_>>()
        };
        let (east, north) = (from_least(&east), from_least(&north));
        let most = |values: &[f64]| values.iter().copied().fold(0.0, f64::max);
        // CASA holds the track and the run of the wind as `Float`s.
        let run_m = f64::from((windspeed_m_s * f64::from(track_s as f32) * BLOW_MARGIN) as f32);
        let columns = (most(&east) + SCREEN_BORDER_PIXELS + run_m / pixel_m).ceil() as usize;
        let mut rows = (most(&north) + SCREEN_BORDER_PIXELS).ceil() as usize;
        if columns > LONGEST_ASPECT * rows {
            rows = columns / LONGEST_ASPECT;
        }
        let antennas = north
            .iter()
            .zip(&east)
            .map(|(&row, &column)| [row as usize, column as usize])
            .collect();
        Self {
            pixel_m,
            shape: [rows, columns],
            antennas,
        }
    }

    /// The real transform's elements: the screen's spectrum, which the
    /// real screen overwrites in place.
    pub(crate) fn spectrum_len(&self) -> usize {
        self.shape[0] * (self.shape[1] / 2 + 1)
    }

    /// The columns the wind has moved the screen by in slot `slot` of
    /// `slots` at `windspeed_m_s`.
    fn blown(&self, slots: &Slots, slot: usize, windspeed_m_s: f64) -> usize {
        ((slot as f64 * slots.width_s * windspeed_m_s) / self.pixel_m).floor() as usize
    }
}

/// `screen` mode's excess: one screen of fractional Brownian motion of
/// spectral exponent `beta` laid out by `layout`, scaled to a
/// root-mean-square of `scale` about its mean, the mean kept, drawn from
/// `seed`'s streams; each antenna reads its pixel, the screen blown on by
/// the wind at `windspeed_m_s` in each of `slots`. The transform runs on
/// `threads` threads. `[slot · antennas + antenna]`.
///
/// # Errors
///
/// The screen's transform cannot be planned.
pub(crate) fn screen_excess(
    seed: i32,
    (beta, scale): (f64, f64),
    layout: &ScreenLayout,
    (slots, windspeed_m_s): (&Slots, f64),
    threads: usize,
) -> Result<Vec<f32>, casa_fft::FftError> {
    let [rows, columns] = layout.shape;
    let half = columns / 2 + 1;
    let mut fft = RealFft2::<f32>::with_threads([rows, columns], threads.max(1))?
        .with_estimated_plan()
        .with_owned_plan();
    let mut plane = vec![Complex32::new(0.0, 0.0); layout.spectrum_len()];
    let key = [seed as u32, (PURPOSE_TROPOSPHERE_SCREEN << 24) | 1];
    for (row, spectrum) in plane.chunks_exact_mut(half).enumerate() {
        // Cycles per pixel, the full axis wrapped.
        let north = row.min(rows - row) as f64 / rows as f64;
        for (column, value) in spectrum.iter_mut().enumerate() {
            if row == 0 && column == 0 {
                continue;
            }
            let east = column as f64 / columns as f64;
            let counter = [row as u32, column as u32, 0, 0];
            let phase = 2.0 * std::f64::consts::PI * uniform(key, counter);
            let [normal, _] = normal_pair(key, [row as u32, column as u32, 1, 0]);
            let amplitude = north.hypot(east).powf(-0.5 * (beta + 1.0)) * normal;
            let polar = num_complex::Complex64::from_polar(amplitude, phase);
            *value = Complex32::new(polar.re as f32, polar.im as f32);
        }
    }
    fft.inverse(&mut plane)?;
    // The real screen, `2·half` scalars a row: a column's pixel is its pair's
    // real or imaginary part.
    debug_assert_eq!(fft.real_row_stride(), 2 * half);
    let pixel = |row: usize, column: usize| -> f64 {
        let value = plane[row * half + column / 2];
        f64::from(if column.is_multiple_of(2) {
            value.re
        } else {
            value.im
        })
    };
    let count = (rows * columns) as f64;
    let mean = (0..rows)
        .flat_map(|row| (0..columns).map(move |column| (row, column)))
        .map(|(row, column)| pixel(row, column))
        .sum::<f64>()
        / count;
    let variance = (0..rows)
        .flat_map(|row| (0..columns).map(move |column| (row, column)))
        .map(|(row, column)| (pixel(row, column) - mean).powi(2))
        .sum::<f64>()
        / count;
    let factor = if variance > 0.0 {
        scale / variance.sqrt()
    } else {
        0.0
    };
    let antennas = layout.antennas.len();
    let mut excess = vec![0.0; slots.count * antennas];
    for slot in 0..slots.count {
        let blown = layout.blown(slots, slot, windspeed_m_s);
        for (antenna, &[row, column]) in layout.antennas.iter().enumerate() {
            excess[slot * antennas + antenna] = (pixel(row, column + blown) * factor) as f32;
        }
    }
    Ok(excess)
}

#[cfg(test)]
#[path = "troposphere_tests.rs"]
mod tests;
