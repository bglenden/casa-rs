// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility corruption: thermal noise, CASA's `ANoise` term
//! (`sm.setnoise(mode='simplenoise')` then `sm.corrupt()`).
//!
//! # What CASA adds
//!
//! Measured against CASA 6.7.6 (simulation Phase 1d probes, #841), each
//! `corrupt` adds, for every noise term, to every DATA sample:
//! - on a cross-correlation row, independent real and imaginary draws, each
//!   N(0, σ²), where σ is the term's `simplenoise` in Jy;
//! - on an autocorrelation row, a real draw only: the imaginary part is
//!   unchanged;
//! - in every correlation, the cross-hands of four-correlation data
//!   included, each with its own draw;
//! - whatever the flags: flagged rows and samples get noise too.
//!
//! WEIGHT and SIGMA are left as observed. A draw is made in double precision,
//! scaled by σ held as a `Float`, and rounded to single precision before it
//! is added, as `ANoiseCorruptor::simPar` computes `Complex(r·amp, i·amp)`.
//!
//! # Where the draws come from
//!
//! CASA draws from one `MLCG` stream per term, in the order its visibility
//! iterator visits rows, channels and correlations, and the stream carries on
//! across `corrupt` calls. casa-rs keys each draw instead
//! ([`philox`], Philox4x32-10): the key is the term's seed and its
//! application ordinal (how many `corrupt` calls the term had seen before),
//! and the counter is the absolute MAIN row, the channel and the
//! correlation pair. The two are statistically equivalent, not bit-identical
//! (plan, "Random numbers"), and they share the structure scripts see:
//! - two terms with the same seed draw the same values in the same
//!   `corrupt`, so their noise adds coherently (variance 4σ² for two);
//! - one term applied twice draws independent values (variance 2σ²);
//! - a different seed draws different values.
//!
//! The result depends only on (seed, ordinal, row, channel, correlation), so
//! it is bitwise the same for any batch size, worker count or backend.
//!
//! Normals come from Box–Muller on a pair of 32-bit words, in `f64`, with
//! the logarithm, sine and cosine evaluated on the words ([`normal`]), so
//! they are also bitwise the same on every platform. The largest draw is
//! √(64 ln 2) ≈ 6.66σ, one in about 4·10⁹ beyond that in an exact normal.
//!
//! # Drawing channels together
//!
//! A row's draws are made [`LANES`] channels at a time: the counters of one
//! correlation pair in consecutive channels differ only in their channel
//! word, so the generator and Box–Muller run on all of them in step, in
//! vector registers. Every lane performs exactly the operations of a draw
//! made alone, in the same order and with nothing fused, so the batching
//! changes the speed and not one bit of the noise; the tests check this
//! against the one-draw-at-a-time code it replaced ([`oracle`]).

mod normal;
#[cfg(test)]
mod oracle;
mod philox;
#[cfg(test)]
mod tests;

use num_complex::Complex32;

#[cfg(test)]
use normal::box_muller;
use philox::Key;

/// Channels drawn together. On an Apple M2, eight lanes (four 128-bit
/// registers per `f64` value) ran fastest of 4 to 32 (#855): fewer leave the
/// polynomials' latency exposed, more spill their coefficients and running
/// sums out of the 32 vector registers.
const LANES: usize = 8;

/// The top byte of the key's second word names what the stream is for, so
/// later corruptions (gains, fBM screens) draw from streams disjoint from
/// thermal noise under the same seed.
const PURPOSE_THERMAL_NOISE: u32 = 0x01;

/// Application ordinals share the key word with the purpose byte, so a term
/// can be applied at most this many times.
pub(crate) const MAXIMUM_APPLICATIONS: u32 = 1 << 24;

/// One application of a noise term: the noise one `corrupt` adds for one
/// `setnoise` term.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NoiseApplication {
    /// The seed in force when the term was set (`sm.setseed`).
    pub(crate) seed: i32,
    /// σ of each real and imaginary draw, Jy, held as CASA holds it.
    pub(crate) sigma_jy: f32,
    /// How many times the term had been applied before; below
    /// [`MAXIMUM_APPLICATIONS`].
    pub(crate) ordinal: u32,
}

impl NoiseApplication {
    fn key(&self) -> Key {
        debug_assert!(self.ordinal < MAXIMUM_APPLICATIONS);
        [
            self.seed as u32,
            (PURPOSE_THERMAL_NOISE << 24) | self.ordinal,
        ]
    }

    /// Add this application's noise to the cell of MAIN row `row`:
    /// `correlations × channels` samples, correlation fastest. An
    /// autocorrelation row gets real draws only.
    ///
    /// Each draw of the counter (row, channel, pair) gives four words: the
    /// first two make correlation `2·pair`'s (real, imaginary) normals, the
    /// last two correlation `2·pair + 1`'s, when the row has it. The draws
    /// are made [`LANES`] channels at a time; a last, partial batch draws
    /// the full width and keeps the channels the cell has. Samples past the
    /// last whole channel are left alone.
    pub(crate) fn add_to_row(
        &self,
        row: u64,
        autocorrelation: bool,
        correlations: usize,
        cell: &mut [Complex32],
    ) {
        // Adding σ·N with σ = 0 changes nothing but the sign of a zero.
        if self.sigma_jy == 0.0 || correlations == 0 {
            return;
        }
        // The common cross-correlation shapes, each compiled with its
        // correlation count fixed so that the additions into the cell
        // vectorise; every arm runs the same code.
        match (correlations, autocorrelation) {
            (1, false) => self.add_batches(row, 1, false, cell),
            (2, false) => self.add_batches(row, 2, false, cell),
            (4, false) => self.add_batches(row, 4, false, cell),
            _ => self.add_batches(row, correlations, autocorrelation, cell),
        }
    }

    /// [`Self::add_to_row`] in batches of [`LANES`] channels, and a last,
    /// partial batch.
    #[inline(always)]
    fn add_batches(
        &self,
        row: u64,
        correlations: usize,
        autocorrelation: bool,
        cell: &mut [Complex32],
    ) {
        let batch = Batch {
            key: self.key(),
            sigma: f64::from(self.sigma_jy),
            row,
            correlations,
            autocorrelation,
        };
        let channels = cell.len() / correlations;
        let mut batches = cell[..channels * correlations].chunks_exact_mut(LANES * correlations);
        for (index, samples) in (&mut batches).enumerate() {
            batch.add(index * LANES, samples, LANES);
        }
        let tail = batches.into_remainder();
        if !tail.is_empty() {
            batch.add_partial(channels - channels % LANES, tail, channels % LANES);
        }
    }
}

/// What every batch of one row's noise shares.
struct Batch {
    /// The application's stream key.
    key: Key,
    /// σ of each draw, Jy, widened from CASA's `Float`.
    sigma: f64,
    /// The MAIN row.
    row: u64,
    /// Correlations per channel.
    correlations: usize,
    /// An autocorrelation row, which gets real draws only.
    autocorrelation: bool,
}

impl Batch {
    /// Add the noise of the `lanes` channels from `first`, at most
    /// [`LANES`], to their `samples`: the draws of each correlation pair in
    /// all the channels, then the additions, both correlations of the pair
    /// together.
    #[inline(always)]
    fn add(&self, first: usize, samples: &mut [Complex32], lanes: usize) {
        let correlations = self.correlations;
        for pair in 0..correlations.div_ceil(2) {
            let words = philox::philox4x32_10_lanes(counters(self.row, first, pair), self.key);
            let both = correlations - 2 * pair >= 2;
            let [re0, im0] = scaled_draws(&words[0], &words[1], self.sigma);
            let [re1, im1] = if both {
                scaled_draws(&words[2], &words[3], self.sigma)
            } else {
                [[0.0; LANES]; 2]
            };
            for lane in 0..lanes {
                let at = lane * correlations + 2 * pair;
                samples[at].re += re0[lane];
                if both {
                    samples[at + 1].re += re1[lane];
                }
                if !self.autocorrelation {
                    samples[at].im += im0[lane];
                    if both {
                        samples[at + 1].im += im1[lane];
                    }
                }
            }
        }
    }

    /// [`Self::add`] for a row's last, partial batch: one copy for every
    /// shape, since it runs at most once a row.
    #[inline(never)]
    fn add_partial(&self, first: usize, samples: &mut [Complex32], lanes: usize) {
        self.add(first, samples, lanes);
    }
}

/// The counters of one correlation pair's draws in the [`LANES`] channels
/// from `first`: the MAIN row (two words), the channel, and the correlation
/// pair (correlations 0 and 1 are pair 0, 2 and 3 pair 1).
fn counters(row: u64, first: usize, pair: usize) -> philox::Lanes<LANES> {
    [
        [row as u32; LANES],
        [(row >> 32) as u32; LANES],
        std::array::from_fn(|lane| (first + lane) as u32),
        [pair as u32; LANES],
    ]
}

/// The real and imaginary noise of `LANES` samples from their two words
/// each: σ times the Box–Muller pair, in `f64`, rounded to single
/// precision as CASA rounds `Complex(r·amp, i·amp)`.
///
/// Each lane computes Box–Muller's products, `r·cos θ` and `r·sin θ`, as a
/// draw made alone does, but in two passes, the radii and then the angles:
/// each pass's polynomial coefficients and running sums then fit the
/// vector registers together, which keeps more lanes' arithmetic in flight.
#[inline(always)]
fn scaled_draws<const LANES: usize>(
    first: &[u32; LANES],
    second: &[u32; LANES],
    sigma: f64,
) -> [[f32; LANES]; 2] {
    let mut radii = [0.0; LANES];
    for (radius, &word) in radii.iter_mut().zip(first) {
        *radius = normal::radius(word);
    }
    let mut draws = [[0.0; LANES]; 2];
    for lane in 0..LANES {
        let (sin, cos) = normal::sin_cos_turn(second[lane]);
        draws[0][lane] = ((radii[lane] * cos) * sigma) as f32;
        draws[1][lane] = ((radii[lane] * sin) * sigma) as f32;
    }
    draws
}

/// The four standard normals of one draw, slot 0's (real, imaginary) then
/// slot 1's, as [`NoiseApplication::add_to_row`] uses them.
#[cfg(test)]
fn standard_normals(
    application: &NoiseApplication,
    row: u64,
    channel: usize,
    pair: usize,
) -> [f64; 4] {
    let counter = [row as u32, (row >> 32) as u32, channel as u32, pair as u32];
    let words = philox::philox4x32_10(counter, application.key());
    let (a, b) = box_muller(words[0], words[1]);
    let (c, d) = box_muller(words[2], words[3]);
    [a, b, c, d]
}
