// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counter-based Gaussian noise for visibility cells: σ times a standard
//! normal added to every real and imaginary part of a cell, each draw a
//! pure function of a stream key and the sample's place in the data.
//!
//! # A draw
//!
//! A draw is Philox4x32-10 ([`philox4x32_10`]; Salmon, Moraes, Dror and
//! Shaw, SC11) of a 128-bit counter under a 64-bit key: four 32-bit words.
//! The counter is the row (its low word, then its high word), the channel
//! and the correlation pair (correlations 0 and 1 are pair 0, 2 and 3
//! pair 1). Words 0 and 1 make correlation `2·pair`'s (real, imaginary)
//! normals, words 2 and 3 correlation `2·pair + 1`'s. The key names the
//! stream and belongs to the caller: casa-simulation, for one, keys thermal
//! noise by the term's seed and how many times the term has been applied.
//! Distinct keys give independent streams, and one key the same values
//! every time.
//!
//! Philox keeps no state, so the noise of a sample depends only on (key,
//! row, channel, correlation): it is bitwise the same in any order, for any
//! batch size, on any number of workers and on any backend that computes
//! the same arithmetic.
//!
//! Normals come from Box–Muller on a pair of words ([`box_muller`]): with
//! `u₁ = (w₀ + 1)·2⁻³²` in (0, 1] and `θ = 2π·w₁·2⁻³²`, the pair
//! `√(−2 ln u₁)·(cos θ, sin θ)` is two independent standard normals. They are
//! computed in `f32`, with the logarithm, sine and cosine evaluated on the
//! words themselves rather than by the platform's `libm`: only IEEE-754
//! additions, multiplications, one division and a square root, all
//! correctly rounded and none fused, so a draw is bitwise the same on every
//! platform. The noise added is `(r·σ)·cos θ` and `(r·σ)·sin θ`
//! ([`scaled_draw`]): scaling the radius once saves a multiplication per
//! sample over scaling each normal, with the same number of roundings.
//!
//! # Accuracy
//!
//! The draws replace the double-precision draws the same words gave before
//! (#855; the tests keep that code as an oracle). With `ε = 2⁻²⁴`, the unit
//! roundoff of `f32`, the largest errors against it over all 2³² words are
//! 2.003ε for the radius and 2.892ε for the sine and the cosine, relatively.
//! The radius depends on the first word only and the angle on the second
//! only, so a standard normal, rounded once more, is within
//! `2.003ε + 2.892ε + ε < 6ε` of the double-precision normal, relatively,
//! for every pair of words. Over 2³³ normals the largest error is 5.18ε, and
//! 56 % of them are the double-precision normal rounded to `f32`. Scaling
//! by σ, as `(r·σ)·cos θ` rather than `(r·cos θ)·σ`, adds one rounding.
//! Against noise of standard deviation σ the difference is of order 10⁻⁷σ,
//! below what an `f32` visibility resolves.
//!
//! Each bound is relative down to the smallest values, because each part is
//! computed from exact integers where precision matters: near u₁ = 1 the
//! logarithm takes `1 − u₁` as an integer, and the angle within its octant
//! is taken from the word's low bits. The largest draw is `√(64 ln 2)`
//! ≈ 6.66 standard deviations (6.660437, that value correctly rounded to
//! `f32`, at `w₀ = 0`); an exact normal exceeds it about once in 4·10⁹.
//!
//! # Drawing channels together
//!
//! A row's draws are made sixteen channels at a time: the counters of one
//! correlation pair in consecutive channels differ only in their channel
//! word, so the generator and Box–Muller run on all of them in step, in
//! vector registers (Philox's multiplies with NEON intrinsics on aarch64).
//! Every lane performs exactly the operations of a draw made alone, in the
//! same order and with nothing fused, so the batching changes the speed and
//! not one bit of the noise. The tests check this against the same draws
//! made one at a time ([`philox4x32_10`] and [`scaled_draw`]), check both
//! against the double-precision oracle within the bounds above, and hold a
//! few draws and one cell to known bits, so that a platform that rounds
//! differently fails.

mod metal;
mod normal;
#[cfg(test)]
mod oracle;
mod philox;
#[cfg(test)]
mod tests;

use num_complex::Complex32;

pub use metal::{
    METAL_EXACT_SIGMA, METAL_KERNEL, MetalNoiseParams, MetalNoiseRow, MetalNoiseTerm,
    metal_draws_exactly, metal_source,
};
pub use normal::{box_muller, scaled_draw};
pub use philox::philox4x32_10;

/// Channels drawn together. On an Apple M2, sixteen lanes (four 128-bit
/// registers per `f32` value) ran 12–15 % faster than eight and 8–17 %
/// faster than thirty-two (#855): fewer leave the latency of the
/// polynomials, the division and the square root exposed; more spill
/// Philox's words and the polynomials' coefficients and running sums out of
/// the 32 vector registers. For the double-precision draws before them,
/// eight lanes, also four registers per value, were fastest.
const LANES: usize = 16;

/// Gaussian noise of standard deviation σ for the cells of one stream: a
/// 64-bit Philox key and σ.
///
/// [`Self::add_to_row`] adds σ times an independent standard normal to the
/// real and the imaginary part of every sample of a row's cell. Sample
/// (channel `c`, correlation `k`) of row `r` takes the words Philox4x32-10
/// ([`philox4x32_10`]) gives the counter
/// `[r as u32, (r >> 32) as u32, c as u32, (k / 2) as u32]` under the key:
/// words 0 and 1 when `k` is even, 2 and 3 when it is odd, made into
/// `(r·σ)·cos θ` and `(r·σ)·sin θ` by [`scaled_draw`]. The noise is
/// therefore a pure function of the key, σ and the sample's place, the same
/// whichever rows are drawn, in whatever order and on whichever thread; the
/// [module documentation](crate::cell_noise) gives the generator, the
/// normals and their accuracy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CellNoise {
    /// Philox's 64-bit key: which stream.
    key: [u32; 2],
    /// σ of each real and imaginary draw, in the cell's units.
    sigma: f32,
}

impl CellNoise {
    /// The stream `key` (Philox's 64-bit key) adding σ = `sigma` per real
    /// and imaginary draw.
    ///
    /// Any key is valid; keys that differ in any bit give independent
    /// streams. A caller with several kinds of noise under one seed keeps
    /// their keys disjoint itself (casa-simulation reserves the top byte of
    /// the second word for the purpose).
    #[must_use]
    pub const fn new(key: [u32; 2], sigma: f32) -> Self {
        Self { key, sigma }
    }

    /// The stream's Philox key.
    #[must_use]
    pub const fn key(&self) -> [u32; 2] {
        self.key
    }

    /// σ of each real and imaginary draw.
    #[must_use]
    pub const fn sigma(&self) -> f32 {
        self.sigma
    }

    /// Add σ·N to every sample of the cell of row `row`:
    /// `correlations × channels` samples, correlation fastest. An
    /// autocorrelation row gets real draws only; its imaginary parts are
    /// left as they are.
    ///
    /// Each draw of the counter (row, channel, pair) gives four words: the
    /// first two make correlation `2·pair`'s (real, imaginary) normals, the
    /// last two correlation `2·pair + 1`'s, when the row has it. The draws
    /// are made sixteen channels at a time; a last, partial batch draws the
    /// full width and keeps the channels the cell has. Samples past the last
    /// whole channel are left alone, and so is the whole cell when σ is 0 or
    /// `correlations` is 0.
    pub fn add_to_row(
        &self,
        row: u64,
        autocorrelation: bool,
        correlations: usize,
        cell: &mut [Complex32],
    ) {
        // Adding σ·N with σ = 0 changes nothing but the sign of a zero.
        if self.sigma == 0.0 || correlations == 0 {
            return;
        }
        self.add_shapes(
            row,
            autocorrelation,
            correlations,
            Uniform(self.sigma),
            cell,
        );
    }

    /// [`Self::add_to_row`] with σ scaled per channel: channel `c`'s samples
    /// get σ·`channel_scale[c]` (a noise whose σ depends on frequency, as a
    /// system temperature does). The draws are [`Self::add_to_row`]'s, so a
    /// scale of 1 everywhere adds bit for bit the same noise.
    ///
    /// # Panics
    ///
    /// When `channel_scale` has fewer values than the cell has whole
    /// channels.
    pub fn add_to_row_scaled(
        &self,
        row: u64,
        autocorrelation: bool,
        correlations: usize,
        channel_scale: &[f32],
        cell: &mut [Complex32],
    ) {
        if self.sigma == 0.0 || correlations == 0 {
            return;
        }
        assert!(
            channel_scale.len() >= cell.len() / correlations,
            "a scale for every channel of the cell"
        );
        self.add_shapes(
            row,
            autocorrelation,
            correlations,
            Scaled {
                sigma: self.sigma,
                channel_scale,
            },
            cell,
        );
    }

    /// The common cross-correlation shapes, each compiled with its
    /// correlation count fixed so that the additions into the cell
    /// vectorise; every arm runs the same code.
    #[inline(always)]
    fn add_shapes<S: LaneSigma>(
        &self,
        row: u64,
        autocorrelation: bool,
        correlations: usize,
        sigma: S,
        cell: &mut [Complex32],
    ) {
        match (correlations, autocorrelation) {
            (1, false) => self.add_batches(row, 1, false, sigma, cell),
            (2, false) => self.add_batches(row, 2, false, sigma, cell),
            (4, false) => self.add_batches(row, 4, false, sigma, cell),
            _ => self.add_batches(row, correlations, autocorrelation, sigma, cell),
        }
    }

    /// [`Self::add_to_row`] in batches of [`LANES`] channels, and a last,
    /// partial batch.
    #[inline(always)]
    fn add_batches<S: LaneSigma>(
        &self,
        row: u64,
        correlations: usize,
        autocorrelation: bool,
        sigma: S,
        cell: &mut [Complex32],
    ) {
        let batch = Batch {
            key: self.key,
            sigma,
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

/// σ of each of a batch's [`LANES`] channels.
trait LaneSigma: Copy {
    /// σ of the channels from `first`.
    fn lanes(self, first: usize) -> [f32; LANES];
}

/// One σ for every channel.
#[derive(Clone, Copy)]
struct Uniform(f32);

impl LaneSigma for Uniform {
    #[inline(always)]
    fn lanes(self, _first: usize) -> [f32; LANES] {
        [self.0; LANES]
    }
}

/// σ times each channel's scale; channels past the scales (a partial
/// batch's draws beyond the cell) get 0.
#[derive(Clone, Copy)]
struct Scaled<'a> {
    sigma: f32,
    channel_scale: &'a [f32],
}

impl LaneSigma for Scaled<'_> {
    #[inline(always)]
    fn lanes(self, first: usize) -> [f32; LANES] {
        std::array::from_fn(|lane| {
            self.channel_scale
                .get(first + lane)
                .map_or(0.0, |scale| self.sigma * scale)
        })
    }
}

/// What every batch of one row's noise shares.
struct Batch<S> {
    /// The stream key.
    key: philox::Key,
    /// σ of each draw.
    sigma: S,
    /// The row.
    row: u64,
    /// Correlations per channel.
    correlations: usize,
    /// An autocorrelation row, which gets real draws only.
    autocorrelation: bool,
}

impl<S: LaneSigma> Batch<S> {
    /// Add the noise of the `lanes` channels from `first`, at most
    /// [`LANES`], to their `samples`: the draws of each correlation pair in
    /// all the channels, then the additions, both correlations of the pair
    /// together.
    #[inline(always)]
    fn add(&self, first: usize, samples: &mut [Complex32], lanes: usize) {
        let correlations = self.correlations;
        let sigma = self.sigma.lanes(first);
        for pair in 0..correlations.div_ceil(2) {
            let words = philox::philox4x32_10_lanes(counters(self.row, first, pair), self.key);
            let both = correlations - 2 * pair >= 2;
            let [re0, im0] = scaled_draws(&words[0], &words[1], &sigma);
            let [re1, im1] = if both {
                scaled_draws(&words[2], &words[3], &sigma)
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
/// from `first`: the row (two words), the channel, and the correlation pair
/// (correlations 0 and 1 are pair 0, 2 and 3 pair 1).
fn counters(row: u64, first: usize, pair: usize) -> philox::Lanes<LANES> {
    [
        [row as u32; LANES],
        [(row >> 32) as u32; LANES],
        std::array::from_fn(|lane| (first + lane) as u32),
        [pair as u32; LANES],
    ]
}

/// The real and imaginary noise of `LANES` samples from their two words
/// each: Box–Muller's `(r·σ)·cos θ` and `(r·σ)·sin θ`, in `f32`. Scaling
/// the radius once saves a multiplication per sample over scaling each
/// normal, with the same number of roundings.
///
/// Each lane computes the products as a draw made alone does
/// ([`scaled_draw`]), but in two passes, the radii and then the angles:
/// each pass's polynomial coefficients and running sums then fit the vector
/// registers together, which keeps more lanes' arithmetic in flight.
#[inline(always)]
fn scaled_draws<const LANES: usize>(
    first: &[u32; LANES],
    second: &[u32; LANES],
    sigma: &[f32; LANES],
) -> [[f32; LANES]; 2] {
    let mut radii = [0.0; LANES];
    for ((radius, &word), &sigma) in radii.iter_mut().zip(first).zip(sigma) {
        *radius = normal::radius(word) * sigma;
    }
    let mut draws = [[0.0; LANES]; 2];
    for lane in 0..LANES {
        let (sin, cos) = normal::sin_cos_turn(second[lane]);
        draws[0][lane] = radii[lane] * cos;
        draws[1][lane] = radii[lane] * sin;
    }
    draws
}

/// The four standard normals of one draw, slot 0's (real, imaginary) then
/// slot 1's, as [`CellNoise::add_to_row`] uses them.
#[cfg(test)]
fn standard_normals(noise: &CellNoise, row: u64, channel: usize, pair: usize) -> [f32; 4] {
    let counter = [row as u32, (row >> 32) as u32, channel as u32, pair as u32];
    let words = philox::philox4x32_10(counter, noise.key());
    let (a, b) = box_muller(words[0], words[1]);
    let (c, d) = box_muller(words[2], words[3]);
    [a, b, c, d]
}
