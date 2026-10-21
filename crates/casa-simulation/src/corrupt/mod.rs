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

mod normal;
mod philox;
#[cfg(test)]
mod tests;

use num_complex::Complex32;

use normal::box_muller;
use philox::Key;

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
        let sigma = f64::from(self.sigma_jy);
        let key = self.key();
        for (channel, samples) in cell.chunks_exact_mut(correlations).enumerate() {
            for (pair, slots) in samples.chunks_mut(2).enumerate() {
                let words = philox::philox4x32_10(counter(row, channel, pair), key);
                for (slot, sample) in slots.iter_mut().enumerate() {
                    let (re, im) = box_muller(words[2 * slot], words[2 * slot + 1]);
                    sample.re += (re * sigma) as f32;
                    if !autocorrelation {
                        sample.im += (im * sigma) as f32;
                    }
                }
            }
        }
    }
}

/// The counter of a draw: the MAIN row (two words), the channel, and the
/// correlation pair (correlations 0 and 1 are pair 0, 2 and 3 pair 1).
fn counter(row: u64, channel: usize, pair: usize) -> philox::Counter {
    [row as u32, (row >> 32) as u32, channel as u32, pair as u32]
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
    let words = philox::philox4x32_10(counter(row, channel, pair), application.key());
    let (a, b) = box_muller(words[0], words[1]);
    let (c, d) = box_muller(words[2], words[3]);
    [a, b, c, d]
}
