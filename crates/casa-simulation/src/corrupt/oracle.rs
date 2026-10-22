// SPDX-License-Identifier: LGPL-3.0-or-later
//! The noise path as it was before draws were batched across channels and
//! made in single precision (#855): one Philox counter and one Box–Muller
//! pair at a time, in `f64`, copied from commit `0cf4a8994a`. Since then
//! [`radius`], [`sin_cos_turn`] and [`standard_normals`] are exposed to the
//! tests, and the tests that held the logarithm, sine and cosine to `std`
//! live here.
//!
//! It is the reference the single-precision [`NoiseApplication::add_to_row`]
//! must stay within the bounds of [`super::normal`] of, and it calls nothing
//! the noise path uses, so a change to the generator or the key, or one that
//! moves a draw by more than those bounds, fails the comparison. Changing
//! the draws is a science change; this file changes only with one.

use std::f64::consts::{FRAC_PI_4, LN_2, SQRT_2};

use num_complex::Complex32;

use super::{MAXIMUM_APPLICATIONS, NoiseApplication, PURPOSE_THERMAL_NOISE};

/// Philox's 128-bit counter.
type Counter = [u32; 4];
/// Philox's 64-bit key.
type Key = [u32; 2];

/// `PHILOX_M4x32_0` and `PHILOX_M4x32_1`.
const MULTIPLIERS: [u32; 2] = [0xD251_1F53, 0xCD9E_8D57];
/// `PHILOX_W32_0` and `PHILOX_W32_1`: the golden ratio and √3 − 1 as 32-bit
/// fractions.
const WEYL: [u32; 2] = [0x9E37_79B9, 0xBB67_AE85];

/// The high and low words of `a × b`.
#[inline(always)]
fn multiply_high_low(a: u32, b: u32) -> (u32, u32) {
    let product = u64::from(a) * u64::from(b);
    ((product >> 32) as u32, product as u32)
}

/// One round (`_philox4x32round`).
#[inline(always)]
fn round(counter: Counter, key: Key) -> Counter {
    let (high0, low0) = multiply_high_low(MULTIPLIERS[0], counter[0]);
    let (high1, low1) = multiply_high_low(MULTIPLIERS[1], counter[2]);
    [
        high1 ^ counter[1] ^ key[0],
        low1,
        high0 ^ counter[3] ^ key[1],
        low0,
    ]
}

/// Philox4x32 with `ROUNDS` rounds: the first with `key`, each later one
/// with the key bumped once more by the Weyl constants.
#[inline(always)]
fn philox4x32<const ROUNDS: usize>(counter: Counter, key: Key) -> Counter {
    let mut counter = counter;
    let mut key = key;
    for index in 0..ROUNDS {
        if index > 0 {
            key = [key[0].wrapping_add(WEYL[0]), key[1].wrapping_add(WEYL[1])];
        }
        counter = round(counter, key);
    }
    counter
}

/// Philox4x32-10, Random123's recommended round count.
#[inline(always)]
fn philox4x32_10(counter: Counter, key: Key) -> Counter {
    philox4x32::<10>(counter, key)
}

/// 2⁻³², a 32-bit word's scale to [0, 1).
const WORD_SCALE: f64 = 1.0 / 4_294_967_296.0;
/// Words per octant of the angle: 2²⁹.
const OCTANT_WORDS: u32 = 1 << 29;

/// Two independent standard normals from two uniform words.
#[inline(always)]
pub(super) fn box_muller(first: u32, second: u32) -> (f64, f64) {
    let radius = radius(first);
    let (sin, cos) = sin_cos_turn(second);
    (radius * cos, radius * sin)
}

/// Box–Muller's radius `√(−2 ln u₁)` from the first word.
#[inline(always)]
pub(super) fn radius(word: u32) -> f64 {
    (-2.0 * ln_uniform(word)).sqrt()
}

/// `ln((w + 1)·2⁻³²)`.
///
/// `u = (w + 1)·2⁻³²` is exact. Writing `u = m·2ᵉ` with `m` in [√½, √2),
/// `ln u = e·ln 2 + 2 atanh(z)` with `z = (m − 1)/(m + 1)`, `|z| < 0.172`,
/// and the series `atanh z = z (1 + z²/3 + z⁴/5 + …)` converges to `f64`
/// precision by z¹⁹. Near u = 1 the result keeps its relative precision,
/// since `m − 1` is exact there.
#[inline(always)]
fn ln_uniform(word: u32) -> f64 {
    let u = (f64::from(word) + 1.0) * WORD_SCALE;
    let bits = u.to_bits();
    let exponent = ((bits >> 52) as i64 - 1023) as f64;
    let mantissa = f64::from_bits((bits & 0x000F_FFFF_FFFF_FFFF) | 0x3FF0_0000_0000_0000);
    let (mantissa, exponent) = if mantissa > SQRT_2 {
        (mantissa * 0.5, exponent + 1.0)
    } else {
        (mantissa, exponent)
    };
    let z = (mantissa - 1.0) / (mantissa + 1.0);
    let z2 = z * z;
    let series = 1.0
        + z2 * (1.0 / 3.0
            + z2 * (1.0 / 5.0
                + z2 * (1.0 / 7.0
                    + z2 * (1.0 / 9.0
                        + z2 * (1.0 / 11.0
                            + z2 * (1.0 / 13.0
                                + z2 * (1.0 / 15.0 + z2 * (1.0 / 17.0 + z2 * (1.0 / 19.0)))))))));
    exponent * LN_2 + 2.0 * z * series
}

/// `(sin θ, cos θ)` for `θ = 2π·w·2⁻³²`.
///
/// The top three bits of `w` are the octant. Within it, the angle from the
/// nearer quadrant axis is `x` in [0, π/4], taken exactly from the low bits;
/// the Taylor series of `sin x` to x¹⁵ and `cos x` to x¹⁶ are accurate to
/// rounding there. The octant then swaps and negates them.
#[inline(always)]
pub(super) fn sin_cos_turn(word: u32) -> (f64, f64) {
    let octant = word >> 29;
    let within = word & (OCTANT_WORDS - 1);
    let odd = octant & 1 == 1;
    // In an odd octant measure from the next quadrant axis instead.
    let steps = if odd { OCTANT_WORDS - within } else { within };
    let x = f64::from(steps) * (FRAC_PI_4 / f64::from(OCTANT_WORDS));
    let x2 = x * x;
    let sin = x
        * (1.0
            + x2 * (-1.0 / 6.0
                + x2 * (1.0 / 120.0
                    + x2 * (-1.0 / 5_040.0
                        + x2 * (1.0 / 362_880.0
                            + x2 * (-1.0 / 39_916_800.0
                                + x2 * (1.0 / 6_227_020_800.0
                                    + x2 * (-1.0 / 1_307_674_368_000.0))))))));
    let cos = 1.0
        + x2 * (-0.5
            + x2 * (1.0 / 24.0
                + x2 * (-1.0 / 720.0
                    + x2 * (1.0 / 40_320.0
                        + x2 * (-1.0 / 3_628_800.0
                            + x2 * (1.0 / 479_001_600.0
                                + x2 * (-1.0 / 87_178_291_200.0
                                    + x2 * (1.0 / 20_922_789_888_000.0))))))));
    // The angle within the quadrant, and its sine and cosine.
    let (sin, cos) = if odd { (cos, sin) } else { (sin, cos) };
    match octant >> 1 {
        0 => (sin, cos),
        1 => (cos, -sin),
        2 => (-sin, -cos),
        _ => (-cos, sin),
    }
}

/// The stream key of `application`: its seed, and its ordinal under the
/// thermal-noise purpose byte.
fn key(application: &NoiseApplication) -> Key {
    debug_assert!(application.ordinal < MAXIMUM_APPLICATIONS);
    [
        application.seed as u32,
        (PURPOSE_THERMAL_NOISE << 24) | application.ordinal,
    ]
}

/// The counter of a draw: the MAIN row (two words), the channel, and the
/// correlation pair (correlations 0 and 1 are pair 0, 2 and 3 pair 1).
fn counter(row: u64, channel: usize, pair: usize) -> Counter {
    [row as u32, (row >> 32) as u32, channel as u32, pair as u32]
}

/// The four standard normals of the draw of (row, channel, pair): slot 0's
/// (real, imaginary) then slot 1's.
pub(super) fn standard_normals(
    application: &NoiseApplication,
    row: u64,
    channel: usize,
    pair: usize,
) -> [f64; 4] {
    let words = philox4x32_10(counter(row, channel, pair), key(application));
    let (a, b) = box_muller(words[0], words[1]);
    let (c, d) = box_muller(words[2], words[3]);
    [a, b, c, d]
}

/// `application`'s noise added to the cell of MAIN row `row`, one draw at a
/// time: `NoiseApplication::add_to_row` as it was.
pub(super) fn add_to_row(
    application: &NoiseApplication,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    cell: &mut [Complex32],
) {
    // Adding σ·N with σ = 0 changes nothing but the sign of a zero.
    if application.sigma_jy == 0.0 || correlations == 0 {
        return;
    }
    let sigma = f64::from(application.sigma_jy);
    let key = key(application);
    for (channel, samples) in cell.chunks_exact_mut(correlations).enumerate() {
        for (pair, slots) in samples.chunks_mut(2).enumerate() {
            let words = philox4x32_10(counter(row, channel, pair), key);
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

/// The oracle's logarithm, sine and cosine against `std`, so that the
/// single-precision draws are measured against the right values; the
/// tests the double-precision draws had before #855 moved them here.
mod tests {
    use super::*;

    /// 2²⁰ words spread over the whole range, and the edges.
    fn words() -> impl Iterator<Item = u32> {
        (0..1_u32 << 20)
            .map(|index| index.wrapping_mul(0x9E37_79B9) ^ (index >> 7))
            .chain([
                0,
                1,
                u32::MAX,
                u32::MAX - 1,
                1 << 31,
                OCTANT_WORDS - 1,
                OCTANT_WORDS,
                OCTANT_WORDS + 1,
                7 * OCTANT_WORDS,
            ])
    }

    #[test]
    fn the_logarithm_agrees_with_std() {
        for word in words() {
            let expected = ((f64::from(word) + 1.0) * WORD_SCALE).ln();
            let found = ln_uniform(word);
            if expected == 0.0 {
                assert_eq!(found, 0.0, "word {word:#x}");
            } else {
                let relative = ((found - expected) / expected).abs();
                assert!(
                    relative < 2e-15,
                    "word {word:#x}: {found} against {expected}"
                );
            }
        }
    }

    #[test]
    fn the_sine_and_cosine_agree_with_std() {
        for word in words() {
            let (sin, cos) = (std::f64::consts::TAU * f64::from(word) * WORD_SCALE).sin_cos();
            let (found_sin, found_cos) = sin_cos_turn(word);
            assert!(
                (found_sin - sin).abs() < 1e-15 && (found_cos - cos).abs() < 1e-15,
                "word {word:#x}: ({found_sin}, {found_cos}) against ({sin}, {cos})"
            );
        }
    }

    #[test]
    fn the_radius_is_zero_at_u_one_and_bounded_at_the_smallest_u() {
        assert_eq!(box_muller(u32::MAX, 0), (0.0, 0.0));
        let (largest, zero) = box_muller(0, 0);
        assert_eq!(zero, 0.0);
        assert!((largest - (64.0 * LN_2).sqrt()).abs() < 1e-14);
    }
}
