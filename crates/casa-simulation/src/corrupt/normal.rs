// SPDX-License-Identifier: LGPL-3.0-or-later
//! Standard normals from pairs of 32-bit words: Box–Muller, evaluated on
//! the words themselves.
//!
//! With `u₁ = (w₀ + 1)·2⁻³²` in (0, 1] and `θ = 2π·w₁·2⁻³²`, the pair
//! `√(−2 ln u₁)·(cos θ, sin θ)` is two independent standard normals. The
//! logarithm and the sine and cosine are computed here rather than by the
//! platform's `libm`, for two reasons:
//! - **Speed.** The angle's octant is the word's top three bits, so the
//!   sine and cosine need only a short polynomial on [0, π/4] and no range
//!   reduction; the logarithm splits `u₁` into exponent and mantissa and
//!   sums an `atanh` series. A draw costs about half what `ln` and
//!   `sin_cos` cost, which is most of the noise pass.
//! - **Reproducibility.** Only IEEE-754 additions, multiplications, one
//!   division and a square root are used, all correctly rounded, so a draw
//!   is bitwise the same on every platform.
//!
//! Both are accurate to a few units in the last place of `f64` (the tests
//! compare them with `std` over 2²⁰ words), far below the `f32` the
//! visibilities are stored in.

use std::f64::consts::{FRAC_PI_4, LN_2, SQRT_2};

/// 2⁻³², a 32-bit word's scale to [0, 1).
const WORD_SCALE: f64 = 1.0 / 4_294_967_296.0;
/// Words per octant of the angle: 2²⁹.
const OCTANT_WORDS: u32 = 1 << 29;

/// Two independent standard normals from two uniform words.
#[inline(always)]
pub(super) fn box_muller(first: u32, second: u32) -> (f64, f64) {
    let radius = (-2.0 * ln_uniform(first)).sqrt();
    let (sin, cos) = sin_cos_turn(second);
    (radius * cos, radius * sin)
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
fn sin_cos_turn(word: u32) -> (f64, f64) {
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

#[cfg(test)]
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
