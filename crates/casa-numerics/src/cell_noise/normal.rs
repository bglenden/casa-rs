// SPDX-License-Identifier: LGPL-3.0-or-later
//! Standard normals from pairs of 32-bit words: Box–Muller in single
//! precision, evaluated on the words themselves.
//!
//! With `u₁ = (w₀ + 1)·2⁻³²` in (0, 1] and `θ = 2π·w₁·2⁻³²`, the pair
//! `√(−2 ln u₁)·(cos θ, sin θ)` is two independent standard normals. The
//! logarithm and the sine and cosine are computed here rather than by the
//! platform's `libm`, for two reasons:
//! - **Speed.** The angle's octant is the word's top three bits, so the
//!   sine and cosine need only a short polynomial on [0, π/4] and no range
//!   reduction; the logarithm splits `u₁` into exponent and mantissa with
//!   integer operations and sums a short `atanh` series. In `f32`, a vector
//!   register holds four lanes rather than two, and the series are about
//!   half as long as `f64` needs (#855).
//! - **Reproducibility.** Only IEEE-754 additions, multiplications, one
//!   division and a square root are used, all correctly rounded and none
//!   fused, so a draw is bitwise the same on every platform.
//!
//! # Accuracy
//!
//! The draws replace the double-precision draws the same words gave before
//! (#855; the tests' `oracle` keeps them), and differ from them by less
//! than the `f32` the visibilities are stored in resolves. With `ε = 2⁻²⁴`,
//! the unit roundoff of `f32`, the largest errors against the
//! double-precision code over all 2³² words are
//! - 2.003ε for the radius, relatively;
//! - 2.892ε for the sine and for the cosine, relatively.
//!
//! The radius depends on the first word only and the angle on the second
//! only, so a normal `r·cos θ` or `r·sin θ`, rounded once more, is within
//! `2.003ε + 2.892ε + ε < 6ε` of the double-precision normal, relatively,
//! for every pair of words. That is `3·2⁻²³`, within #855's acceptance bound
//! `|n₃₂ − n₆₄| ≤ 4·2⁻²³·max(|n₆₄|, 1)`. Over 2³² pairs, each word with a
//! partner so that every word appears once in each role, the largest error
//! is 5.18ε, or 2.49 in units of `2⁻²³·max(|n₆₄|, 1)`, and at most four
//! units in the last place of the double-precision normal rounded to `f32`.
//! Of those 2³³ normals, 56 % are that rounded value exactly and 97 % are
//! within a unit of it. An ignored test measures these over every word, and
//! the regular tests hold 2²⁰ words and the edges to the bounds.
//!
//! The errors come from a few roundings:
//! - The radius. `f = m − 1` is an integer of up to 31 bits rounded once to
//!   `f32`, by up to ε. For u₁ in [√½, 1), `ln u₁ = ln(1 + f)` carries that
//!   rounding magnified by `f/((1 + f)·ln(1 + f))`, at most 1.2, and the
//!   logarithm's own roundings add about ε; the square root halves the
//!   logarithm's error and rounds once more. The largest, at
//!   `w₀ = 0xdfc7501f`, is a word whose `f` rounds from an exact tie.
//! - The angle. `t`, the angle's fraction of its octant, rounds once when
//!   its integer needs more than 24 bits; π/4, the series' leading
//!   coefficient, rounds by 0.47ε; and the series' last addition and its
//!   product with `t` round once each.
//!
//! Each bound is relative, down to the smallest values, because each part
//! is computed from exact integers where precision matters:
//! - Near u₁ = 1 the radius is small, and `ln u₁` must keep its relative
//!   precision. It does, because `m − 1`, the mantissa less one, is formed
//!   as an integer (for u₁ near 1, `1 − u₁ = (2³² − 1 − w₀)·2⁻³²`) and
//!   rounded once to `f32`, rather than computed from `u₁` rounded to
//!   `f32`, which near 1 would lose every bit of `1 − u₁` below 2⁻²⁴.
//! - The exponent comes from the word's leading zeros, so the largest
//!   radius, `√(64 ln 2)` at `w₀ = 0`, is right; it comes out as that value
//!   correctly rounded.
//! - The angle within its octant is taken from the low bits as an integer,
//!   so a sine or cosine near zero is accurate relative to itself.
//!
//! Nothing here branches on the words: the octant and the mantissa range
//! pick values by selection and sign flips. Applied lane by lane to several
//! draws ([`super::CellNoise::add_to_row`] takes [`super::LANES`] channels
//! at a time), the same operations run in vector registers, each lane
//! rounding exactly as a draw made alone.

use std::f64::consts::{FRAC_PI_4, LN_2};

/// 2⁻³², a 32-bit word's scale to [0, 1); a power of two, exact in `f32`.
pub(super) const WORD_SCALE: f32 = 1.0 / 4_294_967_296.0;
/// Words per octant of the angle: 2²⁹.
pub(super) const OCTANT_WORDS: u32 = 1 << 29;
/// ⌈√½·2³²⌉: a word normalised to [2³¹, 2³²) at or above this, read as a
/// fraction of 2³², is in [√½, 1).
pub(super) const SQRT_HALF_WORD: u32 = 3_037_000_500;
/// ln 2 to 17 significant bits, so that `k·LN_2_HIGH` is exact for any
/// exponent `k` below 2⁷ in magnitude (fdlibm's `ln2_hi`).
pub(super) const LN_2_HIGH: f32 = f32::from_bits(0x3F31_7180);
/// The rest of ln 2.
pub(super) const LN_2_LOW: f32 = (LN_2 - LN_2_HIGH as f64) as f32;

/// The Taylor coefficients of `atanh`'s series in `ln(1 + f) = 2 atanh s`,
/// beyond the first: `2/3, 2/5, 2/7, 2/9`.
pub(super) const ATANH: [f32; 4] = [2.0 / 3.0, 2.0 / 5.0, 2.0 / 7.0, 2.0 / 9.0];

/// `(π/4)ᵏ/k!`, rounded to `f32`: the magnitude of the coefficient of `tᵏ`
/// in the Taylor series of `sin(πt/4)` (k odd) or `cos(πt/4)` (k even).
const fn turn_coefficient(k: u32) -> f32 {
    let mut value = 1.0_f64;
    let mut index = 1;
    while index <= k {
        value *= FRAC_PI_4 / index as f64;
        index += 1;
    }
    value as f32
}

/// `sin(πt/4)/t` in powers of t², to t⁸: the Taylor series to t⁹.
pub(super) const SINE: [f32; 5] = [
    turn_coefficient(1),
    -turn_coefficient(3),
    turn_coefficient(5),
    -turn_coefficient(7),
    turn_coefficient(9),
];
/// `cos(πt/4)` in powers of t², from t² to t¹⁰: the Taylor series to t¹⁰.
pub(super) const COSINE: [f32; 5] = [
    -turn_coefficient(2),
    turn_coefficient(4),
    -turn_coefficient(6),
    turn_coefficient(8),
    -turn_coefficient(10),
];

/// Two independent standard normals from two uniform 32-bit words:
/// `(r·cos θ, r·sin θ)` with `r = √(−2 ln u₁)`, `u₁ = (first + 1)·2⁻³²` in
/// (0, 1], and `θ = 2π·second·2⁻³²`.
///
/// Evaluated in `f32` on the words themselves: the logarithm splits `u₁`
/// into exponent and mantissa with integer operations and sums a short
/// `atanh` series, and the angle's octant is the word's top three bits, so
/// the sine and cosine need only short polynomials on [0, π/4] and no range
/// reduction. Only correctly rounded IEEE-754 operations are used, none
/// fused, so the result is bitwise the same on every platform. Each normal
/// is within 6·2⁻²⁴ of the double-precision Box–Muller of the same words,
/// relatively. `first = u32::MAX` (u₁ = 1) gives a radius of −0, and
/// `first = 0` the largest, `√(64 ln 2)` correctly rounded.
///
/// The noise [`CellNoise::add_to_row`](super::CellNoise::add_to_row) adds
/// is these normals scaled by σ, as [`scaled_draw`] computes them.
#[must_use]
pub fn box_muller(first: u32, second: u32) -> (f32, f32) {
    let radius = radius(first);
    let (sin, cos) = sin_cos_turn(second);
    (radius * cos, radius * sin)
}

/// The real and imaginary noise of one sample from its two words, one draw
/// at a time: `(r·σ)·cos θ` and `(r·σ)·sin θ`, with `r` and `θ` as in
/// [`box_muller`].
///
/// Scaling the radius before the products saves a multiplication per
/// sample over scaling each normal, with the same number of roundings; the
/// result can differ from σ times [`box_muller`]'s normal in its last bits.
/// [`CellNoise::add_to_row`](super::CellNoise::add_to_row) computes exactly
/// these products, lane by lane, so a draw made here is bit for bit the
/// noise it adds to the sample (to a zero sample; to any other, the sum
/// rounds once more).
#[must_use]
pub fn scaled_draw(first: u32, second: u32, sigma: f32) -> (f32, f32) {
    let scaled = radius(first) * sigma;
    let (sin, cos) = sin_cos_turn(second);
    (scaled * cos, scaled * sin)
}

/// Box–Muller's radius `√(−2 ln u₁)` from the first word.
#[inline(always)]
pub(super) fn radius(word: u32) -> f32 {
    (-2.0 * ln_uniform(word)).sqrt()
}

/// `ln((w + 1)·2⁻³²)`.
///
/// With `v = w + 1` shifted left by its leading zeros to `n` in [2³¹, 2³²),
/// `u = n·2⁻³²·2⁻ˢʰⁱᶠᵗ`. Writing `u = m·2ᵏ` with `m` in [√½, √2) (`m` is
/// `n·2⁻³²` when that is at least √½, else `n·2⁻³¹`), `f = m − 1` is an
/// integer (`n − 2³²` or `n − 2³¹`) times a power of two, rounded once to
/// `f32`. Then `ln u = k·ln 2 + ln(1 + f)` and, with `s = f/(2 + f)` in
/// (−0.172, 0.172),
///
/// `ln(1 + f) = 2 atanh s = f − (f²/2 − s·(f²/2 + R))`, `R = 2s²/3 + 2s⁴/5 + …`
///
/// (fdlibm's `logf` arrangement, with Taylor coefficients): the leading `f`
/// is added last, the correction is small, and `R`'s series to s⁸ is
/// accurate to `f32` rounding (its next term, `2s¹⁰/11`, contributes at
/// most 2·10⁻⁹ of `ln(1 + f)`, 0.03ε). `k·ln 2` is split so that its large
/// part is exact.
///
/// At `w = u32::MAX`, `v` wraps to 0, which takes no shift and gives
/// `f = −0`, `k = 0` and `ln u = +0`, as u = 1 should.
#[inline(always)]
fn ln_uniform(word: u32) -> f32 {
    let v = word.wrapping_add(1);
    let shift = v.leading_zeros() & 31;
    let normal = v << shift;
    // m = normal·2⁻³² in [√½, 1), or u = 1 (normal 0); else m = normal·2⁻³¹.
    let upper = normal.wrapping_sub(1) >= SQRT_HALF_WORD - 1;
    let (numerator, scale) = if upper {
        (normal.wrapping_neg(), -WORD_SCALE)
    } else {
        (normal & 0x7FFF_FFFF, 2.0 * WORD_SCALE)
    };
    // Both numerators are below 2³¹, so they convert as signed integers.
    let f = numerator as i32 as f32 * scale;
    let k = (-(shift as i32) - i32::from(!upper)) as f32;
    let s = f / (2.0 + f);
    let z = s * s;
    let r = z * (ATANH[0] + z * (ATANH[1] + z * (ATANH[2] + z * ATANH[3])));
    let half_square = 0.5 * f * f;
    k * LN_2_HIGH - ((half_square - (s * (half_square + r) + k * LN_2_LOW)) - f)
}

/// `(sin θ, cos θ)` for `θ = 2π·w·2⁻³²`.
///
/// The top three bits of `w` are the octant. Within it, the angle from the
/// nearer quadrant axis is `x = πt/4` with `t` in [0, 1], taken from the
/// low bits as an integer (rounded once to `f32` when it needs more than 24
/// bits, so exact for small angles) and scaled by a power of two; the
/// Taylor series of `sin(πt/4)` to t⁹ and `cos(πt/4)` to t¹⁰ are accurate
/// to `f32` rounding there (their next terms are below 3·10⁻⁹ of the
/// result). Evaluating in `t` rather than `x` folds π/4 into the
/// coefficients and saves the rounding of `x`. The octant then swaps and
/// negates them.
///
/// The octant's bits are three reflections. In an odd octant `x` is measured
/// back from the next quadrant axis, which swaps sine and cosine; an odd
/// quadrant (θ advanced by π/2) swaps them again, so they end swapped when
/// exactly one of the two holds. The sine is negative in octants 4 to 7
/// (θ ≥ π) and the cosine in octants 2 to 5 (π/2 ≤ θ < 3π/2). Each sign is
/// applied by flipping the sign bit, which is what IEEE-754 negation does,
/// zeros included.
#[inline(always)]
pub(super) fn sin_cos_turn(word: u32) -> (f32, f32) {
    let octant = word >> 29;
    let within = word & (OCTANT_WORDS - 1);
    let odd = octant & 1 == 1;
    // In an odd octant measure from the next quadrant axis instead.
    let steps = if odd { OCTANT_WORDS - within } else { within };
    // At most 2²⁹, so it converts as a signed integer.
    let t = steps as i32 as f32 * (1.0 / OCTANT_WORDS as f32);
    let t2 = t * t;
    let sin = t * (SINE[0] + t2 * (SINE[1] + t2 * (SINE[2] + t2 * (SINE[3] + t2 * SINE[4]))));
    let cos = 1.0
        + t2 * (COSINE[0]
            + t2 * (COSINE[1] + t2 * (COSINE[2] + t2 * (COSINE[3] + t2 * COSINE[4]))));
    let swapped = (octant ^ (octant >> 1)) & 1 == 1;
    let (sin, cos) = if swapped { (cos, sin) } else { (sin, cos) };
    (
        negate_if(sin, octant >> 2 == 1),
        negate_if(cos, (octant + 2) >> 2 == 1),
    )
}

/// `−x` when `negate`, else `x`, bit for bit: the sign bit flipped, as IEEE-754
/// negation flips it.
#[inline(always)]
fn negate_if(x: f32, negate: bool) -> f32 {
    f32::from_bits(x.to_bits() ^ (u32::from(negate) << 31))
}

/// `f32`'s unit roundoff, 2⁻²⁴: half a unit in the last place of 1.
#[cfg(test)]
pub(super) const EPSILON: f64 = 1.0 / 16_777_216.0;
/// The radius's largest error against the double-precision radius, in
/// [`EPSILON`], relative: 2.0031 over all 2³² words, rounded up.
#[cfg(test)]
pub(super) const RADIUS_BOUND: f64 = 2.1;
/// The sine's and cosine's largest error against the double-precision
/// ones, in [`EPSILON`], relative: 2.8919 over all 2³² words, rounded up.
#[cfg(test)]
pub(super) const ANGLE_BOUND: f64 = 2.9;
/// A normal's largest error against the double-precision normal, in
/// [`EPSILON`], relative: the radius's, the angle's, and the rounding of
/// their product (the second-order terms are below 10⁻⁶ ε). Scaling by σ
/// rounds once more.
#[cfg(test)]
pub(super) const NORMAL_BOUND: f64 = RADIUS_BOUND + ANGLE_BOUND + 1.0;

/// The error of `found` against `expected`, in [`EPSILON`], relative; an
/// exact zero must be found exactly, its sign included, or the error is
/// infinite.
#[cfg(test)]
pub(super) fn relative_error(found: f32, expected: f64) -> f64 {
    if expected == 0.0 {
        let same = found.to_bits() == (expected as f32).to_bits();
        return if same { 0.0 } else { f64::INFINITY };
    }
    ((f64::from(found) - expected) / expected).abs() / EPSILON
}

/// The spacing of `f32` just above `|x|`: a unit in the last place of `x`.
#[cfg(test)]
pub(super) fn ulp(x: f32) -> f64 {
    let x = x.abs();
    f64::from(f32::from_bits(x.to_bits() + 1)) - f64::from(x)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_noise::oracle;

    /// Words where the arithmetic is delicate: u₁ = 1 and its neighbours,
    /// every power of two and its neighbours (where the exponent steps), the
    /// mantissa's switch at √½, and every octant boundary and its
    /// neighbours.
    fn edge_words() -> Vec<u32> {
        let mut words = vec![
            0,
            1,
            2,
            u32::MAX,
            u32::MAX - 1,
            u32::MAX - 2,
            SQRT_HALF_WORD - 2,
            SQRT_HALF_WORD - 1,
            SQRT_HALF_WORD,
        ];
        for bit in 0..32 {
            let power = 1_u32 << bit;
            words.extend([
                power.wrapping_sub(2),
                power - 1,
                power,
                power.wrapping_add(1),
            ]);
            // u₁ just above and below √½ times a power of two.
            words.extend([(SQRT_HALF_WORD >> bit) - 1, SQRT_HALF_WORD >> bit]);
        }
        for octant in 0..8_u32 {
            let start = octant << 29;
            words.extend([start, start.wrapping_sub(1), start + 1, start + (1 << 24)]);
        }
        words
    }

    /// 2²⁰ words spread over the whole range, and the edges.
    fn words() -> impl Iterator<Item = u32> {
        (0..1_u32 << 20)
            .map(|index| index.wrapping_mul(0x9E37_79B9) ^ (index >> 7))
            .chain(edge_words())
    }

    /// The angle word paired with radius word `word` when normals are
    /// surveyed: a bijection, so that over every radius word every angle
    /// word appears once too.
    fn partner(word: u32) -> u32 {
        word.wrapping_mul(0x9E37_79B9).rotate_left(16)
    }

    /// `candidate` into `worst` when it is the larger error, or not a number.
    fn keep_largest(worst: &mut (f64, u32), candidate: (f64, u32)) {
        if candidate.0.is_nan() || candidate.0 > worst.0 {
            *worst = candidate;
        }
    }

    /// The largest errors against double precision over a run of words,
    /// each with the word it was found at: of each word's radius, sine and
    /// cosine, and of the two normals of each word with its [`partner`].
    #[derive(Clone, Copy, Default)]
    struct Survey {
        /// The radius's, the sine's and the cosine's, relative, in ε.
        parts: [(f64, u32); 3],
        /// The normals', relative, in ε.
        normal: (f64, u32),
        /// The normals', in units of 2⁻²³·max(|n|, 1) for the
        /// double-precision normal n: the measure of #855's acceptance
        /// bound, which allows 4.
        floored: (f64, u32),
        /// The normals', in units in the last place of n rounded to `f32`.
        ulps: (f64, u32),
        /// Normals that are n rounded to `f32`, bit for bit.
        rounded: u64,
        /// Normals within one unit in the last place of n rounded.
        within_one: u64,
        /// Normals surveyed.
        normals: u64,
    }

    impl Survey {
        fn of(words: impl Iterator<Item = u32>) -> Self {
            let mut survey = Self::default();
            for word in words {
                let (sin, cos) = sin_cos_turn(word);
                let (expected_sin, expected_cos) = oracle::sin_cos_turn(word);
                let errors = [
                    relative_error(radius(word), oracle::radius(word)),
                    relative_error(sin, expected_sin),
                    relative_error(cos, expected_cos),
                ];
                for (worst, error) in survey.parts.iter_mut().zip(errors) {
                    keep_largest(worst, (error, word));
                }
                let (re, im) = box_muller(word, partner(word));
                let (expected_re, expected_im) = oracle::box_muller(word, partner(word));
                survey.add_normal(re, expected_re, word);
                survey.add_normal(im, expected_im, word);
            }
            survey
        }

        fn add_normal(&mut self, found: f32, expected: f64, word: u32) {
            let rounded = expected as f32;
            let difference = (f64::from(found) - expected).abs();
            let ulps = if expected == 0.0 {
                relative_error(found, expected)
            } else {
                (f64::from(found) - f64::from(rounded)).abs() / ulp(rounded)
            };
            let floored = difference / (2.0 * EPSILON * expected.abs().max(1.0));
            keep_largest(&mut self.normal, (relative_error(found, expected), word));
            keep_largest(&mut self.floored, (floored, word));
            keep_largest(&mut self.ulps, (ulps, word));
            self.rounded += u64::from(found.to_bits() == rounded.to_bits());
            self.within_one += u64::from(ulps <= 1.0);
            self.normals += 1;
        }

        fn merge(mut self, other: Self) -> Self {
            for (worst, candidate) in self.parts.iter_mut().zip(other.parts) {
                keep_largest(worst, candidate);
            }
            keep_largest(&mut self.normal, other.normal);
            keep_largest(&mut self.floored, other.floored);
            keep_largest(&mut self.ulps, other.ulps);
            self.rounded += other.rounded;
            self.within_one += other.within_one;
            self.normals += other.normals;
            self
        }

        fn assert_within_bounds(&self) {
            let [radius, sin, cos] = self.parts;
            let checks = [
                ("radius", radius, RADIUS_BOUND),
                ("sine", sin, ANGLE_BOUND),
                ("cosine", cos, ANGLE_BOUND),
                ("normal", self.normal, NORMAL_BOUND),
                ("normal in #855's measure", self.floored, 4.0),
            ];
            for (name, (error, word), bound) in checks {
                assert!(error <= bound, "{name}: {error:.3} at word {word:#x}");
            }
        }

        fn report(&self) -> String {
            let [radius, sin, cos] = self.parts;
            let at =
                |(error, word): (f64, u32), unit: &str| format!("{error:.4} {unit} at {word:#x}");
            format!(
                "radius {}, sine {}, cosine {}; normals {} relative, {}, {} of n rounded; of {} \
                 normals, {} are n rounded and {} within an ulp of it",
                at(radius, "ε"),
                at(sin, "ε"),
                at(cos, "ε"),
                at(self.normal, "ε"),
                at(self.floored, "× 2⁻²³·max(|n|, 1)"),
                at(self.ulps, "ulp"),
                self.normals,
                self.rounded,
                self.within_one
            )
        }
    }

    #[test]
    fn the_radius_sine_cosine_and_normals_are_within_their_bounds_of_double_precision() {
        Survey::of(words()).assert_within_bounds();
    }

    #[test]
    fn the_radius_is_minus_zero_at_u_one_and_correctly_rounded_at_the_smallest_u() {
        // −2·ln 1 is −0, as in double precision, whose square root is −0.
        assert_eq!(radius(u32::MAX).to_bits(), (-0.0_f32).to_bits());
        assert_eq!(
            radius(u32::MAX).to_bits(),
            (oracle::radius(u32::MAX) as f32).to_bits()
        );
        assert_eq!(box_muller(u32::MAX, 0), (0.0, 0.0));
        let (largest, zero) = box_muller(0, 0);
        assert_eq!(zero, 0.0);
        assert_eq!(largest, (64.0 * LN_2).sqrt() as f32);
    }

    #[test]
    fn the_split_ln_2_sums_to_ln_2() {
        // To the low part's rounding: ln 2 to about 2⁻⁴¹.
        let sum = f64::from(LN_2_HIGH) + f64::from(LN_2_LOW);
        assert!((sum - LN_2).abs() <= EPSILON * f64::from(LN_2_LOW), "{sum}");
        // 17 significant bits: k·LN_2_HIGH is exact for |k| < 2⁷.
        assert_eq!(LN_2_HIGH.to_bits() & 0x7F, 0);
    }

    /// Every one of the 2³² words, and the 2³² normal pairs of each with its
    /// [`partner`], on all cores: the errors the module docs quote. Run it
    /// optimised:
    /// `cargo test --release -p casa-numerics --lib -- --ignored --nocapture every_word`.
    #[test]
    #[ignore = "2³² words; run with --release"]
    fn every_word_is_within_the_bounds() {
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let chunk = (1_u64 << 32).div_ceil(threads as u64);
        let survey = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads as u64)
                .map(|part| {
                    let start = part * chunk;
                    let end = (start + chunk).min(1 << 32);
                    scope.spawn(move || Survey::of((start..end).map(|word| word as u32)))
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .fold(Survey::default(), Survey::merge)
        });
        println!("over all 2³² words: {}", survey.report());
        survey.assert_within_bounds();
    }

    /// Words, and the bits of their radius, sine and cosine, as computed on
    /// aarch64 when the draws moved to single precision (#855): the ends of
    /// the range, the mantissa's switch at √½, the halfway word, octant
    /// boundaries, and three words with no structure.
    const KNOWN: [(u32, [u32; 3]); 16] = [
        (0x0000_0000, [0x40d5_224d, 0x0000_0000, 0x3f80_0000]),
        (0x0000_0001, [0x40d1_c6ff, 0x30c9_0fdb, 0x3f80_0000]),
        (0xffff_ffff, [0x8000_0000, 0xb0c9_0fdb, 0x3f80_0000]),
        (0xffff_fffe, [0x37b5_04f3, 0xb149_0fdb, 0x3f80_0000]),
        (0xb504_f333, [0x3f55_224d, 0xbf76_c251, 0xbe88_529f]),
        (0xb504_f334, [0x3f55_224d, 0xbf76_c251, 0xbe88_529f]),
        (0x7fff_ffff, [0x3f96_b55f, 0x30c9_0fdb, 0xbf80_0000]),
        (0x8000_0000, [0x3f96_b55f, 0x8000_0000, 0xbf80_0000]),
        (0x0001_0000, [0x4096_b558, 0x38c9_0fdb, 0x3f80_0000]),
        (0x1fff_ffff, [0x4002_8473, 0x3f35_04f4, 0x3f35_04f4]),
        (0x2000_0000, [0x4002_8473, 0x3f35_04f4, 0x3f35_04f4]),
        (0x6000_0001, [0x3fb3_469b, 0x3f35_04f4, 0xbf35_04f4]),
        (0xdfff_ffff, [0x3f04_4bc5, 0xbf35_04f4, 0x3f35_04f4]),
        (0x243f_6a88, [0x3ffd_173a, 0x3f46_dfda, 0x3f21_32d3]),
        (0x9e37_79b9, [0x3f7b_24e9, 0xbf2c_ecf0, 0xbf3c_c435]),
        (0xdead_beef, [0x3f07_31e7, 0xbf3a_cb26, 0x3f2f_0e07]),
    ];

    /// [`KNOWN`], bit for bit. The arithmetic is IEEE-754 with nothing
    /// fused, so every platform must give these bits; CI's x86-64 runners
    /// check that they do.
    #[test]
    fn known_words_give_their_known_bits_on_every_platform() {
        for (word, bits) in KNOWN {
            let (sin, cos) = sin_cos_turn(word);
            assert_eq!(
                [radius(word), sin, cos].map(f32::to_bits),
                bits,
                "word {word:#x}"
            );
        }
    }
}
