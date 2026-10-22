// SPDX-License-Identifier: LGPL-3.0-or-later
//! The quality of the noise draws, how [`CellNoise`] fills a cell, that the
//! batched path draws bit for bit what the same single-precision draws made
//! one at a time give, and that both stay within [`normal::NORMAL_BOUND`]
//! of the double-precision [`oracle`].
//!
//! Every statistic is over 2²⁰ draws and must lie within five standard
//! errors of its value for independent standard normals; the KS distance
//! must be below the 0.1 % critical value. The draws are deterministic, so a
//! pass is permanent; the margins are those of an honest test, not tuned to
//! these draws.

use std::hint::black_box;
use std::time::Instant;

use num_complex::Complex32;

use super::normal::{EPSILON, NORMAL_BOUND, relative_error, scaled_draw, ulp};
use super::*;

/// Draws per statistic.
const N: usize = 1 << 20;
/// The smallest positive `f32`, 2⁻¹⁴⁹: the spacing of the subnormals, which
/// bounds the absolute error of a product rounded into them.
const SUBNORMAL: f64 = 1.401_298_464_324_817e-45;
/// One more than the largest application ordinal casa-simulation puts in a
/// key's low 24 bits.
const ORDINALS: u32 = 1 << 24;

/// A key as casa-simulation forms its thermal-noise keys: the seed's 32
/// bits, then the purpose byte 0x01 above the application ordinal. Any key
/// would do; these keep the cases the tests had when the generator lived
/// in casa-simulation (#855).
fn key(seed: i32, ordinal: u32) -> [u32; 2] {
    debug_assert!(ordinal < ORDINALS);
    [seed as u32, (0x01 << 24) | ordinal]
}

/// σ = 1 under [`key`]`(seed, ordinal)`.
fn unit_noise(seed: i32, ordinal: u32) -> CellNoise {
    CellNoise::new(key(seed, ordinal), 1.0)
}

/// Mean, variance, skewness and excess kurtosis.
fn moments(values: &[f64]) -> [f64; 4] {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let central = |power: i32| values.iter().map(|v| (v - mean).powi(power)).sum::<f64>() / n;
    let variance = central(2);
    [
        mean,
        variance,
        central(3) / variance.powf(1.5),
        central(4) / (variance * variance) - 3.0,
    ]
}

fn correlation(left: &[f64], right: &[f64]) -> f64 {
    let [mean_l, var_l, ..] = moments(left);
    let [mean_r, var_r, ..] = moments(right);
    let covariance = left
        .iter()
        .zip(right)
        .map(|(l, r)| (l - mean_l) * (r - mean_r))
        .sum::<f64>()
        / left.len() as f64;
    covariance / (var_l * var_r).sqrt()
}

/// |r| below five standard errors of the correlation of independent draws.
/// Each check of this and the other statistics prints what it measured,
/// shown with `--nocapture`.
fn assert_uncorrelated(left: &[f64], right: &[f64], what: &str) {
    let r = correlation(left, right);
    let limit = 5.0 / (left.len() as f64).sqrt();
    println!("{what}: r = {r:.5}, limit {limit:.5}");
    assert!(r.abs() < limit, "{what}: r = {r:.5}, limit {limit:.5}");
}

/// The sample variance of `values` within five standard errors of
/// `expected`, the variance of the normal they are drawn from.
fn assert_variance(values: &[f64], expected: f64, what: &str) {
    let [_, variance, ..] = moments(values);
    let limit = 5.0 * expected * (2.0 / values.len() as f64).sqrt();
    println!("{what}: variance {variance:.5}, expected {expected} within {limit:.5}");
    assert!(
        (variance - expected).abs() < limit,
        "{what}: variance {variance:.5}, expected {expected} within {limit:.5}"
    );
}

/// Abramowitz and Stegun 7.1.26: erf(x) for x ≥ 0 within 1.5e-7.
fn erf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736
                + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    1.0 - poly * (-x * x).exp()
}

fn standard_normal_cdf(x: f64) -> f64 {
    let half = 0.5 * erf(x.abs() / std::f64::consts::SQRT_2);
    if x >= 0.0 { 0.5 + half } else { 0.5 - half }
}

/// The first draw (slot 0, real) of `N` consecutive rows from `start`.
fn first_draws(noise: &CellNoise, start: u64, channel: usize) -> Vec<f64> {
    (start..start + N as u64)
        .map(|row| f64::from(standard_normals(noise, row, channel, 0)[0]))
        .collect()
}

/// Every draw of rows 0..N/4, channel 0, pair 0.
fn all_draws() -> Vec<f64> {
    let noise = unit_noise(11_111, 0);
    (0..(N / 4) as u64)
        .flat_map(|row| standard_normals(&noise, row, 0, 0).map(f64::from))
        .collect()
}

/// The mean, variance, skewness and excess kurtosis of `values` each within
/// five standard errors of a standard normal's.
fn assert_standard_moments(values: &[f64], what: &str) {
    let [mean, variance, skewness, kurtosis] = moments(values);
    let n = values.len() as f64;
    println!(
        "{what}: {n} values, mean {mean:.5} (limit {:.5}), variance {variance:.5} (1 ± {:.5}), \
         skewness {skewness:.5} (limit {:.5}), excess kurtosis {kurtosis:.5} (limit {:.5})",
        5.0 / n.sqrt(),
        5.0 * (2.0 / n).sqrt(),
        5.0 * (6.0 / n).sqrt(),
        5.0 * (24.0 / n).sqrt()
    );
    assert!(mean.abs() < 5.0 / n.sqrt(), "{what}: mean {mean}");
    assert!(
        (variance - 1.0).abs() < 5.0 * (2.0 / n).sqrt(),
        "{what}: variance {variance}"
    );
    assert!(
        skewness.abs() < 5.0 * (6.0 / n).sqrt(),
        "{what}: skewness {skewness}"
    );
    assert!(
        kurtosis.abs() < 5.0 * (24.0 / n).sqrt(),
        "{what}: excess kurtosis {kurtosis}"
    );
}

/// The Kolmogorov–Smirnov distance of `values` from the standard normal
/// below the asymptotic 0.1 % critical value.
fn assert_standard_distribution(values: &[f64], what: &str) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len() as f64;
    let distance = sorted
        .iter()
        .enumerate()
        .map(|(index, &x)| {
            let cdf = standard_normal_cdf(x);
            (cdf - index as f64 / n).max((index + 1) as f64 / n - cdf)
        })
        .fold(0.0_f64, f64::max);
    let critical = 1.949 / n.sqrt();
    println!("{what}: KS D = {distance:.6}, critical {critical:.6}");
    assert!(
        distance < critical,
        "{what}: D = {distance:.6}, critical {critical:.6}"
    );
}

#[test]
fn draws_have_the_moments_of_a_standard_normal() {
    assert_standard_moments(&all_draws(), "standard normals");
}

#[test]
fn draws_pass_a_kolmogorov_smirnov_test_against_the_standard_normal() {
    let draws = all_draws();
    assert_standard_distribution(&draws, "standard normals");
    // Box–Muller's largest draw from 32-bit words, rounded to f32.
    let largest = draws
        .iter()
        .fold(0.0_f64, |largest, x| largest.max(x.abs()));
    assert!(largest <= f64::from((64.0 * std::f64::consts::LN_2).sqrt() as f32));
}

/// The noise `add_to_row` adds at σ = 0.1, about 10⁶ parts in each of
/// one, two and four correlations: divided by σ, standard normals by their
/// moments and distribution, real and imaginary parts uncorrelated. On
/// autocorrelation rows the real parts are the same and the imaginary
/// parts untouched.
#[test]
fn added_noise_is_normal_with_the_streams_sigma_in_every_shape() {
    const CHANNELS: usize = 64;
    let noise = CellNoise::new(key(2_718, 4), 0.1);
    let sigma = f64::from(noise.sigma());
    for correlations in [1, 2, 4] {
        let rows = (N / 2 / (CHANNELS * correlations)) as u64;
        let mut re = Vec::with_capacity(N / 2);
        let mut im = Vec::with_capacity(N / 2);
        for row in (1 << 32) - rows / 2..(1 << 32) + rows / 2 {
            let mut cross = vec![Complex32::new(0.0, 0.0); CHANNELS * correlations];
            let mut auto = cross.clone();
            noise.add_to_row(row, false, correlations, &mut cross);
            noise.add_to_row(row, true, correlations, &mut auto);
            for (cross, auto) in cross.iter().zip(&auto) {
                assert_eq!(auto.re.to_bits(), cross.re.to_bits());
                assert_eq!(auto.im.to_bits(), 0.0_f32.to_bits());
            }
            re.extend(cross.iter().map(|sample| f64::from(sample.re) / sigma));
            im.extend(cross.iter().map(|sample| f64::from(sample.im) / sigma));
        }
        let what = format!("{correlations} correlations, σ = {}", noise.sigma());
        assert_uncorrelated(&re, &im, &what);
        let parts = [re, im].concat();
        assert_standard_moments(&parts, &what);
        assert_standard_distribution(&parts, &what);
    }
}

#[test]
fn draws_are_independent_across_parts_slots_and_pairs() {
    let noise = unit_noise(11_111, 0);
    let rows = 0..N as u64;
    let pick = |pair: usize, slot: usize| -> Vec<f64> {
        rows.clone()
            .map(|row| f64::from(standard_normals(&noise, row, 3, pair)[slot]))
            .collect()
    };
    let (re0, im0, re1, im1) = (pick(0, 0), pick(0, 1), pick(0, 2), pick(0, 3));
    let re2 = pick(1, 0);
    assert_uncorrelated(&re0, &im0, "real and imaginary parts");
    assert_uncorrelated(&re1, &im1, "real and imaginary parts of slot 1");
    assert_uncorrelated(&re0, &re1, "correlations 0 and 1");
    assert_uncorrelated(&im0, &re1, "correlation 0 imaginary, 1 real");
    assert_uncorrelated(&re0, &re2, "correlations 0 and 2 (another pair)");
    // Independent parts: the squared parts are uncorrelated too, which a
    // dependence through the shared radius would break.
    let square = |values: &[f64]| values.iter().map(|v| v * v).collect::<Vec<_>>();
    assert_uncorrelated(
        &square(&re0),
        &square(&im0),
        "squared real and imaginary parts",
    );
}

/// Keys differing in either word, by one or in sign, draw independent
/// streams: the seeds and ordinals casa-simulation puts in them.
#[test]
fn draws_are_independent_across_rows_channels_and_keys() {
    let noise = unit_noise(11_111, 0);
    let base = first_draws(&noise, 0, 0);
    assert_uncorrelated(&base, &first_draws(&noise, 1, 0), "adjacent rows");
    assert_uncorrelated(&base, &first_draws(&noise, 0, 1), "adjacent channels");
    assert_uncorrelated(
        &base,
        &first_draws(&unit_noise(11_111, 1), 0, 0),
        "second key words one apart (ordinals 0 and 1)",
    );
    assert_uncorrelated(
        &base,
        &first_draws(&unit_noise(11_112, 0), 0, 0),
        "first key words one apart (seeds 11111 and 11112)",
    );
    assert_uncorrelated(
        &base,
        &first_draws(&unit_noise(-11_111, 0), 0, 0),
        "first key words of opposite sign (seeds 11111 and -11111)",
    );
    // Rows beyond 2³² use the counter's second word.
    let high = first_draws(&noise, 1 << 32, 0);
    assert_uncorrelated(&base, &high, "rows r and r + 2³²");
    assert_variance(&high, 1.0, "rows beyond 2³²");
}

/// A two-channel cell of `correlations`, every sample `value`.
fn cell(correlations: usize, value: Complex32) -> Vec<Complex32> {
    vec![value; 2 * correlations]
}

#[test]
fn a_cross_correlation_gets_sigma_times_each_draw_in_single_precision() {
    let noise = CellNoise::new(key(9, 2), 0.1);
    let mut samples = cell(4, Complex32::new(0.0, 0.0));
    noise.add_to_row(17, false, 4, &mut samples);
    for channel in 0..2 {
        for correlation in 0..4 {
            let counter = [17, 0, channel as u32, (correlation / 2) as u32];
            let words = philox::philox4x32_10(counter, noise.key());
            let slot = correlation % 2;
            let (re, im) = scaled_draw(words[2 * slot], words[2 * slot + 1], 0.1);
            let found = samples[channel * 4 + correlation];
            assert_eq!(
                (found.re.to_bits(), found.im.to_bits()),
                (re.to_bits(), im.to_bits())
            );
            // σ times the standard normal: (r·σ)·cos θ against (r·cos θ)·σ
            // is three roundings apart at most.
            let normals = standard_normals(&noise, 17, channel, correlation / 2);
            let sigma = f64::from(0.1_f32);
            for (part, normal) in [(re, normals[2 * slot]), (im, normals[2 * slot + 1])] {
                let exact = f64::from(normal) * sigma;
                assert!((f64::from(part) - exact).abs() <= 3.0 * EPSILON * exact.abs());
            }
        }
    }
}

#[test]
fn every_correlation_gets_its_own_draw_whatever_the_correlation_count() {
    let noise = unit_noise(5, 0);
    let mut four = cell(4, Complex32::new(0.0, 0.0));
    let mut two = cell(2, Complex32::new(0.0, 0.0));
    let mut one = cell(1, Complex32::new(0.0, 0.0));
    noise.add_to_row(3, false, 4, &mut four);
    noise.add_to_row(3, false, 2, &mut two);
    noise.add_to_row(3, false, 1, &mut one);
    for channel in 0..2 {
        let row = &four[channel * 4..channel * 4 + 4];
        for (index, sample) in row.iter().enumerate() {
            assert!(sample.re != 0.0 && sample.im != 0.0);
            assert!(row[index + 1..].iter().all(|other| other != sample));
        }
        assert_eq!(&two[channel * 2..channel * 2 + 2], &row[..2]);
        assert_eq!(one[channel], row[0]);
    }
}

#[test]
fn an_autocorrelation_gets_real_draws_only() {
    let noise = unit_noise(5, 0);
    let start = Complex32::new(1.5, -0.25);
    let mut auto = cell(4, start);
    let mut cross = cell(4, start);
    noise.add_to_row(8, true, 4, &mut auto);
    noise.add_to_row(8, false, 4, &mut cross);
    for (auto, cross) in auto.iter().zip(&cross) {
        assert_eq!(auto.im.to_bits(), start.im.to_bits());
        assert_eq!(auto.re, cross.re);
        assert_ne!(auto.re, start.re);
    }
}

/// Words where Box–Muller's rounding is delicate: the ends of the range
/// (u = 1 gives a radius of −0), every octant boundary and its neighbours
/// (a zero sine or cosine, whose sign the octant sets), and the halfway
/// word.
fn edge_words() -> Vec<u32> {
    let mut words = vec![0, 1, 2, u32::MAX, u32::MAX - 1, 1 << 31];
    for octant in 0..8_u32 {
        let start = octant << 29;
        words.extend([start, start.wrapping_sub(1), start + 1]);
    }
    words
}

/// Every pair of edge words, then pairs spread over the whole range.
fn word_pairs() -> Vec<(u32, u32)> {
    let edges = edge_words();
    let spread = (0..1_u32 << 14).map(|index| {
        let mixed = index.wrapping_mul(0x9E37_79B9);
        (mixed ^ (index >> 3), mixed.rotate_left(13) ^ index)
    });
    edges
        .iter()
        .flat_map(|&first| edges.iter().map(move |&second| (first, second)))
        .chain(spread)
        .collect()
}

#[test]
fn the_normals_are_within_the_bound_of_the_double_precision_oracle() {
    for (first, second) in word_pairs() {
        let (re, im) = box_muller(first, second);
        let (expected_re, expected_im) = oracle::box_muller(first, second);
        let errors = [
            relative_error(re, expected_re),
            relative_error(im, expected_im),
        ];
        assert!(
            errors.iter().all(|&error| error <= NORMAL_BOUND),
            "words {first:#x}, {second:#x}: ({re}, {im}) against ({expected_re}, {expected_im}), \
             {errors:?} ε"
        );
    }
}

/// The largest difference a draw `σ·n` scaled in `f32` may have from the
/// double-precision `exact`: the normal's bound and σ's rounding, and, for
/// a product rounded into the subnormals, their spacing.
fn draw_tolerance(exact: f64) -> f64 {
    (NORMAL_BOUND + 1.0) * EPSILON * exact.abs() + SUBNORMAL
}

#[test]
fn every_lane_draws_as_a_draw_made_alone() {
    let pairs = word_pairs();
    for sigma in [1.0, 0.1, 3.7e-3, 2.5e3, f32::MIN_POSITIVE] {
        // Each shift of the windows puts every pair in every lane.
        for shift in 0..LANES {
            for window in pairs[shift..].as_chunks::<LANES>().0 {
                let [re, im] = scaled_draws(
                    &window.map(|pair| pair.0),
                    &window.map(|pair| pair.1),
                    sigma,
                );
                for (lane, &(a, b)) in window.iter().enumerate() {
                    let (alone_re, alone_im) = scaled_draw(a, b, sigma);
                    assert_eq!(
                        (re[lane].to_bits(), im[lane].to_bits()),
                        (alone_re.to_bits(), alone_im.to_bits()),
                        "words {a:#x}, {b:#x} in lane {lane}, σ {sigma}"
                    );
                    let (normal_re, normal_im) = oracle::box_muller(a, b);
                    for (found, normal) in [(re[lane], normal_re), (im[lane], normal_im)] {
                        let exact = normal * f64::from(sigma);
                        let error = (f64::from(found) - exact).abs();
                        assert!(
                            error <= draw_tolerance(exact),
                            "words {a:#x}, {b:#x}, σ {sigma}: {found} against {exact}"
                        );
                    }
                }
            }
        }
    }
}

/// A cell of `samples` samples holding varied values, signed zeros among
/// them, so that a missing, doubled or misplaced addition shows.
fn varied_cell(samples: usize, salt: u64) -> Vec<Complex32> {
    const VALUES: [f32; 8] = [0.0, -0.0, 1.5, -2.25, 1e-8, -3.0e4, 0.1, -0.0];
    (0..samples)
        .map(|index| {
            let pick = (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ salt;
            Complex32::new(
                VALUES[(pick % 8) as usize],
                VALUES[((pick >> 8) % 8) as usize],
            )
        })
        .collect()
}

/// Keys from seeds and ordinals, and σ, zero among them, for the cell
/// comparisons.
fn streams() -> Vec<CellNoise> {
    let seeds = [0, 1, 11_111, -11_111, i32::MAX, i32::MIN];
    let ordinals = [0, 1, 7, ORDINALS - 1];
    let sigmas = [1.0, 0.1, 3.7e-3, 0.0, 2.5e3, f32::MIN_POSITIVE];
    seeds
        .iter()
        .enumerate()
        .flat_map(|(index, &seed)| {
            ordinals.iter().map(move |&ordinal| {
                let sigma = sigmas[(index + ordinal as usize) % sigmas.len()];
                CellNoise::new(key(seed, ordinal), sigma)
            })
        })
        .collect()
}

/// One cell to fill: `channels` channels of `correlations`, plus `extra`
/// samples past the last whole channel, which must be left alone.
struct Case {
    noise: CellNoise,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    channels: usize,
    extra: usize,
}

impl Case {
    /// The cell's starting samples.
    fn start(&self) -> Vec<Complex32> {
        let samples = self.channels * self.correlations + self.extra;
        varied_cell(samples, self.row ^ self.channels as u64)
    }

    fn describe(&self, index: usize) -> String {
        format!(
            "{:?}, row {}, autocorrelation {}, {} correlations, {} channels, sample {index}",
            self.noise, self.row, self.autocorrelation, self.correlations, self.channels
        )
    }
}

/// Rows either side of 2³² and at the ends, 0 to 101 channels across the
/// batch width, 1 to 4 correlations, cross- and autocorrelations, three
/// streams per cell rotating through them all; then many consecutive rows
/// across 2³² in the production shapes.
fn cases() -> Vec<Case> {
    let streams = streams();
    let rows = [
        0,
        1,
        17,
        999,
        (1 << 32) - 1,
        1 << 32,
        (1 << 32) + 5,
        (1 << 40) + 3,
        u64::MAX,
    ];
    let channel_counts = [0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 33, 64, 101];
    let mut cases = Vec::new();
    for &row in &rows {
        for &channels in &channel_counts {
            for correlations in 1..=4 {
                for autocorrelation in [false, true] {
                    for offset in 0..3 {
                        let index = cases.len() / 3;
                        cases.push(Case {
                            noise: streams[(index + 7 * offset) % streams.len()],
                            row,
                            autocorrelation,
                            correlations,
                            channels,
                            extra: (index + offset) % correlations,
                        });
                    }
                }
            }
        }
    }
    let noise = unit_noise(11_111, 2);
    for row in (1 << 32) - 600..(1 << 32) + 600 {
        for (correlations, autocorrelation, channels) in
            [(2, row % 37 == 0, 37), (4, false, 13), (1, false, 19)]
        {
            cases.push(Case {
                noise,
                row,
                autocorrelation,
                correlations,
                channels,
                extra: 0,
            });
        }
    }
    cases
}

/// `noise` added one draw at a time, in single precision: what the batched
/// path must give bit for bit.
fn add_one_draw_at_a_time(
    noise: &CellNoise,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    cell: &mut [Complex32],
) {
    if noise.sigma() == 0.0 || correlations == 0 {
        return;
    }
    for (channel, samples) in cell.chunks_exact_mut(correlations).enumerate() {
        for (pair, slots) in samples.chunks_mut(2).enumerate() {
            let counter = [row as u32, (row >> 32) as u32, channel as u32, pair as u32];
            let words = philox::philox4x32_10(counter, noise.key());
            for (slot, sample) in slots.iter_mut().enumerate() {
                let (re, im) = scaled_draw(words[2 * slot], words[2 * slot + 1], noise.sigma());
                sample.re += re;
                if !autocorrelation {
                    sample.im += im;
                }
            }
        }
    }
}

#[test]
fn the_batched_path_matches_draws_made_one_at_a_time_bit_for_bit() {
    for case in cases() {
        let start = case.start();
        let mut batched = start.clone();
        let mut alone = start;
        let (row, auto, correlations) = (case.row, case.autocorrelation, case.correlations);
        case.noise.add_to_row(row, auto, correlations, &mut batched);
        add_one_draw_at_a_time(&case.noise, row, auto, correlations, &mut alone);
        for (index, (found, wanted)) in batched.iter().zip(&alone).enumerate() {
            assert_eq!(
                (found.re.to_bits(), found.im.to_bits()),
                (wanted.re.to_bits(), wanted.im.to_bits()),
                "{}",
                case.describe(index)
            );
        }
    }
}

/// One part of one sample of a [`Case`], the batched path's `found` against
/// the oracle's `wanted`, given the double-precision normal drawn for it,
/// or `None` where no noise is added.
fn part_within_oracle(
    found: f32,
    wanted: f32,
    start: f32,
    normal: Option<f64>,
    sigma: f64,
) -> Result<(), String> {
    let bits = (found.to_bits(), wanted.to_bits());
    let Some(normal) = normal.filter(|_| sigma != 0.0) else {
        // Left alone by both, bit for bit.
        return if bits == (start.to_bits(), start.to_bits()) {
            Ok(())
        } else {
            Err(format!(
                "{found} and {wanted} from {start}, where no noise is added"
            ))
        };
    };
    if normal == 0.0 {
        // The same signed zero, so the same sum, bit for bit.
        return if bits.0 == bits.1 {
            Ok(())
        } else {
            Err(format!("{found} against {wanted}, a zero draw"))
        };
    }
    // The two sums round once each, by at most half a unit in the last place
    // of the larger, and the draws differ by at most the bound, the oracle's
    // rounding of σ·n to f32 and the subnormals' spacing, taken twice.
    let drawn = normal * sigma;
    let tolerance = ulp(found.abs().max(wanted.abs())) + draw_tolerance(drawn) * 2.0;
    let difference = (f64::from(found) - f64::from(wanted)).abs();
    if difference <= tolerance {
        Ok(())
    } else {
        Err(format!(
            "{found} against {wanted}, draw {drawn}, tolerance {tolerance}"
        ))
    }
}

#[test]
fn the_batched_path_is_within_the_bound_of_the_double_precision_oracle() {
    for case in cases() {
        let start = case.start();
        let mut batched = start.clone();
        let mut wanted = start.clone();
        let (row, auto, correlations) = (case.row, case.autocorrelation, case.correlations);
        case.noise.add_to_row(row, auto, correlations, &mut batched);
        oracle::add_to_row(&case.noise, row, auto, correlations, &mut wanted);
        let sigma = f64::from(case.noise.sigma());
        for (index, ((found, wanted), start)) in batched.iter().zip(&wanted).zip(&start).enumerate()
        {
            let (channel, correlation) = (index / correlations, index % correlations);
            let normals = (channel < case.channels)
                .then(|| oracle::standard_normals(&case.noise, row, channel, correlation / 2));
            let slot = 2 * (correlation % 2);
            let re = normals.map(|normals| normals[slot]);
            let im = normals.filter(|_| !auto).map(|normals| normals[slot + 1]);
            let check = part_within_oracle(found.re, wanted.re, start.re, re, sigma)
                .and_then(|()| part_within_oracle(found.im, wanted.im, start.im, im, sigma));
            if let Err(message) = check {
                panic!("{}: {message}", case.describe(index));
            }
        }
    }
}

#[test]
fn zero_sigma_leaves_the_cell_unchanged() {
    let noise = CellNoise::new(key(1, 0), 0.0);
    let start = Complex32::new(-0.0, 2.0);
    let mut samples = cell(2, start);
    noise.add_to_row(0, false, 2, &mut samples);
    assert!(
        samples
            .iter()
            .all(|s| s.re.to_bits() == start.re.to_bits() && s.im == start.im)
    );
}

/// The noise of three channels of four correlations of row 2³² + 5, under
/// the key `[11111, 0x0100_0003]` (casa-simulation's seed 11111, ordinal 3)
/// and σ = 0.1, bit for bit, (real, imaginary) by channel then correlation,
/// as computed on aarch64 when the draws moved to single precision (#855).
/// The generator, the counters, Box–Muller and the scaling are integer and
/// IEEE-754 arithmetic with nothing fused, so every platform must give these
/// bits; CI's x86-64 runners check that they do. casa-simulation holds the
/// same cell to the same bits through its own keys.
#[test]
fn a_known_cell_gets_its_known_bits_on_every_platform() {
    const KNOWN: [[u32; 2]; 12] = [
        [0x3e1d_fc8c, 0x3d90_052b],
        [0x3e16_de85, 0xbd21_c4b6],
        [0x3d6d_e0d4, 0x3da8_b85b],
        [0xbdbb_e1a5, 0x3e19_16ca],
        [0xbd4c_0290, 0xbb2e_ceba],
        [0x3dde_5ff2, 0x3d54_9052],
        [0xbd8e_d7be, 0xbe69_2d8d],
        [0xbde4_54d7, 0xbe62_ddf4],
        [0xbdc0_63e4, 0x3e26_71bb],
        [0x3d3a_0893, 0x3e1f_69f6],
        [0x3dfc_9901, 0x3da2_10b7],
        [0xbd8f_48c0, 0x3e07_8ae4],
    ];
    let noise = CellNoise::new([11_111, 0x0100_0003], 0.1);
    let mut cell = vec![Complex32::new(0.0, 0.0); 3 * 4];
    noise.add_to_row((1 << 32) + 5, false, 4, &mut cell);
    let bits: Vec<[u32; 2]> = cell
        .iter()
        .map(|sample| [sample.re.to_bits(), sample.im.to_bits()])
        .collect();
    assert_eq!(bits, KNOWN);
}

/// Not a check: the time per complex sample of the batched path and of the
/// oracle, alternately, on one thread, and of the batched path's Philox
/// words alone. The first shape is the sustained run's (#855:
/// cross-correlation rows of 4096 channels and two correlations). Run it
/// optimised:
/// `cargo test --release -p casa-numerics --lib -- --ignored --nocapture noise_timing`.
#[test]
#[ignore = "timing, not a check; run with --release"]
fn noise_timing() {
    const CHANNELS: usize = 4096;
    /// Complex samples per timed pass.
    const SAMPLES: usize = 1 << 26;
    let noise = CellNoise::new(key(11_111, 0), 0.1);
    for (correlations, autocorrelation) in [(2, false), (4, false), (1, false), (2, true)] {
        let mut samples = vec![Complex32::new(0.0, 0.0); CHANNELS * correlations];
        let rows = (SAMPLES / samples.len()) as u64;
        let rows = (1 << 32) - rows / 2..(1 << 32) + rows / 2;
        let per_pass = ((rows.end - rows.start) as usize * samples.len()) as f64;
        for run in 1..=3 {
            for path in ["scalar oracle", "batched", "philox only"] {
                let start = Instant::now();
                for row in rows.clone() {
                    let (noise, row) = (black_box(&noise), black_box(row));
                    let cell = black_box(&mut samples);
                    match path {
                        "batched" => noise.add_to_row(row, autocorrelation, correlations, cell),
                        "scalar oracle" => {
                            oracle::add_to_row(noise, row, autocorrelation, correlations, cell);
                        }
                        _ => {
                            black_box(philox_words(noise, row, CHANNELS, correlations));
                        }
                    }
                }
                let nanoseconds = start.elapsed().as_secs_f64() * 1e9 / per_pass;
                println!(
                    "{correlations} correlations, autocorrelation {autocorrelation}, \
                     run {run}, {path}: {nanoseconds:.3} ns per complex sample"
                );
            }
        }
    }
}

/// The Philox words of one row of [`noise_timing`]'s cells, folded by
/// exclusive or so that none can be skipped.
#[inline(never)]
fn philox_words(noise: &CellNoise, row: u64, channels: usize, correlations: usize) -> u32 {
    let mut folded = 0;
    for first in (0..channels).step_by(LANES) {
        for pair in 0..correlations.div_ceil(2) {
            let words = philox::philox4x32_10_lanes(counters(row, first, pair), noise.key());
            for lanes in words {
                folded ^= lanes.iter().fold(0, |folded, word| folded ^ word);
            }
        }
    }
    folded
}
