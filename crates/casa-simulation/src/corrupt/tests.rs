// SPDX-License-Identifier: LGPL-3.0-or-later
//! The quality of the noise draws, how a noise application fills a cell,
//! and that the batched path draws bit for bit what the one-draw-at-a-time
//! [`oracle`] draws.
//!
//! Every statistic is over 2¹⁸ draws and must lie within five standard
//! errors of its value for independent standard normals; the KS distance
//! must be below the 0.1 % critical value. The draws are deterministic, so a
//! pass is permanent; the margins are those of an honest test, not tuned to
//! these draws.

use std::hint::black_box;
use std::time::Instant;

use num_complex::Complex32;

use super::*;

/// Draws per statistic.
const N: usize = 1 << 18;

fn application(seed: i32, ordinal: u32) -> NoiseApplication {
    NoiseApplication {
        seed,
        sigma_jy: 1.0,
        ordinal,
    }
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
fn assert_uncorrelated(left: &[f64], right: &[f64], what: &str) {
    let r = correlation(left, right);
    let limit = 5.0 / (left.len() as f64).sqrt();
    assert!(r.abs() < limit, "{what}: r = {r:.5}, limit {limit:.5}");
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
fn first_draws(application: &NoiseApplication, start: u64, channel: usize) -> Vec<f64> {
    (start..start + N as u64)
        .map(|row| standard_normals(application, row, channel, 0)[0])
        .collect()
}

/// Every draw of rows 0..N/4, channel 0, pair 0.
fn all_draws() -> Vec<f64> {
    let noise = application(11_111, 0);
    (0..(N / 4) as u64)
        .flat_map(|row| standard_normals(&noise, row, 0, 0))
        .collect()
}

#[test]
fn draws_have_the_moments_of_a_standard_normal() {
    let [mean, variance, skewness, kurtosis] = moments(&all_draws());
    let n = N as f64;
    assert!(mean.abs() < 5.0 / n.sqrt(), "mean {mean}");
    assert!(
        (variance - 1.0).abs() < 5.0 * (2.0 / n).sqrt(),
        "variance {variance}"
    );
    assert!(
        skewness.abs() < 5.0 * (6.0 / n).sqrt(),
        "skewness {skewness}"
    );
    assert!(
        kurtosis.abs() < 5.0 * (24.0 / n).sqrt(),
        "excess kurtosis {kurtosis}"
    );
}

#[test]
fn draws_pass_a_kolmogorov_smirnov_test_against_the_standard_normal() {
    let mut draws = all_draws();
    draws.sort_by(f64::total_cmp);
    let n = draws.len() as f64;
    let distance = draws
        .iter()
        .enumerate()
        .map(|(index, &x)| {
            let cdf = standard_normal_cdf(x);
            (cdf - index as f64 / n).max((index + 1) as f64 / n - cdf)
        })
        .fold(0.0_f64, f64::max);
    // The asymptotic 0.1 % critical value of the one-sample KS statistic.
    let critical = 1.949 / n.sqrt();
    assert!(
        distance < critical,
        "D = {distance:.6}, critical {critical:.6}"
    );
    // Box–Muller's largest draw from 32-bit words.
    let largest = draws[draws.len() - 1].max(-draws[0]);
    assert!(largest < (64.0 * std::f64::consts::LN_2).sqrt() + 1e-12);
}

#[test]
fn draws_are_independent_across_parts_slots_and_pairs() {
    let noise = application(11_111, 0);
    let rows = 0..N as u64;
    let pick = |pair: usize, slot: usize| -> Vec<f64> {
        rows.clone()
            .map(|row| standard_normals(&noise, row, 3, pair)[slot])
            .collect()
    };
    let (re0, im0, re1, im1) = (pick(0, 0), pick(0, 1), pick(0, 2), pick(0, 3));
    let re2 = pick(1, 0);
    assert_uncorrelated(&re0, &im0, "real and imaginary parts");
    assert_uncorrelated(&re1, &im1, "real and imaginary parts of slot 1");
    assert_uncorrelated(&re0, &re1, "correlations 0 and 1");
    assert_uncorrelated(&im0, &re1, "correlation 0 imaginary, 1 real");
    assert_uncorrelated(&re0, &re2, "correlations 0 and 2 (another pair)");
}

#[test]
fn draws_are_independent_across_rows_channels_ordinals_and_seeds() {
    let noise = application(11_111, 0);
    let base = first_draws(&noise, 0, 0);
    assert_uncorrelated(&base, &first_draws(&noise, 1, 0), "adjacent rows");
    assert_uncorrelated(&base, &first_draws(&noise, 0, 1), "adjacent channels");
    assert_uncorrelated(
        &base,
        &first_draws(&application(11_111, 1), 0, 0),
        "ordinals 0 and 1",
    );
    assert_uncorrelated(
        &base,
        &first_draws(&application(11_112, 0), 0, 0),
        "seeds 11111 and 11112",
    );
    assert_uncorrelated(
        &base,
        &first_draws(&application(-11_111, 0), 0, 0),
        "seeds of opposite sign",
    );
    // Rows beyond 2³² use the counter's second word.
    let high = first_draws(&noise, 1 << 32, 0);
    assert_uncorrelated(&base, &high, "rows r and r + 2³²");
    let [_, variance, ..] = moments(&high);
    assert!((variance - 1.0).abs() < 5.0 * (2.0 / N as f64).sqrt());
}

#[test]
fn the_same_seed_and_ordinal_draw_the_same_values() {
    let first = application(42, 3);
    let second = application(42, 3);
    for row in [0, 7, 1 << 40] {
        assert_eq!(
            standard_normals(&first, row, 5, 1),
            standard_normals(&second, row, 5, 1)
        );
    }
}

/// A two-channel cell of `correlations`, every sample `value`.
fn cell(correlations: usize, value: Complex32) -> Vec<Complex32> {
    vec![value; 2 * correlations]
}

#[test]
fn a_cross_correlation_gets_sigma_times_each_draw_in_single_precision() {
    let noise = NoiseApplication {
        seed: 9,
        sigma_jy: 0.1,
        ordinal: 2,
    };
    let mut samples = cell(4, Complex32::new(0.0, 0.0));
    noise.add_to_row(17, false, 4, &mut samples);
    let sigma = f64::from(0.1_f32);
    for channel in 0..2 {
        for correlation in 0..4 {
            let draws = standard_normals(&noise, 17, channel, correlation / 2);
            let slot = correlation % 2;
            let expected = Complex32::new(
                (draws[2 * slot] * sigma) as f32,
                (draws[2 * slot + 1] * sigma) as f32,
            );
            assert_eq!(samples[channel * 4 + correlation], expected);
        }
    }
}

#[test]
fn every_correlation_gets_its_own_draw_whatever_the_correlation_count() {
    let noise = application(5, 0);
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
    let noise = application(5, 0);
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
fn the_normals_match_the_scalar_oracle_bit_for_bit() {
    for (first, second) in word_pairs() {
        let (re, im) = box_muller(first, second);
        let (expected_re, expected_im) = oracle::box_muller(first, second);
        assert_eq!(
            (re.to_bits(), im.to_bits()),
            (expected_re.to_bits(), expected_im.to_bits()),
            "words {first:#x}, {second:#x}: ({re}, {im}) against ({expected_re}, {expected_im})"
        );
    }
}

#[test]
fn every_lane_draws_as_the_scalar_oracle_does() {
    let pairs = word_pairs();
    for sigma in [1.0, f64::from(0.1_f32), f64::from(3.7e-3_f32)] {
        // Each shift of the windows puts every pair in every lane.
        for shift in 0..LANES {
            for window in pairs[shift..].as_chunks::<LANES>().0 {
                let [re, im] = scaled_draws(
                    &window.map(|pair| pair.0),
                    &window.map(|pair| pair.1),
                    sigma,
                );
                for (lane, &(a, b)) in window.iter().enumerate() {
                    let (expected_re, expected_im) = oracle::box_muller(a, b);
                    assert_eq!(re[lane].to_bits(), ((expected_re * sigma) as f32).to_bits());
                    assert_eq!(im[lane].to_bits(), ((expected_im * sigma) as f32).to_bits());
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

/// Seeds, ordinals and σ, zero among them, for the cell comparison.
fn oracle_applications() -> Vec<NoiseApplication> {
    let seeds = [0, 1, 11_111, -11_111, i32::MAX, i32::MIN];
    let ordinals = [0, 1, 7, MAXIMUM_APPLICATIONS - 1];
    let sigmas = [1.0, 0.1, 3.7e-3, 0.0, 2.5e3, f32::MIN_POSITIVE];
    seeds
        .iter()
        .enumerate()
        .flat_map(|(index, &seed)| {
            ordinals.iter().map(move |&ordinal| NoiseApplication {
                seed,
                sigma_jy: sigmas[(index + ordinal as usize) % sigmas.len()],
                ordinal,
            })
        })
        .collect()
}

/// The batched path and the oracle on one cell of `channels` channels of
/// `correlations`, plus `extra` samples past the last whole channel.
fn assert_matches_oracle(
    noise: &NoiseApplication,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    channels: usize,
    extra: usize,
) {
    let start = varied_cell(channels * correlations + extra, row ^ channels as u64);
    let mut batched = start.clone();
    let mut expected = start;
    noise.add_to_row(row, autocorrelation, correlations, &mut batched);
    oracle::add_to_row(noise, row, autocorrelation, correlations, &mut expected);
    for (index, (found, wanted)) in batched.iter().zip(&expected).enumerate() {
        assert_eq!(
            (found.re.to_bits(), found.im.to_bits()),
            (wanted.re.to_bits(), wanted.im.to_bits()),
            "{noise:?}, row {row}, autocorrelation {autocorrelation}, \
             {correlations} correlations, {channels} channels, sample {index}"
        );
    }
}

#[test]
fn the_batched_path_matches_the_scalar_oracle_bit_for_bit() {
    let applications = oracle_applications();
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
    let mut case = 0;
    for &row in &rows {
        for &channels in &channel_counts {
            for correlations in 1..=4 {
                for autocorrelation in [false, true] {
                    // Three applications per cell, rotating through them all.
                    for offset in 0..3 {
                        let noise = &applications[(case + 7 * offset) % applications.len()];
                        let extra = (case + offset) % correlations;
                        assert_matches_oracle(
                            noise,
                            row,
                            autocorrelation,
                            correlations,
                            channels,
                            extra,
                        );
                    }
                    case += 1;
                }
            }
        }
    }
    // Many consecutive rows across the 2³² boundary, the production shapes.
    let noise = application(11_111, 2);
    for row in (1 << 32) - 600..(1 << 32) + 600 {
        assert_matches_oracle(&noise, row, row % 37 == 0, 2, 37, 0);
        assert_matches_oracle(&noise, row, false, 4, 13, 0);
        assert_matches_oracle(&noise, row, false, 1, 19, 0);
    }
}

#[test]
fn zero_sigma_leaves_the_cell_unchanged() {
    let noise = NoiseApplication {
        seed: 1,
        sigma_jy: 0.0,
        ordinal: 0,
    };
    let start = Complex32::new(-0.0, 2.0);
    let mut samples = cell(2, start);
    noise.add_to_row(0, false, 2, &mut samples);
    assert!(
        samples
            .iter()
            .all(|s| s.re.to_bits() == start.re.to_bits() && s.im == start.im)
    );
}

/// Not a check: the time per complex sample of the batched path and of the
/// oracle, alternately, on one thread. The first shape is the sustained
/// run's (#855: cross-correlation rows of 4096 channels and two
/// correlations). Run it optimised:
/// `cargo test --release -p casa-simulation --lib -- --ignored --nocapture noise_timing`.
#[test]
#[ignore = "timing, not a check; run with --release"]
fn noise_timing() {
    const CHANNELS: usize = 4096;
    /// Complex samples per timed pass.
    const SAMPLES: usize = 1 << 26;
    let noise = NoiseApplication {
        seed: 11_111,
        sigma_jy: 0.1,
        ordinal: 0,
    };
    for (correlations, autocorrelation) in [(2, false), (4, false), (1, false), (2, true)] {
        let mut samples = vec![Complex32::new(0.0, 0.0); CHANNELS * correlations];
        let rows = (SAMPLES / samples.len()) as u64;
        let rows = (1 << 32) - rows / 2..(1 << 32) + rows / 2;
        let per_pass = ((rows.end - rows.start) as usize * samples.len()) as f64;
        for run in 1..=3 {
            for batched in [false, true] {
                let start = Instant::now();
                for row in rows.clone() {
                    let (noise, row) = (black_box(&noise), black_box(row));
                    let cell = black_box(&mut samples);
                    if batched {
                        noise.add_to_row(row, autocorrelation, correlations, cell);
                    } else {
                        oracle::add_to_row(noise, row, autocorrelation, correlations, cell);
                    }
                }
                let nanoseconds = start.elapsed().as_secs_f64() * 1e9 / per_pass;
                let path = if batched { "batched" } else { "scalar oracle" };
                println!(
                    "{correlations} correlations, autocorrelation {autocorrelation}, \
                     run {run}, {path}: {nanoseconds:.3} ns per complex sample"
                );
            }
        }
    }
}
