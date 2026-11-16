// SPDX-License-Identifier: LGPL-3.0-or-later

//! The robust noise of a residual plane, as CASA's `tclean` estimates it.
//!
//! tclean estimates the noise its `nsigma` threshold and its automatic mask
//! read in one of two ways, chosen by `fastnoise`:
//!
//! * **fast** (`fastnoise=True`, `SDMaskHandler::calcImageStatistics`):
//!   casacore's classical median and median absolute deviation of every
//!   valid pixel;
//! * **robust** (`fastnoise=False`,
//!   `SDMaskHandler::calcRobustImageStatistics`): a plane with no clean
//!   mask, or whose mask covers the whole first plane, takes casacore's
//!   Chauvenet-criterion statistics of its valid pixels
//!   (`ImageStatsCalculator::configureChauvenet(-1, 5)`); any other plane
//!   takes the classical statistics of the valid pixels outside its mask.
//!
//! The noise is 1.4826 times the median absolute deviation, CASA's literal
//! factor (`SIImageStore::calcRobustRMS`).

use crate::RobustNoise;
pub use casa_imaging_model::NoiseEstimate;

/// CASA's factor from the median absolute deviation to a Gaussian RMS
/// (`mads*1.4826` in `SDMaskHandler` and `SIImageStore::calcRobustRMS`).
pub const CASA_MAD_TO_RMS: f64 = 1.4826;

/// Chauvenet iterations tclean allows (`configureChauvenet(-1.0, 5)`).
const CHAUVENET_ITERATIONS: usize = 5;

/// casacore `ZScoreCalculator`'s table of the largest sample size each
/// z-score is the Chauvenet criterion of.
const MAX_ZSCORES: [(u64, f64); 14] = [
    (0, 0.5),
    (1, 1.0),
    (3, 1.5),
    (10, 2.0),
    (40, 2.5),
    (185, 3.0),
    (1_074, 3.5),
    (7_893, 4.0),
    (73_579, 4.5),
    (872_138, 5.0),
    (13_165_126, 5.5),
    (253_398_672, 6.0),
    (6_225_098_696, 6.5),
    (195_341_107_722, 7.0),
];

/// The classical median and scaled median absolute deviation of
/// `samples`, which it reorders; `None` when there are none.
#[must_use]
pub fn classical_noise(samples: &mut [f64]) -> Option<RobustNoise> {
    let median = median_in_place(samples)?;
    for sample in samples.iter_mut() {
        *sample = (*sample - median).abs();
    }
    let deviation = median_in_place(samples)?;
    Some(RobustNoise {
        median,
        rms: CASA_MAD_TO_RMS * deviation,
    })
}

/// casacore's `ChauvenetCriterionStatistics` with tclean's configuration
/// (z-score from the sample size, five iterations): the median and scaled
/// median absolute deviation of the samples left inside
/// `mean ± z·stddev` once clipping converges or the iterations run out.
/// `None` when there are no samples.
#[must_use]
pub fn chauvenet_noise(mut samples: Vec<f64>) -> Option<RobustNoise> {
    // `ChauvenetCriterionStatistics::_setRange`: the classical statistics
    // of every sample set the first range; each later pass recomputes them
    // inside the range until the count stops changing.
    let mut range: Option<(f64, f64)> = None;
    let mut previous_count = None;
    for _ in 0..=CHAUVENET_ITERATIONS {
        let (count, mean, stddev) = moments(&samples, range);
        if previous_count == Some(count) {
            break;
        }
        let z = max_zscore(count as u64);
        range = Some((mean - z * stddev, mean + z * stddev));
        previous_count = Some(count);
    }
    if let Some((low, high)) = range {
        samples.retain(|value| *value >= low && *value <= high);
    }
    classical_noise(&mut samples)
}

/// tclean's noise of one plane: the fast estimate of the `valid` values,
/// or the robust one with `mask` the plane's clean mask (`None` or empty
/// for none) and `mask_full` whether the mask covers the image's whole
/// first plane (`SDMaskHandler::calcRobustImageStatistics`). `None` when
/// no value counts.
#[must_use]
pub fn plane_noise(
    values: &[f64],
    valid: &[bool],
    estimate: NoiseEstimate,
    mask: Option<&[bool]>,
    mask_full: bool,
) -> Option<RobustNoise> {
    let selected = |outside: Option<&[bool]>| {
        values
            .iter()
            .enumerate()
            .filter(|(index, _)| valid[*index] && outside.is_none_or(|mask| !mask[*index]))
            .map(|(_, value)| *value)
            .collect::<Vec<_>>()
    };
    match (estimate, mask) {
        (NoiseEstimate::Fast, _) => classical_noise(&mut selected(None)),
        (NoiseEstimate::Robust, Some(mask)) if !mask_full && mask.iter().any(|set| *set) => {
            classical_noise(&mut selected(Some(mask)))
        }
        (NoiseEstimate::Robust, _) => chauvenet_noise(selected(None)),
    }
}

/// casacore `ZScoreCalculator::getMaxZScore`: the z-score beyond which
/// fewer than half a sample of `count` normal samples is expected, found
/// by bisection between the table's neighbours of `count`, as a fresh
/// process finds it (casacore caches each answer in its table, so a long
/// process may land elsewhere in the same one-count interval).
fn max_zscore(count: u64) -> f64 {
    if let Some((_, z)) = MAX_ZSCORES.iter().find(|(n, _)| *n == count) {
        return *z;
    }
    let (mut low, mut high) = match MAX_ZSCORES.iter().position(|(n, _)| *n > count) {
        Some(upper) => (MAX_ZSCORES[upper - 1].1, MAX_ZSCORES[upper].1),
        None => {
            let last = MAX_ZSCORES[MAX_ZSCORES.len() - 1].1;
            let mut z = last + 0.5;
            loop {
                let reached = zscore_to_count(z);
                if reached == count {
                    return z;
                }
                if reached > count {
                    break (last, z);
                }
                z += 0.5;
            }
        }
    };
    let mut z = 0.5 * (low + high);
    loop {
        let reached = zscore_to_count(z);
        if reached == count || near(low, high) {
            return z;
        }
        if reached > count {
            high = z;
        } else {
            low = z;
        }
        z = 0.5 * (low + high);
    }
}

/// casacore `ZScoreCalculator::zscoreToNpts`.
fn zscore_to_count(z: f64) -> u64 {
    (0.5 / libm::erfc(z / std::f64::consts::SQRT_2)) as u64
}

/// casacore `near(a, b)` at its default relative tolerance of 1e-13, for
/// positive arguments.
fn near(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= 1.0e-13 * a.abs().max(b.abs())
}

/// The count, mean and sample standard deviation (casacore's `stddev`,
/// over `n − 1`) of the samples inside the inclusive `range`.
fn moments(samples: &[f64], range: Option<(f64, f64)>) -> (usize, f64, f64) {
    let (mut count, mut mean, mut scatter) = (0_usize, 0.0, 0.0);
    for value in samples {
        if range.is_some_and(|(low, high)| *value < low || *value > high) {
            continue;
        }
        count += 1;
        let previous = mean;
        mean += (value - previous) / count as f64;
        scatter += (value - previous) * (value - mean);
    }
    let variance = if count > 1 {
        scatter / (count - 1) as f64
    } else {
        0.0
    };
    (count, mean, variance.sqrt())
}

/// casacore's median: the middle value, or the mean of the two middle
/// values of an even count. Reorders `values`.
fn median_in_place(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let even = values.len().is_multiple_of(2);
    let middle = values.len() / 2;
    let (below, upper, _) = values.select_nth_unstable_by(middle, f64::total_cmp);
    let upper = *upper;
    if !even {
        return Some(upper);
    }
    let lower = below.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Some(0.5 * (lower + upper))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classical_noise_averages_the_two_middle_values_of_an_even_count() {
        let mut values = [4.0, 1.0, 3.0, 2.0];
        let noise = classical_noise(&mut values).expect("samples");
        assert_eq!(noise.median, 2.5);
        // Deviations 1.5, 1.5, 0.5, 0.5: their median is 1.
        assert_eq!(noise.rms, CASA_MAD_TO_RMS);
        assert_eq!(classical_noise(&mut []), None);
    }

    #[test]
    fn max_zscore_follows_casacores_table_and_bisection() {
        assert_eq!(max_zscore(185), 3.0);
        assert_eq!(max_zscore(0), 0.5);
        // Between table entries the z-score is the criterion's crossing.
        for count in [2_u64, 50, 10_000, 1_000_000, 300_000_000_000] {
            let z = max_zscore(count);
            assert!(zscore_to_count(z) == count || zscore_to_count(z * (1.0 + 1e-9)) >= count);
            assert!(zscore_to_count(z * (1.0 - 1e-6)) <= count, "{count}");
        }
        assert!(max_zscore(10_000) > 4.0 && max_zscore(10_000) < 4.5);
    }

    #[test]
    fn chauvenet_clips_outliers_before_the_median() {
        // A symmetric core and one far outlier: the classical median moves,
        // Chauvenet's does not.
        let mut values = (0..101)
            .map(|i| f64::from(i - 50) * 0.01)
            .collect::<Vec<_>>();
        values.push(1000.0);
        let clipped = chauvenet_noise(values.clone()).expect("samples");
        assert_eq!(clipped.median, 0.0);
        let classical = classical_noise(&mut values).expect("samples");
        assert!(classical.median > 0.0);
    }

    #[test]
    fn robust_noise_excludes_the_mask_unless_it_is_empty_or_full() {
        let values = [1.0, 2.0, 3.0, 100.0, 200.0];
        let valid = [true; 5];
        let mask = [false, false, false, true, true];
        let outside = plane_noise(&values, &valid, NoiseEstimate::Robust, Some(&mask), false)
            .expect("samples");
        assert_eq!(outside.median, 2.0);
        let fast =
            plane_noise(&values, &valid, NoiseEstimate::Fast, Some(&mask), false).expect("samples");
        assert_eq!(fast.median, 3.0);
        // A full mask, or none, takes Chauvenet over every valid value.
        let full = plane_noise(&values, &valid, NoiseEstimate::Robust, Some(&mask), true);
        let none = plane_noise(&values, &valid, NoiseEstimate::Robust, None, false);
        assert_eq!(full, none);
        assert_eq!(none, chauvenet_noise(values.to_vec()));
    }
}
