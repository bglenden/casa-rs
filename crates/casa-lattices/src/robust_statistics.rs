// SPDX-License-Identifier: LGPL-3.0-or-later
//! Robust sample statistics: CASA's statistics-framework algorithms.
//!
//! C++ casacore implements `imstat`'s `hinges-fences`, `chauvenet`,
//! `fit-half` and `biweight` algorithms in
//! `casacore/scimath/StatsFramework`. This module follows those sources on
//! a materialized sample of `(value, linear position)` pairs: every
//! algorithm selects its own effective sample from the caller's, and the
//! statistics reported are the classical ones over that sample.
//!
//! The quantile rule is the framework's: the quantile at fraction `q` of
//! `n` sorted values is the value at index `ceil(q * n) - 1`, without
//! interpolation; the median and the median absolute deviation average
//! the two middle values of even samples. The biweight's initial scale
//! is casacore's `C::probit_3_4` constant, `1.482602218505602` — despite
//! its name, the reciprocal of the probit at 3/4, the factor that makes
//! the median absolute deviation estimate a Gaussian sigma.

use crate::statistics::{
    OrderStatsSummary, RunningStats, Statistic, framework_order_summary, quartiles_and_median,
    selection_median,
};

/// The MAD-to-sigma factor casacore names `C::probit_3_4`.
const PROBIT_3_4: f64 = 1.482602218505602;

/// The biweight tuning constant CASA's statistics surface uses.
const BIWEIGHT_C: f64 = 6.0;

/// Which of CASA's statistics algorithms a computation ran.
///
/// It fixes the keys the CASA record carries: the biweight reports only
/// its location and scale, and `fit-half` reports only the real
/// extremum's position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatisticsAlgorithmKind {
    /// The classical moments and order statistics.
    Classic,
    /// Classical statistics over the hinges-and-fences range.
    HingesFences,
    /// Classical statistics over Chauvenet's converged range.
    Chauvenet,
    /// Classical statistics over the reflected half sample.
    FitToHalf,
    /// The biweight location and scale.
    Biweight,
}

/// One of CASA's statistics algorithms, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatisticsAlgorithm {
    /// The classical moments and order statistics.
    Classic,
    /// Data outside `[q1 - fence * d, q3 + fence * d]`, for the first
    /// and third quartiles of the full sample and their difference `d`,
    /// is excluded. A negative fence keeps every value, exactly as
    /// `classic`.
    HingesFences {
        /// The fence multiplier; negative keeps the whole sample.
        fence: f64,
    },
    /// Data is excluded iteratively beyond `zscore` standard deviations
    /// of the current mean, or, when `zscore` is negative, beyond
    /// Chauvenet criterion for the current sample size. `maxiter`
    /// bounds the iterations; negative means until the excluded set
    /// stops changing.
    Chauvenet {
        /// The fixed z-score, or a negative value for Chauvenet's
        /// criterion.
        zscore: f64,
        /// The iteration bound; negative means until convergence.
        maxiter: i64,
    },
    /// Only values at or below (or at or above) a center are kept, and
    /// each is mirrored about the center so the doubled sample is
    /// symmetric. The center is the full sample's mean or median, or
    /// zero.
    FitToHalf {
        /// Which full-sample statistic the center is.
        center: FitHalfCenter,
        /// Whether the kept half is at or above the center, rather
        /// than at or below it.
        upper: bool,
    },
    /// The biweight location and scale, iterating at most `niter`
    /// times; a negative count takes the single fast pass.
    Biweight {
        /// The iteration bound; negative takes the fast pass.
        niter: i64,
    },
}

impl StatisticsAlgorithm {
    /// The kind of record this algorithm produces.
    pub fn kind(&self) -> StatisticsAlgorithmKind {
        match self {
            Self::Classic => StatisticsAlgorithmKind::Classic,
            Self::HingesFences { .. } => StatisticsAlgorithmKind::HingesFences,
            Self::Chauvenet { .. } => StatisticsAlgorithmKind::Chauvenet,
            Self::FitToHalf { .. } => StatisticsAlgorithmKind::FitToHalf,
            Self::Biweight { .. } => StatisticsAlgorithmKind::Biweight,
        }
    }
}

/// The center a `fit-half` computation reflects about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FitHalfCenter {
    /// The full sample's mean.
    Mean,
    /// The full sample's median.
    Median,
    /// Zero.
    Zero,
}

/// A sample value with its linear position in the caller's indexing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SamplePoint {
    /// The selected value.
    pub value: f64,
    /// The value's linear position in the caller's index space.
    pub position: u64,
}

/// The statistics an algorithm's effective sample reports.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RobustSampleStats {
    /// Number of points in the effective sample.
    pub npts: f64,
    /// Effective-sample minimum.
    pub min: f64,
    /// Effective-sample maximum.
    pub max: f64,
    /// Effective-sample sum.
    pub sum: f64,
    /// Effective-sample sum of squares.
    pub sumsq: f64,
    /// Effective-sample mean.
    pub mean: f64,
    /// Effective-sample root-mean-square.
    pub rms: f64,
    /// Effective-sample Bessel-corrected standard deviation.
    pub sigma: f64,
    /// Effective-sample median.
    pub median: f64,
    /// Effective-sample median absolute deviation.
    pub medabsdevmed: f64,
    /// Effective-sample first quartile.
    pub q1: f64,
    /// Effective-sample third quartile.
    pub q3: f64,
    /// `q3 - q1`.
    pub quartile: f64,
    /// The effective-sample minimum's position, when it is a real
    /// point of the caller's sample.
    pub minpos: Option<u64>,
    /// The effective-sample maximum's position, when it is a real point
    /// of the caller's sample.
    pub maxpos: Option<u64>,
    /// CASA's reported Chauvenet iteration count, capped by `maxiter`.
    pub chauvenet_iterations: Option<usize>,
}

/// The outcome of one robust computation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RobustStats {
    /// The full classical statistics of an algorithm's effective
    /// sample, as `hinges-fences`, `chauvenet` and `fit-half` report.
    Sample(RobustSampleStats),
    /// The biweight's location and scale.
    Biweight { location: f64, scale: f64 },
}

/// Why a robust computation cannot proceed, with CASA's wording.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RobustStatsError {
    /// The biweight needs at least two points.
    #[error(
        "npts is {npts}. There must be at least two points to compute the biweight location and scale"
    )]
    BiweightNeedsTwoPoints {
        /// The number of points the sample held.
        npts: u64,
    },
    /// `fit-half`'s center excluded every point.
    #[error("No data included using current configuration, cannot compute min and max")]
    FitHalfEmpty,
}

/// Parse CASA's `algorithm` argument: minimum match, in any case, of
/// `biweight`, `chauvenet`, `classic`, `fit-half` and `hinges-fences`.
///
/// An empty, ambiguous or unknown token fails with CASA's wording, as
/// `c` does against `classic` and `chauvenet`.
pub fn parse_algorithm(token: &str) -> Result<StatisticsAlgorithmKind, String> {
    const NAMES: [(&str, StatisticsAlgorithmKind); 5] = [
        ("biweight", StatisticsAlgorithmKind::Biweight),
        ("chauvenet", StatisticsAlgorithmKind::Chauvenet),
        ("classic", StatisticsAlgorithmKind::Classic),
        ("fit-half", StatisticsAlgorithmKind::FitToHalf),
        ("hinges-fences", StatisticsAlgorithmKind::HingesFences),
    ];
    let lowered = token.to_ascii_lowercase();
    let matches: Vec<_> = NAMES
        .iter()
        .filter(|(name, _)| name.starts_with(&lowered))
        .collect();
    match matches.as_slice() {
        [(_, kind)] => Ok(*kind),
        _ => Err(format!("Unsupported algorithm {lowered}")),
    }
}

/// Parse CASA's `center` argument for `fit-half`: minimum match, in any
/// case, of `mean`, `median` and `zero`.
///
/// A minimum match must be at least three characters long for the mean
/// and median, as CASA's parser requires; `zero` matches from its first
/// letter.
pub fn parse_center(token: &str) -> Result<FitHalfCenter, String> {
    const NAMES: [(&str, FitHalfCenter, usize); 3] = [
        ("mean", FitHalfCenter::Mean, 3),
        ("median", FitHalfCenter::Median, 3),
        ("zero", FitHalfCenter::Zero, 1),
    ];
    let lowered = token.to_ascii_lowercase();
    let matches: Vec<_> = NAMES
        .iter()
        .filter(|(name, _, minimum)| lowered.len() >= *minimum && name.starts_with(&lowered))
        .collect();
    match matches.as_slice() {
        [(_, center, _)] => Ok(*center),
        _ => Err(format!("Unsupported center value {lowered}")),
    }
}

/// Parse CASA's `clmethod` argument: minimum match, in any case, of
/// `auto`, `tiled` and `framework`.
///
/// The choice is an execution strategy and the statistics it produces
/// are the same, so the parsed value is accepted and not otherwise used.
pub fn parse_clmethod(token: &str) -> Result<(), String> {
    const NAMES: [&str; 3] = ["auto", "tiled", "framework"];
    let lowered = token.to_ascii_lowercase();
    if NAMES.iter().any(|name| name.starts_with(&lowered)) {
        Ok(())
    } else {
        Err(format!("Unsupported classical method {lowered}"))
    }
}

/// Compute one robust algorithm over the sample.
///
/// # Errors
///
/// The biweight with fewer than two points, or a `fit-half` whose center
/// excludes every point, with CASA's wording.
pub fn compute_robust_stats(
    sample: &[SamplePoint],
    algorithm: &StatisticsAlgorithm,
) -> Result<RobustStats, RobustStatsError> {
    match algorithm {
        StatisticsAlgorithm::Classic => Ok(RobustStats::Sample(classical_stats(sample))),
        StatisticsAlgorithm::HingesFences { fence } => {
            if *fence < 0.0 {
                return Ok(RobustStats::Sample(classical_stats(sample)));
            }
            let mut values: Vec<f64> = sample.iter().map(|point| point.value).collect();
            let [q1, _, q3] = quartiles_and_median(&mut values);
            let span = q3 - q1;
            let (low, high) = (q1 - fence * span, q3 + fence * span);
            let kept: Vec<SamplePoint> = sample
                .iter()
                .copied()
                .filter(|point| low <= point.value && point.value <= high)
                .collect();
            Ok(RobustStats::Sample(classical_stats(&kept)))
        }
        StatisticsAlgorithm::Chauvenet { zscore, maxiter } => {
            let max_iterations = if *maxiter < 0 { 1000 } else { *maxiter };
            let mut kept: Vec<SamplePoint> = sample.to_vec();
            let mut previous_npts: Option<f64> = None;
            let mut reductions = 0;
            for iteration in 0..=max_iterations {
                let stats = classical_stats(&kept);
                if iteration > 0 && previous_npts == Some(stats.npts) {
                    break;
                }
                let z = if *zscore >= 0.0 {
                    *zscore
                } else {
                    max_zscore(stats.npts as u64)
                };
                let center = if stats.npts == 0. { 0. } else { stats.mean };
                let low = center - z * stats.sigma;
                let high = center + z * stats.sigma;
                // Casacore replaces the range on the original data at each
                // iteration. An empty previous range has mean and sigma zero.
                kept.clear();
                kept.extend(
                    sample
                        .iter()
                        .copied()
                        .filter(|point| low <= point.value && point.value <= high),
                );
                if kept.len() < stats.npts as usize {
                    reductions += 1;
                }
                previous_npts = Some(stats.npts);
            }
            let mut stats = classical_stats(&kept);
            stats.chauvenet_iterations = Some(reductions.min(max_iterations as usize));
            Ok(RobustStats::Sample(stats))
        }
        StatisticsAlgorithm::FitToHalf { center, upper } => {
            let center_value = match center {
                FitHalfCenter::Mean => sample_mean(sample),
                FitHalfCenter::Median => sample_median(sample),
                FitHalfCenter::Zero => 0.0,
            };
            let reals: Vec<&SamplePoint> = sample
                .iter()
                .filter(|point| {
                    if *upper {
                        point.value >= center_value
                    } else {
                        point.value <= center_value
                    }
                })
                .collect();
            if reals.is_empty() {
                return Err(RobustStatsError::FitHalfEmpty);
            }
            // The effective sample is the kept values joined with their
            // reflections about the center; only the real extremum has a
            // position, and the median is the center itself.
            let mut values: Vec<f64> = Vec::with_capacity(2 * reals.len());
            for point in &reals {
                values.push(point.value);
                values.push(2.0 * center_value - point.value);
            }
            let mut stats = classical_stats_of_values(&values);
            stats.median = center_value;
            if *upper {
                let real_max = reals
                    .iter()
                    .copied()
                    .max_by(|left, right| {
                        left.value
                            .total_cmp(&right.value)
                            .then(right.position.cmp(&left.position))
                    })
                    .expect("the kept sample is non-empty");
                stats.max = real_max.value;
                stats.maxpos = Some(real_max.position);
                stats.min = 2.0 * center_value - real_max.value;
            } else {
                let real_min = reals
                    .iter()
                    .copied()
                    .min_by(|left, right| {
                        left.value
                            .total_cmp(&right.value)
                            .then(left.position.cmp(&right.position))
                    })
                    .expect("the kept sample is non-empty");
                stats.min = real_min.value;
                stats.minpos = Some(real_min.position);
                stats.max = 2.0 * center_value - real_min.value;
            }
            Ok(RobustStats::Sample(stats))
        }
        StatisticsAlgorithm::Biweight { niter } => {
            let values: Vec<f64> = sample.iter().map(|point| point.value).collect();
            let (location, scale) = biweight_location_scale(&values, BIWEIGHT_C, *niter)?;
            Ok(RobustStats::Biweight { location, scale })
        }
    }
}

/// The classical statistics of a sample: the framework's moments and
/// order statistics, with the extremum positions of the first extremum
/// in sample order.
fn classical_stats(sample: &[SamplePoint]) -> RobustSampleStats {
    let mut stats =
        classical_stats_of_values(&sample.iter().map(|point| point.value).collect::<Vec<_>>());
    let mut minpos = None;
    let mut maxpos = None;
    for point in sample {
        if minpos.is_none() || point.value < stats.min {
            stats.min = point.value;
            minpos = Some(point.position);
        }
        if maxpos.is_none() || point.value > stats.max {
            stats.max = point.value;
            maxpos = Some(point.position);
        }
    }
    stats.minpos = minpos;
    stats.maxpos = maxpos;
    stats
}

/// The classical statistics of a bare value sample, without positions.
fn classical_stats_of_values(values: &[f64]) -> RobustSampleStats {
    let npts = values.len();
    let mut moments = RunningStats::default();
    let first = values.first().copied().unwrap_or(f64::NAN);
    let (mut min, mut max) = (first, first);
    for &value in values {
        moments.push(value);
        if value.total_cmp(&min).is_lt() {
            min = value;
        }
        if value.total_cmp(&max).is_gt() {
            max = value;
        }
    }
    let [sum, sumsq, mean, rms, sigma] = if npts == 0 {
        [0.0, 0.0, f64::NAN, f64::NAN, 0.0]
    } else {
        [
            Statistic::Sum,
            Statistic::SumSq,
            Statistic::Mean,
            Statistic::Rms,
            Statistic::Sigma,
        ]
        .map(|statistic| moments.finish(statistic))
    };
    let OrderStatsSummary {
        median,
        q1,
        q3,
        quartile,
        mad: medabsdevmed,
    } = framework_order_summary(&mut values.to_vec());
    RobustSampleStats {
        npts: npts as f64,
        min,
        max,
        sum,
        sumsq,
        mean,
        rms,
        sigma,
        median,
        medabsdevmed,
        q1,
        q3,
        quartile,
        minpos: None,
        maxpos: None,
        chauvenet_iterations: None,
    }
}

fn sample_median(sample: &[SamplePoint]) -> f64 {
    let mut values: Vec<f64> = sample.iter().map(|point| point.value).collect();
    selection_median(&mut values)
}

fn sample_mean(sample: &[SamplePoint]) -> f64 {
    if sample.is_empty() {
        return f64::NAN;
    }
    sample.iter().map(|point| point.value).sum::<f64>() / sample.len() as f64
}

/// The biweight location and scale, as casacore's
/// `BiweightStatistics` computes them.
///
/// The location starts at the sample median and the scale at
/// `probit_3_4 * MAD`. With a non-negative iteration count the scale is
/// refined once from the initial weights, and then location-and-scale
/// pairs iterate until the scale's relative change falls below
/// `0.03 / sqrt(2 * (npts - 1))` or the count runs out. With a negative
/// count both are taken in one pass over the initial weights. Only
/// values strictly inside `c * scale` of the location contribute.
fn biweight_location_scale(
    values: &[f64],
    c: f64,
    max_niter: i64,
) -> Result<(f64, f64), RobustStatsError> {
    let npts = values.len() as u64;
    if npts <= 1 {
        return Err(RobustStatsError::BiweightNeedsTwoPoints { npts });
    }
    let mut workspace = values.to_vec();
    let mut location = selection_median(&mut workspace);
    for (deviation, value) in workspace.iter_mut().zip(values) {
        *deviation = (value - location).abs();
    }
    let mut scale = PROBIT_3_4 * selection_median(&mut workspace);
    drop(workspace);

    let mut spread = c * scale;
    let (mut low, mut high) = (location - spread, location + spread);
    let location_sums = |location: f64, scale: f64, low: f64, high: f64| -> f64 {
        let (mut sxw2, mut sw2) = (0.0, 0.0);
        for &value in values {
            if low < value && value < high {
                let u = (value - location) / (c * scale);
                let w = 1.0 - u * u;
                sxw2 += value * w * w;
                sw2 += w * w;
            }
        }
        sxw2 / sw2
    };
    let scale_sums = |location: f64, scale: f64, low: f64, high: f64| -> f64 {
        let (mut sx_m2w4, mut ww_4u2) = (0.0, 0.0);
        for &value in values {
            if low < value && value < high {
                let u = (value - location) / (c * scale);
                let w = 1.0 - u * u;
                sx_m2w4 += (value - location) * (value - location) * w * w * w * w;
                ww_4u2 += w * (5.0 * w - 4.0);
            }
        }
        let p = ww_4u2.abs();
        (npts as f64 * sx_m2w4 / (p * (p - 1.0).max(1.0))).sqrt()
    };

    if max_niter >= 0 {
        scale = scale_sums(location, scale, low, high);
        let epsilon = 0.03 / std::f64::consts::SQRT_2 / (npts as f64 - 1.0).sqrt();
        for iteration in 1..=max_niter {
            let previous = scale;
            location = location_sums(location, scale, low, high);
            low = location - spread;
            high = location + spread;
            scale = scale_sums(location, scale, low, high);
            if (1.0 - scale / previous).abs() < epsilon || iteration == max_niter {
                break;
            }
            spread = c * scale;
            low = location - spread;
            high = location + spread;
        }
    } else {
        let initial_location = location;
        location = location_sums(initial_location, scale, low, high);
        scale = scale_sums(initial_location, scale, low, high);
    }
    Ok((location, scale))
}

/// The maximum z-score Chauvenet's criterion allows for a sample of
/// `npts` points, as casacore's `ZScoreCalculator` computes it: the
/// tabulated breakpoints, then a bisection on
/// `0.5 / erfc(z / sqrt(2))`.
pub fn max_zscore(npts: u64) -> f64 {
    const TABLE: [(u64, f64); 14] = [
        (0, 0.5),
        (1, 1.0),
        (3, 1.5),
        (10, 2.0),
        (40, 2.5),
        (185, 3.0),
        (1074, 3.5),
        (7893, 4.0),
        (73579, 4.5),
        (872138, 5.0),
        (13165126, 5.5),
        (253398672, 6.0),
        (6225098696, 6.5),
        (195341107722, 7.0),
    ];
    if let Some((_, z)) = TABLE.iter().find(|(n, _)| *n == npts) {
        return *z;
    }
    let (mut low_z, mut high_z);
    if npts > TABLE[TABLE.len() - 1].0 {
        low_z = TABLE[TABLE.len() - 1].1;
        high_z = low_z + 0.5;
        while zscore_to_npts(high_z) < npts {
            low_z = high_z;
            high_z += 0.5;
        }
    } else {
        let pair = TABLE
            .windows(2)
            .find(|pair| pair[0].0 < npts && npts < pair[1].0)
            .expect("the table starts at zero");
        low_z = pair[0].1;
        high_z = pair[1].1;
    }
    let mut z = (low_z + high_z) / 2.0;
    loop {
        let npts_min = zscore_to_npts(z);
        if npts_min == npts || (high_z - low_z).abs() <= 1.0e-13 * high_z.abs().max(1.0) {
            return z;
        }
        if npts_min > npts {
            high_z = z;
        } else {
            low_z = z;
        }
        z = (low_z + high_z) / 2.0;
    }
}

/// The number of points whose maximum expected z-score is `z`:
/// casacore's `zscoreToNpts`, `0.5 / erfc(z / sqrt(2))`.
fn zscore_to_npts(z: f64) -> u64 {
    (0.5 / erfc(z / std::f64::consts::SQRT_2)) as u64
}

/// The complementary error function.
///
/// `erfc(x) = Q(1/2, x * x)`, the regularized upper incomplete gamma
/// function, evaluated with its alternating series for small arguments
/// and the modified Lentz continued fraction for large ones, to
/// double-precision accuracy.
fn erfc(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    if x < 0.0 {
        return 2.0 - erfc(-x);
    }
    if x == 0.0 {
        return 1.0;
    }
    if x < 1.3 {
        // The alternating Maclaurin series of erf.
        let x_squared = x * x;
        let mut term = x;
        let mut sum = x;
        for n in 1..100 {
            term *= -x_squared * (2 * n - 1) as f64 / (n * (2 * n + 1)) as f64;
            sum += term;
            if term.abs() < 1.0e-18 * sum.abs() {
                break;
            }
        }
        1.0 - 2.0 / std::f64::consts::PI.sqrt() * sum
    } else {
        // erfc(x) = Q(1/2, x^2): the continued fraction of the
        // regularized upper incomplete gamma function.
        let z = x * x;
        let a = 0.5;
        let tiny = 1.0e-300;
        let mut b = z + 1.0 - a;
        let mut c = 1.0 / tiny;
        let mut d = 1.0 / b;
        let mut h = d;
        for iteration in 1..300 {
            let an = -(iteration as f64) * (iteration as f64 - a);
            b += 2.0;
            d = an * d + b;
            if d.abs() < tiny {
                d = tiny;
            }
            c = b + an / c;
            if c.abs() < tiny {
                c = tiny;
            }
            d = 1.0 / d;
            let delta = d * c;
            h *= delta;
            if (delta - 1.0).abs() < 1.0e-16 {
                break;
            }
        }
        (-z + a * z.ln() - std::f64::consts::PI.sqrt().ln()).exp() * h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(values: &[f64]) -> Vec<SamplePoint> {
        values
            .iter()
            .enumerate()
            .map(|(index, &value)| SamplePoint {
                value,
                position: index as u64,
            })
            .collect()
    }

    #[test]
    fn algorithm_names_minimum_match() {
        for (token, expected) in [
            ("b", StatisticsAlgorithmKind::Biweight),
            ("bi", StatisticsAlgorithmKind::Biweight),
            ("biweight", StatisticsAlgorithmKind::Biweight),
            ("BIWEIGHT", StatisticsAlgorithmKind::Biweight),
            ("ch", StatisticsAlgorithmKind::Chauvenet),
            ("cla", StatisticsAlgorithmKind::Classic),
            ("classic", StatisticsAlgorithmKind::Classic),
            ("fit", StatisticsAlgorithmKind::FitToHalf),
            ("h", StatisticsAlgorithmKind::HingesFences),
            ("Hinges-Fences", StatisticsAlgorithmKind::HingesFences),
        ] {
            assert_eq!(parse_algorithm(token), Ok(expected), "token {token}");
        }
        for token in ["", "c", "x", "half", "fences"] {
            assert!(
                parse_algorithm(token).is_err(),
                "token {token} must be refused"
            );
        }
        assert_eq!(parse_algorithm("c").unwrap_err(), "Unsupported algorithm c");
        assert_eq!(parse_algorithm("").unwrap_err(), "Unsupported algorithm ");
    }

    #[test]
    fn center_and_clmethod_minimum_match() {
        assert_eq!(parse_center("mea"), Ok(FitHalfCenter::Mean));
        assert_eq!(parse_center("MEAN"), Ok(FitHalfCenter::Mean));
        assert_eq!(parse_center("med"), Ok(FitHalfCenter::Median));
        assert_eq!(parse_center("Med"), Ok(FitHalfCenter::Median));
        assert_eq!(parse_center("MEDIAN"), Ok(FitHalfCenter::Median));
        assert_eq!(parse_center("z"), Ok(FitHalfCenter::Zero));
        assert_eq!(parse_center("zer"), Ok(FitHalfCenter::Zero));
        // A minimum match of the mean and median needs three letters,
        // as CASA's parser requires; shorter m-tokens are ambiguous.
        assert_eq!(parse_center("m").unwrap_err(), "Unsupported center value m");
        assert_eq!(
            parse_center("me").unwrap_err(),
            "Unsupported center value me"
        );
        assert_eq!(
            parse_center("bad").unwrap_err(),
            "Unsupported center value bad"
        );
        assert!(parse_clmethod("a").is_ok());
        assert!(parse_clmethod("F").is_ok());
        assert_eq!(
            parse_clmethod("bad").unwrap_err(),
            "Unsupported classical method bad"
        );
    }

    #[test]
    fn framework_quantiles_and_median() {
        let stats = classical_stats_of_values(&[7.0, 0.0, 2.0, 1.0, 6.0, 4.0, 5.0, 3.0]);
        assert_eq!([stats.q1, stats.median, stats.q3], [1.0, 3.5, 5.0]);
        assert_eq!(
            sample_median(&sample(&[6.0, 4.0, 0.0, 2.0, 3.0, 5.0, 1.0])),
            3.0
        );
    }

    #[test]
    fn erfc_matches_the_system_values() {
        let reference = [
            (0.0, 1.0),
            (0.5, 0.4795001221869535),
            (1.0, 0.15729920705028516),
            (1.3, 0.06599205505934755),
            (2.0, 0.0046777349810472645),
            (3.0, 2.2090496998585438e-5),
            (5.0, 1.537459794428035e-12),
            (7.0, 4.183825607779415e-23),
        ];
        for (x, expected) in reference {
            let relative = if expected == 0.0 {
                f64::INFINITY
            } else {
                ((erfc(x) - expected) / expected).abs()
            };
            assert!(relative < 1.0e-12, "erfc({x}) = {} vs {expected}", erfc(x));
        }
        assert!((erfc(-1.0) - (2.0 - 0.15729920705028516)).abs() < 1.0e-15);
    }

    #[test]
    fn zscores_at_the_tabulated_breakpoints() {
        for (npts, z) in [
            (0, 0.5),
            (1, 1.0),
            (3, 1.5),
            (10, 2.0),
            (40, 2.5),
            (185, 3.0),
            (1074, 3.5),
            (7893, 4.0),
            (73579, 4.5),
            (872138, 5.0),
            (13165126, 5.5),
            (253398672, 6.0),
        ] {
            assert_eq!(max_zscore(npts), z, "npts {npts}");
        }
        // Between breakpoints the criterion reproduces the sample size.
        let z = max_zscore(256);
        assert!((3.0..3.5).contains(&z), "z(256) = {z}");
        assert_eq!(zscore_to_npts(z), 256);
        let z = max_zscore(14);
        assert!((2.0..2.5).contains(&z), "z(14) = {z}");
        assert_eq!(zscore_to_npts(z), 14);
    }

    #[test]
    fn biweight_needs_two_points() {
        let one = sample(&[4.0]);
        let error =
            compute_robust_stats(&one, &StatisticsAlgorithm::Biweight { niter: 3 }).unwrap_err();
        assert_eq!(
            error.to_string(),
            "npts is 1. There must be at least two points to compute the biweight location and scale"
        );
    }

    #[test]
    fn biweight_on_a_symmetric_sample_returns_its_center() {
        let values = sample(&[-4.0, -2.0, 2.0, 4.0]);
        let (location, scale) =
            match compute_robust_stats(&values, &StatisticsAlgorithm::Biweight { niter: 3 })
                .unwrap()
            {
                RobustStats::Biweight { location, scale } => (location, scale),
                other => panic!("the biweight reports its location: {other:?}"),
            };
        assert!(location.abs() < 1.0e-12, "location {location}");
        assert!(scale > 0.0);
        // A single point on each side keeps the fast path finite too.
        let (location, scale) =
            match compute_robust_stats(&values, &StatisticsAlgorithm::Biweight { niter: -1 })
                .unwrap()
            {
                RobustStats::Biweight { location, scale } => (location, scale),
                other => panic!("the biweight reports its location: {other:?}"),
            };
        assert!(location.abs() < 1.0e-12, "location {location}");
        assert!(scale > 0.0);
    }

    #[test]
    fn fit_half_reflects_about_the_center() {
        let points = sample(&[1.0, 2.0, 8.0]);
        let stats = match compute_robust_stats(
            &points,
            &StatisticsAlgorithm::FitToHalf {
                center: FitHalfCenter::Mean,
                upper: false,
            },
        )
        .unwrap()
        {
            RobustStats::Sample(stats) => stats,
            other => panic!("fit-half reports sample statistics: {other:?}"),
        };
        let center = 11.0 / 3.0;
        assert_eq!(stats.npts, 4.0);
        assert!((stats.mean - center).abs() < 1.0e-12);
        assert_eq!(stats.median, center);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.minpos, Some(0));
        assert!((stats.max - (2.0 * center - 1.0)).abs() < 1.0e-12);
        assert_eq!(stats.maxpos, None);
    }

    #[test]
    fn fit_half_upper_keeps_the_real_maximum() {
        let points = sample(&[1.0, 2.0, 8.0]);
        let stats = match compute_robust_stats(
            &points,
            &StatisticsAlgorithm::FitToHalf {
                center: FitHalfCenter::Mean,
                upper: true,
            },
        )
        .unwrap()
        {
            RobustStats::Sample(stats) => stats,
            other => panic!("fit-half reports sample statistics: {other:?}"),
        };
        let center = 11.0 / 3.0;
        assert_eq!(stats.npts, 2.0);
        assert_eq!(stats.max, 8.0);
        assert_eq!(stats.maxpos, Some(2));
        assert!((stats.min - (2.0 * center - 8.0)).abs() < 1.0e-12);
        assert_eq!(stats.minpos, None);
    }

    #[test]
    fn fit_half_without_kept_values_fails() {
        let points = sample(&[1.0, 2.0, 8.0]);
        let error = compute_robust_stats(
            &points,
            &StatisticsAlgorithm::FitToHalf {
                center: FitHalfCenter::Zero,
                upper: false,
            },
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "No data included using current configuration, cannot compute min and max"
        );
    }

    #[test]
    fn hinges_fences_cut_the_outliers() {
        let mut values: Vec<f64> = (0..100).map(|index| index as f64).collect();
        values.push(1000.0);
        let points = sample(&values);
        let stats =
            match compute_robust_stats(&points, &StatisticsAlgorithm::HingesFences { fence: 1.0 })
                .unwrap()
            {
                RobustStats::Sample(stats) => stats,
                other => panic!("hinges-fences reports sample statistics: {other:?}"),
            };
        // q1 = 25, q3 = 75, so the fences are [-25, 125]: the spike is
        // out.
        assert_eq!(stats.npts, 100.0);
        assert_eq!(stats.max, 99.0);
        // A negative fence keeps everything, as classic does.
        let stats =
            match compute_robust_stats(&points, &StatisticsAlgorithm::HingesFences { fence: -1.0 })
                .unwrap()
            {
                RobustStats::Sample(stats) => stats,
                other => panic!("hinges-fences reports sample statistics: {other:?}"),
            };
        assert_eq!(stats.npts, 101.0);
        assert_eq!(stats.max, 1000.0);
    }

    #[test]
    fn chauvenet_converges_on_a_clean_sample() {
        let mut values: Vec<f64> = (0..100).map(|index| index as f64).collect();
        values.push(1000.0);
        let points = sample(&values);
        let stats = match compute_robust_stats(
            &points,
            &StatisticsAlgorithm::Chauvenet {
                zscore: -1.0,
                maxiter: -1,
            },
        )
        .unwrap()
        {
            RobustStats::Sample(stats) => stats,
            other => panic!("chauvenet reports sample statistics: {other:?}"),
        };
        assert_eq!(stats.npts, 100.0);
        assert_eq!(stats.max, 99.0);
        // A tight fixed z-score drives the sample to a stable core.
        let stats = match compute_robust_stats(
            &points,
            &StatisticsAlgorithm::Chauvenet {
                zscore: 0.5,
                maxiter: -1,
            },
        )
        .unwrap()
        {
            RobustStats::Sample(stats) => stats,
            other => panic!("chauvenet reports sample statistics: {other:?}"),
        };
        assert!(stats.npts < 100.0, "the range tightens: {stats:?}");
    }
}
