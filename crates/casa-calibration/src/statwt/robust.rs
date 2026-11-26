// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exposure-weighted robust variances for the real and imaginary visibility parts.

use casa_core::Complex32;
use casa_lattices::robust_statistics::{FitHalfCenter, max_zscore};
use casa_ms::{MsError, MsResult};

use super::classic::Moments;

/// Statistical algorithm used to estimate a visibility's variance.
#[derive(Clone, Copy, Default)]
pub enum StatWtAlgorithm {
    /// Classical moments over all the unflagged samples.
    #[default]
    Classic,
    /// Estimate over the interval delimited by multiples of the IQR.
    HingesFences {
        /// Multiple of the IQR beyond a quartile. Negative keeps every value.
        fence: f64,
    },
    /// Iteratively estimate over samples within a standard deviation limit.
    Chauvenet {
        /// Limit in standard deviations. Negative uses Chauvenet's criterion.
        zscore: f64,
        /// Maximum refinements. Negative seeks convergence.
        maxiter: i64,
    },
    /// Reflect one side of the sample about its center.
    FitToHalf {
        /// Quantity about which to reflect the sample.
        center: FitHalfCenter,
        /// Keep samples on the lower side of the center.
        lower: bool,
    },
}

impl StatWtAlgorithm {
    /// Parse the CASA algorithm name and the parameters it uses.
    pub fn parse(
        name: &str,
        fence: f64,
        center: &str,
        lower: bool,
        zscore: f64,
        maxiter: i64,
    ) -> MsResult<Self> {
        let name = name.to_ascii_lowercase();
        if name.starts_with("cl") {
            Ok(Self::Classic)
        } else if name.starts_with("ch") {
            Ok(Self::Chauvenet { zscore, maxiter })
        } else if name.starts_with('h') {
            Ok(Self::HingesFences { fence })
        } else if name.starts_with('f') {
            let center = match center.to_ascii_lowercase().as_str() {
                "mean" => FitHalfCenter::Mean,
                "median" => FitHalfCenter::Median,
                "zero" => FitHalfCenter::Zero,
                _ => {
                    return Err(MsError::InvalidInput(format!(
                        "unknown statwt center {center:?}"
                    )));
                }
            };
            Ok(Self::FitToHalf { center, lower })
        } else {
            Err(MsError::InvalidInput(format!(
                "unknown statwt algorithm {name:?}"
            )))
        }
    }

    pub(super) fn uses_classical_moments(self) -> bool {
        matches!(self, Self::Classic) || matches!(self, Self::HingesFences { fence } if fence < 0.0)
    }

    pub(super) fn variance(self, samples: &[(Complex32, f64)]) -> f64 {
        let mut part: Vec<_> = samples
            .iter()
            .filter(|(_, exposure)| *exposure > 0.0)
            .map(|(data, exposure)| (f64::from(data.re), *exposure))
            .collect();
        let real = self.component_variance(&part);
        part.clear();
        part.extend(
            samples
                .iter()
                .filter(|(_, exposure)| *exposure > 0.0)
                .map(|(data, exposure)| (f64::from(data.im), *exposure)),
        );
        (real + self.component_variance(&part)) / 2.0
    }

    fn component_variance(self, points: &[(f64, f64)]) -> f64 {
        if points.is_empty() {
            return f64::NAN;
        }
        let moments = |low: f64, high: f64| {
            let mut result = Moments::default();
            for &(value, weight) in points {
                if low <= value && value <= high {
                    result.add_weighted(value, weight);
                }
            }
            result
        };
        let all = || moments(f64::NEG_INFINITY, f64::INFINITY);
        let sorted = || {
            let mut sorted = points.iter().map(|&(value, _)| value).collect::<Vec<_>>();
            sorted.sort_unstable_by(f64::total_cmp);
            sorted
        };
        let accepted = match self {
            Self::Classic => all(),
            Self::HingesFences { fence } if fence >= 0.0 => {
                let sorted = sorted();
                let q1 = sorted[sorted.len().div_ceil(4) - 1];
                let q3 = sorted[sorted.len() - sorted.len() / 4 - 1];
                let margin = fence * (q3 - q1);
                moments(q1 - margin, q3 + margin)
            }
            Self::HingesFences { .. } => all(),
            Self::Chauvenet { zscore, maxiter } => {
                let mut accepted = all();
                let iterations = if maxiter < 0 { 1000 } else { maxiter };
                for _ in 0..=iterations {
                    if accepted.count <= 1 {
                        break;
                    }
                    let score = if zscore < 0.0 {
                        max_zscore(accepted.count as u64)
                    } else {
                        zscore
                    };
                    let variance = if accepted.total_weight > 1.0 {
                        accepted.squared_deviation / (accepted.total_weight - 1.0)
                    } else {
                        0.0
                    };
                    let limit = score * variance.sqrt();
                    let next = moments(accepted.mean - limit, accepted.mean + limit);
                    if next.count == accepted.count {
                        accepted = next;
                        break;
                    }
                    accepted = next;
                }
                accepted
            }
            Self::FitToHalf { center, lower } => {
                let center = match center {
                    FitHalfCenter::Mean => all().mean,
                    FitHalfCenter::Median => {
                        let sorted = sorted();
                        (sorted[sorted.len() / 2] + sorted[(sorted.len() - 1) / 2]) / 2.0
                    }
                    FitHalfCenter::Zero => 0.0,
                };
                let mut variance = 0.0;
                let mut count = 0;
                for &(value, weight) in points {
                    if (lower && value <= center) || (!lower && value >= center) {
                        variance += weight * (value - center).powi(2);
                        count += 1;
                    }
                }
                return if count == 0 {
                    f64::NAN
                } else {
                    variance / f64::from(count)
                };
            }
        };
        if accepted.count == 0 {
            0.0
        } else {
            accepted.squared_deviation / accepted.count as f64
        }
    }
}
