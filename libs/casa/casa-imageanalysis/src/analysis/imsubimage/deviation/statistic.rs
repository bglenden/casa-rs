// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::statistics::RunningStats;
use casa_lattices::{
    RobustStats, SamplePoint, Statistic, StatisticsAlgorithm, compute_robust_stats,
};

use casa_images::ImageError;

use super::invalid;

pub(super) struct LocalStatistic {
    kind: Statistic,
    scale: f64,
    algorithm: StatisticsAlgorithm,
}

pub(super) enum Samples {
    Running(RunningStats),
    Retained(Vec<SamplePoint>),
}

impl Samples {
    pub(super) fn clear(&mut self) {
        match self {
            Self::Running(stats) => *stats = RunningStats::default(),
            Self::Retained(points) => points.clear(),
        }
    }

    pub(super) fn push(&mut self, value: f32) {
        let value = f64::from(value);
        match self {
            Self::Running(stats) => stats.push(value),
            Self::Retained(points) => points.push(SamplePoint {
                value,
                position: points.len() as u64,
            }),
        }
    }
}

impl LocalStatistic {
    pub(super) fn new(
        name: &str,
        algorithm: &str,
        zscore: f64,
        maxiter: i64,
    ) -> Result<Self, ImageError> {
        use Statistic as S;
        let kind = name.to_ascii_lowercase();
        let stat = [
            ("i", S::Quartile),
            ("max", S::Max),
            ("mea", S::Mean),
            ("meda", S::MedAbsDevMed),
            ("mad", S::MedAbsDevMed),
            ("x", S::MedAbsDevMed),
            ("medi", S::Median),
            ("mi", S::Min),
            ("n", S::Npts),
            ("q1", S::Q1),
            ("q3", S::Q3),
            ("r", S::Rms),
            ("si", S::Sigma),
            ("st", S::Sigma),
            ("sums", S::SumSq),
            ("sum", S::Sum),
            ("v", S::Variance),
        ]
        .into_iter()
        .find_map(|(prefix, stat)| kind.starts_with(prefix).then_some(stat))
        .ok_or_else(|| invalid(format!("Statistic {name} not supported.")))?;
        let normalized = algorithm.to_ascii_lowercase();
        let algorithm = if normalized.starts_with("cl") {
            StatisticsAlgorithm::Classic
        } else if normalized.starts_with("ch") {
            StatisticsAlgorithm::Chauvenet { zscore, maxiter }
        } else {
            return Err(invalid(format!("Unsupported stats algorithm {algorithm}")));
        };
        Ok(Self {
            kind: stat,
            scale: if kind.starts_with('x') {
                1.482602218505602
            } else {
                1.
            },
            algorithm,
        })
    }

    pub(super) fn samples(&self) -> Samples {
        use Statistic as S;
        match (self.algorithm, self.kind) {
            (
                StatisticsAlgorithm::Classic,
                S::Median | S::Q1 | S::Q3 | S::Quartile | S::MedAbsDevMed,
            )
            | (StatisticsAlgorithm::Chauvenet { .. }, _) => Samples::Retained(Vec::new()),
            _ => Samples::Running(RunningStats::default()),
        }
    }

    pub(super) fn evaluate(&self, samples: &Samples) -> Result<Option<f32>, ImageError> {
        use Statistic as S;
        let points = match samples {
            Samples::Running(stats) => {
                return Ok(stats.statistic(self.kind).map(|value| value as f32));
            }
            Samples::Retained(points) => points,
        };
        if points.is_empty() {
            return Ok(None);
        }
        if let &[point] = points.as_slice() {
            return Ok(Some(self.single(point.value as f32)));
        }
        let RobustStats::Sample(stats) = compute_robust_stats(points, &self.algorithm)
            .map_err(|error| invalid(error.to_string()))?
        else {
            unreachable!("the local image statistics algorithms return samples");
        };
        if stats.npts == 0. {
            return match self.kind {
                S::Min => Err(invalid(
                    "Logic Error: stat min is not defined. Please file a defect report.",
                )),
                S::Max => Err(invalid(
                    "Logic Error: stat max is not defined. Please file a defect report.",
                )),
                S::Median | S::MedAbsDevMed | S::Q1 | S::Q3 | S::Quartile => {
                    Err(invalid("No valid data found"))
                }
                _ => Ok(Some(0.)),
            };
        }
        let value = match self.kind {
            S::Npts => stats.npts,
            S::Min => stats.min,
            S::Max => stats.max,
            S::Sum => stats.sum,
            S::SumSq => stats.sumsq,
            S::Mean => stats.mean,
            S::Rms => stats.rms,
            S::Variance => stats.sigma * stats.sigma,
            S::Sigma => stats.sigma,
            S::Median => stats.median,
            S::MedAbsDevMed => stats.medabsdevmed,
            S::Q1 => stats.q1,
            S::Q3 => stats.q3,
            S::Quartile => stats.quartile,
        };
        Ok(Some((f64::from(value as f32) * self.scale) as f32))
    }

    pub(super) fn single(&self, value: f32) -> f32 {
        use Statistic as S;
        match self.kind {
            S::Npts => 1.,
            S::SumSq => value * value,
            S::Rms => value.abs(),
            S::Variance | S::Sigma | S::Quartile | S::MedAbsDevMed => 0.,
            _ => value,
        }
    }
}
