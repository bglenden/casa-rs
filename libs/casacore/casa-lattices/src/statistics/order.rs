// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exact order statistics for the casacore statistics framework.

use casa_core::cancellation::Cancellation;

use super::progress::{BATCH_SIZE, StatisticsInterrupted, batches, check};
#[cfg(feature = "statistics-test-hooks")]
use crate::statistics_test_hooks::{Checkpoint, observe};

#[derive(Clone, Copy)]
pub(crate) struct OrderStatsSummary {
    pub(crate) median: f64,
    pub(crate) q1: f64,
    pub(crate) q3: f64,
    pub(crate) quartile: f64,
    pub(crate) mad: f64,
}

impl OrderStatsSummary {
    pub(super) fn nan() -> Self {
        Self {
            median: f64::NAN,
            q1: f64::NAN,
            q3: f64::NAN,
            quartile: f64::NAN,
            mad: f64::NAN,
        }
    }
}

pub(crate) fn framework_order_summary(
    values: &mut [f64],
    cancellation: Option<&Cancellation>,
) -> Result<OrderStatsSummary, StatisticsInterrupted> {
    if values.is_empty() {
        return Ok(OrderStatsSummary::nan());
    }
    let [q1, median, q3] = quartiles_and_median(values, cancellation)?;
    absolute_deviations(values, median, cancellation)?;
    Ok(OrderStatsSummary {
        median,
        q1,
        q3,
        quartile: q3 - q1,
        mad: selection_median(values, cancellation)?,
    })
}

pub(super) fn absolute_deviations(
    values: &mut [f64],
    median: f64,
    cancellation: Option<&Cancellation>,
) -> Result<(), StatisticsInterrupted> {
    for batch in values.chunks_mut(BATCH_SIZE) {
        check(cancellation)?;
        #[cfg(feature = "statistics-test-hooks")]
        observe(Checkpoint::DeviationBatchStarted);
        for value in batch {
            *value = (*value - median).abs();
        }
        #[cfg(feature = "statistics-test-hooks")]
        observe(Checkpoint::DeviationBatchFinished);
    }
    Ok(())
}

/// Casacore takes the fractile at ceil(q * n) - 1 and averages the middle
/// pair for an even median. Partition around the median once so both
/// quartiles can be selected within their respective halves.
pub(crate) fn quartiles_and_median(
    values: &mut [f64],
    cancellation: Option<&Cancellation>,
) -> Result<[f64; 3], StatisticsInterrupted> {
    check(cancellation)?;
    let len = values.len();
    if len < 2 {
        return Ok([values.first().copied().unwrap_or(f64::NAN); 3]);
    }

    let middle = len / 2;
    let (lower, upper_middle, upper) = values.select_nth_unstable_by(middle, f64::total_cmp);
    let median = center_from_partition(lower, *upper_middle, len, cancellation)?;
    check(cancellation)?;
    let q1 = *lower
        .select_nth_unstable_by(len.div_ceil(4) - 1, f64::total_cmp)
        .1;
    let q3_rank = len - len / 4 - 1;
    let q3 = if q3_rank == middle {
        *upper_middle
    } else {
        check(cancellation)?;
        *upper
            .select_nth_unstable_by(q3_rank - middle - 1, f64::total_cmp)
            .1
    };
    Ok([q1, median, q3])
}

pub(crate) fn selection_median(
    values: &mut [f64],
    cancellation: Option<&Cancellation>,
) -> Result<f64, StatisticsInterrupted> {
    check(cancellation)?;
    let len = values.len();
    if len == 0 {
        return Ok(f64::NAN);
    }
    let (lower, upper_middle, _) = values.select_nth_unstable_by(len / 2, f64::total_cmp);
    center_from_partition(lower, *upper_middle, len, cancellation)
}

fn center_from_partition(
    lower: &[f64],
    upper_middle: f64,
    len: usize,
    cancellation: Option<&Cancellation>,
) -> Result<f64, StatisticsInterrupted> {
    if len.is_multiple_of(2) {
        let mut lower_middle = lower[0];
        for batch in batches(lower, cancellation) {
            let candidate = batch?.iter().copied().max_by(f64::total_cmp).unwrap();
            if candidate.total_cmp(&lower_middle).is_gt() {
                lower_middle = candidate;
            }
        }
        Ok((lower_middle + upper_middle) / 2.0)
    } else {
        Ok(upper_middle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_statistics_match_a_full_sort() {
        let data = [4.0, -0.0, 8.0, 0.0, 4.0, -1.0, 2.0, 9.0, 2.0, f64::INFINITY];
        let sorted_median = |sorted: &[f64]| {
            let middle = sorted.len() / 2;
            if sorted.len().is_multiple_of(2) {
                (sorted[middle - 1] + sorted[middle]) / 2.0
            } else {
                sorted[middle]
            }
        };
        for len in 1..=data.len() {
            let mut sorted = data[..len].to_vec();
            sorted.sort_by(f64::total_cmp);
            let median = sorted_median(&sorted);
            let q1 = sorted[(0.25 * len as f64).ceil() as usize - 1];
            let q3 = sorted[(0.75 * len as f64).ceil() as usize - 1];
            let mut deviations: Vec<_> =
                sorted.iter().map(|value| (value - median).abs()).collect();
            deviations.sort_by(f64::total_cmp);
            let expected = [q1, median, q3, q3 - q1, sorted_median(&deviations)];
            let summary = framework_order_summary(&mut data[..len].to_vec(), None).unwrap();
            let actual = [
                summary.q1,
                summary.median,
                summary.q3,
                summary.quartile,
                summary.mad,
            ];
            assert_eq!(
                actual.map(f64::to_bits),
                expected.map(f64::to_bits),
                "len {len}"
            );
        }
    }
}
