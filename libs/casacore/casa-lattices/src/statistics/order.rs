// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exact order statistics for the casacore statistics framework.

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

pub(crate) fn framework_order_summary(values: &mut [f64]) -> OrderStatsSummary {
    if values.is_empty() {
        return OrderStatsSummary::nan();
    }
    let [q1, median, q3] = quartiles_and_median(values);
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }
    OrderStatsSummary {
        median,
        q1,
        q3,
        quartile: q3 - q1,
        mad: selection_median(values),
    }
}

/// Casacore takes the fractile at ceil(q * n) - 1 and averages the middle
/// pair for an even median. Partition around the median once so both
/// quartiles can be selected within their respective halves.
pub(crate) fn quartiles_and_median(values: &mut [f64]) -> [f64; 3] {
    let len = values.len();
    if len < 2 {
        return [values.first().copied().unwrap_or(f64::NAN); 3];
    }

    let middle = len / 2;
    let (lower, upper_middle, upper) = values.select_nth_unstable_by(middle, f64::total_cmp);
    let median = center_from_partition(lower, *upper_middle, len);
    let q1 = *lower
        .select_nth_unstable_by(len.div_ceil(4) - 1, f64::total_cmp)
        .1;
    let q3_rank = len - len / 4 - 1;
    let q3 = if q3_rank == middle {
        *upper_middle
    } else {
        *upper
            .select_nth_unstable_by(q3_rank - middle - 1, f64::total_cmp)
            .1
    };
    [q1, median, q3]
}

pub(crate) fn selection_median(values: &mut [f64]) -> f64 {
    let len = values.len();
    if len == 0 {
        return f64::NAN;
    }
    let (lower, upper_middle, _) = values.select_nth_unstable_by(len / 2, f64::total_cmp);
    center_from_partition(lower, *upper_middle, len)
}

fn center_from_partition(lower: &[f64], upper_middle: f64, len: usize) -> f64 {
    if len.is_multiple_of(2) {
        let lower_middle = lower.iter().copied().max_by(f64::total_cmp).unwrap();
        (lower_middle + upper_middle) / 2.0
    } else {
        upper_middle
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
            let summary = framework_order_summary(&mut data[..len].to_vec());
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
