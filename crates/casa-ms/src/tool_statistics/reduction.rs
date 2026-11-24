// SPDX-License-Identifier: LGPL-3.0-or-later
//! Classical moments and order statistics for projected MS samples.

#[derive(Clone, Copy)]
pub(super) struct Extremum {
    pub(super) value: f64,
    pub(super) dataset: usize,
    pub(super) index: usize,
}

pub(super) struct Moments {
    pub(super) count: usize,
    pub(super) sum: f64,
    pub(super) sum_squares: f64,
    pub(super) sum_weights: f64,
    pub(super) mean: f64,
    squared_deviations: f64,
    pub(super) min: Option<Extremum>,
    pub(super) max: Option<Extremum>,
    values: Option<Vec<f64>>,
}

pub(super) struct Quantiles {
    pub(super) first: f64,
    pub(super) median: f64,
    pub(super) third: f64,
    pub(super) median_deviation: f64,
}

impl Moments {
    pub(super) fn new(quantiles: bool) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_squares: 0.0,
            sum_weights: 0.0,
            mean: 0.0,
            squared_deviations: 0.0,
            min: None,
            max: None,
            values: quantiles.then(Vec::new),
        }
    }

    pub(super) fn push(
        &mut self,
        value: f64,
        square: f64,
        weight: f64,
        dataset: usize,
        index: usize,
    ) {
        if weight <= 0.0 {
            return;
        }
        self.count += 1;
        self.sum += value * weight;
        self.sum_squares += square * weight;
        self.sum_weights += weight;
        let residual = value - self.mean;
        self.mean += residual * weight / self.sum_weights;
        self.squared_deviations += weight * residual * (value - self.mean);

        let candidate = Extremum {
            value,
            dataset,
            index,
        };
        if self.min.is_none_or(|min| value < min.value) {
            self.min = Some(candidate);
        }
        if self.max.is_none_or(|max| value > max.value) {
            self.max = Some(candidate);
        }
        if let Some(values) = &mut self.values {
            values.push(value);
        }
    }

    pub(super) fn variance(&self) -> f64 {
        if self.sum_weights > 1.0 {
            self.squared_deviations / (self.sum_weights - 1.0)
        } else {
            0.0
        }
    }

    pub(super) fn rms(&self) -> f64 {
        (self.sum_squares / self.sum_weights).sqrt()
    }

    pub(super) fn quantiles(&mut self) -> Option<Quantiles> {
        let values = self.values.as_mut()?;
        let count = values.len();
        if count == 0 {
            return None;
        }
        let middle = count / 2;
        let (lower, upper, higher) = values.select_nth_unstable_by(middle, f64::total_cmp);
        let median = if count.is_multiple_of(2) {
            let largest_lower = lower.iter().copied().max_by(f64::total_cmp).unwrap();
            (largest_lower + *upper) / 2.0
        } else {
            *upper
        };
        let first_index = count.div_ceil(4) - 1;
        let third_index = count - count / 4 - 1;
        let first = if first_index == middle {
            *upper
        } else {
            *lower.select_nth_unstable_by(first_index, f64::total_cmp).1
        };
        let third = if third_index == middle {
            *upper
        } else {
            *higher
                .select_nth_unstable_by(third_index - middle - 1, f64::total_cmp)
                .1
        };
        for value in values.iter_mut() {
            *value = (*value - median).abs();
        }
        let (lower, upper, _) = values.select_nth_unstable_by(middle, f64::total_cmp);
        let median_deviation = if count.is_multiple_of(2) {
            let largest_lower = lower.iter().copied().max_by(f64::total_cmp).unwrap();
            (largest_lower + *upper) / 2.0
        } else {
            *upper
        };
        Some(Quantiles {
            first,
            median,
            third,
            median_deviation,
        })
    }
}
