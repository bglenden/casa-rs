// SPDX-License-Identifier: LGPL-3.0-or-later
//! Combine the visibility iterator's time-averaged rows into PlotMS baseline averages.

use std::collections::HashMap;

use ndarray::ArrayView2;

use super::{AveragedSeriesAccumulator, AveragingTimeKey, ChannelBin, FloatGrid};
use crate::transform::{PlotTimeGroups, mean_weight_preferring_unflagged};

#[derive(Clone, Copy)]
pub(super) struct Integration {
    pub(super) time: f64,
    pub(super) interval: f64,
}

pub(super) struct TimeBaselineRow<'a> {
    pub(super) time_group: usize,
    pub(super) partition: [i32; 4],
    pub(super) integration: Integration,
    pub(super) first_correlation: usize,
    pub(super) bins: &'a [ChannelBin],
    pub(super) flags: &'a ArrayView2<'a, bool>,
    pub(super) weights: Option<&'a FloatGrid>,
    pub(super) has_unflagged_visibility: bool,
}

#[derive(Default)]
pub(super) struct TimeBaselineStatistics {
    groups: HashMap<usize, TimeAverageStatistics>,
}

struct TimeAverageStatistics {
    partition: [i32; 4],
    first: Integration,
    last: Integration,
    last_usable: Option<Integration>,
    weights: Vec<ChannelWeightSum>,
}

struct ChannelWeightSum {
    flagged: bool,
    sum: f32,
}

impl Default for ChannelWeightSum {
    fn default() -> Self {
        Self {
            flagged: true,
            sum: 0.0,
        }
    }
}

impl ChannelWeightSum {
    fn observe(&mut self, flagged: bool, weight: f32) {
        if flagged == self.flagged {
            self.sum += weight;
        } else if self.flagged {
            self.flagged = false;
            self.sum = weight;
        }
    }
}

impl TimeAverageStatistics {
    fn integration(&self) -> Integration {
        let last = self.last_usable.unwrap_or(self.last);
        let elapsed = last.time - self.first.time;
        Integration {
            time: self.first.time + elapsed * 0.5,
            interval: if elapsed == 0.0 {
                self.first.interval
            } else {
                elapsed + (self.first.interval + last.interval) * 0.5
            },
        }
    }

    fn baseline_weight(&self) -> f64 {
        let weight =
            mean_weight_preferring_unflagged(self.weights.iter().map(|bin| (bin.flagged, bin.sum)));
        f64::from(if weight < f32::MIN_POSITIVE {
            f32::MIN_POSITIVE
        } else {
            weight
        })
    }
}

impl TimeBaselineStatistics {
    pub(super) fn observe(&mut self, row: TimeBaselineRow<'_>) {
        let group = self
            .groups
            .entry(row.time_group)
            .or_insert_with(|| TimeAverageStatistics {
                partition: row.partition,
                first: row.integration,
                last: row.integration,
                last_usable: None,
                weights: row
                    .bins
                    .iter()
                    .map(|_| ChannelWeightSum::default())
                    .collect(),
            });
        if row.integration.time < group.first.time {
            group.first = row.integration;
        }
        if row.integration.time > group.last.time {
            group.last = row.integration;
        }
        if row.has_unflagged_visibility
            && group
                .last_usable
                .is_none_or(|last| row.integration.time > last.time)
        {
            group.last_usable = Some(row.integration);
        }
        for (bin, sample) in row.bins.iter().zip(&mut group.weights) {
            let channel = bin.start;
            let correlation = row.first_correlation;
            let weight = row.weights.map_or(1.0, |grid| {
                grid.values[correlation * grid.chan_count + channel] as f32
            });
            sample.observe(row.flags[(correlation, channel)], weight);
        }
    }

    pub(super) fn plan(
        self,
        time_plan: &PlotTimeGroups,
        rows: &[usize],
        interval: f64,
    ) -> TimeBaselinePlan {
        let mut averages: Vec<BaselineAverage> = Vec::new();
        let mut rows_to_average = HashMap::with_capacity(self.groups.len());
        for buffer in time_plan.buffers() {
            let Some(anchor) = self.groups.get(&buffer.first_group) else {
                continue;
            };
            let bounds = anchor.integration();
            let (start, end) = (
                bounds.time - bounds.interval / 2.0,
                bounds.time + bounds.interval / 2.0,
            );
            let starts_new_average = averages.last().is_none_or(|average| {
                let elapsed = bounds.time - average.anchor_time;
                elapsed < 0.0 || elapsed > interval || average.partition != anchor.partition
            });
            if starts_new_average {
                averages.push(BaselineAverage {
                    partition: anchor.partition,
                    anchor_time: bounds.time,
                    row: rows[time_plan.first_row_slot(buffer.first_group)],
                    start,
                    end,
                });
            } else if let Some(average) = averages.last_mut() {
                average.start = average.start.min(start);
                average.end = average.end.max(end);
            }
            let output = averages.len() - 1;
            for group in buffer.groups.clone() {
                if let Some(statistics) = self.groups.get(&group) {
                    rows_to_average.insert(group, (output, statistics.baseline_weight()));
                }
            }
        }
        TimeBaselinePlan {
            averages,
            rows_to_average,
        }
    }
}

struct BaselineAverage {
    partition: [i32; 4],
    anchor_time: f64,
    row: usize,
    start: f64,
    end: f64,
}

pub(super) struct TimeBaselinePlan {
    averages: Vec<BaselineAverage>,
    rows_to_average: HashMap<usize, (usize, f64)>,
}

impl TimeBaselinePlan {
    pub(super) fn center(&self, average: usize) -> Option<f64> {
        self.averages
            .get(average)
            .map(|average| average.start + (average.end - average.start) / 2.0)
    }

    /// Each point here is a normalized complex time average for one baseline.
    /// Only project to the displayed amplitude or phase after this second mean.
    pub(super) fn combine_series(&self, series: &mut AveragedSeriesAccumulator) {
        for (mut key, mut point) in std::mem::take(&mut series.points) {
            let Some(&(group, weight)) = key
                .time_key
                .group()
                .and_then(|time_group| self.rows_to_average.get(&time_group))
            else {
                continue;
            };
            let Some(y) = point.y_visibility.vector_mean() else {
                continue;
            };
            let x = point.x_visibility.vector_mean();
            key.time_key = AveragingTimeKey::Group(group);
            let output = series.points.entry(key).or_default();
            if output.representative.is_none() {
                if let Some(mut representative) = point.representative.take() {
                    representative.row = self.averages[group].row;
                    output.representative = Some(representative);
                }
                output.display_channel = point.display_channel;
                output.numeric_axis_samples = point.numeric_axis_samples;
            }
            if let Some(x) = x {
                output.x_visibility.add_weighted_value(x, weight);
            }
            output.y_visibility.add_weighted_value(y, weight);
        }
    }
}
