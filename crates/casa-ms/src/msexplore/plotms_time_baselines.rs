// SPDX-License-Identifier: LGPL-3.0-or-later
//! Combine the visibility iterator's time-averaged rows across baselines or by antenna.

use std::collections::HashMap;

use ndarray::ArrayView2;

use super::plotms_ordinates::VisibilityAverageShapes;
use super::{
    AveragedSeriesAccumulator, AveragingBaselineToken, AveragingPointKey, AveragingTimeKey,
    ChannelBin, FloatGrid, MsAverageSpec, plotms_columns,
};
use crate::transform::{
    PlotTimeGroups, centered_time_and_interval, mean_weight_preferring_unflagged,
};

#[derive(Clone, Copy)]
pub(super) struct Integration {
    pub(super) time: f64,
    pub(super) interval: f64,
}

pub(super) struct TimeBaselineRow<'a> {
    pub(super) time_group: usize,
    pub(super) partition: [i32; 4],
    pub(super) integration: Integration,
    pub(super) first_correlation: Option<usize>,
    pub(super) antenna_shape: Option<(usize, usize)>,
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
    channel_bin_widths: Vec<usize>,
    antenna_shape: Option<(usize, usize)>,
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
    fn sample(&self) -> (bool, f32) {
        (self.flagged, self.sum)
    }

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
        let (time, interval) = centered_time_and_interval(
            (self.first.time, self.first.interval),
            (last.time, last.interval),
        );
        Integration { time, interval }
    }

    fn baseline_weight(&self) -> f64 {
        let weight = if self.channel_bin_widths.is_empty() {
            mean_weight_preferring_unflagged(self.weights.iter().map(ChannelWeightSum::sample))
        } else {
            let mut channels = self.weights.as_slice();
            mean_weight_preferring_unflagged(self.channel_bin_widths.iter().map(|&width| {
                let (bin, rest) = channels.split_at(width);
                channels = rest;
                plotms_columns::sum_channel_weights(
                    bin.iter().map(ChannelWeightSum::sample),
                    plotms_columns::ChannelWeightMask::Positive,
                )
            }))
        };
        plotms_columns::baseline_weight(f64::from(weight))
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
                weights: row.first_correlation.map_or_else(Vec::new, |_| {
                    row.bins
                        .iter()
                        .flat_map(ChannelBin::channels)
                        .map(|_| ChannelWeightSum::default())
                        .collect()
                }),
                channel_bin_widths: if row.first_correlation.is_some()
                    && row.bins.iter().any(|bin| bin.averaged)
                {
                    row.bins.iter().map(|bin| bin.channels().count()).collect()
                } else {
                    Vec::new()
                },
                antenna_shape: row.antenna_shape,
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
        if let Some(correlation) = row.first_correlation {
            for (channel, sample) in row
                .bins
                .iter()
                .flat_map(ChannelBin::channels)
                .zip(&mut group.weights)
            {
                let weight = row.weights.map_or(1.0, |grid| {
                    grid.values[correlation * grid.chan_count + channel] as f32
                });
                sample.observe(row.flags[(correlation, channel)], weight);
            }
        }
    }

    pub(super) fn plan(
        self,
        time_plan: &PlotTimeGroups,
        rows: &[usize],
        averaging: &MsAverageSpec,
    ) -> Result<TimeBaselinePlan, String> {
        let interval = averaging.avgtime.unwrap_or(0.0);
        let mut averages: Vec<BaselineAverage> = Vec::new();
        let mut rows_to_average = HashMap::with_capacity(self.groups.len());
        let mut antenna_shapes = VisibilityAverageShapes::default();
        for buffer in time_plan.buffers() {
            let Some(anchor) = self.groups.get(&buffer.start) else {
                continue;
            };
            let bounds = anchor.integration();
            let [observation, scan, field, window] = anchor.partition;
            let partition = [
                observation,
                if averaging.avgscan { 0 } else { scan },
                if averaging.avgfield { 0 } else { field },
                window,
            ];
            let (start, end) = (
                bounds.time - bounds.interval / 2.0,
                bounds.time + bounds.interval / 2.0,
            );
            let starts_new_average = averages.last().is_none_or(|average| {
                let elapsed = bounds.time - average.anchor_time;
                elapsed < 0.0 || elapsed > interval || average.partition != partition
            });
            if starts_new_average {
                averages.push(BaselineAverage {
                    partition,
                    scan,
                    field,
                    anchor_time: bounds.time,
                    row: rows[time_plan.first_row_slot(buffer.start)],
                    start,
                    end,
                });
            } else if let Some(average) = averages.last_mut() {
                average.start = average.start.min(start);
                average.end = average.end.max(end);
                if average.scan != scan {
                    average.scan = -1;
                }
            }
            let output = averages.len() - 1;
            if let Some((correlations, channels)) = anchor.antenna_shape {
                antenna_shapes.check_shape(
                    "vector antenna time averaging",
                    output,
                    correlations,
                    channels,
                )?;
            }
            for group in buffer.clone() {
                if let Some(statistics) = self.groups.get(&group) {
                    rows_to_average.insert(group, (output, statistics.baseline_weight()));
                }
            }
        }
        Ok(TimeBaselinePlan {
            averages,
            rows_to_average,
        })
    }
}

struct BaselineAverage {
    partition: [i32; 4],
    scan: i32,
    field: i32,
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
    pub(super) fn output_group(&self, time_group: usize) -> Option<usize> {
        self.rows_to_average
            .get(&time_group)
            .map(|&(group, _)| group)
    }

    pub(super) fn scan_and_field(&self, average: usize) -> Option<(i32, i32)> {
        self.averages
            .get(average)
            .map(|average| (average.scan, average.field))
    }

    pub(super) fn center(&self, average: usize) -> Option<f64> {
        self.averages
            .get(average)
            .map(|average| average.start + (average.end - average.start) / 2.0)
    }

    /// Each point here is a normalized complex time average for one baseline.
    /// Only project to the displayed amplitude or phase after this second mean.
    pub(super) fn combine_series(&self, series: &mut AveragedSeriesAccumulator) {
        for (mut key, mut point) in std::mem::take(&mut series.points) {
            if !point.has_usable_weight() {
                continue;
            }
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

    /// Reduce the complex means from each time-averaged baseline with its
    /// transformed spectrum. The second antenna receives the conjugate.
    pub(super) fn combine_antenna_series(&self, series: &mut AveragedSeriesAccumulator) {
        for (key, point) in std::mem::take(&mut series.points) {
            let AveragingBaselineToken::Baseline(first, second) = key.baseline else {
                continue;
            };
            let Some(group) = key
                .time_key
                .group()
                .and_then(|group| self.output_group(group))
            else {
                continue;
            };
            let Some((y, channel_weight)) = point.y_visibility.transformed_vector_sample() else {
                continue;
            };
            let x = point
                .x_visibility
                .transformed_vector_sample()
                .map(|(value, _)| value);
            let weight = channel_weight
                .map(f64::from)
                .or(point.combined_primary_weight);
            let weight = plotms_columns::baseline_weight(weight.unwrap_or(1.0)) as f32;
            for (antenna, x, y) in [
                (first, x, y),
                (second, x.map(|value| value.conj()), y.conj()),
            ] {
                let key = AveragingPointKey {
                    time_key: AveragingTimeKey::Group(group),
                    baseline: AveragingBaselineToken::Antenna(antenna),
                    ..key.clone()
                };
                let output = series.points.entry(key).or_default();
                if output.representative.is_none() {
                    output.representative = point.representative.clone();
                    output.display_channel = point.display_channel;
                    output.numeric_axis_samples = point.numeric_axis_samples.clone();
                }
                if let Some(x) = x {
                    output.x_visibility.add_normalized_vector_sample(x, weight);
                }
                output.y_visibility.add_normalized_vector_sample(y, weight);
            }
        }
    }
}
