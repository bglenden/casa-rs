// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum AveragingBaselineToken {
    Baseline(i32, i32),
    Antenna(i32),
    Averaged,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct TimeAverageScopeKey {
    pub(super) field_id: Option<i32>,
    pub(super) scan_number: Option<i32>,
    pub(super) spw_id: Option<i32>,
    pub(super) baseline: AveragingBaselineToken,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SharedTimeAverageScopeKey {
    pub(super) field_id: Option<i32>,
    pub(super) scan_number: Option<i32>,
    pub(super) spw_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum AveragingTimeKey {
    Exact(u64),
    Bucket(i64),
    Group(usize),
}

impl AveragingTimeKey {
    pub(super) fn group(self) -> Option<usize> {
        match self {
            Self::Group(group) => Some(group),
            Self::Exact(_) | Self::Bucket(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum AveragingSampleKey {
    SourceChannel {
        correlation: usize,
        start: usize,
        end: usize,
        ordinal: usize,
    },
    SelectedChannel {
        ordinal: usize,
        correlation: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct AveragingPointKey {
    pub(super) time_key: AveragingTimeKey,
    pub(super) field_id: Option<i32>,
    pub(super) scan_number: Option<i32>,
    pub(super) spw_id: Option<i32>,
    pub(super) baseline: AveragingBaselineToken,
    pub(super) sample: AveragingSampleKey,
}

pub(super) struct AveragingPointKeyInputs<'a> {
    pub(super) field_id: i32,
    pub(super) scan_number: i32,
    pub(super) spw_id: i32,
    pub(super) baseline: AveragingBaselineToken,
    pub(super) corr_index: usize,
    pub(super) row_time_value: f64,
    pub(super) group: Option<usize>,
    pub(super) bin: &'a ChannelBin,
    pub(super) pair_selected_channels: bool,
}

#[derive(Debug, Default)]
pub(super) struct AveragedPointAccumulator {
    pub(super) x_visibility: plotms_ordinates::AveragedVisibility,
    pub(super) y_visibility: plotms_ordinates::AveragedVisibility,
    pub(super) numeric_axis_samples: Vec<(MsAxis, Vec<f64>)>,
    pub(super) time_interval_samples: Vec<f64>,
    pub(super) has_flag_samples: bool,
    pub(super) any_flag_sample: bool,
    pub(super) has_flag_row_samples: bool,
    pub(super) any_flag_row: bool,
    pub(super) representative: Option<MsScatterPointRef>,
    pub(super) display_channel: usize,
    pub(super) combined_primary_weight: Option<f64>,
    pub(super) visibility_flagged: Option<bool>,
}

impl AveragedPointAccumulator {
    pub(super) fn has_usable_weight(&self) -> bool {
        self.combined_primary_weight
            .is_none_or(|weight| weight > 0.0)
    }

    pub(super) fn accept_visibility_sample(&mut self, flagged: bool) -> bool {
        // CASA keeps flagged visibility samples only until a clear sample arrives.
        match (self.visibility_flagged, flagged) {
            (Some(false), true) => return false,
            (Some(true), false) => {
                self.x_visibility = Default::default();
                self.y_visibility = Default::default();
            }
            _ => {}
        }
        self.visibility_flagged = Some(flagged);
        true
    }
}

#[derive(Debug)]
pub(super) struct AveragedSeriesAccumulator {
    pub(super) label: String,
    pub(super) color_group: String,
    pub(super) y_axis: MsAxis,
    pub(super) y_index: usize,
    pub(super) points: std::collections::BTreeMap<AveragingPointKey, AveragedPointAccumulator>,
}

impl Default for AveragedSeriesAccumulator {
    fn default() -> Self {
        Self {
            label: String::new(),
            color_group: String::new(),
            y_axis: MsAxis::Amplitude,
            y_index: 0,
            points: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AveragedPanelAccumulator {
    pub(super) label: String,
    pub(super) iteration_rank: i32,
    pub(super) series: std::collections::BTreeMap<String, AveragedSeriesAccumulator>,
    pub(super) contributing_rows: std::collections::BTreeSet<usize>,
}

pub(super) fn uses_extended_averaging(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgtime.is_some()
        || spec.averaging.avgscan
        || spec.averaging.avgfield
        || spec.averaging.avgbaseline
        || spec.averaging.avgantenna
        || spec.averaging.avgspw
}

pub(super) fn averaging_baseline_tokens(
    antenna1: i32,
    antenna2: i32,
    spec: &MsPlotSpec,
) -> Vec<AveragingBaselineToken> {
    if spec.averaging.avgantenna {
        vec![
            AveragingBaselineToken::Antenna(antenna1),
            AveragingBaselineToken::Antenna(antenna2),
        ]
    } else if spec.averaging.avgbaseline {
        vec![AveragingBaselineToken::Averaged]
    } else {
        vec![AveragingBaselineToken::Baseline(antenna1, antenna2)]
    }
}

pub(super) fn build_time_average_scope_key(
    field_id: i32,
    scan_number: i32,
    spw_id: i32,
    baseline: AveragingBaselineToken,
    spec: &MsPlotSpec,
) -> TimeAverageScopeKey {
    TimeAverageScopeKey {
        field_id: (!spec.averaging.avgfield).then_some(field_id),
        scan_number: (!spec.averaging.avgscan).then_some(scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(spw_id),
        baseline,
    }
}

pub(super) fn build_shared_time_average_scope_key(
    field_id: i32,
    scan_number: i32,
    spw_id: i32,
    spec: &MsPlotSpec,
) -> SharedTimeAverageScopeKey {
    SharedTimeAverageScopeKey {
        field_id: (!spec.averaging.avgfield).then_some(field_id),
        scan_number: (!spec.averaging.avgscan).then_some(scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(spw_id),
    }
}

pub(super) fn build_averaging_point_key(
    inputs: AveragingPointKeyInputs<'_>,
    spec: &MsPlotSpec,
    time_scope_origins: &std::collections::BTreeMap<TimeAverageScopeKey, f64>,
) -> AveragingPointKey {
    let time_key = if let Some(group) = inputs.group {
        AveragingTimeKey::Group(group)
    } else if let Some(avgtime) = spec.averaging.avgtime {
        let scope = build_time_average_scope_key(
            inputs.field_id,
            inputs.scan_number,
            inputs.spw_id,
            inputs.baseline.clone(),
            spec,
        );
        let origin = time_scope_origins
            .get(&scope)
            .copied()
            .unwrap_or(inputs.row_time_value);
        let bucket = ((inputs.row_time_value - origin + AVG_TIME_BUCKET_EPSILON_SECONDS) / avgtime)
            .floor() as i64;
        AveragingTimeKey::Bucket(bucket)
    } else {
        AveragingTimeKey::Exact(inputs.row_time_value.to_bits())
    };
    AveragingPointKey {
        time_key,
        field_id: (!spec.averaging.avgfield).then_some(inputs.field_id),
        scan_number: (!spec.averaging.avgscan).then_some(inputs.scan_number),
        spw_id: (!spec.averaging.avgspw).then_some(inputs.spw_id),
        baseline: inputs.baseline,
        sample: if inputs.pair_selected_channels {
            AveragingSampleKey::SelectedChannel {
                ordinal: inputs.bin.ordinal,
                correlation: inputs.corr_index,
            }
        } else {
            AveragingSampleKey::SourceChannel {
                correlation: inputs.corr_index,
                start: inputs.bin.start,
                end: inputs.bin.end,
                ordinal: inputs.bin.ordinal,
            }
        },
    }
}

pub(super) fn push_numeric_axis_sample(
    accumulator: &mut AveragedPointAccumulator,
    axis: MsAxis,
    value: f64,
) {
    if let Some((_, samples)) = accumulator
        .numeric_axis_samples
        .iter_mut()
        .find(|(candidate, _)| *candidate == axis)
    {
        samples.push(value);
    } else {
        accumulator.numeric_axis_samples.push((axis, vec![value]));
    }
}

pub(super) fn push_time_interval_sample(
    accumulator: &mut AveragedPointAccumulator,
    interval_seconds: f64,
) {
    accumulator.time_interval_samples.push(interval_seconds);
}

pub(super) fn record_scalar_interval(
    bounds: &mut HashMap<usize, (f64, f64)>,
    group: usize,
    time: f64,
    interval: f64,
) {
    let start = time - interval / 2.0;
    let end = time + interval / 2.0;
    bounds
        .entry(group)
        .and_modify(|(first, last)| {
            *first = first.min(start);
            *last = last.max(end);
        })
        .or_insert((start, end));
}
