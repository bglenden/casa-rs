// SPDX-License-Identifier: LGPL-3.0-or-later
//! How far a calibration solve's intervals got, as CASA's `gaincal` and
//! `bandpass` return it in `solvestats` (#931).
//!
//! CASA 6.7.6.14 counts solution intervals, not solutions or channels, at
//! four stages, one count per solved polarization: the intervals there are
//! (`expected`), those with unflagged data (`data_unflagged`), those that
//! keep `minblperant` baselines (`above_minblperant`) and those whose
//! solution passes `minsnr` (`above_minsnr`). It keeps the counts for each
//! spectral window, for each antenna in it, and summed over the windows,
//! and counts each antenna's intervals as the reference antenna
//! (`used_as_refant`). What CASA does, established by running it:
//!
//! - every window and antenna of the MeasurementSet has counts, zero where
//!   the selection does not reach it;
//! - an antenna's `expected` intervals are those its selected
//!   cross-correlation rows reach, flagged or not; a window's are all its
//!   intervals;
//! - an antenna has unflagged data in a polarization when a baseline of it
//!   has positive unflagged weight there, after a flag on any correlation
//!   has flagged the sample for all of them;
//! - a window's `above_minsnr` counts the intervals that pass
//!   `minblperant`, whatever their solutions' SNR;
//! - `above_minsnr` counts the solutions `minsnr` left, before the reference
//!   antenna is chosen;
//! - an antenna is used as the reference antenna in an interval whose gains
//!   are finally referred to it, with the reference antenna `flex` chooses
//!   where the preferred one is flagged, and that kept its solution.

use std::collections::{BTreeMap, BTreeSet};

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::SelectedData;

use crate::solve::grouping::SolveAccumulator;
use crate::solve::kernel::baseline_constrained_antennas;

/// One stage's interval counts, one per solved polarization: two for G and
/// B, one for T.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SolveCounts {
    /// Solution intervals.
    pub expected: Vec<u64>,
    /// Intervals with unflagged data.
    pub data_unflagged: Vec<u64>,
    /// Intervals that keep `minblperant` baselines per antenna.
    pub above_minblperant: Vec<u64>,
    /// Intervals whose solution passes `minsnr`.
    pub above_minsnr: Vec<u64>,
}

impl SolveCounts {
    fn zero(polarizations: usize) -> Self {
        Self {
            expected: vec![0; polarizations],
            data_unflagged: vec![0; polarizations],
            above_minblperant: vec![0; polarizations],
            above_minsnr: vec![0; polarizations],
        }
    }

    fn add(&mut self, other: &Self) {
        for (sum, value) in [
            (&mut self.expected, &other.expected),
            (&mut self.data_unflagged, &other.data_unflagged),
            (&mut self.above_minblperant, &other.above_minblperant),
            (&mut self.above_minsnr, &other.above_minsnr),
        ] {
            for (sum, value) in sum.iter_mut().zip(value) {
                *sum += value;
            }
        }
    }
}

/// One antenna's counts in one spectral window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AntennaSolveStats {
    /// The antenna's id.
    pub antenna_id: i32,
    /// Its intervals at each stage.
    pub counts: SolveCounts,
    /// The intervals it was the reference antenna of.
    pub used_as_refant: u64,
}

/// One spectral window's counts, and its antennas'.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WindowSolveStats {
    /// The window's id.
    pub spw_id: i32,
    /// Its intervals at each stage.
    pub counts: SolveCounts,
    /// Every antenna of the MeasurementSet, by id.
    pub antennas: Vec<AntennaSolveStats>,
}

/// A solve's interval counts: CASA's `solvestats`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SolveStats {
    /// Every spectral window of the MeasurementSet, by id.
    pub windows: Vec<WindowSolveStats>,
}

/// Interval-level counts do not depend on the number of correlations.
#[derive(Debug, Clone, Copy, Default)]
struct IntervalActivity {
    expected: i64,
    attempted: i64,
    succeeded: i64,
}

/// The direct calibrater's per-window solve activity.
#[derive(Default)]
pub struct ToolSolveActivity {
    windows: Vec<IntervalActivity>,
}

impl ToolSolveActivity {
    pub(crate) fn initialize(&mut self, windows: usize) {
        self.windows.resize(windows, IntervalActivity::default());
    }

    pub(crate) fn count_mapped_interval(
        &mut self,
        input_window: i32,
        output_window: i32,
        attempted: bool,
        succeeded: bool,
    ) {
        self.windows[input_window as usize].expected += 1;
        let output = &mut self.windows[output_window as usize];
        output.attempted += i64::from(attempted);
        output.succeeded += i64::from(succeeded);
    }

    pub(crate) fn record(&self) -> Option<RecordValue> {
        if self.windows.is_empty() {
            return None;
        }
        let vector = |field: fn(IntervalActivity) -> i64| {
            Value::Array(ArrayValue::from_i64_vec(
                self.windows.iter().copied().map(field).collect(),
            ))
        };
        Some(RecordValue::new(vec![
            RecordField::new("nAttempt", vector(|activity| activity.attempted)),
            RecordField::new("nExpected", vector(|activity| activity.expected)),
            RecordField::new("nSucceed", vector(|activity| activity.succeeded)),
            RecordField::new(
                "origin",
                Value::Scalar(ScalarValue::String(
                    "Calibrater::genericGatherAndSolve".to_string(),
                )),
            ),
        ]))
    }
}

pub(crate) fn selectvis_record_value(selected: Option<&SelectedData>) -> RecordValue {
    let ids = |ids: &[i32]| {
        Value::Array(ArrayValue::from_i64_vec(
            ids.iter().map(|&id| i64::from(id)).collect(),
        ))
    };
    selected.map_or_else(
        || RecordValue::new(Vec::new()),
        |selected| {
            RecordValue::new(vec![
                RecordField::new("antennas", ids(&selected.antennas)),
                RecordField::new("field", ids(&selected.fields)),
                RecordField::new(
                    "intents",
                    Value::Array(ArrayValue::from_string_vec(selected.intents.clone())),
                ),
                RecordField::new("observation", ids(&selected.observations)),
                RecordField::new("scan", ids(&selected.scans)),
                RecordField::new("spw", ids(&selected.spectral_windows)),
            ])
        },
    )
}

pub(crate) fn solvestats_record(stats: &SolveStats) -> RecordValue {
    let counts = |c: &SolveCounts| -> Vec<(String, Value)> {
        let vec = |values: &[u64]| {
            Value::Array(ArrayValue::from_i64_vec(
                values.iter().map(|&v| v as i64).collect(),
            ))
        };
        vec![
            ("above_minblperant".to_string(), vec(&c.above_minblperant)),
            ("above_minsnr".to_string(), vec(&c.above_minsnr)),
            ("data_unflagged".to_string(), vec(&c.data_unflagged)),
            ("expected".to_string(), vec(&c.expected)),
        ]
    };
    let sorted = |mut entries: Vec<(String, Value)>| {
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        RecordValue::new(
            entries
                .into_iter()
                .map(|(k, v)| RecordField::new(k, v))
                .collect(),
        )
    };
    let windows = stats.windows.iter().map(|window| {
        let antennas = window.antennas.iter().map(|antenna| {
            let mut entries = counts(&antenna.counts);
            entries.push((
                "used_as_refant".to_string(),
                Value::Array(ArrayValue::from_i64_vec(vec![
                    antenna.used_as_refant as i64,
                ])),
            ));
            (
                format!("ant{}", antenna.antenna_id),
                Value::Record(sorted(entries)),
            )
        });
        let mut entries = counts(&window.counts);
        entries.extend(antennas);
        (
            format!("spw{}", window.spw_id),
            Value::Record(sorted(entries)),
        )
    });
    let mut entries = counts(&stats.total().unwrap_or_default());
    entries.extend(windows);
    sorted(entries)
}

impl SolveStats {
    /// The counts summed over the windows, CASA's top-level counts; `None`
    /// for a solve with no windows.
    pub fn total(&self) -> Option<SolveCounts> {
        let first = self.windows.first()?;
        let mut total = SolveCounts::zero(first.counts.expected.len());
        for window in &self.windows {
            total.add(&window.counts);
        }
        Some(total)
    }
}

/// One interval, as its solve's stages saw it.
pub(crate) struct IntervalStages<'a> {
    pub(crate) spw_id: i32,
    /// The antennas its selected cross-correlation rows reach.
    pub(crate) antennas: &'a BTreeSet<i32>,
    /// The interval contains selected cross-correlations.
    pub(crate) expected_window: bool,
    /// Whether the initial gather had positive weight before dropping cross hands.
    pub(crate) attempted_interval: Option<bool>,
    /// For each polarization, the antennas with unflagged data.
    pub(crate) with_data: Vec<BTreeSet<i32>>,
    /// The antennas that keep `minblperant` baselines.
    pub(crate) constrained: BTreeSet<i32>,
}

impl<'a> IntervalStages<'a> {
    /// The stages of `group`, finalized for its solve, under
    /// `min_baselines_per_antenna`. A channelized group passes a stage
    /// where any of its channels does.
    pub(crate) fn of(group: &'a SolveAccumulator, min_baselines_per_antenna: usize) -> Self {
        let polarizations = group.channel_graphs.first().map_or(0, Vec::len);
        let (with_data, attempted_interval) =
            if let Some(([mut first, second], attempted)) = group.direct_gain_data() {
                let with_data = if polarizations == 1 {
                    first.extend(second);
                    vec![first]
                } else {
                    vec![first, second]
                };
                (with_data, Some(attempted))
            } else {
                let with_data = (0..polarizations)
                    .map(|polarization| {
                        group
                            .channel_graphs
                            .iter()
                            .flat_map(|graphs| graphs[polarization].stats.keys())
                            .filter_map(|&(first, second)| (first != second).then_some(first))
                            .collect()
                    })
                    .collect();
                (with_data, None)
            };
        let constrained = group
            .channel_graphs
            .iter()
            .flat_map(|graphs| baseline_constrained_antennas(graphs, min_baselines_per_antenna))
            .collect();
        Self {
            spw_id: group.spw_id,
            antennas: &group.antenna_ids,
            expected_window: group.expects_cross_correlation(),
            attempted_interval,
            with_data,
            constrained,
        }
    }
}

/// One antenna's solution in an interval: for each polarization, whether
/// it passed `minsnr`.
pub(crate) struct AntennaSolution {
    pub(crate) antenna_id: i32,
    pub(crate) passed: Vec<bool>,
}

/// An antenna's counts and its intervals as the reference antenna.
type AntennaTally = (SolveCounts, u64);

/// A window's counts and its antennas', by id.
type WindowTally = (SolveCounts, BTreeMap<i32, AntennaTally>, IntervalActivity);

/// Accumulates [`SolveStats`] interval by interval.
pub(crate) struct SolveStatsBuilder {
    polarizations: usize,
    antenna_ids: Vec<i32>,
    windows: BTreeMap<i32, WindowTally>,
}

impl SolveStatsBuilder {
    /// Zero counts of `polarizations` for every window in `spw_ids` and every
    /// antenna in `antenna_ids`.
    pub(crate) fn new(
        polarizations: usize,
        spw_ids: impl IntoIterator<Item = i32>,
        antenna_ids: &BTreeSet<i32>,
    ) -> Self {
        let antennas = antenna_ids
            .iter()
            .map(|&antenna_id| (antenna_id, (SolveCounts::zero(polarizations), 0)))
            .collect::<BTreeMap<_, _>>();
        Self {
            polarizations,
            antenna_ids: antenna_ids.iter().copied().collect(),
            windows: spw_ids
                .into_iter()
                .map(|spw_id| {
                    (
                        spw_id,
                        (
                            SolveCounts::zero(polarizations),
                            antennas.clone(),
                            IntervalActivity::default(),
                        ),
                    )
                })
                .collect(),
        }
    }

    /// Count one interval before its solution: its data and its constraint.
    pub(crate) fn interval(&mut self, stages: &IntervalStages<'_>) {
        let polarizations = self.polarizations;
        let blank = || (SolveCounts::zero(polarizations), 0);
        let (window, antennas, activity) = self.windows.entry(stages.spw_id).or_insert_with(|| {
            (
                SolveCounts::zero(polarizations),
                self.antenna_ids.iter().map(|&id| (id, blank())).collect(),
                IntervalActivity::default(),
            )
        });
        activity.expected += 1;
        activity.attempted += i64::from(
            stages
                .attempted_interval
                .unwrap_or_else(|| stages.with_data.iter().any(|ants| !ants.is_empty())),
        );
        let passed = !stages.constrained.is_empty();
        for polarization in 0..polarizations {
            let with_data = stages.with_data.get(polarization);
            window.expected[polarization] += u64::from(stages.expected_window);
            window.data_unflagged[polarization] +=
                u64::from(with_data.is_some_and(|antennas| !antennas.is_empty()));
            window.above_minblperant[polarization] += u64::from(passed);
            window.above_minsnr[polarization] += u64::from(passed);
            for &antenna_id in stages.antennas {
                let (counts, _) = antennas.entry(antenna_id).or_insert_with(blank);
                counts.expected[polarization] += 1;
                counts.data_unflagged[polarization] +=
                    u64::from(with_data.is_some_and(|antennas| antennas.contains(&antenna_id)));
                counts.above_minblperant[polarization] +=
                    u64::from(stages.constrained.contains(&antenna_id));
            }
            if let Some(with_data) = with_data {
                for &antenna_id in with_data.difference(stages.antennas) {
                    let (counts, _) = antennas.entry(antenna_id).or_insert_with(blank);
                    counts.data_unflagged[polarization] += 1;
                }
            }
        }
    }

    /// A solver kept this interval, independently of its final SNR flags.
    pub(crate) fn record_success(&mut self, spw_id: i32, succeeded: bool) {
        if let Some((_, _, activity)) = self.windows.get_mut(&spw_id) {
            activity.succeeded += i64::from(succeeded);
        }
    }

    /// Count one interval's solutions as `minsnr` left them.
    pub(crate) fn solutions(
        &mut self,
        spw_id: i32,
        solutions: impl IntoIterator<Item = AntennaSolution>,
    ) {
        let polarizations = self.polarizations;
        let Some((_, antennas, _)) = self.windows.get_mut(&spw_id) else {
            return;
        };
        for solution in solutions {
            let (counts, _) = antennas
                .entry(solution.antenna_id)
                .or_insert_with(|| (SolveCounts::zero(polarizations), 0));
            for (count, passed) in counts.above_minsnr.iter_mut().zip(&solution.passed) {
                *count += u64::from(*passed);
            }
        }
    }

    /// Count an interval of `spw_id` whose gains are referred to
    /// `antenna_id`, a solution of which the interval kept.
    pub(crate) fn used_as_refant(&mut self, spw_id: i32, antenna_id: i32) {
        let polarizations = self.polarizations;
        if let Some((_, antennas, _)) = self.windows.get_mut(&spw_id) {
            antennas
                .entry(antenna_id)
                .or_insert_with(|| (SolveCounts::zero(polarizations), 0))
                .1 += 1;
        }
    }

    /// Transfer the additional interval totals requested by the direct tool.
    pub(crate) fn track_activity(&self, target: Option<&mut ToolSolveActivity>) {
        if let Some(target) = target {
            target.windows = self
                .windows
                .values()
                .map(|(_, _, counts)| *counts)
                .collect();
        }
    }

    /// The counts.
    pub(crate) fn finish(self) -> SolveStats {
        SolveStats {
            windows: self
                .windows
                .into_iter()
                .map(|(spw_id, (counts, antennas, _))| WindowSolveStats {
                    spw_id,
                    counts,
                    antennas: antennas
                        .into_iter()
                        .map(|(antenna_id, (counts, used_as_refant))| AntennaSolveStats {
                            antenna_id,
                            counts,
                            used_as_refant,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(ids: &[i32]) -> BTreeSet<i32> {
        ids.iter().copied().collect()
    }

    #[test]
    fn intervals_count_at_each_stage_per_window_and_antenna() {
        let mut stats = SolveStatsBuilder::new(2, [0, 1], &ids(&[0, 1, 2, 3]));
        let reached = ids(&[0, 1, 2]);
        // A solved interval: antenna 2 has data only in the first
        // polarization and too few baselines; antenna 1 fails minsnr.
        stats.interval(&IntervalStages {
            spw_id: 0,
            expected_window: true,
            attempted_interval: None,
            antennas: &reached,
            with_data: vec![ids(&[0, 1, 2]), ids(&[0, 1])],
            constrained: ids(&[0, 1]),
        });
        stats.used_as_refant(0, 0);
        stats.solutions(
            0,
            [
                AntennaSolution {
                    antenna_id: 0,
                    passed: vec![true, true],
                },
                AntennaSolution {
                    antenna_id: 1,
                    passed: vec![false, true],
                },
            ],
        );
        // An interval with no unflagged data.
        stats.interval(&IntervalStages {
            spw_id: 0,
            expected_window: true,
            attempted_interval: None,
            antennas: &reached,
            with_data: vec![BTreeSet::new(), BTreeSet::new()],
            constrained: BTreeSet::new(),
        });
        let stats = stats.finish();

        let window = &stats.windows[0];
        assert_eq!(
            window.counts,
            SolveCounts {
                expected: vec![2, 2],
                data_unflagged: vec![1, 1],
                above_minblperant: vec![1, 1],
                above_minsnr: vec![1, 1],
            }
        );
        let antenna = |id: usize| &window.antennas[id];
        assert_eq!(antenna(0).counts.above_minsnr, [1, 1]);
        assert_eq!(antenna(0).used_as_refant, 1);
        assert_eq!(antenna(1).counts.above_minsnr, [0, 1]);
        assert_eq!(antenna(2).counts.data_unflagged, [1, 0]);
        assert_eq!(antenna(2).counts.above_minblperant, [0, 0]);
        // Antenna 3 and window 1 are not reached: zero, but present.
        assert_eq!(antenna(3).counts, SolveCounts::zero(2));
        assert_eq!(stats.windows[1].counts, SolveCounts::zero(2));
        assert_eq!(stats.windows[1].antennas.len(), 4);
        assert_eq!(stats.total().unwrap().expected, [2, 2]);
    }
}
