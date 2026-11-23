// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline additive offsets and CASA's cached Mueller matrix direction.

use std::sync::Mutex;

use super::*;
use crate::lookup::TimeQuery;

pub(super) const SUBTYPE: &str = "A Mueller";

pub(super) fn in_plan(plan: &ApplyPlan) -> bool {
    plan.calibration_tables
        .iter()
        .any(|table| table.summary.table_subtype == SUBTYPE)
}

/// The attached tool retains each term's last matrix independently per MS SPW.
#[derive(Clone, Default)]
pub(crate) struct ToolState(Arc<Mutex<HashMap<(usize, i32), CachedMatrix>>>);

type LocalMatrices = HashMap<(usize, i32), CachedMatrix>;

#[derive(Clone, Copy)]
pub(super) struct CachedMatrix {
    context: Context,
    forward: bool,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) struct Context {
    field: i32,
    window: i32,
    observation: i32,
    scan: i32,
    time: f64,
}

impl Context {
    pub(super) fn new(
        table: &LoadedCalibrationTable,
        row: &ApplyRowPlan,
        field: i32,
        window: i32,
    ) -> Self {
        let observation = row.calibration_observation_id;
        let scan = row.scan_number;
        let (observation, scan) = table
            .callibrary
            .as_ref()
            .map_or((observation, scan), |lib| {
                (lib.observation_map.map(observation), lib.scan_map.map(scan))
            });
        Self {
            field,
            window,
            observation,
            scan,
            time: row.time_seconds,
        }
    }

    fn query(self) -> TimeQuery {
        TimeQuery {
            observation: self.observation,
            scan: self.scan,
            window: self.window,
            antenna: 0,
            time: self.time,
            frequency_hz: None,
        }
    }
}

impl ToolState {
    pub(super) fn direction(
        &self,
        key: (usize, i32),
        context: Context,
        table: &LoadedCalibrationTable,
        requested_forward: bool,
        local: Option<&mut LocalMatrices>,
    ) -> bool {
        if let Some(matrix) = local.as_ref().and_then(|matrices| matrices.get(&key))
            && matrix.context == context
        {
            return matrix.forward;
        }
        let mut matrices = self.0.lock().expect("calibrater additive cache");
        let forward = matrices.get(&key).map_or(requested_forward, |previous| {
            if previous.forward == requested_forward
                || previous.context.field != context.field
                || !table.time_plans.get(&context.field).is_some_and(|plan| {
                    plan.same_window_solution(previous.context.query(), context.query())
                })
            {
                requested_forward
            } else {
                previous.forward
            }
        });
        let matrix = CachedMatrix { context, forward };
        matrices.insert(key, matrix);
        if let Some(local) = local {
            local.insert(key, matrix);
        }
        forward
    }
}

pub(super) fn apply(
    grid: Option<&CalibrationGrid>,
    path: &Path,
    reversed: bool,
    add: bool,
    replace_model: bool,
    output: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    let grid = grid.map(|grid| match grid {
        CalibrationGrid::Complex(grid) => grid,
        _ => unreachable!("baseline grid was prepared as CPARAM"),
    });
    let correlations = baseline::correlation_count(output)?;
    for correlation in 0..correlations {
        for channel in 0..output.corrected.shape()[1] {
            let data = &mut output.corrected[[correlation, channel]];
            if correlations == 4 && matches!(correlation, 1 | 2) {
                *data = Complex32::ZERO;
                continue;
            }
            let sample = match grid {
                Some(grid) => grid.sample(
                    correlation % 2,
                    channel,
                    output.data_channels,
                    &[],
                    ApplyInterpolationMode::Nearest,
                    path,
                )?,
                None => GainSample {
                    value: Complex32::ZERO,
                    flagged: true,
                },
            };
            if replace_model {
                *data = Complex32::ZERO;
            }
            if sample.flagged {
                if output.propagate_flags && !output.flags[[correlation, channel]] {
                    output.flags[[correlation, channel]] = true;
                    *output.newly_flagged_samples += 1;
                }
                continue;
            }
            let offset = if reversed {
                sample.value.conj()
            } else {
                sample.value
            };
            *data += if add { offset } else { -offset };
        }
    }
    Ok(())
}
