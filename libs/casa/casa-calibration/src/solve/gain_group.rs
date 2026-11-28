// SPDX-License-Identifier: LGPL-3.0-or-later
//! Solve and report one closed gain interval.

use std::collections::BTreeSet;

use super::control::SolveControl;
use super::kernel::solve_group;
use super::{GainSolveError, GainSolveRequest, grouping, kernel};
use crate::solve_stats::{AntennaSolution, IntervalStages, SolveStatsBuilder};

#[cfg(test)]
pub(crate) fn solve_closed_group(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
    correlation_dependent_flags: bool,
) -> Option<Vec<kernel::SolutionRow>> {
    solve_closed_group_with_control(
        group,
        available_antennas,
        request,
        refant_id,
        correlation_dependent_flags,
        SolveControl::default(),
    )
}

fn solve_closed_group_with_control(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
    correlation_dependent_flags: bool,
    control: SolveControl<'_>,
) -> Option<Vec<kernel::SolutionRow>> {
    solve_group(
        group,
        available_antennas,
        request.gain_type,
        request.solve_mode,
        request.min_snr,
        kernel::SolveGraphOptions {
            refant_id,
            min_baselines_per_antenna: request.min_baselines_per_antenna,
            group_seed: None,
            correlation_dependent_flags,
            defer_reference: true,
            robustness: request.robustness.enabled().then_some(&request.robustness),
            control,
        },
    )
}

pub(super) fn solve_group_with_stats(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
    correlation_dependent_flags: bool,
    stats: &mut SolveStatsBuilder,
    control: SolveControl<'_>,
) -> Result<Option<Vec<kernel::SolutionRow>>, GainSolveError> {
    control.check()?;
    stats.interval(&IntervalStages::of(
        &group,
        request.min_baselines_per_antenna,
    ));
    let spw_id = group.spw_id;
    let rows = solve_closed_group_with_control(
        group,
        available_antennas,
        request,
        refant_id,
        correlation_dependent_flags,
        control,
    );
    control.check()?;
    let Some(rows) = rows else { return Ok(None) };
    stats.record_success(spw_id, true);
    stats.solutions(
        spw_id,
        rows.iter().map(|row| AntennaSolution {
            antenna_id: row.antenna_id,
            passed: row.flags.iter().map(|flagged| !flagged).collect(),
        }),
    );
    Ok(Some(rows))
}

#[cfg(test)]
pub(crate) fn solve_gain(
    ms: &casa_ms::MeasurementSet,
    request: &GainSolveRequest,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut crate::solve_stats::ToolSolveActivity>,
    legacy_iterator: bool,
) -> Result<super::GainSolveReport, GainSolveError> {
    super::solve_gain_with_control(
        ms,
        request,
        mode,
        activity,
        legacy_iterator,
        SolveControl::default(),
    )
}
