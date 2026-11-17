// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traditional setapply selects calibration rows, then merges fields in time.

use super::*;
use crate::lookup::{TimePlan, TimeQuery};

pub(super) fn load(
    plan: &ApplyCalibrationTablePlan,
    engine: Option<&Arc<MsCalEngine>>,
    ms: &MeasurementSet,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    let mut table = load_calibration_table(plan, engine)?;
    if !plan.resolved_gainfields.is_empty() {
        table.solutions.retain(|(field, _, _), _| {
            plan.resolved_gainfields
                .iter()
                .any(|selected| selected.field_id == *field)
        });
    }
    // One flagged template per window handles missing antenna/context slices
    // without copying a full parameter grid for each baseline.
    for (&(_, window, _), rows) in &table.solutions {
        if let std::collections::hash_map::Entry::Vacant(entry) = table.missing_grids.entry(window)
            && let Some(row) = rows.first()
        {
            entry.insert(flagged(&row.grid));
        }
    }
    let nearest_field = matches!(plan.spec.gainfield, Some(crate::GainFieldSelector::Nearest));
    let sample = table
        .solutions
        .values()
        .flatten()
        .next()
        .map(|row| row.grid.as_ref());
    let curve = matches!(
        sample,
        Some(CalibrationGrid::Complex(_) | CalibrationGrid::Delay(_))
    );
    if curve {
        table.time_is_delay = matches!(sample, Some(CalibrationGrid::Delay(_)));
        let observation_count = ms
            .observation()
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: display_ms_path(ms),
                source,
            })?
            .row_count();
        let mut options = plan.interp.time_options(observation_count);
        // CLPatchPanel partitions through explicit maps, not these traditional
        // interpolation-string modifiers. Library maps are handled separately.
        if plan.spec.callibrary.is_some() {
            options.per_observation = false;
            options.per_scan = false;
        }
        // CASA's legacy BPOLY conversion emits unpartitioned modern headers.
        if plan.summary.table_subtype == "BPOLY" {
            options.per_scan = false;
            options.per_observation = false;
            options.phase_delay = false;
        }
        // CASA dispatches scalar PD by Jones family, even for a one-channel B table.
        options.phase_delay &= matches!(plan.summary.table_subtype.as_str(), "G Jones" | "T Jones");
        table.phase_delay = options.phase_delay;
        let context = TimePlan::apply_context(&table, options)?;
        let mut groups = HashMap::<_, HashMap<_, _>>::new();
        for (key, mut rows) in std::mem::take(&mut table.solutions) {
            if table.time_is_delay {
                for row in &mut rows {
                    let CalibrationGrid::Delay(delay) = row.grid.as_ref() else {
                        unreachable!("one parameter family")
                    };
                    row.grid = Arc::new(CalibrationGrid::Complex(GainGrid {
                        receptor_count: delay.receptor_count,
                        channel_count: delay.channel_count,
                        values: delay.values_ns.mapv(|value| Complex32::new(value, 0.)),
                        flags: delay.flags.clone(),
                    }));
                }
            }
            groups
                .entry(if nearest_field { key.0 } else { -1 })
                .or_default()
                .insert(key, rows);
        }
        for (field, solutions) in groups {
            let selected = LoadedCalibrationTable {
                path: table.path.clone(),
                interp: table.interp,
                supports_calwt: false,
                engine: None,
                solutions,
                time_plans: HashMap::new(),
                missing_grids: HashMap::new(),
                time_is_delay: false,
                phase_delay: false,
            };
            table.time_plans.insert(
                field,
                TimePlan::for_apply(&selected, options, table.time_is_delay, &context)?,
            );
        }
    } else if !nearest_field {
        table.solutions = crate::lookup::across_fields(&table)
            .into_iter()
            .map(|((window, antenna), series)| ((-1, window, antenna), series))
            .collect();
    }
    Ok(table)
}

pub(super) fn lookup(
    table: &LoadedCalibrationTable,
    field: i32,
    window: i32,
    antenna: i32,
    row: &ApplyRowPlan,
    data_window: &crate::plan::SpectralWindowPlan,
) -> Result<Option<Arc<CalibrationGrid>>, ApplyExecutionError> {
    let Some(plan) = table.time_plans.get(&field) else {
        if !table.time_plans.is_empty() {
            return Ok(table.missing_grids.get(&window).cloned());
        }
        return table
            .lookup(field, window, antenna, row.time_seconds, table.interp)
            .map(|grid| grid.or_else(|| table.missing_grids.get(&window).cloned()));
    };
    let frequencies = &data_window.channel_frequencies_hz;
    let frequency_hz = table
        .phase_delay
        .then(|| frequencies.iter().sum::<f64>() / frequencies.len() as f64);
    let query = TimeQuery {
        observation: row.observation_id,
        scan: row.scan_number,
        window,
        antenna,
        time: row.time_seconds,
        frequency_hz,
    };
    if let Some(grid) = plan.registered_grid(&query) {
        return Ok(Some(grid));
    }
    let Some(solution) = plan.solution(&table.path, query)? else {
        return Ok(table.missing_grids.get(&window).cloned());
    };
    let shape = IxDyn(&[solution.receptors, solution.channels]);
    let values =
        ArrayD::from_shape_vec(shape.clone(), solution.values).expect("time grid dimensions");
    let flags = ArrayD::from_shape_vec(shape, solution.flags).expect("time flag dimensions");
    Ok(Some(Arc::new(if table.time_is_delay {
        CalibrationGrid::Delay(DelayGrid {
            receptor_count: solution.receptors,
            channel_count: solution.channels,
            values_ns: values.mapv(|value| value.re),
            flags,
        })
    } else {
        CalibrationGrid::Complex(GainGrid {
            receptor_count: solution.receptors,
            channel_count: solution.channels,
            values,
            flags,
        })
    })))
}

/// A missing slice has unity effect with flags, retaining its partner's gain.
pub(super) fn flagged(grid: &CalibrationGrid) -> Arc<CalibrationGrid> {
    let mut grid = grid.clone();
    match &mut grid {
        CalibrationGrid::Complex(grid) => grid.flags.fill(true),
        CalibrationGrid::Delay(grid) => grid.flags.fill(true),
        CalibrationGrid::Antpos(grid) => grid.flagged = true,
        CalibrationGrid::GainCurve(grid) => grid.flags.fill(true),
        CalibrationGrid::Opacity(grid) => grid.flagged = true,
    }
    Arc::new(grid)
}
