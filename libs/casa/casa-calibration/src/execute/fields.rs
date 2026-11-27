// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traditional setapply selects calibration rows, then merges fields in time.

use super::*;
use crate::lookup::{TimePlan, TimeQuery};

pub(crate) fn load(
    plan: &ApplyCalibrationTablePlan,
    engine: Option<&Arc<MsCalEngine>>,
    ms: &MeasurementSet,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    let family = TimeParameterFamily::from_subtype(&plan.summary.table_subtype);
    if matches!(family, Some(TimeParameterFamily::SwitchedPower)) {
        switched_power::validate_context(ms, &plan.spec.path)?;
    }
    // Interpolate the real FPARAM coefficients before constructing the Jones
    // matrix. Antenna positions, opacity and gain-curve coefficients share
    // the same real time operator as delays.
    let mut table = load_parameters(plan, engine, family.is_some(), Some(ms))?;
    if table.spline.is_some() {
        return Ok(table);
    }
    table.time_parameter_family = family;
    table.callibrary = plan.spec.callibrary.clone();
    if !plan.resolved_gainfields.is_empty() {
        table.solutions.retain(|(field, _, _), _| {
            plan.resolved_gainfields
                .iter()
                .any(|selected| selected.field_id == *field)
        });
    }
    if let Some(library) = plan
        .spec
        .callibrary
        .as_ref()
        .filter(|library| !library.field_map.is_empty())
    {
        table
            .solutions
            .retain(|(field, _, _), _| library.field_map.contains(field));
    }
    // One flagged template per window handles missing antenna/context slices
    // without copying a full parameter grid for each baseline.
    for (&(_, window, _), rows) in &table.solutions {
        if let std::collections::hash_map::Entry::Vacant(entry) = table.missing_grids.entry(window)
            && let Some(row) = rows.first()
        {
            let grid = physical_grid(&table.path, family, Arc::clone(&row.grid))?;
            entry.insert(flagged(&grid));
        }
    }
    let nearest_field = plan.summary.table_subtype != "BPOLY"
        && (matches!(
            plan.spec.gainfield,
            Some(crate::apply::GainFieldSelector::Nearest)
        ) || plan
            .spec
            .callibrary
            .as_ref()
            .is_some_and(|library| !library.field_map.is_empty()));
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
            options.per_observation = plan
                .spec
                .callibrary
                .as_ref()
                .is_some_and(|library| library.observation_map.active());
            options.per_scan = plan
                .spec
                .callibrary
                .as_ref()
                .is_some_and(|library| library.scan_map.active());
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
        let mut context = TimePlan::apply_context(&table, options)?;
        if let Some(library) = &plan.spec.callibrary {
            context.mapped_contexts(library.observation_map.active(), library.scan_map.active());
        }
        let mut groups = HashMap::<_, HashMap<_, _>>::new();
        for (key, rows) in std::mem::take(&mut table.solutions) {
            groups
                .entry(if nearest_field { key.0 } else { -1 })
                .or_default()
                .insert(key, rows);
        }
        for (field, solutions) in groups {
            let selected = LoadedCalibrationTable {
                path: table.path.clone(),
                baseline: table.baseline,
                interp: table.interp,
                supports_calwt: false,
                engine: None,
                antpos_tropo: None,
                ionosphere: None,
                sky_exposures: None,
                spline: None,
                solutions,
                callibrary: None,
                time_plans: HashMap::new(),
                missing_grids: HashMap::new(),
                time_parameter_family: None,
                phase_delay: false,
            };
            table.time_plans.insert(
                field,
                TimePlan::for_apply(&selected, options, family.is_some(), &context)?,
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

#[derive(Clone)]
pub(super) struct GridAtTime {
    pub grid: Arc<CalibrationGrid>,
    pub interpolated: bool,
}

pub(super) fn mapped_antenna(table: &LoadedCalibrationTable, antenna: i32) -> i32 {
    table
        .callibrary
        .as_ref()
        .and_then(|library| {
            library
                .antenna_map
                .get(antenna as usize)
                .or_else(|| library.antenna_map.last())
        })
        .copied()
        .filter(|id| *id >= 0)
        .unwrap_or(antenna)
}

pub(super) fn lookup(
    table: &LoadedCalibrationTable,
    field: i32,
    window: i32,
    antenna: i32,
    (row, scan): (&ApplyRowPlan, i32),
    data_window: &crate::plan::SpectralWindowPlan,
    evaluation_frequency_hz: f64,
) -> Result<Option<GridAtTime>, ApplyExecutionError> {
    let pinned = |grid| GridAtTime {
        grid,
        interpolated: false,
    };
    if let Some(spline) = &table.spline {
        return Ok(gspline::grid(
            spline,
            field,
            antenna,
            row.time_seconds,
            evaluation_frequency_hz
                .is_finite()
                .then_some(evaluation_frequency_hz),
        )
        .map(pinned));
    }
    let antenna = if table.baseline {
        antenna
    } else {
        mapped_antenna(table, antenna)
    };
    let Some(plan) = table.time_plans.get(&field) else {
        if !table.time_plans.is_empty() {
            return Ok(table.missing_grids.get(&window).cloned().map(pinned));
        }
        return table
            .lookup(field, window, antenna, row.time_seconds, table.interp)
            .map(|grid| {
                grid.or_else(|| table.missing_grids.get(&window).cloned())
                    .map(pinned)
            });
    };
    let frequencies = &data_window.channel_frequencies_hz;
    let frequency_hz = table
        .phase_delay
        .then(|| frequencies.iter().sum::<f64>() / frequencies.len() as f64);
    let query = TimeQuery {
        observation: table
            .callibrary
            .as_ref()
            .map_or(row.calibration_observation_id, |library| {
                library.observation_map.map(row.calibration_observation_id)
            }),
        scan: table
            .callibrary
            .as_ref()
            .map_or(scan, |library| library.scan_map.map(scan)),
        window,
        antenna,
        time: row.time_seconds,
        frequency_hz,
    };
    let allow_raw_real = matches!(
        table.time_parameter_family,
        Some(TimeParameterFamily::Tsys | TimeParameterFamily::Sky)
    );
    if let Some(grid) = plan.registered_grid(&query, allow_raw_real) {
        return Ok(Some(pinned(grid)));
    }
    let solution = if matches!(
        table.time_parameter_family,
        Some(TimeParameterFamily::Fringe)
    ) {
        plan.fringe_solution(&table.path, query, evaluation_frequency_hz)?
    } else {
        plan.solution(&table.path, query)?
    };
    let Some(solution) = solution else {
        return Ok(table.missing_grids.get(&window).cloned().map(pinned));
    };
    let shape = IxDyn(&[solution.receptors, solution.channels]);
    let values =
        ArrayD::from_shape_vec(shape.clone(), solution.values).expect("time grid dimensions");
    let flags = ArrayD::from_shape_vec(shape, solution.flags).expect("time flag dimensions");
    let raw = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: solution.receptors,
        channel_count: solution.channels,
        values,
        flags,
    }));
    physical_grid(&table.path, table.time_parameter_family, raw).map(|grid| {
        Some(GridAtTime {
            grid,
            interpolated: true,
        })
    })
}

/// FPARAM interpolation operates on coefficients, not on their geometry-dependent gains.
fn physical_grid(
    path: &Path,
    family: Option<TimeParameterFamily>,
    raw: Arc<CalibrationGrid>,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let Some(family) = family else {
        return Ok(raw);
    };
    if matches!(
        family,
        TimeParameterFamily::Tsys
            | TimeParameterFamily::Sky
            | TimeParameterFamily::PositionAngle
            | TimeParameterFamily::Tec
    ) {
        // These coefficients require real interpolation in both dimensions.
        return Ok(raw);
    }
    let CalibrationGrid::Complex(grid) = raw.as_ref() else {
        unreachable!("real parameter carrier");
    };
    if matches!(family, TimeParameterFamily::Fringe) {
        return Ok(Arc::new(CalibrationGrid::Fringe(grid.clone())));
    }
    if matches!(family, TimeParameterFamily::SpectralOpacity) {
        return Ok(Arc::new(CalibrationGrid::SpectralOpacity(grid.clone())));
    }
    let values = ArrayValue::Float32(grid.values.mapv(|value| value.re));
    let flags = ArrayValue::Bool(grid.flags.clone());
    Ok(Arc::new(match family {
        TimeParameterFamily::Delay => {
            CalibrationGrid::Delay(DelayGrid::from_arrays(path, &values, &flags)?)
        }
        TimeParameterFamily::Antpos => {
            CalibrationGrid::Antpos(AntposGrid::from_arrays(path, &values, &flags)?)
        }
        TimeParameterFamily::GainCurve { power } => {
            CalibrationGrid::GainCurve(GainCurveGrid::from_arrays(path, &values, &flags, power)?)
        }
        TimeParameterFamily::Opacity => {
            CalibrationGrid::Opacity(OpacityGrid::from_arrays(path, &values, &flags)?)
        }
        TimeParameterFamily::Tsys
        | TimeParameterFamily::Sky
        | TimeParameterFamily::PositionAngle
        | TimeParameterFamily::Tec
        | TimeParameterFamily::Fringe
        | TimeParameterFamily::SpectralOpacity => {
            unreachable!("coefficient uses the raw interpolated value")
        }
        TimeParameterFamily::SwitchedPower => CalibrationGrid::SwitchedPower(
            switched_power::SwitchedPowerGrid::from_grid(path, grid)?,
        ),
    }))
}

/// A missing slice has unity effect with flags, retaining its partner's gain.
pub(super) fn flagged(grid: &CalibrationGrid) -> Arc<CalibrationGrid> {
    let mut grid = grid.clone();
    match &mut grid {
        CalibrationGrid::Complex(grid)
        | CalibrationGrid::Fringe(grid)
        | CalibrationGrid::SpectralOpacity(grid) => grid.flags.fill(true),
        CalibrationGrid::Delay(grid) => grid.flags.fill(true),
        CalibrationGrid::Antpos(grid) => grid.flagged = true,
        CalibrationGrid::GainCurve(grid) => grid.flags.fill(true),
        CalibrationGrid::Opacity(grid) => grid.flagged = true,
        CalibrationGrid::SwitchedPower(grid) => grid.flag_all(),
    }
    Arc::new(grid)
}
