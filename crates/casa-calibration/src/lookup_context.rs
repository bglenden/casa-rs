// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator-only time segmentation and phase-delay context.
use super::*;
use casa_tables::{
    RequiredScalarColumnDestination as Destination, RequiredScalarColumnValuesMut as Values, Table,
    TableOptions,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Time lookup options used when imposing calibration errors on simulated DATA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeInterpolationOptions {
    /// Amplitude/phase curve within each selected time series.
    pub mode: TimeInterpolation,
    /// Scale phase by data/calibration reference frequency, tracking cycles.
    pub phase_delay: bool,
    /// Partition by OBSERVATION_ID when calibration and MS observations agree.
    pub per_observation: bool,
    /// Partition by SCAN_NUMBER; takes precedence over per-observation lookup.
    pub per_scan: bool,
    /// Number of OBSERVATION rows in the simulated MeasurementSet.
    pub observation_count: usize,
}

impl TimeInterpolationOptions {
    /// A curve across all fields, without phase-delay or time partitioning.
    #[must_use]
    pub const fn new(mode: TimeInterpolation) -> Self {
        Self {
            mode,
            phase_delay: false,
            per_observation: false,
            per_scan: false,
            observation_count: 1,
        }
    }
    fn contextual(self) -> bool {
        self.phase_delay || self.per_observation || self.per_scan
    }
}

impl CalibrationTableLayout {
    /// Extra retained heap for segmented series and phase-cycle arrays.
    #[must_use]
    pub fn context_kept_bytes(self, options: TimeInterpolationOptions) -> u64 {
        if options.contextual() {
            (self.rows as u64)
                .saturating_mul(128)
                .saturating_add(if options.phase_delay {
                    (self.values as u64).saturating_mul(4)
                } else {
                    0
                })
        } else {
            0
        }
    }
    /// Extra loading heap for scalar row headers and their grouping positions.
    #[must_use]
    pub fn context_loading_bytes(self, options: TimeInterpolationOptions) -> u64 {
        if options.contextual() {
            (self.rows as u64).saturating_mul(96)
        } else {
            0
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TimeScope {
    All,
    Observation,
    Scan,
}

pub(super) struct TimeQuery {
    pub observation: i32,
    pub scan: i32,
    pub window: i32,
    pub antenna: i32,
    pub time: f64,
    pub frequency_hz: Option<f64>,
}

impl CalibrationTableLookup {
    pub(in crate::lookup) fn fringe_phases(
        &self,
        observation: i32,
        scan: i32,
        window: i32,
        antenna: i32,
        time: f64,
        frequency: f64,
    ) -> Result<[f32; 2], ApplyExecutionError> {
        let plan = self
            .advanced_time
            .as_ref()
            .ok_or_else(|| invalid(&self.path, "fringe requires real time curves"))?;
        let segment = match plan.scope {
            TimeScope::All => -1,
            TimeScope::Observation => observation,
            TimeScope::Scan => scan,
        };
        let rows = plan
            .series
            .get(&(segment, window, antenna))
            .or_else(|| plan.series.get(&(segment, window, -1)))
            .filter(|rows| !rows.is_empty())
            .ok_or_else(|| invalid(&self.path, "fringe time series is missing"))?;
        let reference = (rows[0].time_seconds - 1.).floor();
        let query = relative(time, reference);
        let at = |index: usize| relative(rows[index].time_seconds, reference);
        let upper = rows.partition_point(|row| relative(row.time_seconds, reference) < query);
        let exact = upper == 0 || upper == rows.len() || at(upper) == query;
        let mut output = [0.; 2];
        for (receptor, phase) in output.iter_mut().enumerate() {
            let advanced = |index| {
                crate::lookup::fringe::advanced_phase(
                    &rows[index],
                    receptor,
                    time,
                    frequency,
                    &self.path,
                )
            };
            *phase = if exact || plan.mode == TimeInterpolation::Nearest {
                let index = if exact {
                    upper.min(rows.len() - 1)
                } else if at(upper) - query < query - at(upper - 1) {
                    upper
                } else {
                    upper - 1
                };
                advanced(index)? as f32
            } else {
                let weight = ((at(upper) - query) / (at(upper) - at(upper - 1))) as f32;
                let phasor = |phase: f64| Complex32::new(phase.cos() as f32, phase.sin() as f32);
                let lower = phasor(advanced(upper - 1)?);
                let higher = phasor(advanced(upper)?);
                (weight * lower + (1. - weight) * higher).arg()
            };
        }
        Ok(output)
    }

    /// Inspect dimensions for simulator lookup, including a phase-delay
    /// reference frequency axis even for frequency-independent parameters.
    /// Callers first admit the ordinary metadata charge.
    ///
    /// # Errors
    /// Shape or spectral-window metadata cannot be read.
    pub fn layout_for_simulation(
        path: impl AsRef<Path>,
        options: TimeInterpolationOptions,
    ) -> Result<CalibrationTableLayout, ApplyExecutionError> {
        Self::layout_for_simulation_on_grids(path, options, &[])
    }

    /// Simulator layout with the data frequencies used to sample legacy
    /// BPOLY coefficients. Other table families retain their own grids.
    ///
    /// # Errors
    /// Shape metadata is invalid, or BPOLY has no supplied data grid.
    pub fn layout_for_simulation_on_grids(
        path: impl AsRef<Path>,
        options: TimeInterpolationOptions,
        frequencies: &[Vec<f64>],
    ) -> Result<CalibrationTableLayout, ApplyExecutionError> {
        let path = path.as_ref();
        let table =
            Table::open(TableOptions::new(path)).map_err(|e| invalid(path, &e.to_string()))?;
        let bpoly = table.info().sub_type == "BPOLY";
        let real = real_parameters(&table);
        drop(table);
        if bpoly {
            return Self::bpoly_layout(path, frequencies);
        }
        let mut layout = Self::parameter_layout(path, if real { "FPARAM" } else { "CPARAM" })?;
        if (options.phase_delay || real)
            && layout.frequency_values == 0
            && let Some(spectral) = super::super::spectral_window_path(path)?
        {
            layout.frequency_values = super::super::shape_counts(&spectral, "CHAN_FREQ")
                .map_err(|e| invalid(path, &e))?
                .1;
        }
        Ok(layout)
    }

    /// Load simulator time curves and optional scan/observation/phase context.
    /// Admit ordinary layout, time and context loading/kept costs first.
    ///
    /// # Errors
    /// The parameter table, its scalar context, or its time curves are invalid.
    pub fn open_for_simulation(
        path: impl AsRef<Path>,
        options: TimeInterpolationOptions,
    ) -> Result<Self, ApplyExecutionError> {
        Self::open_for_simulation_on_grids(path, options, &[])
    }

    /// Load supplied simulator tables, sampling legacy BPOLY on the data
    /// frequency grids before the ordinary time interpolation is prepared.
    /// Admit `layout_for_simulation_on_grids` and time/context costs first.
    ///
    /// # Errors
    /// The table, frequency grids, or time curves are invalid.
    pub fn open_for_simulation_on_grids(
        path: impl AsRef<Path>,
        options: TimeInterpolationOptions,
        frequencies: &[Vec<f64>],
    ) -> Result<Self, ApplyExecutionError> {
        let basic = if options.mode == TimeInterpolation::Nearest {
            ApplyInterpolationMode::Nearest
        } else {
            ApplyInterpolationMode::Linear
        };
        let path = path.as_ref();
        let table =
            Table::open(TableOptions::new(path)).map_err(|e| invalid(path, &e.to_string()))?;
        let real = real_parameters(&table);
        let bpoly = table.info().sub_type == "BPOLY";
        drop(table);
        let mut lookup = if bpoly {
            Self::open_bpoly(path, basic, frequencies)?
        } else {
            Self::open_parameters(path, basic, FieldSelection::AnyField, real)?
        };
        // CASA's BPOLY-to-B conversion writes unpartitioned (-1) scan and
        // observation headers. The legacy input has neither modern column.
        let options = if bpoly {
            TimeInterpolationOptions {
                per_scan: false,
                per_observation: false,
                phase_delay: false,
                ..options
            }
        } else {
            options
        };
        if real
            || options.contextual()
            || matches!(
                options.mode,
                TimeInterpolation::Cubic | TimeInterpolation::Spline
            )
        {
            let plan = TimePlan::new(&mut lookup, options, real)?;
            lookup.advanced_time = Some(plan);
        }
        Ok(lookup)
    }

    /// Load a baseline-based complex table for simulation. Query indices
    /// are `min(p,q) * antennas + max(p,q)`; numerical interpolation and
    /// flags are the ordinary simulator lookup's. Admit the ordinary layout,
    /// time and context costs before loading.
    ///
    /// # Errors
    /// The table, baseline coordinates, or interpolation parameters are invalid.
    pub fn open_for_baseline_simulation(
        path: impl AsRef<Path>,
        options: TimeInterpolationOptions,
        antennas: usize,
    ) -> Result<Self, ApplyExecutionError> {
        let basic = if options.mode == TimeInterpolation::Nearest {
            ApplyInterpolationMode::Nearest
        } else {
            ApplyInterpolationMode::Linear
        };
        let mut lookup =
            Self::open_parameters(path.as_ref(), basic, FieldSelection::AnyField, false)?;
        lookup.baseline_rows(antennas)?;
        if options.contextual()
            || matches!(
                options.mode,
                TimeInterpolation::Cubic | TimeInterpolation::Spline
            )
        {
            lookup.advanced_time = Some(TimePlan::new(&mut lookup, options, false)?);
        }
        Ok(lookup)
    }

    /// One simulator antenna's solution with observation/scan context.
    /// `frequency_hz` is the native data spectral-centroid frequency for scalar-channel
    /// phase-delay application; frequency-dependent tables pass `None`, as
    /// CASA's vector-frequency interpolation does not apply phase-delay scaling.
    /// All calibration fields participate in the selected time segment.
    ///
    /// # Errors
    /// Interpolation fails or a phase-delay reference frequency is invalid.
    pub fn simulation_solution(
        &self,
        observation: i32,
        scan: i32,
        spw: i32,
        antenna: i32,
        time: f64,
        frequency_hz: Option<f64>,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        if let Some(plan) = &self.advanced_time {
            plan.solution(
                &self.path,
                TimeQuery {
                    observation,
                    scan,
                    window: spw,
                    antenna,
                    time,
                    frequency_hz,
                },
            )
        } else {
            self.solution(0, spw, antenna, time)
        }
    }
}

impl TimePlan {
    fn new(
        lookup: &mut CalibrationTableLookup,
        options: TimeInterpolationOptions,
        real: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let mut scope = TimeScope::All;
        let series = if options.per_observation || options.per_scan {
            let mut rows = headers(&lookup.path)?;
            lookup.baseline_headers(&mut rows)?;
            if options.per_scan {
                if rows
                    .iter()
                    .map(|r| r[4])
                    .min()
                    .is_some_and(|scan| scan != -1)
                {
                    scope = TimeScope::Scan;
                }
            } else if options.per_observation {
                let observations = rows.iter().map(|r| r[3]).collect::<HashSet<_>>();
                let table = Table::open(TableOptions::new(&lookup.path))
                    .map_err(|e| invalid(&lookup.path, &e.to_string()))?;
                let path = table
                    .keywords()
                    .get("OBSERVATION")
                    .and_then(casa_types::Value::as_table_ref)
                    .map(|name| casa_tables::add_directory(name, &lookup.path))
                    .unwrap_or_else(|| lookup.path.join("OBSERVATION"));
                let count = Table::rows_on_disk(&path)
                    .map_err(|e| invalid(&lookup.path, &e.to_string()))?;
                if observations.len() > 1
                    && observations.len() == count
                    && count == options.observation_count
                {
                    scope = TimeScope::Observation;
                }
            }
            let result = segmented(lookup, &rows, scope)?;
            lookup.across_fields = None;
            result
        } else {
            lookup
                .across_fields
                .take()
                .expect("all fields")
                .into_iter()
                .map(|((spw, antenna), rows)| ((-1, spw, antenna), rows))
                .collect()
        };
        let mut plan = Self {
            mode: options.mode,
            real,
            scope,
            series,
            splines: HashMap::new(),
            cycles: HashMap::new(),
            reference_hz: Vec::new(),
        };
        if options.phase_delay && !plan.real {
            plan.reference_hz = CalibrationTableLookup::frequency_grids_on_disk(&lookup.path)?
                .iter()
                .map(|axis| axis.get(axis.len() / 2).copied().unwrap_or(0.))
                .collect();
        }
        for (&key, rows) in &plan.series {
            if options.mode == TimeInterpolation::Spline && !rows.is_empty() {
                plan.splines
                    .insert(key, Spline::new(&lookup.path, rows, plan.real)?);
            }
            if options.phase_delay && !plan.real && !rows.is_empty() {
                plan.cycles.insert(key, phase_cycles(&lookup.path, rows)?);
            }
        }
        Ok(plan)
    }

    pub(super) fn phase_scale(
        &self,
        path: &Path,
        spw: i32,
        frequency: f64,
    ) -> Result<f32, ApplyExecutionError> {
        if !frequency.is_finite() || frequency <= 0. {
            return Err(invalid(path, "phase-delay data frequency must be positive"));
        }
        let reference = self.reference_hz.get(spw as usize).copied().unwrap_or(0.);
        Ok(if reference > 0. {
            (frequency / reference) as f32
        } else {
            1.
        })
    }
}

/// Only scalar columns are read; CPARAM arrays are never rematerialized.
fn headers(path: &Path) -> Result<Vec<[i32; 5]>, ApplyExecutionError> {
    let table = Table::open(TableOptions::new(path)).map_err(|e| invalid(path, &e.to_string()))?;
    let mut rows = Vec::with_capacity(table.row_count());
    let [
        mut fields,
        mut windows,
        mut antennas,
        mut observations,
        mut scans,
    ] = std::array::from_fn(|_| Vec::new());
    for first in (0..table.row_count()).step_by(512) {
        for values in [
            &mut fields,
            &mut windows,
            &mut antennas,
            &mut observations,
            &mut scans,
        ] {
            values.clear();
        }
        let indices = (first..table.row_count().min(first + 512)).collect::<Vec<_>>();
        table
            .required_scalar_columns_for_rows_into(
                &indices,
                &mut [
                    Destination::new("FIELD_ID", Values::Int32(&mut fields)),
                    Destination::new("SPECTRAL_WINDOW_ID", Values::Int32(&mut windows)),
                    Destination::new("ANTENNA1", Values::Int32(&mut antennas)),
                    Destination::new("OBSERVATION_ID", Values::Int32(&mut observations)),
                    Destination::new("SCAN_NUMBER", Values::Int32(&mut scans)),
                ],
            )
            .map_err(|e| invalid(path, &e.to_string()))?;
        for i in 0..indices.len() {
            rows.push([
                fields[i],
                windows[i],
                antennas[i],
                observations[i],
                scans[i],
            ]);
        }
    }
    Ok(rows)
}

fn segmented(
    lookup: &CalibrationTableLookup,
    headers: &[[i32; 5]],
    scope: TimeScope,
) -> Result<TimeSeries, ApplyExecutionError> {
    let mut positions = HashMap::<(i32, i32, i32), usize>::new();
    let mut grouped = HashMap::<_, Vec<(i32, &CalibrationSolution)>>::new();
    for &[field, window, antenna, observation, scan] in headers {
        let position = positions.entry((field, window, antenna)).or_default();
        let row = lookup
            .table
            .solutions
            .get(&(field, window, antenna))
            .and_then(|rows| rows.get(*position))
            .ok_or_else(|| {
                invalid(
                    &lookup.path,
                    "calibration scalar context differs from loaded solutions",
                )
            })?;
        *position += 1;
        let segment = match scope {
            TimeScope::All => -1,
            TimeScope::Observation => observation,
            TimeScope::Scan => scan,
        };
        grouped
            .entry((segment, window, antenna))
            .or_default()
            .push((field, row));
    }
    Ok(grouped
        .into_iter()
        .map(|(key, mut rows)| {
            rows.sort_by(|a, b| {
                a.1.time_seconds
                    .total_cmp(&b.1.time_seconds)
                    .then(a.0.cmp(&b.0))
            });
            (
                key,
                rows.into_iter()
                    .map(|(_, row)| CalibrationSolution {
                        time_seconds: row.time_seconds,
                        grid: Arc::clone(&row.grid),
                    })
                    .collect(),
            )
        })
        .collect())
}

fn phase_cycles(
    path: &Path,
    rows: &[CalibrationSolution],
) -> Result<Vec<f32>, ApplyExecutionError> {
    let first = grid(path, &rows[0])?;
    let count = first.receptor_count * first.channel_count;
    let mut cycles = vec![0.; rows.len() * count];
    for index in 0..count {
        let (z, flag) = value(first, index);
        let mut previous = if flag { 0. } else { z.arg() };
        let mut shift = 0.;
        for (time, row) in rows.iter().enumerate().skip(1) {
            let current = grid(path, row)?;
            if current.values.shape() != first.values.shape() {
                return Err(invalid(path, "phase-delay parameter shapes must match"));
            }
            let (z, flag) = value(current, index);
            if !flag {
                let difference = z.arg() - previous;
                if difference > std::f32::consts::PI {
                    shift -= std::f32::consts::TAU;
                }
                if difference < -std::f32::consts::PI {
                    shift += std::f32::consts::TAU;
                }
                previous = z.arg();
            }
            cycles[time * count + index] = shift;
        }
    }
    Ok(cycles)
}

fn real_parameters(table: &Table) -> bool {
    use casa_types::{ScalarValue, Value};
    match table.keywords().get("ParType") {
        Some(Value::Scalar(ScalarValue::String(kind))) if kind == "Float" => true,
        Some(Value::Scalar(ScalarValue::String(kind))) if kind == "Complex" => false,
        _ => table.schema().is_some_and(|schema| {
            let contains = |name: &str| schema.columns().iter().any(|column| column.name() == name);
            contains("FPARAM") && !contains("CPARAM")
        }),
    }
}
