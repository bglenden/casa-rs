// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and evaluate legacy CASA GSPLINE gain tables.

use std::collections::HashMap;
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue};
use casa_tables::{Table, TableOptions};
use num_complex::Complex32;

use crate::{CalibrationTableLayout, ComplexSolution};

/// Legacy time-dependent diagonal gains, keyed by antenna and field.
pub struct LegacySplineTable {
    rows: HashMap<(i32, usize), Row>,
    windows: usize,
}

struct Row {
    antenna: i32,
    field: i32,
    scale: Complex32,
    reference_hz: f64,
    amplitude: Option<Curve>,
    phase: Option<Curve>,
}

struct Curve {
    knots: Vec<f64>,
    coefficients: Vec<f64>,
    stride: usize,
}

impl LegacySplineTable {
    /// Number of data windows covered at the time the table was configured.
    pub fn windows(&self) -> usize {
        self.windows
    }

    /// Inspect the coefficient cell shapes without loading the arrays.
    ///
    /// # Errors
    /// Returns a string describing an invalid table or a size overflow.
    pub fn layout(path: &Path, windows: usize) -> Result<CalibrationTableLayout, String> {
        let table = Table::open(TableOptions::new(path)).map_err(|error| error.to_string())?;
        let rows = table.row_count();
        let mut values = 0usize;
        for name in [
            "SPLINE_KNOTS_AMP",
            "SPLINE_KNOTS_PHASE",
            "POLY_COEFF_AMP",
            "POLY_COEFF_PHASE",
        ] {
            let column = table
                .column_accessor(name)
                .map_err(|error| error.to_string())?;
            for start in (0..rows).step_by(512) {
                let selected: Vec<_> = (start..rows.min(start + 512)).collect();
                for shape in column
                    .array_shapes_uncached(&selected)
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .flatten()
                {
                    let size = shape
                        .into_iter()
                        .try_fold(1usize, usize::checked_mul)
                        .ok_or_else(|| "GSPLINE cell size overflows usize".to_owned())?;
                    values = values
                        .checked_add(size)
                        .ok_or_else(|| "GSPLINE value count overflows usize".to_owned())?;
                }
            }
        }
        Ok(CalibrationTableLayout {
            rows,
            values,
            max_cell_values: 2,
            frequency_values: 0,
            frequency_windows: windows,
        })
    }

    /// Load the first matching solution for each antenna and field in disk order.
    ///
    /// # Errors
    /// Returns a string describing a malformed or inaccessible table.
    pub fn open(path: &Path, windows: usize) -> Result<Self, String> {
        let table = Table::open(TableOptions::new(path)).map_err(|error| error.to_string())?;
        let mut rows = HashMap::with_capacity(table.row_count());
        for index in 0..table.row_count() {
            let mode = match table
                .cell_accessor(index, "POLY_MODE")
                .map_err(|error| error.to_string())?
                .scalar()
                .map_err(|error| error.to_string())?
            {
                ScalarValue::String(mode) => mode,
                _ => return Err("GSPLINE POLY_MODE is not a string".to_owned()),
            };
            let scale = match table
                .cell_accessor(index, "SCALE_FACTOR")
                .map_err(|error| error.to_string())?
                .scalar()
                .map_err(|error| error.to_string())?
            {
                ScalarValue::Complex32(value) => *value,
                _ => return Err("GSPLINE SCALE_FACTOR is not complex".to_owned()),
            };
            let reference = floats(&table, index, "REF_FREQUENCY")?;
            let row = Row {
                antenna: integer(&table, index, "ANTENNA1")?,
                field: integer(&table, index, "FIELD_ID")?,
                scale,
                reference_hz: reference.first().copied().unwrap_or(0.),
                amplitude: (mode.contains("AMP") || mode.contains("A&P"))
                    .then(|| Curve::open(&table, index, "AMP"))
                    .transpose()?,
                phase: (mode.contains("PHAS") || mode.contains("A&P"))
                    .then(|| Curve::open(&table, index, "PHASE"))
                    .transpose()?,
            };
            rows.entry((row.antenna, row.field as usize)).or_insert(row);
        }
        Ok(Self { rows, windows })
    }

    /// Evaluate a two-receptor gain at the requested time and mean data frequency.
    /// No solution exists when the table has no row for the antenna and field.
    pub fn solution(
        &self,
        antenna: i32,
        field: usize,
        time: f64,
        frequency_hz: Option<f64>,
    ) -> Option<ComplexSolution> {
        // The legacy buffer matches antenna AND field and takes its first
        // matching row. Interpolation arguments do not change these splines.
        let row = self.rows.get(&(antenna, field))?;
        let ratio = frequency_hz
            .filter(|_| row.reference_hz.abs() > 0.)
            .map_or(1., |frequency| frequency / row.reference_hz);
        let values = (0..2)
            .map(|receptor| {
                let amplitude = row
                    .amplitude
                    .as_ref()
                    .map_or(1., |c| c.value(time, receptor).exp());
                // GILDAS phases have the opposite sign; CASA also scales them
                // by mean observing frequency / calibration reference frequency.
                let phase = row
                    .phase
                    .as_ref()
                    .map_or(0., |c| -c.value(time, receptor) * ratio);
                row.scale * Complex32::from_polar(amplitude as f32, phase as f32)
            })
            .collect();
        Some(ComplexSolution {
            receptors: 2,
            channels: 1,
            values,
            flags: vec![false; 2],
        })
    }
}

impl Curve {
    fn open(table: &Table, row: usize, suffix: &str) -> Result<Self, String> {
        let stride = usize::try_from(integer(table, row, &format!("N_POLY_{suffix}"))?)
            .map_err(|_| "GSPLINE coefficient count is negative".to_owned())?;
        let count = usize::try_from(integer(table, row, &format!("N_KNOTS_{suffix}"))?)
            .map_err(|_| "GSPLINE knot count is negative".to_owned())?;
        let mut knots = floats(table, row, &format!("SPLINE_KNOTS_{suffix}"))?;
        let mut coefficients = floats(table, row, &format!("POLY_COEFF_{suffix}"))?;
        let used_coefficients = stride
            .checked_mul(2)
            .ok_or("GSPLINE coefficient count overflow")?;
        // A table assembled by append can have a common physical size for
        // rows with different logical lengths. The tails are unused.
        knots.truncate(count);
        coefficients.truncate(used_coefficients);
        if count < 8
            || knots.len() != count
            || stride < count - 4
            || coefficients.len() != used_coefficients
            || knots.iter().chain(&coefficients).any(|v| !v.is_finite())
            || knots.windows(2).any(|w| w[0] > w[1])
            || knots[3] >= knots[count - 4]
        {
            return Err("invalid GSPLINE cubic knot/coefficient layout".to_owned());
        }
        Ok(Self {
            knots,
            coefficients,
            stride,
        })
    }

    fn value(&self, time: f64, receptor: usize) -> f64 {
        let knots = &self.knots;
        let count = knots.len() - 4;
        // GETBSPL returns zero outside the fitted interval. Consequently
        // the gain retains SCALE_FACTOR, with unit amplitude and zero phase.
        if time < knots[3] || time > knots[count] {
            return 0.;
        }
        let span = knots
            .partition_point(|&knot| knot <= time)
            .saturating_sub(1)
            .clamp(3, count - 1);
        let coefficients = &self.coefficients[receptor * self.stride..];
        let mut values: [f64; 4] = std::array::from_fn(|j| coefficients[span - 3 + j]);
        // de Boor's cubic recurrence, retaining only four values on the stack.
        for order in 1..=3 {
            for j in (order..=3).rev() {
                let i = span - 3 + j;
                let width = knots[i + 4 - order] - knots[i];
                let fraction = if width > 0. {
                    (time - knots[i]) / width
                } else {
                    0.
                };
                values[j] = (1. - fraction) * values[j - 1] + fraction * values[j];
            }
        }
        values[3]
    }
}

fn integer(table: &Table, row: usize, name: &str) -> Result<i32, String> {
    match table
        .cell_accessor(row, name)
        .map_err(|error| error.to_string())?
        .scalar()
        .map_err(|error| error.to_string())?
    {
        ScalarValue::Int32(value) => Ok(*value),
        _ => Err(format!("GSPLINE {name} is not an integer")),
    }
}

fn floats(table: &Table, row: usize, name: &str) -> Result<Vec<f64>, String> {
    match table
        .cell_accessor(row, name)
        .map_err(|error| error.to_string())?
        .array()
        .map_err(|error| error.to_string())?
    {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        _ => Err(format!("GSPLINE {name} is not a double array")),
    }
}
