// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plot the legacy BPOLY coefficients with CASA's display normalization.

use casa_core::{ArrayValue, ScalarValue};
use casa_tables::Table;

use casa_calibration::apply::legacy_bpoly_chebyshev_value;

use super::spectrum::{Curve, Ordinate};
use super::table::array;

pub(super) struct Polynomial {
    pub(super) frequencies_hz: Vec<f64>,
    pub(super) domain: [f64; 2],
    amplitudes: Vec<Vec<f64>>,
    phases: Vec<Vec<f64>>,
    degrees: [usize; 2],
}

impl Polynomial {
    pub(super) fn read(table: &Table, row: usize) -> Result<Self, String> {
        if text(table, row, "POLY_TYPE")? != "CHEBYSHEV" {
            return Err(format!("unsupported polynomial type at BPOLY row {row}"));
        }
        let domain: [f64; 2] = numeric_array(table, row, "VALID_DOMAIN")?
            .try_into()
            .map_err(|_| "BPOLY VALID_DOMAIN needs two frequencies")?;
        if !domain.iter().all(|v| v.is_finite()) || domain[1] <= domain[0] {
            return Err("BPOLY VALID_DOMAIN must be a finite increasing interval".into());
        }
        let mode = text(table, row, "POLY_MODE")?;
        let amp = coefficients(
            table,
            row,
            "N_POLY_AMP",
            "POLY_COEFF_AMP",
            mode.contains('A'),
        )?;
        let phase = coefficients(
            table,
            row,
            "N_POLY_PHASE",
            "POLY_COEFF_PHASE",
            mode.contains('P'),
        )?;
        let receptors = amp.len().max(phase.len()).max(1);
        let degrees = [&amp, &phase].map(|values| {
            values
                .first()
                .map_or(0, |values| values.len().saturating_sub(1))
        });
        let scale = match scalar(table, row, "SCALE_FACTOR")? {
            ScalarValue::Complex32(value) => f64::from(value.re),
            ScalarValue::Complex64(value) => value.re,
            _ => return Err("BPOLY SCALE_FACTOR must be complex".into()),
        };
        let [start, end] = domain;
        let width = end - start;
        let step = width * 0.001;
        // NumPy arange takes the effective representable step at the first
        // physical channel, even when the domain starts at a large frequency.
        let stride = (start + step) - start;
        let frequencies_hz = (0..(width / step).ceil() as usize)
            .map(|index| start + index as f64 * stride)
            .collect::<Vec<_>>();
        let amplitudes = (0..receptors)
            .map(|receptor| {
                let values = amp.get(receptor).map_or(&[] as &[f64], Vec::as_slice);
                let mut values = evaluate(values, domain, &frequencies_hz);
                for value in &mut values {
                    *value += scale;
                }
                let offset = 1.0 - mean(&values);
                for value in &mut values {
                    *value += offset;
                }
                values
            })
            .collect();
        let phases = (0..receptors)
            .map(|receptor| {
                let values = phase.get(receptor).map_or(&[] as &[f64], Vec::as_slice);
                evaluate(values, domain, &frequencies_hz)
                    .into_iter()
                    .map(f64::to_degrees)
                    .collect()
            })
            .collect();
        Ok(Self {
            frequencies_hz,
            domain,
            amplitudes,
            phases,
            degrees,
        })
    }

    pub(super) fn receptors(&self) -> usize {
        self.amplitudes.len()
    }

    pub(super) fn degrees(&self) -> [usize; 2] {
        self.degrees
    }

    pub(super) fn curve(&self, receptor: usize, ordinate: Ordinate) -> Result<Curve, String> {
        let values = match ordinate {
            Ordinate::Phase => &self.phases,
            _ => &self.amplitudes,
        };
        let mut y = values
            .get(receptor)
            .ok_or("BPOLY spectrum missing receptor")?
            .clone();
        if ordinate == Ordinate::Decibels {
            for value in &mut y {
                *value = 10.0 * value.log10();
            }
        }
        let x = self.frequencies_hz.iter().map(|hz| hz * 1e-9).collect();
        Ok(Curve { x, y })
    }
}

fn evaluate(coefficients: &[f64], [lower, upper]: [f64; 2], frequencies: &[f64]) -> Vec<f64> {
    frequencies
        .iter()
        .map(|&hz| legacy_bpoly_chebyshev_value(coefficients, lower, upper, hz * 1e-9 * 1e9))
        .collect()
}

fn coefficients(
    table: &Table,
    row: usize,
    count: &str,
    name: &str,
    active: bool,
) -> Result<Vec<Vec<f64>>, String> {
    if !active {
        return Ok(Vec::new());
    }
    let count = match scalar(table, row, count)? {
        ScalarValue::Int32(value) if *value >= 0 => *value as usize,
        _ => return Err(format!("invalid BPOLY coefficient count at row {row}")),
    };
    let values = numeric_array(table, row, name)?;
    if count == 0 || !values.len().is_multiple_of(count) {
        return Err(format!("invalid BPOLY {name} at row {row}"));
    }
    Ok(values
        .chunks(count)
        .map(|chunk| {
            let mut coefficients = chunk.to_vec();
            // The CASA plotting script zeroes the constant term before
            // evaluating either display curve.
            coefficients[0] = 0.0;
            coefficients
        })
        .collect())
}

fn numeric_array(table: &Table, row: usize, name: &str) -> Result<Vec<f64>, String> {
    match array(table, row, name)? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Float32(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        _ => Err(format!("BPOLY {name} at row {row} is not a floating array")),
    }
}

fn text<'a>(table: &'a Table, row: usize, name: &str) -> Result<&'a str, String> {
    match scalar(table, row, name)? {
        ScalarValue::String(text) => Ok(text),
        _ => Err(format!("BPOLY {name} at row {row} is not a string")),
    }
}

fn scalar<'a>(table: &'a Table, row: usize, name: &str) -> Result<&'a ScalarValue, String> {
    table
        .cell_accessor(row, name)
        .and_then(|cell| cell.scalar())
        .map_err(|error| error.to_string())
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}
