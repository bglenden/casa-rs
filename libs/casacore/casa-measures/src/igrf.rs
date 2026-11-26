// SPDX-License-Identifier: LGPL-3.0-or-later
//! IGRF coefficient loading, interpolation and date conversion.

use std::path::Path;

use casa_core::{ScalarValue, Value};

use super::{MeasuresDataError, RuntimeTable, ScientificStateHasher};

#[derive(Debug, Clone)]
pub(super) struct IgrfTable {
    years: Vec<f64>,
    mjd0: f64,
    step_days: f64,
    coeffs_by_year: Vec<Vec<f64>>,
    rates_by_year: Vec<Vec<f64>>,
    nmax: usize,
}

impl IgrfTable {
    pub(super) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .years
            .capacity()
            .checked_mul(size_of::<f64>())?
            .checked_add(
                self.coeffs_by_year
                    .capacity()
                    .checked_mul(size_of::<Vec<f64>>())?,
            )?
            .checked_add(
                self.rates_by_year
                    .capacity()
                    .checked_mul(size_of::<Vec<f64>>())?,
            )?;
        for coefficients in self.coeffs_by_year.iter().chain(&self.rates_by_year) {
            bytes = bytes.checked_add(coefficients.capacity().checked_mul(size_of::<f64>())?)?;
        }
        Some(bytes)
    }

    pub(super) fn coefficients_at_mjd(
        &self,
        mjd: f64,
    ) -> Result<(Vec<f64>, usize), MeasuresDataError> {
        if !mjd.is_finite() || self.coeffs_by_year.is_empty() {
            return Err(MeasuresDataError::MissingData(
                "invalid IGRF model epoch or empty coefficient table".into(),
            ));
        }
        // casacore uses a uniform five-year grid. MJD0 is one interval
        // before the first row. The end rows supply extrapolation rates.
        let row = (((mjd - self.mjd0) / self.step_days) as usize)
            .saturating_sub(1)
            .min(self.coeffs_by_year.len() - 1);
        let row_mjd = self.mjd0 + (row + 1) as f64 * self.step_days;
        let years = 5.0 * (mjd - row_mjd) / self.step_days;
        let values = self.coeffs_by_year[row]
            .iter()
            .zip(&self.rates_by_year[row])
            .map(|(base, rate)| base + rate * years)
            .collect();
        Ok((values, self.nmax))
    }

    pub(super) fn coefficients_for_decimal_year(
        &self,
        decimal_year: f64,
    ) -> Result<(Vec<f64>, usize), MeasuresDataError> {
        let min_year = *self.years.first().ok_or_else(|| {
            MeasuresDataError::MissingData("IGRF table has no year rows".to_string())
        })?;
        let max_year = self.years.last().copied().unwrap_or(min_year) + 5.0;
        if decimal_year < min_year || decimal_year > max_year {
            return Err(MeasuresDataError::MissingData(format!(
                "IGRF date must be between {min_year:.0}-01-01 and {max_year:.0}-12-31"
            )));
        }

        let coeffs = if decimal_year >= *self.years.last().expect("years non-empty") {
            extrapolate_coefficients(
                decimal_year,
                *self.years.last().expect("years non-empty"),
                self.coeffs_by_year.last().expect("coefficients non-empty"),
                self.rates_by_year.last().expect("rates non-empty"),
            )
        } else {
            let upper = self
                .years
                .iter()
                .position(|year| *year > decimal_year)
                .expect("decimal year below final row has upper interval");
            interpolate_coefficients(
                decimal_year,
                self.years[upper - 1],
                &self.coeffs_by_year[upper - 1],
                self.years[upper],
                &self.coeffs_by_year[upper],
            )
        };

        Ok((coeffs, self.nmax))
    }
}

impl IgrfTable {
    pub(super) fn hash_state(
        &self,
        state: &mut ScientificStateHasher,
    ) -> Result<(), MeasuresDataError> {
        state.sequence_len(self.years.len())?;
        for year in &self.years {
            state.float(*year);
        }
        state.sequence_len(self.coeffs_by_year.len())?;
        for coefficients in &self.coeffs_by_year {
            state.sequence_len(coefficients.len())?;
            for coefficient in coefficients {
                state.float(*coefficient);
            }
        }
        state.float(self.mjd0);
        state.float(self.step_days);
        state.sequence_len(self.rates_by_year.len())?;
        for rates in &self.rates_by_year {
            state.sequence_len(rates.len())?;
            for rate in rates {
                state.float(*rate);
            }
        }
        state.sequence_len(self.nmax)?;

        Ok(())
    }
}

pub(super) fn load_igrf_from(root: &Path) -> Result<IgrfTable, MeasuresDataError> {
    let table = RuntimeTable::open(&root.join("geodetic/IGRF"))?;
    let mjd = table.scalar_f64("MJD")?;
    let mut years = Vec::with_capacity(table.row_count());
    let mut coeffs_by_year = Vec::with_capacity(table.row_count());
    let mut rates_by_year = Vec::with_capacity(table.row_count());
    let keyword = |name| match table.table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if value.is_finite() && *value > 0.0 => {
            Ok(*value)
        }
        _ => Err(MeasuresDataError::MissingData(format!(
            "invalid IGRF table keyword {name}"
        ))),
    };
    let mjd0 = keyword("MJD0")?;
    let step_days = keyword("dMJD")?;
    let mut nmax = 0usize;

    for (row, mjd_value) in mjd.iter().copied().enumerate().take(table.row_count()) {
        years.push(decimal_year_from_mjd(mjd_value));
        let coeffs = table.array_f64_cell("COEF", row)?;
        nmax = solve_igrf_nmax(coeffs.len())?;
        coeffs_by_year.push(coeffs);
        let rates = table.array_f64_cell("dCOEF", row)?;
        if rates.len() != coeffs_by_year[row].len() {
            return Err(MeasuresDataError::MissingData(format!(
                "wrong IGRF rate count at row {row}"
            )));
        }
        rates_by_year.push(rates);
    }

    Ok(IgrfTable {
        years,
        coeffs_by_year,
        mjd0,
        step_days,
        rates_by_year,
        nmax,
    })
}

fn decimal_year_from_mjd(mjd: f64) -> f64 {
    let jd = mjd + 2_400_000.5;
    let z = (jd + 0.5).floor();
    let f = (jd + 0.5) - z;
    let mut a = z;
    if z >= 2_299_161.0 {
        let alpha = ((z - 1_867_216.25) / 36_524.25).floor();
        a = z + 1.0 + alpha - (alpha / 4.0).floor();
    }
    let b = a + 1524.0;
    let c = ((b - 122.1) / 365.25).floor();
    let d = (365.25 * c).floor();
    let e = ((b - d) / 30.6001).floor();
    let day = b - d - (30.6001 * e).floor() + f;
    let month = if e < 14.0 { e - 1.0 } else { e - 13.0 };
    let year = if month > 2.0 { c - 4716.0 } else { c - 4715.0 };

    let year_i = year as i32;
    let month_i = month as i32;
    let day_i = day.floor() as i32;
    let leap = if (year_i % 4 == 0) && ((year_i % 100 != 0) || (year_i % 400 == 0)) {
        1
    } else {
        0
    };
    const DAYS: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut day_in_year = DAYS[(month_i - 1) as usize] + day_i;
    if month_i > 2 {
        day_in_year += leap;
    }
    f64::from(year_i) + (f64::from(day_in_year) / (365.0 + f64::from(leap)))
}

fn solve_igrf_nmax(coeff_len: usize) -> Result<usize, MeasuresDataError> {
    for nmax in 1..32 {
        if nmax * (nmax + 2) == coeff_len {
            return Ok(nmax);
        }
    }
    Err(MeasuresDataError::MissingData(format!(
        "unable to derive IGRF nmax from coefficient length {coeff_len}"
    )))
}

fn interpolate_coefficients(
    date: f64,
    year1: f64,
    coeffs1: &[f64],
    year2: f64,
    coeffs2: &[f64],
) -> Vec<f64> {
    let factor = (date - year1) / (year2 - year1);
    coeffs1
        .iter()
        .zip(coeffs2.iter())
        .map(|(lhs, rhs)| lhs + factor * (rhs - lhs))
        .collect()
}

fn extrapolate_coefficients(date: f64, base_year: f64, main: &[f64], sv: &[f64]) -> Vec<f64> {
    let factor = date - base_year;
    main.iter()
        .zip(sv.iter())
        .map(|(main_coeff, sv_coeff)| main_coeff + factor * sv_coeff)
        .collect()
}
