// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibrator flux polynomials from the runtime VLA standards tables.

use std::path::{Path, PathBuf};

use casa_measures_data::MeasuresRuntime;
use casa_ms::MsResult;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::MDirection;
use casa_types::{ArrayValue, ScalarValue};
use time::{Date, Month};

use crate::ft::invalid;
use crate::interpolation::{self, Curve};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogScale {
    PerleyButler2017,
    PerleyButler2013,
    ScaifeHeald2012,
}

impl CatalogScale {
    pub(crate) fn time_interpolation(self) -> bool {
        self != Self::ScaifeHeald2012
    }

    fn table(self) -> &'static str {
        match self {
            Self::PerleyButler2017 => "PerleyButler2017Coeffs",
            Self::PerleyButler2013 => "PerleyButler2013Coeffs",
            Self::ScaifeHeald2012 => "ScaifeHeald2012Coeffs",
        }
    }
}

pub(crate) struct FluxCatalog {
    scale: CatalogScale,
    sources: Vec<Calibrator>,
    coefficients: Table,
    epochs: Vec<f64>,
}

struct Calibrator {
    name: String,
    aliases: Vec<String>,
    ra: f64,
    dec: f64,
}

pub(crate) struct CalibratorSpectrum<'a> {
    scale: CatalogScale,
    coefficients: Vec<Vec<f32>>,
    epochs: &'a [f64],
    time_mjd: f64,
    interpolation: Curve,
}

impl FluxCatalog {
    pub(crate) fn open(scale: CatalogScale) -> MsResult<Self> {
        let candidates = MeasuresRuntime::discover_candidates().map_err(invalid)?;
        let roots: Vec<_> = candidates
            .into_iter()
            .map(|candidate| candidate.path.join("nrao/VLA/standards"))
            .collect();
        let sources = open_table("fluxcalibrator.data", &roots)?;
        let coefficients = open_table(scale.table(), &roots)?;
        let epochs = read_epochs(&coefficients)?;
        let name = sources.column_accessor("Name")?;
        let aliases = sources.column_accessor("AltNames")?;
        let ra = sources.column_accessor("RA_J2000")?;
        let dec = sources.column_accessor("Dec_J2000")?;
        let sources = (0..sources.row_count())
            .map(|row| {
                let ScalarValue::String(name) = name.scalar_cell(row)? else {
                    return Err(invalid("flux calibrator Name must be a string"));
                };
                let ArrayValue::String(aliases) = aliases.array_cell(row)? else {
                    return Err(invalid("flux calibrator AltNames must be a string array"));
                };
                let (ScalarValue::Float64(ra), ScalarValue::Float64(dec)) =
                    (ra.scalar_cell(row)?, dec.scalar_cell(row)?)
                else {
                    return Err(invalid(
                        "flux calibrator direction must be in double-precision radians",
                    ));
                };
                Ok(Calibrator {
                    name: name.clone(),
                    aliases: aliases.iter().map(|s| s.to_ascii_uppercase()).collect(),
                    ra: *ra,
                    dec: *dec,
                })
            })
            .collect::<MsResult<_>>()?;
        Ok(Self {
            scale,
            sources,
            coefficients,
            epochs,
        })
    }

    pub(crate) fn spectrum(
        &self,
        field: &str,
        direction: &MDirection,
        time_mjd: f64,
        interpolation: Curve,
    ) -> MsResult<Option<CalibratorSpectrum<'_>>> {
        let field = field.to_ascii_uppercase();
        let source = self
            .sources
            .iter()
            .find(|source| {
                source
                    .aliases
                    .iter()
                    .any(|alias| !alias.is_empty() && field.contains(alias))
            })
            .or_else(|| self.sources.iter().find(|source| source.is_near(direction)));
        let Some(source) = source else {
            return Ok(None);
        };
        let name = if source.name == "0133-3629" {
            "J0133-3629"
        } else {
            &source.name
        };
        let column_name = format!("{name}_coeffs");
        if self
            .coefficients
            .schema()
            .and_then(|schema| schema.column(&column_name))
            .is_none()
        {
            return Ok(None);
        }
        let column = self.coefficients.column_accessor(&column_name)?;
        let variable =
            self.scale.time_interpolation() && matches!(name, "3C48" | "3C138" | "3C147");
        let count = if variable { self.epochs.len() } else { 1 };
        if variable && count < 4 {
            return Err(invalid(format!(
                "flux standard for {name} needs four distinct epochs"
            )));
        }
        let coefficients = (0..count)
            .map(|row| match column.array_cell(row)? {
                ArrayValue::Float32(coefficients) if !coefficients.is_empty() => {
                    Ok(coefficients.iter().copied().collect())
                }
                _ => Err(invalid(format!(
                    "invalid flux calibrator coefficients for {name}"
                ))),
            })
            .collect::<MsResult<_>>()?;
        Ok(Some(CalibratorSpectrum {
            scale: self.scale,
            coefficients,
            epochs: &self.epochs[..count],
            time_mjd,
            interpolation,
        }))
    }
}

impl Calibrator {
    fn is_near(&self, direction: &MDirection) -> bool {
        let lat = direction.latitude_rad();
        let haversine = ((self.dec - lat) * 0.5).sin().powi(2)
            + self.dec.cos()
                * lat.cos()
                * ((self.ra - direction.longitude_rad()) * 0.5).sin().powi(2);
        haversine <= (0.5_f64.to_radians() / 3600.).sin().powi(2)
    }
}

impl CalibratorSpectrum<'_> {
    pub(crate) fn flux(&self, frequency_hz: f64) -> f64 {
        if self.scale == CatalogScale::ScaifeHeald2012 {
            let log_freq = (frequency_hz / 150e6).log10();
            let coefficients = &self.coefficients[0];
            let log_flux = coefficients[1..]
                .iter()
                .rev()
                .fold(0., |acc, &coefficient| {
                    (acc + f64::from(coefficient)) * log_freq
                });
            return f64::from(coefficients[0]) * 10_f64.powf(log_flux);
        }
        let log_freq = (frequency_hz * 1e-9).log10();
        let value = |row: usize| {
            let log_flux = self.coefficients[row]
                .iter()
                .rev()
                .fold(0., |acc, &coefficient| {
                    acc * log_freq + f64::from(coefficient)
                });
            10_f64.powf(log_flux) as f32
        };
        if self.epochs.len() == 1 {
            return f64::from(value(0));
        }
        if self.interpolation == Curve::Nearest {
            // Interpolate1D selects the previous epoch until half a day
            // before the next one and holds both ends outside the table.
            let next = self.epochs.partition_point(|&epoch| epoch < self.time_mjd);
            let index = match self.epochs.get(next) {
                Some(&epoch) if epoch - self.time_mjd < 0.5 => next,
                _ => next.saturating_sub(1),
            };
            return f64::from(value(index));
        }
        let points = self
            .epochs
            .iter()
            .enumerate()
            .map(|(row, &epoch)| (epoch, value(row), 0.))
            .collect::<Vec<_>>();
        let second = if self.interpolation == Curve::Spline {
            // casacore Interpolate1D leaves the first interior value at
            // its forward-elimination result, as does InterpolateArray1D.
            interpolation::tsys_weight_spline_derivatives(&points)
        } else {
            Vec::new()
        };
        f64::from(
            interpolation::sample_extrapolating(
                &points,
                self.time_mjd,
                self.interpolation,
                &second,
            )
            .0,
        )
    }
}

fn open_table(name: &str, roots: &[PathBuf]) -> MsResult<Table> {
    let local = PathBuf::from(name);
    let path = std::iter::once(local)
        .chain(roots.iter().map(|root| root.join(name)))
        .find(|path| Path::is_dir(path))
        .ok_or_else(|| invalid(format!("VLA flux standard table {name} was not found")))?;
    Ok(Table::open(TableOptions::new(path))?)
}

fn read_epochs(table: &Table) -> MsResult<Vec<f64>> {
    let column = table.column_accessor("Epoch")?;
    let mut epochs = Vec::new();
    let mut days_in_year = 365.;
    for row in 0..table.row_count() {
        let ScalarValue::Float64(epoch) = column.scalar_cell(row)? else {
            return Err(invalid(
                "flux standard Epoch must be a double-precision fractional year",
            ));
        };
        if !epoch.is_finite() {
            return Err(invalid("invalid flux standard epoch"));
        }
        let year = epoch.trunc() as i32;
        let start = Date::from_calendar_date(year, Month::January, 1).map_err(invalid)?;
        if time::util::is_leap_year(year) {
            // The CASA table reader retains 366 for subsequent rows.
            days_in_year = 366.;
        }
        let days = (epoch.fract() * days_in_year) as f32;
        epochs.push(f64::from(start.to_julian_day() - 2_400_001) + f64::from(days));
    }
    if epochs.is_empty() || !epochs.is_sorted_by(|a, b| a < b) {
        return Err(invalid("invalid flux standard epoch grid"));
    }
    Ok(epochs)
}
