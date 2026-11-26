// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fixed and time-dependent calibrator flux polynomials.

use std::path::{Path, PathBuf};

use casa_measures_data::MeasuresRuntime;
use casa_ms::MsResult;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::MDirection;
use casa_types::{ArrayValue, ScalarValue};
use time::{Date, Month};

use crate::ft::invalid;
use crate::interpolation::{self, Curve};

pub mod fixed;
mod sources;
pub use sources::source_direction;

use fixed::{FixedScale, FixedSpectrum};

#[derive(Clone, Copy)]
pub enum FluxScale {
    Table(CatalogScale),
    Fixed(FixedScale),
}

/// Match the descriptive scale strings accepted by the legacy imager.
pub fn canonical_tool_standard(name: &str) -> Option<&'static str> {
    let name = name.to_ascii_lowercase();
    let patterns: &[(&str, &[&str])] = &[
        ("Perley 90", &["perley", "90"]),
        ("Perley-Taylor 95", &["perley", "taylor", "95"]),
        ("Perley-Taylor 99", &["perley", "taylor", "99"]),
        ("Perley-Butler 2010", &["perley", "butler", "10"]),
        ("Perley-Butler 2013", &["perley", "butler", "13"]),
        ("Perley-Butler 2017", &["perley", "butler", "17"]),
        ("Scaife-Heald 2012", &["scaife", "heald", "12"]),
        ("Stevens-Reynolds 2016", &["stevens", "reynolds", "16"]),
        ("Baars", &["baars"]),
        ("Butler-JPL-Horizons 2010", &["jpl"]),
        ("Butler-JPL-Horizons 2010", &["horizons"]),
    ];
    patterns.iter().find_map(|&(standard, words)| {
        words
            .iter()
            .all(|word| name.contains(word))
            .then_some(standard)
    })
}

impl FluxScale {
    pub fn named(name: &str) -> Option<Self> {
        Some(match name {
            "Perley-Butler 2017" => Self::Table(CatalogScale::PerleyButler2017),
            "Perley-Butler 2013" => Self::Table(CatalogScale::PerleyButler2013),
            "Scaife-Heald 2012" => Self::Table(CatalogScale::ScaifeHeald2012),
            "Baars" => Self::Fixed(FixedScale::Baars),
            "Perley 90" => Self::Fixed(FixedScale::Perley90),
            "Perley-Taylor 95" => Self::Fixed(FixedScale::PerleyTaylor95),
            "Perley-Taylor 99" => Self::Fixed(FixedScale::PerleyTaylor99),
            "Perley-Butler 2010" => Self::Fixed(FixedScale::PerleyButler2010),
            "Stevens-Reynolds 2016" => Self::Fixed(FixedScale::StevensReynolds2016),
            _ => return None,
        })
    }

    pub fn time_interpolation(self) -> bool {
        matches!(self, Self::Table(scale) if scale.time_interpolation())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CatalogScale {
    PerleyButler2017,
    PerleyButler2013,
    ScaifeHeald2012,
}

impl CatalogScale {
    fn time_interpolation(self) -> bool {
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

pub struct FluxCatalog {
    cone_table: Option<PathBuf>,
    coefficients: Coefficients,
}

enum Coefficients {
    Table(TabulatedCoefficients),
    Fixed(FixedScale),
}

struct TabulatedCoefficients {
    scale: CatalogScale,
    table: Box<Table>,
    epochs: Vec<f64>,
}

pub enum CatalogMatch<'a> {
    Spectrum(CalibratorSpectrum<'a>),
    UnknownSource,
    MissingCoefficients(&'static str),
}

pub enum CalibratorSpectrum<'a> {
    Table(TabulatedSpectrum<'a>),
    Fixed(FixedSpectrum),
}

pub struct TabulatedSpectrum<'a> {
    scale: CatalogScale,
    coefficients: Vec<Vec<f32>>,
    epochs: &'a [f64],
    time_mjd: f64,
    interpolation: Curve,
}

impl FluxCatalog {
    pub fn open(scale: FluxScale) -> MsResult<Self> {
        let candidates = MeasuresRuntime::discover_candidates().map_err(invalid)?;
        let roots: Vec<_> = candidates
            .into_iter()
            .map(|candidate| candidate.path.join("nrao/VLA/standards"))
            .collect();
        let cone_table = find_table("fluxcalibrator.data", &roots);
        let coefficients = match scale {
            FluxScale::Table(scale) => {
                Coefficients::Table(TabulatedCoefficients::open(scale, &roots)?)
            }
            FluxScale::Fixed(scale) => Coefficients::Fixed(scale),
        };
        Ok(Self {
            cone_table,
            coefficients,
        })
    }

    pub fn standard_name(&self) -> &'static str {
        match &self.coefficients {
            Coefficients::Table(table) => match table.scale {
                CatalogScale::PerleyButler2017 => "Perley-Butler 2017",
                CatalogScale::PerleyButler2013 => "Perley-Butler 2013",
                CatalogScale::ScaifeHeald2012 => "Scaife-Heald 2012",
            },
            Coefficients::Fixed(scale) => match scale {
                FixedScale::Baars => "Baars",
                FixedScale::Perley90 => "Perley 90",
                FixedScale::PerleyTaylor95 => "Perley-Taylor 95",
                FixedScale::PerleyTaylor99 => "Perley-Taylor 99",
                FixedScale::PerleyButler2010 => "Perley-Butler 2010",
                FixedScale::StevensReynolds2016 => "Stevens-Reynolds 2016",
            },
        }
    }

    pub fn missing_coefficients(&self, source: &str) -> casa_ms::MsError {
        let table = match &self.coefficients {
            Coefficients::Table(coefficients) => coefficients.table.path().map_or_else(
                || coefficients.scale.table().into(),
                |path| path.display().to_string(),
            ),
            Coefficients::Fixed(_) => "the requested standard".into(),
        };
        invalid(format!("{source} does not appears to be in {table}"))
    }

    pub fn spectrum(
        &self,
        field: &str,
        direction: &MDirection,
        time_mjd: f64,
        interpolation: Curve,
    ) -> MsResult<CatalogMatch<'_>> {
        let Some(name) = sources::identify(field, direction, self.cone_table.as_deref())? else {
            return Ok(CatalogMatch::UnknownSource);
        };
        match &self.coefficients {
            Coefficients::Table(coefficients) => {
                coefficients.spectrum(name, time_mjd, interpolation)
            }
            Coefficients::Fixed(scale) => Ok(match scale.spectrum(name) {
                Some(spectrum) => CatalogMatch::Spectrum(CalibratorSpectrum::Fixed(spectrum)),
                None => CatalogMatch::UnknownSource,
            }),
        }
    }
}

impl TabulatedCoefficients {
    fn open(scale: CatalogScale, roots: &[PathBuf]) -> MsResult<Self> {
        let path = find_table(scale.table(), roots).ok_or_else(|| {
            invalid(format!(
                "VLA flux standard table {} was not found",
                scale.table()
            ))
        })?;
        let table = Box::new(Table::open(TableOptions::new(path))?);
        let epochs = read_epochs(&table)?;
        Ok(Self {
            scale,
            table,
            epochs,
        })
    }

    fn spectrum(
        &self,
        name: &'static str,
        time_mjd: f64,
        interpolation: Curve,
    ) -> MsResult<CatalogMatch<'_>> {
        let name = if name == "0133-3629" {
            "J0133-3629"
        } else {
            name
        };
        let column_name = format!("{name}_coeffs");
        if self
            .table
            .schema()
            .and_then(|schema| schema.column(&column_name))
            .is_none()
        {
            return Ok(CatalogMatch::MissingCoefficients(name));
        }
        let column = self.table.column_accessor(&column_name)?;
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
        Ok(CatalogMatch::Spectrum(CalibratorSpectrum::Table(
            TabulatedSpectrum {
                scale: self.scale,
                coefficients,
                epochs: &self.epochs[..count],
                time_mjd,
                interpolation,
            },
        )))
    }
}

impl CalibratorSpectrum<'_> {
    pub fn flux(&self, frequency_hz: f64) -> f64 {
        match self {
            Self::Table(spectrum) => spectrum.flux(frequency_hz),
            Self::Fixed(spectrum) => spectrum.flux(frequency_hz),
        }
    }
}

impl TabulatedSpectrum<'_> {
    fn flux(&self, frequency_hz: f64) -> f64 {
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
        let value =
            |row: usize| 10_f64.powf(log_polynomial(&self.coefficients[row], log_freq)) as f32;
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
            interpolation::array_spline_derivatives(&points)
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

fn log_polynomial(coefficients: &[f32], log_frequency: f64) -> f64 {
    coefficients.iter().rev().fold(0., |acc, &coefficient| {
        acc * log_frequency + f64::from(coefficient)
    })
}

fn find_table(name: &str, roots: &[PathBuf]) -> Option<PathBuf> {
    std::iter::once(PathBuf::from(name))
        .chain(roots.iter().map(|root| root.join(name)))
        .find(|path| Path::is_dir(path))
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
