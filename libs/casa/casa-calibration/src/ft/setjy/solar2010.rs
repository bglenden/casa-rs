// SPDX-License-Identifier: LGPL-3.0-or-later
//! The Butler–JPL-Horizons 2010 disk flux standard.

use std::path::{Path, PathBuf};

use casa_components::{ComponentShape, ShapeType, SkyComponent};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::MFrequency;
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ScalarValue, Value};
use casa_measures::MeasuresRuntime;
use casa_ms::{MsError, MsResult};
use casa_tables::{Table, TableOptions};

use crate::ft::{invalid, setjy::sampled_shape};

const LIGHT_SPEED: f64 = 299_792_458.;
const PLANCK: f64 = 6.626_075_5e-34;
const BOLTZMANN: f64 = 1.380_657_798_751_064_7e-23;

pub struct Solar2010 {
    direction: MDirection,
    diameter_rad: f64,
    temperature: Temperature,
}

impl Solar2010 {
    pub fn open(workspace: &Path, body: &str, epoch: &MEpoch, frame: &MeasFrame) -> MsResult<Self> {
        let mut temperature = Temperature::for_body(body).ok_or_else(|| {
            invalid(format!(
                "Solar System object {body:?} is not supported by Butler-JPL-Horizons 2010"
            ))
        })?;
        let path = find_table(workspace, body, epoch, frame)?;
        let table = Table::open(TableOptions::new(path))?;
        let radius = quantity_keyword(&table, "meanrad", "AU")?
            .ok_or_else(|| invalid("ephemeris has no meanrad"))?;
        let interpolation = EphemerisSample::new(&table, epoch.value().as_mjd())?;
        let distance = interpolation.sample(&table, "Rho")?;
        let has_direction = table.schema().is_some_and(|schema| {
            ["RA", "DEC"]
                .iter()
                .all(|name| schema.column(name).is_some())
        });
        let direction = if has_direction {
            MDirection::from_angles(
                interpolation.sample(&table, "RA")?.to_radians(),
                interpolation.sample(&table, "DEC")?.to_radians(),
                DirectionRef::J2000,
            )
        } else {
            MDirection::from_angles(0., std::f64::consts::FRAC_PI_2, DirectionRef::J2000)
        };
        if let Temperature::Ephemeris = temperature {
            temperature = Temperature::Constant(
                quantity_keyword(&table, "T_mean", "K")?
                    .filter(|value| *value > 0.)
                    .ok_or_else(|| {
                        invalid(format!(
                            "the ephemeris has no brightness temperature for {body}"
                        ))
                    })?,
            );
        }
        if !radius.is_finite() || radius <= 0. || !distance.is_finite() || distance <= 0. {
            return Err(invalid("invalid Solar System radius or range"));
        }
        Ok(Self {
            direction,
            diameter_rad: 2. * radius / distance,
            temperature,
        })
    }

    pub fn component(
        &self,
        position: Option<MDirection>,
        frequencies: &[MFrequency],
    ) -> MsResult<(SkyComponent, [f64; 4])> {
        // Explicit FIELD references are retained even at the north pole.
        let direction = position.unwrap_or_else(|| self.direction.clone());
        let shape = ComponentShape::from_parameters(
            ShapeType::Disk,
            direction,
            &[self.diameter_rad, self.diameter_rad, 0.],
            None,
        )
        .map_err(invalid)?;
        let exponent = PLANCK / BOLTZMANN;
        let area = std::f64::consts::PI * (self.diameter_rad * 0.5 / LIGHT_SPEED).powi(2);
        let fluxes = frequencies
            .iter()
            .map(|frequency| {
                let hz = frequency.hz();
                if !hz.is_finite() || hz <= 0. {
                    return Err(invalid(
                        "Solar System flux needs positive finite frequencies",
                    ));
                }
                let temp = self.temperature.at(hz);
                let flux = 2.0e26 * PLANCK * area * hz.powi(3) / (exponent * hz / temp).exp_m1();
                Ok([flux, 0., 0., 0.])
            })
            .collect::<MsResult<Vec<_>>>()?;
        sampled_shape(shape, frequencies, fluxes)
    }
}

fn find_table(
    workspace: &Path,
    body: &str,
    epoch: &MEpoch,
    frame: &MeasFrame,
) -> MsResult<PathBuf> {
    let mut roots = vec![workspace.to_path_buf()];
    for runtime in MeasuresRuntime::discover_candidates().unwrap_or_default() {
        roots.push(runtime.path.join("ephemerides/JPL-Horizons"));
    }
    let prefix = format!("{}_", body.to_ascii_lowercase());
    for root in roots {
        let candidates = std::fs::read_dir(root)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.path().is_dir());
        // CASA accepts the first covering table in directory iteration order.
        for candidate in candidates {
            let name = candidate.file_name();
            let lower = name.to_string_lossy().to_ascii_lowercase();
            let Some(range) = lower
                .strip_prefix(&prefix)
                .and_then(|name| name.strip_suffix(".tab"))
            else {
                continue;
            };
            if covers(range, epoch, frame) {
                return Ok(candidate.path());
            }
        }
    }
    Err(invalid(format!(
        "no JPL-Horizons ephemeris for {body} at MJD {}",
        epoch.value().as_mjd()
    )))
}

fn covers(range: &str, epoch: &MEpoch, frame: &MeasFrame) -> bool {
    let Some(separator) = range
        .get(1..)
        .and_then(|range| range.find('-'))
        .map(|index| index + 1)
    else {
        return false;
    };
    let (start, rest) = range.split_at(separator);
    let rest = &rest[1..];
    let Some(unit) = rest.find(['d', 'h', 'm', 's', 'y']) else {
        return false;
    };
    let (end, time) = rest.split_at(unit);
    let (Ok(start), Ok(end), Ok(unit), Ok(reference)) = (
        start.parse::<f64>(),
        end.parse::<f64>(),
        Unit::new(&time[..1]),
        time[1..].parse::<EpochRef>(),
    ) else {
        return false;
    };
    let Ok(time) = epoch.convert_to(reference, frame) else {
        return false;
    };
    Quantity::new(time.value().as_mjd(), "d")
        .and_then(|date| date.get_value_in(&unit))
        .is_ok_and(|value| (start..=end).contains(&value))
}

fn quantity_keyword(table: &Table, name: &str, unit: &str) -> MsResult<Option<f64>> {
    let Some(Value::Record(record)) = table.keywords().get(name) else {
        return Ok(None);
    };
    let (
        Some(Value::Scalar(ScalarValue::Float64(value))),
        Some(Value::Scalar(ScalarValue::String(input_unit))),
    ) = (record.get("value"), record.get("unit"))
    else {
        return Err(invalid(format!("invalid ephemeris quantity {name}")));
    };
    let unit = Unit::new(unit).map_err(invalid)?;
    Quantity::new(*value, input_unit)
        .and_then(|quantity| quantity.get_value_in(&unit))
        .map(Some)
        .map_err(invalid)
}

struct EphemerisSample {
    indices: [usize; 3],
    offsets: [f64; 2],
    f: f64,
}

impl EphemerisSample {
    fn new(table: &Table, epoch: f64) -> MsResult<Self> {
        let count = table.row_count();
        if count == 0 {
            return Err(invalid("empty JPL-Horizons ephemeris"));
        }
        let column = table.column_accessor("MJD")?;
        let nearest = legacy_sample_center(&column, count, epoch)?;
        let indices = [
            nearest.saturating_sub(1),
            nearest,
            (nearest + 1).min(count - 1),
        ];
        let [a, b, c] = indices.map(|row| scalar(&column, row));
        let [a, b, c] = [a?, b?, c?];
        Ok(Self {
            indices,
            offsets: [b - a, c - b],
            f: epoch - b,
        })
    }

    fn sample(&self, table: &Table, name: &str) -> MsResult<f64> {
        let column = table.column_accessor(name)?;
        let [prior, middle, next] = self.indices.map(|row| scalar(&column, row));
        let (prior, middle, next) = (prior?, middle?, next?);
        let [left, right] = self.offsets;
        let span = left + right;
        if span == 0. {
            return Ok(middle);
        }
        let phase = self.f / span;
        let curvature = if left > 0. && right > 0. {
            span * ((next - middle) / right - (middle - prior) / left)
        } else {
            0.
        };
        Ok(middle + phase * (next - prior + phase * curvature))
    }
}

// The 2010 standard starts from the first grid interval, then doubles or
// halves its stride. It can center the quadratic on either bracketing row.
fn legacy_sample_center(
    column: &casa_tables::TableColumn<'_>,
    count: usize,
    epoch: f64,
) -> MsResult<usize> {
    let first = scalar(column, 0)?;
    let last = scalar(column, count - 1)?;
    if !(first..=last).contains(&epoch) {
        return Err(invalid("observation is outside the JPL-Horizons ephemeris"));
    }
    if epoch == first {
        return Ok(0);
    }
    if epoch == last {
        return Ok(count - 1);
    }
    let mut anchor = 1;
    while anchor < count - 1 && scalar(column, anchor)? == first {
        anchor += 1;
    }
    let interval = (scalar(column, anchor)? - first) / anchor as f64;
    let mut lower = 0_isize;
    let mut upper = (count - 1) as isize;
    let mut row = (((epoch - first) / interval).round_ties_even() as isize).clamp(lower, upper);
    let mut forward = scalar(column, row as usize)? < epoch;
    let mut step = 1;
    for _ in 0..count {
        let earlier = scalar(column, row as usize)? < epoch;
        if earlier {
            lower = lower.max(row);
        } else {
            upper = upper.min(row);
        }
        step = if earlier == forward {
            step * 2
        } else {
            step / 2
        };
        forward = earlier;
        let limit = if forward { upper - row } else { row - lower };
        if step > limit {
            step = (limit - 1).max(0);
        }
        if step == 0 {
            return Ok(row as usize);
        }
        row += if forward { step } else { -step };
    }
    Err(invalid(
        "could not bracket the JPL-Horizons observation epoch",
    ))
}

pub(super) fn scalar(column: &casa_tables::TableColumn<'_>, row: usize) -> Result<f64, MsError> {
    match column.scalar_cell(row)? {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(invalid("expected a double-precision ephemeris column")),
    }
}

enum Temperature {
    Constant(f64),
    Ephemeris,
    Venus,
    Jupiter,
    Uranus,
    Neptune,
}

impl Temperature {
    fn for_body(body: &str) -> Option<Self> {
        Some(match body.to_ascii_lowercase().as_str() {
            "venus" => Self::Venus,
            "jupiter" => Self::Jupiter,
            "uranus" => Self::Uranus,
            "neptune" => Self::Neptune,
            "io" | "ganymede" => Self::Constant(110.),
            "callisto" => Self::Constant(134.),
            "europa" => Self::Constant(109.),
            "titan" => Self::Constant(76.6),
            "triton" => Self::Constant(38.),
            "pluto" => Self::Constant(35.),
            "ceres" => Self::Constant(167.),
            "pallas" => Self::Constant(164.),
            "juno" => Self::Constant(163.),
            "vesta" => Self::Constant(160.),
            "mercury" | "mars" | "victoria" | "davida" => Self::Ephemeris,
            _ => return None,
        })
    }

    fn at(&self, hz: f64) -> f64 {
        let wavelength_cm = 100. * LIGHT_SPEED / hz;
        match self {
            Self::Constant(temperature) => *temperature,
            Self::Ephemeris => {
                unreachable!("the ephemeris temperature was read when opening the model")
            }
            Self::Venus => venus(hz).max(2.7),
            Self::Jupiter => {
                let wave = wavelength_cm.clamp(0.1, 6.2);
                if wave < 0.44 {
                    170.
                } else if wave < 0.7 {
                    160. + 21.537539 * (0.7 / wave).ln()
                } else if wave < 1.3 {
                    130. + 48.462196889 * (1.3 / wave).ln()
                } else {
                    130. + 65.38532335444 * (wave / 1.3).log10()
                }
            }
            Self::Uranus => {
                let wave = wavelength_cm.clamp(0.07, 6.2);
                if wave < 0.4 {
                    90. + 32.46063842 * (10. * wave).ln()
                } else if wave < 1. {
                    135.
                } else {
                    135. + 105. * wave.log10()
                }
            }
            Self::Neptune => {
                let ghz = (hz * 1e-9).clamp(4., 1000.);
                if ghz < 70. {
                    140. - 30.083556662 * (ghz / 70.).ln()
                } else {
                    240. - 34.93815 * (ghz / 4.).ln()
                }
            }
        }
    }
}

fn venus(hz: f64) -> f64 {
    // The published 2010 grid uses Float frequencies and a four-point cubic.
    let ghz = f64::from((hz * 1e-9) as f32);
    let first = VENUS
        .partition_point(|&(freq, _)| f64::from(freq) <= ghz)
        .saturating_sub(2)
        .min(VENUS.len() - 4);
    let samples = &VENUS[first..first + 4];
    samples
        .iter()
        .enumerate()
        .map(|(i, &(x, y))| {
            let x = f64::from(x);
            samples.iter().enumerate().filter(|(j, _)| *j != i).fold(
                y,
                |weight, (_, &(other, _))| {
                    let other = f64::from(other);
                    weight * (ghz - other) / (x - other)
                },
            )
        })
        .sum()
}

const VENUS: [(f32, f64); 75] = [
    (0.303, 520.0),
    (0.333, 525.0),
    (0.366, 530.0),
    (0.403, 535.0),
    (0.443, 540.0),
    (0.487, 545.0),
    (0.536, 550.0),
    (0.59, 555.0),
    (0.649, 560.0),
    (0.714, 565.0),
    (0.785, 570.0),
    (0.864, 575.0),
    (0.95, 580.0),
    (1.045, 586.0),
    (1.149, 592.0),
    (1.264, 598.0),
    (1.391, 604.0),
    (1.53, 610.0),
    (1.683, 617.0),
    (1.851, 624.0),
    (2.036, 631.0),
    (2.24, 638.0),
    (2.464, 646.0),
    (2.71, 655.0),
    (2.981, 664.0),
    (3.28, 671.0),
    (3.608, 676.0),
    (3.968, 679.0),
    (4.365, 680.0),
    (4.802, 680.0),
    (5.282, 679.0),
    (5.81, 676.0),
    (6.391, 671.0),
    (7.03, 665.0),
    (7.733, 657.0),
    (8.507, 648.0),
    (9.357, 638.9),
    (10.293, 626.5),
    (11.322, 613.0),
    (12.454, 598.7),
    (13.7, 583.8),
    (15.07, 568.7),
    (16.577, 553.7),
    (18.235, 538.8),
    (20.058, 524.1),
    (22.064, 509.9),
    (24.27, 496.1),
    (26.697, 482.8),
    (29.367, 469.8),
    (32.304, 457.3),
    (35.534, 445.3),
    (39.087, 433.8),
    (42.996, 422.8),
    (47.296, 412.3),
    (52.025, 402.3),
    (57.228, 392.7),
    (62.951, 383.6),
    (69.246, 374.9),
    (76.17, 366.7),
    (83.787, 358.7),
    (92.166, 351.1),
    (101.383, 343.7),
    (111.521, 336.6),
    (122.673, 329.6),
    (134.94, 322.8),
    (148.434, 316.1),
    (163.278, 309.7),
    (179.605, 303.5),
    (197.566, 297.6),
    (217.322, 292.1),
    (239.055, 286.8),
    (262.96, 282.0),
    (289.256, 277.7),
    (318.182, 273.8),
    (350.0, 270.2),
];
