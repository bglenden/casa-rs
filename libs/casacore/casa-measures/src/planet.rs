// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction of a named planet from the casacore JPL reference table.

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MeasFrame};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};

use super::{MeasuresDataError, MeasuresRuntime};

/// Casacore's default DE200 coefficients, read one time block at a time.
pub struct PlanetaryEphemeris {
    ephemeris: PlanetaryTable,
    body: usize,
}

/// Casacore's DE200 table shared across directions to different bodies.
#[derive(Debug)]
pub struct PlanetaryTable {
    table: Table,
    coefficients: [[usize; 13]; 3],
    first_mjd: f64,
    step_days: f64,
    au_km: f64,
    light_speed: f64,
    sun_mass: f64,
    sun_radius: f64,
    earth_moon_ratio: f64,
}

impl PlanetaryEphemeris {
    /// Bind the named body to the discovered runtime's DE200 table.
    pub fn open_discovered(name: &str) -> Result<Self, MeasuresDataError> {
        Ok(Self {
            body: body_index(&name.to_ascii_uppercase())?,
            ephemeris: PlanetaryTable::open_discovered()?,
        })
    }

    /// Evaluate the light-time and parallax corrected direction at the
    /// epoch and observatory in the frame.
    pub fn direction(&self, frame: &MeasFrame) -> Result<MDirection, MeasuresDataError> {
        let (direction, distance) = self.ephemeris.geocentric(self.body, None, frame)?;
        topocentric(direction, distance * self.ephemeris.au_km * 1000., frame)
    }
}

fn body_index(name: &str) -> Result<usize, MeasuresDataError> {
    Ok(match name {
        "MERCURY" => 0,
        "VENUS" => 1,
        "MARS" => 3,
        "JUPITER" => 4,
        "SATURN" => 5,
        "URANUS" => 6,
        "NEPTUNE" => 7,
        "PLUTO" => 8,
        "MOON" => 9,
        "SUN" => 10,
        _ => {
            return Err(invalid(format!(
                "unknown CASA planetary direction {name:?}"
            )));
        }
    })
}

impl PlanetaryTable {
    /// Open the DE200 table in the configured measures runtime.
    pub fn open_discovered() -> Result<Self, MeasuresDataError> {
        let runtime = MeasuresRuntime::open_discovered(Default::default())?;
        let table = Table::open(TableOptions::new(runtime.root().join("ephemerides/DE200")))?;
        let keys = table.keywords();
        let first_mjd = positive(keys, "MJD0")?;
        let step_days = positive(keys, "dMJD")?;
        let au_km = positive(keys, "AU")?;
        let light_speed = 86400. * positive(keys, "CLIGHT")? / au_km;
        let sun_mass = positive(keys, "GMS")? / light_speed.powi(2);
        let sun_radius = positive(keys, "RADS").or_else(|_| positive(keys, "ASUN"))? / au_km;
        let earth_moon_ratio = 1. + positive(keys, "EMRAT")?;
        let Some(Value::Array(ArrayValue::Int32(layout))) = table
            .column_keywords("x")
            .and_then(|keys| keys.get("Description"))
        else {
            return Err(invalid("missing JPL coefficient description"));
        };
        let layout = layout
            .as_slice()
            .filter(|layout| layout.len() == 39)
            .ok_or_else(|| invalid("invalid JPL coefficient description"))?;
        let mut coefficients = [[0; 13]; 3];
        for (row, indices) in coefficients.iter_mut().enumerate() {
            for (column, index) in indices.iter_mut().enumerate() {
                *index = usize::try_from(layout[13 * row + column])
                    .map_err(|_| invalid("negative JPL coefficient index"))?;
            }
        }
        Ok(Self {
            table,
            coefficients,
            first_mjd,
            step_days,
            au_km,
            light_speed,
            sun_mass,
            sun_radius,
            earth_moon_ratio,
        })
    }

    /// Resolve a solar-system direction in the geocentric JNAT reference
    /// and return its geocentric parallax distance in metres.
    ///
    /// The frame supplies the epoch. A direction with a latitude of +90°
    /// denotes the default position of the body. Otherwise the direction's
    /// angles shift its DE200 position before solar deflection, as in
    /// casacore's `MCDirection::R_PLANET`.
    pub fn geocentric_direction(
        &self,
        model: &MDirection,
        frame: &MeasFrame,
    ) -> Result<(MDirection, f64), MeasuresDataError> {
        let body = body_index(model.refer().as_str())?;
        let offset = (model.cosines()[2] != 1.).then(|| model.as_angles());
        let (direction, distance_au) = self.geocentric(body, offset, frame)?;
        Ok((
            MDirection::from_cosines(direction, DirectionRef::JNAT),
            distance_au * self.au_km * 1000.0,
        ))
    }

    fn geocentric(
        &self,
        body_index: usize,
        offset: Option<(f64, f64)>,
        frame: &MeasFrame,
    ) -> Result<([f64; 3], f64), MeasuresDataError> {
        let epoch = frame
            .epoch()
            .ok_or_else(|| invalid("planetary direction needs an epoch"))?;
        let time = epoch
            .convert_to(EpochRef::TDB, frame)
            .map_err(scientific_error)?
            .value()
            .as_mjd();
        let emb = self.position(2, time)?;
        let moon = self.position(9, time)?;
        let earth = std::array::from_fn(|axis| emb[axis] - moon[axis] / self.earth_moon_ratio);
        let earth_sun = subtract(earth, self.position(10, time)?);
        let earth_distance = norm(earth_sun);
        let mut light_time = 0.;
        for _ in 0..16 {
            let emission = time - light_time;
            let mut body = self.position(body_index, emission)?;
            if body_index == 9 {
                let barycentre = self.position(2, emission)?;
                body = std::array::from_fn(|axis| body[axis] + barycentre[axis]);
            }
            let body_sun = subtract(body, self.position(10, emission)?);
            let delta = subtract(body, earth);
            let distance = norm(delta);
            let elongation_sum = earth_distance + norm(body_sun);
            let delay = if body_index == 10 {
                distance
            } else {
                distance
                    + 2. * self.sun_mass
                        * ((elongation_sum + distance) / (elongation_sum - distance)).ln()
            } / self.light_speed;
            if (delay - light_time).abs() <= 1e-9 * delay {
                let mut direction = delta.map(|coordinate| coordinate / distance);
                if let Some((longitude_offset, latitude_offset)) = offset {
                    let [x, y, z] = direction;
                    direction = MDirection::from_angles(
                        y.atan2(x) + longitude_offset,
                        z.atan2(x.hypot(y)) + latitude_offset,
                        DirectionRef::JNAT,
                    )
                    .cosines();
                }
                let earth_unit = earth_sun.map(|coordinate| coordinate / earth_distance);
                let body_distance = norm(body_sun);
                let projection = dot(direction, earth_unit);
                if body_index != 10
                    && body_distance > 0.
                    && (projection - 1.).abs() > 1. - (self.sun_radius / earth_distance).cos()
                {
                    let body_unit = body_sun.map(|coordinate| coordinate / body_distance);
                    let scale =
                        2. * self.sun_mass / (earth_distance * (1. + dot(body_unit, earth_unit)));
                    let forward = dot(direction, body_unit);
                    direction = std::array::from_fn(|axis| {
                        direction[axis]
                            + scale * (forward * earth_unit[axis] - projection * body_unit[axis])
                    });
                }
                return Ok((direction, distance));
            }
            light_time = delay;
        }
        Err(invalid("JPL light-time calculation did not converge"))
    }

    /// Evaluate a DE200 Chebyshev series. Table positions are in km.
    fn position(&self, body: usize, time: f64) -> Result<[f64; 3], MeasuresDataError> {
        let block = (time - self.first_mjd) / self.step_days;
        if !(block >= 1. && block < self.table.row_count() as f64 + 1.) {
            return Err(invalid(format!(
                "planetary epoch MJD {time} is outside DE200"
            )));
        }
        let cell = self.table.cell_accessor(block as usize - 1, "x")?;
        let ArrayValue::Float64(values) = cell.array()? else {
            return Err(invalid("invalid JPL coefficient column"));
        };
        let [offset, order, subdivisions] = self.coefficients.map(|row| row[body]);
        if !(1..=18).contains(&order) || subdivisions == 0 || offset < 3 {
            return Err(invalid("invalid JPL coefficients for body"));
        }
        let subinterval = block.fract() * subdivisions as f64;
        let x = 2. * subinterval.fract() - 1.;
        let start = offset - 3 + 3 * order * subinterval as usize;
        let values = values
            .as_slice()
            .and_then(|v| v.get(start..start + 3 * order))
            .ok_or_else(|| invalid("truncated JPL data row"))?;
        let mut chebyshev = [0.; 18];
        chebyshev[0] = 1.;
        chebyshev[1] = x;
        for n in 2..order {
            chebyshev[n] = 2. * x * chebyshev[n - 1] - chebyshev[n - 2];
        }
        Ok(std::array::from_fn(|axis| {
            (0..order)
                .rev()
                .map(|n| chebyshev[n] * values[axis * order + n])
                .sum::<f64>()
                / self.au_km
        }))
    }
}

fn topocentric(
    direction: [f64; 3],
    distance_m: f64,
    frame: &MeasFrame,
) -> Result<MDirection, MeasuresDataError> {
    let apparent = MDirection::from_cosines(direction, DirectionRef::JNAT)
        .convert_to(DirectionRef::APP, frame)
        .map_err(scientific_error)?;
    let position = frame
        .position()
        .ok_or_else(|| invalid("planetary direction needs an observatory"))?;
    let [x, y, z] = position.as_itrf();
    let radius = x.hypot(y);
    let epoch = frame.epoch().expect("planetary epoch");
    let last = epoch
        .convert_to(EpochRef::LAST, frame)
        .map_err(scientific_error)?
        .value()
        .as_mjd()
        .fract();
    let (sin_last, cos_last) = (std::f64::consts::TAU * last).sin_cos();
    let offset = [radius * cos_last, radius * sin_last, z].map(|axis| axis / distance_m);
    Ok(MDirection::from_cosines(
        subtract(apparent.cosines(), offset),
        DirectionRef::APP,
    ))
}

fn subtract(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    std::array::from_fn(|axis| a[axis] - b[axis])
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a.into_iter().zip(b).map(|(a, b)| a * b).sum()
}

fn norm(a: [f64; 3]) -> f64 {
    a[0].hypot(a[1]).hypot(a[2])
}

fn positive(record: &RecordValue, key: &str) -> Result<f64, MeasuresDataError> {
    match record.get(key) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if value.is_finite() && *value > 0. => {
            Ok(*value)
        }
        _ => Err(invalid(format!("invalid JPL keyword {key}"))),
    }
}

fn invalid(reason: impl Into<String>) -> MeasuresDataError {
    MeasuresDataError::MissingData(reason.into())
}

fn scientific_error(error: casa_core::measures::MeasureError) -> MeasuresDataError {
    invalid(error.to_string())
}
