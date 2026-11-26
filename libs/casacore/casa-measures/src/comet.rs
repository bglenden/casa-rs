// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the fixed sampling grid of a casacore MeasComet table.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::quanta::{Quantity, Unit, UnitVal, casacore};
use casa_core::{PrimitiveType, ScalarValue, Value};
use casa_tables::ColumnType;

use crate::{MeasuresDataError, RuntimeTable};

const REQUIRED_COLUMNS: [&str; 5] = ["MJD", "RA", "DEC", "Rho", "RadVel"];

/// A read-only comet ephemeris. It reads positions on demand and reuses the
/// two consecutive rows that bracket the last observation.
#[derive(Debug)]
pub struct CometTable {
    table: RuntimeTable,
    name: String,
    origin_mjd: f64,
    step_days: f64,
    topocentre_m: Option<[f64; 3]>,
    table_reference: DirectionRef,
    input_reference: DirectionRef,
    mean_radius_au: Option<f64>,
    au_m: f64,
    zero_range_au: f64,
    bracket: Mutex<Option<Bracket>>,
}

#[derive(Debug)]
struct Bracket {
    index: usize,
    start_mjd: f64,
    start_au: [f64; 3],
    end_au: [f64; 3],
}

/// A direction and distance interpolated on the native comet table grid.
#[derive(Debug)]
pub struct CometDirection {
    /// The interpolation expressed in the table's conversion axes.
    pub direction: MDirection,
    /// The length of the interpolated position, in metres.
    pub distance_m: f64,
    /// Casacore first removes observer parallax unless the table is APP.
    pub remove_parallax: bool,
}

impl CometTable {
    /// Open the requested casacore table and validate its fixed-grid metadata.
    pub fn open(path: &Path) -> Result<Self, MeasuresDataError> {
        let table = RuntimeTable::open(path)?;
        let invalid = |reason| {
            MeasuresDataError::TableRead(format!(
                "invalid comet table {}: {reason}",
                path.display()
            ))
        };
        let keywords = table.table.keywords();
        let version_date_valid = match keywords.get("VS_DATE") {
            Some(Value::Scalar(ScalarValue::String(value))) => {
                Quantity::read(value).is_ok_and(|date| {
                    let unit = date.unit().val();
                    [UnitVal::NODIM, UnitVal::TIME, UnitVal::ANGLE]
                        .iter()
                        .any(|valid| unit.conformant(valid))
                })
            }
            _ => false,
        };
        if table.table.info().table_type != "IERS"
            || !version_date_valid
            || !matches!(
                keywords.get("VS_VERSION"),
                Some(Value::Scalar(ScalarValue::String(_)))
            )
            || keywords.get("VS_CREATE").is_none()
            || keywords.get("VS_TYPE").is_none()
        {
            return Err(invalid("invalid IERS table metadata"));
        }
        let column = |name| table.table.schema().and_then(|schema| schema.column(name));
        let scalar_double = |column: &casa_tables::ColumnSchema| {
            column.column_type() == &ColumnType::Scalar
                && column.data_type() == Some(PrimitiveType::Float64)
        };
        if REQUIRED_COLUMNS
            .iter()
            .any(|name| !column(name).is_some_and(scalar_double))
            || ["DiskLong", "DiskLat"]
                .iter()
                .any(|name| column(name).is_some_and(|col| !scalar_double(col)))
        {
            return Err(invalid("expected double comet columns"));
        }
        let number = |name| keyword_double(keywords.get(name)).ok_or_else(|| invalid(name));
        let name = match keywords.get("NAME") {
            Some(Value::Scalar(ScalarValue::String(name))) => name.clone(),
            _ => return Err(invalid("NAME")),
        };
        let origin_mjd = number("MJD0")?;
        let step_days = number("dMJD")?;
        let count = table.row_count();
        if origin_mjd < 10_000.0 || step_days <= 0.0 || count == 0 {
            return Err(invalid("invalid MJD sampling grid"));
        }
        let last_mjd = scalar(&table, "MJD", count - 1)?;
        if !last_mjd.is_finite()
            || (last_mjd - (origin_mjd + count as f64 * step_days)).abs() > step_days * 0.1
        {
            return Err(invalid("invalid MJD sampling grid"));
        }

        let distance_km = number("GeoDist")?;
        let longitude = number("GeoLong")?.to_radians();
        let latitude = number("GeoLat")?.to_radians();
        let explicit = match keywords.get("posrefsys") {
            Some(Value::Scalar(ScalarValue::String(reference))) => Some(comet_reference(reference)),
            None => None,
            _ => return Err(invalid("posrefsys")),
        };
        let table_reference = explicit.unwrap_or(if distance_km != 0.0 {
            DirectionRef::TOPO
        } else {
            DirectionRef::APP
        });
        let topocentre_m = (table_reference == DirectionRef::TOPO).then(|| {
            cartesian(
                if distance_km == 0.0 {
                    1e-6
                } else {
                    distance_km * 1e3
                },
                longitude,
                latitude,
            )
        });

        let au = Unit::new("AU").map_err(|_| invalid("invalid AU unit"))?;
        let mean_radius_au = match keywords.get("meanrad") {
            Some(Value::Record(record)) => {
                match (keyword_double(record.get("value")), record.get("unit")) {
                    (Some(value), Some(Value::Scalar(ScalarValue::String(unit)))) => {
                        Quantity::new(value, unit)
                            .ok()
                            .and_then(|radius| casacore::convert(&radius, Some(&au)).ok())
                            .map(|radius| radius.value())
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        Ok(Self {
            table,
            name,
            origin_mjd,
            step_days,
            topocentre_m,
            table_reference,
            input_reference: explicit.unwrap_or(DirectionRef::APP),
            mean_radius_au,
            au_m: au.val().factor,
            zero_range_au: 1e-6 / au.val().factor,
            bracket: Mutex::new(None),
        })
    }

    /// The object's name from the table keyword.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The first and last sample dates on the table's native MJD grid.
    pub fn span_mjd(&self) -> [f64; 2] {
        [
            self.origin_mjd + self.step_days,
            self.origin_mjd + self.table.row_count() as f64 * self.step_days,
        ]
    }

    /// Cartesian topocentre in metres, when the ephemeris uses TOPO.
    pub fn topocentre_m(&self) -> Option<[f64; 3]> {
        self.topocentre_m
    }

    /// Mean physical radius, when the table provides it.
    pub fn mean_radius_au(&self) -> Option<f64> {
        self.mean_radius_au
    }

    /// Interpolate the geocentric or topocentric position on the table's
    /// native MJD grid and return its norm in AU. The last row is the upper
    /// endpoint of the last bracket and is excluded as an observation date.
    pub fn distance_au(&self, mjd: f64) -> Result<Option<f64>, MeasuresDataError> {
        self.position_au(mjd).map(|position| position.map(norm))
    }

    /// Interpolate the comet at the epoch expressed as TDB MJD. Casacore
    /// uses this value directly as the coordinate on the table's native grid.
    pub fn direction_at(&self, mjd: f64) -> Result<Option<CometDirection>, MeasuresDataError> {
        let Some(xyz) = self.position_au(mjd)? else {
            return Ok(None);
        };
        Ok(Some(CometDirection {
            direction: MDirection::from_cosines(xyz, self.input_reference),
            distance_m: norm(xyz) * self.au_m,
            remove_parallax: self.table_reference != DirectionRef::APP,
        }))
    }

    /// Interpolate the radial velocity in metres per second on the native
    /// comet table grid. Returns `None` outside the interpolation span.
    ///
    /// # Errors
    ///
    /// Returns a data error when the bracketing rows cannot be read.
    pub fn radial_velocity_m_s(&self, mjd: f64) -> Result<Option<f64>, MeasuresDataError> {
        self.within_bracket(mjd, |bracket, fraction| {
            let first = scalar(&self.table, "RadVel", bracket.index)?;
            let last = scalar(&self.table, "RadVel", bracket.index + 1)?;
            Ok((first + fraction * (last - first)) * self.au_m / 86_400.)
        })?
        .transpose()
    }

    fn position_au(&self, mjd: f64) -> Result<Option<[f64; 3]>, MeasuresDataError> {
        self.within_bracket(mjd, |bracket, fraction| {
            std::array::from_fn(|i| {
                bracket.start_au[i] + fraction * (bracket.end_au[i] - bracket.start_au[i])
            })
        })
    }

    fn within_bracket<T>(
        &self,
        mjd: f64,
        sample: impl FnOnce(&Bracket, f64) -> T,
    ) -> Result<Option<T>, MeasuresDataError> {
        let index = ((mjd - self.origin_mjd) / self.step_days).floor() - 1.0;
        if !(0.0..(self.table.row_count() - 1) as f64).contains(&index) {
            return Ok(None);
        }
        let index = index as usize;
        let mut cache = self.bracket.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.as_ref().is_none_or(|bracket| bracket.index != index) {
            *cache = Some(Bracket {
                index,
                start_mjd: scalar(&self.table, "MJD", index)?,
                start_au: position(&self.table, index, self.zero_range_au)?,
                end_au: position(&self.table, index + 1, self.zero_range_au)?,
            });
        }
        let bracket = cache.as_ref().expect("comet bracket is loaded");
        let fraction = (mjd - bracket.start_mjd) / self.step_days;
        Ok(Some(sample(bracket, fraction)))
    }
}

fn comet_reference(reference: &str) -> DirectionRef {
    let reference = reference.to_ascii_uppercase();
    [
        DirectionRef::J2000,
        DirectionRef::B1950,
        DirectionRef::APP,
        DirectionRef::ICRS,
        DirectionRef::TOPO,
    ]
    .into_iter()
    .find(|code| reference.contains(code.as_str()))
    .unwrap_or(DirectionRef::APP)
}

fn norm(xyz: [f64; 3]) -> f64 {
    xyz.iter().map(|part| part * part).sum::<f64>().sqrt()
}

// RecordInterface::asDouble promotes integer and float scalar keywords.
fn keyword_double(value: Option<&Value>) -> Option<f64> {
    let number = match value? {
        Value::Scalar(ScalarValue::UInt8(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Int16(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Int32(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::UInt32(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Int64(value)) => *value as f64,
        Value::Scalar(ScalarValue::Float32(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Float64(value)) => *value,
        _ => return None,
    };
    number.is_finite().then_some(number)
}

fn scalar(table: &RuntimeTable, name: &str, row: usize) -> Result<f64, MeasuresDataError> {
    match table.table.column_accessor(name)?.scalar_cell(row)? {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(MeasuresDataError::TableRead(format!(
            "comet column {name} row {row} must contain a double"
        ))),
    }
}

fn position(
    table: &RuntimeTable,
    row: usize,
    zero_range_au: f64,
) -> Result<[f64; 3], MeasuresDataError> {
    let range = scalar(table, "Rho", row)?;
    // MVPosition substitutes a micrometre when readjusting a zero radius.
    let distance = if range == 0.0 { zero_range_au } else { range };
    let longitude = scalar(table, "RA", row)?.to_radians();
    let latitude = scalar(table, "DEC", row)?.to_radians();
    Ok(cartesian(distance, longitude, latitude))
}

fn cartesian(distance: f64, longitude: f64, latitude: f64) -> [f64; 3] {
    let (sin_lon, cos_lon) = longitude.sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    [
        distance * cos_lat * cos_lon,
        distance * cos_lat * sin_lon,
        distance * sin_lat,
    ]
}
