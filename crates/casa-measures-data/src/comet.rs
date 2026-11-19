// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the fixed sampling grid of a casacore MeasComet table.

use std::path::{Path, PathBuf};

use casa_tables::ColumnType;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{PrimitiveType, ScalarValue, Value};

use crate::{DEFAULT_MEASURES_ENV, MeasuresDataError, RuntimeTable, default_measures_path};

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
    mean_radius_au: Option<f64>,
    bracket: Option<Bracket>,
}

#[derive(Debug)]
struct Bracket {
    index: usize,
    start_mjd: f64,
    start_au: [f64; 3],
    end_au: [f64; 3],
}

impl CometTable {
    /// Open an explicit table or a path relative to the detected runtime's
    /// ephemerides or geodetic directory, using casacore's fixed-grid metadata.
    pub fn open(path: &Path) -> Result<Self, MeasuresDataError> {
        let path = resolve(path)?;
        let table = RuntimeTable::open(&path)?;
        let invalid = |reason| {
            MeasuresDataError::TableRead(format!(
                "invalid comet table {}: {reason}",
                path.display()
            ))
        };
        let keywords = table.table.keywords();
        let version_date_valid = match keywords.get("VS_DATE") {
            Some(Value::Scalar(ScalarValue::String(value))) => {
                Quantity::read(value).is_ok_and(|date| date.unit().name() == "d")
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
        let schema = table.table.schema();
        if REQUIRED_COLUMNS.iter().any(|name| {
            !schema
                .and_then(|schema| schema.column(name))
                .is_some_and(|column| {
                    column.column_type() == &ColumnType::Scalar
                        && column.data_type() == Some(PrimitiveType::Float64)
                })
        }) {
            return Err(invalid(
                "expected double MJD, RA, DEC, Rho and RadVel columns",
            ));
        }
        let number = |name| keyword_double(keywords.get(name)).ok_or_else(|| invalid(name));
        let name = match keywords.get("NAME") {
            Some(Value::Scalar(ScalarValue::String(name))) => name.clone(),
            _ => return Err(invalid("NAME")),
        };
        let origin_mjd = number("MJD0")?;
        let step_days = number("dMJD")?;
        let count = table.row_count();
        if origin_mjd < 10_000.0
            || step_days <= 0.0
            || count == 0
            || (scalar(&table, "MJD", count - 1)? - (origin_mjd + count as f64 * step_days)).abs()
                > step_days * 0.1
        {
            return Err(invalid("invalid MJD sampling grid"));
        }

        let distance_km = number("GeoDist")?;
        let longitude = number("GeoLong")?.to_radians();
        let latitude = number("GeoLat")?.to_radians();
        let is_topocentric = match keywords.get("posrefsys") {
            Some(Value::Scalar(ScalarValue::String(reference))) => {
                let reference = reference.to_ascii_uppercase();
                !["J2000", "B1950", "APP", "ICRS"]
                    .iter()
                    .any(|code| reference.contains(code))
                    && reference.contains("TOPO")
            }
            None => distance_km != 0.0,
            _ => return Err(invalid("posrefsys")),
        };
        let topocentre_m = is_topocentric.then(|| {
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

        let mean_radius_au = match keywords.get("meanrad") {
            Some(Value::Record(record)) => {
                match (keyword_double(record.get("value")), record.get("unit")) {
                    (Some(value), Some(Value::Scalar(ScalarValue::String(unit)))) => {
                        Quantity::new(value, unit)
                            .and_then(|radius| radius.get_value_in(&Unit::new("AU")?))
                            .ok()
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
            mean_radius_au,
            bracket: None,
        })
    }

    /// The object's name from the table keyword.
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn distance_au(&mut self, mjd: f64) -> Result<Option<f64>, MeasuresDataError> {
        let index = ((mjd - self.origin_mjd) / self.step_days).floor() - 1.0;
        if !(0.0..(self.table.row_count() - 1) as f64).contains(&index) {
            return Ok(None);
        }
        let index = index as usize;
        if self
            .bracket
            .as_ref()
            .is_none_or(|bracket| bracket.index != index)
        {
            self.bracket = Some(Bracket {
                index,
                start_mjd: scalar(&self.table, "MJD", index)?,
                start_au: position(&self.table, index)?,
                end_au: position(&self.table, index + 1)?,
            });
        }
        let bracket = self.bracket.as_ref().expect("comet bracket is loaded");
        let fraction = (mjd - bracket.start_mjd) / self.step_days;
        let xyz: [f64; 3] = std::array::from_fn(|i| {
            bracket.start_au[i] + fraction * (bracket.end_au[i] - bracket.start_au[i])
        });
        Ok(Some(
            xyz.iter()
                .map(|component| component * component)
                .sum::<f64>()
                .sqrt(),
        ))
    }
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

fn resolve(path: &Path) -> Result<PathBuf, MeasuresDataError> {
    if path.is_dir() {
        return Ok(path.to_owned());
    }
    if !path.as_os_str().is_empty() && path.is_relative() {
        // MeasIERS::findTab checks only the requested table in these two
        // directories. Opening an ephemeris needs no other measures catalogs.
        let root = match std::env::var_os(DEFAULT_MEASURES_ENV) {
            Some(root) => PathBuf::from(root),
            None => default_measures_path()?,
        };
        for directory in ["ephemerides", "geodetic"] {
            let table = root.join(directory).join(path);
            if table.is_dir() {
                return Ok(table);
            }
        }
    }
    Err(MeasuresDataError::MissingData(format!(
        "comet table {}",
        path.display()
    )))
}

fn scalar(table: &RuntimeTable, name: &str, row: usize) -> Result<f64, MeasuresDataError> {
    match table.table.column_accessor(name)?.scalar_cell(row)? {
        ScalarValue::Float64(value) if value.is_finite() => Ok(*value),
        _ => Err(MeasuresDataError::TableRead(format!(
            "comet column {name} row {row} must contain a finite double"
        ))),
    }
}

fn position(table: &RuntimeTable, row: usize) -> Result<[f64; 3], MeasuresDataError> {
    let distance = scalar(table, "Rho", row)?;
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
