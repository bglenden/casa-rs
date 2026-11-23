// SPDX-License-Identifier: LGPL-3.0-or-later
//! MeasurementSet measure column metadata used when binding MSCAL geometry.

use std::collections::BTreeMap;
use std::str::FromStr;

use casa_tables::Table;
use casa_tables::table_measures::{MeasRefDesc, MeasureType, TableMeasDesc};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_tables::taql::TaqlError;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::position::{MPosition, PositionRef};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::{MsError, MsResult};

pub(super) struct TimeColumn {
    reference: Reference<EpochRef>,
    seconds_per_unit: f64,
}

impl TimeColumn {
    pub(super) fn read(table: &Table) -> MsResult<Self> {
        Ok(Self {
            reference: Reference::read(table, "TIME", MeasureType::Epoch)?,
            seconds_per_unit: unit_scales(table, "TIME", ["s"])?[0],
        })
    }

    pub(super) fn epoch(&self, row: &RecordValue) -> Result<MEpoch, TaqlError> {
        let Some(Value::Scalar(ScalarValue::Float64(time))) = row.get("TIME") else {
            return Err(TaqlError::Table(
                "MSCAL requires a double TIME column".into(),
            ));
        };
        Ok(MEpoch::from_mjd_seconds(
            time * self.seconds_per_unit,
            self.reference.at(row)?,
        ))
    }
}

pub(super) fn directions(table: &Table, column: &str) -> MsResult<Vec<MDirection>> {
    let reference = Reference::<DirectionRef>::read(table, column, MeasureType::Direction)?;
    let scale = unit_scales(table, column, ["rad"; 2])?;
    let values = table.column_accessor(column)?;
    (0..table.row_count())
        .map(|index| {
            let ArrayValue::Float64(angles) = values.array_cell(index)? else {
                return Err(invalid(format!("FIELD {column} requires a double array")));
            };
            let shape = angles.shape();
            if shape.len() < 2 || shape[0] != 2 || angles.len() < 2 {
                return Err(invalid(format!(
                    "FIELD {column} requires two direction axes"
                )));
            }
            let mut first = vec![0; shape.len()];
            let longitude = angles[first.as_slice()];
            first[0] = 1;
            let latitude = angles[first.as_slice()];
            let row = table.row_accessor().row(index)?;
            let frame = reference.at(row).map_err(|err| invalid(err.to_string()))?;
            Ok(MDirection::from_angles(
                longitude * scale[0],
                latitude * scale[1],
                frame,
            ))
        })
        .collect()
}

pub(super) fn antenna_positions(table: &Table) -> MsResult<Vec<MPosition>> {
    let reference = Reference::<PositionRef>::read(table, "POSITION", MeasureType::Position)?;
    let units = TableQuantumDesc::reconstruct(table, "POSITION");
    let metric = unit_scales(table, "POSITION", ["m"; 3]);
    let spherical = unit_scales(table, "POSITION", ["m", "rad", "rad"]);
    let values = table.column_accessor("POSITION")?;
    (0..table.row_count())
        .map(|index| {
            let ArrayValue::Float64(coords) = values.array_cell(index)? else {
                return Err(invalid("ANTENNA POSITION requires a double array"));
            };
            let Some(&[x, y, z]) = coords.as_slice() else {
                return Err(invalid("ANTENNA POSITION requires three coordinates"));
            };
            let row = table.row_accessor().row(index)?;
            let frame = reference.at(row).map_err(|err| invalid(err.to_string()))?;
            let cartesian = |scales: [f64; 3]| {
                let [sx, sy, sz] = scales;
                [x * sx, y * sy, z * sz]
            };
            let position = match frame {
                PositionRef::ITRF => {
                    let [x, y, z] = cartesian(
                        metric
                            .as_ref()
                            .copied()
                            .map_err(|err| invalid(err.to_string()))?,
                    );
                    MPosition::new_itrf(x, y, z)
                }
                PositionRef::WGS84 => {
                    if units.is_some() && metric.is_err() {
                        let [height, lon, lat] = cartesian(
                            spherical
                                .as_ref()
                                .copied()
                                .map_err(|err| invalid(err.to_string()))?,
                        );
                        MPosition::new_wgs84(lon, lat, height)
                    } else {
                        let [x, y, z] = cartesian(
                            metric
                                .as_ref()
                                .copied()
                                .map_err(|err| invalid(err.to_string()))?,
                        );
                        let radial = x.hypot(y);
                        MPosition::new_wgs84(y.atan2(x), z.atan2(radial), radial.hypot(z))
                    }
                }
            };
            let [x, y, z] = position.as_itrf();
            Ok(MPosition::new_itrf(x, y, z))
        })
        .collect()
}

enum Reference<T> {
    Fixed(T),
    Integer {
        column: String,
        refs: BTreeMap<i64, T>,
    },
    Text(String),
}

impl<T: FromStr + Copy> Reference<T> {
    fn read(table: &Table, column: &str, measure: MeasureType) -> MsResult<Self> {
        let invalid_measure = || {
            invalid(format!(
                "MSCAL requires valid {measure:?} MEASINFO on {column}"
            ))
        };
        let desc = TableMeasDesc::reconstruct(table, column).ok_or_else(invalid_measure)?;
        if desc.measure_type() != measure {
            return Err(invalid_measure());
        }
        let parse = |value: &str| value.parse().map_err(|_| invalid_measure());
        Ok(match desc.ref_desc() {
            MeasRefDesc::Fixed { refer } => Self::Fixed(parse(refer)?),
            MeasRefDesc::VariableString { ref_column } => Self::Text(ref_column.clone()),
            MeasRefDesc::VariableInt {
                ref_column,
                tab_ref_types,
                tab_ref_codes,
            } => {
                let refs = tab_ref_codes
                    .iter()
                    .zip(tab_ref_types)
                    .map(|(&code, name)| Ok((i64::from(code), parse(name)?)))
                    .collect::<MsResult<_>>()?;
                Self::Integer {
                    column: ref_column.clone(),
                    refs,
                }
            }
        })
    }

    fn at(&self, row: &RecordValue) -> Result<T, TaqlError> {
        let bad =
            |column: &str| TaqlError::Table(format!("invalid MSCAL measure reference in {column}"));
        match self {
            Self::Fixed(frame) => Ok(*frame),
            Self::Text(column) => match row.get(column) {
                Some(Value::Scalar(ScalarValue::String(name))) => {
                    name.parse().map_err(|_| bad(column))
                }
                _ => Err(bad(column)),
            },
            Self::Integer { column, refs } => {
                let code = match row.get(column) {
                    Some(Value::Scalar(ScalarValue::Int32(code))) => i64::from(*code),
                    Some(Value::Scalar(ScalarValue::UInt32(code))) => i64::from(*code),
                    Some(Value::Scalar(ScalarValue::Int64(code))) => *code,
                    _ => return Err(bad(column)),
                };
                refs.get(&code).copied().ok_or_else(|| bad(column))
            }
        }
    }
}

fn unit_scales<const N: usize>(
    table: &Table,
    column: &str,
    expected: [&str; N],
) -> MsResult<[f64; N]> {
    let Some(desc) = TableQuantumDesc::reconstruct(table, column) else {
        return Ok([1.0; N]);
    };
    let mut scales = [1.0; N];
    for (axis, expected) in expected.into_iter().enumerate() {
        let source = desc
            .units()
            .get(axis)
            .or_else(|| desc.units().first())
            .ok_or_else(|| invalid(format!("missing MSCAL QuantumUnits for {column}")))?;
        scales[axis] = Quantity::new(1.0, source)
            .and_then(|one| one.get_value_in(&Unit::new(expected)?))
            .map_err(|err| invalid(format!("invalid MSCAL QuantumUnits on {column}: {err}")))?;
    }
    Ok(scales)
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
