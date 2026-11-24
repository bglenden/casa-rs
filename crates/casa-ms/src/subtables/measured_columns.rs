// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read stored references and units for MeasurementSet geometry.

use std::collections::BTreeMap;
use std::str::FromStr;

use casa_tables::table_measures::{MeasRefDesc, MeasureType, TableMeasDesc};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_tables::taql::TaqlError;
use casa_tables::{Table, TableColumn};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::position::{MPosition, PositionRef};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::{MsError, MsResult};

pub(crate) struct TimeColumn {
    reference: Reference<EpochRef>,
    seconds_per_unit: f64,
}

impl TimeColumn {
    pub(crate) fn read(table: &Table) -> MsResult<Self> {
        Ok(Self {
            reference: Reference::read(table, "TIME", MeasureType::Epoch)?,
            seconds_per_unit: unit_scales(table, "TIME", ["s"])?[0],
        })
    }

    pub(crate) fn epoch(&self, row: &RecordValue) -> Result<MEpoch, TaqlError> {
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

    pub(crate) fn reference_at(&self, table: &Table, index: usize) -> MsResult<EpochRef> {
        self.reference.at_table_row(table, index)
    }
}

pub(crate) fn directions(table: &Table, column: &str) -> MsResult<Vec<MDirection>> {
    let reference = Reference::<DirectionRef>::read(table, column, MeasureType::Direction)?;
    let scale = unit_scales(table, column, ["rad"; 2])?;
    let values = table.column_accessor(column)?;
    (0..table.row_count())
        .map(|index| {
            let ArrayValue::Float64(angles) = values.array_cell(index)? else {
                return Err(invalid(format!("FIELD {column} requires a double array")));
            };
            let shape = angles.shape();
            if shape.first() != Some(&2) || angles.len() < 2 {
                return Err(invalid(format!(
                    "FIELD {column} requires two direction axes"
                )));
            }
            let mut first = vec![0; shape.len()];
            let longitude = angles[first.as_slice()];
            first[0] = 1;
            let latitude = angles[first.as_slice()];
            let frame = reference.at_table_row(table, index)?;
            Ok(MDirection::from_angles(
                longitude * scale[0],
                latitude * scale[1],
                frame,
            ))
        })
        .collect()
}

pub(crate) fn antenna_positions(table: &Table) -> MsResult<Vec<MPosition>> {
    let column = PositionColumn::read(table)?;
    (0..table.row_count())
        .map(|index| {
            let [x, y, z] = column.position(index)?.as_itrf();
            Ok(MPosition::new_itrf(x, y, z))
        })
        .collect()
}

pub(crate) fn antenna_position(table: &Table, index: usize) -> MsResult<MPosition> {
    PositionColumn::read(table)?.position(index)
}

struct PositionColumn<'a> {
    table: &'a Table,
    values: TableColumn<'a>,
    reference: Reference<PositionRef>,
    metric: MsResult<[f64; 3]>,
    spherical: MsResult<[f64; 3]>,
}

impl<'a> PositionColumn<'a> {
    fn read(table: &'a Table) -> MsResult<Self> {
        Ok(Self {
            table,
            values: table.column_accessor("POSITION")?,
            reference: Reference::read(table, "POSITION", MeasureType::Position)?,
            metric: unit_scales(table, "POSITION", ["m"; 3]),
            spherical: unit_scales(table, "POSITION", ["m", "rad", "rad"]),
        })
    }

    fn position(&self, index: usize) -> MsResult<MPosition> {
        let ArrayValue::Float64(coords) = self.values.array_cell(index)? else {
            return Err(invalid("ANTENNA POSITION requires a double array"));
        };
        let Some(&[x, y, z]) = coords.as_slice() else {
            return Err(invalid("ANTENNA POSITION requires three coordinates"));
        };
        let frame = self.reference.at_table_row(self.table, index)?;
        let scaled = |scales: &MsResult<[f64; 3]>| {
            scales
                .as_ref()
                .map(|&[sx, sy, sz]| [x * sx, y * sy, z * sz])
                .map_err(|err| invalid(err.to_string()))
        };
        match frame {
            PositionRef::ITRF => {
                let [x, y, z] = scaled(&self.metric)?;
                Ok(MPosition::new_itrf(x, y, z))
            }
            PositionRef::WGS84 if self.metric.is_err() => {
                let [height, lon, lat] = scaled(&self.spherical)?;
                Ok(MPosition::new_wgs84(lon, lat, height))
            }
            PositionRef::WGS84 => {
                let [x, y, z] = scaled(&self.metric)?;
                let radial = x.hypot(y);
                Ok(MPosition::new_wgs84(
                    y.atan2(x),
                    z.atan2(radial),
                    radial.hypot(z),
                ))
            }
        }
    }
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

    fn at_table_row(&self, table: &Table, index: usize) -> MsResult<T> {
        if let Self::Fixed(frame) = self {
            return Ok(*frame);
        }
        self.at(table.row_accessor().row(index)?)
            .map_err(|error| invalid(error.to_string()))
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
