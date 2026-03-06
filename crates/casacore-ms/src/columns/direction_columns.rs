// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for direction columns (PHASE_DIR, DELAY_DIR, REFERENCE_DIR).
//!
//! Direction columns in the FIELD subtable store polynomial coefficients
//! with shape `[2, NUM_POLY+1]`. The constant term gives the direction
//! at the reference time; higher-order terms give rates.
//!
//! Cf. C++ `MSFieldColumns::phaseDirMeas()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;
use casacore_types::measures::direction::{DirectionRef, MDirection};

use crate::error::{MsError, MsResult};
use crate::subtables::get_array;

/// Typed accessor for a direction column (polynomial in time).
pub struct DirectionColumn<'a> {
    table: &'a Table,
    column: &'static str,
    refer: DirectionRef,
}

impl<'a> DirectionColumn<'a> {
    /// Create a PHASE_DIR column accessor.
    pub fn phase_dir(table: &'a Table) -> Self {
        Self {
            table,
            column: "PHASE_DIR",
            refer: DirectionRef::J2000,
        }
    }

    /// Create a DELAY_DIR column accessor.
    pub fn delay_dir(table: &'a Table) -> Self {
        Self {
            table,
            column: "DELAY_DIR",
            refer: DirectionRef::J2000,
        }
    }

    /// Create a REFERENCE_DIR column accessor.
    pub fn reference_dir(table: &'a Table) -> Self {
        Self {
            table,
            column: "REFERENCE_DIR",
            refer: DirectionRef::J2000,
        }
    }

    /// Read the direction at the constant term (t=0) as [`MDirection`].
    ///
    /// This extracts the first column of the polynomial (the direction
    /// at the reference time).
    pub fn get_direction(&self, row: usize) -> MsResult<MDirection> {
        let arr = get_array(self.table, row, self.column)?;
        let (lon, lat) = extract_direction_constant(arr, self.column)?;
        Ok(MDirection::from_angles(lon, lat, self.refer))
    }

    /// Evaluate the direction polynomial at time offset `dt` seconds
    /// from the TIME reference.
    ///
    /// For `NUM_POLY == 0`, this returns the same as `get_direction()`.
    /// For higher orders, evaluates:
    ///   lon(dt) = lon0 + lon1*dt + lon2*dt^2 + ...
    ///   lat(dt) = lat0 + lat1*dt + lat2*dt^2 + ...
    pub fn get_direction_at(&self, row: usize, dt: f64) -> MsResult<MDirection> {
        let arr = get_array(self.table, row, self.column)?;
        let (lon, lat) = evaluate_direction_polynomial(arr, self.column, dt)?;
        Ok(MDirection::from_angles(lon, lat, self.refer))
    }
}

/// Extract the constant direction (first column of the polynomial array).
fn extract_direction_constant(arr: &ArrayValue, col_name: &str) -> MsResult<(f64, f64)> {
    match arr {
        ArrayValue::Float64(a) => {
            let shape = a.shape();
            if shape.len() != 2 || shape[0] != 2 {
                return Err(MsError::ColumnTypeMismatch {
                    column: col_name.to_string(),
                    table: "FIELD".to_string(),
                    expected: "shape [2, N]".to_string(),
                    found: format!("shape {shape:?}"),
                });
            }
            let lon = a[[0, 0]];
            let lat = a[[1, 0]];
            Ok((lon, lat))
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: col_name.to_string(),
            table: "FIELD".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Evaluate the direction polynomial at time offset dt.
fn evaluate_direction_polynomial(
    arr: &ArrayValue,
    col_name: &str,
    dt: f64,
) -> MsResult<(f64, f64)> {
    match arr {
        ArrayValue::Float64(a) => {
            let shape = a.shape();
            if shape.len() != 2 || shape[0] != 2 {
                return Err(MsError::ColumnTypeMismatch {
                    column: col_name.to_string(),
                    table: "FIELD".to_string(),
                    expected: "shape [2, N]".to_string(),
                    found: format!("shape {shape:?}"),
                });
            }
            let n_poly = shape[1];
            let mut lon = 0.0;
            let mut lat = 0.0;
            let mut dt_power = 1.0;
            for i in 0..n_poly {
                lon += a[[0, i]] * dt_power;
                lat += a[[1, i]] * dt_power;
                dt_power *= dt;
            }
            Ok((lon, lat))
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: col_name.to_string(),
            table: "FIELD".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    fn make_field_table_with_direction(ra: f64, dec: f64) -> Table {
        let schema = build_table_schema(schema::field::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        let dir = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());

        let row = RecordValue::new(vec![
            RecordField::new("CODE", Value::Scalar(ScalarValue::String(String::new()))),
            RecordField::new("DELAY_DIR", Value::Array(dir.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("test".to_string())),
            ),
            RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("PHASE_DIR", Value::Array(dir.clone())),
            RecordField::new("REFERENCE_DIR", Value::Array(dir)),
            RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
        ]);
        table.add_row(row).unwrap();
        table
    }

    #[test]
    fn constant_direction() {
        let ra = 1.5;
        let dec = 0.3;
        let table = make_field_table_with_direction(ra, dec);

        let col = DirectionColumn::phase_dir(&table);
        let dir = col.get_direction(0).unwrap();
        assert_eq!(dir.refer(), DirectionRef::J2000);

        // Convert back to angles and check
        let (lon, lat) = dir.as_angles();
        assert!((lon - ra).abs() < 1e-14);
        assert!((lat - dec).abs() < 1e-14);
    }

    #[test]
    fn polynomial_at_t0_matches_constant() {
        let ra = 2.0;
        let dec = -0.5;
        let table = make_field_table_with_direction(ra, dec);

        let col = DirectionColumn::phase_dir(&table);
        let dir_const = col.get_direction(0).unwrap();
        let dir_t0 = col.get_direction_at(0, 0.0).unwrap();

        let (lon1, lat1) = dir_const.as_angles();
        let (lon2, lat2) = dir_t0.as_angles();
        assert!((lon1 - lon2).abs() < 1e-14);
        assert!((lat1 - lat2).abs() < 1e-14);
    }
}
