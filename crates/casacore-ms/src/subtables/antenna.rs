// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrapper for the ANTENNA subtable.
//!
//! Provides read access to antenna positions, names, stations, and dish
//! diameters. The mutable variant additionally supports writing.
//!
//! Cf. C++ `MSAntennaColumns` / `MSAntenna`.

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue, Value};
use ndarray::ArrayD;

use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};
use crate::subtables::{
    SubTable, get_array, get_bool, get_f64, get_string, has_column, set_array, set_scalar,
};

/// Read-only typed wrapper for the ANTENNA subtable.
///
/// Cf. C++ `MSAntennaColumns`.
pub struct MsAntenna<'a> {
    table: &'a Table,
}

impl<'a> MsAntenna<'a> {
    /// Wrap an existing table as an ANTENNA subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Number of antenna rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Antenna name (e.g. `"VLA22"`, `"CA03"`).
    pub fn name(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "NAME")
    }

    /// Station (antenna pad) name.
    pub fn station(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "STATION")
    }

    /// Antenna type (e.g. `"GROUND-BASED"`).
    pub fn antenna_type(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "TYPE")
    }

    /// Mount type (e.g. `"ALT-AZ"`, `"EQUATORIAL"`).
    pub fn mount(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "MOUNT")
    }

    /// Antenna position as `[x, y, z]` in meters (ITRF).
    pub fn position(&self, row: usize) -> MsResult<[f64; 3]> {
        extract_f64_3(get_array(self.table, row, "POSITION")?)
    }

    /// Axes offset of mount to feed reference point, `[x, y, z]` in meters.
    pub fn offset(&self, row: usize) -> MsResult<[f64; 3]> {
        extract_f64_3(get_array(self.table, row, "OFFSET")?)
    }

    /// Physical diameter of the dish in meters.
    pub fn dish_diameter(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "DISH_DIAMETER")
    }

    /// Row flag.
    pub fn flag_row(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }

    /// Mean Keplerian orbit elements (optional, 6 elements).
    pub fn mean_orbit(&self, row: usize) -> MsResult<Option<&ArrayValue>> {
        if !has_column(self.table, "MEAN_ORBIT") {
            return Ok(None);
        }
        Ok(Some(get_array(self.table, row, "MEAN_ORBIT")?))
    }

    /// Orbit ID (optional).
    pub fn orbit_id(&self, row: usize) -> MsResult<Option<i32>> {
        optional_i32(self.table, row, "ORBIT_ID")
    }

    /// Phased array ID (optional).
    pub fn phased_array_id(&self, row: usize) -> MsResult<Option<i32>> {
        optional_i32(self.table, row, "PHASED_ARRAY_ID")
    }
}

impl SubTable for MsAntenna<'_> {
    fn id() -> SubtableId {
        SubtableId::Antenna
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::antenna::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::antenna::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

/// Mutable typed wrapper for the ANTENNA subtable.
///
/// Cf. C++ `MSAntennaColumns` (write methods).
pub struct MsAntennaMut<'a> {
    table: &'a mut Table,
}

impl<'a> MsAntennaMut<'a> {
    /// Wrap an existing mutable table as an ANTENNA subtable.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table }
    }

    /// Get an immutable view.
    pub fn as_ref(&self) -> MsAntenna<'_> {
        MsAntenna { table: self.table }
    }

    /// Number of antenna rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Set the antenna name.
    pub fn put_name(&mut self, row: usize, name: &str) -> MsResult<()> {
        set_scalar(
            self.table,
            row,
            "NAME",
            ScalarValue::String(name.to_string()),
        )
    }

    /// Set the station name.
    pub fn put_station(&mut self, row: usize, station: &str) -> MsResult<()> {
        set_scalar(
            self.table,
            row,
            "STATION",
            ScalarValue::String(station.to_string()),
        )
    }

    /// Set the antenna type.
    pub fn put_type(&mut self, row: usize, antenna_type: &str) -> MsResult<()> {
        set_scalar(
            self.table,
            row,
            "TYPE",
            ScalarValue::String(antenna_type.to_string()),
        )
    }

    /// Set the mount type.
    pub fn put_mount(&mut self, row: usize, mount: &str) -> MsResult<()> {
        set_scalar(
            self.table,
            row,
            "MOUNT",
            ScalarValue::String(mount.to_string()),
        )
    }

    /// Set the antenna position `[x, y, z]` in meters.
    pub fn put_position(&mut self, row: usize, pos: [f64; 3]) -> MsResult<()> {
        set_array(
            self.table,
            row,
            "POSITION",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], pos.to_vec()).unwrap()),
        )
    }

    /// Set the axes offset `[x, y, z]` in meters.
    pub fn put_offset(&mut self, row: usize, offset: [f64; 3]) -> MsResult<()> {
        set_array(
            self.table,
            row,
            "OFFSET",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], offset.to_vec()).unwrap()),
        )
    }

    /// Set the dish diameter in meters.
    pub fn put_dish_diameter(&mut self, row: usize, diameter: f64) -> MsResult<()> {
        set_scalar(
            self.table,
            row,
            "DISH_DIAMETER",
            ScalarValue::Float64(diameter),
        )
    }

    /// Set the row flag.
    pub fn put_flag_row(&mut self, row: usize, flag: bool) -> MsResult<()> {
        set_scalar(self.table, row, "FLAG_ROW", ScalarValue::Bool(flag))
    }

    /// Add a new antenna row with all required fields.
    #[allow(clippy::too_many_arguments)]
    pub fn add_antenna(
        &mut self,
        name: &str,
        station: &str,
        antenna_type: &str,
        mount: &str,
        position: [f64; 3],
        offset: [f64; 3],
        dish_diameter: f64,
    ) -> MsResult<usize> {
        use casacore_types::{RecordField, RecordValue};

        let pos_arr =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], position.to_vec()).unwrap());
        let off_arr =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], offset.to_vec()).unwrap());

        let row = RecordValue::new(vec![
            RecordField::new(
                "DISH_DIAMETER",
                Value::Scalar(ScalarValue::Float64(dish_diameter)),
            ),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "MOUNT",
                Value::Scalar(ScalarValue::String(mount.to_string())),
            ),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("OFFSET", Value::Array(off_arr)),
            RecordField::new("POSITION", Value::Array(pos_arr)),
            RecordField::new(
                "STATION",
                Value::Scalar(ScalarValue::String(station.to_string())),
            ),
            RecordField::new(
                "TYPE",
                Value::Scalar(ScalarValue::String(antenna_type.to_string())),
            ),
        ]);

        let idx = self.table.row_count();
        self.table.add_row(row)?;
        Ok(idx)
    }
}

impl SubTable for MsAntennaMut<'_> {
    fn id() -> SubtableId {
        SubtableId::Antenna
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::antenna::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::antenna::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

// ---- helpers ----

fn extract_f64_3(arr: &ArrayValue) -> MsResult<[f64; 3]> {
    match arr {
        ArrayValue::Float64(a) => {
            let slice = a.as_slice().ok_or_else(|| MsError::ColumnTypeMismatch {
                column: "array".to_string(),
                table: "ANTENNA".to_string(),
                expected: "contiguous f64[3]".to_string(),
                found: "non-contiguous array".to_string(),
            })?;
            if slice.len() != 3 {
                return Err(MsError::ColumnTypeMismatch {
                    column: "array".to_string(),
                    table: "ANTENNA".to_string(),
                    expected: "f64[3]".to_string(),
                    found: format!("f64[{}]", slice.len()),
                });
            }
            Ok([slice[0], slice[1], slice[2]])
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "ANTENNA".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn optional_i32(table: &Table, row: usize, col: &str) -> MsResult<Option<i32>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(super::get_i32(table, row, col)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    fn make_antenna_table() -> Table {
        let schema = build_table_schema(schema::antenna::REQUIRED_COLUMNS).expect("valid schema");
        Table::with_schema(schema)
    }

    #[test]
    fn add_and_read_antenna() {
        let mut table = make_antenna_table();
        let mut ant = MsAntennaMut::new(&mut table);

        let idx = ant
            .add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [100.0, 200.0, 300.0],
                [0.0, 0.0, 0.0],
                25.0,
            )
            .expect("add_antenna");
        assert_eq!(idx, 0);

        let r = ant.as_ref();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.name(0).unwrap(), "VLA01");
        assert_eq!(r.station(0).unwrap(), "N01");
        assert_eq!(r.antenna_type(0).unwrap(), "GROUND-BASED");
        assert_eq!(r.mount(0).unwrap(), "ALT-AZ");
        assert_eq!(r.position(0).unwrap(), [100.0, 200.0, 300.0]);
        assert_eq!(r.offset(0).unwrap(), [0.0, 0.0, 0.0]);
        assert_eq!(r.dish_diameter(0).unwrap(), 25.0);
        assert!(!r.flag_row(0).unwrap());
    }

    #[test]
    fn optional_columns_return_none() {
        let mut table = make_antenna_table();
        let mut ant = MsAntennaMut::new(&mut table);
        ant.add_antenna("A", "S", "T", "M", [0.0; 3], [0.0; 3], 10.0)
            .unwrap();
        let r = ant.as_ref();
        assert_eq!(r.orbit_id(0).unwrap(), None);
        assert_eq!(r.phased_array_id(0).unwrap(), None);
        assert!(r.mean_orbit(0).unwrap().is_none());
    }

    #[test]
    fn put_updates_values() {
        let mut table = make_antenna_table();
        let mut ant = MsAntennaMut::new(&mut table);
        ant.add_antenna("OLD", "S", "T", "M", [0.0; 3], [0.0; 3], 10.0)
            .unwrap();
        ant.put_name(0, "NEW").unwrap();
        ant.put_dish_diameter(0, 42.0).unwrap();
        assert_eq!(ant.as_ref().name(0).unwrap(), "NEW");
        assert_eq!(ant.as_ref().dish_diameter(0).unwrap(), 42.0);
    }
}
