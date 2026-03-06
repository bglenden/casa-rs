// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-row derived column accessors for the MS main table.
//!
//! [`DerivedColumns`] wraps an [`MsCalEngine`] and provides per-row
//! accessors that read ANTENNA1, ANTENNA2, TIME, and FIELD_ID from
//! the main table and delegate to the engine for computation.
//!
//! Cf. C++ `DerivedColumn`.

use casacore_tables::Table;

use crate::error::MsResult;
use crate::subtables::{get_f64, get_i32};

use super::engine::MsCalEngine;

/// Per-row derived column accessors.
///
/// Reads TIME, FIELD_ID, ANTENNA1, ANTENNA2 from each row of the main
/// table and computes derived quantities via the [`MsCalEngine`].
///
/// # Example
///
/// ```rust,no_run
/// use casacore_ms::derived::columns::DerivedColumns;
/// use casacore_ms::derived::engine::MsCalEngine;
/// use casacore_ms::ms::MeasurementSet;
///
/// # fn example(ms: &MeasurementSet) -> casacore_ms::MsResult<()> {
/// let engine = MsCalEngine::new(ms)?;
/// let derived = DerivedColumns::new(ms.main_table(), engine);
/// for row in 0..ms.row_count() {
///     let ha1 = derived.ha1(row)?;
///     let pa1 = derived.pa1(row)?;
///     println!("Row {row}: HA1={ha1:.4}, PA1={pa1:.4}");
/// }
/// # Ok(())
/// # }
/// ```
///
/// Cf. C++ `DerivedColumn`.
pub struct DerivedColumns<'a> {
    table: &'a Table,
    engine: MsCalEngine,
}

impl<'a> DerivedColumns<'a> {
    /// Create derived column accessors for the given main table and engine.
    pub fn new(table: &'a Table, engine: MsCalEngine) -> Self {
        Self { table, engine }
    }

    /// The underlying engine.
    pub fn engine(&self) -> &MsCalEngine {
        &self.engine
    }

    // ---- Row metadata readers ----

    fn time(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "TIME")
    }

    fn field_id(&self, row: usize) -> MsResult<usize> {
        Ok(get_i32(self.table, row, "FIELD_ID")? as usize)
    }

    fn antenna1(&self, row: usize) -> MsResult<usize> {
        Ok(get_i32(self.table, row, "ANTENNA1")? as usize)
    }

    fn antenna2(&self, row: usize) -> MsResult<usize> {
        Ok(get_i32(self.table, row, "ANTENNA2")? as usize)
    }

    // ---- Hour angle ----

    /// Hour angle (radians) for ANTENNA1 at the given row.
    pub fn ha1(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna1(row)?;
        self.engine.hour_angle(time, field, ant)
    }

    /// Hour angle (radians) for ANTENNA2 at the given row.
    pub fn ha2(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna2(row)?;
        self.engine.hour_angle(time, field, ant)
    }

    /// Hour angle (radians) using the observatory position.
    pub fn ha(&self, row: usize) -> MsResult<f64> {
        // Use antenna 0 as the reference
        self.ha1(row)
    }

    // ---- Parallactic angle ----

    /// Parallactic angle (radians) for ANTENNA1 at the given row.
    pub fn pa1(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna1(row)?;
        self.engine.parallactic_angle(time, field, ant)
    }

    /// Parallactic angle (radians) for ANTENNA2 at the given row.
    pub fn pa2(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna2(row)?;
        self.engine.parallactic_angle(time, field, ant)
    }

    // ---- Azimuth / Elevation ----

    /// Azimuth and elevation (radians) for ANTENNA1 at the given row.
    pub fn azel1(&self, row: usize) -> MsResult<(f64, f64)> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna1(row)?;
        self.engine.azel(time, field, ant)
    }

    /// Azimuth and elevation (radians) for ANTENNA2 at the given row.
    pub fn azel2(&self, row: usize) -> MsResult<(f64, f64)> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna2(row)?;
        self.engine.azel(time, field, ant)
    }

    /// Azimuth and elevation (radians) using ANTENNA1 as reference.
    pub fn azel(&self, row: usize) -> MsResult<(f64, f64)> {
        self.azel1(row)
    }

    // ---- Local Apparent Sidereal Time ----

    /// LAST (radians) for ANTENNA1 at the given row.
    pub fn last1(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let ant = self.antenna1(row)?;
        self.engine.last(time, ant)
    }

    /// LAST (radians) for ANTENNA2 at the given row.
    pub fn last2(&self, row: usize) -> MsResult<f64> {
        let time = self.time(row)?;
        let ant = self.antenna2(row)?;
        self.engine.last(time, ant)
    }

    /// LAST (radians) using ANTENNA1 as reference.
    pub fn last(&self, row: usize) -> MsResult<f64> {
        self.last1(row)
    }

    // ---- UVW in J2000 ----

    /// UVW in J2000 frame for the baseline at the given row.
    pub fn uvw_j2000(&self, row: usize) -> MsResult<[f64; 3]> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant1 = self.antenna1(row)?;
        let ant2 = self.antenna2(row)?;
        self.engine.uvw_j2000(time, field, ant1, ant2)
    }

    // ---- HADEC ----

    /// Hour angle and declination (radians) for ANTENNA1 at the given row.
    pub fn hadec1(&self, row: usize) -> MsResult<(f64, f64)> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna1(row)?;
        self.engine.hadec(time, field, ant)
    }

    /// Hour angle and declination (radians) for ANTENNA2 at the given row.
    pub fn hadec2(&self, row: usize) -> MsResult<(f64, f64)> {
        let time = self.time(row)?;
        let field = self.field_id(row)?;
        let ant = self.antenna2(row)?;
        self.engine.hadec(time, field, ant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::ms::MeasurementSet;
    use crate::test_helpers::default_value;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    fn make_ms_with_row() -> MeasurementSet {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add two antennas at VLA
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [-1601185.4, -5041977.5, 3554875.9],
                [0.0; 3],
                25.0,
            )
            .unwrap();
            ant.add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [-1601085.4, -5041977.5, 3554875.9],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }

        // Add a field
        {
            use crate::schema;
            use crate::test_helpers::default_value_for_def;
            use casacore_types::ArrayValue;
            use ndarray::ArrayD;

            let field_table = ms.subtable_mut(crate::schema::SubtableId::Field).unwrap();
            let fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
                .iter()
                .map(|c| {
                    if c.name == "PHASE_DIR" {
                        let ra = 0.5;
                        let dec = 0.8;
                        RecordField::new(
                            "PHASE_DIR",
                            Value::Array(ArrayValue::Float64(
                                ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap(),
                            )),
                        )
                    } else {
                        RecordField::new(c.name, default_value_for_def(c))
                    }
                })
                .collect();
            field_table.add_row(RecordValue::new(fields)).unwrap();
        }

        // Add a main table row
        let time = 59000.5 * 86400.0;
        let schema = ms.main_table().schema().unwrap().clone();
        let fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| match col.name() {
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();

        ms
    }

    #[test]
    fn derived_ha_pa_finite() {
        let ms = make_ms_with_row();
        let engine = MsCalEngine::new(&ms).unwrap();
        let derived = DerivedColumns::new(ms.main_table(), engine);

        let ha1 = derived.ha1(0).unwrap();
        let pa1 = derived.pa1(0).unwrap();
        assert!(ha1.is_finite());
        assert!(pa1.is_finite());
    }

    #[test]
    fn derived_last_positive() {
        let ms = make_ms_with_row();
        let engine = MsCalEngine::new(&ms).unwrap();
        let derived = DerivedColumns::new(ms.main_table(), engine);

        let last = derived.last1(0).unwrap();
        assert!((0.0..2.0 * std::f64::consts::PI).contains(&last));
    }

    #[test]
    fn derived_uvw_j2000_finite() {
        let ms = make_ms_with_row();
        let engine = MsCalEngine::new(&ms).unwrap();
        let derived = DerivedColumns::new(ms.main_table(), engine);

        let uvw = derived.uvw_j2000(0).unwrap();
        assert!(uvw[0].is_finite());
        assert!(uvw[1].is_finite());
        assert!(uvw[2].is_finite());
    }
}
