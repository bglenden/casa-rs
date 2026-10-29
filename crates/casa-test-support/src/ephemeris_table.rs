// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ephemeris tables for tests: the layout casacore's `MeasComet`
//! reads (an `IERS` table of regularly spaced samples with its `MJD0`,
//! `dMJD` and `VS_*` keywords) and XRADIO's converter reads (`GeoLong`,
//! `GeoLat`, `GeoDist`, `obsloc`, `posrefsys`), both as a MeasurementSet
//! field's attached ephemeris (`FIELD/EPHEM<id>_<name>.tab`) and as a
//! `phasecenter` table.

use std::path::{Path, PathBuf};

use casa_tables::{ColumnSchema, Table, TableInfo, TableOptions, TableSchema};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// One sample: MJD (days), RA and DEC (degrees, ICRS), geocentric
/// distance `Rho` (AU) and radial velocity `RadVel` (AU a day).
pub type EphemerisRow = [f64; 5];

/// Write a geocentric ICRS ephemeris of body `name` at `path`.
///
/// # Panics
///
/// If the table cannot be built or saved, or `rows` are fewer than two or
/// not equally spaced in time, as `MeasComet` requires.
pub fn write_ephemeris_table(path: &Path, name: &str, rows: &[EphemerisRow]) {
    let step = rows[1][0] - rows[0][0];
    assert!(
        rows.windows(2)
            .all(|pair| ((pair[1][0] - pair[0][0]) - step).abs() <= 1.0e-9),
        "MeasComet reads equally spaced samples"
    );
    let schema = TableSchema::new(
        ["MJD", "RA", "DEC", "Rho", "RadVel"]
            .into_iter()
            .map(|column| ColumnSchema::scalar(column, PrimitiveType::Float64))
            .collect(),
    )
    .expect("an ephemeris schema");
    let mut table = Table::with_schema(schema);
    for row in rows {
        table
            .add_row(RecordValue::new(
                ["MJD", "RA", "DEC", "Rho", "RadVel"]
                    .into_iter()
                    .zip(row)
                    .map(|(column, value)| {
                        RecordField::new(column, Value::Scalar(ScalarValue::Float64(*value)))
                    })
                    .collect(),
            ))
            .expect("an ephemeris row");
    }
    for (keyword, value) in [
        ("VS_CREATE", "2026/10/29/00:00"),
        ("VS_DATE", "2026/10/29/00:00"),
        ("VS_VERSION", "0001.0001"),
        ("VS_TYPE", "Table of comet/planetary positions"),
        ("NAME", name),
        ("posrefsys", "ICRF/ICRS"),
        ("obsloc", "GEOCENTRIC"),
    ] {
        table.keywords_mut().push(RecordField::new(
            keyword,
            Value::Scalar(ScalarValue::String(value.to_string())),
        ));
    }
    // MeasComet indexes rows from MJD0, one step before the first sample.
    for (keyword, value) in [
        ("MJD0", rows[0][0] - step),
        ("dMJD", step),
        ("GeoLong", 0.0),
        ("GeoLat", 0.0),
        ("GeoDist", 0.0),
    ] {
        table.keywords_mut().push(RecordField::new(
            keyword,
            Value::Scalar(ScalarValue::Float64(value)),
        ));
    }
    table.set_info(TableInfo {
        table_type: "IERS".to_string(),
        sub_type: String::new(),
        readme: Vec::new(),
    });
    table
        .save(TableOptions::new(path))
        .expect("save the ephemeris table");
}

/// Write body `name`'s ephemeris as the table the fields of the
/// MeasurementSet at `measurement_set` whose `EPHEMERIS_ID` is
/// `ephemeris_id` follow, and return its path. The caller sets the
/// fields' `EPHEMERIS_ID`.
///
/// # Panics
///
/// If the table cannot be built or saved.
pub fn write_field_ephemeris(
    measurement_set: &Path,
    ephemeris_id: i32,
    name: &str,
    rows: &[EphemerisRow],
) -> PathBuf {
    let path = measurement_set
        .join("FIELD")
        .join(format!("EPHEM{ephemeris_id}_{name}.tab"));
    write_ephemeris_table(&path, name, rows);
    path
}
