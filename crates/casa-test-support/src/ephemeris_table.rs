// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ephemeris tables for tests: the layout casacore's `MeasComet`
//! reads (an `IERS` table of regularly spaced samples with its `MJD0`,
//! `dMJD` and `VS_*` keywords) and XRADIO's converter reads (`GeoLong`,
//! `GeoLat`, `GeoDist`, `obsloc`, `posrefsys`, the columns' `UNIT`), both as a MeasurementSet
//! field's attached ephemeris (`FIELD/EPHEM<id>_<name>.tab`) and as a
//! `phasecenter` table.

use std::path::{Path, PathBuf};

use casa_tables::{ColumnSchema, Table, TableInfo, TableOptions, TableSchema};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// One sample: MJD (days), RA and DEC (degrees, ICRS), geocentric
/// distance `Rho` (AU) and radial velocity `RadVel` (AU a day).
pub type EphemerisRow = [f64; 5];

/// The frame of an ephemeris's directions, as its `posrefsys` keyword
/// names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EphemerisFrame {
    /// `ICRF/ICRS`.
    Icrs,
    /// `ICRF/J2000.0`.
    J2000,
}

/// Write a geocentric ICRS ephemeris of body `name` at `path`.
///
/// # Panics
///
/// If the table cannot be built or saved, or `rows` are fewer than two or
/// not equally spaced in time, as `MeasComet` requires.
pub fn write_ephemeris_table(path: &Path, name: &str, rows: &[EphemerisRow]) {
    write_ephemeris_table_in(path, name, rows, EphemerisFrame::Icrs);
}

/// [`write_ephemeris_table`] with its directions in `frame`.
///
/// # Panics
///
/// As [`write_ephemeris_table`].
pub fn write_ephemeris_table_in(
    path: &Path,
    name: &str,
    rows: &[EphemerisRow],
    frame: EphemerisFrame,
) {
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
    for (column, unit) in ["MJD", "RA", "DEC", "Rho", "RadVel"]
        .into_iter()
        .zip(["d", "deg", "deg", "AU", "AU/d"])
    {
        table.set_column_keywords(
            column,
            RecordValue::new(vec![RecordField::new(
                "UNIT",
                Value::Scalar(ScalarValue::String(unit.to_string())),
            )]),
        );
    }
    let posrefsys = match frame {
        EphemerisFrame::Icrs => "ICRF/ICRS",
        EphemerisFrame::J2000 => "ICRF/J2000.0",
    };
    for (keyword, value) in [
        ("VS_CREATE", "2026/10/29/00:00"),
        ("VS_DATE", "2026/10/29/00:00"),
        ("VS_VERSION", "0001.0001"),
        ("VS_TYPE", "Table of comet/planetary positions"),
        ("NAME", name),
        ("posrefsys", posrefsys),
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
    write_field_ephemeris_in(
        measurement_set,
        ephemeris_id,
        name,
        rows,
        EphemerisFrame::Icrs,
    )
}

/// [`write_field_ephemeris`] with its directions in `frame`.
///
/// # Panics
///
/// As [`write_field_ephemeris`].
pub fn write_field_ephemeris_in(
    measurement_set: &Path,
    ephemeris_id: i32,
    name: &str,
    rows: &[EphemerisRow],
    frame: EphemerisFrame,
) -> PathBuf {
    let path = measurement_set
        .join("FIELD")
        .join(format!("EPHEM{ephemeris_id}_{name}.tab"));
    write_ephemeris_table_in(&path, name, rows, frame);
    path
}
