// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fields that follow an ephemeris, written into an MSv2: each ephemeris
//! the catalogue's fields follow as the table casacore's `MeasComet` reads
//! (`FIELD/EPHEM<id>_<name>.tab`), and each field's `EPHEMERIS_ID`. A
//! field's `PHASE_DIR` is then its offset from the body, which is how the
//! catalogue holds its phase centre.
//!
//! `MeasComet` reads a table of evenly spaced samples: row `i` at
//! `MJD0 + (i + 1)·dMJD`, its `RA` and `DEC` in degrees, `Rho` in AU and
//! `RadVel` in AU a day whatever the columns' units, with the `VS_*`
//! keywords and table type `IERS` of a Measures table
//! (`MeasIERS::handle_keywords`). XRADIO's converter also reads `GeoLong`,
//! `GeoLat`, `GeoDist`, `obsloc`, `posrefsys` and the columns' `UNIT`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_observation::{Catalogue, FieldEphemeris, ObservationError};
use casa_tables::{ColumnSchema, RowRange, Table, TableInfo, TableOptions, TableSchema};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use crate::ephemeris::AU_METRES;
use crate::observation_io::update::storage;
use crate::schema::SubtableId;
use crate::{MeasurementSet, MsError};

/// How far a sample's time may be from its place in an evenly spaced
/// table, days: 1 ms, well above the rounding of a time that went through
/// MSv4's Unix seconds and well below any motion that matters.
const SPACING_TOLERANCE_DAYS: f64 = 1.0e-3 / 86_400.0;

/// The columns written, their units, and how each sample gives them.
const COLUMNS: [(&str, &str); 5] = [
    ("MJD", "d"),
    ("RA", "deg"),
    ("DEC", "deg"),
    ("Rho", "AU"),
    ("RadVel", "AU/d"),
];

/// The keywords written; anything else a source table states is not
/// carried.
pub(super) const KEYWORDS: [&str; 12] = [
    "VS_CREATE",
    "VS_DATE",
    "VS_VERSION",
    "VS_TYPE",
    "NAME",
    "MJD0",
    "dMJD",
    "GeoLong",
    "GeoLat",
    "GeoDist",
    "posrefsys",
    "obsloc",
];

/// Each distinct ephemeris the catalogue's fields follow, in the order of
/// the first field that follows it, and each field's index into them (its
/// `EPHEMERIS_ID`), or `None`.
pub(super) fn followed(catalogue: &Catalogue) -> (Vec<&Arc<FieldEphemeris>>, Vec<Option<usize>>) {
    let mut distinct: Vec<&Arc<FieldEphemeris>> = Vec::new();
    let ids = catalogue
        .fields
        .iter()
        .map(|field| {
            let ephemeris = field.ephemeris.as_ref()?;
            Some(
                distinct
                    .iter()
                    .position(|known| Arc::ptr_eq(known, ephemeris) || ***known == **ephemeris)
                    .unwrap_or_else(|| {
                        distinct.push(ephemeris);
                        distinct.len() - 1
                    }),
            )
        })
        .collect();
    (distinct, ids)
}

/// Refuse an ephemeris an MSv2 cannot hold, before anything is written:
/// samples not evenly spaced ([`SPACING_TOLERANCE_DAYS`]), which
/// `MeasComet` cannot index, or radial velocities stated from anywhere but
/// the Earth's centre, which the table this writes (`GeoDist` 0) would
/// relabel.
///
/// # Errors
///
/// [`ObservationError::Unsupported`] for either.
pub(super) fn refuse_unwritable(catalogue: &Catalogue) -> Result<(), ObservationError> {
    for ephemeris in followed(catalogue).0 {
        if ephemeris.velocity_frame() != RadialVelocityRef::GEO {
            return Err(ObservationError::Unsupported {
                what: "writing an MSv2 ephemeris whose radial velocities are not geocentric",
            });
        }
        if spacing(ephemeris).is_none() {
            return Err(ObservationError::Unsupported {
                what: "writing an MSv2 ephemeris whose samples are not evenly spaced in time \
                       (casacore's MeasComet reads only evenly spaced tables)",
            });
        }
    }
    Ok(())
}

/// The samples' step, days, when they are evenly spaced.
fn spacing(ephemeris: &FieldEphemeris) -> Option<f64> {
    let times = ephemeris.times_mjd();
    let first = times[0];
    let step = (times[times.len() - 1] - first) / (times.len() - 1) as f64;
    times
        .iter()
        .enumerate()
        .all(|(index, time)| (time - (first + index as f64 * step)).abs() <= SPACING_TOLERANCE_DAYS)
        .then_some(step)
}

/// Give `ms`'s `FIELD` rows (one per catalogue field, in order) the
/// `EPHEMERIS_ID` [`followed`] numbers, when any field follows one.
///
/// # Errors
///
/// Any error writing the column.
pub(super) fn mark_fields(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
) -> Result<(), ObservationError> {
    let (distinct, ids) = followed(catalogue);
    if distinct.is_empty() {
        return Ok(());
    }
    let field = ms
        .subtable_mut(SubtableId::Field)
        .ok_or_else(|| storage(MsError::MissingSubtable("FIELD".to_string())))?;
    field
        .add_column(
            ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(-1))),
        )
        .map_err(storage)?;
    for (row, id) in ids.iter().enumerate() {
        if let Some(id) = id {
            field
                .fill_column_range(
                    "EPHEMERIS_ID",
                    RowRange::new(row, row + 1),
                    Value::Scalar(ScalarValue::Int32(
                        i32::try_from(*id).expect("few ephemerides"),
                    )),
                )
                .map_err(storage)?;
        }
    }
    Ok(())
}

/// Write each ephemeris [`followed`] numbers as its table in the MSv2 at
/// `root`, stamped `creation_date` (ISO 8601).
///
/// # Errors
///
/// Any error writing a table.
pub(super) fn write_tables(
    root: &Path,
    catalogue: &Catalogue,
    creation_date: &str,
) -> Result<(), ObservationError> {
    for (id, ephemeris) in followed(catalogue).0.into_iter().enumerate() {
        write_table(
            &table_path(root, id, ephemeris.name()),
            ephemeris,
            creation_date,
        )?;
    }
    Ok(())
}

/// Where ephemeris `id` named `name` is written: `FIELD/EPHEM<id>_<name>.tab`,
/// the name's characters outside letters, digits and `_+-.` replaced by `_`.
fn table_path(root: &Path, id: usize, name: &str) -> PathBuf {
    let name: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "_+-.".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    root.join("FIELD").join(format!("EPHEM{id}_{name}.tab"))
}

/// Write `ephemeris` at `path` ([`refuse_unwritable`] has checked it).
fn write_table(
    path: &Path,
    ephemeris: &FieldEphemeris,
    creation_date: &str,
) -> Result<(), ObservationError> {
    let step = spacing(ephemeris).ok_or(ObservationError::Unsupported {
        what: "writing an MSv2 ephemeris whose samples are not evenly spaced in time",
    })?;
    let schema = TableSchema::new(
        COLUMNS
            .iter()
            .map(|(column, _)| ColumnSchema::scalar(*column, PrimitiveType::Float64))
            .collect(),
    )
    .map_err(storage)?;
    let mut table = Table::with_schema(schema);
    for sample in 0..ephemeris.times_mjd().len() {
        let [ra, dec] = ephemeris.directions_rad()[sample];
        let values = [
            ephemeris.times_mjd()[sample],
            ra.to_degrees(),
            dec.to_degrees(),
            ephemeris.distances_m()[sample] / AU_METRES,
            ephemeris.radial_velocities_m_s()[sample] * 86_400.0 / AU_METRES,
        ];
        table
            .add_row(RecordValue::new(
                COLUMNS
                    .iter()
                    .zip(values)
                    .map(|((column, _), value)| {
                        RecordField::new(*column, Value::Scalar(ScalarValue::Float64(value)))
                    })
                    .collect(),
            ))
            .map_err(storage)?;
    }
    for (column, unit) in COLUMNS {
        table.set_column_keywords(
            column,
            RecordValue::new(vec![RecordField::new("UNIT", string(unit))]),
        );
    }
    let stamp = measures_date(creation_date);
    let posrefsys = match ephemeris.frame() {
        DirectionRef::J2000 => "ICRF/J2000.0",
        _ => "ICRF/ICRS",
    };
    let first = ephemeris.times_mjd()[0];
    for (keyword, value) in [
        ("VS_CREATE", string(&stamp)),
        ("VS_DATE", string(&stamp)),
        ("VS_VERSION", string("0001.0001")),
        ("VS_TYPE", string("Table of comet/planetary positions")),
        ("NAME", string(ephemeris.name())),
        // MeasComet indexes rows from MJD0, one step before the first.
        ("MJD0", number(first - step)),
        ("dMJD", number(step)),
        ("GeoLong", number(0.0)),
        ("GeoLat", number(0.0)),
        ("GeoDist", number(0.0)),
        ("posrefsys", string(posrefsys)),
        ("obsloc", string("GEOCENTRIC")),
    ] {
        table.keywords_mut().push(RecordField::new(keyword, value));
    }
    table.set_info(TableInfo {
        table_type: "IERS".to_string(),
        sub_type: String::new(),
        readme: Vec::new(),
    });
    table.save(TableOptions::new(path)).map_err(storage)
}

fn string(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn number(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

/// An ISO 8601 date (`2026-10-29T13:05:00`) as the Measures tables state
/// `VS_DATE` (`2026/10/29/13:05`), which casacore's `MVTime::read` parses;
/// the Unix epoch for anything else.
fn measures_date(iso: &str) -> String {
    let digits = |range: std::ops::Range<usize>| {
        iso.get(range)
            .filter(|part| part.bytes().all(|byte| byte.is_ascii_digit()))
    };
    match (
        digits(0..4),
        digits(5..7),
        digits(8..10),
        digits(11..13),
        digits(14..16),
    ) {
        (Some(year), Some(month), Some(day), Some(hour), Some(minute)) => {
            format!("{year}/{month}/{day}/{hour}:{minute}")
        }
        _ => "1970/01/01/00:00".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ephemeris(times: Vec<f64>) -> FieldEphemeris {
        let count = times.len();
        FieldEphemeris::new(
            "Moon",
            times,
            vec![[1.0, 0.2]; count],
            DirectionRef::ICRS,
            vec![3.8e8; count],
            (vec![10.0; count], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris")
    }

    #[test]
    fn only_evenly_spaced_samples_have_a_step() {
        let step = 1.0 / 1_440.0;
        let even: Vec<f64> = (0..5).map(|i| 60_000.0 + f64::from(i) * step).collect();
        assert!((spacing(&ephemeris(even.clone())).expect("even") - step).abs() < 1e-12);
        let mut gapped = even;
        gapped.remove(2);
        assert!(spacing(&ephemeris(gapped)).is_none());
    }

    #[test]
    fn creation_dates_become_measures_dates() {
        assert_eq!(measures_date("2026-10-29T13:05:07.5"), "2026/10/29/13:05");
        assert_eq!(measures_date("yesterday"), "1970/01/01/00:00");
    }

    #[test]
    fn table_names_keep_only_safe_characters() {
        assert_eq!(
            table_path(Path::new("x.ms"), 3, "C/2023 A3"),
            Path::new("x.ms/FIELD/EPHEM3_C_2023_A3.tab")
        );
    }
}
