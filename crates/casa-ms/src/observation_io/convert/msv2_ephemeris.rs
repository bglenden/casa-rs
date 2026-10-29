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

use super::Loss;
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

/// The keywords whose values are carried; anything else a source table
/// states is not. The `VS_*` set is written too, but anew: it dates and
/// versions the table written, so a source table's own is not carried.
pub(super) const KEYWORDS: [&str; 8] = [
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
        if table_grid(ephemeris).is_none() {
            return Err(ObservationError::Unsupported {
                what: "writing an MSv2 ephemeris whose samples are not on one evenly spaced \
                       grid of times (casacore's MeasComet reads only evenly spaced tables)",
            });
        }
    }
    Ok(())
}

/// The most rows a table this writes may hold.
const MOST_ROWS: usize = 1_000_000;

/// The evenly spaced times of an MSv2 table of an ephemeris: its step
/// (days) and, at each of its rows, the sample there, or `None` for a time
/// between samples. A union of datasets' samples has gaps (two sessions,
/// say), but its samples lie on the original table's grid.
struct TableGrid {
    step: f64,
    rows: Vec<Option<usize>>,
}

/// [`TableGrid`] of `ephemeris`, when every sample is on the grid of the
/// smallest step (to [`SPACING_TOLERANCE_DAYS`]) and the table holds at
/// most [`MOST_ROWS`] rows.
fn table_grid(ephemeris: &FieldEphemeris) -> Option<TableGrid> {
    let times = ephemeris.times_mjd();
    let first = times[0];
    let smallest = times
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .fold(f64::INFINITY, f64::min);
    // The step from the whole span, which rounding disturbs least.
    let span = ((times[times.len() - 1] - first) / smallest).round();
    if !(span.is_finite() && span >= 1.0 && span < MOST_ROWS as f64) {
        return None;
    }
    let step = (times[times.len() - 1] - first) / span;
    let mut rows = vec![None; span as usize + 1];
    for (sample, time) in times.iter().enumerate() {
        let row = ((time - first) / step).round();
        if (time - (first + row * step)).abs() > SPACING_TOLERANCE_DAYS {
            return None;
        }
        rows[row as usize] = Some(sample);
    }
    Some(TableGrid { step, rows })
}

/// Each ephemeris the catalogue's fields follow whose samples have gaps on
/// their grid, as a loss: an MSv2 table holds every row of it, so the rows
/// in the gaps are the ephemeris's own interpolation
/// ([`FieldEphemeris::sample`]), as casacore and casa-rs would evaluate it
/// there.
pub(super) fn filled_rows(catalogue: &Catalogue) -> Vec<Loss> {
    followed(catalogue)
        .0
        .into_iter()
        .filter_map(|ephemeris| {
            let grid = table_grid(ephemeris)?;
            let filled = grid.rows.iter().filter(|row| row.is_none()).count();
            (filled > 0).then(|| {
                Loss::Column(format!(
                    "the ephemeris of {}: that its samples have gaps ({filled} rows filled by \
                     its interpolation, as casacore's MeasComet reads only evenly spaced \
                     tables)",
                    ephemeris.name()
                ))
            })
        })
        .collect()
}

/// The table row at `mjd` of `ephemeris`: `MJD`, `RA` and `DEC` (degrees,
/// the ephemeris's frame), `Rho` (AU) and `RadVel` (AU a day), from
/// `sample` or, between samples, from the ephemeris's interpolation.
fn table_row(ephemeris: &FieldEphemeris, mjd: f64, sample: Option<usize>) -> Option<[f64; 5]> {
    let (time, [ra, dec], distance, velocity) = match sample {
        Some(sample) => (
            ephemeris.times_mjd()[sample],
            ephemeris.directions_rad()[sample],
            ephemeris.distances_m()[sample],
            ephemeris.radial_velocities_m_s()[sample],
        ),
        None => {
            let at = ephemeris.sample(mjd)?;
            let [x, y, z] = at.position_m;
            let distance = (x * x + y * y + z * z).sqrt();
            (
                mjd,
                [
                    y.atan2(x).rem_euclid(std::f64::consts::TAU),
                    (z / distance).asin(),
                ],
                distance,
                at.radial_velocity_m_s,
            )
        }
    };
    Some([
        time,
        ra.to_degrees(),
        dec.to_degrees(),
        distance / AU_METRES,
        velocity * 86_400.0 / AU_METRES,
    ])
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
    let TableGrid { step, rows } = table_grid(ephemeris).ok_or(ObservationError::Unsupported {
        what: "writing an MSv2 ephemeris whose samples are not on one evenly spaced grid",
    })?;
    let schema = TableSchema::new(
        COLUMNS
            .iter()
            .map(|(column, _)| ColumnSchema::scalar(*column, PrimitiveType::Float64))
            .collect(),
    )
    .map_err(storage)?;
    let mut table = Table::with_schema(schema);
    let first = ephemeris.times_mjd()[0];
    for (row, sample) in rows.into_iter().enumerate() {
        let values = table_row(ephemeris, first + row as f64 * step, sample).ok_or(
            ObservationError::Unsupported {
                what: "writing an MSv2 ephemeris row outside its samples",
            },
        )?;
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
    fn samples_on_one_grid_fill_their_gaps_and_others_have_none() {
        let step = 1.0 / 1_440.0;
        let even: Vec<f64> = (0..5).map(|i| 60_000.0 + f64::from(i) * step).collect();
        let grid = table_grid(&ephemeris(even.clone())).expect("even");
        assert!((grid.step - step).abs() < 1e-12);
        assert!(grid.rows.iter().all(Option::is_some));
        let mut gapped = even.clone();
        gapped.remove(2);
        let gapped = ephemeris(gapped);
        let grid = table_grid(&gapped).expect("on the grid");
        assert_eq!(grid.rows, [Some(0), Some(1), None, Some(2), Some(3)]);
        let filled = table_row(&gapped, even[2], None).expect("between samples");
        let sample = gapped.sample(even[2]).expect("between samples");
        assert!((filled[3] * AU_METRES - 3.8e8).abs() < 1e-3);
        assert!((filled[4] * AU_METRES / 86_400.0 - sample.radial_velocity_m_s).abs() < 1e-12);
        let mut uneven = even;
        uneven[2] += 10.0 / 86_400.0;
        assert!(table_grid(&ephemeris(uneven)).is_none());
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
