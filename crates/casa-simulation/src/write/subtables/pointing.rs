// SPDX-License-Identifier: LGPL-3.0-or-later
//! POINTING as `NewMSSimulator` writes it: one row per antenna per
//! integration, at the integration's centre, tracking the scan's phase
//! centre.
//!
//! The rows are not held: the columns are generated as the MeasurementSet
//! is saved, from what each scan records. ANTENNA_ID and TIME are computed
//! per row, finding the row's scan among the scans' first rows; DIRECTION,
//! TARGET, INTERVAL and TIME_ORIGIN are runs of one value per scan; NAME,
//! NUM_POLY and TRACKING are constant. The save writes the same files as the
//! rows would (see the tests).

use std::sync::{Arc, Mutex, PoisonError};

use casa_tables::{
    ColumnOverrides, GeneratedArrayColumn, GeneratedArrayValueRun, GeneratedScalarColumn,
    GeneratedScalarValueRun, TableError,
};
use casa_types::{ArrayValue, ScalarValue};
use ndarray::ArrayD;

use crate::session::observe::Scan;

/// What POINTING needs of a scan.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct PointingScan {
    /// Rows per integration: the antennas defined when the scan was observed.
    antennas: usize,
    integrations: usize,
    start_s: f64,
    integration_s: f64,
    /// The J2000 phase centre, radians.
    direction_rad: [f64; 2],
}

impl PointingScan {
    pub(super) fn of(scan: &Scan) -> Self {
        let pointing = Self {
            antennas: scan.antenna_count,
            integrations: scan.integration_count,
            start_s: scan.start_s,
            integration_s: scan.integration_s,
            direction_rad: scan.phase_center_rad,
        };
        debug_assert!(
            scan.integration_times_s()
                .eq(pointing.integration_times_s()),
            "POINTING's TIME follows the scan's integration times"
        );
        pointing
    }

    fn rows(&self) -> usize {
        self.antennas * self.integrations
    }

    /// The integrations' centres, as [`Scan::integration_times_s`]
    /// accumulates them.
    fn integration_times_s(&self) -> impl Iterator<Item = f64> + '_ {
        let mut time = self.start_s;
        (0..self.integrations).map(move |_| {
            let centre = time + self.integration_s / 2.0;
            time += self.integration_s;
            centre
        })
    }
}

/// The scans that have rows, and the first POINTING row of each.
struct Layout {
    scans: Vec<PointingScan>,
    first_rows: Vec<usize>,
}

impl Layout {
    fn new(scans: &[PointingScan]) -> (Self, usize) {
        let mut layout = Self {
            scans: Vec::new(),
            first_rows: Vec::new(),
        };
        let mut rows = 0;
        for scan in scans.iter().filter(|scan| scan.rows() > 0) {
            layout.scans.push(*scan);
            layout.first_rows.push(rows);
            rows += scan.rows();
        }
        (layout, rows)
    }

    /// The scan holding `row`, and the row's place in it.
    fn locate(&self, row: usize) -> (usize, usize) {
        let scan = self.first_rows.partition_point(|&first| first <= row) - 1;
        (scan, row - self.first_rows[scan])
    }
}

/// Where the accumulation of a scan's integration times stands: the running
/// time at the start of `integration`.
struct TimeCursor {
    scan: usize,
    integration: usize,
    time_s: f64,
}

/// The integration centres of POINTING's rows.
///
/// A centre is the scan start plus a running sum of integration times, as
/// MAIN's TIME is, so the two agree bit for bit; summing in another order
/// would not. The cursor resumes the sum where the last row left it, which
/// a writer asking for the rows in order makes one addition per
/// integration, and restarts it from the scan start for an earlier row.
struct IntegrationTimes {
    layout: Arc<Layout>,
    cursor: Mutex<TimeCursor>,
}

impl IntegrationTimes {
    fn new(layout: Arc<Layout>) -> Self {
        let start_s = layout.scans.first().map_or(0.0, |scan| scan.start_s);
        Self {
            layout,
            cursor: Mutex::new(TimeCursor {
                scan: 0,
                integration: 0,
                time_s: start_s,
            }),
        }
    }

    fn centre_s(&self, scan: usize, integration: usize) -> f64 {
        let pointing = &self.layout.scans[scan];
        let mut cursor = self.cursor.lock().unwrap_or_else(PoisonError::into_inner);
        if cursor.scan != scan || cursor.integration > integration {
            *cursor = TimeCursor {
                scan,
                integration: 0,
                time_s: pointing.start_s,
            };
        }
        while cursor.integration < integration {
            cursor.time_s += pointing.integration_s;
            cursor.integration += 1;
        }
        cursor.time_s + pointing.integration_s / 2.0
    }
}

/// A direction cell (`[2, 1]`): one polynomial term of (longitude,
/// latitude).
fn direction(direction_rad: [f64; 2]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], direction_rad.to_vec()).expect("shape matches values"),
    )
}

/// The POINTING columns of `scans` as generated overrides, for a save.
pub(super) fn pointing_overrides(scans: &[PointingScan]) -> Result<ColumnOverrides, TableError> {
    let (layout, rows) = Layout::new(scans);
    let layout = Arc::new(layout);
    let mut overrides = ColumnOverrides::for_row_count(rows);

    let antennas = Arc::clone(&layout);
    overrides.insert_generated_scalar(
        "ANTENNA_ID",
        GeneratedScalarColumn::new(rows, move |row| {
            let (scan, offset) = antennas.locate(row);
            Some(ScalarValue::Int32(
                (offset % antennas.scans[scan].antennas) as i32,
            ))
        }),
    );
    let times = IntegrationTimes::new(Arc::clone(&layout));
    overrides.insert_generated_scalar(
        "TIME",
        GeneratedScalarColumn::new(rows, move |row| {
            let (scan, offset) = times.layout.locate(row);
            let integration = offset / times.layout.scans[scan].antennas;
            Some(ScalarValue::Float64(times.centre_s(scan, integration)))
        }),
    );

    let starts = || layout.first_rows.iter().copied().zip(&layout.scans);
    for column in ["DIRECTION", "TARGET"] {
        let runs = starts()
            .map(|(start, scan)| {
                GeneratedArrayValueRun::new(start, Some(direction(scan.direction_rad)))
            })
            .collect();
        overrides
            .insert_generated_array(column, GeneratedArrayColumn::from_array_runs(rows, runs)?);
    }
    for (column, value) in [
        (
            "INTERVAL",
            (|scan| scan.integration_s) as fn(&PointingScan) -> f64,
        ),
        ("TIME_ORIGIN", |scan| scan.start_s),
    ] {
        let runs = starts()
            .map(|(start, scan)| {
                GeneratedScalarValueRun::new(start, Some(ScalarValue::Float64(value(scan))))
            })
            .collect();
        overrides
            .insert_generated_scalar(column, GeneratedScalarColumn::from_scalar_runs(rows, runs)?);
    }
    for (column, value) in [
        ("NAME", ScalarValue::String(String::new())),
        ("NUM_POLY", ScalarValue::Int32(0)),
        ("TRACKING", ScalarValue::Bool(true)),
    ] {
        overrides
            .insert_generated_scalar(column, GeneratedScalarColumn::constant(rows, Some(value)));
    }
    Ok(overrides)
}

#[cfg(test)]
mod tests {
    //! The generated POINTING writes the files of today's rows, written as
    //! the simulator wrote them before it generated the columns.

    use std::path::Path;

    use casa_ms::schema::SubtableId;
    use casa_ms::{MeasurementSet, MeasurementSetBuilder, SubtableColumnOverrides};
    use casa_types::RecordValue;

    use super::super::{direction_cell, row, scalar_bool, scalar_f64, scalar_i32, scalar_string};
    use super::*;

    /// POINTING's rows as the simulator held them: one record per antenna
    /// per integration.
    fn pointing_rows(scans: &[PointingScan]) -> Vec<RecordValue> {
        let mut rows = Vec::new();
        for scan in scans {
            let direction = direction_cell(scan.direction_rad);
            for time_s in scan.integration_times_s() {
                for antenna in 0..scan.antennas {
                    rows.push(row(
                        SubtableId::Pointing,
                        vec![
                            ("ANTENNA_ID", scalar_i32(antenna as i32)),
                            ("DIRECTION", direction.clone()),
                            ("INTERVAL", scalar_f64(scan.integration_s)),
                            ("NAME", scalar_string("")),
                            ("NUM_POLY", scalar_i32(0)),
                            ("TARGET", direction.clone()),
                            ("TIME", scalar_f64(time_s)),
                            ("TIME_ORIGIN", scalar_f64(scan.start_s)),
                            ("TRACKING", scalar_bool(true)),
                        ],
                    ));
                }
            }
        }
        rows
    }

    fn scan(
        antennas: usize,
        integrations: usize,
        start_s: f64,
        integration_s: f64,
        direction_rad: [f64; 2],
    ) -> PointingScan {
        PointingScan {
            antennas,
            integrations,
            start_s,
            integration_s,
            direction_rad,
        }
    }

    /// Scans of different fields, integration times and antenna counts,
    /// one without antennas, two in a row on one field, and enough rows for
    /// several buckets.
    fn scans() -> Vec<PointingScan> {
        let start = 5.08e9 + 0.123;
        vec![
            scan(4, 7, start, 2.7, [1.2, -0.4]),
            scan(6, 300, start + 30.0, 10.0, [2.5, 0.7]),
            scan(0, 12, start + 3100.0, 10.0, [2.5, 0.7]),
            scan(6, 40, start + 3200.0, 0.1, [2.5, 0.7]),
            scan(3, 1, start + 3300.0, 1.0 / 3.0, [0.0, 1.5]),
            scan(5, 450, start + 3301.7, 4.4, [-0.3, -1.1]),
        ]
    }

    /// Every file of a table, by name.
    fn files(path: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files = std::fs::read_dir(path)
            .expect("table directory")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.is_file())
            .map(|file| {
                let name = file.file_name().unwrap().to_string_lossy().into_owned();
                (name, std::fs::read(&file).expect("read file"))
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    /// Save a MeasurementSet whose POINTING holds `scans`, as rows or as
    /// generated columns, and return the POINTING files.
    fn pointing_files(
        root: &Path,
        scans: &[PointingScan],
        generated: bool,
    ) -> Vec<(String, Vec<u8>)> {
        let path = root.join(if generated { "generated.ms" } else { "rows.ms" });
        let mut ms = MeasurementSet::create(&path, MeasurementSetBuilder::new()).expect("create");
        if generated {
            let mut subtables = SubtableColumnOverrides::new();
            subtables.insert(
                SubtableId::Pointing,
                pointing_overrides(scans).expect("overrides"),
            );
            ms.save_with_column_overrides(&ColumnOverrides::new(), &subtables)
                .expect("save generated");
        } else {
            let table = ms.subtable_mut(SubtableId::Pointing).expect("POINTING");
            for record in pointing_rows(scans) {
                table.add_row(record).expect("row");
            }
            ms.save().expect("save rows");
        }
        files(&path.join("POINTING"))
    }

    #[test]
    fn generated_pointing_writes_the_files_of_its_rows() {
        for scans in [
            scans(),
            scans()[..1].to_vec(),
            scans()[2..3].to_vec(),
            Vec::new(),
        ] {
            let root = tempfile::tempdir().expect("tempdir");
            let rows = pointing_files(root.path(), &scans, false);
            let generated = pointing_files(root.path(), &scans, true);
            let names = rows
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>();
            assert!(names.contains(&"table.f0"), "{names:?}");
            assert!(
                generated == rows,
                "POINTING files differ for {} scans",
                scans.len()
            );
        }
    }

    #[test]
    fn integration_centres_accumulate_as_the_scan_does_in_any_order() {
        let scans = scans();
        let (layout, _) = Layout::new(&scans);
        let layout = Arc::new(layout);
        let expected = layout
            .scans
            .iter()
            .map(|scan| scan.integration_times_s().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let times = IntegrationTimes::new(Arc::clone(&layout));
        for (scan, centres) in expected.iter().enumerate() {
            for (integration, centre) in centres.iter().enumerate() {
                assert_eq!(
                    times.centre_s(scan, integration).to_bits(),
                    centre.to_bits()
                );
            }
        }
        // Out of order, as another writer might ask; the scan without
        // antennas has no rows, so the last scan is the fifth.
        for (scan, integration) in [
            (1, 299),
            (1, 3),
            (0, 6),
            (4, 449),
            (4, 0),
            (1, 150),
            (2, 39),
        ] {
            assert_eq!(
                times.centre_s(scan, integration).to_bits(),
                expected[scan][integration].to_bits(),
                "scan {scan} integration {integration}"
            );
        }
        // A closed form differs in the last bits.
        assert!(layout.scans.iter().zip(&expected).any(|(scan, centres)| {
            centres.iter().enumerate().any(|(integration, centre)| {
                scan.start_s + (integration as f64 + 0.5) * scan.integration_s != *centre
            })
        }));
    }

    #[test]
    fn rows_are_located_in_their_scans() {
        let scans = scans();
        let (layout, rows) = Layout::new(&scans);
        assert_eq!(rows, scans.iter().map(PointingScan::rows).sum::<usize>());
        assert_eq!(
            layout.scans.len(),
            5,
            "the scan without antennas has no rows"
        );
        assert_eq!(layout.locate(0), (0, 0));
        assert_eq!(layout.locate(27), (0, 27));
        assert_eq!(layout.locate(28), (1, 0));
        assert_eq!(layout.locate(28 + 1800), (2, 0));
        assert_eq!(layout.locate(rows - 1), (4, 5 * 450 - 1));
    }
}
