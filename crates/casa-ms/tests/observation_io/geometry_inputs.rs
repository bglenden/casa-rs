// SPDX-License-Identifier: LGPL-3.0-or-later
//! Geometry's inputs on the boundary (#932, step 3a;
//! `docs/measurement-set/imaging-port.md` §2.2.1): what imaging's per-row
//! geometry reads from a MeasurementSet today, carried by both stores.

use std::path::Path;

use casa_ms::observation_io::convert::{Loss, LossKind};
use casa_ms::observation_io::{OpenOptions, open};
use casa_ms::{MeasurementSet, SubtableId};
use casa_observation::{
    AntennaId, ObservationError, PartitionId, PointingDirection, PointingRequest,
};
use casa_tables::ColumnSchema;
use casa_tables::table_measures::{MeasureType, TableMeasDesc, default_direction_ref_map};
use casa_types::measures::EpochRef;
use casa_types::measures::direction::DirectionRef;
use casa_types::{PrimitiveType, ScalarValue, Value};

use super::fields::mosaic;

/// Give the MSv2 at `path` a variable `PHASE_DIR` reference, as ALMA's
/// `PhaseDir_Ref`: an integer column holding each field's casacore code.
fn variable_phase_references(path: &Path, frames: impl Fn(usize) -> DirectionRef) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let field = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    field
        .add_column(
            ColumnSchema::scalar("PhaseDir_Ref", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .expect("column");
    for row in 0..field.row_count() {
        let code = frames(row).casacore_code();
        field
            .fill_column_range(
                "PhaseDir_Ref",
                casa_tables::RowRange::new(row, row + 1),
                Value::Scalar(ScalarValue::Int32(code)),
            )
            .expect("code");
    }
    let (types, codes) = default_direction_ref_map();
    TableMeasDesc::new_variable_int(
        "PHASE_DIR",
        MeasureType::Direction,
        "PhaseDir_Ref",
        types,
        codes,
    )
    .expect("measure")
    .write(field)
    .expect("write");
    ms.save().expect("save");
}

/// Each field's phase centre keeps its stored angles and resolves its frame
/// per row from a variable reference column, and carries `FIELD.TIME` as
/// its reference epoch; fields in different frames are refused. An MSv4
/// field has no reference epoch: its frame is celestial.
#[test]
fn field_directions_resolve_variable_frames_and_carry_reference_epochs() {
    let root = tempfile::tempdir().expect("root");
    let variant = |name: &str| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).expect("dir");
        mosaic(&dir, 4)
    };
    let (fixed, v4) = variant("fixed");
    let stored = open(&fixed, &OpenOptions::default()).expect("open");
    let stored = stored.catalogue().fields.clone();
    assert_eq!(stored.len(), 3);
    let times: Vec<f64> = {
        let ms = MeasurementSet::open(&fixed).expect("ms");
        let field = ms.field().expect("FIELD");
        (0..field.row_count())
            .map(|row| field.time(row).expect("TIME"))
            .collect()
    };
    for (field, time_s) in stored.iter().zip(&times) {
        let epoch = field.reference_epoch.as_ref().expect("FIELD.TIME");
        // As the imaging engine states it (`MEpoch::from_mjd(TIME / 86400)`).
        assert_eq!(epoch.value().as_mjd(), time_s / 86_400.0);
        assert_eq!(epoch.refer(), EpochRef::UTC);
    }

    let (path, _) = variant("variable");
    variable_phase_references(&path, |_| DirectionRef::J2000);
    let opened = open(&path, &OpenOptions::default()).expect("variable");
    for (read, field) in opened.catalogue().fields.iter().zip(&stored) {
        assert_eq!(read.phase_centre.refer(), DirectionRef::J2000);
        assert_eq!(
            read.phase_centre.as_angles(),
            field.phase_centre.as_angles()
        );
    }

    let (path, _) = variant("mixed");
    variable_phase_references(&path, |row| {
        if row == 0 {
            DirectionRef::J2000
        } else {
            DirectionRef::ICRS
        }
    });
    assert!(matches!(
        open(&path, &OpenOptions::default()),
        Err(ObservationError::Unsupported { .. })
    ));

    let v4 = open(&v4, &OpenOptions::default()).expect("msv4");
    assert!(
        v4.catalogue()
            .fields
            .iter()
            .all(|field| field.reference_epoch.is_none())
    );
}

/// One `POINTING` row of the simulated mosaic (`NUM_POLY` 0).
#[derive(Clone, Copy, Debug)]
struct Row {
    antenna: u32,
    time_s: f64,
    interval_s: f64,
    direction: [f64; 2],
}

fn pointing_rows(path: &Path) -> Vec<Row> {
    let ms = MeasurementSet::open(path).expect("ms");
    let pointing = ms.pointing().expect("POINTING");
    (0..pointing.row_count())
        .map(|row| {
            let direction = match pointing.array(row, "DIRECTION").expect("DIRECTION") {
                casa_types::ArrayValue::Float64(values) => [values[[0, 0]], values[[1, 0]]],
                other => panic!("DIRECTION is {other:?}"),
            };
            Row {
                antenna: u32::try_from(pointing.i32(row, "ANTENNA_ID").expect("antenna"))
                    .expect("antenna"),
                time_s: pointing.f64(row, "TIME").expect("TIME"),
                interval_s: pointing.f64(row, "INTERVAL").expect("INTERVAL"),
                direction,
            }
        })
        .collect()
}

/// The rows today's casa-rs reader brackets `time_s` with for
/// `antenna`: the row whose interval holds it (nearest first), the last at
/// or before it and the first at or after it.
fn expected(rows: &[Row], antenna: u32, time_s: f64) -> [Option<Row>; 3] {
    let mut own: Vec<Row> = rows
        .iter()
        .copied()
        .filter(|row| row.antenna == antenna)
        .collect();
    own.sort_by(|a, b| a.time_s.total_cmp(&b.time_s));
    let covering = own
        .iter()
        .copied()
        .filter(|row| row.interval_s > 0.0 && (row.time_s - time_s).abs() <= 0.5 * row.interval_s)
        .min_by(|a, b| {
            (a.time_s - time_s)
                .abs()
                .total_cmp(&(b.time_s - time_s).abs())
        });
    let before = own.iter().copied().rev().find(|row| row.time_s <= time_s);
    let after = own.iter().copied().find(|row| row.time_s >= time_s);
    [covering, before, after]
}

fn series(observation: &dyn casa_observation::Observation) -> casa_observation::PointingSeries {
    let grid = observation.grid(PartitionId(0), "base").expect("grid");
    observation
        .pointing(&PointingRequest {
            partition: PartitionId(0),
            direction: PointingDirection::Beam,
            antennas: (0..3).map(AntennaId).collect(),
            times: grid.times_s[0]..=grid.times_s[grid.times_s.len() - 1],
            budget_bytes: 1 << 20,
        })
        .expect("pointing")
}

/// Each store's pointing brackets a time with its own samples, evaluated
/// there: an MSv2's are the `POINTING` rows today's casa-rs reader names
/// (the nearest row whose positive `INTERVAL` covers the time, the last
/// before and the first after; casacore's `pointingIndex` differs, #948);
/// its MSv4 conversion holds the same samples at the same instants, with no
/// intervals, so nothing covers; and converting back writes the same rows,
/// with `INTERVAL` 0.
#[test]
fn pointing_brackets_hold_each_stores_samples_and_survive_conversion() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 4);
    let rows = pointing_rows(&v2);
    assert!(!rows.is_empty());
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    let msv4 = open(&v4, &OpenOptions::default()).expect("msv4");
    let (from_v2, from_v4) = (series(msv2.as_ref()), series(msv4.as_ref()));
    assert_eq!(from_v2.frame, DirectionRef::J2000);
    assert_eq!((from_v4.frame, from_v4.terms), (DirectionRef::J2000, 1));
    // Unix seconds from MJD seconds, as the MSv4 writer states its times.
    let unix = |mjd_s: f64| mjd_s - 40_587.0 * 86_400.0;
    let grid = msv2.grid(PartitionId(0), "base").expect("grid");
    let mut queries: Vec<f64> = grid
        .times_s
        .iter()
        .flat_map(|time| [time - 3.0, *time, time + 3.0])
        .collect();
    queries.extend([
        grid.times_s[0] - 20.0,
        grid.times_s[grid.times_s.len() - 1] + 20.0,
    ]);
    for antenna in 0..3 {
        for time in &queries {
            let bracket = from_v2.bracket(AntennaId(antenna), *time).expect("v2");
            let [covering, before, after] = expected(&rows, antenna, *time);
            let seen = [bracket.covering, bracket.before, bracket.after]
                .map(|candidate| candidate.map(|c| (c.time_s, c.direction_rad)));
            let wanted = [covering, before, after].map(|row| row.map(|r| (r.time_s, r.direction)));
            assert_eq!(seen, wanted, "MSv2 antenna {antenna} at {time}");

            let on_v4 = from_v4
                .bracket(AntennaId(antenna), unix(*time))
                .expect("v4");
            assert_eq!(on_v4.covering, None);
            for (v4, v2) in [(on_v4.before, bracket.before), (on_v4.after, bracket.after)] {
                assert_eq!(
                    v4.map(|c| (c.time_s, c.direction_rad)),
                    v2.map(|c| (unix(c.time_s), c.direction_rad)),
                    "MSv4 antenna {antenna} at {time}"
                );
            }
        }
    }

    let back = root.path().join("back.ms");
    casa_ms::observation_io::convert::to_msv2(
        &v4,
        &back,
        &casa_ms::observation_io::convert::ConversionOptions {
            data_groups: None,
            accept: vec![LossKind::Sigma, LossKind::Tables, LossKind::Columns],
            batch_bytes: 1 << 20,
            workers: 1,
            chunks: [0, 0, 0, 0],
            codec: casa_ms::observation_io::Msv4Codec::None,
            padding: casa_ms::observation_io::convert::PaddingRows::Flagged,
            creator: "casa-rs".to_string(),
            creation_date: "2026-10-27T00:00:00+00:00".to_string(),
        },
    )
    .expect("to msv2");
    let mut again = pointing_rows(&back);
    let mut first = rows.clone();
    for set in [&mut again, &mut first] {
        set.sort_by(|a, b| {
            (a.antenna, a.time_s)
                .partial_cmp(&(b.antenna, b.time_s))
                .expect("ordered")
        });
    }
    assert_eq!(again.len(), first.len());
    for (again, first) in again.iter().zip(&first) {
        assert_eq!(
            (
                again.antenna,
                again.time_s,
                again.direction,
                again.interval_s
            ),
            (first.antenna, first.time_s, first.direction, 0.0)
        );
    }
}

/// Give every `POINTING` row of the MSv2 at `path` a rate term:
/// `NUM_POLY` 1, direction `[ra + 1e-6 r, dec - 2e-6 r]` per second `r`
/// since its `TIME_ORIGIN` (the simulator's, half an integration before
/// its time).
fn rate_terms(path: &Path) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    {
        let mut pointing = ms.pointing_mut().expect("POINTING");
        for row in 0..pointing.row_count() {
            let [ra, dec] = match pointing
                .as_ref()
                .array(row, "DIRECTION")
                .expect("DIRECTION")
            {
                casa_types::ArrayValue::Float64(values) => [values[[0, 0]], values[[1, 0]]],
                other => panic!("DIRECTION is {other:?}"),
            };
            let terms = ndarray::arr2(&[[ra, 1e-6], [dec, -2e-6]]).into_dyn();
            pointing.set_i32(row, "NUM_POLY", 1).expect("NUM_POLY");
            for column in ["DIRECTION", "TARGET"] {
                pointing
                    .set_array(row, column, casa_types::ArrayValue::Float64(terms.clone()))
                    .expect("terms");
            }
        }
    }
    ms.save().expect("save");
}

/// One partition's pointing for antennas 0 to 2 over `times`.
fn series_over(
    observation: &dyn casa_observation::Observation,
    times: std::ops::RangeInclusive<f64>,
) -> casa_observation::PointingSeries {
    observation
        .pointing(&PointingRequest {
            partition: PartitionId(0),
            direction: PointingDirection::Beam,
            antennas: (0..3).map(AntennaId).collect(),
            times,
            budget_bytes: 1 << 20,
        })
        .expect("pointing")
}

/// An MSv4's pointing grid is the union of its antennas' sample times, NaN
/// where an antenna has none. Each antenna's nearest sample either side of
/// the requested times is its own, past however many of the others' times;
/// an antenna the pointing does not name has none. The MSv2 it came from
/// says the same.
#[test]
fn msv4_pointing_finds_each_antennas_own_nearest_samples() {
    let root = tempfile::tempdir().expect("root");
    let (v2, _) = mosaic(root.path(), 4);
    // Antenna 1 sampled 1 s after the others; antenna 2 not at all.
    {
        let mut ms = MeasurementSet::open(&v2).expect("ms");
        let mut gone = Vec::new();
        {
            let mut pointing = ms.pointing_mut().expect("POINTING");
            for row in 0..pointing.row_count() {
                match pointing.as_ref().i32(row, "ANTENNA_ID").expect("antenna") {
                    1 => {
                        for column in ["TIME", "TIME_ORIGIN"] {
                            let time = pointing.as_ref().f64(row, column).expect("time");
                            pointing.set_f64(row, column, time + 1.0).expect("later");
                        }
                    }
                    2 => gone.push(row),
                    _ => {}
                }
            }
        }
        ms.subtable_mut(SubtableId::Pointing)
            .expect("POINTING")
            .remove_rows(&gone)
            .expect("remove");
        ms.save().expect("save");
    }
    let v4 = root.path().join("staggered.ps.zarr");
    casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion()).expect("to msv4");
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    let msv4 = open(&v4, &OpenOptions::default()).expect("msv4");
    let unix = |mjd_s: f64| mjd_s - 40_587.0 * 86_400.0;
    let grid = msv2.grid(PartitionId(0), "base").expect("grid");
    for time in &grid.times_s {
        // No antenna has a sample within these times.
        let (start, end) = (time + 0.2, time + 0.4);
        let on_v2 = series_over(msv2.as_ref(), start..=end);
        let on_v4 = series_over(msv4.as_ref(), unix(start)..=unix(end));
        assert!(!on_v4.antennas.contains_key(&AntennaId(2)));
        assert!(!on_v2.antennas.contains_key(&AntennaId(2)));
        for antenna in [AntennaId(0), AntennaId(1)] {
            let samples = &on_v4.antennas[&antenna];
            assert!(!samples.times_s().is_empty() && samples.times_s().len() <= 2);
            let query = time + 0.3;
            let v2 = on_v2.bracket(antenna, query).expect("v2");
            let v4 = on_v4.bracket(antenna, unix(query)).expect("v4");
            for (v4, v2) in [(v4.before, v2.before), (v4.after, v2.after)] {
                assert_eq!(
                    v4.map(|c| (c.time_s, c.direction_rad)),
                    v2.map(|c| (unix(c.time_s), c.direction_rad)),
                    "{antenna:?} at {query}"
                );
            }
            if antenna == AntennaId(1) {
                assert!(v4.after.is_some(), "antenna 1's next sample, a second on");
            }
        }
    }
}

/// The frame of the MSv2 at `path`'s `POINTING` directions.
fn pointing_frame(path: &Path) -> String {
    let ms = MeasurementSet::open(path).expect("ms");
    let table = ms.subtable(SubtableId::Pointing).expect("POINTING");
    match TableMeasDesc::reconstruct(table, "DIRECTION").map(|desc| desc.ref_desc().clone()) {
        Some(casa_tables::table_measures::MeasRefDesc::Fixed { refer }) => refer,
        other => panic!("DIRECTION's reference is {other:?}"),
    }
}

/// Edit the MSv2 at `path`'s `POINTING`: label its directions `frame`, and
/// add a copy of row 0 `later_s` later for each of `copies`.
fn edit_pointing(path: &Path, frame: Option<&str>, copies: &[f64]) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let table = ms.subtable_mut(SubtableId::Pointing).expect("POINTING");
    if let Some(frame) = frame {
        for column in ["DIRECTION", "TARGET"] {
            TableMeasDesc::new_fixed(column, MeasureType::Direction, frame)
                .write(table)
                .expect("frame");
        }
    }
    for later_s in copies {
        let mut copy = table.row_accessor().row(0).expect("row").clone();
        for column in ["TIME", "TIME_ORIGIN"] {
            let time = match copy.get(column) {
                Some(Value::Scalar(ScalarValue::Float64(time))) => *time,
                other => panic!("{column} is {other:?}"),
            };
            copy.upsert(column, Value::Scalar(ScalarValue::Float64(time + later_s)));
        }
        table.add_row(copy).expect("add");
    }
    ms.save().expect("save");
}

/// What the destination cannot hold of the pointing is found before
/// anything is written. Directions in a frame MSv4 names keep it both ways;
/// in one it does not name, the MSv4 conversion loses the pointing whole
/// (an MSv2 destination keeps it). A row at the antenna and time of another
/// is a loss into an MSv4, which holds one sample there, and kept into an
/// MSv2; rows beyond the partition's times and each antenna's nearest
/// either side are a counted loss; rows a read would refuse lose the
/// pointing whole. Partitions sharing an MSv2's rows write them once.
#[test]
fn pointing_the_destination_cannot_hold_is_reported_before_writing() {
    let root = tempfile::tempdir().expect("root");
    let variant = |name: &str| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).expect("dir");
        mosaic(&dir, 4).0
    };
    let losses = |path: &Path, to_v4: bool| {
        let out = path.with_extension(if to_v4 { "ps.zarr" } else { "copy.ms" });
        let report = if to_v4 {
            casa_ms::observation_io::convert::to_msv4(path, &out, &conversion())
        } else {
            casa_ms::observation_io::convert::to_msv2(path, &out, &conversion())
        }
        .expect("convert");
        let pointing: Vec<String> = report
            .losses
            .iter()
            .filter_map(|loss| match loss {
                Loss::Table(table) if table.starts_with("POINTING") => Some(table.clone()),
                _ => None,
            })
            .collect();
        (out, pointing)
    };
    let columns = "POINTING INTERVAL, NAME, TARGET, TRACKING".to_string();

    let v2 = variant("azel");
    edit_pointing(&v2, Some("AZELGEO"), &[]);
    let (v4, lost) = losses(&v2, true);
    assert_eq!(lost, std::slice::from_ref(&columns));
    let (back, _) = losses(&v4, false);
    assert_eq!(pointing_frame(&back), "AZELGEO");
    assert_eq!(pointing_rows(&back).len(), pointing_rows(&v2).len());

    let v2 = variant("galactic");
    edit_pointing(&v2, Some("GALACTIC"), &[]);
    let (v4, lost) = losses(&v2, true);
    assert_eq!(
        lost,
        ["POINTING (directions in GALACTIC, which MSv4 does not name)"]
    );
    let dataset = std::fs::read_dir(&v4)
        .expect("datasets")
        .map(|entry| entry.expect("entry").path())
        .find(|path| path.join("zarr.json").is_file())
        .expect("a dataset");
    assert!(
        !dataset.join("pointing_xds").exists(),
        "no pointing written"
    );
    let (copy, lost) = losses(&v2, false);
    assert_eq!(lost, ["POINTING NAME, TARGET, TRACKING"]);
    assert_eq!(pointing_frame(&copy), "GALACTIC");
    assert_eq!(pointing_rows(&copy).len(), pointing_rows(&v2).len());

    // Row 0 again at its time, and twice far before the observation: the
    // nearer of those is antenna 0's nearest sample before it.
    let v2 = variant("extra");
    edit_pointing(&v2, None, &[0.0, -1.0e5, -2.0e5]);
    let (_, lost) = losses(&v2, true);
    assert_eq!(
        lost,
        [
            columns,
            "POINTING rows at the antenna and time of an earlier one (1)".to_string(),
            "POINTING rows beyond every partition's times and each antenna's nearest either \
             side (1)"
                .to_string(),
        ]
    );
    let (copy, _) = losses(&v2, false);
    assert_eq!(pointing_rows(&copy).len(), pointing_rows(&v2).len());

    // A row whose times are not finite would refuse the reads, so the
    // pointing is lost whole.
    let v2 = variant("nan");
    edit_pointing(&v2, None, &[f64::NAN]);
    let (_, lost) = losses(&v2, true);
    assert_eq!(lost, ["POINTING (rows of non-finite times)"]);

    let dir = root.path().join("two");
    std::fs::create_dir(&dir).expect("dir");
    let v2 = two_windows(&dir);
    let (v4, _) = losses(&v2, true);
    let (back, _) = losses(&v4, false);
    assert_eq!(
        pointing_rows(&back).len(),
        pointing_rows(&v2).len(),
        "written once"
    );
    let (copy, _) = losses(&v2, false);
    assert_eq!(
        pointing_rows(&copy).len(),
        pointing_rows(&v2).len(),
        "each antenna read once"
    );
}

/// A pointing polynomial in time is evaluated at the query time on the
/// MSv2, about its `TIME_ORIGIN`. MSv4 holds one term (XRADIO defines no
/// origin for more), so the conversion keeps each row's direction at its own
/// time and reports the terms as a loss; converting back writes rows of
/// `NUM_POLY` 0 at those directions.
#[test]
fn pointing_polynomials_keep_each_rows_direction_at_its_time_in_msv4() {
    let root = tempfile::tempdir().expect("root");
    let source = root.path().join("source");
    std::fs::create_dir(&source).expect("dir");
    let (v2, _) = mosaic(&source, 4);
    rate_terms(&v2);
    let rows = {
        let ms = MeasurementSet::open(&v2).expect("ms");
        let pointing = ms.pointing().expect("POINTING");
        (0..pointing.row_count())
            .map(|row| {
                (
                    pointing.i32(row, "ANTENNA_ID").expect("antenna"),
                    pointing.f64(row, "TIME").expect("TIME"),
                    pointing.f64(row, "TIME_ORIGIN").expect("TIME_ORIGIN"),
                    match pointing.array(row, "DIRECTION").expect("DIRECTION") {
                        casa_types::ArrayValue::Float64(values) => values.clone(),
                        other => panic!("DIRECTION is {other:?}"),
                    },
                )
            })
            .collect::<Vec<_>>()
    };
    // Row `antenna, time`'s direction at `at`.
    let direction = |antenna: u32, time: f64, at: f64| {
        let (_, _, origin, terms) = rows
            .iter()
            .find(|(id, row_time, _, _)| {
                *id == i32::try_from(antenna).expect("id") && *row_time == time
            })
            .expect("its row");
        let since = at - origin;
        [1e-6 * since + terms[[0, 0]], -2e-6 * since + terms[[1, 0]]]
    };
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    let from_v2 = series(msv2.as_ref());
    assert_eq!(from_v2.terms, 2);
    let v4 = root.path().join("rates.ps.zarr");
    let report =
        casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion()).expect("to msv4");
    assert!(report.losses.contains(&Loss::Table(
        "POINTING polynomial terms (each row's direction at its own time is kept)".to_string()
    )));
    let from_v4 = series(open(&v4, &OpenOptions::default()).expect("msv4").as_ref());
    assert_eq!(from_v4.terms, 1);
    let back = root.path().join("rates-back.ms");
    casa_ms::observation_io::convert::to_msv2(&v4, &back, &conversion()).expect("to msv2");
    let again = series(open(&back, &OpenOptions::default()).expect("back").as_ref());
    assert_eq!(again.terms, 1);
    let unix = |mjd_s: f64| mjd_s - 40_587.0 * 86_400.0;
    let grid = msv2.grid(PartitionId(0), "base").expect("grid");
    for antenna in 0..3_u32 {
        for time in grid.times_s.iter().map(|time| time + 2.5) {
            let before = from_v2
                .bracket(AntennaId(antenna), time)
                .expect("v2")
                .before
                .expect("a sample before");
            assert_eq!(
                before.direction_rad,
                direction(antenna, before.time_s, time)
            );
            let at_its_time = direction(antenna, before.time_s, before.time_s);
            let on_v4 = from_v4.bracket(AntennaId(antenna), unix(time)).expect("v4");
            let on_back = again.bracket(AntennaId(antenna), time).expect("back");
            for kept in [on_v4.before, on_back.before] {
                assert_eq!(kept.expect("a sample before").direction_rad, at_its_time);
            }
        }
    }
}

fn conversion() -> casa_ms::observation_io::convert::ConversionOptions {
    casa_ms::observation_io::convert::ConversionOptions {
        data_groups: None,
        accept: vec![LossKind::Sigma, LossKind::Tables],
        batch_bytes: 1 << 20,
        workers: 1,
        chunks: [4, 0, 0, 0],
        codec: casa_ms::observation_io::Msv4Codec::None,
        padding: casa_ms::observation_io::convert::PaddingRows::Flagged,
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-27T00:00:00+00:00".to_string(),
    }
}

/// A two-window simulation of three antennas. The simulator writes each
/// antenna's `FEED` row for window 0 only (#943).
fn two_windows(root: &Path) -> std::path::PathBuf {
    let mut request = casa_ms::SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        root.join("two.ms"),
        vec![
            casa_ms::SyntheticAntenna::vla(
                "VLA01",
                "N01",
                [-1_601_185.4, -5_041_977.5, 3_554_875.9],
            ),
            casa_ms::SyntheticAntenna::vla(
                "VLA02",
                "N02",
                [-1_601_085.4, -5_041_977.5, 3_554_875.9],
            ),
            casa_ms::SyntheticAntenna::vla(
                "VLA03",
                "N03",
                [-1_601_185.4, -5_041_877.5, 3_554_875.9],
            ),
        ],
    );
    request.predict_model = false;
    request.duration_seconds = 40.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = [44.0e9, 45.0e9]
        .iter()
        .enumerate()
        .map(|(index, start)| casa_ms::SyntheticSpectralSetup {
            name: format!("band{index}"),
            start_frequency_hz: *start,
            channel_width_hz: 2.0e6,
            channel_count: 4,
        })
        .collect();
    casa_ms::generate_synthetic_observation_ms(&request).expect("simulate");
    request.output_ms
}

/// Edit the MSv2's `FEED`: give every row window `every` when set, then
/// add a copy of each row `(row, window, angle, later_s)` names, in window
/// `window` with receptor angles `angle`, `later_s` after it.
fn edit_feed(path: &Path, every: Option<i32>, copies: &[(usize, i32, f64, f64)]) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    if let Some(window) = every {
        let mut feed = ms.feed_mut().expect("FEED");
        for row in 0..feed.row_count() {
            feed.set_i32(row, "SPECTRAL_WINDOW_ID", window)
                .expect("window");
        }
    }
    let feed = ms.subtable_mut(SubtableId::Feed).expect("FEED");
    for (row, window, angle, later_s) in copies {
        let mut copy = feed.row_accessor().row(*row).expect("row").clone();
        let time = match copy.get("TIME") {
            Some(Value::Scalar(ScalarValue::Float64(time))) => *time,
            other => panic!("TIME is {other:?}"),
        };
        copy.upsert(
            "SPECTRAL_WINDOW_ID",
            Value::Scalar(ScalarValue::Int32(*window)),
        );
        copy.upsert("TIME", Value::Scalar(ScalarValue::Float64(time + later_s)));
        copy.upsert(
            "RECEPTOR_ANGLE",
            Value::Array(casa_types::ArrayValue::Float64(
                ndarray::arr1(&[*angle, *angle]).into_dyn(),
            )),
        );
        feed.add_row(copy).expect("add");
    }
    ms.save().expect("save");
}

/// Each partition's receptor angles, by antenna.
fn angles(observation: &dyn casa_observation::Observation) -> Vec<Vec<f64>> {
    observation
        .catalogue()
        .partitions
        .iter()
        .map(|entry| {
            entry
                .receptors
                .values()
                .flat_map(|own| own.angles.iter().map(|angle| angle.value()))
                .collect()
        })
        .collect()
}

/// The (antenna, window) of every `FEED` row of the MSv2 at `path`, sorted.
fn feed_rows(path: &Path) -> Vec<(i32, i32)> {
    let ms = MeasurementSet::open(path).expect("ms");
    let feed = ms.feed().expect("FEED");
    let mut rows: Vec<(i32, i32)> = (0..feed.row_count())
        .map(|row| {
            (
                feed.i32(row, "ANTENNA_ID").expect("antenna"),
                feed.i32(row, "SPECTRAL_WINDOW_ID").expect("window"),
            )
        })
        .collect();
    rows.sort_unstable();
    rows
}

/// Each partition states its antennas' receptors in its own window, read
/// as casacore's `MSIter` and XRADIO's converter read `FEED`: a window's
/// own rows once any row names a window, else the rows for every window
/// (-1). A conversion carries them both ways; where one antenna's windows
/// differ, the MSv2 written has a row per window for every antenna, since a
/// row for every window would then be ignored. Of rows of one antenna,
/// feed and window that differ in time, the last is taken, as casacore's
/// `MSIter` takes it, and a conversion reports the others as lost.
#[test]
fn receptors_are_each_partitions_own() {
    let root = tempfile::tempdir().expect("root");
    let variant = |name: &str| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).expect("dir");
        two_windows(&dir)
    };
    // Window 1 rows: antenna 0's angle differs from window 0's, the
    // others' agree.
    let v2 = variant("mixed");
    edit_feed(
        &v2,
        None,
        &[(0, 1, 0.25, 0.0), (1, 1, 0.0, 0.0), (2, 1, 0.0, 0.0)],
    );
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    let mixed = [vec![0.0; 6], vec![0.25, 0.25, 0.0, 0.0, 0.0, 0.0]];
    assert_eq!(angles(msv2.as_ref()), mixed);
    let v4 = root.path().join("two.ps.zarr");
    casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion()).expect("to msv4");
    let msv4 = open(&v4, &OpenOptions::default()).expect("msv4");
    assert_eq!(angles(msv4.as_ref()), mixed);
    let back = root.path().join("back.ms");
    casa_ms::observation_io::convert::to_msv2(&v4, &back, &conversion()).expect("to msv2");
    let again = open(&back, &OpenOptions::default()).expect("back");
    assert_eq!(angles(again.as_ref()), mixed);
    assert_eq!(
        feed_rows(&back),
        [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)],
        "a row per window for every antenna, none for every window"
    );

    // Rows for every window serve both; with a window-1 row beside them,
    // casacore ignores them, so window 0 has no receptors.
    let v2 = variant("every");
    edit_feed(&v2, Some(-1), &[]);
    let every = open(&v2, &OpenOptions::default()).expect("every");
    assert_eq!(angles(every.as_ref()), [vec![0.0; 6], vec![0.0; 6]]);
    let v2 = variant("override");
    edit_feed(&v2, Some(-1), &[(0, 1, 0.25, 0.0)]);
    let overridden = open(&v2, &OpenOptions::default()).expect("override");
    assert_eq!(angles(overridden.as_ref()), [vec![], vec![0.25, 0.25]]);

    // Antenna 0's window-0 receptors change 10 s later: the later row,
    // last in the table, is the one read; a conversion refuses the change
    // until columns are accepted, then carries that row and reports it.
    let v2 = variant("varying");
    edit_feed(&v2, None, &[(0, 0, 0.5, 10.0)]);
    let varying = open(&v2, &OpenOptions::default()).expect("varying");
    let last = [vec![0.5, 0.5, 0.0, 0.0, 0.0, 0.0], vec![]];
    assert_eq!(angles(varying.as_ref()), last);
    let v4 = root.path().join("varying.ps.zarr");
    let refused = casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion());
    assert!(
        matches!(&refused, Err(ObservationError::InvalidRequest { reason }) if reason.contains("FEED")),
        "{refused:?}"
    );
    let mut accepting = conversion();
    accepting.accept.push(LossKind::Columns);
    let report = casa_ms::observation_io::convert::to_msv4(&v2, &v4, &accepting).expect("to msv4");
    assert!(
        report.losses.iter().any(|loss| matches!(
            loss,
            Loss::Column(column) if column.starts_with("FEED receptors' time dependence")
        )),
        "{:?}",
        report.losses
    );
    let converted = open(&v4, &OpenOptions::default()).expect("msv4");
    assert_eq!(angles(converted.as_ref()), last);

    // Table order, not time, decides: a row 10 s earlier but last in the
    // table is still the one read, as casacore's MSIter reads it.
    let v2 = variant("earlier");
    edit_feed(&v2, None, &[(0, 0, 0.5, -10.0)]);
    let earlier = open(&v2, &OpenOptions::default()).expect("earlier");
    assert_eq!(angles(earlier.as_ref()), last);

    // Rows for every window are not read beside window rows, so their
    // changing in time is no loss.
    let v2 = variant("unread");
    edit_feed(&v2, None, &[(0, -1, 0.5, 0.0), (0, -1, 0.75, 10.0)]);
    let unread = open(&v2, &OpenOptions::default()).expect("unread");
    assert_eq!(angles(unread.as_ref()), [vec![0.0; 6], vec![]]);
    let v4 = root.path().join("unread.ps.zarr");
    casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion())
        .expect("no FEED loss to accept");
}

/// #937: a store says whether it holds any pointing at all, whatever the
/// request's times, so that a single-dish gridder can tell a store without
/// pointing, which CASA images at the FIELD phase centre (`havePointings`),
/// from an acquisition no sample covers, which it does not grid. Shown for
/// usable samples, times outside the coverage, an emptied `POINTING`, an
/// MSv4 with `pointing_xds` and one without.
#[test]
fn a_store_says_whether_it_holds_pointing_whatever_the_times() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 4);
    {
        let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
        let usable = series(msv2.as_ref());
        assert!(usable.store_has_pointing && !usable.antennas.is_empty());
        assert!(series_over(msv2.as_ref(), 1.0e12..=1.0e12 + 1.0).store_has_pointing);
        let msv4 = open(&v4, &OpenOptions::default()).expect("msv4");
        assert!(series(msv4.as_ref()).store_has_pointing);
    }
    {
        let mut ms = MeasurementSet::open(&v2).expect("ms");
        let pointing = ms.subtable_mut(SubtableId::Pointing).expect("POINTING");
        let rows: Vec<usize> = (0..pointing.row_count()).collect();
        pointing.remove_rows(&rows).expect("empty POINTING");
        ms.save().expect("save");
    }
    let emptied = open(&v2, &OpenOptions::default()).expect("msv2");
    let empty = series(emptied.as_ref());
    assert!(!empty.store_has_pointing && empty.antennas.is_empty());
    let trial = super::fixture(root.path(), "ms_trial.ps.zarr");
    let without = open(&trial, &OpenOptions::default()).expect("msv4");
    assert!(
        !series(without.as_ref()).store_has_pointing,
        "no pointing_xds"
    );
}
