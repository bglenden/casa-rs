// SPDX-License-Identifier: LGPL-3.0-or-later
//! Geometry's inputs on the boundary (#932, step 3a;
//! `docs/measurement-set/imaging-port.md` §2.2.1): what imaging's per-row
//! geometry reads from a MeasurementSet today, carried by both stores.

use std::path::Path;

use casa_ms::observation_io::convert::LossKind;
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

/// The rows the MeasurementSet definition brackets `time_s` with for
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
/// there: an MSv2's are the `POINTING` rows the MeasurementSet definition
/// names (the covering row by `INTERVAL`, the last before and the first
/// after); its MSv4 conversion holds the same samples at the same instants,
/// with no intervals, so nothing covers; and converting back writes the
/// same rows, with `INTERVAL` 0.
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

/// A pointing polynomial in time is evaluated at the query time on both
/// stores: an MSv2 row's about its `TIME_ORIGIN`, as the MeasurementSet
/// defines it; the MSv4 conversion re-expresses it about its sample's own
/// time, as MSv4 states it, and the reverse conversion writes it with
/// `TIME_ORIGIN` at the sample. The directions agree to rounding.
#[test]
fn pointing_polynomials_are_evaluated_at_the_query_time_on_both_stores() {
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
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    let from_v2 = series(msv2.as_ref());
    assert_eq!(from_v2.terms, 2);
    let v4 = root.path().join("rates.ps.zarr");
    casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion()).expect("to msv4");
    let from_v4 = series(open(&v4, &OpenOptions::default()).expect("msv4").as_ref());
    assert_eq!(from_v4.terms, 2);
    let back = root.path().join("rates-back.ms");
    casa_ms::observation_io::convert::to_msv2(&v4, &back, &conversion()).expect("to msv2");
    let again = series(open(&back, &OpenOptions::default()).expect("back").as_ref());
    assert_eq!(again.terms, 2);
    for samples in again.antennas.values() {
        assert_eq!(
            samples.origins_s(),
            samples.times_s(),
            "TIME_ORIGIN at TIME"
        );
    }
    let unix = |mjd_s: f64| mjd_s - 40_587.0 * 86_400.0;
    let grid = msv2.grid(PartitionId(0), "base").expect("grid");
    for antenna in 0..3_u32 {
        for time in grid.times_s.iter().map(|time| time + 2.5) {
            let bracket = from_v2.bracket(AntennaId(antenna), time).expect("v2");
            let before = bracket.before.expect("a sample before");
            let (_, row_time, origin, terms) = rows
                .iter()
                .find(|(id, row_time, _, _)| {
                    *id == i32::try_from(antenna).expect("id") && *row_time == before.time_s
                })
                .expect("its row");
            assert_eq!(*row_time, before.time_s);
            let since = time - origin;
            assert_eq!(
                before.direction_rad,
                [1e-6 * since + terms[[0, 0]], -2e-6 * since + terms[[1, 0]]]
            );
            let on_v4 = from_v4.bracket(AntennaId(antenna), unix(time)).expect("v4");
            let on_back = again.bracket(AntennaId(antenna), time).expect("back");
            for shifted in [on_v4.before, on_back.before] {
                let shifted = shifted.expect("a sample before").direction_rad;
                for axis in 0..2 {
                    assert!(
                        (shifted[axis] - before.direction_rad[axis]).abs() < 1e-14,
                        "{shifted:?} against {:?}",
                        before.direction_rad
                    );
                }
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

/// A two-window simulation (the simulator states each antenna's receptors
/// for window 0 only) given window 1 receptors of its own: the window-0
/// rows copied with receptor angles `angle`.
fn two_windows(root: &Path, angle: f64) -> std::path::PathBuf {
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
    let mut ms = MeasurementSet::open(&request.output_ms).expect("ms");
    let feed = ms.subtable_mut(SubtableId::Feed).expect("FEED");
    for row in 0..feed.row_count() {
        let mut copy = feed.row_accessor().row(row).expect("row").clone();
        copy.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1)));
        copy.upsert(
            "RECEPTOR_ANGLE",
            Value::Array(casa_types::ArrayValue::Float64(
                ndarray::arr1(&[angle, angle]).into_dyn(),
            )),
        );
        feed.add_row(copy).expect("add");
    }
    ms.save().expect("save");
    request.output_ms
}

/// The receptor angles each partition states, by antenna.
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

/// Each partition states its antennas' receptors in its own window: the
/// MSv2's `FEED` rows for the window (or every window), the MSv4's
/// `antenna_xds`; a conversion carries them both ways, writing per-window
/// `FEED` rows where windows differ. Rows of one antenna, feed and window
/// that differ in time are refused, as XRADIO's converter refuses them.
#[test]
fn receptors_are_each_partitions_own() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_windows(root.path(), 0.25);
    let msv2 = open(&v2, &OpenOptions::default()).expect("msv2");
    assert_eq!(angles(msv2.as_ref()), [vec![0.0; 6], vec![0.25; 6]]);
    let v4 = root.path().join("two.ps.zarr");
    casa_ms::observation_io::convert::to_msv4(&v2, &v4, &conversion()).expect("to msv4");
    let msv4 = open(&v4, &OpenOptions::default()).expect("msv4");
    assert_eq!(angles(msv4.as_ref()), angles(msv2.as_ref()));
    let back = root.path().join("back.ms");
    casa_ms::observation_io::convert::to_msv2(&v4, &back, &conversion()).expect("to msv2");
    let again = open(&back, &OpenOptions::default()).expect("back");
    assert_eq!(angles(again.as_ref()), angles(msv2.as_ref()));

    let varying = root.path().join("varying");
    std::fs::create_dir(&varying).expect("dir");
    let v2 = two_windows(&varying, 0.25);
    {
        let mut ms = MeasurementSet::open(&v2).expect("ms");
        let feed = ms.subtable_mut(SubtableId::Feed).expect("FEED");
        let mut later = feed.row_accessor().row(0).expect("row").clone();
        later.upsert(
            "RECEPTOR_ANGLE",
            Value::Array(casa_types::ArrayValue::Float64(
                ndarray::arr1(&[0.5, 0.5]).into_dyn(),
            )),
        );
        feed.add_row(later).expect("add");
        ms.save().expect("save");
    }
    assert!(matches!(
        open(&v2, &OpenOptions::default()),
        Err(ObservationError::Unsupported { what }) if what.contains("differ in time")
    ));
}
