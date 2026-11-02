// SPDX-License-Identifier: LGPL-3.0-or-later

//! A field that follows an ephemeris (#942): the T1 track's field attached
//! to a distant body's table, imaged tracking the body (`TRACKFIELD`),
//! following the same body from a table given as the phase centre, as a
//! cube in the body's rest frame, on a fixed centre, and without a phase
//! centre, each on the production route, whose batch geometry follows the
//! body. The track's MSv4 conversion keeps that geometry to the store
//! law's 1e-12.

use std::f64::consts::{PI, TAU};
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateModel;
use casa_ms::{MeasurementSet, SubtableId};
use casa_test_support::ephemeris_table::{
    EphemerisRow, write_ephemeris_table, write_field_ephemeris,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::ephemeris::offset_true_angle;
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD};

const POINT: Component = Component {
    offset_px: [10, -6],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// The field's phase centre, its offset from the body: radians.
const OFFSET_RAD: [f64; 2] = [1.0e-7, -5.0e-8];

/// A body 30 AU away at the T1 phase centre, drifting about 0.2 arcsec a
/// day and receding at about 17 km/s, sampled over the track's day and the
/// days either side.
fn body() -> Vec<EphemerisRow> {
    let [ra, dec] = PHASE_CENTER_RAD.map(f64::to_degrees);
    let drift_deg = 1.0e-6_f64.to_degrees();
    (0..3)
        .map(|day| {
            let step = f64::from(day);
            [
                56_656.5 + step,
                ra + step * drift_deg,
                dec - step * drift_deg,
                30.0 + 1.0e-4 * step,
                0.01,
            ]
        })
        .collect()
}

/// The track with its field following [`body`] (`FIELD.EPHEMERIS_ID` 0),
/// its `PHASE_DIR` the offset from it.
pub(super) fn moving_track() -> Observation {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let path = observation.measurement_set();
    let mut ms = MeasurementSet::open(path).expect("the T1 MeasurementSet");
    let field = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    field
        .add_column(
            casa_tables::ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .expect("EPHEMERIS_ID");
    let offset = Value::Array(ArrayValue::Float64(
        ndarray::ArrayD::from_shape_vec(vec![2, 1], OFFSET_RAD.to_vec()).expect("a direction"),
    ));
    for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
        field
            .fill_column_range(column, casa_tables::RowRange::new(0, 1), offset.clone())
            .expect("the offset");
    }
    ms.save().expect("save");
    drop(ms);
    write_field_ephemeris(path, 0, "T1BODY", &body());
    observation
}

/// The body's table copied outside the MeasurementSet, as a `phasecenter`.
fn table_outside(observation: &Observation) -> PathBuf {
    let table = observation.scratch("t1body.tab");
    write_ephemeris_table(&table, "T1BODY", &body());
    table
}

fn path_text(path: &Path) -> String {
    path.to_str().expect("a UTF-8 path").to_string()
}

/// Tracking the field's own body, an MFS image and a rest-frame cube.
#[test]
fn trackfield_images_mfs_and_a_rest_frame_cube() {
    let observation = moving_track();
    observation.image(
        "trackfield-mfs",
        json!({ "phasecenter": "TRACKFIELD", "niter": 0 }),
    );
    observation.image(
        "trackfield-rest-cube",
        json!({
            "phasecenter": "TRACKFIELD",
            "specmode": "cubesource",
            "outframe": "REST",
            "restfreq": "45GHz",
            "channel_count": 4,
            "niter": 0,
        }),
    );
}

/// A fixed phase centre over the moving field: rows reproject, and
/// frequencies convert, from the field's direction at each time; and
/// Stokes IV under `TRACKFIELD`, whose parallactic angles follow the moving
/// field.
#[test]
fn a_fixed_centre_and_parallactic_angles_follow_the_moving_field() {
    let observation = moving_track();
    let [ra, dec] = PHASE_CENTER_RAD;
    observation.image(
        "fixed-mfs",
        json!({
            "phasecenter": format!("J2000 {}rad {}rad", ra + 2.0e-6, dec - 1.0e-6),
            "niter": 0,
        }),
    );
    observation.image(
        "trackfield-iv",
        json!({ "phasecenter": "TRACKFIELD", "stokes": "IV", "niter": 0 }),
    );
}

/// Following the body from a table given as the phase centre.
#[test]
fn a_table_phase_centre_follows_the_body() {
    let observation = moving_track();
    let table = table_outside(&observation);
    observation.image(
        "table-mfs",
        json!({ "phasecenter": path_text(&table), "niter": 0 }),
    );
}

/// Without a phase centre the image is centred, as CASA's `tclean`
/// centres it, on casacore's `phaseDirMeas` of the field at no time: the
/// body's geocentric direction at the FIELD row's `TIME`, offset by
/// `PHASE_DIR`, in the ephemeris's frame (ICRS). The centre stays fixed
/// while the field moves (#987).
#[test]
fn a_moving_field_without_a_phase_centre_is_centred_where_casa_centres_it() {
    let observation = moving_track();
    let (_, products) = observation.image("no-centre", json!({ "niter": 0 }));
    let ms = MeasurementSet::open(observation.measurement_set()).expect("the T1 MeasurementSet");
    let field_time_mjd = ms.field().expect("FIELD").time(0).expect("FIELD TIME") / 86_400.0;
    let expected = offset_true_angle(&geocentric_body_direction(field_time_mjd), OFFSET_RAD)
        .expect("the offset direction");
    let image = &products.get(".image").image;
    let CoordinateModel::Direction(direction) = image.coordinates().coordinate(0) else {
        panic!(".image coordinate 0 is not a direction coordinate");
    };
    assert_eq!(direction.direction_ref(), DirectionRef::ICRS);
    let reference = image.coordinates().coordinate(0).reference_value();
    let (longitude, latitude) = expected.as_angles();
    for (axis, (actual, expected)) in reference.iter().zip([longitude, latitude]).enumerate() {
        let difference = (actual - expected + PI).rem_euclid(TAU) - PI;
        assert!(
            difference.abs() < 1.0e-12,
            "image centre axis {axis}: {actual} rad, expected {expected} rad"
        );
    }
}

/// The FIELD row's `TIME` enters only the centre of an image without a
/// phase centre: with it outside the body's table, `TRACKFIELD` images as
/// before and only the empty phase centre is refused, as casacore refuses
/// `phaseDirMeas` there.
#[test]
fn only_an_empty_phase_centre_reads_the_moving_field_time() {
    let observation = moving_track();
    let mut ms =
        MeasurementSet::open(observation.measurement_set()).expect("the T1 MeasurementSet");
    ms.subtable_mut(SubtableId::Field)
        .expect("FIELD")
        .fill_column_range(
            "TIME",
            casa_tables::RowRange::new(0, 1),
            Value::Scalar(ScalarValue::Float64(0.0)),
        )
        .expect("a FIELD TIME outside the table");
    ms.save().expect("save");
    drop(ms);
    observation.image(
        "trackfield-unanchored",
        json!({ "phasecenter": "TRACKFIELD", "niter": 0 }),
    );
    let refused = observation
        .try_image("no-centre-unanchored", json!({ "niter": 0 }))
        .err()
        .expect("no body at the FIELD row's TIME");
    assert!(refused.contains("T1BODY"), "{refused}");
}

/// The track converted to MSv4 keeps its moving field's geometry (#942):
/// the converter writes the body's samples (`field_and_source_ephemeris`),
/// the MSv4 reader reads them back with the field's offset, and every
/// geometry quantity of the converted store's rows equals the
/// MeasurementSet's to the law's 1e-12, tracking the body, following its
/// table, and on a fixed centre.
#[test]
fn a_moving_field_keeps_its_geometry_in_msv4() {
    use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
    let observation = moving_track();
    let converted = observation.scratch("moving.ps.zarr");
    to_msv4(
        observation.measurement_set(),
        &converted,
        &ConversionOptions {
            data_groups: None,
            accept: vec![
                LossKind::Sigma,
                LossKind::RowFlags,
                LossKind::FlagCategories,
                LossKind::Tables,
                LossKind::Columns,
            ],
            batch_bytes: 64 << 20,
            workers: 1,
            chunks: [0, 0, 0, 0],
            codec: casa_ms::observation_io::Msv4Codec::Zstd,
            padding: PaddingRows::Flagged,
            creator: "casars-imager t1".to_string(),
            creation_date: "2026-10-29T00:00:00".to_string(),
        },
    )
    .expect("the track as MSv4");
    let table = table_outside(&observation);
    let [ra, dec] = PHASE_CENTER_RAD;
    for (name, controls) in [
        ("msv4-trackfield", json!({ "phasecenter": "TRACKFIELD" })),
        ("msv4-table", json!({ "phasecenter": path_text(&table) })),
        (
            "msv4-fixed",
            json!({ "phasecenter": format!("J2000 {}rad {}rad", ra + 2.0e-6, dec - 1.0e-6) }),
        ),
    ] {
        observation.assert_store_geometry(name, controls, &converted);
    }
}

/// [`body`]'s geocentric ICRS direction at `mjd` (UTC), its positions
/// interpolated linearly in time, as casacore's `MeasComet` reads them.
fn geocentric_body_direction(mjd: f64) -> MDirection {
    let rows = body();
    let pair = rows
        .windows(2)
        .find(|pair| pair[0][0] <= mjd && mjd <= pair[1][0])
        .expect("the body's table covers the field's time");
    let position = |row: &EphemerisRow| {
        let [ra, dec] = [row[1].to_radians(), row[2].to_radians()];
        [
            row[3] * dec.cos() * ra.cos(),
            row[3] * dec.cos() * ra.sin(),
            row[3] * dec.sin(),
        ]
    };
    let fraction = (mjd - pair[0][0]) / (pair[1][0] - pair[0][0]);
    let [start, end] = [position(&pair[0]), position(&pair[1])];
    MDirection::from_cosines(
        std::array::from_fn(|axis| start[axis] + fraction * (end[axis] - start[axis])),
        DirectionRef::ICRS,
    )
}
