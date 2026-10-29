// SPDX-License-Identifier: LGPL-3.0-or-later

//! A field that follows an ephemeris (#942): the T1 track's field attached
//! to a distant body's table, imaged tracking the body (`TRACKFIELD`),
//! following the same body from a table given as the phase centre, and as
//! a cube in the body's rest frame. Each run holds the geometry law and the
//! route law, so the observation-batch route images moving fields as
//! today's route does.

use std::path::{Path, PathBuf};

use casa_ms::{MeasurementSet, SubtableId};
use casa_test_support::ephemeris_table::{
    EphemerisRow, write_ephemeris_table, write_field_ephemeris,
};
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
fn moving_track() -> Observation {
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
fn trackfield_images_agree_on_both_routes() {
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
fn a_fixed_centre_and_parallactic_angles_follow_the_moving_field_on_both_routes() {
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
fn a_table_phase_centre_agrees_on_both_routes() {
    let observation = moving_track();
    let table = table_outside(&observation);
    observation.image(
        "table-mfs",
        json!({ "phasecenter": path_text(&table), "niter": 0 }),
    );
}

/// Without a phase centre, the image centre would be read from the moving
/// field's `PHASE_DIR`, an offset; both routes refuse it.
#[test]
fn a_moving_field_without_a_phase_centre_is_refused() {
    let observation = moving_track();
    let refused = observation
        .try_image("no-centre", json!({ "niter": 0 }))
        .err()
        .expect("a moving field without a phase centre");
    assert!(refused.contains("follows an ephemeris"), "{refused}");
}
