// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's sidereal time at the existing simulation UVW regression epochs (#1053).

use casa_types::measures::baseline::local_apparent_sidereal_time;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};

#[test]
fn simulation_sidereal_time_follows_casa_across_dates() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("data/casa_simulation_sidereal.json")).unwrap();
    assert_eq!(reference["casa_version"], "6.7.6.14");
    let position = reference["position_itrf_m"].as_array().unwrap();
    let position = MPosition::new_itrf(
        position[0].as_f64().unwrap(),
        position[1].as_f64().unwrap(),
        position[2].as_f64().unwrap(),
    );
    let mut maximum_error = 0.0_f64;
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 5);
    for case in cases {
        let frame = MeasFrame::new()
            .with_position(position.clone())
            .with_epoch(MEpoch::from_mjd(
                case["epoch_ut1_mjd"].as_f64().unwrap(),
                EpochRef::UT1,
            ));
        let actual = local_apparent_sidereal_time(&frame).unwrap();
        let expected = case["last_rad"].as_f64().unwrap();
        assert!(actual.is_finite() && expected.is_finite());
        let error = ((actual - expected + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
            - std::f64::consts::PI)
            .abs();
        println!("{}: LAST difference {error:.9e} rad", case["name"]);
        maximum_error = maximum_error.max(error);
    }
    // CASA returns sidereal epochs as a single double day count. Allow
    // that rounding while requiring <0.1 micrometre on these 150 m arrays.
    assert!(
        maximum_error < 5e-10,
        "maximum LAST error {maximum_error} rad"
    );
}
