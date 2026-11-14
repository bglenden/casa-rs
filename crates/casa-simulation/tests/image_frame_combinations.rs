// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's remaining accepted image frames and beam/mosaic combinations.
#[path = "image_frame_combinations/eop.rs"]
mod eop;
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, PredictSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions,
    VoltagePatternSpec,
};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn accepted_image_frames_and_beams_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_image_frames_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
    ];
    if host.metal {
        policies.push((
            "metal3",
            SessionResources::pinned(
                host,
                casa_imaging_runtime::ResourcePolicy::Explicit {
                    workers: 3,
                    memory: 2 << 30,
                },
                BackendRequest::Metal,
            )
            .unwrap(),
        ));
    }
    for (prefix, resources) in policies {
        let mut failures = Vec::new();
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            if std::env::var("CASA_RS_IMAGE_FRAME_CASE").is_ok_and(|selected| selected != name) {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("frames.ms");
            copy(&data.join("sm_image_layouts/base.ms"), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: eop::provider(&case["eop"]),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_data(&[], &[0], "").unwrap();
            if case.get("location_itrf_m").is_some() || case["mosaic"] == true {
                let position = case.get("location_itrf_m");
                sm.set_options(&PredictionOptions {
                    location_itrf_m: position.map_or([0.; 3], |position| {
                        std::array::from_fn(|axis| position[axis].as_f64().unwrap())
                    }),
                    ftmachine: if case["mosaic"] == true {
                        "mosaic"
                    } else {
                        "ft"
                    }
                    .into(),
                    ..PredictionOptions::default()
                })
                .unwrap();
            }
            if case["beam"] == true {
                sm.set_vp(&VoltagePatternSpec {
                    do_squint: false,
                    ..VoltagePatternSpec::default()
                })
                .unwrap();
            }
            let result = sm
                .predict(&PredictSpec {
                    images: vec![data.join("sm_image_frames").join(format!("{name}.image"))],
                    ..PredictSpec::default()
                })
                .and_then(|()| sm.close());
            if let Err(error) = result {
                failures.push(format!("{name} {prefix}: {error}"));
                continue;
            }
            // CASA's affine beam axis mislabels the last two irregular
            // frequencies. Independent constant-frequency CASA predictions
            // supply their corrected values; the fixture retains raw DATA.
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = existing_ms_calculations::compare(&path, &reference);
            println!("image frames {name} {prefix}: NRMS {nrms}");
            if nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: NRMS {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_IMAGE_FRAMES_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}

#[test]
fn reference_earth_orientation_matches_casa_hour_angle() {
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::{MPosition, MeasFrame};
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_image_frames_casa.json.gz")[..],
    ))
    .unwrap();
    let frame = MeasFrame::new()
        .with_measures(eop::provider(&fixture["cases"][0]["eop"]))
        .with_epoch(MEpoch::from_mjd(60462. + 5. / 86400., EpochRef::UTC))
        .with_position(MPosition::new_itrf(0., 0., 0.));
    let phase = MDirection::from_angles(
        support::CENTRE_RAD[0],
        support::CENTRE_RAD[1],
        DirectionRef::J2000,
    );
    let tangent =
        MDirection::from_angles(0.5563861741133894, 0.5914240127373699, DirectionRef::HADEC);
    let mut projection = casa_imaging_model::GridFtProjection::between_directions_with_frame(
        &phase, &tangent, &frame,
    )
    .unwrap();
    let raw = [-20.70965263689868, -8.559785038232803, -8.82271883636713];
    // Actual CASA 6.7.6.14 C++ UVWMachine, at the geocentre limit to
    // avoid MVPosition::readjust(0)'s spurious aberration.
    for (seconds, expected, expected_phase) in [
        (
            5.,
            [5.467063586284301, 1.2741567833510392, -23.420232278730072],
            14.597027123114067,
        ),
        (
            15.,
            [5.452366591645415, 1.2763765272105112, -23.423537333751028],
            14.587788380345136,
        ),
    ] {
        let current = frame
            .clone()
            .with_epoch(MEpoch::from_mjd(60462. + seconds / 86400., EpochRef::UTC));
        projection
            .recalculate_with_frame(&phase, &tangent, &frame, &current)
            .unwrap();
        let (uvw, path) = projection.apply(raw);
        for (actual, reference) in uvw.into_iter().zip(expected) {
            assert!((actual - reference).abs() < 1e-8);
        }
        assert!((path - expected_phase).abs() < 1e-8);
    }
    let direction = MDirection::from_angles(
        support::CENTRE_RAD[0],
        support::CENTRE_RAD[1],
        DirectionRef::J2000,
    )
    .convert_to(DirectionRef::HADEC, &frame)
    .unwrap();
    // Independent CASA 6.7.6.14 measures() at the same UTC and the
    // geocentre limit (its record conversion substitutes a 1 μm radius).
    let (longitude, latitude) = direction.as_angles();
    println!("reference HADEC: {longitude:.16} {latitude:.16}");
    assert!((longitude - 1.7389336791603918).abs() < 1e-10);
    assert!((latitude - 0.5914251906082281).abs() < 1e-10);
    for (source, expected) in [
        ([0., 0.], [-1.9254339387959654, 0.002350546452393686]),
        (
            [std::f64::consts::FRAC_PI_2, 0.],
            [2.787016503623245, 2.4203564707665478e-05],
        ),
        (
            [0., std::f64::consts::FRAC_PI_2],
            [1.1862357582065837, 1.568523658111506],
        ),
    ] {
        let value = MDirection::from_angles(source[0], source[1], DirectionRef::J2000)
            .convert_to(DirectionRef::HADEC, &frame)
            .unwrap();
        let expected = MDirection::from_angles(expected[0], expected[1], DirectionRef::HADEC);
        let error = value
            .cosines()
            .into_iter()
            .zip(expected.cosines())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>()
            .sqrt();
        println!(
            "reference HADEC axis {source:?}: {:?}, error {error}",
            value.as_angles()
        );
        assert!(error < 1e-10);
    }
}
