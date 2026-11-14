// SPDX-License-Identifier: LGPL-3.0-or-later
//! VLA component squint and CASA's accepted EVLA no-op response.
mod existing_ms_calculations;

use casa_simulation::{
    ArrayConfig, BackendRequest, CoordinateSystem, PredictSpec, SessionResources, Simulator,
    SimulatorOptions, SpectralWindowSpec, VoltagePatternSpec,
};
use casa_types::measures::{
    EpochRef, MEpoch,
    direction::{DirectionRef, MDirection},
    frequency::FrequencyRef,
};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn observation(path: &Path, case: &Value, resources: SessionResources) -> Simulator {
    let mut sm = Simulator::open(
        path,
        SimulatorOptions {
            resources,
            measures: casa_test_support::deterministic_measures_provider(),
            now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
        },
    );
    let centre = [-1601185., -5041977., 3554875.];
    let coordinates = |axis: usize, step: f64| {
        (0..8)
            .map(|index| centre[axis] + step * f64::from(index))
            .collect()
    };
    sm.set_config(&ArrayConfig {
        telescope: case["telescope"].as_str().unwrap().into(),
        x: coordinates(0, 40.),
        y: coordinates(1, -12.),
        z: coordinates(2, 24.),
        dish_diameter: vec![25.],
        offset: vec![0.],
        mount: vec!["alt-az".into()],
        antenna_names: vec!["VA".into()],
        pad_names: vec!["A".into()],
        coordinate_system: CoordinateSystem::Global,
    })
    .unwrap();
    let frequency = case["frequency_hz"].as_f64().unwrap();
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "beam".into(),
        start_frequency_hz: frequency,
        channel_width_hz: frequency * 0.01,
        resolution_hz: frequency * 0.01,
        frame: FrequencyRef::TOPO,
        channel_count: 4,
        stokes: "RR RL LR LL".into(),
    })
    .unwrap();
    sm.set_feed("perfect R L", &[], &[], &[]).unwrap();
    sm.set_field(
        "target",
        &MDirection::from_angles(
            support::CENTRE_RAD[0],
            support::CENTRE_RAD[1],
            DirectionRef::J2000,
        ),
        "",
        0.,
    )
    .unwrap();
    sm.set_times(
        10.,
        false,
        MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
    )
    .unwrap();
    for start in [0., 3600.] {
        let mut scan = support::scan("target", start, start + 20.);
        scan.spectral_window = "beam".into();
        scan.observer = "CASA simulator".into();
        scan.project = "CASA simulation".into();
        sm.observe(&scan).unwrap();
    }
    sm
}

#[test]
fn component_squint_and_angle_refresh_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_squint_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_squint");
    let mut failures = Vec::new();
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
    ];
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
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
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("squint.ms");
            let mut sm = observation(&path, case, resources);
            sm.set_vp(&VoltagePatternSpec {
                do_squint: case["squint"].as_bool().unwrap(),
                parallactic_angle_increment_deg: case["increment_deg"].as_f64().unwrap(),
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            let telescope = case["telescope"].as_str().unwrap().to_lowercase();
            sm.predict(&PredictSpec {
                component_lists: vec![data.join(format!("{telescope}.cl"))],
                ..PredictSpec::default()
            })
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            sm.close().unwrap();
            if let Some(export) = std::env::var_os("CASA_RS_SQUINT_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
            let nrms = existing_ms_calculations::compare_created(&path, case);
            println!("component beam {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
