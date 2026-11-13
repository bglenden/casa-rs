// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fresh simulations with several correlation/channel cell shapes.
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, PredictSpec, SessionResources, Simulator, SpectralWindowSpec,
};
use casa_types::measures::frequency::FrequencyRef;
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn fresh_mixed_shape_observations_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_mixed_creation_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let image =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts/iquv.image");
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
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("mixed.ms");
            let mut sm = observed(&path, &case["spec"], resources);
            if case["spec"]["selected"] == true {
                sm.set_data(&[1], &[], "").unwrap();
            }
            let mut component_lists = Vec::new();
            if case["spec"]["components"] == true {
                let component = directory.path().join("point.cl");
                support::write_stokes_point_list(
                    &component,
                    support::CENTRE_RAD,
                    [0.25, 0.05, -0.02, 0.01],
                );
                component_lists.push(component);
            }
            sm.predict(&PredictSpec {
                images: vec![image.clone()],
                component_lists,
                incremental: false,
            })
            .unwrap();
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare_created(&path, case);
            println!("fresh mixed {name} {prefix}: NRMS {nrms}");
            assert!(nrms < 1e-3, "{name} {prefix}: {nrms}");
            if let Some(export) = std::env::var_os("CASA_RS_MIXED_CREATION_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
}

fn observed(path: &Path, spec: &Value, resources: SessionResources) -> Simulator {
    let mut sm = setup(path, spec, resources);
    add_scan(&mut sm, "target", 0., 20., "band6");
    add_scan(&mut sm, "offset", 20., 40., "band6");
    for (source, start, stop) in [("target", 40., 60.), ("offset", 60., 80.)] {
        add_scan(&mut sm, source, start, stop, "mixed");
    }
    sm
}

fn setup(path: &Path, spec: &Value, resources: SessionResources) -> Simulator {
    let mut sm = support::session(path, "ALMA", "XX YY", resources);
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "mixed".into(),
        start_frequency_hz: 230e9,
        channel_width_hz: 15.625e6,
        resolution_hz: 15.625e6,
        frame: FrequencyRef::TOPO,
        channel_count: spec["channels"].as_u64().unwrap_or(4) as usize,
        stokes: spec["stokes"].as_str().unwrap().into(),
    })
    .unwrap();
    sm
}

fn add_scan(sm: &mut Simulator, source: &str, start: f64, stop: f64, window: &str) {
    let mut scan = support::scan(source, start, stop);
    scan.spectral_window = window.into();
    // Match the CASA oracle's observe() defaults, rather than the generic
    // Rust test helper's explicit observer/project strings.
    scan.observer = "CASA simulator".into();
    scan.project = "CASA simulation".into();
    sm.observe(&scan).unwrap();
}

#[derive(Clone)]
struct Preparations(std::sync::Arc<std::sync::atomic::AtomicU64>);

impl tracing::field::Visit for Preparations {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "simulation_prepared_scans" {
            self.0
                .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Preparations {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        event.record(&mut self.clone());
    }
}

#[test]
fn alternating_shapes_prepare_each_scan_a_bounded_number_of_times() {
    use std::sync::atomic::Ordering;
    use tracing_subscriber::prelude::*;
    let image =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts/iquv.image");
    for count in [32, 128] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("alternating.ms");
        let mut sm = setup(
            &path,
            &serde_json::json!({"stokes": "XX XY YX YY"}),
            support::resources(1, 512 << 20),
        );
        for scan in 0..count {
            add_scan(
                &mut sm,
                "target",
                scan as f64 * 10.,
                (scan + 1) as f64 * 10.,
                if scan % 2 == 0 { "band6" } else { "mixed" },
            );
        }
        sm.predict(&PredictSpec {
            images: vec![image.clone()],
            ..PredictSpec::default()
        })
        .unwrap();
        let visits = Preparations(Default::default());
        let subscriber = tracing_subscriber::registry().with(visits.clone());
        let started = std::time::Instant::now();
        tracing::subscriber::with_default(subscriber, || sm.close().unwrap());
        let visits = visits.0.load(Ordering::Relaxed);
        println!(
            "{count} alternating fresh scans: {visits} preparations, {:.3}s",
            started.elapsed().as_secs_f64()
        );
        assert!(visits > 0 && visits <= 2 * count);
    }
}

#[test]
fn empty_mixed_observations_close_and_reopen() {
    use casa_simulation::{ArrayConfig, CoordinateSystem, SimulatorOptions};
    use casa_types::measures::{EpochRef, MEpoch};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("empty.ms");
    let resources = support::resources(1, 512 << 20);
    let options = SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources,
        now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
    };
    let mut sm = Simulator::open(&path, options.clone());
    sm.set_config(&ArrayConfig {
        telescope: "ALMA".into(),
        x: vec![-1_601_185.],
        y: vec![-5_041_977.],
        z: vec![3_554_875.],
        dish_diameter: vec![12.],
        offset: vec![0.],
        mount: vec!["alt-az".into()],
        antenna_names: vec!["DA".into()],
        pad_names: vec!["A".into()],
        coordinate_system: CoordinateSystem::Global,
    })
    .unwrap();
    sm.set_feed("perfect X Y", &[], &[], &[]).unwrap();
    for (name, stokes) in [("band6", "XX YY"), ("mixed", "XX XY YX YY")] {
        sm.set_spectral_window(&SpectralWindowSpec {
            name: name.into(),
            start_frequency_hz: 230e9,
            channel_width_hz: 15.625e6,
            resolution_hz: 15.625e6,
            frame: FrequencyRef::TOPO,
            channel_count: 4,
            stokes: stokes.into(),
        })
        .unwrap();
    }
    sm.set_field(
        "target",
        &casa_types::measures::direction::MDirection::from_angles(
            support::CENTRE_RAD[0],
            support::CENTRE_RAD[1],
            casa_types::measures::direction::DirectionRef::J2000,
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
    sm.set_auto(0.);
    add_scan(&mut sm, "target", 0., 10., "band6");
    add_scan(&mut sm, "target", 10., 20., "mixed");
    sm.close().unwrap();
    let ms = casa_ms::MeasurementSet::open(&path).unwrap();
    assert_eq!(ms.main_table().row_count(), 0);
    drop(ms);
    Simulator::open_from_ms(&path, options)
        .unwrap()
        .close()
        .unwrap();
    if let Some(export) = std::env::var_os("CASA_RS_MIXED_CREATION_OUT") {
        copy(&path, &Path::new(&export).join("empty.ms"));
    }
}

#[test]
fn mixed_writer_budget_failure_retains_typed_admission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("refused.ms");
    let sm = observed(
        &path,
        &serde_json::json!({"stokes": "XX XY YX YY"}),
        // The policy also covers already charged process caches when the
        // other tests have run in this process.
        support::resources(1, casa_imaging_runtime::reserved_memory() + (128 << 10)),
    );
    let error = sm.close().unwrap_err();
    let admission = error
        .admission_refused()
        .expect("writer refusal remains typed");
    assert_eq!(admission.phase, "the simulator's MeasurementSet writer");
    assert!(admission.required > admission.available);
    assert!(!path.exists());
}
