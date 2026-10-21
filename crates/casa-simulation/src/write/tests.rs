// SPDX-License-Identifier: LGPL-3.0-or-later
//! How `close` runs does not change what it writes: the whole-MeasurementSet
//! digest is the same for one worker and four, and for a memory budget that
//! writes MAIN in one batch and one that writes it in batches of under a
//! hundred rows, with noise on (design §9, C5).
//!
//! The session covers what the pipeline splits on: integrations larger than
//! a batch and integrations that straddle batches, scans with and without
//! autocorrelations (so integrations of two sizes), a scan boundary inside a
//! batch between scans with different noise, flagged rows, and the four
//! correlations that draw from two counter pairs. With a model predicted
//! too, the digests agree as well: a predict covering the first scan, and
//! an incremental one covering the first three, so the batches meet scans
//! predicted from two domains, from one, and from none.
//!
//! Admission is the process's, so these tests assume no other test holds
//! memory at once (nextest runs each test in its own process).

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem as ImageCoordinates, DirectionCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_runtime::pass::BackendChoice;
use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_test_support::ms_digest::measurement_set_manifest;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use ndarray::{ArrayD, IxDyn};

use super::prediction::PredictionStage;
use super::resources::{MainShape, admit_main_write, writer_plan};
use super::{cell_shape, main_shape};
use crate::session::{BackendRequest, SessionResources};
use crate::{
    ArrayConfig, CoordinateSystem, NoiseSpec, ObserveSpec, PredictSpec, Simulator,
    SimulatorOptions, SpectralWindowSpec, StateSpec,
};

/// The field, and the centre of the model.
fn field_rad() -> [f64; 2] {
    [150.0_f64.to_radians(), 34.0_f64.to_radians()]
}

/// Pixels a side of the model, of 4″: the 720 m baselines at 1.4 GHz fall
/// inside its padded grid.
const MODEL_PIXELS: usize = 32;

/// A one-plane Stokes I model spanning the session's band: a few points
/// around the field.
fn write_model(dir: &Path) -> PathBuf {
    let path = dir.join("model.image");
    let cell = 2.0e-5;
    let mut coordinates = ImageCoordinates::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        field_rad(),
        [-cell, cell],
        [(MODEL_PIXELS / 2) as f64, (MODEL_PIXELS / 2) as f64],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.401e9,
        3.0e6,
        0.0,
        1.401e9,
    ));
    let shape = vec![MODEL_PIXELS, MODEL_PIXELS, 1, 1];
    let mut image =
        PagedImage::<f32>::create(shape.clone(), coordinates, &path).expect("create model");
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for (x, y, flux) in [(16, 16, 1.0_f32), (20, 13, 0.5), (5, 27, 0.25)] {
        pixels[IxDyn(&[x, y, 0, 0])] = flux;
    }
    image.put_slice(&pixels, &[0, 0, 0, 0]).expect("pixels");
    image.save().expect("save model");
    path
}

/// 2024-06-01 00:00 UTC.
const REFERENCE_MJD: f64 = 60_462.0;
const ANTENNAS: usize = 16;

fn resources(workers: usize, memory: u64) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit { workers, memory },
        BackendRequest::Cpu,
    )
    .expect("resources")
}

fn scan(start_s: f64, stop_s: f64) -> ObserveSpec {
    ObserveSpec {
        source: "target".to_string(),
        spectral_window: "LBand".to_string(),
        start_s,
        stop_s,
        add_observation: false,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: "OBSERVE_TARGET.ON_SOURCE".to_string(),
        },
        observer: "tester".to_string(),
        project: "pipeline".to_string(),
    }
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: sigma_jy,
        on_the_fly: true,
    }
}

/// The session, before `close`: four scans of 16 VLA antennas with four
/// correlations and three channels.
/// - Scan 1 (300 s, autocorrelations: 136 rows an integration) is corrupted
///   twice.
/// - Scan 2 (100 s) is observed after the first `corrupt` with every row
///   below the elevation limit.
/// - Scan 3 (100 s) has no autocorrelations: 120 rows an integration.
/// - The second `corrupt` adds a second term with another seed; scan 4
///   (50 s) is observed after it, without noise.
fn session(path: &Path, resources: SessionResources, noise: bool) -> Simulator {
    session_predicting(path, resources, noise, None)
}

/// [`session`], with `model`, when given, predicted into scan 1 before its
/// first `corrupt`, and incrementally into scans 1 to 3 before the second.
fn session_predicting(
    path: &Path,
    resources: SessionResources,
    noise: bool,
    model: Option<&Path>,
) -> Simulator {
    let predict = |session: &mut Simulator, incremental: bool| {
        if let Some(model) = model {
            session
                .predict(&PredictSpec {
                    images: vec![model.to_path_buf()],
                    component_list: None,
                    incremental,
                })
                .expect("predict");
        }
    };
    let options = SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources,
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    };
    let mut session = Simulator::open(path, options);
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    let along = |axis: usize, step: f64| {
        (0..ANTENNAS)
            .map(|index| centre[axis] + step * index as f64)
            .collect()
    };
    session
        .set_config(&ArrayConfig {
            telescope: "VLA".to_string(),
            x: along(0, 40.0),
            y: along(1, -12.0),
            z: along(2, 24.0),
            dish_diameter: vec![25.0],
            offset: vec![0.0],
            mount: vec!["alt-az".to_string()],
            antenna_names: vec!["ea".to_string()],
            pad_names: vec!["W".to_string()],
            coordinate_system: CoordinateSystem::Global,
        })
        .expect("setconfig");
    session
        .set_spectral_window(&SpectralWindowSpec {
            name: "LBand".to_string(),
            start_frequency_hz: 1.4e9,
            channel_width_hz: 1.0e6,
            resolution_hz: 1.0e6,
            frame: FrequencyRef::TOPO,
            channel_count: 3,
            stokes: "XX XY YX YY".to_string(),
        })
        .expect("setspwindow");
    session
        .set_field(
            "target",
            &MDirection::from_angles(
                150.0_f64.to_radians(),
                34.0_f64.to_radians(),
                DirectionRef::J2000,
            ),
            "",
            0.0,
        )
        .expect("setfield");
    session
        .set_times(10.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("settimes");
    session.observe(&scan(0.0, 300.0)).expect("observe");
    predict(&mut session, false);
    if noise {
        session.set_noise(&simple_noise(0.1)).expect("setnoise");
        session.corrupt().expect("corrupt");
    }
    session.set_limits(1.0e-6, 91.0_f64.to_radians());
    session.observe(&scan(300.0, 400.0)).expect("observe");
    session.set_limits(1.0e-6, 8.0_f64.to_radians());
    session.set_auto(0.0);
    session.observe(&scan(400.0, 500.0)).expect("observe");
    predict(&mut session, true);
    if noise {
        session.set_seed(7);
        session.set_noise(&simple_noise(0.3)).expect("setnoise");
        session.corrupt().expect("corrupt");
    }
    session.observe(&scan(500.0, 550.0)).expect("observe");
    session
}

/// MAIN of the session.
fn shape(session: &Simulator) -> MainShape<'_> {
    let cell = cell_shape(session).expect("one shape").expect("scans");
    main_shape(session, cell)
}

/// Write the session with `resources` to `directory/name` and digest it.
fn manifest(directory: &Path, name: &str, resources: SessionResources) -> String {
    let path: PathBuf = directory.join(name);
    session(&path, resources, true).close().expect("close");
    measurement_set_manifest(&path)
}

/// The smallest budget whose writer batches at least `rows` rows: the
/// writer gets half of what the policy leaves free.
fn budget_for_batches_of(shape: &MainShape<'_>, rows: usize) -> u64 {
    let batch_rows =
        |budget: u64| writer_plan(shape, (budget / 2) as usize).map_or(0, |plan| plan.batch_rows());
    let (mut low, mut high) = (1_u64, 1 << 34);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if batch_rows(middle) >= rows {
            high = middle;
        } else {
            low = middle;
        }
    }
    high
}

#[test]
fn the_measurement_set_is_the_same_for_any_worker_count_and_budget() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let probe = session(
        &directory.path().join("probe.ms"),
        resources(1, 1 << 30),
        true,
    );
    let shape = shape(&probe);
    assert_eq!(shape.rows, 30 * 136 + 10 * 136 + 10 * 120 + 5 * 120);
    let large = 4_u64 << 30;
    let small = budget_for_batches_of(&shape, 97);
    let batch_rows = |memory: u64, workers: usize| {
        let admitted =
            admit_main_write(&resources(workers, memory), &shape, None).expect("admitted");
        (admitted.plan.batch_rows(), admitted.cores.streaming_workers)
    };
    let (large_rows, four) = batch_rows(large, 5);
    let (small_rows, small_four) = batch_rows(small, 5);
    assert_eq!(
        (large_rows, four, small_four),
        (shape.rows, 4, 4),
        "one batch with the large budget, and four workers"
    );
    assert_eq!(batch_rows(large, 1).1, 1);
    assert!(
        small_rows < 136
            && !shape.rows.is_multiple_of(small_rows)
            && !136_usize.is_multiple_of(small_rows)
            && !120_usize.is_multiple_of(small_rows),
        "small batches split integrations: {small_rows} rows of {}",
        shape.rows
    );

    let first = manifest(directory.path(), "first.ms", resources(1, large));
    assert_eq!(
        manifest(directory.path(), "again.ms", resources(1, large)),
        first,
        "the manifest does not depend on the path"
    );
    for (name, workers, memory) in [
        ("four.ms", 5, large),
        ("small.ms", 1, small),
        ("small-four.ms", 5, small),
    ] {
        assert_eq!(
            manifest(directory.path(), name, resources(workers, memory)),
            first,
            "{name}: a policy of {workers} workers and {memory} bytes"
        );
    }
    let clean = directory.path().join("clean.ms");
    session(&clean, resources(5, small), false)
        .close()
        .expect("close");
    assert_ne!(
        measurement_set_manifest(&clean),
        first,
        "the sessions add noise"
    );
}

#[test]
fn the_predicted_measurement_set_is_the_same_for_any_worker_count_and_budget() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = write_model(directory.path());
    let large = 4_u64 << 30;
    // The smallest budget that admits four workers and batches of at least
    // 97 rows beside the model's grids and the process caches, which the
    // probe holds as every session does: monotone in the budget.
    let small = {
        let probe = session_predicting(
            &directory.path().join("probe.ms"),
            resources(5, large),
            true,
            Some(&model),
        );
        let cell = cell_shape(&probe).expect("one shape").expect("scans");
        let shape = main_shape(&probe, cell);
        let stage = PredictionStage::new(&probe, cell, BackendChoice::Cpu)
            .expect("stage")
            .expect("the session predicts");
        let fits = |budget: u64| {
            admit_main_write(&resources(5, budget), &shape, Some(&stage)).is_ok_and(|admitted| {
                admitted.cores.streaming_workers == 4 && admitted.plan.batch_rows() >= 97
            })
        };
        let (mut low, mut high) = (1_u64, large);
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if fits(middle) {
                high = middle;
            } else {
                low = middle;
            }
        }
        let admitted = admit_main_write(&resources(5, high), &shape, Some(&stage)).expect("fits");
        let rows = admitted.plan.batch_rows();
        assert!(
            rows < 136 && !shape.rows.is_multiple_of(rows) && !136_usize.is_multiple_of(rows),
            "small batches split integrations: {rows} rows of {}",
            shape.rows
        );
        high
    };
    let manifest = |name: &str, workers: usize, memory: u64| {
        let path = directory.path().join(name);
        session_predicting(&path, resources(workers, memory), true, Some(&model))
            .close()
            .expect("close");
        measurement_set_manifest(&path)
    };
    let first = manifest("first.ms", 1, large);
    for (name, workers, memory) in [
        ("four.ms", 5, large),
        ("small.ms", 1, small),
        ("small-four.ms", 5, small),
    ] {
        assert_eq!(
            manifest(name, workers, memory),
            first,
            "{name}: a policy of {workers} workers and {memory} bytes"
        );
    }
    let noise_only = directory.path().join("noise-only.ms");
    session(&noise_only, resources(1, large), true)
        .close()
        .expect("close");
    assert_ne!(
        measurement_set_manifest(&noise_only),
        first,
        "the sessions predict the model"
    );
}

#[test]
fn a_close_refused_admission_writes_nothing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("refused.ms");
    let error = session(&path, resources(1, 1 << 20), true)
        .close()
        .expect_err("a megabyte does not hold the writer");
    let refusal = error.admission_refused().expect("an admission refusal");
    assert_eq!(refusal.available, 1 << 20);
    assert!(refusal.required > 1 << 20);
    assert!(
        std::fs::read_dir(directory.path())
            .expect("directory")
            .next()
            .is_none(),
        "nothing is staged or published"
    );
}
