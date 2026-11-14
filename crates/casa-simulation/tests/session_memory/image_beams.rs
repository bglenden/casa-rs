// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admitted image-plane beams retain and prepare their sky within their charges.
use super::*;
use casa_tables::{Table, TableOptions};
use casa_types::Value;

fn profile(path: &Path, n: usize) {
    let mut cs = ImageCoordinates::new();
    cs.add_coordinate(DirectionCoordinate::new(
        DirectionRef::AZEL,
        Projection::new(ProjectionType::SIN),
        [0., 0.],
        [2e-6, 2e-6],
        [(n / 2) as f64; 2],
    ));
    cs.add_coordinate(StokesCoordinate::new(vec![
        StokesType::XX,
        StokesType::XY,
        StokesType::YX,
        StokesType::YY,
    ]));
    cs.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::TOPO,
        150e9,
        1e9,
        0.,
        150e9,
    ));
    let shape = vec![n, n, 4, 1];
    let mut image = PagedImage::<Complex32>::create(shape.clone(), cs, path).unwrap();
    let values = ArrayD::from_shape_fn(IxDyn(&shape), |i| {
        let radius = ((i[0] as f64 - n as f64 / 2.) / n as f64).powi(2)
            + ((i[1] as f64 - n as f64 / 2.) / n as f64).powi(2);
        let profile = (-16. * radius).exp() as f32;
        if i[2] == 0 || i[2] == 3 {
            Complex32::new(0.8 * profile, 0.1 * profile)
        } else {
            Complex32::new(0.02 * profile, 0.)
        }
    });
    image.put_slice(&values, &[0, 0, 0, 0]).unwrap();
    image.save().unwrap();
}

fn beam_table(source: &Path, path: &Path, image: &Path) {
    std::fs::create_dir_all(path).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = path.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            beam_table(&entry.path(), &to, image);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
    if path.join("VPLIST_DEFAULTS").exists() {
        let mut table = Table::open(TableOptions::new(path)).unwrap();
        let mut record = table
            .cell_accessor(0, "pbdescription")
            .unwrap()
            .record()
            .unwrap();
        *record.get_mut("compleximage").unwrap() = Value::Scalar(casa_types::ScalarValue::String(
            image.to_string_lossy().into_owned(),
        ));
        table
            .cell_accessor_mut(0, "pbdescription")
            .unwrap()
            .set_record(record)
            .unwrap();
        table.flush().unwrap();
    }
}

fn measured(
    path: &Path,
    sky: &Path,
    beam: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    radial: bool,
) -> (Predicted, Closed) {
    let mut sm = if radial {
        configured_with_beam(
            path,
            measures,
            BUDGET_320,
            4,
            &[25.],
            "RR RL LR LL",
            ("VLA", 1.4e9),
        )
    } else {
        configured(path, measures, BUDGET_320, 4)
    };
    sm.observe(&scan(2)).unwrap();
    sm.set_vp(&VoltagePatternSpec {
        use_default_vp: false,
        vp_table: beam.to_string_lossy().into_owned(),
        do_squint: radial,
        ..VoltagePatternSpec::default()
    })
    .unwrap();
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    sm.predict(&PredictSpec {
        images: vec![sky.to_path_buf()],
        component_lists: if radial {
            Vec::new()
        } else {
            vec![
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_voltage_patterns/sky.cl"),
            ]
        },
        ..PredictSpec::default()
    })
    .unwrap();
    let predicted = Predicted {
        during: Closed {
            heap: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - reserved,
        },
        beyond: EXCESS.load(Ordering::Relaxed) - before,
        kept: Closed {
            heap: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
            admitted: reserved_memory() - reserved,
        },
    };
    drop(window);
    let (closed, pass) = measured_close(sm, path, true);
    assert!(pass.unwrap().batches > 0);
    (predicted, closed)
}

#[test]
fn jones_image_storage_is_admitted_once_for_both_sky_consumers() {
    if !alone("image_beams::jones_image_storage_is_admitted_once_for_both_sky_consumers") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let sky = directory.path().join("sky.image");
    write_model_of(&sky, 4, 64);
    let mut small: Option<Predicted> = None;
    for n in [96, 768] {
        let image = directory.path().join(format!("{n}.image"));
        profile(&image, n);
        let beam = directory.path().join(format!("{n}.vp"));
        beam_table(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_beams/complex.vp"),
            &beam,
            &image,
        );
        let path = directory.path().join(format!("{n}.ms"));
        measured(&path, &sky, &beam, &measures, false);
        for _ in 0..MEASURED_RUNS {
            let (predicted, closed) = measured(&path, &sky, &beam, &measures, false);
            println!("{n} squared Jones image: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
            if n == 96 {
                small = Some(predicted);
            } else {
                let small = small.as_ref().unwrap();
                let pixels = (n * n * size_of::<[Complex32; 4]>()) as u64;
                assert!(predicted.kept.heap > pixels);
                assert!(predicted.kept.heap <= small.kept.heap + pixels + ALLOWANCE_BYTES);
                assert!(predicted.kept.admitted <= small.kept.admitted + pixels + ALLOWANCE_BYTES);
            }
        }
    }
}

#[test]
fn squinted_polarized_image_grids_keep_their_admission() {
    if !alone("image_beams::squinted_polarized_image_grids_keep_their_admission") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let beam =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_squint/custom_gaussian.vp");
    for n in [96, 512] {
        let sky = directory.path().join(format!("{n}.image"));
        let mut cs = ImageCoordinates::new();
        cs.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            CENTRE_RAD,
            [-2e-4, 2e-4],
            [(n / 2) as f64; 2],
        ));
        cs.add_coordinate(StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
            StokesType::U,
            StokesType::V,
        ]));
        cs.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::TOPO,
            1.4e9,
            1e9,
            0.,
            1.4e9,
        ));
        let shape = vec![n, n, 4, 1];
        let mut image = PagedImage::<f32>::create(shape.clone(), cs, &sky).unwrap();
        let values = ArrayD::from_shape_fn(IxDyn(&shape), |i| {
            if i[0] == n / 2 && i[1] == n / 2 {
                [1., 0.1, 0.04, 0.03][i[2]]
            } else {
                0.
            }
        });
        image.put_slice(&values, &[0, 0, 0, 0]).unwrap();
        image.set_units("Jy/pixel").unwrap();
        image.save().unwrap();
        drop(image);
        drop(values);
        let path = directory.path().join(format!("{n}.ms"));
        measured(&path, &sky, &beam, &measures, true);
        for _ in 0..MEASURED_RUNS {
            let (predicted, closed) = measured(&path, &sky, &beam, &measures, true);
            println!("{n} squared squinted image: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
        }
    }
}
