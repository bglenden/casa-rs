// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full-Stokes planes, saved Jones beams and row-dependent total-power visits.
use super::*;

fn model(path: &Path, n: usize, local: bool, measures: &Arc<dyn MeasuresProvider>) {
    let mut coordinates = ImageCoordinates::new();
    let direction = MDirection::from_angles(CENTRE_RAD[0], CENTRE_RAD[1], DirectionRef::J2000);
    let frame = casa_types::measures::MeasFrame::new()
        .with_measures(measures.clone())
        .with_epoch(MEpoch::from_mjd(REFERENCE_MJD + 5. / 86400., EpochRef::UTC))
        .with_position(casa_types::measures::MPosition::new_itrf(
            -1601185., -5041977., 3554875.,
        ));
    let reference = if local {
        DirectionRef::AZEL
    } else {
        DirectionRef::J2000
    };
    let centre = direction.convert_to(reference, &frame).unwrap().as_angles();
    coordinates.add_coordinate(DirectionCoordinate::new(
        reference,
        Projection::new(ProjectionType::SIN),
        [centre.0, centre.1],
        [-CELL_RAD, CELL_RAD],
        [(n / 2) as f64; 2],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![
        StokesType::I,
        StokesType::Q,
        StokesType::U,
        StokesType::V,
    ]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        FREQUENCY_HZ,
        8. * CHANNEL_HZ,
        0.,
        FREQUENCY_HZ,
    ));
    let shape = vec![n, n, 4, 4];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for plane in 0..4 {
        for (pol, value) in [1., 0.2, -0.1, 0.05].into_iter().enumerate() {
            pixels[IxDyn(&[n / 2, n / 2, pol, plane])] = value;
        }
    }
    image.put_slice(&pixels, &[0, 0, 0, 0]).unwrap();
    image.set_units("Jy/pixel").unwrap();
    image.save().unwrap();
}

fn measured(
    ms: &Path,
    sky: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    jones: bool,
    integrations: usize,
) -> (Predicted, Closed) {
    let mut sm = configured_with_products(ms, measures, BUDGET_320, 32, &[12.], "XX XY YX YY");
    sm.observe(&scan(integrations)).unwrap();
    sm.set_options(&PredictionOptions {
        ftmachine: "sd".into(),
        grid_function: "BOX".into(),
        ..PredictionOptions::default()
    })
    .unwrap();
    if jones {
        let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_beams");
        let beam = ms.parent().unwrap().join("beam.vp");
        if !beam.exists() {
            image_beams::beam_table(&data.join("jones.vp"), &beam, &data.join("jones.image"));
        }
        sm.set_vp(&VoltagePatternSpec {
            vp_table: beam.to_string_lossy().into_owned(),
            use_default_vp: false,
            do_squint: false,
            ..VoltagePatternSpec::default()
        })
        .unwrap();
    }
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    sm.predict(&PredictSpec {
        images: vec![sky.to_path_buf()],
        ..PredictSpec::default()
    })
    .unwrap();
    let prediction = Predicted {
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
    let (closed, _) = measured_close(sm, ms, false);
    (prediction, closed)
}

#[test]
fn planes_beams_and_timed_visits_hold_their_admission() {
    if !alone("total_power::planes_beams_and_timed_visits_hold_their_admission") {
        return;
    }
    let resources = resources(BUDGET_320);
    charge_process_caches(resources.host(), resources.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for (n, integrations) in [(96, 8), (768, 1024)] {
        for (name, local, jones) in [
            ("stokes", false, false),
            ("jones", false, true),
            ("azel", true, false),
        ] {
            let sky = directory.path().join(format!("{name}-{n}.image"));
            model(&sky, n, local, &measures);
            let ms = directory.path().join(format!("{name}-{n}.ms"));
            measured(&ms, &sky, &measures, jones, integrations);
            for _ in 0..MEASURED_RUNS {
                let (prediction, close) = measured(&ms, &sky, &measures, jones, integrations);
                println!(
                    "SD {name} {n} pixels/{integrations} integrations: {prediction:?}; close {close:?}"
                );
                assert!(prediction.beyond <= ALLOWANCE_BYTES as i64);
                assert!(prediction.during.heap <= prediction.during.admitted + ALLOWANCE_BYTES);
                assert!(prediction.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
                assert!(close.heap <= close.admitted + ALLOWANCE_BYTES);
            }
        }
    }
}
