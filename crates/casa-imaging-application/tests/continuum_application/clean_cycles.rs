// SPDX-License-Identifier: LGPL-3.0-or-later

//! Deconvolution through the major-cycle pass: minor-cycle bounds, worker
//! teams, iteration accounting and reconstruction masks.

use super::*;

#[test]
fn application_executes_single_ddid_stokes_i_mfs_hogbom_with_one_iteration() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("hogbom");

    let result = execute(&request(&measurement_set, &image_name, json!({})))
        .expect("native Högbom application execution");

    assert_eq!(result.total_minor_iterations, 1);
    assert_eq!(result.stop, Some(CleanStop::Iterations));
    assert_eq!(
        result
            .minor_cycles
            .last()
            .expect("minor diagnostic")
            .recorded_components
            .len(),
        1
    );
    assert!(
        result.visibility_products.is_none(),
        "a no-write clean must not manufacture per-visibility diagnostics"
    );
    if std::thread::available_parallelism().is_ok_and(|threads| threads.get() > 1) {
        assert!(
            result.workers > 1,
            "the balanced policy uses more than one worker on a parallel host",
        );
    }
    assert_standard_products(&image_name, &result.product_names());
}

#[test]
fn a_serial_request_runs_on_one_worker() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("serial-hogbom");
    let imaging = request(&measurement_set, &image_name, json!({ "parallel": false }));

    let result =
        casa_imaging_application::execute(&imaging, context_with(imaging.resource_policy()))
            .expect("serial native Högbom execution");
    assert_eq!(result.workers, 1);
}

#[test]
fn uniform_multi_spw_mfs_clark_matches_serial_with_four_admitted_workers() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let measurement_set = four_spw_vla_measurement_set(root.path());
    for selection in ["0~3", "0:0,1:0~2,2:0~4,3:0~6"] {
        assert_uniform_mfs_workers(&measurement_set, root.path(), selection);
    }
}

fn assert_uniform_mfs_workers(measurement_set: &Path, root: &Path, selection: &str) {
    // An eight-thread host, so the policy, not this machine, sets the team.
    let host = HostResources {
        threads: 8,
        performance_cores: 8,
        ..HostResources::detect().expect("host")
    };
    let mut prefixes = Vec::new();
    for workers in [1, 4, 8] {
        let prefix = root.join(format!("uniform-mfs-{selection}-w{workers}"));
        let imaging = request(
            measurement_set,
            &prefix,
            json!({
                "deconvolver": "clark",
                "imsize": 256,
                "cell": "3arcsec",
                "ddid": null,
                "spw": selection,
                "channel_start": null,
                "channel_count": null,
                "weighting": "uniform",
                "gain": 0.1,
            }),
        );
        let result = casa_imaging_application::execute(
            &imaging,
            RunContext {
                host,
                policy: ResourcePolicy::Explicit {
                    workers,
                    memory: 2 << 30,
                },
                cancel: Cancel::new(),
                summary: None,
            },
        )
        .expect("uniform multi-SPW MFS Clark execution");
        assert!(result.total_actual_minor_iterations > 0);
        assert_eq!(result.workers, workers);
        assert_standard_products(&prefix, &result.product_names());
        prefixes.push(prefix);
    }
    assert!(
        prefixes.len() >= 2,
        "parity needs a serial and a parallel run"
    );
    for candidate in 1..prefixes.len() {
        for suffix in PRODUCT_SUFFIXES {
            let left = PagedImage::<f32>::open(PathBuf::from(format!(
                "{}{suffix}",
                prefixes[0].display()
            )))
            .unwrap();
            let right = PagedImage::<f32>::open(PathBuf::from(format!(
                "{}{suffix}",
                prefixes[candidate].display()
            )))
            .unwrap();
            assert_eq!(left.shape(), right.shape());
            assert_eq!(left.units(), right.units());
            assert_eq!(left.default_mask_name(), right.default_mask_name());
            let left = left.get().unwrap();
            let right = right.get().unwrap();
            let mut square_error = 0.0_f64;
            let mut square_signal = 0.0_f64;
            for (&left, &right) in left.iter().zip(right.iter()) {
                assert_eq!(left.is_finite(), right.is_finite(), "{suffix} validity");
                if left.is_finite() {
                    if suffix == ".mask" {
                        assert_eq!(left, right);
                    }
                    square_error += f64::from(left - right).powi(2);
                    square_signal += f64::from(left).powi(2);
                }
            }
            assert!(
                square_error.sqrt() <= 1e-6 * square_signal.sqrt().max(1e-12),
                "{suffix} normalized difference"
            );
        }
    }
}

#[test]
fn application_clark_cleans_until_a_casa_stopping_rule() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("clark");

    let result = execute(&request(
        &measurement_set,
        &image_name,
        json!({ "deconvolver": "clark" }),
    ))
    .expect("exact Clark execution");

    assert!(
        result.total_minor_iterations > 0,
        "active Clark execution must make scientific progress"
    );
    assert!(result.stop.is_some(), "cleaning ends on a CASA stop code");
}

#[test]
fn application_reconciles_between_bounded_minor_cycles() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("bounded-cycles");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "niter": 3,
            "minor_cycle_length": 1,
            "nmajor": 3,
            "gain": 0.37,
            "threshold": "1e-12Jy",
            "nsigma": 1.0e-12,
            "cyclefactor": 1.4,
        }),
    );

    let result = execute(&imaging).expect("bounded multi-cycle execution");

    assert_eq!(result.total_minor_iterations, 3);
    assert_eq!(result.total_actual_minor_iterations, 3);
    assert_eq!(result.major_cycle_count, 4);
    assert_eq!(result.minor_cycles.len(), 3);
    assert_eq!(
        result
            .minor_cycles
            .iter()
            .map(|cycle| cycle.cycle)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        result
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1)
    );
    assert_eq!(
        result
            .minor_cycles
            .iter()
            .map(|cycle| (
                cycle.iterations_entering,
                cycle.iterations,
                cycle.total_iterations,
                cycle.associated_replay_ordinal,
            ))
            .collect::<Vec<_>>(),
        vec![(0, 1, 1, 1), (1, 1, 2, 2), (2, 1, 3, 3)]
    );
    assert!(result.minor_cycles.iter().all(|cycle| {
        cycle.initial_peak_flux.is_finite()
            && cycle.final_peak_flux.is_finite()
            && cycle.global_threshold.is_finite()
            && cycle.effective_threshold.is_finite()
    }));
    assert_eq!(result.stop, Some(CleanStop::Iterations));
    assert_standard_products(&image_name, &result.product_names());
}

#[test]
fn application_uses_reported_iterations_for_casa_inclusive_continuation() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let imaging = request(
        &measurement_set,
        &root.path().join("unlimited-major-cycles"),
        json!({
            "niter": 3,
            "minor_cycle_length": 1,
            "hogbom_iteration_mode": "casa-inclusive",
            "nmajor": -1,
            "gain": 0.37,
            "threshold": "1e-12Jy",
            "nsigma": 1.0e-12,
            "cyclefactor": 0.01,
            "minpsffraction": 0.0,
            "maxpsffraction": 0.01,
        }),
    );

    let result = execute(&imaging).expect("unlimited major-cycle execution");

    assert_eq!(result.total_minor_iterations, 3);
    assert_eq!(result.total_actual_minor_iterations, 6);
    assert_eq!(result.minor_cycles.len(), 3);
    assert_eq!(result.major_cycle_count, 4);
    assert!(
        result
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1 && cycle.actual_iterations == 2),
        "every bound-stopped cycle charges one reported iteration after applying two components"
    );
}

#[test]
fn application_materializes_static_and_auto_masks_at_the_normal_state_boundary() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");

    let static_ms = tiny_measurement_set(root.path());
    let static_image = root.path().join("static-mask");
    let static_result = execute(&request(
        &static_ms,
        &static_image,
        json!({ "mask_box": "4,4,11,11" }),
    ))
    .expect("static-mask solve");
    assert!(
        static_result
            .minor_cycles
            .last()
            .expect("minor-cycle evidence")
            .auto_mask
            .is_none()
    );
    let published_mask = PagedImage::<f32>::open(root.path().join("static-mask.mask"))
        .expect("open published reconstruction mask");
    let mask_pixels = published_mask
        .get_slice(&[0, 0, 0, 0], &[16, 16, 1, 1])
        .expect("read published reconstruction mask");
    assert_eq!(mask_pixels[[0, 0, 0, 0]], 0.0);
    assert_eq!(mask_pixels[[8, 8, 0, 0]], 1.0);
    assert_model_residual_respect_mask(&static_image, 64);

    let image_root = root.path().join("image-mask-input");
    std::fs::create_dir(&image_root).expect("image-mask fixture directory");
    let image_ms = tiny_measurement_set(&image_root);
    let mask_path = root.path().join("shifted.mask");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        casa_types::measures::direction::DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [
            -std::f64::consts::PI / (180.0 * 3600.0),
            std::f64::consts::PI / (180.0 * 3600.0),
        ],
        [9.0, 8.0],
    ));
    let mut image = PagedImage::<f32>::create(vec![16, 16], coordinates, &mask_path)
        .expect("create shifted CASA image mask");
    let mut pixels = ArrayD::from_elem(ndarray::IxDyn(&[16, 16]), 0.0_f32);
    pixels[[3, 3]] = 1.0;
    image
        .put_slice(&pixels, &[0, 0])
        .expect("write mask pixels");
    image.save().expect("persist image mask");
    let image_output = root.path().join("image-mask");
    let image_result = execute(&request(
        &image_ms,
        &image_output,
        json!({ "mask_image": mask_path }),
    ))
    .expect("reprojected image-mask solve");
    assert!(!image_result.minor_cycles.is_empty());
    let reprojected_mask = product_plane(&image_output, ".mask");
    assert_eq!(reprojected_mask[[2, 3, 0, 0]], 1.0);
    assert_model_residual_respect_mask(&image_output, 1);

    let auto_root = root.path().join("auto-input");
    std::fs::create_dir(&auto_root).expect("auto fixture directory");
    let auto_ms = tiny_measurement_set(&auto_root);
    let auto_result = execute(&request(
        &auto_ms,
        &root.path().join("auto-mask"),
        json!({
            "usemask": "auto-multithresh",
            "sidelobethreshold": 0.0,
            "noisethreshold": 0.0,
            "lownoisethreshold": 0.0,
            "negativethreshold": 0.0,
            "minbeamfrac": 0.0,
            "growiterations": 0,
            "niter": 2,
            "minor_cycle_length": 1,
            "nmajor": 2,
            "gain": 0.1,
        }),
    ))
    .expect("auto-mask solve");
    let cycles = &auto_result.minor_cycles;
    assert_eq!(cycles.len(), 2);
    // The first automatic mask has no prior mask, so every support pixel is a
    // change; the second changes exactly the pixels where it differs from
    // the first, which it consumed as its prior mask.
    let first_evidence = cycles[0].auto_mask.expect("first auto-mask evidence");
    assert_eq!(
        first_evidence.changed_pixels,
        cycles[0]
            .mask_support
            .iter()
            .filter(|value| **value)
            .count()
    );
    let evidence = cycles[1].auto_mask.expect("second auto-mask evidence");
    assert_eq!(
        evidence.changed_pixels,
        cycles[0]
            .mask_support
            .iter()
            .zip(&cycles[1].mask_support)
            .filter(|(first, second)| first != second)
            .count(),
        "the next automatic mask must evolve from the prior mask"
    );
    assert!(evidence.robust_rms.is_finite());
    assert!(evidence.positive_threshold.is_finite());
    assert_eq!(auto_result.major_cycle_count, 3);
    assert_eq!(auto_result.total_minor_iterations, 2);
}
