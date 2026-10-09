// SPDX-License-Identifier: LGPL-3.0-or-later

//! Deconvolution through the major-cycle pass: minor-cycle bounds, worker
//! teams, iteration accounting and reconstruction masks.

use super::*;

#[test]
fn application_executes_single_ddid_stokes_i_mfs_hogbom_with_one_iteration() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("hogbom");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

    let result = execute_continuum(imaging).expect("native Högbom application execution");

    assert_eq!(result.minor_iterations, 1);
    assert_eq!(result.stop, Some(CleanStop::Iterations));
    assert_eq!(
        result
            .outcome
            .output
            .minor_cycles
            .last()
            .expect("minor diagnostic")
            .recorded_components
            .len(),
        1
    );
    let output = &result.outcome.output;
    assert!(
        output.visibility_products.is_none(),
        "a no-write clean must not manufacture per-visibility diagnostics"
    );
    if std::thread::available_parallelism().is_ok_and(|threads| threads.get() > 1) {
        assert!(
            output.workers > 1,
            "normal production planning must not pin the passes to the serial baseline on a parallel host",
        );
    }
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn application_serial_cpu_requirement_caps_replay_to_one_worker() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("serial-hogbom");
    let mut imaging = request(measurement_set, image_name, ContinuumAlgorithm::Hogbom);
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];
    imaging.resource_policy =
        casa_imaging_runtime::ResourcePolicy::Explicit(casa_imaging_runtime::ResourceOverride {
            workers: Some(1),
            ..casa_imaging_runtime::ResourceOverride::default()
        });

    let result = execute_continuum(imaging).expect("serial native Högbom execution");
    assert_eq!(result.outcome.output.workers, 1);
}

#[test]
fn uniform_multi_spw_mfs_clark_matches_serial_with_four_admitted_workers() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    set_production_io_environment();
    let root = tempfile::tempdir().unwrap();
    let measurement_set = four_spw_vla_measurement_set(root.path());
    for selection in ["0~3", "0:0,1:0~2,2:0~4,3:0~6"] {
        assert_uniform_mfs_workers(measurement_set.clone(), root.path(), selection);
    }
}

fn assert_uniform_mfs_workers(measurement_set: PathBuf, root: &Path, selection: &str) {
    // Admission caps explicit worker overrides at the host thread count, so a
    // team larger than this host is infeasible rather than a parity failure.
    let host_threads = std::thread::available_parallelism().unwrap().get() as u64;
    assert!(host_threads >= 4, "parity needs at least four host threads");
    let mut prefixes = Vec::new();
    for workers in [1, 4, 8]
        .into_iter()
        .filter(|&workers| workers <= host_threads)
    {
        let prefix = root.join(format!("uniform-mfs-{selection}-w{workers}"));
        let mut imaging = request(
            measurement_set.clone(),
            prefix.clone(),
            ContinuumAlgorithm::Clark,
        );
        imaging.image_size = 256;
        imaging.cell_arcsec = 3.0;
        imaging.data_description = None;
        imaging.spectral_window = Some(selection.into());
        imaging.channel_start = None;
        imaging.channel_count = None;
        imaging.weighting = ContinuumWeighting::Uniform;
        imaging.gain = 0.1;
        imaging.task_requirements = if workers == 1 {
            vec![TaskRequirement::SerialCpu]
        } else {
            vec![]
        };
        imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(workers),
                memory_bytes: std::collections::BTreeMap::from([(
                    casa_imaging_runtime::CapacityDomainId::new("host-memory"),
                    2 << 30,
                )]),
                ..Default::default()
            },
        );
        let result = execute_continuum(imaging).expect("uniform multi-SPW MFS Clark execution");
        assert!(result.actual_minor_iterations > 0);
        let pass_workers = result.outcome.output.workers as u64;
        assert!((1..=workers).contains(&pass_workers));
        if workers <= 4 {
            assert_eq!(pass_workers, workers);
        }
        assert_standard_products(&prefix, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("clark");
    let imaging = request(measurement_set, image_name, ContinuumAlgorithm::Clark);

    let result = execute_continuum(imaging).expect("exact Clark execution");

    assert!(
        result.minor_iterations > 0,
        "active Clark execution must make scientific progress"
    );
    assert!(result.stop.is_some(), "cleaning ends on a CASA stop code");
}

#[test]
fn application_reconciles_between_bounded_minor_cycles() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("bounded-cycles");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.cycle_factor = 1.4;

    let result = execute_continuum(imaging).expect("bounded multi-cycle execution");

    assert_eq!(result.minor_iterations, 3);
    assert_eq!(result.actual_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_actual_minor_iterations, 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert_eq!(result.outcome.output.minor_cycles.len(), 3);
    assert_eq!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .map(|cycle| cycle.cycle)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1)
    );
    assert_eq!(
        result
            .outcome
            .output
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
    assert!(result.outcome.output.minor_cycles.iter().all(|cycle| {
        cycle.initial_peak_flux.is_finite()
            && cycle.final_peak_flux.is_finite()
            && cycle.global_threshold.is_finite()
            && cycle.effective_threshold.is_finite()
    }));
    assert_eq!(result.stop, Some(CleanStop::Iterations));
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn application_uses_reported_iterations_for_casa_inclusive_continuation() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let mut imaging = request(
        measurement_set,
        root.path().join("unlimited-major-cycles"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.hogbom_iteration_accounting =
        casa_imaging_application::HogbomIterationAccounting::CasaInclusive;
    imaging.maximum_major_cycles = None;
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.cycle_factor = 0.01;
    imaging.minimum_psf_fraction = 0.0;
    imaging.maximum_psf_fraction = 0.01;

    let result = execute_continuum(imaging).expect("unlimited major-cycle execution");

    assert_eq!(result.outcome.output.total_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_actual_minor_iterations, 6);
    assert_eq!(result.outcome.output.minor_cycles.len(), 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1 && cycle.actual_iterations == 2),
        "every bound-stopped cycle charges one reported iteration after applying two components"
    );
}

#[test]
fn application_materializes_static_and_auto_masks_at_the_normal_state_boundary() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");

    let static_ms = tiny_measurement_set(root.path());
    let static_image = root.path().join("static-mask");
    let mut static_request = request(static_ms, static_image.clone(), ContinuumAlgorithm::Hogbom);
    static_request.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
        blc: [4, 4],
        trc: [11, 11],
    }]);
    let static_result = execute_continuum(static_request).expect("static-mask solve");
    assert!(
        static_result
            .outcome
            .output
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
    let mut image_request = request(image_ms, image_output.clone(), ContinuumAlgorithm::Hogbom);
    image_request.mask = ContinuumMask::Image(mask_path);
    let image_result = execute_continuum(image_request).expect("reprojected image-mask solve");
    assert!(!image_result.outcome.output.minor_cycles.is_empty());
    let reprojected_mask = product_plane(&image_output, ".mask");
    assert_eq!(reprojected_mask[[2, 3, 0, 0]], 1.0);
    assert_model_residual_respect_mask(&image_output, 1);

    let auto_root = root.path().join("auto-input");
    std::fs::create_dir(&auto_root).expect("auto fixture directory");
    let auto_ms = tiny_measurement_set(&auto_root);
    let mut auto_request = request(
        auto_ms,
        root.path().join("auto-mask"),
        ContinuumAlgorithm::Hogbom,
    );
    auto_request.mask = ContinuumMask::AutoMultithresh(ContinuumAutoMaskControls {
        sidelobe_factor: 0.0,
        noise_factor: 0.0,
        low_noise_factor: 0.0,
        negative_factor: 0.0,
        minimum_beam_fraction: 0.0,
        smooth_factor: 1.0,
        cut_threshold: 0.01,
        grow_iterations: 0,
        minimum_percent_change: -1.0,
    });
    auto_request.iterations = 2;
    auto_request.cycle_iterations = 1;
    auto_request.maximum_major_cycles = Some(2);
    auto_request.gain = 0.1;
    let auto_result = execute_continuum(auto_request).expect("auto-mask solve");
    let cycles = &auto_result.outcome.output.minor_cycles;
    assert_eq!(cycles.len(), 2);
    let first_evidence = cycles[0].auto_mask.expect("first auto-mask evidence");
    assert_eq!(first_evidence.previous_mask_generation, None);
    let evidence = cycles[1].auto_mask.expect("second auto-mask evidence");
    assert_eq!(
        evidence.previous_mask_generation,
        Some(cycles[0].mask_generation),
        "the next automatic mask must retain the exact prior generation"
    );
    assert!(cycles.iter().all(|cycle| cycle.mask_normal_state.is_some()));
    assert_ne!(
        cycles[0].mask_normal_state, cycles[1].mask_normal_state,
        "each automatic-mask generation must consume the current reconciled Normal State"
    );
    assert_ne!(
        cycles[0].mask_model_generation, cycles[1].mask_model_generation,
        "each automatic mask must constrain the current model generation"
    );
    assert!(evidence.robust_rms.is_finite());
    assert!(evidence.positive_threshold.is_finite());
    assert_eq!(auto_result.outcome.output.major_cycle_count, 3);
    assert_eq!(auto_result.minor_iterations, 2);
}
