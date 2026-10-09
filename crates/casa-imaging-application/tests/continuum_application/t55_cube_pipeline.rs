// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn streaming_cube_complete_application_handoff() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let image_name = root.path().join("native-cube");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Clark,
    );
    imaging.image_size = 64;
    imaging.weighting = ContinuumWeighting::Natural;
    imaging.spectral_window = Some("0:0~3".into());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(4),
    };
    imaging.beam_policy = ContinuumBeamPolicy::Common;
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.resource_policy = ResourcePolicy::Explicit {
        workers: 1,
        memory: 4 << 30,
    };
    let mut capped = imaging.clone();
    capped.resource_policy = ResourcePolicy::Explicit {
        workers: 1,
        memory: 1 << 20,
    };
    let error = execute_continuum(capped)
        .err()
        .expect("insufficient memory must fail closed");
    assert!(
        matches!(
            error,
            casa_imaging_application::ApplicationDispatchError::Admission(_)
        ),
        "{error}"
    );
    assert!(!image_name.with_extension("image").exists());

    let result = execute_continuum(imaging.clone()).expect("complete native cube application");
    assert_standard_products(&image_name, &result.product_names);
    assert_eq!(result.outcome.output.major_cycle_count, 3);
    assert_eq!(result.outcome.output.minor_cycles.len(), 2);
    assert!(result.actual_minor_iterations > 0);
    for channel in 0..4 {
        let normal = result
            .outcome
            .output
            .scientific
            .normal_state()
            .read_window(channel..channel + 1)
            .unwrap();
        assert!(
            normal
                .residual()
                .iter()
                .all(|v| v.re.is_finite() && v.im.is_finite())
        );
    }
    drop(result);
    imaging.image_name = root.path().join("cube-model-column");
    imaging.save_model_column = true;
    imaging.resource_policy = ResourcePolicy::Explicit {
        workers: 2,
        memory: 4 << 30,
    };
    let output = execute_continuum(imaging).expect("cube visibility output keeps its owner");
    assert_eq!(output.outcome.output.major_cycle_count, 3);
}

#[test]
fn streaming_cube_single_output_runs_clean_refresh_and_publication() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let mut imaging = request(
        measurement_set,
        root.path().join("single-plane"),
        ContinuumAlgorithm::Clark,
    );
    imaging.image_size = 64;
    imaging.spectral_window = Some("0:0~3".into());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(1),
    };
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];
    let prefix = imaging.image_name.clone();
    let result = execute_continuum(imaging).expect("one output plane on native cube path");
    assert_standard_products(&prefix, &result.product_names);
    assert_eq!(result.actual_minor_iterations, 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert_eq!(
        PagedImage::<f32>::open(prefix.with_extension("image"))
            .unwrap()
            .shape(),
        &[64, 64, 1, 1]
    );
}

#[test]
fn t55_shifted_cube_density_retains_native_endpoint_weights() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = four_channel_measurement_set(root.path());
    let mut imaging = request(
        measurement_set,
        root.path().join("shifted-density"),
        ContinuumAlgorithm::Dirty,
    );
    imaging.weighting = ContinuumWeighting::Briggs(0.5);
    imaging.spectral_window = Some("0:0~3".into());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::FrequencyHz {
                hz: 44.0e9 - 200_000.0,
                frame: Some(FrequencyRef::TOPO),
            }),
            width: Some(CubeAxisValue::FrequencyHz {
                hz: 1_000_000.0,
                frame: Some(FrequencyRef::TOPO),
            }),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(4),
    };
    imaging.task_requirements =
        vec![casa_imaging_application::TaskRequirement::PerChannelWeightDensity];
    let result = execute_continuum(imaging).expect("shifted native endpoint density execution");
    // CASA `estimateSwingChanPad`: no frame swing, plus max(min(4, nchan/10), 1).
    let weighting = result.outcome.output.problem.weighting();
    assert_eq!(weighting.casa_cube_density_padding(), Some(1));
    assert_eq!(
        weighting.density_scope(),
        WeightDensityScope::PerOutputChannel
    );
}

#[test]
fn t55_per_channel_density_request_is_bound_into_the_executed_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    for cube in [true, false] {
        for per_channel in [true, false] {
            for weighting in [
                ContinuumWeighting::Briggs(0.5),
                ContinuumWeighting::Uniform,
                ContinuumWeighting::Natural,
            ] {
                let mut imaging = request(
                    measurement_set.clone(),
                    root.path()
                        .join(format!("density-{cube}-{per_channel}-{weighting:?}")),
                    ContinuumAlgorithm::Dirty,
                );
                imaging.weighting = weighting;
                imaging.spectral_window = Some("0:0~3".into());
                imaging.channel_count = Some(4);
                if cube {
                    imaging.spectral_mode = SpectralImagingMode::Cube {
                        axis: CubeAxisConfig {
                            outframe: FrequencyRef::TOPO,
                            ..CubeAxisConfig::default()
                        },
                        output_channels: Some(4),
                    };
                }
                if per_channel {
                    imaging
                        .task_requirements
                        .push(casa_imaging_application::TaskRequirement::PerChannelWeightDensity);
                }
                let result = execute_continuum(imaging).expect("density scope execution");
                let expected = if weighting == ContinuumWeighting::Natural {
                    WeightDensityScope::NotApplicable
                } else if cube && per_channel {
                    WeightDensityScope::PerOutputChannel
                } else {
                    WeightDensityScope::GlobalSelection
                };
                assert_eq!(
                    result.outcome.output.problem.weighting().density_scope(),
                    expected,
                    "cube={cube} per_channel={per_channel} weighting={weighting:?}",
                );
            }
        }
    }
}

#[test]
fn t55_clark_cube_products_and_repeated_cycles_agree_across_worker_counts() {
    compare_clark_cube_cases(
        &[(1, None), (2, None), (4, None)],
        &[ContinuumWeighting::Natural, ContinuumWeighting::Briggs(0.5)],
    );
}

#[test]
fn t55_clark_cube_products_and_repeated_cycles_agree_across_channel_windows() {
    compare_clark_cube_cases(
        &[
            (1, None),
            (1, Some((8 << 20) + (512 << 10))),
            (1, Some((9 << 20) + (128 << 10))),
            (1, Some((10 << 20) + (128 << 10))),
        ],
        &[ContinuumWeighting::Briggs(0.5)],
    );
}

fn compare_clark_cube_cases(cases: &[(usize, Option<u64>)], weightings: &[ContinuumWeighting]) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    for &weighting in weightings {
        let mut baseline = None;
        for &(workers, memory_bytes) in cases {
            let image_name = root
                .path()
                .join(format!("clark-{weighting:?}-{workers}-{memory_bytes:?}"));
            let mut imaging = request(
                measurement_set.clone(),
                image_name.clone(),
                ContinuumAlgorithm::Clark,
            );
            imaging.image_size = 64;
            imaging.weighting = weighting;
            imaging.spectral_window = Some("0:0~3".into());
            imaging.channel_count = Some(4);
            imaging.spectral_mode = SpectralImagingMode::Cube {
                axis: CubeAxisConfig {
                    outframe: FrequencyRef::TOPO,
                    start: Some(CubeAxisValue::Channel(3)),
                    width: Some(CubeAxisValue::Channel(-1)),
                    ..CubeAxisConfig::default()
                },
                output_channels: Some(4),
            };
            imaging.beam_policy = ContinuumBeamPolicy::Common;
            imaging.iterations = 3;
            imaging.cycle_iterations = 1;
            imaging.maximum_major_cycles = Some(3);
            imaging.gain = 0.37;
            imaging.threshold_jy = 1.0e-12;
            imaging.noise_sigma = Some(1.0e-12);
            // A four-thread host, so the policy, not this machine, sets the team.
            imaging.host = HostResources {
                threads: 4,
                performance_cores: 4,
                ..HostResources::detect().expect("host")
            };
            imaging.resource_policy = ResourcePolicy::Explicit {
                workers,
                memory: memory_bytes.unwrap_or(u64::MAX),
            };
            let started = std::time::Instant::now();
            let result = execute_continuum(imaging).expect("bounded production Clark cube");
            eprintln!(
                "t55_canonical_cube_timing image_size=64 channels=4 weighting={weighting:?} requested_workers={workers} memory_bytes={memory_bytes:?} execute_continuum_seconds={:.9} major_cycles={} minor_iterations={}",
                started.elapsed().as_secs_f64(),
                result.outcome.output.major_cycle_count,
                result.actual_minor_iterations
            );
            assert_standard_products(&image_name, &result.product_names);
            assert!(result.outcome.output.major_cycle_count > 1);
            assert!(
                result.outcome.output.minor_cycles.len() > 1,
                "fixture must cross a synchronized major-cycle boundary"
            );
            assert!(result.actual_minor_iterations > 0);
            assert_eq!(result.outcome.output.workers, workers);
            let mut products = Vec::new();
            for suffix in PRODUCT_SUFFIXES {
                let product = PagedImage::<f32>::open(PathBuf::from(format!(
                    "{}{suffix}",
                    image_name.display()
                )))
                .expect("open every cube product");
                let shape = product.shape().to_vec();
                let values = product
                    .get_slice(&[0; 4], &shape)
                    .expect("read complete product")
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                let mask = product
                    .get_mask_slice(&[0; 4], &shape, &[1; 4])
                    .expect("product validity")
                    .map(|mask| mask.iter().copied().collect::<Vec<_>>());
                let first = product
                    .coordinates()
                    .to_world(&[0.0; 4])
                    .expect("first pixel WCS");
                let last = product
                    .coordinates()
                    .to_world(&[0.0, 0.0, 0.0, 3.0])
                    .expect("last channel WCS");
                assert!(first[3] > last[3]);
                if suffix == ".mask" {
                    let blank_support = product
                        .get_slice(&[0; 4], &[64, 64, 1, 1])
                        .expect("blank channel search support");
                    assert!(
                        blank_support.iter().all(|value| *value == 1.0),
                        "full-plane CLEAN support is independent of blank-channel data validity"
                    );
                }
                if matches!(suffix, ".residual" | ".image") {
                    assert!(
                        product.mask_names().is_empty(),
                        "no PB source means no stored mask"
                    );
                    assert!(
                        product
                            .get_slice(&[0; 4], &[64, 64, 1, 1])
                            .unwrap()
                            .iter()
                            .all(|value| *value == 0.0)
                    );
                }
                products.push((
                    suffix,
                    shape,
                    values,
                    mask,
                    product.units().to_owned(),
                    first,
                    last,
                ));
            }
            let science = &result.outcome.output.scientific;
            let evidence = (
                products,
                fixture_model_samples(science.final_model()),
                (0..science.normal_state().sum_weights().len())
                    .flat_map(|channel| {
                        science
                            .normal_state()
                            .read_window(channel..channel + 1)
                            .unwrap()
                            .residual()
                            .iter()
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
                science.normal_state().sum_weights().to_vec(),
                result.actual_minor_iterations,
                result.outcome.output.major_cycle_count,
            );
            if let Some((products, model, residual, weights, iterations, majors)) =
                baseline.replace(evidence)
            {
                let evidence = baseline.as_ref().unwrap();
                for (expected, actual) in products.iter().zip(&evidence.0) {
                    assert_eq!(expected.0, actual.0);
                    assert_eq!(expected.1, actual.1);
                    assert_real_agreement(&expected.2, &actual.2);
                    assert_eq!(expected.3, actual.3);
                    assert_eq!(expected.4, actual.4);
                    assert_eq!(expected.5, actual.5);
                    assert_eq!(expected.6, actual.6);
                }
                assert_model_agreement(&model, &evidence.1);
                assert_complex_agreement(&residual, &evidence.2);
                assert_real_agreement(&weights, &evidence.3);
                assert_eq!(iterations, evidence.4);
                assert_eq!(majors, evidence.5);
                baseline = Some((products, model, residual, weights, iterations, majors));
            }
        }
    }
}

#[test]
fn t55_signed_primary_beam_limit_separates_pixels_search_support_and_stored_masks() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = vla_spectral_line_measurement_set(root.path());
    for weighting in [ContinuumWeighting::Natural, ContinuumWeighting::Briggs(0.5)] {
        for cell_arcsec in [1.0, 120.0] {
            let image_stem = format!("signed-pb-{weighting:?}-{cell_arcsec}");
            let negative_image_name = root.path().join(format!("{image_stem}-negative"));
            let mut positive_pixels = None;
            let mut positive_graph = None;
            for pblimit in [0.2, -0.2] {
                let image_name = if pblimit > 0.0 {
                    root.path().join(format!("{image_stem}-positive"))
                } else {
                    negative_image_name.clone()
                };
                let mut imaging = request(
                    measurement_set.clone(),
                    image_name.clone(),
                    ContinuumAlgorithm::Clark,
                );
                imaging.cell_arcsec = cell_arcsec;
                imaging.weighting = weighting;
                imaging.channel_count = Some(4);
                imaging.spectral_window = Some("0:0~3".into());
                imaging.spectral_mode = SpectralImagingMode::Cube {
                    axis: CubeAxisConfig {
                        outframe: FrequencyRef::TOPO,
                        start: Some(CubeAxisValue::Channel(3)),
                        width: Some(CubeAxisValue::Channel(-1)),
                        ..CubeAxisConfig::default()
                    },
                    output_channels: Some(4),
                };
                imaging.beam_policy = ContinuumBeamPolicy::Common;
                imaging.iterations = 3;
                imaging.cycle_iterations = 1;
                imaging.maximum_major_cycles = Some(3);
                imaging.gain = 0.37;
                imaging.noise_sigma = Some(1.0e-12);
                imaging.primary_beam_limit = pblimit;
                imaging.write_primary_beam = true;
                imaging.pbcor = true;
                imaging.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
                    blc: [3, 4],
                    trc: [10, 12],
                }]);
                imaging.task_requirements = vec![TaskRequirement::SerialCpu];
                imaging.resource_policy =
                    resource_policy_for_task_requirements(&imaging.task_requirements);
                let result = execute_continuum(imaging).expect("signed PB cube");
                assert!(result.outcome.output.major_cycle_count > 1);
                for role in [
                    ProductRole::Residual(ProductTerm::Single),
                    ProductRole::RestoredImage(ProductTerm::Single),
                ] {
                    assert_eq!(
                        result
                            .outcome
                            .output
                            .planned_products
                            .members()
                            .iter()
                            .find(|member| member.role() == role)
                            .expect("uncorrected image product")
                            .validity(),
                        ProductValidityRule::FinalNormalState,
                        "PB cutoff must not zero uncorrected image pixels"
                    );
                }
                let open = |suffix: &str| {
                    PagedImage::<f32>::open(PathBuf::from(format!(
                        "{}{suffix}",
                        image_name.display()
                    )))
                    .unwrap()
                };
                let pb = open(".pb");
                assert_eq!(pb.mask_names(), ["mask0"]);
                assert_eq!(pb.default_mask_name().as_deref(), Some("mask0"));
                let pb_mask = pb
                    .get_mask_slice(&[0; 4], &[16, 16, 1, 4], &[1; 4])
                    .unwrap()
                    .unwrap();
                let pb_pixels = pb.get_slice(&[0; 4], &[16, 16, 1, 4]).unwrap();
                assert!(
                    pb_pixels[[8, 8, 0, 0]] > 0.2,
                    "analytic PB survives a blank channel"
                );
                if cell_arcsec == 1.0 {
                    assert!(pb_mask.iter().all(|valid| *valid));
                } else {
                    assert!(pb_mask.iter().any(|valid| !*valid));
                }
                for suffix in [".residual", ".image", ".image.pbcor"] {
                    let product = open(suffix);
                    let pixels = product.get_slice(&[0; 4], &[16, 16, 1, 4]).unwrap();
                    assert!(
                        product
                            .get_slice(&[0; 4], &[16, 16, 1, 1])
                            .unwrap()
                            .iter()
                            .all(|value| *value == 0.0)
                    );
                    if pblimit > 0.0 || suffix == ".image.pbcor" {
                        assert_eq!(product.mask_names(), ["mask0"]);
                        assert_eq!(product.default_mask_name().as_deref(), Some("mask0"));
                        assert_eq!(
                            product
                                .get_mask_slice(&[0; 4], &[16, 16, 1, 4], &[1; 4])
                                .unwrap()
                                .unwrap(),
                            pb_mask
                        );
                    } else {
                        assert!(
                            product.mask_names().is_empty(),
                            "replacement must remove mask0"
                        );
                        assert_eq!(product.default_mask_name(), None);
                    }
                    if suffix == ".residual" {
                        assert_eq!(product.units(), "");
                        assert!(product.image_info().unwrap().beam_set.is_empty());
                        if cell_arcsec > 1.0 {
                            assert!(
                                pixels
                                    .iter()
                                    .zip(pb_mask.iter())
                                    .any(|(value, valid)| !valid && *value != 0.0),
                                "a stored PB mask must not zero uncorrected standard pixels"
                            );
                        }
                    } else {
                        assert_eq!(product.units(), "Jy/beam");
                    }
                }
                let search = open(".mask");
                assert!(search.mask_names().is_empty());
                let search_pixels = search.get_slice(&[0; 4], &[16, 16, 1, 4]).unwrap();
                for x in 0..16 {
                    for y in 0..16 {
                        for channel in 0..4 {
                            assert_eq!(
                                search_pixels[[x, y, 0, channel]],
                                if (3..=10).contains(&x) && (4..=12).contains(&y) {
                                    1.0
                                } else {
                                    0.0
                                }
                            );
                        }
                    }
                }
                let psf = open(".psf");
                assert_eq!(psf.units(), "");
                assert!(!psf.image_info().unwrap().beam_set.is_empty());
                let pixels = result
                    .product_names
                    .iter()
                    .map(|suffix| {
                        let product = open(suffix);
                        (
                            suffix.clone(),
                            product
                                .get_slice(&[0; 4], product.shape())
                                .unwrap()
                                .iter()
                                .map(|value| value.to_bits())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                let graph = result.outcome.output.planned_products.graph_id();
                if let Some(positive) = &positive_pixels {
                    assert_eq!(
                        positive, &pixels,
                        "pblimit sign must not change numerical products"
                    );
                    assert_ne!(positive_graph, Some(graph));
                } else {
                    positive_pixels = Some(pixels);
                    positive_graph = Some(graph);
                    for suffix in &result.product_names {
                        std::fs::rename(
                            PathBuf::from(format!("{}{suffix}", image_name.display())),
                            PathBuf::from(format!("{}{suffix}", negative_image_name.display())),
                        )
                        .expect("seed positive products under a fresh execution-attempt target");
                    }
                }
            }
        }
    }
}
