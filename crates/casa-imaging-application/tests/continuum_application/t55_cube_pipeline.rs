// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn t55_clark_cube_products_and_repeated_cycles_are_exact_across_worker_counts() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    for weighting in [ContinuumWeighting::Natural, ContinuumWeighting::Briggs(0.5)] {
        let mut baseline = None;
        for workers in [1, 2, 3] {
            let image_name = root.path().join(format!("clark-{weighting:?}-{workers}"));
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
            imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit(
                casa_imaging_runtime::ResourceOverride {
                    workers: Some(workers),
                    ..casa_imaging_runtime::ResourceOverride::default()
                },
            );
            let result = execute_continuum(imaging).expect("bounded production Clark cube");
            assert_standard_products(&image_name, &result.product_names);
            assert!(result.outcome.output.major_cycle_count > 1);
            assert!(
                result.outcome.output.minor_cycles.len() > 1,
                "fixture must cross a synchronized major-cycle boundary"
            );
            assert!(result.actual_minor_iterations > 0);
            let receipt = &result.outcome.output.initial_receipt;
            assert_eq!(
                receipt
                    .selected_alternative_projection()
                    .demand
                    .workers
                    .hard(),
                workers
            );
            let actual_workers = receipt
                .actual_resource_peak(
                    &casa_imaging_runtime::WorkNodeId::new("spectral-cycle-minor-cycle"),
                    &LeaseResource::Workers,
                    &ClaimLifetime::Work,
                )
                .expect("physical minor worker evidence");
            assert!(actual_workers > 0 && actual_workers <= workers);
            if workers > 1 {
                assert!(
                    actual_workers > 1,
                    "parallel cube must execute on multiple physical workers"
                );
            }
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
                    .map(|value| value.to_bits())
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
                science.final_model().samples().to_vec(),
                science.normal_state().residual().to_vec(),
                science.normal_state().sum_weights().to_vec(),
                result.actual_minor_iterations,
                result.outcome.output.major_cycle_count,
            );
            match &baseline {
                Some(baseline) => assert_eq!(
                    baseline, &evidence,
                    "worker count changed scientific products"
                ),
                None => baseline = Some(evidence),
            }
        }
    }
}

#[test]
fn t55_signed_primary_beam_limit_separates_pixels_search_support_and_stored_masks() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
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
