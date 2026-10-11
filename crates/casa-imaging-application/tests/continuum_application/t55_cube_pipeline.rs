// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// A cleaned 64 × 64 cube of the four-channel line fixture, its channels
/// in descending order, with a common restoring beam, and `overrides`.
fn clark_cube(
    measurement_set: &Path,
    image_name: &Path,
    overrides: serde_json::Value,
) -> ImagingRequest {
    let mut values = json!({
        "deconvolver": "clark",
        "imsize": 64,
        "spw": "0:0~3",
        "channel_count": 4,
        "specmode": "cube",
        "outframe": "TOPO",
        "restoringbeam": "common",
        "niter": 3,
        "minor_cycle_length": 1,
        "nmajor": 3,
        "gain": 0.37,
        "threshold": "1e-12Jy",
        "nsigma": 1.0e-12,
    });
    values
        .as_object_mut()
        .expect("cube controls")
        .extend(overrides.as_object().expect("overrides").clone());
    request(measurement_set, image_name, values)
}

#[test]
fn streaming_cube_complete_application_handoff() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let image_name = root.path().join("native-cube");
    let mut imaging = clark_cube(&measurement_set, &image_name, json!({}));
    let one_worker = |memory| ResourcePolicy::Explicit { workers: 1, memory };
    let error = casa_imaging_application::execute(&imaging, context_with(one_worker(1 << 20)))
        .err()
        .expect("insufficient memory must fail closed");
    assert!(
        matches!(error, ApplicationDispatchError::Admission(_)),
        "{error}"
    );
    assert!(!image_name.with_extension("image").exists());

    let outcome = casa_imaging_application::execute(&imaging, context_with(one_worker(4 << 30)))
        .expect("complete native cube application");
    assert_standard_products(&image_name, &outcome.product_names());
    assert_eq!(outcome.major_cycle_count, 3);
    assert_eq!(outcome.minor_cycles.len(), 2);
    assert!(outcome.total_actual_minor_iterations > 0);
    for channel in 0..4 {
        let normal = outcome
            .scientific()
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
    drop(outcome);
    imaging.imagename = root.path().join("cube-model-column");
    imaging.savemodel = true;
    let outcome = casa_imaging_application::execute(
        &imaging,
        context_with(ResourcePolicy::Explicit {
            workers: 2,
            memory: 4 << 30,
        }),
    )
    .expect("cube visibility output keeps its owner");
    assert_eq!(outcome.major_cycle_count, 3);
}

#[test]
fn streaming_cube_single_output_runs_clean_refresh_and_publication() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let prefix = root.path().join("single-plane");
    let imaging = request(
        &measurement_set,
        &prefix,
        json!({
            "deconvolver": "clark",
            "imsize": 64,
            "spw": "0:0~3",
            "channel_count": 1,
            "specmode": "cube",
            "outframe": "TOPO",
            "niter": 3,
            "minor_cycle_length": 1,
            "nmajor": 3,
            "gain": 0.37,
            "threshold": "1e-12Jy",
        }),
    );
    let outcome =
        casa_imaging_application::execute(&imaging, context_with(imaging.resource_policy()))
            .expect("one output plane on native cube path");
    assert_standard_products(&prefix, &outcome.product_names());
    assert_eq!(outcome.total_actual_minor_iterations, 3);
    assert_eq!(outcome.major_cycle_count, 4);
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
    let imaging = request(
        &measurement_set,
        &root.path().join("shifted-density"),
        json!({
            "niter": 0,
            "weighting": "briggs",
            "robust": 0.5,
            "spw": "0:0~3",
            "channel_count": 4,
            "specmode": "cube",
            "outframe": "TOPO",
            "start": "43999800000Hz",
            "width": "1MHz",
            "perchanweightdensity": true,
        }),
    );
    let outcome = execute(&imaging).expect("shifted native endpoint density execution");
    // CASA `estimateSwingChanPad`: no frame swing, plus max(min(4, nchan/10), 1).
    let weighting = outcome.problem.weighting();
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
            for weighting in ["briggs", "uniform", "natural"] {
                let imaging = request(
                    &measurement_set,
                    &root
                        .path()
                        .join(format!("density-{cube}-{per_channel}-{weighting}")),
                    json!({
                        "niter": 0,
                        "weighting": weighting,
                        "spw": "0:0~3",
                        "channel_count": 4,
                        "specmode": if cube { "cube" } else { "mfs" },
                        "outframe": "TOPO",
                        "perchanweightdensity": per_channel,
                    }),
                );
                let outcome = execute(&imaging).expect("density scope execution");
                let expected = if weighting == "natural" {
                    WeightDensityScope::NotApplicable
                } else if cube && per_channel {
                    WeightDensityScope::PerOutputChannel
                } else {
                    WeightDensityScope::GlobalSelection
                };
                assert_eq!(
                    outcome.problem.weighting().density_scope(),
                    expected,
                    "cube={cube} per_channel={per_channel} weighting={weighting}",
                );
            }
        }
    }
}

#[test]
fn t55_clark_cube_products_and_repeated_cycles_agree_across_worker_counts() {
    compare_clark_cube_cases(&[(1, None), (2, None), (4, None)], &["natural", "briggs"]);
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
        &["briggs"],
    );
}

fn compare_clark_cube_cases(cases: &[(usize, Option<u64>)], weightings: &[&str]) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    for &weighting in weightings {
        let mut baseline = None;
        for &(workers, memory_bytes) in cases {
            let image_name = root
                .path()
                .join(format!("clark-{weighting}-{workers}-{memory_bytes:?}"));
            let imaging = clark_cube(
                &measurement_set,
                &image_name,
                json!({ "weighting": weighting, "start": "3", "width": "-1" }),
            );
            // A four-thread host, so the policy, not this machine, sets the team.
            let context = RunContext {
                host: HostResources {
                    threads: 4,
                    performance_cores: 4,
                    ..HostResources::detect().expect("host")
                },
                policy: ResourcePolicy::Explicit {
                    workers,
                    memory: memory_bytes.map_or(u64::MAX, |memory| memory + compile_resident()),
                },
                cancel: Cancel::new(),
                summary: None,
            };
            let started = std::time::Instant::now();
            let outcome = casa_imaging_application::execute(&imaging, context)
                .expect("bounded production Clark cube");
            eprintln!(
                "t55_canonical_cube_timing image_size=64 channels=4 weighting={weighting} requested_workers={workers} memory_bytes={memory_bytes:?} execute_seconds={:.9} major_cycles={} minor_iterations={}",
                started.elapsed().as_secs_f64(),
                outcome.major_cycle_count,
                outcome.total_actual_minor_iterations
            );
            assert_standard_products(&image_name, &outcome.product_names());
            assert!(outcome.major_cycle_count > 1);
            assert!(
                outcome.minor_cycles.len() > 1,
                "fixture must cross a synchronized major-cycle boundary"
            );
            assert!(outcome.total_actual_minor_iterations > 0);
            assert_eq!(outcome.workers, workers);
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
            let science = outcome.scientific();
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
                outcome.total_actual_minor_iterations,
                outcome.major_cycle_count,
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
    for weighting in ["natural", "briggs"] {
        for cell_arcsec in [1.0, 120.0] {
            let image_stem = format!("signed-pb-{weighting}-{cell_arcsec}");
            let negative_image_name = root.path().join(format!("{image_stem}-negative"));
            let mut positive_pixels = None;
            let mut positive_storage = None;
            for pblimit in [0.2, -0.2] {
                let image_name = if pblimit > 0.0 {
                    root.path().join(format!("{image_stem}-positive"))
                } else {
                    negative_image_name.clone()
                };
                let imaging = clark_cube(
                    &measurement_set,
                    &image_name,
                    json!({
                        "imsize": 16,
                        "cell": format!("{cell_arcsec}arcsec"),
                        "weighting": weighting,
                        "start": "3",
                        "width": "-1",
                        "threshold": "0Jy",
                        "pblimit": pblimit,
                        "write_pb": true,
                        "pbcor": true,
                        "mask_box": "3,4,10,12",
                    }),
                );
                let outcome = casa_imaging_application::execute(
                    &imaging,
                    context_with(imaging.resource_policy()),
                )
                .expect("signed PB cube");
                assert!(outcome.major_cycle_count > 1);
                for role in [
                    ProductRole::Residual(ProductTerm::Single),
                    ProductRole::RestoredImage(ProductTerm::Single),
                ] {
                    assert_eq!(
                        outcome
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
                let product_names = outcome.product_names();
                let pixels = product_names
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
                let storage = outcome
                    .planned_products
                    .members()
                    .iter()
                    .map(|member| member.storage())
                    .collect::<Vec<_>>();
                if let Some(positive) = &positive_pixels {
                    assert_eq!(
                        positive, &pixels,
                        "pblimit sign must not change numerical products"
                    );
                    assert_ne!(positive_storage, Some(storage));
                } else {
                    positive_pixels = Some(pixels);
                    positive_storage = Some(storage);
                    for suffix in &product_names {
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
