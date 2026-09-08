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
                if matches!(suffix, ".residual" | ".image") {
                    let validity = product
                        .get_mask_slice(&[0; 4], &[64, 64, 1, 1], &[1; 4])
                        .expect("blank plane validity")
                        .expect("published validity mask");
                    assert!(validity.iter().all(|valid| !*valid));
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
