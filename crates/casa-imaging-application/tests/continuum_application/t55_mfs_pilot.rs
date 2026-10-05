// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in 4096-square application observations on caller-selected MFS inputs.
//! The caller provides durable inputs/outputs and the sampled 16-GiB RSS guard.

use super::*;
use casa_imaging_runtime::{CapacityDomainId, ResourceOverride, ResourcePolicy};
use std::{collections::BTreeMap, fs};

#[test]
#[cfg(target_os = "macos")]
#[ignore = "requires an actual Metal device and the guarded integration qualification"]
fn metal_mfs_clean_refresh_and_publication_matches_cpu() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    set_production_io_environment();
    let root = tempfile::tempdir().unwrap();
    let ms = spectral_line_measurement_set(root.path());
    let mut baseline: Option<Vec<Vec<f32>>> = None;
    for (metal, cache, label) in [
        (false, false, "cpu"),
        (true, true, "metal"),
        (true, false, "metal-streaming"),
    ] {
        let prefix = root.path().join(label);
        let mut imaging = request(ms.clone(), prefix.clone(), ContinuumAlgorithm::Clark);
        imaging.image_size = 64;
        imaging.weighting = ContinuumWeighting::Natural;
        imaging.spectral_window = Some("0:0~3".into());
        imaging.channel_start = None;
        imaging.channel_count = None;
        imaging.iterations = 3;
        imaging.cycle_iterations = 1;
        imaging.maximum_major_cycles = Some(3);
        imaging.gain = 0.37;
        imaging.threshold_jy = 1e-12;
        imaging.noise_sigma = Some(1e-12);
        if metal {
            imaging
                .task_requirements
                .push(TaskRequirement::MetalGridder);
        }
        imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(2),
            cache_bytes: (!cache).then_some(0),
            memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 4 << 30)]),
            ..ResourceOverride::default()
        });
        let result = execute_continuum(imaging).expect("connected MFS normal backend");
        assert_standard_products(&prefix, &result.product_names);
        assert!(result.outcome.output.major_cycle_count >= 2);
        let receipt = result.outcome.output.final_major_receipt.as_ref().unwrap();
        assert_eq!(receipt.status(), ReceiptStatus::Completed);
        assert_eq!(
            !receipt
                .selected_alternative_projection()
                .demand
                .accelerators
                .is_empty(),
            metal
        );
        let products = PRODUCT_SUFFIXES
            .iter()
            .map(|suffix| {
                let image =
                    PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))
                        .unwrap();
                image
                    .get_slice(&[0; 4], image.shape())
                    .unwrap()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if let Some(expected) = &baseline {
            for (actual, expected) in products.iter().zip(expected) {
                assert_real_agreement(expected, actual);
            }
        } else {
            baseline = Some(products);
        }
    }
}

#[test]
#[ignore = "requires a caller-selected MFS input, durable outputs and a 16-GiB RSS guard"]
fn full_field_application() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    let input = PathBuf::from(std::env::var_os("CASA_RS_MFS_MS").expect("pilot MS"));
    let root = PathBuf::from(std::env::var_os("CASA_RS_MFS_OUTPUT").expect("fresh output"));
    let workers: u64 = std::env::var("CASA_RS_MFS_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    let terms: usize = std::env::var("CASA_RS_MFS_TERMS").unwrap().parse().unwrap();
    let iterations: usize = std::env::var("CASA_RS_MFS_NITER").unwrap().parse().unwrap();
    let threshold_jy: f64 = std::env::var("CASA_RS_MFS_THRESHOLD_JY")
        .unwrap_or_else(|_| "0.005".into())
        .parse()
        .unwrap();
    assert!(threshold_jy.is_finite() && threshold_jy > 0.0);
    let cell_arcsec: f64 = std::env::var("CASA_RS_MFS_CELL_ARCSEC")
        .unwrap_or_else(|_| "0.05".into())
        .parse()
        .unwrap();
    assert!(cell_arcsec.is_finite() && cell_arcsec > 0.0);
    let gridder = std::env::var("CASA_RS_MFS_GRIDDER").unwrap_or_else(|_| "standard".into());
    let spectral_window = std::env::var("CASA_RS_MFS_SPW").unwrap_or_else(|_| "0~31".into());
    assert!(["standard", "wproject"].contains(&gridder.as_str()));
    assert!([1, 4].contains(&workers));
    assert!((1..=4).contains(&terms));
    fs::create_dir(&root).expect("fresh durable output");
    let prefix = root.join("image");
    let algorithm = if terms == 1 {
        ContinuumAlgorithm::Clark
    } else {
        ContinuumAlgorithm::Mtmfs {
            terms,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        }
    };
    let mut imaging = request(input, prefix.clone(), algorithm);
    imaging.image_size = 4096;
    imaging.cell_arcsec = cell_arcsec;
    imaging.data_description = None;
    imaging.spectral_window = Some(spectral_window.clone());
    imaging.channel_start = None;
    imaging.channel_count = None;
    imaging.weighting = ContinuumWeighting::Uniform;
    imaging.iterations = iterations;
    imaging.cycle_iterations = 1000;
    imaging.maximum_major_cycles = None;
    imaging.gain = 0.1;
    imaging.threshold_jy = threshold_jy;
    imaging.psf_cutoff = 0.35;
    imaging.primary_beam_limit = -0.2;
    imaging.normalization = casa_imaging_model::ProductNormalization::FlatNoise;
    imaging.write_primary_beam = true;
    imaging.w_projection_planes = (gridder == "wproject").then_some(32);
    imaging.task_requirements = if gridder == "wproject" {
        vec![TaskRequirement::WProjection]
    } else {
        vec![]
    };
    if workers == 1 {
        imaging.task_requirements.push(TaskRequirement::SerialCpu);
    }
    let metal = std::env::var_os("CASA_RS_MFS_METAL").is_some();
    if metal {
        imaging
            .task_requirements
            .push(TaskRequirement::MetalGridder);
    }
    imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(workers),
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 16 << 30)]),
        ..ResourceOverride::default()
    });
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    eprintln!("MFS pilot application start root={}", root.display());
    let started = std::time::Instant::now();
    let result = execute_continuum(imaging).unwrap_or_else(|error| {
        fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
        panic!("MFS application failed: {error}");
    });
    let seconds = started.elapsed().as_secs_f64();
    let psf_suffix = if terms == 1 { ".psf" } else { ".psf.tt0" };
    assert_unit_psf_planes(&root.join(format!("image{psf_suffix}")));
    let output = &result.outcome.output;
    assert_eq!(output.initial_receipt.status(), ReceiptStatus::Completed);
    assert_eq!(
        output.publication_receipt.status(),
        ReceiptStatus::Completed
    );
    assert_eq!(
        output.initial_receipt.initial_execution_knobs().workers,
        workers
    );
    let image_suffix = if terms == 1 { ".image" } else { ".image.tt0" };
    assert_eq!(
        PagedImage::<f32>::open(root.join(format!("image{image_suffix}")))
            .unwrap()
            .shape(),
        &[4096, 4096, 1, 1]
    );
    let summary = serde_json::json!({
        "seconds": seconds, "workers": workers, "terms": terms,
        "residual_backend": if metal { "metal_normal" } else { "cpu" },
        "initial_admitted_workers": output.initial_receipt.selected_alternative_projection().demand.workers.hard(),
        "final_major_admitted_workers": output.final_major_receipt.as_ref().map(|receipt| receipt.selected_alternative_projection().demand.workers.hard()),
        "worker_count_scope": "workers is the request; admitted counts are phase reservations, not measured concurrent grid workers; see stream/replay measurements for execution",
        "native_memory_bytes": 16_u64 << 30, "image_size": 4096, "cell_arcsec": cell_arcsec,
        "iterations": result.actual_minor_iterations,
        "iteration_limit": iterations, "threshold_jy": threshold_jy,
        "majors": output.major_cycle_count, "products": result.product_names,
        "prefix": prefix, "weighting": "uniform", "gridder": gridder,
        "spectral_window": spectral_window,
        "wplanes": (gridder == "wproject").then_some(32),
        "scope": "application observation; workload identity supplied by caller; not intrinsic-sky or full T55 acceptance",
        "timing_boundary": "execute_continuum: input preparation through publication"
    });
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("{summary}");
}
