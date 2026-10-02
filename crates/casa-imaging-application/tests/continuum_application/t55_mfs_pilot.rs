// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in 4096-square application smoke on the CASA-generated A+C MFS pilot.
//! The caller provides durable inputs/outputs and the sampled 16-GiB RSS guard.

use super::*;
use casa_imaging_runtime::{CapacityDomainId, ResourceOverride, ResourcePolicy};
use std::{collections::BTreeMap, fs};

#[test]
#[ignore = "requires the external A+C MFS pilot, durable outputs and a 16-GiB RSS guard"]
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
    imaging.cell_arcsec = 0.05;
    imaging.data_description = None;
    imaging.spectral_window = Some(spectral_window.clone());
    imaging.channel_start = None;
    imaging.channel_count = None;
    imaging.weighting = ContinuumWeighting::Uniform;
    imaging.iterations = iterations;
    imaging.cycle_iterations = 1000;
    imaging.maximum_major_cycles = None;
    imaging.gain = 0.1;
    imaging.threshold_jy = 0.005;
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
        "initial_admitted_workers": output.initial_receipt.selected_alternative_projection().demand.workers.hard(),
        "final_major_admitted_workers": output.final_major_receipt.as_ref().map(|receipt| receipt.selected_alternative_projection().demand.workers.hard()),
        "worker_count_scope": "workers is the request; admitted counts are phase reservations, not measured concurrent grid workers; see stream/replay measurements for execution",
        "native_memory_bytes": 16_u64 << 30, "image_size": 4096, "cell_arcsec": 0.05,
        "iterations": result.actual_minor_iterations,
        "majors": output.major_cycle_count, "products": result.product_names,
        "prefix": prefix, "weighting": "uniform", "gridder": gridder,
        "spectral_window": spectral_window,
        "wplanes": (gridder == "wproject").then_some(32),
        "scope": "application smoke; not full-workload performance or sky-model acceptance",
        "timing_boundary": "execute_continuum: input preparation through publication"
    });
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("{summary}");
}
