// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in 4096-square application observations on caller-selected MFS inputs.
//! The caller provides durable inputs/outputs and the sampled 16-GiB RSS guard.

use super::*;
use std::fs;

#[test]
#[ignore = "requires a caller-selected MFS input, durable outputs and a 16-GiB RSS guard"]
fn full_field_application() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    let input = PathBuf::from(std::env::var_os("CASA_RS_MFS_MS").expect("pilot MS"));
    let root = PathBuf::from(std::env::var_os("CASA_RS_MFS_OUTPUT").expect("fresh output"));
    let workers: usize = std::env::var("CASA_RS_MFS_WORKERS")
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
    let spectral_window = std::env::var("CASA_RS_MFS_SPW").unwrap_or_else(|_| "0~31".into());
    let backend = match std::env::var("CASA_RS_MFS_BACKEND").as_deref() {
        Ok("metal") => casa_imaging_application::BackendChoice::Metal,
        Ok("cpu") | Err(_) => casa_imaging_application::BackendChoice::Cpu,
        Ok(other) => panic!("CASA_RS_MFS_BACKEND must be cpu or metal, not {other}"),
    };
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
    imaging.backend = backend;
    if workers == 1 {
        imaging.task_requirements.push(TaskRequirement::SerialCpu);
    }
    imaging.resource_policy = ResourcePolicy::Explicit {
        workers,
        memory: 16 << 30,
    };
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
    assert_eq!(output.workers, workers);
    let image_suffix = if terms == 1 { ".image" } else { ".image.tt0" };
    assert_eq!(
        PagedImage::<f32>::open(root.join(format!("image{image_suffix}")))
            .unwrap()
            .shape(),
        &[4096, 4096, 1, 1]
    );
    let summary = serde_json::json!({
        "seconds": seconds, "workers": workers, "terms": terms,
        "backend": format!("{backend:?}").to_lowercase(),
        "pass_workers": output.workers,
        "worker_count_scope": "workers is the request; pass_workers is the worker team the major-cycle passes ran on",
        "native_memory_bytes": 16_u64 << 30, "image_size": 4096, "cell_arcsec": cell_arcsec,
        "iterations": result.actual_minor_iterations,
        "iteration_limit": iterations, "threshold_jy": threshold_jy,
        "majors": output.major_cycle_count, "products": result.product_names,
        "prefix": prefix, "weighting": "uniform", "gridder": "standard",
        "spectral_window": spectral_window,
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
