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
    let backend = std::env::var("CASA_RS_MFS_BACKEND").unwrap_or_else(|_| "cpu".into());
    assert!(
        ["cpu", "metal"].contains(&backend.as_str()),
        "CASA_RS_MFS_BACKEND must be cpu or metal, not {backend}"
    );
    assert!([1, 4].contains(&workers));
    assert!((1..=4).contains(&terms));
    fs::create_dir(&root).expect("fresh durable output");
    let prefix = root.join("image");
    let imaging = request(
        &input,
        &prefix,
        json!({
            "deconvolver": if terms == 1 { "clark" } else { "mtmfs" },
            "nterms": terms,
            "imsize": 4096,
            "cell": format!("{cell_arcsec}arcsec"),
            "ddid": null,
            "spw": spectral_window,
            "channel_start": null,
            "channel_count": null,
            "weighting": "uniform",
            "niter": iterations,
            "minor_cycle_length": 1000,
            "nmajor": -1,
            "gain": 0.1,
            "threshold": format!("{threshold_jy}Jy"),
            "psfcutoff": 0.35,
            "pblimit": -0.2,
            "write_pb": true,
            "backend": backend,
        }),
    );
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    eprintln!("MFS pilot application start root={}", root.display());
    let started = std::time::Instant::now();
    let output = casa_imaging_application::execute(
        &imaging,
        context_with(ResourcePolicy::Explicit {
            workers,
            memory: 16 << 30,
        }),
    )
    .unwrap_or_else(|error| {
        fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
        panic!("MFS application failed: {error}");
    });
    let seconds = started.elapsed().as_secs_f64();
    let psf_suffix = if terms == 1 { ".psf" } else { ".psf.tt0" };
    assert_unit_psf_planes(&root.join(format!("image{psf_suffix}")));
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
        "backend": backend,
        "pass_workers": output.workers,
        "worker_count_scope": "workers is the request; pass_workers is the worker team the major-cycle passes ran on",
        "native_memory_bytes": 16_u64 << 30, "image_size": 4096, "cell_arcsec": cell_arcsec,
        "iterations": output.total_actual_minor_iterations,
        "iteration_limit": iterations, "threshold_jy": threshold_jy,
        "majors": output.major_cycle_count, "products": output.product_names(),
        "prefix": prefix, "weighting": "uniform", "gridder": "standard",
        "spectral_window": spectral_window,
        "scope": "application observation; workload identity supplied by caller; not intrinsic-sky or full T55 acceptance",
        "timing_boundary": "execute: input preparation through publication"
    });
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("{summary}");
}
