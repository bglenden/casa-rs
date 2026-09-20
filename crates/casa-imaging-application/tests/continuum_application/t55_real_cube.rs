// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in production-measures counterpart of the bounded T55 cube workload.
//! Requires CASA_RS_T55_REAL_MS, CASA_RS_T55_ARTIFACT_ROOT (a fresh directory),
//! and CASA_RS_T55_NATIVE_MEMORY_BYTES. Products and receipts are never removed.
//! Initialize the imaging-owner manifest explicitly on an isolated input copy
//! with casa-ms's `initialize_imaging_owner` example before invoking this test.

use super::*;
use casa_imaging_runtime::{
    CapacityDomainId, ExecutionReceiptStore, ReceiptRetention, ResourceOverride, ResourcePolicy,
    WorkNodeId,
};
use std::{collections::BTreeMap, fs};

const REAL_PRODUCTS: [&str; 7] = [
    ".psf",
    ".residual",
    ".model",
    ".image",
    ".sumwt",
    ".mask",
    ".pb",
];

#[derive(Debug, PartialEq)]
struct ProductSnapshot {
    suffix: &'static str,
    shape: Vec<usize>,
    pixels: Vec<u32>,
    masks: Vec<(String, Vec<bool>)>,
    default_mask: Option<String>,
    coordinates: RecordValue,
    units: String,
    image_info: casa_images::ImageInfo,
}

fn required_path(variable: &str) -> PathBuf {
    let value = std::env::var_os(variable).unwrap_or_else(|| panic!("{variable} is required"));
    assert!(!value.is_empty(), "{variable} must not be empty");
    PathBuf::from(value)
}

#[test]
#[ignore = "Q-band diagnostic only: requires owner-initialized reduced-row/512-channel fixture, fresh durable artifacts and external 8GiB guard"]
fn t55_q_band_rebaseline_preflight() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let image_size: usize = std::env::var("CASA_RS_T55_PREFLIGHT_IMAGE_SIZE")
        .map(|value| value.parse().expect("positive diagnostic image size"))
        .unwrap_or(64);
    assert!([64, 128, 512].contains(&image_size));
    let expected_rows: usize = std::env::var("CASA_RS_T55_PREFLIGHT_ROWS")
        .map(|value| value.parse().expect("positive diagnostic row count"))
        .unwrap_or(351);
    assert!(expected_rows > 0 && expected_rows <= 42_120 && expected_rows % 351 == 0);
    let workers: u64 = std::env::var("CASA_RS_T55_PREFLIGHT_WORKERS")
        .map(|value| value.parse().expect("positive diagnostic worker count"))
        .unwrap_or(1);
    assert!([1, 2, 4].contains(&workers));
    let measurement_set = required_path("CASA_RS_T55_REAL_MS");
    let ms = MeasurementSet::open(&measurement_set).expect("preflight MS");
    assert_eq!(
        ms.row_count(),
        expected_rows,
        "explicit reduced-row fixture"
    );
    drop(ms);
    let root = required_path("CASA_RS_T55_ARTIFACT_ROOT");
    fs::create_dir(&root).expect("fresh retained artifact directory");
    let image_name = root.join("image");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Clark,
    );
    imaging.image_size = image_size;
    imaging.cell_arcsec = 0.35;
    imaging.data_description = None;
    imaging.spectral_window = Some("0".into());
    imaging.channel_count = Some(512);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::LSRK,
            start: Some(CubeAxisValue::Channel(0)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(512),
    };
    imaging.iterations = 9;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.1;
    imaging.psf_cutoff = casa_imaging_products::DEFAULT_PSF_CUTOFF;
    imaging.primary_beam_limit = -0.2;
    imaging.write_primary_beam = true;
    imaging.task_requirements = vec![TaskRequirement::PerChannelWeightDensity];
    imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(workers),
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 4 << 30)]),
        ..ResourceOverride::default()
    });
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    let started = std::time::Instant::now();
    let result = execute_continuum(imaging).unwrap_or_else(|error| {
        fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
        panic!("Q-band preflight failed: {error}");
    });
    let task_wall_seconds = started.elapsed().as_secs_f64();
    let worker_evidence = [
        ("initial-major", &result.outcome.output.initial_receipt),
        (
            "final-major",
            result
                .outcome
                .output
                .final_major_receipt
                .as_ref()
                .expect("final-major receipt"),
        ),
    ]
    .into_iter()
    .map(|(phase, receipt)| {
        assert_eq!(receipt.initial_execution_knobs().workers, workers);
        let peaks = receipt
            .plan_node_identities()
            .into_iter()
            .map(|node| {
                let peak = receipt.actual_resource_peak(
                    &node,
                    &LeaseResource::Workers,
                    &ClaimLifetime::Work,
                );
                (node.as_str().to_owned(), peak)
            })
            .collect::<BTreeMap<_, _>>();
        (
            phase,
            serde_json::json!({"admitted_workers": workers, "worker_peaks": peaks}),
        )
    })
    .collect::<BTreeMap<_, _>>();
    assert_products(&image_name, &result.product_names, &REAL_PRODUCTS);
    let pb = PagedImage::<f32>::open(root.join("image.pb")).expect("published PB");
    assert_eq!(pb.shape(), &[image_size, image_size, 1, 512]);
    let publication = &result.outcome.output.publication_receipt;
    assert_eq!(publication.status(), ReceiptStatus::Completed);
    let publication_stage_nanos = ["product-generation-write", "product-publication-commit"]
        .into_iter()
        .map(|name| {
            (
                name,
                publication
                    .stage_actual_elapsed_nanos(&WorkNodeId::new(name))
                    .expect("completed publication stage timing"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let publication_seconds = publication_stage_nanos.values().sum::<u64>() as f64 / 1.0e9;
    let fingerprints = std::env::var_os("CASA_RS_T55_PUBLICATION_PROBE")
        .map(|_| publication_probe_fingerprints(&root));
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "scope": "diagnostic only: reduced rows, all 512 Q-band channels, CPU Clark cube",
            "rows": expected_rows,
            "requested_workers": workers,
            "worker_evidence": worker_evidence,
            "image_size": image_size,
            "task_wall_seconds": task_wall_seconds,
            "publication_seconds": publication_seconds,
            "publication_stage_nanos": publication_stage_nanos,
            "product_fingerprints": fingerprints,
            "major_cycles": result.outcome.output.major_cycle_count,
            "minor_iterations": result.minor_iterations,
            "actual_minor_iterations": result.actual_minor_iterations,
            "products": result.product_names,
        }))
        .unwrap(),
    )
    .unwrap();
}

fn publication_probe_fingerprints(root: &std::path::Path) -> BTreeMap<&'static str, String> {
    use sha2::{Digest, Sha256};

    REAL_PRODUCTS
        .into_iter()
        .map(|suffix| {
            let image = PagedImage::<f32>::open(root.join(format!("image{suffix}"))).unwrap();
            let shape = image.shape().to_vec();
            let mut digest = Sha256::new();
            digest.update(format!(
                "{:?}|{:?}|{:?}|{:?}|{:?}",
                shape,
                image.coordinates().to_record(),
                image.units(),
                image.image_info().unwrap(),
                image.default_mask_name(),
            ));
            for channel in 0..shape[3] {
                let pixels = image
                    .get_slice(&[0, 0, 0, channel], &[shape[0], shape[1], shape[2], 1])
                    .unwrap();
                for value in &pixels {
                    assert!(value.is_finite(), "nonfinite {suffix}");
                    digest.update(value.to_bits().to_le_bytes());
                }
            }
            let mut names = image.mask_names();
            names.sort();
            for name in names {
                digest.update((name.len() as u64).to_le_bytes());
                digest.update(name.as_bytes());
                let mask = image.get_named_mask(&name).unwrap();
                assert_eq!(mask.shape(), shape);
                for value in &mask {
                    digest.update([u8::from(*value)]);
                }
            }
            (suffix, format!("{:x}", digest.finalize()))
        })
        .collect()
}

#[test]
#[ignore = "requires isolated complete 32 GiB VLA input, explicit resources, fresh artifacts, and an external wall/RSS guard"]
fn t55_full_dataset_clark_timing() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let measurement_set = required_path("CASA_RS_T55_REAL_MS")
        .canonicalize()
        .expect("existing full MeasurementSet");
    assert_eq!(
        measurement_set.file_name().unwrap(),
        "wave1-vla-single-medium.ms"
    );
    let ms = MeasurementSet::open(&measurement_set).unwrap();
    assert_eq!(ms.row_count(), 4_094_064, "all benchmark rows are required");
    drop(ms);
    let workers: u64 = std::env::var("CASA_RS_T55_FULL_WORKERS")
        .expect("explicit worker count")
        .parse()
        .unwrap();
    assert!([1, 4, 10].contains(&workers));
    let memory_bytes: u64 = std::env::var("CASA_RS_T55_NATIVE_MEMORY_BYTES")
        .expect("explicit native memory limit")
        .parse()
        .unwrap();
    assert!(memory_bytes > 0 && memory_bytes <= 24 << 30);
    let root = required_path("CASA_RS_T55_ARTIFACT_ROOT");
    fs::create_dir(&root).expect("fresh retained artifact root");
    let image_name = root.join("image");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Clark,
    );
    imaging.image_size = 512;
    imaging.cell_arcsec = 0.35;
    imaging.data_description = None;
    imaging.spectral_window = Some("0".into());
    imaging.channel_count = Some(512);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::LSRK,
            start: Some(CubeAxisValue::Channel(0)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(512),
    };
    imaging.iterations = 9;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.1;
    imaging.threshold_jy = 0.0;
    imaging.psf_cutoff = casa_imaging_products::DEFAULT_PSF_CUTOFF;
    imaging.primary_beam_limit = -0.2;
    imaging.write_primary_beam = true;
    imaging.task_requirements = vec![TaskRequirement::PerChannelWeightDensity];
    imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(workers),
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), memory_bytes)]),
        ..ResourceOverride::default()
    });
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    eprintln!(
        "T55 full-data timing start workers={workers} root={}",
        root.display()
    );
    let started = std::time::Instant::now();
    let result = match execute_continuum(imaging) {
        Ok(result) => result,
        Err(error) => {
            fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
            panic!("full-data timing failed: {error}");
        }
    };
    let task_wall_seconds = started.elapsed().as_secs_f64();
    let output = &result.outcome.output;
    let final_receipt = output.final_major_receipt.as_ref().expect("final major");
    let receipts = [
        &output.initial_receipt,
        final_receipt,
        &output.publication_receipt,
    ];
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.status() == ReceiptStatus::Completed)
    );
    assert!(output.major_cycle_count > 1 && result.actual_minor_iterations > 0);
    assert_products(&image_name, &result.product_names, &REAL_PRODUCTS);
    let minor_workers = output
        .initial_receipt
        .actual_resource_peak(
            &WorkNodeId::new("spectral-cycle-minor-cycle"),
            &LeaseResource::Workers,
            &ClaimLifetime::Work,
        )
        .expect("executed minor worker evidence");
    assert!(minor_workers > 0 && minor_workers <= workers);
    fs::write(root.join("summary.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "scope": "full input, Natural, linear LSRK Clark cube; numerical CASA comparison required separately",
        "selected_rows": 4_094_064, "source_channels": 512, "output_channels": 512,
        "image_size": 512, "requested_workers": workers, "actual_minor_workers": minor_workers,
        "task_wall_seconds": task_wall_seconds, "native_memory_bytes": memory_bytes,
        "major_cycles": output.major_cycle_count, "minor_iterations": result.minor_iterations,
        "actual_minor_iterations": result.actual_minor_iterations, "products": result.product_names,
        "timing_boundary": "execute_continuum: selection and preparation through final publication, excluding staging and post-run comparison",
        "receipt_ids": receipts.iter().map(|receipt| receipt.attempt_id().to_string()).collect::<Vec<_>>(),
    })).unwrap()).unwrap();
}

#[test]
#[ignore = "requires explicit real T55 MS, fresh artifact root, memory ceiling, and production measures"]
fn t55_real_clark_cube_products_are_exact_for_one_two_three_workers() {
    real_clark_worker_cases(
        "tools/perf/imager/workloads/t55-clark-cube-development.json",
        "refim_point_withline.ms",
        128,
        8,
        4,
        9,
        &[
            ("natural", ContinuumWeighting::Natural),
            ("briggs-0.5", ContinuumWeighting::Briggs(0.5)),
        ],
    );
}

#[test]
#[ignore = "requires local real T55 MS, explicit resources, fresh artifacts, and external wall/RSS guard"]
fn t55_intermediate_clark_cube_worker_scaling() {
    real_clark_worker_cases(
        "t55-intermediate-256-square-16-channel-natural-clark",
        "refim_point_withline.ms",
        256,
        2,
        16,
        9,
        &[("natural", ContinuumWeighting::Natural)],
    );
}

#[test]
#[ignore = "diagnostic synthetic scaling only; requires explicit fixture, resources and external guard"]
fn t55_synthetic_clark_cube_worker_scaling() {
    let channels = std::env::var("CASA_RS_T55_SYNTHETIC_CHANNELS")
        .expect("explicit synthetic channel count")
        .parse::<usize>()
        .expect("integer synthetic channel count");
    assert!(matches!(channels, 16 | 64));
    real_clark_worker_cases(
        "t55-exploratory-synthetic-channel-scaling",
        "scaling-cube.ms",
        256,
        2,
        channels,
        64,
        &[("natural", ContinuumWeighting::Natural)],
    );
}

fn real_clark_worker_cases(
    workload: &str,
    expected_filename: &str,
    image_size: usize,
    first_channel: usize,
    channels: usize,
    iterations: usize,
    weightings: &[(&str, ContinuumWeighting)],
) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let measurement_set = required_path("CASA_RS_T55_REAL_MS")
        .canonicalize()
        .expect("existing real MeasurementSet");
    assert!(measurement_set.is_dir());
    assert_eq!(
        measurement_set.file_name().unwrap(),
        expected_filename,
        "this gate is bounded to its named T55 workload"
    );
    let memory_bytes: u64 = std::env::var("CASA_RS_T55_NATIVE_MEMORY_BYTES")
        .expect("CASA_RS_T55_NATIVE_MEMORY_BYTES is required")
        .parse()
        .expect("native memory ceiling must be an integer byte count");
    assert!(memory_bytes > 0, "native memory ceiling must be positive");
    let worker_counts = std::env::var("CASA_RS_T55_TIMING_WORKERS")
        .map(|value| {
            value
                .split(',')
                .map(|worker| worker.parse::<u64>().expect("positive worker count"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|_| vec![1, 2, 3]);
    assert!(!worker_counts.is_empty() && worker_counts.iter().all(|workers| *workers > 0));
    let repetitions = std::env::var("CASA_RS_T55_TIMING_REPETITIONS")
        .map(|value| value.parse::<usize>().expect("positive repetition count"))
        .unwrap_or(1);
    assert!(repetitions > 0);
    let root = required_path("CASA_RS_T55_ARTIFACT_ROOT");
    fs::create_dir(&root).expect("artifact root must be fresh and its parent must exist");
    let root = root.canonicalize().unwrap();
    set_production_io_environment();
    fs::write(
        root.join("request.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "workload": workload,
            "measurement_set": measurement_set,
            "native_memory_bytes": memory_bytes,
            "workers": worker_counts, "repetitions": repetitions,
            "weightings": weightings.iter().map(|(label, _)| *label).collect::<Vec<_>>(),
            "timing_boundary": "execute_continuum: selection and preparation through final product publication; excludes fixture staging and post-run comparison",
            "measures": "production casa_ms::open_measures_runtime",
            "imsize": image_size, "cell_arcsec": 8, "field": "0",
            "spw": format!("0:{first_channel}~{}", first_channel + channels - 1),
            "channel_start": 0, "channel_count": channels, "start": first_channel, "width": 1,
            "outframe": "LSRK", "interpolation": "linear", "gridder": "standard",
            "perchanweightdensity": true,
            "deconvolver": "clark", "niter": iterations, "cycle_iterations": 1,
            "maximum_major_cycles": 3, "gain": 0.1, "threshold_jy": 0,
            "psf_cutoff": casa_imaging_products::DEFAULT_PSF_CUTOFF,
            "pblimit": -0.2, "write_pb": true, "pbcor": false, "wterm": "none",
            "spill_read_bytes_per_second": 1_000_000_000_u64,
            "spill_write_bytes_per_second": 1_000_000_000_u64,
        }))
        .unwrap(),
    )
    .unwrap();

    for repetition in 0..repetitions {
        let root = if repetitions == 1 {
            root.clone()
        } else {
            let directory = root.join(format!("round-{repetition}"));
            fs::create_dir(&directory).unwrap();
            directory
        };
        let mut round_workers = worker_counts.clone();
        if repetition % 2 == 1 {
            round_workers.reverse();
        }
        for &(label, weighting) in weightings {
            let mut baseline: Option<(Vec<ProductSnapshot>, _)> = None;
            for workers in round_workers.iter().copied() {
                let directory = root.join(format!("{label}-w{workers}"));
                fs::create_dir(&directory).unwrap();
                let image_name = directory.join("image");
                let mut imaging = request(
                    measurement_set.clone(),
                    image_name.clone(),
                    ContinuumAlgorithm::Clark,
                );
                imaging.image_size = image_size;
                imaging.cell_arcsec = 8.0;
                imaging.data_description = None;
                imaging.weighting = weighting;
                imaging.spectral_window = Some(format!(
                    "0:{first_channel}~{}",
                    first_channel + channels - 1
                ));
                imaging.channel_count = Some(channels);
                imaging.spectral_mode = SpectralImagingMode::Cube {
                    axis: CubeAxisConfig {
                        outframe: FrequencyRef::LSRK,
                        start: Some(CubeAxisValue::Channel(
                            i32::try_from(first_channel).unwrap(),
                        )),
                        width: Some(CubeAxisValue::Channel(1)),
                        ..CubeAxisConfig::default()
                    },
                    output_channels: Some(channels),
                };
                imaging.iterations = iterations;
                imaging.cycle_iterations = 1;
                imaging.maximum_major_cycles = Some(3);
                imaging.gain = 0.1;
                imaging.threshold_jy = 0.0;
                imaging.psf_cutoff = casa_imaging_products::DEFAULT_PSF_CUTOFF;
                imaging.primary_beam_limit = -0.2;
                imaging.write_primary_beam = true;
                imaging.pbcor = false;
                imaging.task_requirements =
                    vec![casa_imaging_application::TaskRequirement::PerChannelWeightDensity];
                imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
                    workers: Some(workers),
                    memory_bytes: BTreeMap::from([(
                        CapacityDomainId::new("host-memory"),
                        memory_bytes,
                    )]),
                    ..ResourceOverride::default()
                });
                eprintln!(
                    "T55 real cube start: {label} workers={workers} artifacts={}",
                    directory.display()
                );
                let task_started = std::time::Instant::now();
                let result = match execute_continuum(imaging) {
                    Ok(result) => result,
                    Err(error) => {
                        fs::write(directory.join("failure.txt"), format!("{error:#?}\n")).unwrap();
                        panic!(
                            "{label} W{workers} failed; retained artifacts at {}: {error}",
                            directory.display()
                        );
                    }
                };
                let task_wall_seconds = task_started.elapsed().as_secs_f64();
                let output = &result.outcome.output;
                let receipts = ExecutionReceiptStore::new(
                    directory.join(".casa-rs-imaging-receipts"),
                    ReceiptRetention::new(512, 256 << 20).unwrap(),
                )
                .unwrap();
                let final_receipt = output
                    .final_major_receipt
                    .as_ref()
                    .expect("final-major receipt");
                let mut receipt_summary = Vec::new();
                for (phase, receipt) in [
                    ("initial", &output.initial_receipt),
                    ("final-major", final_receipt),
                    ("publication", &output.publication_receipt),
                ] {
                    assert_eq!(receipts.open(receipt.attempt_id()).unwrap(), *receipt);
                    assert_eq!(receipt.status(), ReceiptStatus::Completed);
                    assert_eq!(
                        receipt
                            .compiled_problem_evidence()
                            .field("weighting.density_scope"),
                        Some(if weighting == ContinuumWeighting::Natural {
                            "not_applicable"
                        } else {
                            "per_output_channel"
                        }),
                        "executed weighting scope must match the CASA workload",
                    );
                    assert_eq!(
                        receipt
                            .compiled_problem_evidence()
                            .field("weighting.casa_cube_density_padding"),
                        if weighting == ContinuumWeighting::Natural {
                            None
                        } else {
                            Some("1")
                        },
                        "the single-field LSRK cube binds CASA nominal density padding"
                    );
                    let peaks = receipt
                        .plan_node_identities()
                        .into_iter()
                        .map(|node| {
                            let peak = receipt.actual_resource_peak(
                                &node,
                                &LeaseResource::Workers,
                                &ClaimLifetime::Work,
                            );
                            (node.as_str().to_owned(), peak)
                        })
                        .collect::<BTreeMap<_, _>>();
                    receipt_summary.push(serde_json::json!({
                    "phase": phase, "attempt_id": receipt.attempt_id().to_string(),
                    "path": receipts.root_path().join(format!("{}.receipt.json", receipt.attempt_id())),
                    "selected_workers": receipt.initial_execution_knobs().workers,
                    "worker_peaks": peaks,
                }));
                }
                let actual_workers = output
                    .initial_receipt
                    .actual_resource_peak(
                        &WorkNodeId::new("spectral-cycle-minor-cycle"),
                        &LeaseResource::Workers,
                        &ClaimLifetime::Work,
                    )
                    .expect("physical minor worker evidence");
                fs::write(directory.join("summary.json"), serde_json::to_vec_pretty(&serde_json::json!({
                "weighting": label, "requested_workers": workers, "actual_minor_workers": actual_workers,
                "task_wall_seconds": task_wall_seconds, "repetition": repetition,
                "native_memory_bytes": memory_bytes, "major_cycles": output.major_cycle_count,
                "minor_cycles": output.minor_cycles.len(), "minor_iterations": result.minor_iterations,
                "actual_minor_iterations": result.actual_minor_iterations,
                "products": result.product_names, "receipts": receipt_summary,
            })).unwrap()).unwrap();
                for receipt in [&output.initial_receipt, final_receipt] {
                    assert_eq!(receipt.initial_execution_knobs().workers, workers);
                    assert_eq!(
                        receipt
                            .selected_alternative_projection()
                            .demand
                            .workers
                            .hard(),
                        workers
                    );
                }
                assert!(actual_workers > 0 && actual_workers <= workers);
                assert!(
                    workers == 1 || actual_workers > 1,
                    "parallel execution must be real"
                );
                assert!(output.major_cycle_count > 1 && output.minor_cycles.len() > 1);
                assert!(result.actual_minor_iterations > 0);
                assert_products(&image_name, &result.product_names, &REAL_PRODUCTS);
                let actual_products = fs::read_dir(&directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                    .filter_map(|name| {
                        name.strip_prefix("image.")
                            .map(|suffix| format!(".{suffix}"))
                    })
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(
                    actual_products,
                    REAL_PRODUCTS.into_iter().map(str::to_owned).collect()
                );
                assert_eq!(output.products.members().len(), REAL_PRODUCTS.len());
                let products = REAL_PRODUCTS
                    .into_iter()
                    .map(|suffix| {
                        let product = PagedImage::<f32>::open(PathBuf::from(format!(
                            "{}{suffix}",
                            image_name.display()
                        )))
                        .unwrap();
                        let shape = product.shape().to_vec();
                        assert_eq!(
                            shape,
                            if suffix == ".sumwt" {
                                vec![1, 1, 1, channels]
                            } else {
                                vec![image_size, image_size, 1, channels]
                            },
                            "{suffix} cube topology"
                        );
                        for channel in 0..channels {
                            for x in [0, shape[0] - 1] {
                                for y in [0, shape[1] - 1] {
                                    let world = product
                                        .coordinates()
                                        .to_world(&[x as f64, y as f64, 0.0, channel as f64])
                                        .expect("complete channel/corner WCS must be supported");
                                    assert!(world.iter().all(|value| value.is_finite()));
                                }
                            }
                        }
                        let pixels = product.get_slice(&[0; 4], &shape).unwrap();
                        assert!(
                            pixels.iter().all(|value| value.is_finite()),
                            "nonfinite {suffix}"
                        );
                        let masks = product
                            .mask_names()
                            .into_iter()
                            .map(|name| {
                                let mask = product
                                    .get_named_mask(&name)
                                    .expect("read every named mask");
                                assert_eq!(mask.shape(), shape);
                                (name, mask.iter().copied().collect())
                            })
                            .collect();
                        let misc = product.misc_info();
                        let expected_misc = RecordValue::new(
                            [("casars_imager_role", suffix[1..].to_owned())]
                                .into_iter()
                                .map(|(name, value)| RecordField::new(name, string(&value)))
                                .collect(),
                        );
                        assert_eq!(misc, expected_misc, "{suffix} publication metadata");
                        ProductSnapshot {
                            suffix,
                            shape,
                            pixels: pixels.iter().map(|value| value.to_bits()).collect(),
                            masks,
                            default_mask: product.default_mask_name(),
                            coordinates: product.coordinates().to_record(),
                            units: product.units().to_owned(),
                            image_info: product.image_info().expect("complete beam/image metadata"),
                        }
                    })
                    .collect::<Vec<_>>();
                let science = &output.scientific;
                let normal = science.normal_state();
                let windows = (0..normal.sum_weights().len())
                    .map(|channel| {
                        normal
                            .read_window(channel..channel + 1)
                            .expect("fixture normal window")
                    })
                    .collect::<Vec<_>>();
                let mut evidence = (
                    fixture_model_samples(science.final_model()),
                    windows
                        .iter()
                        .flat_map(|window| window.residual().iter().copied())
                        .collect::<Vec<_>>(),
                    windows
                        .iter()
                        .flat_map(|window| window.normal_approximation().iter().copied())
                        .collect::<Vec<_>>(),
                    normal.sum_weights().to_vec(),
                    normal.published_sum_weights().to_vec(),
                    normal.channel_sum_weights().to_vec(),
                    windows
                        .iter()
                        .map(|window| window.primary_beam_weighted_sum().map(<[f64]>::to_vec))
                        .collect::<Option<Vec<_>>>()
                        .map(|planes| planes.into_iter().flatten().collect::<Vec<_>>()),
                    output.minor_cycles.clone(),
                    output.major_cycle_count,
                    result.minor_iterations,
                    result.actual_minor_iterations,
                );
                match &baseline {
                    None => baseline = Some((products, evidence)),
                    Some((baseline_products, baseline_evidence)) => {
                        // Affine mask/model generation IDs are local to each execution.
                        // Keep every scientific field, component and support bit exact.
                        assert_eq!(baseline_evidence.7.len(), evidence.7.len());
                        for (expected, actual) in baseline_evidence.7.iter().zip(&mut evidence.7) {
                            actual.mask_generation = expected.mask_generation;
                            actual.mask_model_generation = expected.mask_model_generation;
                        }
                        for (expected, actual) in baseline_products.iter().zip(&products) {
                            assert!(
                                expected == actual,
                                "{label} W{workers}: product {} differs from the first worker case",
                                actual.suffix
                            );
                        }
                        assert!(
                            baseline_evidence == &evidence,
                            "{label} W{workers}: scientific evidence differs from the first worker case"
                        );
                    }
                }
                fs::write(
                    directory.join("accepted.txt"),
                    "Exact worker/product checks passed.\n",
                )
                .unwrap();
            }
        }
    }
}
