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
#[ignore = "requires explicit real T55 MS, fresh artifact root, memory ceiling, and production measures"]
fn t55_real_clark_cube_products_are_exact_for_one_two_three_workers() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let measurement_set = required_path("CASA_RS_T55_REAL_MS")
        .canonicalize()
        .expect("existing real MeasurementSet");
    assert!(measurement_set.is_dir());
    assert_eq!(
        measurement_set.file_name().unwrap(),
        "refim_point_withline.ms",
        "this gate is bounded to the named T55 real-data workload"
    );
    let memory_bytes: u64 = std::env::var("CASA_RS_T55_NATIVE_MEMORY_BYTES")
        .expect("CASA_RS_T55_NATIVE_MEMORY_BYTES is required")
        .parse()
        .expect("native memory ceiling must be an integer byte count");
    assert!(memory_bytes > 0, "native memory ceiling must be positive");
    let root = required_path("CASA_RS_T55_ARTIFACT_ROOT");
    fs::create_dir(&root).expect("artifact root must be fresh and its parent must exist");
    let root = root.canonicalize().unwrap();
    set_production_io_environment();
    fs::write(
        root.join("request.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "workload": "tools/perf/imager/workloads/t55-clark-cube-development.json",
            "measurement_set": measurement_set,
            "native_memory_bytes": memory_bytes,
            "workers": [1, 2, 3], "weightings": ["natural", "briggs-0.5"],
            "measures": "production casa_ms::open_measures_runtime",
            "imsize": 128, "cell_arcsec": 8, "field": "0", "spw": "0:8~11",
            "channel_start": 0, "channel_count": 4, "start": 8, "width": 1,
            "outframe": "LSRK", "interpolation": "linear", "gridder": "standard",
            "deconvolver": "clark", "niter": 9, "cycle_iterations": 1,
            "maximum_major_cycles": 3, "gain": 0.1, "threshold_jy": 0,
            "psf_cutoff": casa_imaging_products::DEFAULT_PSF_CUTOFF,
            "pblimit": -0.2, "write_pb": true, "pbcor": false, "wterm": "none",
            "spill_read_bytes_per_second": 1_000_000_000_u64,
            "spill_write_bytes_per_second": 1_000_000_000_u64,
        }))
        .unwrap(),
    )
    .unwrap();

    for (label, weighting) in [
        ("natural", ContinuumWeighting::Natural),
        ("briggs-0.5", ContinuumWeighting::Briggs(0.5)),
    ] {
        let mut baseline: Option<(Vec<ProductSnapshot>, _)> = None;
        for workers in [1, 2, 3] {
            let directory = root.join(format!("{label}-w{workers}"));
            fs::create_dir(&directory).unwrap();
            let image_name = directory.join("image");
            let mut imaging = request(
                measurement_set.clone(),
                image_name.clone(),
                ContinuumAlgorithm::Clark,
            );
            imaging.image_size = 128;
            imaging.cell_arcsec = 8.0;
            imaging.data_description = None;
            imaging.weighting = weighting;
            imaging.spectral_window = Some("0:8~11".into());
            imaging.channel_count = Some(4);
            imaging.spectral_mode = SpectralImagingMode::Cube {
                axis: CubeAxisConfig {
                    outframe: FrequencyRef::LSRK,
                    start: Some(CubeAxisValue::Channel(8)),
                    width: Some(CubeAxisValue::Channel(1)),
                    ..CubeAxisConfig::default()
                },
                output_channels: Some(4),
            };
            imaging.iterations = 9;
            imaging.cycle_iterations = 1;
            imaging.maximum_major_cycles = Some(3);
            imaging.gain = 0.1;
            imaging.threshold_jy = 0.0;
            imaging.psf_cutoff = casa_imaging_products::DEFAULT_PSF_CUTOFF;
            imaging.primary_beam_limit = -0.2;
            imaging.write_primary_beam = true;
            imaging.pbcor = false;
            assert!(imaging.task_requirements.is_empty());
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
            let mut publication_identities = output
                .publication_receipt
                .artifact_identities()
                .into_iter()
                .map(|planned| {
                    let observed = output
                        .publication_receipt
                        .artifact_observed_identity(planned)
                        .expect("observed identity for published product");
                    let hex = |bytes: [u8; 32]| -> String {
                        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
                    };
                    (hex(planned.as_bytes()), hex(observed))
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(publication_identities.len(), REAL_PRODUCTS.len());
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
                            vec![1, 1, 1, 4]
                        } else {
                            vec![128, 128, 1, 4]
                        },
                        "{suffix} cube topology"
                    );
                    for channel in 0..4 {
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
                    // Publication identities bind this run, not just its pixels.
                    let misc = product.misc_info();
                    let Some(Value::Scalar(ScalarValue::String(planned))) =
                        misc.get("casa_rs_planned_product_identity")
                    else {
                        panic!("{suffix}: missing planned publication identity");
                    };
                    let observed = publication_identities
                        .remove(planned)
                        .expect("unique planned identity from the reopened publication receipt");
                    let expected_misc = RecordValue::new(
                        [
                            ("casars_imager_role", suffix[1..].to_owned()),
                            ("casa_rs_planned_product_identity", planned.clone()),
                            ("casa_rs_observed_product_identity", observed),
                        ]
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
            assert!(publication_identities.is_empty());
            let science = &output.scientific;
            let normal = science.normal_state();
            let mut evidence = (
                science.final_model().samples().to_vec(),
                normal.residual().to_vec(),
                normal.normal_approximation().to_vec(),
                normal.sum_weights().to_vec(),
                normal.published_sum_weights().to_vec(),
                normal.channel_sum_weights().to_vec(),
                normal.primary_beam_weighted_sum().map(<[f64]>::to_vec),
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
                            "{label} W{workers}: product {} differs from W1",
                            actual.suffix
                        );
                    }
                    assert!(
                        baseline_evidence == &evidence,
                        "{label} W{workers}: scientific evidence differs from W1"
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
