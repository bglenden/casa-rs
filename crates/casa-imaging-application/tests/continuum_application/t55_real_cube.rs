// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in production-measures counterpart of the bounded T55 cube workload.
//! Requires CASA_RS_T55_REAL_MS, CASA_RS_T55_ARTIFACT_ROOT (a fresh directory),
//! and CASA_RS_T55_NATIVE_MEMORY_BYTES. Products and receipts are never removed.
//! Point CASA_RS_T55_REAL_MS at an isolated input copy: the run may write
//! MODEL_DATA into it.

use super::*;
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

#[derive(Debug)]
struct ProductSnapshot {
    suffix: &'static str,
    shape: Vec<usize>,
    pixels: Vec<f32>,
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
#[ignore = "Q-band diagnostic only: requires a reduced-row/512-channel fixture, fresh durable artifacts and an external RSS guard"]
fn t55_q_band_rebaseline_preflight() {
    let image_size: usize = std::env::var("CASA_RS_T55_PREFLIGHT_IMAGE_SIZE")
        .map(|value| value.parse().expect("positive diagnostic image size"))
        .unwrap_or(64);
    assert!([64, 128, 512].contains(&image_size));
    let expected_rows: usize = std::env::var("CASA_RS_T55_PREFLIGHT_ROWS")
        .map(|value| value.parse().expect("positive diagnostic row count"))
        .unwrap_or(351);
    assert!(expected_rows > 0 && expected_rows <= 84_240 && expected_rows.is_multiple_of(351));
    run_q_band_cube(expected_rows, image_size, false);
}

fn run_q_band_cube(expected_rows: usize, image_size: usize, full_input: bool) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let worker_variable = if full_input {
        "CASA_RS_T55_FULL_WORKERS"
    } else {
        "CASA_RS_T55_PREFLIGHT_WORKERS"
    };
    let workers: usize = std::env::var(worker_variable)
        .map(|value| value.parse().expect("positive diagnostic worker count"))
        .unwrap_or(1);
    assert!([1, 2, 4].contains(&workers));
    let memory_bytes: u64 = std::env::var("CASA_RS_T55_NATIVE_MEMORY_BYTES")
        .map(|value| value.parse().expect("positive diagnostic memory budget"))
        .unwrap_or(4 << 30);
    assert!(memory_bytes > 0 && memory_bytes <= 16 << 30);
    let measurement_set = required_path("CASA_RS_T55_REAL_MS");
    let ms = MeasurementSet::open(&measurement_set).expect("preflight MS");
    assert_eq!(
        ms.row_count(),
        expected_rows,
        "all rows of the explicitly selected fixture are required"
    );
    {
        let spectral = ms.spectral_window().expect("Q-band spectral window");
        assert_eq!(spectral.row_count(), 1);
        assert_eq!(spectral.num_chan(0).unwrap(), 512);
        assert_eq!(
            spectral.chan_freq(0).unwrap(),
            (0..512)
                .map(|channel| 44e9 + f64::from(channel) * 2e6)
                .collect::<Vec<_>>(),
            "the retired out-of-band fixture must not be used"
        );
    }
    drop(ms);
    let root = required_path("CASA_RS_T55_ARTIFACT_ROOT");
    fs::create_dir(&root).expect("fresh retained artifact directory");
    let image_name = root.join("image");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "deconvolver": "clark",
            "imsize": image_size,
            "cell": "0.35arcsec",
            "ddid": null,
            "spw": "0",
            "channel_count": 512,
            "specmode": "cube",
            "outframe": "LSRK",
            "start": "0",
            "width": "1",
            "niter": 9,
            "minor_cycle_length": 1,
            "nmajor": 3,
            "gain": 0.1,
            "psfcutoff": f64::from(casa_imaging_products::DEFAULT_PSF_CUTOFF),
            "pblimit": -0.2,
            "write_pb": true,
            "perchanweightdensity": true,
        }),
    );
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    if std::env::var_os("CASA_RS_PROFILE_CUBE").is_some() {
        eprintln!(
            "cube_profile_application start_unix_nanos={}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
    }
    let started = std::time::Instant::now();
    let result = casa_imaging_application::execute(
        &imaging,
        context_with(ResourcePolicy::Explicit {
            workers,
            memory: memory_bytes,
        }),
    )
    .unwrap_or_else(|error| {
        fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
        panic!("Q-band preflight failed: {error}");
    });
    let task_wall_seconds = started.elapsed().as_secs_f64();
    assert!(result.major_cycle_count > 1);
    assert!(result.total_actual_minor_iterations > 0);
    if std::env::var_os("CASA_RS_PROFILE_CUBE").is_some() {
        eprintln!(
            "cube_profile_application end_unix_nanos={}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
    }
    let pass_workers = result.workers;
    assert_eq!(pass_workers, workers);
    assert_products(&image_name, &result.product_names(), &REAL_PRODUCTS);
    let pb = PagedImage::<f32>::open(root.join("image.pb")).expect("published PB");
    assert_eq!(pb.shape(), &[image_size, image_size, 1, 512]);
    let publication_seconds = result
        .summary
        .phases
        .iter()
        .find(|phase| phase.name == "products")
        .expect("the products phase")
        .seconds;
    let fingerprints = std::env::var_os("CASA_RS_T55_PUBLICATION_PROBE")
        .map(|_| publication_probe_fingerprints(&root));
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "scope": if full_input {
                "full corrected Q-band input, all 512 channels/pixels, ordinary CPU Clark cube"
            } else {
                "diagnostic only: reduced rows, all 512 Q-band channels, CPU Clark cube"
            },
            "execution_route": "major-cycle-pass",
            "rows": expected_rows,
            "requested_workers": workers,
            "native_memory_bytes": memory_bytes,
            "pass_workers": pass_workers,
            "image_size": image_size,
            "task_wall_seconds": task_wall_seconds,
            "publication_seconds": publication_seconds,
            "product_fingerprints": fingerprints,
            "major_cycles": result.major_cycle_count,
            "minor_iterations": result.total_minor_iterations,
            "actual_minor_iterations": result.total_actual_minor_iterations,
            "products": result.product_names(),
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
#[ignore = "requires complete corrected Q-band input, explicit 16-GiB resources, fresh durable artifacts and external RSS guard"]
fn t55_full_dataset_clark_timing() {
    run_q_band_cube(4_094_064, 512, true);
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
        &[("natural", "natural"), ("briggs-0.5", "briggs")],
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
        &[("natural", "natural")],
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
        &[("natural", "natural")],
    );
}

/// `weightings`: label and catalog `weighting` (Briggs at robust 0.5).
fn real_clark_worker_cases(
    workload: &str,
    expected_filename: &str,
    image_size: usize,
    first_channel: usize,
    channels: usize,
    iterations: usize,
    weightings: &[(&str, &str)],
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
                .map(|worker| worker.parse::<usize>().expect("positive worker count"))
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
    fs::write(
        root.join("request.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "workload": workload,
            "measurement_set": measurement_set,
            "native_memory_bytes": memory_bytes,
            "workers": worker_counts, "repetitions": repetitions,
            "weightings": weightings.iter().map(|(label, _)| *label).collect::<Vec<_>>(),
            "timing_boundary": "execute: selection and preparation through final product publication; excludes fixture staging and post-run comparison",
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
                let imaging = request(
                    &measurement_set,
                    &image_name,
                    json!({
                        "deconvolver": "clark",
                        "imsize": image_size,
                        "cell": "8arcsec",
                        "ddid": null,
                        "weighting": weighting,
                        "robust": 0.5,
                        "spw": format!("0:{first_channel}~{}", first_channel + channels - 1),
                        "channel_count": channels,
                        "specmode": "cube",
                        "outframe": "LSRK",
                        "start": first_channel.to_string(),
                        "width": "1",
                        "niter": iterations,
                        "minor_cycle_length": 1,
                        "nmajor": 3,
                        "gain": 0.1,
                        "threshold": "0Jy",
                        "psfcutoff": f64::from(casa_imaging_products::DEFAULT_PSF_CUTOFF),
                        "pblimit": -0.2,
                        "write_pb": true,
                        "perchanweightdensity": true,
                    }),
                );
                eprintln!(
                    "T55 real cube start: {label} workers={workers} artifacts={}",
                    directory.display()
                );
                let task_started = std::time::Instant::now();
                let output = match casa_imaging_application::execute(
                    &imaging,
                    context_with(ResourcePolicy::Explicit {
                        workers,
                        memory: memory_bytes,
                    }),
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        fs::write(directory.join("failure.txt"), format!("{error:#?}\n")).unwrap();
                        panic!(
                            "{label} W{workers} failed; retained artifacts at {}: {error}",
                            directory.display()
                        );
                    }
                };
                let task_wall_seconds = task_started.elapsed().as_secs_f64();
                let contract = output.problem.weighting();
                let natural = weighting == "natural";
                assert_eq!(
                    contract.density_scope(),
                    if natural {
                        WeightDensityScope::NotApplicable
                    } else {
                        WeightDensityScope::PerOutputChannel
                    },
                    "executed weighting scope must match the CASA workload",
                );
                assert_eq!(
                    contract.casa_cube_density_padding(),
                    (!natural).then_some(1),
                    "the single-field LSRK cube binds CASA nominal density padding"
                );
                let product_names = output.product_names();
                fs::write(directory.join("summary.json"), serde_json::to_vec_pretty(&serde_json::json!({
                "weighting": label, "requested_workers": workers, "pass_workers": output.workers,
                "task_wall_seconds": task_wall_seconds, "repetition": repetition,
                "native_memory_bytes": memory_bytes, "major_cycles": output.major_cycle_count,
                "minor_cycles": output.minor_cycles.len(),
                "minor_iterations": output.total_minor_iterations,
                "actual_minor_iterations": output.total_actual_minor_iterations,
                "products": product_names, "phases": output.summary.phases,
            })).unwrap()).unwrap();
                assert_eq!(output.workers, workers);
                assert!(output.major_cycle_count > 1 && output.minor_cycles.len() > 1);
                assert!(output.total_actual_minor_iterations > 0);
                assert_products(&image_name, &product_names, &REAL_PRODUCTS);
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
                            pixels: pixels.iter().copied().collect(),
                            masks,
                            default_mask: product.default_mask_name(),
                            coordinates: product.coordinates().to_record(),
                            units: product.units().to_owned(),
                            image_info: product.image_info().expect("complete beam/image metadata"),
                        }
                    })
                    .collect::<Vec<_>>();
                let science = output.scientific();
                let normal = science.normal_state();
                let windows = (0..normal.sum_weights().len())
                    .map(|channel| {
                        normal
                            .read_window(channel..channel + 1)
                            .expect("fixture normal window")
                    })
                    .collect::<Vec<_>>();
                let evidence = (
                    fixture_model_samples(science.final_model()),
                    windows
                        .iter()
                        .flat_map(|window| window.residual().iter())
                        .collect::<Vec<_>>(),
                    windows
                        .iter()
                        .flat_map(|window| window.normal_approximation().iter())
                        .collect::<Vec<_>>(),
                    normal.sum_weights().to_vec(),
                    normal.published_sum_weights().to_vec(),
                    output.minor_cycles.clone(),
                    output.major_cycle_count,
                    output.total_minor_iterations,
                    output.total_actual_minor_iterations,
                );
                match &baseline {
                    None => baseline = Some((products, evidence)),
                    Some((baseline_products, baseline_evidence)) => {
                        // Numerical reductions may round differently.
                        assert_eq!(baseline_evidence.5.len(), evidence.5.len());
                        for (expected, actual) in baseline_products.iter().zip(&products) {
                            assert_eq!(expected.suffix, actual.suffix);
                            assert_eq!(expected.shape, actual.shape);
                            assert_real_agreement(&expected.pixels, &actual.pixels);
                            assert_eq!(expected.masks, actual.masks);
                            assert_eq!(expected.default_mask, actual.default_mask);
                            assert_eq!(expected.coordinates, actual.coordinates);
                            assert_eq!(expected.units, actual.units);
                            assert_eq!(expected.image_info, actual.image_info);
                        }
                        assert_model_agreement(&baseline_evidence.0, &evidence.0);
                        assert_complex_agreement(&baseline_evidence.1, &evidence.1);
                        assert_complex_agreement(&baseline_evidence.2, &evidence.2);
                        assert_real_agreement(&baseline_evidence.3, &evidence.3);
                        assert_real_agreement(&baseline_evidence.4, &evidence.4);
                        assert_eq!(baseline_evidence.6, evidence.6);
                        assert_eq!(baseline_evidence.7, evidence.7);
                        assert_eq!(baseline_evidence.8, evidence.8);
                    }
                }
                fs::write(
                    directory.join("accepted.txt"),
                    "Worker/product scientific agreement checks passed.\n",
                )
                .unwrap();
            }
        }
    }
}
