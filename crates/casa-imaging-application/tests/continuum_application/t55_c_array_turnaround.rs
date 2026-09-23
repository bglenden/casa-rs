// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in, matched CASA natural/Clark C-array streaming-cube diagnostic.
//! The caller supplies an isolated owner-initialized MS, existing CASA user mask,
//! fresh durable product directory, and the sampled aggregate 16-GiB RSS guard.

use super::*;
use casa_imaging_runtime::{
    CapacityDomainId, ClaimLifetime, LeaseResource, ResourceOverride, ResourcePolicy,
};
use std::{collections::BTreeMap, fs};

#[test]
#[ignore = "requires explicit C-array turnaround MS, channel, CASA mask, durable outputs and 16-GiB RSS guard"]
fn first_middle_last_plane() {
    run_c_array(false);
}

#[test]
#[ignore = "requires explicit contiguous C-array channel range, CASA mask, durable outputs and 16-GiB RSS guard"]
fn contiguous_spectral_block() {
    run_c_array(true);
}

fn run_c_array(block: bool) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let input = PathBuf::from(std::env::var_os("CASA_RS_C_ARRAY_MS").expect("isolated MS"));
    let root = PathBuf::from(std::env::var_os("CASA_RS_C_ARRAY_OUTPUT").expect("fresh output"));
    let mask = PathBuf::from(std::env::var_os("CASA_RS_C_ARRAY_MASK").expect("CASA user mask"));
    let channel: i32 = std::env::var("CASA_RS_C_ARRAY_CHANNEL")
        .expect("explicit channel")
        .parse()
        .expect("channel number");
    let output_channels: usize = if block {
        std::env::var("CASA_RS_C_ARRAY_OUTPUT_CHANNELS")
            .expect("explicit block channel count")
            .parse()
            .expect("block channel count")
    } else {
        1
    };
    let workers: u64 = if block {
        std::env::var("CASA_RS_C_ARRAY_WORKERS")
            .expect("explicit block worker count")
            .parse()
            .expect("worker count")
    } else {
        1
    };
    let dirty_only = std::env::var_os("CASA_RS_C_ARRAY_DIRTY_ONLY").is_some();
    let iterations = std::env::var("CASA_RS_C_ARRAY_NITER")
        .map(|value| value.parse::<usize>().expect("iteration limit"))
        .unwrap_or(20_000 * output_channels);
    assert!(output_channels > 0 && workers > 0);
    assert!(channel >= 0 && usize::try_from(channel).unwrap() + output_channels <= 512);
    if !block {
        assert!([0, 256, 511].contains(&channel));
    }
    let input_start = channel.min(510);
    let input_end = (channel + i32::try_from(output_channels).unwrap()).min(511);
    let input_channels = usize::try_from(input_end - input_start + 1).unwrap();
    let frequency_hz = 44e9 + f64::from(channel) * 2e6;
    let ms = MeasurementSet::open(&input).expect("C-array turnaround MS");
    assert_eq!(ms.row_count(), 168_480);
    let spectral = ms.spectral_window().expect("spectral metadata");
    assert_eq!(spectral.num_chan(0).unwrap(), 512);
    assert_eq!(
        spectral.meas_freq_ref(0).unwrap(),
        FrequencyRef::LSRK.casacore_code()
    );
    assert_eq!(
        spectral.chan_freq(0).unwrap()[channel as usize],
        frequency_hz
    );
    drop(ms);
    fs::create_dir(&root).expect("fresh retained output directory");
    let prefix = root.join("image");
    let mut imaging = request(input, prefix.clone(), ContinuumAlgorithm::Clark);
    imaging.image_size = 1024;
    imaging.cell_arcsec = 0.06;
    imaging.data_description = None;
    imaging.spectral_window = Some(format!("0:{input_start}~{input_end}"));
    imaging.channel_start = None;
    imaging.channel_count = Some(input_channels);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::LSRK,
            interpolation: casa_ms::CubeInterpolation::Linear,
            start: Some(CubeAxisValue::FrequencyHz {
                hz: frequency_hz,
                frame: None,
            }),
            width: Some(CubeAxisValue::FrequencyHz {
                hz: 2e6,
                frame: None,
            }),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(output_channels),
    };
    imaging.weighting = ContinuumWeighting::Natural;
    imaging.iterations = if dirty_only { 0 } else { iterations };
    imaging.cycle_iterations = 1000;
    imaging.maximum_major_cycles = None;
    imaging.gain = 0.1;
    imaging.threshold_jy = 0.0005;
    imaging.psf_cutoff = 0.35;
    imaging.primary_beam_limit = -0.2;
    imaging.normalization = casa_imaging_model::ProductNormalization::FlatNoise;
    imaging.mask = ContinuumMask::Image(mask);
    imaging.write_primary_beam = true;
    imaging.task_requirements = vec![TaskRequirement::PerChannelWeightDensity];
    if !block {
        imaging.task_requirements.push(TaskRequirement::SerialCpu);
    }
    imaging.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(workers),
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 16 << 30)]),
        ..ResourceOverride::default()
    });
    fs::write(root.join("request.txt"), format!("{imaging:#?}\n")).unwrap();
    eprintln!(
        "C-array matched application start channel={channel} root={}",
        root.display()
    );
    let started = std::time::Instant::now();
    let result = execute_continuum(imaging).unwrap_or_else(|error| {
        fs::write(root.join("failure.txt"), format!("{error:#?}\n")).unwrap();
        panic!("C-array application failed: {error}");
    });
    let seconds = started.elapsed().as_secs_f64();
    eprintln!("C-array application completed channel={channel} seconds={seconds}");
    if !block && !dirty_only && channel == 511 && iterations == 20_000 {
        assert_eq!(
            result.actual_minor_iterations, 3731,
            "CASA stops at the masked global peak after the final major cycle"
        );
    }
    let output = &result.outcome.output;
    assert_eq!(output.initial_receipt.status(), ReceiptStatus::Completed);
    assert_eq!(
        output.publication_receipt.status(),
        ReceiptStatus::Completed
    );
    if dirty_only {
        assert!(output.final_major_receipt.is_none());
    } else {
        assert_eq!(
            output.final_major_receipt.as_ref().unwrap().status(),
            ReceiptStatus::Completed
        );
    }
    let memory = &output
        .initial_receipt
        .selected_alternative_projection()
        .demand
        .memory;
    let native_cube = memory
        .iter()
        .any(|item| item.allocation_id.starts_with("native-cube-"));
    assert!(
        native_cube,
        "this diagnostic must execute the new streaming-cube path"
    );
    let route = "native-streaming-cube";
    super::t55_cube_pipeline::assert_cube_execution_route(&result, true);
    let worker_evidence = std::iter::once(("initial-major", &output.initial_receipt))
        .chain(
            output
                .final_major_receipt
                .as_ref()
                .map(|receipt| ("final-major", receipt)),
        )
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
    let mut products = vec![".image", ".model", ".residual", ".psf", ".pb", ".sumwt"];
    if !dirty_only {
        products.push(".mask");
    }
    let mut expected = products.clone();
    expected.sort_unstable();
    let mut actual = result
        .product_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    for suffix in products {
        assert!(
            root.join(format!("image{suffix}")).is_dir(),
            "missing {suffix}"
        );
    }
    if dirty_only {
        let pixel = |suffix: &str| {
            PagedImage::<f32>::open(root.join(format!("image{suffix}")))
                .unwrap()
                .get_slice(&[16, 3, 0, 0], &[1, 1, 1, 1])
                .unwrap()[[0, 0, 0, 0]]
        };
        assert_eq!(pixel(".pb"), 0.0);
        assert!(
            pixel(".residual").abs() > 0.001,
            "the uncorrected dirty residual must survive outside PB support"
        );
    }
    assert_eq!(
        PagedImage::<f32>::open(root.join("image.image"))
            .unwrap()
            .shape(),
        &[1024, 1024, 1, output_channels]
    );
    let summary = serde_json::json!({
        "channel": channel, "frequency_hz": frequency_hz, "rows": 168_480,
        "seconds": seconds, "execution_route": route, "workers": workers,
        "input_start": input_start, "input_channels": input_channels,
        "output_channels": output_channels, "display_plane": 0,
        "weighting": "natural", "deconvolver": "clark", "interpolation": "linear",
        "worker_evidence": worker_evidence,
        "native_memory_bytes": 16_u64 << 30, "image_size": 1024, "cell_arcsec": 0.06,
        "iterations": result.actual_minor_iterations, "reported_iterations": result.minor_iterations,
        "dirty_only": dirty_only,
        "majors": output.major_cycle_count, "stop_reason": format!("{:?}", result.minor_stop_reason),
        "prefix": prefix, "products": result.product_names,
        "timing_boundary": "execute_continuum from selected input preparation through publication; excludes input copy and comparison"
    });
    fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("{summary}");
}
