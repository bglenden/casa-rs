// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native runs on the Metal backend equal the same runs on the CPU, product
//! by product, to the order of the device's `f32` additions: an MFS clean, a
//! cleaned cube (native-channel residual predictions on the CPU, every grid
//! on the device), and a dirty cube with output channels twice the native
//! width imaged in waves. Skipped, saying so, without a Metal device.

use casa_imaging_application::{BackendChoice, availability::Unsupported};

use super::*;

/// Every pixel of a product within this fraction of the product's peak.
const TOLERANCE: f64 = 1.0e-4;

fn values(base: &Path, suffix: &str) -> (Vec<usize>, Vec<f32>) {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
        .expect("open product");
    let shape = image.shape().to_vec();
    let values = image
        .get_slice(&vec![0; shape.len()], &shape)
        .expect("read product");
    (shape, values.iter().copied().collect())
}

fn peak(values: &[f32]) -> f64 {
    values
        .iter()
        .fold(0.0_f64, |peak, value| peak.max(f64::from(*value).abs()))
}

/// Every pixel of every product within [`TOLERANCE`] of the larger of the
/// product's peak and the restored image's: a residual's `f32` error scales
/// with the model the device predicts, not with what cleaning leaves.
fn assert_products_close(metal: &Path, cpu: &Path, suffixes: &[&str], label: &str) {
    let image_peak = peak(&values(cpu, ".image").1);
    assert!(image_peak > 0.0, "{label}: the restored image is empty");
    for suffix in suffixes {
        let (metal_shape, metal_values) = values(metal, suffix);
        let (cpu_shape, cpu_values) = values(cpu, suffix);
        assert_eq!(metal_shape, cpu_shape, "{label} {suffix}");
        let scale = peak(&cpu_values).max(image_peak);
        let worst = metal_values
            .iter()
            .zip(&cpu_values)
            .fold(0.0_f64, |worst, (m, c)| {
                worst.max((f64::from(*m) - f64::from(*c)).abs())
            });
        assert!(
            worst <= TOLERANCE * scale,
            "{label} {suffix}: largest difference {worst:e} against a peak of {scale:e}"
        );
    }
}

/// Run `imaging` with `backend = metal`, then `cpu`, each under the
/// policy `policy` gives it; returns the Metal and CPU image names and the
/// Metal run's planes per wave, or `None` (saying so) when the host has no
/// Metal device.
fn both(
    root: &Path,
    label: &str,
    imaging: impl Fn(&Path, &str) -> ImagingRequest,
    policy: impl Fn(BackendChoice) -> ResourcePolicy,
) -> Option<(PathBuf, PathBuf, Option<u32>)> {
    let metal = root.join(format!("{label}-metal"));
    let outcome = match casa_imaging_application::execute(
        &imaging(&metal, "metal"),
        context_with(policy(BackendChoice::Metal)),
    ) {
        Ok(outcome) => outcome,
        Err(ApplicationDispatchError::Unavailable(unavailable))
            if unavailable
                .unsupported()
                .contains(&Unsupported::NoMetalDevice) =>
        {
            eprintln!("skipped: no Metal device");
            return None;
        }
        Err(error) => panic!("{label} on Metal: {error:?}"),
    };
    let cpu = root.join(format!("{label}-cpu"));
    casa_imaging_application::execute(
        &imaging(&cpu, "cpu"),
        context_with(policy(BackendChoice::Cpu)),
    )
    .unwrap_or_else(|error| panic!("{label} on the CPU: {error:?}"));
    Some((metal, cpu, outcome.planes_per_wave))
}

#[test]
fn metal_mfs_clean_equals_the_cpu_clean() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let Some((metal, cpu, _)) = both(
        root.path(),
        "mfs",
        |name, backend| request(&measurement_set, name, json!({ "backend": backend })),
        |_| ResourcePolicy::Balanced,
    ) else {
        return;
    };
    assert_products_close(&metal, &cpu, &DIRTY_PRODUCT_SUFFIXES, "mfs");
}

#[test]
fn metal_cleaned_cube_equals_the_cpu_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let Some((metal, cpu, _)) = both(
        root.path(),
        "cube",
        |name, backend| {
            request(
                &measurement_set,
                name,
                json!({
                    "imsize": 64,
                    "spw": "0:0~3",
                    "channel_count": 4,
                    "specmode": "cube",
                    "outframe": "TOPO",
                    "niter": 3,
                    "minor_cycle_length": 1,
                    "nmajor": 3,
                    "gain": 0.37,
                    "threshold": "1e-12Jy",
                    "backend": backend,
                }),
            )
        },
        |_| ResourcePolicy::Balanced,
    ) else {
        return;
    };
    assert_products_close(&metal, &cpu, &DIRTY_PRODUCT_SUFFIXES, "cube");
}

/// The IF-0 acceptance addition on #653: output channels wider than the
/// native spacing (two native channels each) on Metal, in waves, equal the
/// resident CPU cube.
#[test]
fn metal_cube_with_coarse_output_channels_in_waves_equals_the_cpu_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let Some((metal, cpu, waves)) = both(
        root.path(),
        "coarse",
        |name, backend| {
            request(
                &measurement_set,
                name,
                json!({
                    "niter": 0,
                    "imsize": 128,
                    "spw": "0:1~30",
                    "channel_start": 1,
                    "channel_count": 15,
                    "specmode": "cube",
                    "outframe": "TOPO",
                    "start": "1",
                    "width": "2",
                    "backend": backend,
                }),
            )
        },
        |backend| ResourcePolicy::Explicit {
            workers: 2,
            memory: match backend {
                BackendChoice::Metal => COARSE_METAL_MEMORY_BYTES,
                BackendChoice::Cpu => 4 << 30,
            },
        },
    ) else {
        return;
    };
    assert!(
        waves.is_some_and(|planes| planes < 15),
        "the capped Metal cube runs in waves: {waves:?}"
    );
    assert_eq!(values(&metal, ".image").0, vec![128, 128, 1, 15]);
    assert_products_close(&metal, &cpu, &DIRTY_PRODUCT_SUFFIXES, "coarse");
}

/// Host memory for which the 128 × 128, 15-channel Metal cube needs waves
/// but two owners' rings and one plane fit.
const COARSE_METAL_MEMORY_BYTES: u64 = 28 << 20;
