// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cubes whose passes run in waves, the admission of their memory, and
//! outlier domains of their own shape in the paged cube state.

use casa_imaging_application::{Admission, ApplicationDispatchError};

use super::*;

/// One worker and `memory` bytes of host memory.
const fn memory_policy(memory: u64) -> ResourcePolicy {
    ResourcePolicy::Explicit { workers: 1, memory }
}

/// A dirty cube of the 32-channel fixture's channels 1 … 30 at `image_name`.
fn line_cube(measurement_set: &Path, image_name: PathBuf) -> ContinuumImagingRequest {
    let mut imaging = request(
        measurement_set.to_path_buf(),
        image_name,
        ContinuumAlgorithm::Dirty,
    );
    imaging.image_size = 32;
    imaging.spectral_window = Some("0:1~30".to_string());
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(30);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(30),
    };
    imaging.task_requirements = vec![TaskRequirement::SpectralCube];
    imaging
}

fn read_product(base: &Path, suffix: &str) -> (Vec<usize>, ArrayD<f32>) {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
        .expect("open cube product");
    let shape = image.shape().to_vec();
    let values = image
        .get_slice(&vec![0; shape.len()], &shape)
        .expect("read");
    (shape, values)
}

/// A dirty cube imaged in waves equals the cube imaged in one resident
/// pass, both when its waves read restricted channel windows and under
/// continuum subtraction, whose interior waves reach none of the fit-only
/// channels (0 and 31) and so read whole rows for the fit. (Residual waves
/// with a model are pinned by the runtime's pass laws.)
#[test]
fn dirty_cubes_in_waves_equal_the_resident_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    for (label, continuum) in [("windowed", false), ("continuum", true)] {
        let cube = |name: &str, memory: u64| {
            let image_name = root.path().join(format!("{label}-{name}"));
            let mut imaging = line_cube(&measurement_set, image_name.clone());
            imaging.image_size = 128;
            imaging.continuum_subtraction = continuum.then(|| VisibilityContinuumSubtraction {
                fit_spw: "0:0;31".to_string(),
                fit_order: 1,
            });
            imaging.resource_policy = memory_policy(memory);
            let result = execute_continuum(imaging)
                .unwrap_or_else(|error| panic!("{label} {name}: {error:?}"));
            (image_name, result.outcome.output.planes_per_wave)
        };
        let (resident, resident_waves) = cube("resident", 4 << 30);
        assert_eq!(resident_waves, None, "{label}: every plane fits 4 GiB");
        let (waved, waves) = cube("waved", WAVED_MEMORY_BYTES);
        assert!(
            waves.is_some_and(|planes| planes < 30),
            "{label}: the capped cube runs in waves: {waves:?}"
        );
        for suffix in DIRTY_PRODUCT_SUFFIXES {
            assert_eq!(
                read_product(&waved, suffix),
                read_product(&resident, suffix),
                "{label} {suffix}"
            );
        }
    }
}

/// Host memory for which the 128 × 128, 30-channel line cube's passes need
/// waves but the selection and one plane fit.
const WAVED_MEMORY_BYTES: u64 = 8 << 20;

/// Admission: a memory ceiling that cannot hold the cube is refused, typed,
/// before anything is written; a ceiling just above one plane of the pass
/// runs it one plane per wave.
#[test]
fn a_memory_ceiling_refuses_what_cannot_fit_and_one_plane_runs_one_plane_waves() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let entries = || {
        let mut names = std::fs::read_dir(root.path())
            .expect("output directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    let before = entries();
    let run = |memory: u64| {
        let mut imaging = line_cube(&measurement_set, root.path().join("admitted"));
        imaging.image_size = 128;
        imaging.resource_policy = memory_policy(memory);
        execute_continuum(imaging)
    };
    // Raise the ceiling by what each refusal reports missing until the pass
    // itself is admitted. Each phase admitted before the pass also takes a
    // share of a higher ceiling (the cube cache a quarter of what is free),
    // so each step adds a third more than the gap.
    let mut memory = 64 << 10;
    let mut refusals = 0;
    let waves = loop {
        match run(memory) {
            Ok(result) => break result.outcome.output.planes_per_wave,
            Err(ApplicationDispatchError::Admission(Admission {
                required,
                available,
                ..
            })) => {
                assert_eq!(entries(), before, "a refused run writes nothing");
                let gap = required - available;
                memory += gap + gap.div_ceil(3);
                refusals += 1;
                assert!(refusals < 32, "the ceiling converges");
            }
            Err(error) => panic!("only admission refuses a small ceiling: {error}"),
        }
    };
    assert!(refusals > 0, "64 KiB holds no line cube");
    assert_eq!(waves, Some(1), "a ceiling just above one plane");
}

/// A cleaned cube of the spectral-line fixture's four channels at
/// `image_name`, 64 × 64.
fn cleaned_cube(measurement_set: &Path, image_name: PathBuf) -> ContinuumImagingRequest {
    let mut imaging = request(
        measurement_set.to_path_buf(),
        image_name,
        ContinuumAlgorithm::Hogbom,
    );
    imaging.image_size = 64;
    imaging.spectral_window = Some("0:0~3".into());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(4),
    };
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.task_requirements = vec![TaskRequirement::SpectralCube];
    imaging
}

/// An outlier cube of its own size pages its own planes: a dirty cube with
/// a 32 × 32 main field and a cleaned cube with a 64 × 64 main field, each
/// with a 10 × 10 outlier, complete, and the outlier's products keep its
/// shape.
#[test]
fn outlier_cubes_of_another_size_page_their_own_planes() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let line_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let clean_set = spectral_line_measurement_set(root.path());
    for label in ["dirty", "hogbom"] {
        let main = root.path().join(format!("{label}-main"));
        let outlier = root.path().join(format!("{label}-outlier"));
        let outlier_file = root.path().join(format!("{label}.outlier"));
        std::fs::write(
            &outlier_file,
            format!(
                "imagename={}\nimsize=[10,10]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\n",
                outlier.display()
            ),
        )
        .expect("write the outlier file");
        let (mut imaging, main_pixels, channels) = if label == "dirty" {
            let mut imaging = line_cube(&line_set, main.clone());
            imaging.field_ids = Some(vec![0, 1]);
            (imaging, 32, 30)
        } else {
            (cleaned_cube(&clean_set, main.clone()), 64, 4)
        };
        imaging.outlier_file = Some(outlier_file);
        let result = execute_continuum(imaging).expect("a cube with a smaller outlier");
        assert_eq!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .domain_count(),
            2
        );
        for suffix in DIRTY_PRODUCT_SUFFIXES {
            let plane = |pixels: usize| -> Vec<usize> {
                if suffix == ".sumwt" {
                    vec![1, 1, 1, channels]
                } else {
                    vec![pixels, pixels, 1, channels]
                }
            };
            let (shape, values) = read_product(&outlier, suffix);
            assert_eq!(shape, plane(10), "{label} outlier {suffix}");
            assert!(
                values.iter().all(|value| value.is_finite()),
                "{label} outlier {suffix}"
            );
            assert_eq!(
                read_product(&main, suffix).0,
                plane(main_pixels),
                "{label} {suffix}"
            );
        }
    }
}
