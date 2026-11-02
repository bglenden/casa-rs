// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cubes whose passes run in waves, the admission of their memory, and
//! outlier domains of their own shape in the paged cube state.

use casa_imaging_application::{Admission, SummaryTarget};

use super::*;

/// One worker and `memory` bytes of host memory for the run's phases,
/// beside what compiling leaves resident.
fn memory_policy(memory: u64) -> ResourcePolicy {
    ResourcePolicy::Explicit {
        workers: 1,
        memory: memory + compile_resident(),
    }
}

/// A dirty cube of the 32-channel fixture's channels 1 … 30 at
/// `image_name`, with `overrides`.
fn line_cube(
    measurement_set: &Path,
    image_name: &Path,
    overrides: serde_json::Value,
) -> ImagingRequest {
    let mut values = json!({
        "niter": 0,
        "imsize": 32,
        "spw": "0:1~30",
        "channel_start": 1,
        "channel_count": 30,
        "specmode": "cube",
        "outframe": "TOPO",
        "start": "1",
        "width": "1",
    });
    values
        .as_object_mut()
        .expect("cube controls")
        .extend(overrides.as_object().expect("overrides").clone());
    request(measurement_set, image_name, values)
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
/// pass, both when its waves read restricted channel windows, of all the
/// channels or of a selection with a gap (`0:1~10;20~30`), and under
/// continuum subtraction, whose interior waves reach none of the fit-only
/// channels (0 and 31) and so read whole rows for the fit. (Residual waves
/// with a model are pinned by the runtime's pass laws.)
#[test]
fn dirty_cubes_in_waves_equal_the_resident_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    for (label, overrides) in [
        ("windowed", json!({ "imsize": 128 })),
        ("gapped", json!({ "imsize": 128, "spw": "0:1~10;20~30" })),
        (
            "continuum",
            json!({ "imsize": 128, "fitspw": "0:0;31", "fitorder": 1 }),
        ),
    ] {
        let cube = |name: &str, memory: u64| {
            let image_name = root.path().join(format!("{label}-{name}"));
            let imaging = line_cube(&measurement_set, &image_name, overrides.clone());
            let outcome =
                casa_imaging_application::execute(&imaging, context_with(memory_policy(memory)))
                    .unwrap_or_else(|error| panic!("{label} {name}: {error:?}"));
            (image_name, outcome.planes_per_wave)
        };
        let (resident, resident_waves) = cube("resident", 4 << 30);
        assert_eq!(resident_waves, None, "{label}: every plane fits 4 GiB");
        // Each wave binds its channels per partition, from the conversion
        // factors its times span (#932 step 4).
        let (waved, waves) = cube("waved", WAVED_MEMORY_BYTES);
        assert!(
            waves.is_some_and(|planes| planes < 30),
            "{label}: the capped cube runs in waves: {waves:?}"
        );
        for suffix in DIRTY_PRODUCT_SUFFIXES {
            let nrms = product_law::image_nrms(
                &PathBuf::from(format!("{}{suffix}", resident.display())),
                &PathBuf::from(format!("{}{suffix}", waved.display())),
            )
            .expect("both cubes");
            assert!(
                nrms <= product_law::TOLERANCE,
                "{label} {suffix}: the waves differ by NRMS {nrms:.3e}"
            );
        }
    }
}

/// The density pass reads no visibilities but still applies the continuum
/// transform's channel roles, which flag the fit-only channels (0 and 31)
/// out of the output. On 2 MHz planes from 44.0005 GHz, plane 0 holds
/// fit-only channel 0 and output channel 1, so under uniform weighting
/// channel 0 left in the plane's density would weigh down channel 1. The
/// cube's PSF and weight sums, which read only flags and weights, equal
/// those of the same request over data with channels 0 and 31 flagged in
/// `FLAG` as well.
#[test]
fn fit_only_channels_stay_out_of_the_density() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let image = |name: &str, flagged: bool| {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).expect("run directory");
        let measurement_set = thirty_two_channel_multi_row_measurement_set(&directory);
        if flagged {
            flag_channels(&measurement_set, &[0, 31]);
        }
        let values = json!({
            "niter": 0,
            "imsize": 32,
            "spw": "0:1~30",
            "channel_start": 1,
            "channel_count": 30,
            "specmode": "cube",
            "outframe": "TOPO",
            "start": "44.0005GHz",
            "width": "2MHz",
            "weighting": "uniform",
            "fitspw": "0:0~2;29~31",
            "fitorder": 1,
        });
        let image_name = directory.join("image");
        casa_imaging_application::execute(
            &request(&measurement_set, &image_name, values),
            context(),
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        image_name
    };
    let role_flagged = image("role-flagged", false);
    let flagged = image("flagged", true);
    for suffix in [".psf", ".sumwt"] {
        assert_eq!(
            read_product(&role_flagged, suffix),
            read_product(&flagged, suffix),
            "{suffix}"
        );
    }
}

/// Set FLAG for `channels` of every row and correlation.
fn flag_channels(measurement_set: &Path, channels: &[usize]) {
    let mut ms = MeasurementSet::open(measurement_set).expect("open fixture");
    for row in 0..ms.row_count() {
        let ArrayValue::Bool(mut cell) = ms.flag_column().get(row).expect("FLAG cell").clone()
        else {
            panic!("FLAG is boolean");
        };
        for &channel in channels {
            cell.index_axis_mut(ndarray::Axis(1), channel).fill(true);
        }
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(row, "FLAG", Value::Array(ArrayValue::Bool(cell)))
            .expect("flag the channels");
    }
    ms.save().expect("save fixture");
}

/// Host memory for which the 128 × 128, 30-channel line cube's passes need
/// waves but the selection and one plane fit.
const WAVED_MEMORY_BYTES: u64 = 8 << 20;

/// Admission: a memory ceiling that cannot hold the cube is refused, typed,
/// before anything is written; the least ceiling that admits it runs the
/// cube in waves. When the pass sets that least, a ceiling just above one
/// plane of it runs one plane per wave; when the products do (#932: their
/// restoring convolution can hold more than a plane of a cheap pass), the
/// ceiling just below is refused at the products, before any pass, and the
/// pass takes as many planes as fit beside them.
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
    let imaging = line_cube(
        &measurement_set,
        &root.path().join("admitted"),
        json!({ "imsize": 128 }),
    );
    // A refused run writes nothing: the directory is as it was before it.
    let run = |memory: u64| {
        let before = entries();
        let result =
            casa_imaging_application::execute(&imaging, context_with(memory_policy(memory)));
        if result.is_err() {
            assert_eq!(entries(), before, "a refused run writes nothing");
        }
        result
    };
    // Raise the ceiling by what each refusal reports missing until the pass
    // itself is admitted. Each phase admitted before the pass also takes a
    // share of a higher ceiling (the cube cache a quarter of what is free),
    // so each step adds a third more than the gap.
    let mut memory = 64 << 10;
    let mut refused = memory;
    let mut refusals = 0;
    let admitted = |result: Result<_, _>| match result {
        Ok(outcome) => Ok(outcome),
        Err(ApplicationDispatchError::Admission(Admission {
            phase,
            required,
            available,
        })) => Err((phase, required - available)),
        Err(error) => panic!("only admission refuses a small ceiling: {error}"),
    };
    let mut refused_at = "";
    let mut outcome = loop {
        match admitted(run(memory)) {
            Ok(outcome) => break outcome,
            Err((phase, gap)) => {
                (refused, refused_at) = (memory, phase);
                memory += gap + gap.div_ceil(3);
                refusals += 1;
                assert!(refusals < 32, "the ceiling converges");
            }
        }
    };
    assert!(refusals > 0, "64 KiB holds no line cube");
    // The last step overshoots by a third of its gap: the smallest ceiling
    // admitted, to a kibibyte.
    while memory - refused > 1 << 10 {
        let middle = refused + (memory - refused) / 2;
        match admitted(run(middle)) {
            Ok(smaller) => (memory, outcome) = (middle, smaller),
            Err((phase, _)) => (refused, refused_at) = (middle, phase),
        }
    }
    if refused_at == "products" {
        assert!(
            outcome.planes_per_wave.is_some_and(|planes| planes < 30),
            "the least ceiling ({memory} bytes) runs in waves: {:?}",
            outcome.planes_per_wave
        );
    } else {
        assert_eq!(
            outcome.planes_per_wave,
            Some(1),
            "a ceiling just above one plane ({memory} bytes; refused below at {refused_at:?})"
        );
    }
}

/// A refused run still leaves its summary when the context names one: the
/// request echo and the error, and no products.
#[test]
fn a_refused_run_records_its_error_in_its_summary() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let image_name = root.path().join("refused");
    let imaging = line_cube(&measurement_set, &image_name, json!({}));
    let path = root.path().join("refused.summary.json");
    let error = casa_imaging_application::execute(
        &imaging,
        RunContext {
            summary: Some(SummaryTarget {
                path: path.clone(),
                request: json!({ "case": "refused" }),
            }),
            ..context_with(memory_policy(64 << 10))
        },
    )
    .err()
    .expect("64 KiB holds no cube");
    assert!(
        matches!(error, ApplicationDispatchError::Admission(_)),
        "{error}"
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("the summary")).expect("JSON");
    assert_eq!(summary["request"]["case"], "refused");
    super::run_summary::assert_the_last_phase_failed(&summary);
    assert!(
        summary["error"]
            .as_str()
            .is_some_and(|error| error.contains("needs")),
        "{summary}"
    );
    assert!(!image_name.with_extension("image").exists());
}

/// The paged cube cache stays charged while anything holds the run's model
/// and normal state, products included, and is released with the last
/// holder. The process keeps the charge of its caches, which its first run
/// takes.
#[test]
fn a_completed_cube_keeps_its_cache_charged_until_its_state_drops() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let imaging = line_cube(
        &measurement_set,
        &root.path().join("held"),
        json!({ "imsize": 128 }),
    );
    let context = context_with(memory_policy(64 << 20));
    let (host, policy) = (context.host, context.policy);
    let free = casa_imaging_runtime::free_memory(&host, &policy);
    let table_cache = if free == policy.memory(&host) {
        casa_imaging_runtime::process_cache_bytes()
    } else {
        0
    };
    let outcome =
        casa_imaging_application::execute(&imaging, context).expect("a cube within 64 MiB");
    assert!(
        casa_imaging_runtime::free_memory(&host, &policy) < free - table_cache,
        "the returned cube state still charges its cache"
    );
    drop(outcome);
    assert_eq!(
        casa_imaging_runtime::free_memory(&host, &policy),
        free - table_cache
    );
}

/// A source with many rows grows its blocks by at most a quarter of the
/// memory left past its minimum, so a ceiling that holds its minimum, the
/// cube cache and one plane of the pass runs the cube.
#[test]
fn a_large_source_leaves_room_for_the_cube_cache_and_the_pass() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_many_row_measurement_set(root.path());
    let imaging = line_cube(&measurement_set, &root.path().join("rows"), json!({}));
    let outcome = casa_imaging_application::execute(&imaging, context_with(memory_policy(8 << 20)))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(outcome.major_cycle_count, 1);
}

/// A cleaned cube of the spectral-line fixture's four channels at
/// `image_name`, 64 × 64.
fn cleaned_cube(measurement_set: &Path, image_name: &Path) -> ImagingRequest {
    request(
        measurement_set,
        image_name,
        json!({
            "imsize": 64,
            "spw": "0:0~3",
            "channel_count": 4,
            "specmode": "cube",
            "outframe": "TOPO",
            "niter": 3,
            "cycleniter": 1,
            "nmajor": 3,
            "gain": 0.37,
            "threshold": "1e-12Jy",
        }),
    )
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
            (
                line_cube(&line_set, &main, json!({ "field": "0,1" })),
                32,
                30,
            )
        } else {
            (cleaned_cube(&clean_set, &main), 64, 4)
        };
        imaging.outlierfile = Some(outlier_file);
        let outcome = execute(&imaging).expect("a cube with a smaller outlier");
        assert_eq!(outcome.scientific().normal_state().domain_count(), 2);
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
