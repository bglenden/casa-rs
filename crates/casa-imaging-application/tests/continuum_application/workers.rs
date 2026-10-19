// SPDX-License-Identifier: LGPL-3.0-or-later
//! #737: a run takes the most workers, up to its policy's, whose memory
//! fits, rather than being refused.

use casa_imaging_application::SummaryTarget;
use serde_json::Value;

use super::*;

/// A host of four threads, all performance cores, whose memory no run here
/// reaches: an explicit policy sets the memory.
const HOST: HostResources = HostResources {
    threads: 4,
    performance_cores: 4,
    available_memory: 1 << 40,
    metal: false,
};

/// A cleaned MFS image of the 32-channel fixture at `image_name`.
fn cleaned_mfs(measurement_set: &Path, image_name: &Path) -> ImagingRequest {
    request(
        measurement_set,
        image_name,
        json!({ "imsize": 128, "niter": 20, "spw": "0:1~30" }),
    )
}

/// `request` on [`HOST`] with `workers` workers and `memory` bytes beside
/// what compiling leaves resident.
fn run(
    request: &ImagingRequest,
    workers: usize,
    memory: u64,
) -> Result<ImagingOutcome, ApplicationDispatchError> {
    casa_imaging_application::execute(
        request,
        RunContext {
            host: HOST,
            policy: ResourcePolicy::Explicit {
                workers,
                memory: memory + compile_resident(),
            },
            cancel: Cancel::new(),
            summary: None,
        },
    )
}

/// A dirty 30-channel cube of the 32-channel fixture at `image_name`,
/// 256 × 256: its products' scratch grows with the product lanes.
fn dirty_cube(measurement_set: &Path, image_name: &Path) -> ImagingRequest {
    request(
        measurement_set,
        image_name,
        json!({
            "imsize": 256,
            "niter": 0,
            "spw": "0:1~30",
            "channel_start": 1,
            "channel_count": 30,
            "specmode": "cube",
            "outframe": "TOPO",
            "start": "1",
            "width": "1",
        }),
    )
}

/// A cleaned 128 × 128 MFS image of one component per minor cycle, for at
/// most `nmajor` major cycles after the first.
fn cycled_mfs(measurement_set: &Path, image_name: &Path, nmajor: i64) -> ImagingRequest {
    request(
        measurement_set,
        image_name,
        json!({
            "imsize": 128,
            "niter": 20,
            "minor_cycle_length": 1,
            "nmajor": nmajor,
            "gain": 0.37,
            "spw": "0:1~30",
        }),
    )
}

/// #763 review (Astra, finding 1): a dirty cube whose gridding fits four
/// workers but whose products do not fit four lanes publishes, on the
/// most lanes that fit, rather than being refused after its gridding.
#[test]
fn a_cube_whose_products_do_not_fit_every_lane_publishes() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    for budget in [24 << 20, 20 << 20] {
        let name = root.path().join(format!("cube-{budget}"));
        let outcome = run(&dirty_cube(&measurement_set, &name), 4, budget)
            .unwrap_or_else(|error| panic!("{budget} bytes: {error}"));
        assert_dirty_products(&name, &outcome.product_names());
        let summary = &outcome.summary;
        assert!(
            (1..outcome.workers).contains(&summary.product_lanes),
            "{budget} bytes: products on {} lanes beside {} workers",
            summary.product_lanes,
            outcome.workers
        );
        let reason = summary.product_lanes_reason.as_deref().expect("a reason");
        assert!(
            reason.starts_with(&format!("{} lanes need ", outcome.workers)),
            "{reason}"
        );
    }
}

/// [`run`], writing the run's summary to `summary` and returning it beside
/// the outcome, so a refused run's choice of workers can be read.
fn run_summarised(
    request: &ImagingRequest,
    workers: usize,
    memory: u64,
    summary: &Path,
) -> (Result<ImagingOutcome, ApplicationDispatchError>, Value) {
    let outcome = casa_imaging_application::execute(
        request,
        RunContext {
            host: HOST,
            policy: ResourcePolicy::Explicit {
                workers,
                memory: memory + compile_resident(),
            },
            cancel: Cancel::new(),
            summary: Some(SummaryTarget {
                path: summary.to_path_buf(),
                request: json!({ "case": "workers" }),
            }),
        },
    );
    let written =
        serde_json::from_slice(&std::fs::read(summary).expect("the summary")).expect("JSON");
    (outcome, written)
}

/// #763 review (Astra, finding 2; Fable, findings 1 and 2): a count the
/// fit takes is never refused at a later phase. The refresh passes are
/// admitted beside the normal state and model the run keeps, its masks and
/// the minor cycles' records, which grow with each cycle up to `nmajor`.
/// Over budgets 128 KiB apart, and the two of Astra's reproductions, every
/// run completes or is refused with the fit's own reason that no fewer
/// workers fit; the sweep completes on one worker and on more.
#[test]
fn a_count_the_fit_takes_is_never_refused_later() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let mut completed = std::collections::BTreeSet::new();
    for nmajor in [1, 10] {
        let budgets = (1_u64 << 20..=8 << 20)
            .step_by(128 << 10)
            .chain([6 << 20, 6_488_064]);
        let outcomes = fit_sweep(root.path(), &format!("cycled-{nmajor}"), budgets, |name| {
            cycled_mfs(&measurement_set, name, nmajor)
        });
        completed.extend(outcomes.into_iter().filter_map(|(_, workers)| workers));
    }
    assert!(
        completed.contains(&1),
        "some budget runs on one worker: {completed:?}"
    );
    assert!(
        completed.iter().any(|&workers| workers > 1),
        "some budget runs on more: {completed:?}"
    );
}

/// `request` of an image at `name` under `root`, run on four workers with
/// `budget` bytes: it completes, or is refused before the fit chose or
/// with the fit's own reason that no fewer workers fit, never at a later
/// phase for a count the fit took. The workers it completed on, if it did,
/// and the count the fit took, zero when it took none.
fn fit_run(
    root: &Path,
    name: &str,
    budget: u64,
    request: &impl Fn(&Path) -> ImagingRequest,
) -> (Option<usize>, usize) {
    let summary = root.join(format!("{name}.json"));
    let (outcome, written) = run_summarised(&request(&root.join(name)), 4, budget, &summary);
    let reason = written["workers_reason"].as_str().unwrap_or_default();
    let took = if reason.contains("no fewer fit") {
        0
    } else {
        written["workers"].as_u64().unwrap_or_default() as usize
    };
    match outcome {
        Ok(outcome) => (Some(outcome.workers), took),
        Err(error) => {
            // Refused before the fit chose, in the preparation, or by the
            // fit's own reason.
            assert_eq!(
                took, 0,
                "{name}, {budget} bytes: a count the fit took ({reason:?}) was refused: {error}"
            );
            (None, 0)
        }
    }
}

/// [`fit_run`] of `request` at each of `budgets`, its images named `label`
/// and the budget: the workers each run completed on; `None` for a
/// refusal.
fn fit_sweep(
    root: &Path,
    label: &str,
    budgets: impl IntoIterator<Item = u64>,
    request: impl Fn(&Path) -> ImagingRequest,
) -> Vec<(u64, Option<usize>)> {
    budgets
        .into_iter()
        .map(|budget| {
            let (completed, _) = fit_run(root, &format!("{label}-{budget}"), budget, &request);
            (budget, completed)
        })
        .collect()
}

/// The four channels of [`synthetic_observation::observation`] at
/// `measurement_set`, imaged to `image_name` at 128 pixels and cleaned for
/// one major cycle after the first, in minor cycles of 50 iterations, with
/// `controls`: the request of Astra's #778 reproductions.
fn synthetic_clean(measurement_set: &Path, image_name: &Path, controls: Value) -> ImagingRequest {
    let mut values = json!({
        "vis": measurement_set,
        "imagename": image_name,
        "imsize": 128,
        "cell": format!("{}arcsec", synthetic_observation::CELL_ARCSEC),
        "weighting": "natural",
        "niter": 200,
        "minor_cycle_length": 50,
        "nmajor": 1,
        "gain": 0.2,
        "threshold": "1mJy",
    });
    values
        .as_object_mut()
        .expect("controls")
        .extend(controls.as_object().expect("overrides").clone());
    imaging::request(values)
}

/// #778 review (Astra, finding 1; Fable, findings 2 and 3): a count the fit
/// takes is never refused later when a minor cycle's model terms or the
/// pass that writes visibilities set the run's largest stage. A multiscale
/// or MT-MFS component adds flux to every cell of its scale, and the solve
/// admits the terms while it holds its own charge; a cleaning run writes
/// on a refresh pass, at every plane, beside what the run keeps. The fit's
/// charges are tightest at the least budget on which it takes a count: for
/// one, two and four workers that budget is found to 16 KiB, and over the
/// 160 KiB above it, 16 KiB apart, and Astra's reproductions (10,199,040
/// and 10,223,616 bytes for multiscale, 14,909,440 for MT-MFS), every run
/// completes or is refused with the fit's own reason.
#[test]
fn a_count_the_fit_takes_for_scales_or_a_write_is_never_refused_later() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = synthetic_observation::observation(root.path(), 4);
    let cases = [
        (
            "multiscale",
            json!({ "deconvolver": "multiscale", "scales": "5,12" }),
            &[10_199_040_u64, 10_223_616][..],
        ),
        (
            "mtmfs",
            json!({ "deconvolver": "mtmfs", "nterms": 2, "scales": "5,12" }),
            &[14_909_440],
        ),
        (
            "savemodel",
            json!({ "deconvolver": "hogbom", "savemodel": "modelcolumn" }),
            &[],
        ),
    ];
    for (label, controls, reproductions) in cases {
        let request = |name: &Path| synthetic_clean(&measurement_set, name, controls.clone());
        let mut budgets = reproductions.to_vec();
        for workers in [1, 2, 4] {
            let (mut fewer, mut more) = (0_u64, 64_u64 << 20);
            while more - fewer > 16 << 10 {
                let middle = fewer + (more - fewer) / 2;
                let name = format!("{label}-least-{workers}-{middle}");
                if fit_run(root.path(), &name, middle, &request).1 >= workers {
                    more = middle;
                } else {
                    fewer = middle;
                }
            }
            budgets.extend((more..more + (160 << 10)).step_by(16 << 10));
        }
        budgets.sort_unstable();
        budgets.dedup();
        let outcomes = fit_sweep(root.path(), label, budgets, request);
        assert!(
            outcomes.iter().any(|&(_, workers)| workers == Some(4))
                && outcomes
                    .iter()
                    .any(|&(_, workers)| workers.is_some_and(|workers| workers < 4)),
            "{label}: runs on four workers and on fewer: {outcomes:?}"
        );
    }
}

/// #763 review (Astra, finding 3): a setup refused after the fit chose its
/// workers still reports the choice and why. Briggs weighting at 512 pixels
/// on a 4,096,000-byte budget: the fit finds that no count fits and takes
/// one, and the imaging weights are then refused.
#[test]
fn a_refused_setup_reports_the_workers_it_chose() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let name = root.path().join("briggs");
    let summary = root.path().join("briggs.json");
    let (outcome, written) = run_summarised(
        &request(
            &measurement_set,
            &name,
            json!({
                "imsize": 512,
                "niter": 20,
                "weighting": "briggs",
                "robust": 0.5,
                "spw": "0:1~30",
            }),
        ),
        4,
        4_096_000,
        &summary,
    );
    assert!(outcome.is_err(), "the budget does not hold the run");
    assert_eq!(written["workers"], 1, "{written}");
    assert_eq!(written["workers_policy"], 4, "{written}");
    let reason = written["workers_reason"].as_str().expect("a reason");
    assert!(reason.contains("no fewer fit"), "{reason}");
}

fn read_values(base: &Path, suffix: &str) -> Vec<f32> {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
        .expect("open product");
    let shape = image.shape().to_vec();
    image
        .get_slice(&vec![0; shape.len()], &shape)
        .expect("read")
        .iter()
        .copied()
        .collect()
}

/// A request whose four-worker plan exceeds an explicit memory budget, but
/// whose two-worker plan fits, runs on two workers rather than being
/// refused: the smallest budget on which a policy of four workers runs on
/// more than one runs on two, records why in its summary, and its products
/// are a one-worker run's, apart from rounding. (On an MFS plane three
/// workers stream as many as four, so their plans need the same memory.)
#[test]
fn a_policy_whose_workers_do_not_fit_runs_on_the_most_that_do() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let serial_name = root.path().join("serial");
    let serial = run(&cleaned_mfs(&measurement_set, &serial_name), 1, 4 << 30)
        .expect("one worker and ample memory");
    assert_eq!(serial.workers, 1);
    assert_eq!(serial.summary.workers_reason, None);

    // A refusal must be the fit's own, that no fewer workers fit, or come
    // before the fit chose; never a later refusal of a count it took (#763
    // review).
    let workers = |memory: u64| {
        let name = root.path().join(format!("probe-{memory}"));
        let summary = root.path().join(format!("probe-{memory}.json"));
        let (outcome, written) =
            run_summarised(&cleaned_mfs(&measurement_set, &name), 4, memory, &summary);
        outcome.map_or_else(
            |error| {
                let reason = written["workers_reason"].as_str().unwrap_or_default();
                assert!(
                    written["workers"] == 0 || reason.contains("no fewer fit"),
                    "{memory} bytes: a count the fit took was refused: {error}"
                );
                0
            },
            |outcome| outcome.workers,
        )
    };
    let (mut fewer, mut more) = (0_u64, 4_u64 << 30);
    assert_eq!(workers(more), 4, "ample memory holds four workers");
    // The smallest budget, to within a part in a thousand, on which more
    // than one worker runs.
    while more - fewer > more / 1000 {
        let middle = fewer + (more - fewer) / 2;
        if workers(middle) > 1 {
            more = middle;
        } else {
            fewer = middle;
        }
    }
    let parallel_name = root.path().join("parallel");
    let parallel = run(&cleaned_mfs(&measurement_set, &parallel_name), 4, more)
        .expect("the budget holds two workers");
    assert_eq!(parallel.workers, 2, "four workers do not fit, two do");
    assert_eq!(parallel.summary.workers, 2);
    assert_eq!(parallel.summary.workers_policy, 4);
    let reason = parallel
        .summary
        .workers_reason
        .as_deref()
        .expect("a reason");
    assert!(reason.starts_with("4 workers need "), "{reason}");
    assert!(reason.contains(", 2 fit "), "{reason}");
    // Below it at most one worker fits; a refusal there is the fit's own
    // (`workers` checks). On this 128-pixel field the refresh pass, which
    // hardly grows with the workers, sets both counts' needs, so the budget
    // just below may fit none.
    assert!(
        workers(fewer) <= 1,
        "below the budget at most one worker fits"
    );
    for suffix in [".psf", ".residual", ".model", ".image"] {
        assert_real_agreement(
            &read_values(&serial_name, suffix),
            &read_values(&parallel_name, suffix),
        );
    }
}

/// The minor-cycle layout a run plans before its first pass, from which it
/// chooses its workers, is the layout its first pass forms: for an MFS
/// plane, four Stokes planes, a Taylor family, a cube and an MFS field with
/// an outlier of its own size.
#[test]
fn the_planned_minor_cycle_layout_is_the_one_the_first_pass_forms() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let line_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let stokes_set = full_stokes_measurement_set(root.path());
    let outlier_file = root.path().join("layout.outlier");
    std::fs::write(
        &outlier_file,
        format!(
            "imagename={}\nimsize=[10,10]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\n",
            root.path().join("outlier-field").display()
        ),
    )
    .expect("write the outlier file");
    let cases = [
        ("mfs", json!({})),
        ("stokes", json!({ "stokes": "IQUV" })),
        ("taylor", json!({ "deconvolver": "mtmfs", "nterms": 2 })),
        (
            "cube",
            json!({
                "specmode": "cube",
                "outframe": "TOPO",
                "channel_start": 1,
                "channel_count": 4,
                "start": "1",
                "width": "1",
            }),
        ),
        ("outlier", json!({ "field": "0,1" })),
    ];
    for (label, overrides) in cases {
        let (set, mut values) = if label == "stokes" {
            (&stokes_set, json!({ "imsize": 64, "niter": 0 }))
        } else {
            (
                &line_set,
                json!({ "imsize": 64, "niter": 0, "spw": "0:1~30" }),
            )
        };
        values
            .as_object_mut()
            .expect("controls")
            .extend(overrides.as_object().expect("overrides").clone());
        let mut imaging = request(set, &root.path().join(label), values);
        if label == "outlier" {
            imaging.outlierfile = Some(outlier_file.clone());
        }
        let outcome = execute(&imaging).unwrap_or_else(|error| panic!("{label}: {error:?}"));
        assert_eq!(
            casa_imaging_runtime::MinorLayout::planned(&outcome.problem),
            casa_imaging_runtime::MinorLayout::of(outcome.scientific().normal_state()),
            "{label}"
        );
    }
}
