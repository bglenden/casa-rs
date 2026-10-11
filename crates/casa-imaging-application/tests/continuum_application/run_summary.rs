// SPDX-License-Identifier: LGPL-3.0-or-later

//! The run summary for each way a run ends (plan section 8.3, the R3
//! replan of #658): a completed run records every phase with its memory; a
//! run that fails records the phase that failed last, between its cycles,
//! while publishing or before, with what it did and published until then;
//! and a cancelled run is recorded as cancelled.

use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};

use casa_imaging_application::{HostResources, STAGE_TIMING, SummaryTarget};
use casa_imaging_runtime::{Demand, Reservation, admit, free_memory};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

use super::*;

/// Three one-iteration minor cycles between four major cycles.
fn bounded_cycles(measurement_set: &Path, image_name: &Path) -> ImagingRequest {
    request(
        measurement_set,
        image_name,
        json!({
            "niter": 3,
            "minor_cycle_length": 1,
            "nmajor": 3,
            "gain": 0.37,
            "threshold": "1e-12Jy",
            "nsigma": 1.0e-12,
            "cyclefactor": 1.4,
            "stage_timing": true,
        }),
    )
}

/// The context of a run whose summary goes to `path`.
fn summarised(path: &Path, policy: ResourcePolicy) -> RunContext {
    RunContext {
        summary: Some(SummaryTarget {
            path: path.to_path_buf(),
            request: json!({ "case": "summary" }),
        }),
        ..context_with(policy)
    }
}

fn read_summary(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).expect("the summary")).expect("summary JSON")
}

fn phase_names(summary: &Value) -> Vec<&str> {
    summary["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .map(|phase| phase["name"].as_str().expect("a phase name"))
        .collect()
}

/// A failed run's summary: every phase it records completed but the last,
/// which failed.
pub(super) fn assert_the_last_phase_failed(summary: &Value) {
    assert_eq!(summary["status"], "failed", "{summary}");
    let completed = summary["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .map(|phase| phase["completed"].as_bool().expect("completed"))
        .collect::<Vec<_>>();
    let (last, earlier) = completed
        .split_last()
        .expect("a failed run records the phase that failed");
    assert!(!last, "{summary}");
    assert!(earlier.iter().all(|done| *done), "{summary}");
}

/// The phases of [`bounded_cycles`] between preparation and products: each
/// minor cycle's preparation, the cycle, and the major cycle refreshing the
/// residual; the last preparation decides to stop.
const CYCLES: [&str; 12] = [
    "setup",
    "major cycle 1",
    "minor-cycle preparation 1",
    "minor cycle 1",
    "major cycle 2",
    "minor-cycle preparation 2",
    "minor cycle 2",
    "major cycle 3",
    "minor-cycle preparation 3",
    "minor cycle 3",
    "major cycle 4",
    "minor-cycle preparation 4",
];

#[test]
fn a_completed_run_records_every_phase_with_its_memory() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("completed");
    let path = root.path().join("completed.summary.json");
    let context = summarised(&path, ResourcePolicy::Balanced);
    let capacity = context.policy.memory(&context.host);
    let outcome =
        casa_imaging_application::execute(&bounded_cycles(&measurement_set, &image_name), context)
            .expect("a bounded clean");

    let mut summary = read_summary(&path);
    let status = summary.as_object_mut().expect("an object").remove("status");
    assert_eq!(status, Some(json!("completed")));
    assert!(summary.get("error").is_none(), "{summary}");
    assert_eq!(
        summary,
        serde_json::to_value(&outcome.summary).expect("serializes"),
        "the outcome carries the summary it wrote"
    );
    assert_eq!(summary["request"]["case"], "summary");
    let mut expected = vec!["preparation"];
    expected.extend(CYCLES);
    expected.push("products");
    assert_eq!(phase_names(&summary), expected);
    assert_eq!(summary["minor_cycles"], 3);
    assert_eq!(summary["minor_iterations"], 3);
    assert_eq!(
        summary["stop"],
        json!({ "reason": "iterations", "stopcode": 1 })
    );
    assert_eq!(summary["precision"], "f64");
    assert_eq!(summary["backend"], "cpu");
    assert_eq!(summary["capacity_bytes"], capacity);
    let published = outcome
        .product_names()
        .iter()
        .map(|name| format!("{}{name}", image_name.display()))
        .collect::<Vec<_>>();
    assert_eq!(summary["products"], json!(published));
    let mut previous_rss = 0;
    for phase in summary["phases"].as_array().expect("phases") {
        assert_eq!(phase["completed"], true, "{phase}");
        let field = |name: &str| phase[name].as_u64().expect(name);
        // The high-water mark only rises, and the peak of what admission
        // held covers what it still holds at the phase's end.
        assert!(field("process_peak_rss_bytes") >= previous_rss, "{phase}");
        previous_rss = field("process_peak_rss_bytes");
        assert!(
            field("peak_reserved_bytes") >= field("reserved_bytes"),
            "{phase}"
        );
        assert!(field("peak_reserved_bytes") <= capacity, "{phase}");
    }
    let setup = &summary["phases"][1];
    assert!(
        setup["peak_reserved_bytes"].as_u64() > Some(0),
        "setup admits the operators and weights: {setup}"
    );
}

/// Takes all the memory a run's policy leaves free once the run's phase
/// `after` ends, as another run admitted then would, and holds it until the
/// test drops it.
struct Squeeze {
    after: &'static str,
    context: RunContext,
    held: Arc<Mutex<Option<Reservation>>>,
}

/// The `phase` field of a stage-timing event.
struct PhaseName(Option<String>);

impl Visit for PhaseName {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "phase" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for Squeeze {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != STAGE_TIMING {
            return;
        }
        let mut phase = PhaseName(None);
        event.record(&mut phase);
        if phase.0 != Some(format!("{:?}", self.after)) {
            return;
        }
        let host: HostResources = self.context.host;
        let demand = Demand {
            phase: "another run",
            memory: free_memory(&host, &self.context.policy),
        };
        let held = admit(&host, &self.context.policy, &demand).expect("what is free fits");
        *self.held.lock().expect("held") = Some(held);
    }
}

#[test]
fn a_run_refused_memory_between_its_cycles_records_the_preparation_that_failed() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("refused");
    let path = root.path().join("refused.summary.json");
    let context = summarised(&path, ResourcePolicy::Balanced);
    let held = Arc::new(Mutex::new(None));
    let squeeze = Squeeze {
        after: "major cycle 2",
        context: context.clone(),
        held: held.clone(),
    };
    let error =
        tracing::subscriber::with_default(tracing_subscriber::registry().with(squeeze), || {
            casa_imaging_application::execute(
                &bounded_cycles(&measurement_set, &image_name),
                context,
            )
        })
        .err()
        .expect("nothing is left for the next minor cycle");
    drop(held.lock().expect("held").take().expect("the squeeze ran"));
    assert!(
        matches!(error, ApplicationDispatchError::Admission(_)),
        "{error}"
    );

    let summary = read_summary(&path);
    assert_the_last_phase_failed(&summary);
    assert_eq!(summary["error"], error.to_string());
    let mut expected = vec!["preparation"];
    expected.extend(&CYCLES[..6]);
    assert_eq!(phase_names(&summary), expected);
    assert_eq!(summary["minor_cycles"], 1);
    assert_eq!(summary["minor_iterations"], 1);
    assert_eq!(summary["stop"], Value::Null, "no stop was decided");
    assert_eq!(summary["products"], json!([]));
}

#[test]
fn a_run_that_fails_while_publishing_lists_the_images_it_published() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("partial");
    // A directory cannot replace a symbolic link, so publishing the model
    // fails after the members published before it.
    let model = root.path().join("partial.model");
    std::os::unix::fs::symlink(root.path().join("nowhere"), &model).expect("dangling link");
    let path = root.path().join("partial.summary.json");
    let error = casa_imaging_application::execute(
        &bounded_cycles(&measurement_set, &image_name),
        summarised(&path, ResourcePolicy::Balanced),
    )
    .err()
    .expect("the model cannot be published");

    let summary = read_summary(&path);
    assert_the_last_phase_failed(&summary);
    assert_eq!(summary["error"], error.to_string());
    assert_eq!(phase_names(&summary).last(), Some(&"products"));
    let published = summary["products"]
        .as_array()
        .expect("products")
        .iter()
        .map(|image| PathBuf::from(image.as_str().expect("a path")))
        .collect::<Vec<_>>();
    assert!(
        published.contains(&root.path().join("partial.psf")),
        "the PSF is published first: {summary}"
    );
    assert!(!published.contains(&model), "{summary}");
    for image in &published {
        assert!(
            image.join("table.dat").is_file(),
            "{} is published",
            image.display()
        );
    }
}

#[test]
fn a_run_that_fails_after_its_cycles_keeps_their_record() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    // The cycles write nothing; the products cannot be staged in a
    // directory that refuses new entries.
    let output = root.path().join("read-only");
    std::fs::create_dir(&output).expect("output directory");
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o555))
        .expect("read-only output");
    if std::fs::write(output.join("probe"), b"").is_ok() {
        // A superuser writes through the permissions.
        eprintln!("skipped: the read-only directory accepts new entries");
        return;
    }
    let path = root.path().join("failed.summary.json");
    let result = casa_imaging_application::execute(
        &bounded_cycles(&measurement_set, &output.join("failed")),
        summarised(&path, ResourcePolicy::Balanced),
    );
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o755))
        .expect("writable again");
    let error = result.err().expect("the products cannot be written");

    let summary = read_summary(&path);
    assert_the_last_phase_failed(&summary);
    assert_eq!(summary["error"], error.to_string());
    let mut expected = vec!["preparation"];
    expected.extend(CYCLES);
    expected.push("products");
    assert_eq!(phase_names(&summary), expected);
    assert_eq!(summary["minor_cycles"], 3);
    assert_eq!(summary["minor_iterations"], 3);
    assert_eq!(summary["stop"]["stopcode"], 1);
    assert_eq!(summary["products"], json!([]), "staging failed first");
}

#[test]
fn a_cancelled_run_is_recorded_as_cancelled() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("cancelled");
    let path = root.path().join("cancelled.summary.json");
    let context = summarised(&path, ResourcePolicy::Balanced);
    context.cancel.cancel();
    let error =
        casa_imaging_application::execute(&bounded_cycles(&measurement_set, &image_name), context)
            .err()
            .expect("cancelled before it started");
    assert!(
        matches!(error, ApplicationDispatchError::Cancelled),
        "{error}"
    );

    let summary = read_summary(&path);
    assert_eq!(summary["status"], "cancelled");
    assert!(summary.get("error").is_none(), "{summary}");
    assert_eq!(phase_names(&summary), Vec::<&str>::new());
    assert_eq!(summary["products"], json!([]));
    assert!(!image_name.with_extension("image").exists());
}
