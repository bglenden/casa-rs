// SPDX-License-Identifier: LGPL-3.0-or-later

//! The run summary for each way a run ends (plan section 8.3, the R3
//! replan of #658): a completed run records every phase with its memory,
//! a run that fails after its cycles keeps their record, and a cancelled
//! run is recorded as cancelled.

use std::os::unix::fs::PermissionsExt;

use casa_imaging_application::SummaryTarget;
use serde_json::Value;

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

const CYCLES: [&str; 8] = [
    "setup",
    "major cycle 1",
    "minor cycle 1",
    "major cycle 2",
    "minor cycle 2",
    "major cycle 3",
    "minor cycle 3",
    "major cycle 4",
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

    let summary = read_summary(&path);
    assert_eq!(
        summary,
        serde_json::to_value(&outcome.summary).expect("serializes"),
        "the outcome carries the summary it wrote"
    );
    assert_eq!(summary["status"], "completed");
    assert!(summary.get("error").is_none(), "{summary}");
    assert_eq!(summary["request"]["case"], "summary");
    let mut expected = vec!["preparation"];
    expected.extend(CYCLES);
    expected.push("products");
    assert_eq!(phase_names(&summary), expected);
    assert_eq!(summary["minor_cycles"], 3);
    assert_eq!(summary["minor_iterations"], 3);
    assert_eq!(
        summary["stop"],
        json!({ "reason": "Iterations", "stopcode": 1 })
    );
    assert_eq!(summary["precision"], "f64");
    assert_eq!(summary["backend"], "cpu");
    assert_eq!(summary["capacity_bytes"], capacity);
    assert_eq!(
        summary["products"],
        serde_json::to_value(outcome.product_names()).expect("names")
    );
    let mut previous_rss = 0;
    for phase in summary["phases"].as_array().expect("phases") {
        assert_eq!(phase["completed"], true, "{phase}");
        let field = |name: &str| phase[name].as_u64().expect(name);
        // The high-water mark only rises, and the peak of what admission
        // held covers what it still holds at the phase's end.
        assert!(field("process_peak_rss_bytes") >= previous_rss, "{phase}");
        previous_rss = field("process_peak_rss_bytes");
        assert!(
            field("admitted_bytes") >= field("reserved_bytes"),
            "{phase}"
        );
        assert!(field("admitted_bytes") <= capacity, "{phase}");
    }
    let setup = &summary["phases"][1];
    assert!(
        setup["admitted_bytes"].as_u64() > Some(0),
        "setup admits the operators and weights: {setup}"
    );
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
    let path = root.path().join("failed.summary.json");
    let result = casa_imaging_application::execute(
        &bounded_cycles(&measurement_set, &output.join("failed")),
        summarised(&path, ResourcePolicy::Balanced),
    );
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o755))
        .expect("writable again");
    let error = result.err().expect("the products cannot be written");

    let summary = read_summary(&path);
    assert_eq!(summary["status"], "failed");
    assert_eq!(summary["error"], error.to_string());
    let mut expected = vec!["preparation"];
    expected.extend(CYCLES);
    expected.push("products");
    assert_eq!(phase_names(&summary), expected);
    let completed = summary["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .map(|phase| phase["completed"].as_bool().expect("completed"))
        .collect::<Vec<_>>();
    assert_eq!(completed.iter().filter(|done| !**done).count(), 1);
    assert_eq!(completed.last(), Some(&false), "the products failed");
    assert_eq!(summary["minor_cycles"], 3);
    assert_eq!(summary["minor_iterations"], 3);
    assert_eq!(summary["stop"]["stopcode"], 1);
    assert_eq!(summary["products"], json!([]));
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
