// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` with imaging port step 3b's equality law (#932): images
//! as the imager does, then compares the batch geometry of the run's rows
//! with today's route's and writes the comparison to
//! `<imagename>.geometry-law.json`. A drop-in imager binary for evidence
//! harnesses (T1.5, the baseline workloads): it takes what `casars-imager`
//! takes, on the same task front end, and a run whose law fails is an
//! execution failure (exit 1).
//!
//! ```text
//! cargo build --release -p casars-imager --example imager_geometry_law
//! ```

use std::process::ExitCode;

use casa_imaging_application::geometry_law::{compare_geometry, compare_stores};
use casa_imaging_application::{Cancel, HostResources, ImagingRequest, RunContext};
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "casars-imager",
        casars_imager::imager_task_schema_bundle(),
        &["imager"],
        run,
    )
    .expect("the imager contract carries its surface")
    .main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

/// Image as `casars-imager` does, then hold the run to the law.
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let success = casars_imager::run_task(task)?;
    let (_, request) = casars_imager::task_request(task)?;
    match geometry_law(&request) {
        Ok(true) => Ok(success),
        Ok(false) => Err(TaskFailure::execution(format!(
            "geometry law: the batch geometry differs; see {}.geometry-law.json",
            request.imagename.display()
        ))),
        Err(error) => Err(TaskFailure::execution(format!("geometry law: {error}"))),
    }
}

/// Run the law for `request`, which the run has imaged; whether it holds.
fn geometry_law(request: &ImagingRequest) -> Result<bool, String> {
    let context = RunContext {
        host: HostResources::detect().map_err(|error| error.to_string())?,
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let comparison = compare_geometry(request, &context).map_err(|error| error.to_string())?;
    let receipt = json!({
        "law": "imaging port step 3b: batch geometry equals the selected rows' to 1e-12",
        "holds": comparison.holds(),
        "rows": comparison.rows,
        "unmatched": comparison.unmatched,
        "duplicates": comparison.duplicates,
        "largest": number(comparison.largest()),
        "differences": comparison
            .differences
            .iter()
            .map(|(quantity, (difference, row))| {
                (quantity.to_string(), json!({"relative": number(*difference), "physical_row": row}))
            })
            .collect::<serde_json::Map<_, _>>(),
        "compared": comparison.compared,
    });
    let mut path = request.imagename.clone().into_os_string();
    path.push(".geometry-law.json");
    let text = serde_json::to_string_pretty(&receipt).map_err(|error| error.to_string())?;
    std::fs::write(&path, text).map_err(|error| error.to_string())?;
    eprintln!("geometry law: {receipt}");
    // What the store alone changes: the same rows' batch geometry in an
    // MSv4 conversion of the MeasurementSet, when one is named.
    if let Some(converted) = std::env::var_os("CASA_RS_GEOMETRY_LAW_MSV4") {
        let stores = compare_stores(request, &context, std::path::Path::new(&converted))
            .map_err(|error| error.to_string())?;
        let receipt = json!({
            "measure": "batch geometry of an MSv2 against its MSv4 conversion",
            "converted": std::path::Path::new(&converted).display().to_string(),
            "rows": stores.rows,
            "unmatched": stores.unmatched,
            "duplicates": stores.duplicates,
            "largest": number(stores.largest()),
            "differences": stores
                .differences
                .iter()
                .map(|(quantity, (difference, row))| {
                    (quantity.to_string(), json!({"relative": number(*difference), "physical_row": row}))
                })
                .collect::<serde_json::Map<_, _>>(),
            "compared": stores.compared,
        });
        let mut path = request.imagename.clone().into_os_string();
        path.push(".geometry-stores.json");
        let text = serde_json::to_string_pretty(&receipt).map_err(|error| error.to_string())?;
        std::fs::write(&path, text).map_err(|error| error.to_string())?;
        eprintln!("geometry stores: {receipt}");
    }
    Ok(comparison.holds())
}

/// `value` as JSON: a number, or for an infinite or NaN one (a failed law),
/// its name, where JSON would write `null`.
fn number(value: f64) -> serde_json::Value {
    if value.is_finite() {
        json!(value)
    } else {
        json!(value.to_string())
    }
}
