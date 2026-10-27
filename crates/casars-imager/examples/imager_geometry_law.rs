// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` with imaging port step 3b's equality law (#932): images
//! as the imager does, then compares the batch geometry of the run's rows
//! with today's route's and writes the comparison to
//! `<imagename>.geometry-law.json`. A drop-in imager binary for evidence
//! harnesses (T1.5, the baseline workloads); it exits 1 when the law fails.
//!
//! ```text
//! cargo build --release -p casars-imager --example imager_geometry_law
//! ```

use std::ffi::OsString;

use casa_imaging_application::geometry_law::{compare_geometry, compare_stores};
use casa_imaging_application::{Cancel, HostResources, RunContext};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() {
    let (logging_guard, args) =
        match casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1)) {
            Ok((guard, args)) => (guard, args),
            Err(error) => {
                eprintln!("Error: failed to initialize logging: {error}");
                std::process::exit(1);
            }
        };
    let args: Vec<OsString> = args.into_iter().collect();
    if let Err(error) = casars_imager::run_with_cli_args(args.clone()) {
        eprintln!("Error: {error}");
        let _ = logging_guard.flush();
        std::process::exit(1);
    }
    // An informational invocation (a harness probing the protocol) images
    // nothing, so there is no geometry to compare.
    if args.iter().any(|argument| {
        matches!(
            argument.to_str(),
            Some("--protocol-info" | "--json-schema" | "-h" | "--help")
        )
    }) {
        let _ = logging_guard.flush();
        return;
    }
    let status = match geometry_law(&args) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(error) => {
            eprintln!("Error: geometry law: {error}");
            1
        }
    };
    let _ = logging_guard.flush();
    std::process::exit(status);
}

/// Run the law for the request `args` make (flags, or `--json-run` with a
/// run envelope); whether it holds.
fn geometry_law(args: &[OsString]) -> Result<bool, String> {
    let json_run = args
        .iter()
        .position(|argument| argument == "--json-run")
        .and_then(|at| args.get(at + 1));
    let (_, request) = match json_run {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
            let envelope: casars_imager::ImagerTaskRequest =
                serde_json::from_str(&text).map_err(|error| error.to_string())?;
            let casars_imager::ImagerTaskRequest::Run(parameters) = envelope;
            casars_imager::resolve_request(&parameters)?
        }
        None => casars_imager::resolve_cli_request(args)?,
    };
    let context = RunContext {
        host: HostResources::detect().map_err(|error| error.to_string())?,
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let comparison = compare_geometry(&request, &context).map_err(|error| error.to_string())?;
    let receipt = json!({
        "law": "imaging port step 3b: batch geometry equals the selected rows' to 1e-12",
        "holds": comparison.holds(),
        "rows": comparison.rows,
        "unmatched": comparison.unmatched,
        "duplicates": comparison.duplicates,
        "largest": comparison.largest(),
        "differences": comparison
            .differences
            .iter()
            .map(|(quantity, (difference, row))| {
                (quantity.to_string(), json!({"relative": difference, "physical_row": row}))
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
        let stores = compare_stores(&request, &context, std::path::Path::new(&converted))
            .map_err(|error| error.to_string())?;
        let receipt = json!({
            "measure": "batch geometry of an MSv2 against its MSv4 conversion",
            "converted": std::path::Path::new(&converted).display().to_string(),
            "rows": stores.rows,
            "unmatched": stores.unmatched,
            "duplicates": stores.duplicates,
            "largest": stores.largest(),
            "differences": stores
                .differences
                .iter()
                .map(|(quantity, (difference, row))| {
                    (quantity.to_string(), json!({"relative": difference, "physical_row": row}))
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
