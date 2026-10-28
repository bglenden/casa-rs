// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` with the imaging port's route law (#932, steps 4 and
//! 5): images as the imager does, then images the same request again on
//! the observation-batch route into `<imagename>.batches*`, compares every
//! product by normalized RMS and writes the comparison, with both routes'
//! wall times, to `<imagename>.routes.json`. A run that writes columns
//! back writes them, on the batch route, to a copy of the MeasurementSet
//! taken before either route writes (`<vis>.batches`), and each column is
//! compared too. A drop-in imager binary for evidence harnesses (T1.5, the
//! baseline workloads); it exits 1 when the law fails.
//!
//! ```text
//! cargo build --release -p casars-imager --example imager_routes
//! ```

use std::ffi::OsString;
use std::path::Path;
use std::time::Instant;

use casa_imaging_application::route_law;
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
    // An informational invocation (a harness probing the protocol) images
    // nothing: answer it as the imager does.
    if args.iter().any(|argument| {
        matches!(
            argument.to_str(),
            Some("--protocol-info" | "--json-schema" | "-h" | "--help")
        )
    }) {
        let status = match casars_imager::run_with_cli_args(args) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        };
        let _ = logging_guard.flush();
        std::process::exit(status);
    }
    let status = match routes(&args) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(error) => {
            eprintln!("Error: {error}");
            1
        }
    };
    let _ = logging_guard.flush();
    std::process::exit(status);
}

/// Image the request `args` make (flags, or `--json-run` with a run
/// envelope) on both routes and compare them; whether the law holds.
fn routes(args: &[OsString]) -> Result<bool, String> {
    let json_run = args
        .iter()
        .position(|argument| argument == "--json-run")
        .and_then(|at| args.get(at + 1));
    let (parameters, request) = match json_run {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
            let envelope: casars_imager::ImagerTaskRequest =
                serde_json::from_str(&text).map_err(|error| error.to_string())?;
            let casars_imager::ImagerTaskRequest::Run(parameters) = envelope;
            casars_imager::resolve_request(&parameters)?
        }
        None => casars_imager::resolve_cli_request(args)?,
    };
    let written = route_law::written_columns(&request);
    if !written.is_empty() {
        route_law::copy_for_batches(&request.vis).map_err(|error| error.to_string())?;
    }
    let started = Instant::now();
    let today = casars_imager::run(parameters.clone(), &request)?;
    let today_s = started.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::to_string(&today).map_err(|error| error.to_string())?
    );
    let batches = route_law::on_batches(&request);
    let mut batch_parameters = parameters;
    batch_parameters.0.insert(
        "imagename".to_string(),
        json!(batches.imagename.display().to_string()),
    );
    batch_parameters
        .0
        .insert("vis".to_string(), json!(batches.vis.display().to_string()));
    let started = Instant::now();
    let other = casars_imager::run(batch_parameters, &batches)?;
    let batches_s = started.elapsed().as_secs_f64();
    let prefix = request.imagename.display().to_string();
    let batch_prefix = batches.imagename.display().to_string();
    let mut holds = other.artifacts.len() == today.artifacts.len();
    let mut products = serde_json::Map::new();
    for artifact in &today.artifacts {
        let suffix = artifact
            .path
            .strip_prefix(&prefix)
            .ok_or_else(|| format!("{} outside {prefix}", artifact.path))?;
        let nrms = route_law::image_nrms(
            Path::new(&artifact.path),
            Path::new(&format!("{batch_prefix}{suffix}")),
        )?;
        holds &= nrms <= route_law::TOLERANCE;
        products.insert(suffix.to_string(), number(nrms));
    }
    let mut columns = serde_json::Map::new();
    for column in written {
        let nrms = route_law::column_nrms(&request.vis, &batches.vis, column)?;
        holds &= nrms <= route_law::TOLERANCE;
        columns.insert(column.to_string(), number(nrms));
    }
    let receipt = json!({
        "law": "imaging port steps 4 and 5: the observation-batch route's products and \
                written columns equal the selected rows' to a normalized RMS of 1e-6",
        "holds": holds,
        "tolerance": route_law::TOLERANCE,
        "products": products,
        "columns": columns,
        "wall_seconds": { "selected_rows": today_s, "observation_batches": batches_s },
    });
    let mut path = request.imagename.clone().into_os_string();
    path.push(".routes.json");
    let text = serde_json::to_string_pretty(&receipt).map_err(|error| error.to_string())?;
    std::fs::write(&path, text).map_err(|error| error.to_string())?;
    eprintln!("route law: {receipt}");
    Ok(holds)
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
