// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` with the imaging port's route law (#932, steps 4 and
//! 5): images as the imager does, then images the same request again on
//! the observation-batch route into `<imagename>.batches*`, compares every
//! product by normalized RMS and writes the comparison, with both routes'
//! wall times, to `<imagename>.routes.json`. A run that writes columns
//! back writes them, on the batch route, to a copy of the MeasurementSet
//! taken before either route writes (`<vis>.batches`), and each column is
//! compared too. The copy needs the MeasurementSet's size again on disk; it
//! is removed when the law holds and kept, for diagnosis, when it fails. A
//! drop-in imager binary for evidence harnesses (T1.5, the baseline
//! workloads): it takes what `casars-imager` takes, on the same task front
//! end (#931), and a run whose law fails is an execution failure (exit 1).
//!
//! ```text
//! cargo build --release -p casars-imager --example imager_routes
//! ```

use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use casa_imaging_application::{ImagingRequest, route_law};
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casars_imager::{ImagerRunTaskResult, ImagingParameters};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "casars-imager",
        casars_imager::imager_task_schema_bundle(),
        &["tclean"],
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

/// Image on both routes and hold them to the law; the reply carries today's
/// run, whose record the command line prints.
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let (parameters, request) = casars_imager::task_request(task)?;
    let (today, holds) = routes(parameters, &request).map_err(TaskFailure::execution)?;
    if !holds {
        return Err(TaskFailure::execution(format!(
            "route law: the routes' products or columns differ; see {}.routes.json",
            request.imagename.display()
        )));
    }
    let text = serde_json::to_string(&today)
        .map_err(|error| TaskFailure::execution(format!("serialize the run: {error}")))?;
    Ok(TaskSuccess::new(casa_task_runtime::CasaValue::None, &today)?.with_text(text + "\n"))
}

/// Image `request`, resolved as `parameters`, on both routes and compare
/// them: today's run, and whether the law holds.
fn routes(
    parameters: ImagingParameters,
    request: &ImagingRequest,
) -> Result<(ImagerRunTaskResult, bool), String> {
    let written = route_law::written_columns(request);
    let copy = (!written.is_empty())
        .then(|| route_law::copy_for_batches(&request.vis))
        .transpose()
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let today = casars_imager::run(parameters.clone(), request)?;
    let today_s = started.elapsed().as_secs_f64();
    let batches = route_law::on_batches(request);
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
    if let Some(copy) = copy.filter(|_| holds) {
        std::fs::remove_dir_all(&copy).map_err(|error| format!("{}: {error}", copy.display()))?;
    }
    Ok((today, holds))
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
