// SPDX-License-Identifier: LGPL-3.0-or-later
//! `synthetic-ms`: the synthetic MeasurementSet generator as a developer
//! tool (simulation Phase 5d, #841).
//!
//! It runs one request of the generator in `casa_ms::simulation`, unchanged,
//! from JSON:
//!
//! ```text
//! synthetic-ms --json-run <request.json | ->
//! ```
//!
//! The request is `{"kind": "run", "request": ...}` (a
//! `casa_ms::simulation_task::SimobserveRunTaskRequest`) or `{"kind":
//! "family", "request": ...}` (a `SimobserveFamilyTaskRequest`), and the
//! result envelope is printed as `simobserve --json-run` printed it. The
//! benchmark and dataset tools under `tools/perf/imager` call it, so their
//! datasets stay as they were when `simobserve` becomes CASA's task
//! (Phase 5c). It is not a catalog task and is not installed with the suite.

use std::ffi::OsString;
use std::io::Read;
use std::process::ExitCode;

use casa_ms::simulation_task::SimobserveTaskRequest;

const USAGE: &str = "\
Usage: synthetic-ms --json-run <request.json | ->

Generate a synthetic MeasurementSet from a JSON request, read from the file
or (with -) standard input: {\"kind\": \"run\", \"request\": ...} or
{\"kind\": \"family\", \"request\": ...}. The result is printed as JSON.

Logging takes the options every casa-rs tool takes: --log-table <path>,
--log-table-priority and --log-stderr-priority (or the CASA_RS_LOG_*
environment variables).";

fn main() -> ExitCode {
    let (logging, args) =
        match casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1)) {
            Ok(initialized) => initialized,
            Err(error) => {
                eprintln!("Error: failed to initialize logging: {error}");
                return ExitCode::FAILURE;
            }
        };
    let status = match run(&args) {
        Ok(Some(output)) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Ok(None) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(casa.priority = "SEVERE", error = %error, "synthetic-ms failed");
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    };
    if let Err(error) = logging.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        return ExitCode::FAILURE;
    }
    status
}

/// The result of `args`, or `None` for help.
fn run(args: &[OsString]) -> Result<Option<String>, String> {
    let args = args
        .iter()
        .map(|arg| arg.to_str().ok_or("an argument is not valid UTF-8"))
        .collect::<Result<Vec<_>, _>>()?;
    let source = match args.as_slice() {
        ["--json-run", source] => *source,
        [] | ["-h" | "--help"] => return Ok(None),
        _ => return Err(format!("unexpected arguments {args:?}\n\n{USAGE}")),
    };
    let payload = if source == "-" {
        let mut payload = String::new();
        std::io::stdin()
            .read_to_string(&mut payload)
            .map_err(|error| format!("failed to read the request from standard input: {error}"))?;
        payload
    } else {
        std::fs::read_to_string(source)
            .map_err(|error| format!("failed to read the request from {source}: {error}"))?
    };
    let request: SimobserveTaskRequest = serde_json::from_str(&payload)
        .map_err(|error| format!("failed to parse the request: {error}"))?;
    tracing::info!("synthetic-ms started");
    let result = request.execute()?;
    tracing::info!("synthetic-ms completed");
    serde_json::to_string_pretty(&result)
        .map(Some)
        .map_err(|error| format!("failed to write the result: {error}"))
}
