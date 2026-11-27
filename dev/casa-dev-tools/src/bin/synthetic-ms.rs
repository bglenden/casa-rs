// SPDX-License-Identifier: LGPL-3.0-or-later
//! Developer MeasurementSet staging through casa-simulation's native session.
//! Run/family JSON inputs are described by synthetic::task::Request.
use std::ffi::OsString;
use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use casa_simulation::synthetic::task::Request;
use casa_tables::log_table::{
    CasaLogLayer, CasaLogSink, CasaPriority, CasaTableLogSink, CompositeSink, NullSink, StderrSink,
    TableLogOpenMode,
};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

const USAGE: &str = "\
Usage: synthetic-ms --json-run <request.json | ->

Generate a synthetic MeasurementSet from a JSON request, read from the file
or (with -) standard input: {\"kind\": \"run\", \"request\": ...} or
{\"kind\": \"family\", \"request\": ...}. The result is printed as JSON.

Logging takes the options every casa-rs tool takes: --log-table <path>,
--log-table-priority and --log-stderr-priority (or the CASA_RS_LOG_*
environment variables).";

fn init_logging(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(Arc<dyn CasaLogSink>, Vec<OsString>), String> {
    let mut table_path = std::env::var_os("CASA_RS_LOG_TABLE").map(PathBuf::from);
    let mut table_priority = match std::env::var("CASA_RS_LOG_TABLE_PRIORITY") {
        Ok(v) => CasaPriority::from_str(&v)
            .map_err(|_| format!("invalid CASA_RS_LOG_TABLE_PRIORITY priority {v:?}"))?,
        Err(_) => CasaPriority::Info,
    };
    let is_off = |v: &str| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "none" | "quiet" | "false" | "0"
        )
    };
    let mut stderr_priority = match std::env::var("CASA_RS_LOG_STDERR_PRIORITY") {
        Ok(v) if is_off(&v) => None,
        Ok(v) => Some(
            CasaPriority::from_str(&v)
                .map_err(|_| format!("invalid CASA_RS_LOG_STDERR_PRIORITY priority {v:?}"))?,
        ),
        Err(_) => Some(CasaPriority::Warn),
    };
    let mut filtered = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let s = arg.to_str();
        if let Some(val) = s.and_then(|s| s.strip_prefix("--log-table=")) {
            table_path = Some(PathBuf::from(val));
        } else if arg == "--log-table" {
            let val = iter
                .next()
                .ok_or_else(|| "missing value for --log-table".to_string())?;
            table_path = Some(PathBuf::from(val));
        } else if let Some(val) = s.and_then(|s| s.strip_prefix("--log-table-priority=")) {
            table_priority = CasaPriority::from_str(val).map_err(|e| e.to_string())?;
        } else if arg == "--log-table-priority" {
            let val = iter
                .next()
                .ok_or_else(|| "missing value for --log-table-priority".to_string())?;
            let val = val
                .to_str()
                .ok_or_else(|| "invalid non-UTF-8 value for --log-table-priority".to_string())?;
            table_priority = CasaPriority::from_str(val).map_err(|e| e.to_string())?;
        } else if let Some(val) = s.and_then(|s| s.strip_prefix("--log-stderr-priority=")) {
            stderr_priority = if is_off(val) {
                None
            } else {
                Some(CasaPriority::from_str(val).map_err(|e| e.to_string())?)
            };
        } else if arg == "--log-stderr-priority" {
            let val = iter
                .next()
                .ok_or_else(|| "missing value for --log-stderr-priority".to_string())?;
            let val = val
                .to_str()
                .ok_or_else(|| "invalid non-UTF-8 value for --log-stderr-priority".to_string())?;
            stderr_priority = if is_off(val) {
                None
            } else {
                Some(CasaPriority::from_str(val).map_err(|e| e.to_string())?)
            };
        } else {
            filtered.push(arg);
        }
    }
    let mut sinks: Vec<Arc<dyn CasaLogSink>> = Vec::new();
    if let Some(path) = table_path {
        sinks.push(Arc::new(CasaTableLogSink::new(
            path,
            table_priority,
            TableLogOpenMode::CreateOrAppend,
        )));
    }
    if let Some(priority) = stderr_priority {
        sinks.push(Arc::new(StderrSink::new(priority)));
    }
    let sink: Arc<dyn CasaLogSink> = if sinks.is_empty() {
        Arc::new(NullSink)
    } else {
        Arc::new(CompositeSink::new(sinks))
    };
    tracing::subscriber::set_global_default(
        Registry::default().with(CasaLogLayer::filtered(sink.clone())),
    )
    .map_err(|error| format!("tracing subscriber setup failed: {error}"))?;
    Ok((sink, filtered))
}

fn main() -> ExitCode {
    let (logging, args) = match init_logging(std::env::args_os().skip(1)) {
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
    let mut request: Request = serde_json::from_str(&payload)
        .map_err(|error| format!("failed to parse the request: {error}"))?;
    if let Request::Family(family) = &mut request {
        for name in ["CASA_RS_SIMOBSERVE_CONFIG_ROOT", "CASADATA"] {
            if let Some(paths) = std::env::var_os(name) {
                family.data_roots.extend(std::env::split_paths(&paths));
            }
        }
        if let Some(paths) = std::env::var_os("CASAPATH")
            && let Some(first) = paths.to_string_lossy().split_whitespace().next()
        {
            family
                .data_roots
                .push(std::path::Path::new(first).join("data"));
        }
    }
    tracing::info!("synthetic-ms started");
    let result = request.execute()?;
    tracing::info!("synthetic-ms completed");
    serde_json::to_string_pretty(&result)
        .map(Some)
        .map_err(|error| format!("failed to write the result: {error}"))
}
