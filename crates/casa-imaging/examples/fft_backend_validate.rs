// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validate and time candidate 2-D centered complex FFT backends.
//!
//! Example:
//!
//! ```sh
//! cargo run -p casa-imaging --example fft_backend_validate -- \
//!   --backend rustfft --backend accelerate --precision both --shape 1024x1024
//! ```
//!
//! FFTW is intentionally local-benchmark-only and is not part of Apple GPU
//! backend selection. Set
//! `CASA_RS_FFTW_BENCH_CMD=/path/to/bench` to run an external executable with
//! the arguments `--precision`, `--rows`, `--columns`, and `--use-case`.

use std::process::Command;
use std::time::{Duration, Instant};

use casa_imaging::fft_backend::{
    Fft2Spec, FftBackendChoice, FftDirection, FftPrecision, FftUseCase, FftValidationReport,
    fftw_local_bench_command, validate_fft_backend,
};

#[derive(Debug)]
struct Options {
    backends: Vec<FftBackendChoice>,
    precisions: Vec<FftPrecision>,
    shapes: Vec<(usize, usize)>,
    use_case: FftUseCase,
    json: bool,
    repeat: usize,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(std::env::args().skip(1))?;
    for &(rows, columns) in &options.shapes {
        for &precision in &options.precisions {
            for &backend in &options.backends {
                if backend == FftBackendChoice::FftwLocalBench {
                    print_fftw_local_bench(&options, precision, rows, columns);
                    continue;
                }
                let spec = Fft2Spec::centered_c2c(
                    rows,
                    columns,
                    precision,
                    FftDirection::Forward,
                    options.use_case,
                    backend,
                );
                let mut report = validate_fft_backend(spec);
                for _ in 1..options.repeat {
                    report = validate_fft_backend(spec);
                }
                print_report(&report, options.json);
            }
        }
    }
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut backends = Vec::new();
    let mut precisions = Vec::new();
    let mut shapes = Vec::new();
    let mut use_case = FftUseCase::Benchmark;
    let mut json = false;
    let mut repeat = 1;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--backend" => {
                let value = next_value(&mut args, "--backend")?;
                if value == "all" {
                    backends.extend([
                        FftBackendChoice::RustFft,
                        FftBackendChoice::Accelerate,
                        FftBackendChoice::MetalVkFft,
                        FftBackendChoice::MetalMpsGraph,
                    ]);
                } else if value == "all-with-fftw" {
                    backends.extend([
                        FftBackendChoice::RustFft,
                        FftBackendChoice::Accelerate,
                        FftBackendChoice::MetalVkFft,
                        FftBackendChoice::MetalMpsGraph,
                        FftBackendChoice::FftwLocalBench,
                    ]);
                } else {
                    backends.push(value.parse()?);
                }
            }
            "--precision" => {
                let value = next_value(&mut args, "--precision")?;
                match value.as_str() {
                    "both" => precisions.extend([FftPrecision::F32, FftPrecision::F64]),
                    "f32" => precisions.push(FftPrecision::F32),
                    "f64" => precisions.push(FftPrecision::F64),
                    _ => return Err(format!("unknown precision '{value}'")),
                }
            }
            "--shape" => {
                let value = next_value(&mut args, "--shape")?;
                shapes.push(parse_shape(&value)?);
            }
            "--use-case" => {
                let value = next_value(&mut args, "--use-case")?;
                use_case = parse_use_case(&value)?;
            }
            "--repeat" => {
                let value = next_value(&mut args, "--repeat")?;
                repeat = value
                    .parse::<usize>()
                    .map_err(|error| format!("parse --repeat '{value}': {error}"))?
                    .max(1);
            }
            "--json" => json = true,
            "-h" | "--help" => return Err(help_text()),
            _ => return Err(format!("unknown argument '{arg}'\n{}", help_text())),
        }
    }

    if backends.is_empty() {
        backends.extend([
            FftBackendChoice::RustFft,
            FftBackendChoice::Accelerate,
            FftBackendChoice::MetalVkFft,
            FftBackendChoice::MetalMpsGraph,
        ]);
    }
    if precisions.is_empty() {
        precisions.extend([FftPrecision::F32, FftPrecision::F64]);
    }
    if shapes.is_empty() {
        shapes.extend([(8, 8), (7, 5), (8, 9), (6, 10), (5, 7), (16, 16), (17, 19)]);
    }

    Ok(Options {
        backends,
        precisions,
        shapes,
        use_case,
        json,
        repeat,
    })
}

fn next_value(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value after {flag}"))
}

fn parse_shape(value: &str) -> Result<(usize, usize), String> {
    let (rows, columns) = value
        .split_once('x')
        .or_else(|| value.split_once('X'))
        .ok_or_else(|| format!("shape must be ROWSxCOLUMNS, got '{value}'"))?;
    let rows = rows
        .parse::<usize>()
        .map_err(|error| format!("parse shape rows '{rows}': {error}"))?;
    let columns = columns
        .parse::<usize>()
        .map_err(|error| format!("parse shape columns '{columns}': {error}"))?;
    if rows == 0 || columns == 0 {
        return Err("shape axes must be nonzero".to_string());
    }
    Ok((rows, columns))
}

fn parse_use_case(value: &str) -> Result<FftUseCase, String> {
    match value {
        "dirty" | "dirty-psf-residual" | "dirty_psf_residual" => Ok(FftUseCase::DirtyPsfResidual),
        "model" | "model-degrid" | "model_degrid" => Ok(FftUseCase::ModelDegrid),
        "restoration" | "restore" => Ok(FftUseCase::Restoration),
        "benchmark" => Ok(FftUseCase::Benchmark),
        _ => Err(format!("unknown use case '{value}'")),
    }
}

fn print_report(report: &FftValidationReport, json: bool) {
    if json {
        println!(
            "{{\"backend\":\"{}\",\"selected_backend\":\"{}\",\"precision\":\"{}\",\"rows\":{},\"columns\":{},\"use_case\":\"{}\",\"supported\":{},\"passed\":{},\"reason\":\"{}\",\"forward_max_abs_error\":{},\"inverse_max_abs_error\":{},\"round_trip_max_abs_error\":{},\"tolerance\":{},\"plan_cache_hit\":{},\"pack_ms\":{:.6},\"exec_ms\":{:.6},\"total_ms\":{:.6}}}",
            report.spec.backend_choice,
            report.selection.selected_backend,
            report.spec.precision,
            report.spec.shape.rows,
            report.spec.shape.columns,
            report.spec.use_case,
            report.capability.supported,
            report.passed,
            report.selection.reason,
            json_option_f64(report.forward_max_abs_error),
            json_option_f64(report.inverse_max_abs_error),
            json_option_f64(report.round_trip_max_abs_error),
            report.tolerance,
            report.timing.plan_cache_hit,
            millis(report.timing.pack),
            millis(report.timing.exec),
            millis(report.timing.total),
        );
        return;
    }

    println!(
        "fft_validate backend={} selected_backend={} precision={} shape={}x{} use_case={} supported={} passed={} reason={} forward_max_abs_error={} inverse_max_abs_error={} round_trip_max_abs_error={} tolerance={} plan_cache_hit={} pack_ms={:.6} exec_ms={:.6} total_ms={:.6}",
        report.spec.backend_choice,
        report.selection.selected_backend,
        report.spec.precision,
        report.spec.shape.rows,
        report.spec.shape.columns,
        report.spec.use_case,
        report.capability.supported,
        report.passed,
        report.selection.reason,
        text_option_f64(report.forward_max_abs_error),
        text_option_f64(report.inverse_max_abs_error),
        text_option_f64(report.round_trip_max_abs_error),
        report.tolerance,
        report.timing.plan_cache_hit,
        millis(report.timing.pack),
        millis(report.timing.exec),
        millis(report.timing.total),
    );
}

fn print_fftw_local_bench(options: &Options, precision: FftPrecision, rows: usize, columns: usize) {
    let Some(command) = fftw_local_bench_command() else {
        print_fftw_unconfigured(options.json, precision, rows, columns);
        return;
    };
    let started = Instant::now();
    let output = Command::new(&command)
        .arg("--precision")
        .arg(precision.as_str())
        .arg("--rows")
        .arg(rows.to_string())
        .arg("--columns")
        .arg(columns.to_string())
        .arg("--use-case")
        .arg(options.use_case.as_str())
        .output();
    let wall = started.elapsed();
    match output {
        Ok(output) => {
            let status = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout_first_line = stdout.lines().next().unwrap_or("");
            let stderr_first_line = stderr.lines().next().unwrap_or("");
            if options.json {
                println!(
                    "{{\"backend\":\"{}\",\"precision\":\"{}\",\"rows\":{},\"columns\":{},\"use_case\":\"{}\",\"configured\":true,\"status\":{},\"wall_ms\":{:.6},\"stdout_first_line\":\"{}\",\"stderr_first_line\":\"{}\"}}",
                    FftBackendChoice::FftwLocalBench,
                    precision,
                    rows,
                    columns,
                    options.use_case,
                    status,
                    millis(wall),
                    escape_json(stdout_first_line),
                    escape_json(stderr_first_line),
                );
            } else {
                println!(
                    "fftw_local_bench precision={} shape={}x{} use_case={} configured=true status={} wall_ms={:.6} stdout_first_line={:?} stderr_first_line={:?}",
                    precision,
                    rows,
                    columns,
                    options.use_case,
                    status,
                    millis(wall),
                    stdout_first_line,
                    stderr_first_line,
                );
            }
        }
        Err(error) => {
            if options.json {
                println!(
                    "{{\"backend\":\"{}\",\"precision\":\"{}\",\"rows\":{},\"columns\":{},\"use_case\":\"{}\",\"configured\":true,\"status\":\"spawn_error\",\"wall_ms\":{:.6},\"error\":\"{}\"}}",
                    FftBackendChoice::FftwLocalBench,
                    precision,
                    rows,
                    columns,
                    options.use_case,
                    millis(wall),
                    escape_json(&error.to_string()),
                );
            } else {
                println!(
                    "fftw_local_bench precision={} shape={}x{} use_case={} configured=true status=spawn_error wall_ms={:.6} error={error}",
                    precision,
                    rows,
                    columns,
                    options.use_case,
                    millis(wall),
                );
            }
        }
    }
}

fn print_fftw_unconfigured(json: bool, precision: FftPrecision, rows: usize, columns: usize) {
    if json {
        println!(
            "{{\"backend\":\"{}\",\"precision\":\"{}\",\"rows\":{},\"columns\":{},\"configured\":false,\"reason\":\"set_CASA_RS_FFTW_BENCH_CMD\"}}",
            FftBackendChoice::FftwLocalBench,
            precision,
            rows,
            columns,
        );
    } else {
        println!(
            "fftw_local_bench precision={} shape={}x{} configured=false reason=set_CASA_RS_FFTW_BENCH_CMD",
            precision, rows, columns,
        );
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn json_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn text_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn escape_json(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![character],
        })
        .collect()
}

fn help_text() -> String {
    [
        "Usage: cargo run -p casa-imaging --example fft_backend_validate -- [options]",
        "",
        "Options:",
        "  --backend rustfft|accelerate|metal-vkfft|metal-mpsgraph|fftw-local-bench|all|all-with-fftw",
        "  --precision f32|f64|both",
        "  --shape ROWSxCOLUMNS",
        "  --use-case dirty|model|restoration|benchmark",
        "  --repeat N",
        "  --json",
    ]
    .join("\n")
}
