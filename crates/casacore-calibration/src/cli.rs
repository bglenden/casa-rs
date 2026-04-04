// SPDX-License-Identifier: LGPL-3.0-or-later
//! Developer CLI for calibration-table summary and validation.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{CalibrationTableSummary, summarize_tables};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryFormat {
    Text,
    Json,
}

#[derive(Debug)]
struct CliOptions {
    paths: Vec<PathBuf>,
    format: SummaryFormat,
    output: Option<PathBuf>,
    overwrite: bool,
}

/// Parse environment arguments, run the calibration-table summary CLI, and
/// return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    match parse_args(std::env::args_os().skip(1)) {
        Ok(None) => {
            print!("{}", render_help(program_name));
            0
        }
        Ok(Some(options)) => match run(options) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", render_help(program_name));
            1
        }
    }
}

fn run(options: CliOptions) -> Result<(), String> {
    let path_refs: Vec<_> = options.paths.iter().map(PathBuf::as_path).collect();
    let summaries = summarize_tables(path_refs).map_err(|error| error.to_string())?;
    let rendered = match options.format {
        SummaryFormat::Text => render_text(&summaries),
        SummaryFormat::Json => serde_json::to_string_pretty(&summaries)
            .map_err(|error| format!("serialize summaries: {error}"))?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Option<CliOptions>, String> {
    let args: Vec<_> = args.into_iter().collect();
    if args.is_empty() {
        return Ok(None);
    }

    let mut paths = Vec::new();
    let mut format = SummaryFormat::Text;
    let mut output = None;
    let mut overwrite = false;

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "-h" | "--help" => return Ok(None),
            "--summary-format" => {
                index += 1;
                let value = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --summary-format".to_string())?;
                format = match value {
                    "text" => SummaryFormat::Text,
                    "json" => SummaryFormat::Json,
                    other => {
                        return Err(format!(
                            "unsupported --summary-format {other:?}; expected text or json"
                        ));
                    }
                };
            }
            "--summary-output" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --summary-output".to_string())?;
                output = Some(PathBuf::from(value));
            }
            "--overwrite" => overwrite = true,
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => paths.push(PathBuf::from(&args[index])),
        }
        index += 1;
    }

    if paths.is_empty() {
        return Err("expected at least one calibration-table path".to_string());
    }

    Ok(Some(CliOptions {
        paths,
        format,
        output,
        overwrite,
    }))
}

fn render_help(program_name: &str) -> String {
    format!(
        "\
{program_name} - summarize CASA-compatible calibration tables

USAGE:
  {program_name} [OPTIONS] <caltable>...

OPTIONS:
  --summary-format FORMAT   text | json (default: text)
  --summary-output PATH     write the summary to PATH instead of stdout
  --overwrite               replace an existing output file
  -h, --help                show this help message
"
    )
}

fn render_text(summaries: &[CalibrationTableSummary]) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    for (index, summary) in summaries.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let _ = writeln!(out, "Calibration Table: {}", summary.path.display());
        let _ = writeln!(
            out,
            "  type={} subtype={}",
            summary.table_type, summary.table_subtype
        );
        let _ = writeln!(out, "  rows={}", summary.row_count);
        let _ = writeln!(out, "  par_type={:?}", summary.keywords.par_type);
        let _ = writeln!(out, "  vis_cal={:?}", summary.keywords.vis_cal);
        let _ = writeln!(out, "  parameter_family={:?}", summary.parameter_family);
        let _ = writeln!(
            out,
            "  supported_for_v1_apply={}",
            summary.supported_for_v1_apply()
        );
        let _ = writeln!(out, "  columns={}", summary.columns.join(", "));
        let _ = writeln!(out, "  field_ids={:?}", summary.field_ids);
        let _ = writeln!(
            out,
            "  spectral_window_ids={:?}",
            summary.spectral_window_ids
        );
        let _ = writeln!(out, "  antenna1_ids={:?}", summary.antenna1_ids);
        let _ = writeln!(out, "  antenna2_ids={:?}", summary.antenna2_ids);
        let _ = writeln!(out, "  observation_ids={:?}", summary.observation_ids);
        if let Some(time) = &summary.time_coverage {
            let _ = writeln!(
                out,
                "  time_coverage=[{}, {}] interval={:?}..{:?}",
                time.min_time, time.max_time, time.min_interval, time.max_interval
            );
        }
        for subtable in &summary.subtables {
            let _ = writeln!(
                out,
                "  subtable {} exists={} rows={:?} path={}",
                subtable.name,
                subtable.exists,
                subtable.row_count,
                subtable
                    .resolved_path
                    .as_deref()
                    .unwrap_or_else(|| Path::new("<missing>"))
                    .display()
            );
        }
        if summary.issues.is_empty() {
            let _ = writeln!(out, "  issues=none");
        } else {
            let _ = writeln!(out, "  issues:");
            for issue in &summary.issues {
                let _ = writeln!(
                    out,
                    "    - {:?} {}: {}",
                    issue.severity, issue.code, issue.message
                );
            }
        }
    }
    out
}

fn write_output(path: Option<&Path>, overwrite: bool, text: &str) -> Result<(), String> {
    match path {
        Some(path) => {
            if path.exists() && !overwrite {
                return Err(format!(
                    "refusing to overwrite existing output {}; pass --overwrite to replace it",
                    path.display()
                ));
            }
            fs::write(path, text).map_err(|error| format!("write {}: {error}", path.display()))
        }
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SummaryFormat, parse_args};

    #[test]
    fn parse_args_defaults_to_text_summary() {
        let options = parse_args(["example.gcal".into()])
            .expect("parse succeeds")
            .expect("run action");
        assert_eq!(options.format, SummaryFormat::Text);
        assert_eq!(options.paths.len(), 1);
    }

    #[test]
    fn parse_args_accepts_json_output() {
        let options = parse_args([
            "--summary-format".into(),
            "json".into(),
            "--summary-output".into(),
            "out.json".into(),
            "--overwrite".into(),
            "example.gcal".into(),
        ])
        .expect("parse succeeds")
        .expect("run action");
        assert_eq!(options.format, SummaryFormat::Json);
        assert!(options.overwrite);
        assert_eq!(
            options.output.as_deref().and_then(|path| path.to_str()),
            Some("out.json")
        );
    }
}
