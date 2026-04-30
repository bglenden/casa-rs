// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_ms::selection::MsSelection;
use casa_ms::{MsTransformRequest, TransformDataColumn, mstransform, parse_numeric_id_selector};

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let request = parse_request(&args)?;
    let report = mstransform(&request).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_request(args: &[String]) -> Result<MsTransformRequest, String> {
    let mut input_ms = None;
    let mut output_ms = None;
    let mut spw = None;
    let mut data_column = TransformDataColumn::default();
    let mut selection = MsSelection::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--ms" | "--vis" => {
                index += 1;
                input_ms = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
            }
            "--out" | "--outputvis" => {
                index += 1;
                output_ms = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
            }
            "--spw" => {
                index += 1;
                spw = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--datacolumn" => {
                index += 1;
                data_column = parse_data_column(args.get(index).ok_or_else(usage)?)?;
            }
            "--field" => {
                index += 1;
                selection = selection.field(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "field")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--scan" => {
                index += 1;
                selection = selection.scan(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "scan")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--antenna" => {
                index += 1;
                selection = selection.antenna(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "antenna")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--timerange" => {
                index += 1;
                let value = args.get(index).ok_or_else(usage)?;
                let (start, end) = parse_time_range(value)?;
                selection = selection.time_range(start, end);
            }
            "--msselect" => {
                index += 1;
                selection = selection.taql(args.get(index).ok_or_else(usage)?);
            }
            "--selectdata" => {}
            "--no-selectdata" => {}
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        index += 1;
    }
    Ok(MsTransformRequest {
        input_ms: input_ms.ok_or_else(usage)?,
        output_ms: output_ms.ok_or_else(usage)?,
        spw: spw.ok_or_else(usage)?,
        data_column,
        selection,
    })
}

fn parse_data_column(value: &str) -> Result<TransformDataColumn, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "DATA" => Ok(TransformDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(TransformDataColumn::CorrectedData),
        other => Err(format!(
            "unsupported --datacolumn {other:?}; expected DATA or CORRECTED_DATA"
        )),
    }
}

fn parse_time_range(value: &str) -> Result<(f64, f64), String> {
    let (start, end) = value
        .split_once('~')
        .ok_or_else(|| format!("--timerange must be start~end MJD seconds, got {value:?}"))?;
    let start = start
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange start {start:?}: {error}"))?;
    let end = end
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange end {end:?}: {error}"))?;
    Ok((start, end))
}

fn usage() -> String {
    "usage: mstransform --ms <input.ms> --out <output.ms> --spw <spw[:channels]> [--field <ids>] [--datacolumn DATA|CORRECTED_DATA]".to_string()
}
