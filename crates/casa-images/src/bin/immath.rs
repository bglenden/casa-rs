// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immath` - CASA-style image arithmetic for tutorial expressions.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisProtocolInfo, ImageAnalysisTaskResult, ImageAnalysisTaskSchemaBundle,
    ImmathRequest, image_analysis_ui_schema_json, immath, read_image_analysis_request_source,
    run_image_analysis_task,
};

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
    if args.first().is_some_and(|arg| arg == "--protocol-info") {
        print_json(&ImageAnalysisProtocolInfo::current())?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--json-schema") {
        print_json(&ImageAnalysisTaskSchemaBundle::current("immath"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("immath").map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--json-run") {
        let source = args
            .get(1)
            .ok_or_else(|| "--json-run requires <SOURCE> or -".to_string())?;
        let request =
            read_image_analysis_request_source(source).map_err(|error| error.to_string())?;
        let result = run_image_analysis_task(request).map_err(|error| error.to_string())?;
        print_json(&result)?;
        return Ok(());
    }
    let request = parse_request(&args)?;
    let result = immath(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Immath(result))
}

fn parse_request(args: &[String]) -> Result<ImmathRequest, String> {
    let mut inputs = Vec::new();
    let mut outfile = None;
    let mut expr = None;
    let mut overwrite = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--imagename" | "--input" => {
                idx += 1;
                inputs.push(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--expr" => {
                idx += 1;
                expr = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(ImmathRequest {
        imagename: inputs,
        outfile: outfile.ok_or_else(usage)?,
        expr: expr.ok_or_else(usage)?,
        overwrite,
    })
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn usage() -> String {
    "usage: immath --imagename <image0> [--imagename <image1>] --expr 'IM0 * IM1|IM0 / IM1|scalar*IM0' --outfile <path> [--overwrite] | immath --json-run <SOURCE>".to_string()
}
