// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imregrid` - CASA-style template-image regridding.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisProtocolInfo, ImageAnalysisTaskResult, ImageAnalysisTaskSchemaBundle,
    ImregridRequest, image_analysis_ui_schema_json, imregrid, read_image_analysis_request_source,
    run_image_analysis_task,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--protocol-info") {
        print_json(&ImageAnalysisProtocolInfo::current())?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--json-schema") {
        print_json(&ImageAnalysisTaskSchemaBundle::current("imregrid"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("imregrid").map_err(|error| error.to_string())?
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
    let result = imregrid(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Imregrid(result))
}

fn parse_request(args: &[String]) -> Result<ImregridRequest, String> {
    let mut imagename = None;
    let mut template = None;
    let mut output = None;
    let mut interpolation = "linear".to_string();
    let mut overwrite = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--imagename" | "--input" => {
                idx += 1;
                imagename = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--template" => {
                idx += 1;
                template = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--output" | "--outfile" => {
                idx += 1;
                output = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--interpolation" => {
                idx += 1;
                interpolation = args.get(idx).ok_or_else(usage)?.clone();
            }
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(ImregridRequest {
        imagename: imagename.ok_or_else(usage)?,
        template: template.ok_or_else(usage)?,
        output: output.ok_or_else(usage)?,
        interpolation,
        overwrite,
    })
}

fn usage() -> String {
    "usage: imregrid --imagename <image> --template <image> --output <path> [--interpolation linear|nearest] [--overwrite] | imregrid --json-run <SOURCE>".to_string()
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}
