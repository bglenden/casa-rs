// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imregrid` - CASA-style template-image regridding.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisTaskResult, ImregridRequest, dispatch_image_analysis_task_cli, imregrid,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if let Some(output) = dispatch_image_analysis_task_cli(&args, &usage())? {
        println!("{output}");
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
