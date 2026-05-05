// SPDX-License-Identifier: LGPL-3.0-or-later
//! `feather` - CASA-style Fourier-domain image combination.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    FeatherRequest, ImageAnalysisProtocolInfo, ImageAnalysisTaskResult,
    ImageAnalysisTaskSchemaBundle, feather, image_analysis_ui_schema_json,
    read_image_analysis_request_source, run_image_analysis_task,
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
        print_json(&ImageAnalysisTaskSchemaBundle::current("feather"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("feather").map_err(|error| error.to_string())?
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
    let result = feather(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Feather(result))
}

fn parse_request(args: &[String]) -> Result<FeatherRequest, String> {
    let mut imagename = None;
    let mut highres = None;
    let mut lowres = None;
    let mut sdfactor = 1.0_f32;
    let mut overwrite = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--imagename" | "--output" => {
                idx += 1;
                imagename = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--highres" => {
                idx += 1;
                highres = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--lowres" => {
                idx += 1;
                lowres = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--sdfactor" => {
                idx += 1;
                sdfactor = args
                    .get(idx)
                    .ok_or_else(usage)?
                    .parse()
                    .map_err(|error| format!("parse --sdfactor: {error}"))?;
            }
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(FeatherRequest {
        imagename: imagename.ok_or_else(usage)?,
        highres: highres.ok_or_else(usage)?,
        lowres: lowres.ok_or_else(usage)?,
        sdfactor,
        overwrite,
    })
}

fn usage() -> String {
    "usage: feather --imagename <output> --highres <image> --lowres <image> [--sdfactor N] [--overwrite] | feather --json-run <SOURCE>".to_string()
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}
