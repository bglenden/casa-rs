// SPDX-License-Identifier: LGPL-3.0-or-later
//! `exportfits` - CASA image to FITS export.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ExportFitsRequest, ImageAnalysisProtocolInfo, ImageAnalysisTaskResult,
    ImageAnalysisTaskSchemaBundle, export_fits, image_analysis_ui_schema_json,
    read_image_analysis_request_source, run_image_analysis_task,
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
        print_json(&ImageAnalysisTaskSchemaBundle::current("exportfits"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("exportfits").map_err(|error| error.to_string())?
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
    let result = export_fits(
        &request.imagename,
        &request.fitsimage,
        request.velocity,
        request.overwrite,
    )
    .map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Exportfits(result))
}

fn parse_request(args: &[String]) -> Result<ExportFitsRequest, String> {
    if args.len() < 2 {
        return Err(usage());
    }
    let mut velocity = false;
    let mut overwrite = false;
    for arg in &args[2..] {
        match arg.as_str() {
            "--velocity" => velocity = true,
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
    }
    Ok(ExportFitsRequest {
        imagename: PathBuf::from(&args[0]),
        fitsimage: PathBuf::from(&args[1]),
        velocity,
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
    "usage: exportfits <imagename> <fitsimage> [--velocity] [--overwrite] | exportfits --json-run <SOURCE>".to_string()
}
