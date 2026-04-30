// SPDX-License-Identifier: LGPL-3.0-or-later
//! `importfits` - FITS primary image to CASA image import.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisProtocolInfo, ImageAnalysisTaskResult, ImageAnalysisTaskSchemaBundle,
    ImportFitsRequest, image_analysis_ui_schema_json, import_fits,
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
        print_json(&ImageAnalysisTaskSchemaBundle::current("importfits"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("importfits").map_err(|error| error.to_string())?
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
    let result = import_fits(&request.fitsimage, &request.imagename, request.overwrite)
        .map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Importfits(result))
}

fn parse_request(args: &[String]) -> Result<ImportFitsRequest, String> {
    if args.len() < 2 {
        return Err(usage());
    }
    let mut overwrite = false;
    for arg in &args[2..] {
        match arg.as_str() {
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
    }
    Ok(ImportFitsRequest {
        fitsimage: PathBuf::from(&args[0]),
        imagename: PathBuf::from(&args[1]),
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
    "usage: importfits <fitsimage> <imagename> [--overwrite] | importfits --json-run <SOURCE>"
        .to_string()
}
