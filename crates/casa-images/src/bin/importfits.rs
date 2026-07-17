// SPDX-License-Identifier: LGPL-3.0-or-later
//! `importfits` - FITS primary image to CASA image import.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisTaskResult, ImportFitsRequest, dispatch_image_analysis_task_cli, import_fits,
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
    if let Some(output) = dispatch_image_analysis_task_cli(&args, &usage())? {
        println!("{output}");
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
