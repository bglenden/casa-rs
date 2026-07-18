// SPDX-License-Identifier: LGPL-3.0-or-later
//! `exportfits` - CASA image to FITS export.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ExportFitsRequest, ImageAnalysisTaskResult, dispatch_image_analysis_task_cli, export_fits,
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
