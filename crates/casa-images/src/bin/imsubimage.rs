// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imsubimage` - CASA-style image section extraction.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisTaskResult, ImsubimageRequest, dispatch_image_analysis_task_cli, imsubimage,
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
    let result = imsubimage(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Imsubimage(result))
}

fn parse_request(args: &[String]) -> Result<ImsubimageRequest, String> {
    if args.len() < 2 {
        return Err(usage());
    }
    let mut box_pixels = None;
    let mut chans = None;
    let mut overwrite = false;
    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--box" => {
                idx += 1;
                box_pixels = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(ImsubimageRequest {
        imagename: PathBuf::from(&args[0]),
        outfile: PathBuf::from(&args[1]),
        box_pixels,
        chans,
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
    "usage: imsubimage <imagename> <outfile> [--box x0,y0,x1,y1] [--chans 4~12] [--overwrite] | imsubimage --json-run <SOURCE>".to_string()
}
