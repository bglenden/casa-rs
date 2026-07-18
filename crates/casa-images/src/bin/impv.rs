// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impv` - CASA-style position-velocity extraction.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{ImageAnalysisTaskResult, ImpvRequest, dispatch_image_analysis_task_cli, impv};

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
    let result = impv(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Impv(result))
}

fn parse_request(args: &[String]) -> Result<ImpvRequest, String> {
    let imagename = args.first().ok_or_else(usage)?.as_str().to_string();
    let mut outfile = None;
    let mut mode = "coords".to_string();
    let mut start = None;
    let mut end = None;
    let mut width = 1usize;
    let mut chans = None;
    let mut overwrite = false;
    let mut idx = 1;
    while idx < args.len() {
        match args[idx].as_str() {
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--mode" => {
                idx += 1;
                mode = args.get(idx).ok_or_else(usage)?.clone();
            }
            "--start" => {
                idx += 1;
                start = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--end" => {
                idx += 1;
                end = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--width" => {
                idx += 1;
                width = args
                    .get(idx)
                    .ok_or_else(usage)?
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --width: {error}"))?;
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
    Ok(ImpvRequest {
        imagename: PathBuf::from(imagename),
        outfile: outfile.ok_or_else(usage)?,
        mode,
        start: start.ok_or_else(usage)?,
        end: end.ok_or_else(usage)?,
        width,
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
    "usage: impv <imagename> --outfile <path> --start x,y --end x,y [--width pixels] [--chans 4~12] [--overwrite] | impv --json-run <SOURCE>".to_string()
}
