// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immoments` - CASA-style image moment maps.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisTaskResult, ImmomentsRequest, dispatch_image_analysis_task_cli, immoments,
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
    let result = immoments(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Immoments(result))
}

fn parse_request(args: &[String]) -> Result<ImmomentsRequest, String> {
    let imagename = args.first().ok_or_else(usage)?.as_str().to_string();
    let mut outfile = None;
    let mut moments = 0;
    let mut chans = None;
    let mut includepix = None;
    let mut mask = None;
    let mut overwrite = false;
    let mut idx = 1;
    while idx < args.len() {
        match args[idx].as_str() {
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--moments" => {
                idx += 1;
                moments = args
                    .get(idx)
                    .ok_or_else(usage)?
                    .parse::<i32>()
                    .map_err(|error| format!("invalid --moments: {error}"))?;
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--includepix" => {
                idx += 1;
                includepix = Some(parse_range(args.get(idx).ok_or_else(usage)?)?);
            }
            "--mask" => {
                idx += 1;
                mask = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(ImmomentsRequest {
        imagename: PathBuf::from(imagename),
        outfile: outfile.ok_or_else(usage)?,
        moments,
        chans,
        includepix,
        mask,
        overwrite,
    })
}

fn parse_range(text: &str) -> Result<[f64; 2], String> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f64>()
                .map_err(|error| format!("invalid range {text:?}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != 2 {
        return Err(format!("range must be min,max, got {text:?}"));
    }
    Ok([values[0], values[1]])
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn usage() -> String {
    "usage: immoments <imagename> --outfile <path> [--moments -1|0|1|2|3] [--chans 4~12] [--mask image>threshold] [--includepix min,max] [--overwrite] | immoments --json-run <SOURCE>".to_string()
}
