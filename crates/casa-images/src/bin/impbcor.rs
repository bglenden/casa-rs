// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impbcor` - CASA-style primary-beam correction.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_images::{
    ImageAnalysisProtocolInfo, ImageAnalysisTaskResult, ImageAnalysisTaskSchemaBundle,
    ImpbcorRequest, image_analysis_ui_schema_json, impbcor, read_image_analysis_request_source,
    run_image_analysis_task,
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
        print_json(&ImageAnalysisTaskSchemaBundle::current("impbcor"))?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "--ui-schema") {
        print!(
            "{}",
            image_analysis_ui_schema_json("impbcor").map_err(|error| error.to_string())?
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
    let result = impbcor(&request).map_err(|error| error.to_string())?;
    print_json(&ImageAnalysisTaskResult::Impbcor(result))
}

fn parse_request(args: &[String]) -> Result<ImpbcorRequest, String> {
    let mut imagename = None;
    let mut pbimage = None;
    let mut outfile = None;
    let mut mode = "divide".to_string();
    let mut cutoff = -1.0;
    let mut box_selection = None;
    let mut region = None;
    let mut chans = None;
    let mut stokes = None;
    let mut mask = None;
    let mut stretch = false;
    let mut overwrite = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--imagename" | "--input" => {
                idx += 1;
                imagename = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--pbimage" => {
                idx += 1;
                pbimage = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--mode" => {
                idx += 1;
                mode = args.get(idx).ok_or_else(usage)?.clone();
            }
            "--cutoff" => {
                idx += 1;
                cutoff = args
                    .get(idx)
                    .ok_or_else(usage)?
                    .parse()
                    .map_err(|error| format!("parse --cutoff: {error}"))?;
            }
            "--box" => {
                idx += 1;
                box_selection = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--region" => {
                idx += 1;
                region = Some(PathBuf::from(args.get(idx).ok_or_else(usage)?));
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--stokes" => {
                idx += 1;
                stokes = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--mask" => {
                idx += 1;
                mask = Some(args.get(idx).ok_or_else(usage)?.clone());
            }
            "--stretch" => stretch = true,
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        idx += 1;
    }
    Ok(ImpbcorRequest {
        imagename: imagename.ok_or_else(usage)?,
        pbimage: pbimage.ok_or_else(usage)?,
        outfile: outfile.ok_or_else(usage)?,
        mode,
        cutoff,
        box_selection,
        region,
        chans,
        stokes,
        mask,
        stretch,
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
    "usage: impbcor --imagename <image> --pbimage <pb> --outfile <path> [--cutoff 0.2] [--mode divide|multiply] [--box x0,y0,x1,y1] [--region path|CRTF] [--chans selector] [--stokes selector] [--mask expression] [--stretch] [--overwrite] | impbcor --json-run <SOURCE>".to_string()
}
