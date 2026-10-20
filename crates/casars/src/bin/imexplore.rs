// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - the `imhead` and `imstat` task provider for persistent
//! casacore images. `casars run imhead|imstat` invokes it as
//! `imexplore imhead|imstat ...`; `--protocol-info`, `--json-schema` and
//! `--json-run` answer through the shared image-analysis provider contract.

use std::env;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::process;

use casa_images::{dispatch_image_analysis_task_cli, imhead, imhead_put, imstat};

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
    let raw_args = env::args().skip(1).collect::<Vec<_>>();
    if let Some(output) = dispatch_image_analysis_task_cli(&raw_args, &usage())? {
        println!("{output}");
        return Ok(());
    }
    let mut args = raw_args.into_iter().peekable();
    if args.peek().is_some_and(|arg| arg == "imhead") {
        args.next();
        let imhead_args = parse_imhead_args(args)?;
        let summary = match imhead_args.mode.as_str() {
            "summary" | "list" => imhead(&imhead_args.path)
                .map_err(|error| image_operation_error("imhead", &imhead_args.path, error))?,
            "put" => {
                let hdkey = imhead_args
                    .hdkey
                    .as_deref()
                    .ok_or_else(|| "imexplore imhead --mode put requires --hdkey".to_string())?;
                let hdvalue = imhead_args
                    .hdvalue
                    .as_deref()
                    .ok_or_else(|| "imexplore imhead --mode put requires --hdvalue".to_string())?;
                imhead_put(&imhead_args.path, hdkey, hdvalue)
                    .map_err(|error| image_operation_error("imhead", &imhead_args.path, error))?
            }
            other => {
                return Err(format!(
                    "unsupported imhead mode {other:?}; expected summary, list, or put"
                ));
            }
        };
        if imhead_args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .map_err(|error| format!("serialize imhead summary: {error}"))?
            );
        } else {
            println!("Image name       : {}", summary.imagename);
            println!("Image type       : {}", summary.image_type);
            println!("Pixel type       : {}", summary.pixel_type);
            println!("Image units      : {}", summary.units);
            println!("Shape            : {:?}", summary.shape);
            if let Some(beam) = summary.restoring_beam {
                println!(
                    "Restoring Beam   : {:.6} arcsec, {:.6} arcsec, {:.6} deg",
                    beam.major_arcsec, beam.minor_arcsec, beam.position_angle_deg
                );
            }
            for axis in summary.axes {
                println!(
                    "Axis {} {:<12} {:<18} shape={} refpix={:.3} crval={:.12e} cdelt={:.12e} {}",
                    axis.axis,
                    axis.coordinate_type,
                    axis.name,
                    axis.shape,
                    axis.reference_pixel,
                    axis.reference_value,
                    axis.increment,
                    axis.unit
                );
            }
        }
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "imstat") {
        args.next();
        let stat_args = parse_imstat_args(args)?;
        let summary = imstat(
            &stat_args.path,
            stat_args.box_pixels.as_deref(),
            stat_args.region.as_deref(),
            stat_args.chans.as_deref(),
            None,
        )
        .map_err(|error| image_operation_error("imstat", &stat_args.path, error))?;
        if stat_args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .map_err(|error| format!("serialize imstat summary: {error}"))?
            );
        } else {
            println!("npts: {}", summary.npts);
            println!("min: {}", summary.min);
            println!("max: {}", summary.max);
            println!("sum: {}", summary.sum);
            println!("mean: {}", summary.mean);
            println!("rms: {}", summary.rms);
            println!("sigma: {}", summary.sigma);
            if let Some(flux) = summary.flux {
                println!("flux: {flux}");
            }
        }
        return Ok(());
    }
    Err(usage())
}

fn usage() -> String {
    format!(
        "usage: imexplore imhead|imstat ... | imexplore --protocol-info | imexplore --json-schema | imexplore --json-run <SOURCE>\n  {}\n  {}",
        imhead_usage(),
        imstat_usage()
    )
}

fn image_operation_error(operation: &str, path: &Path, error: impl Display) -> String {
    format!(
        "{operation}: cannot open or read CASA image '{}': {error}",
        path.display()
    )
}

struct ImheadArgs {
    path: PathBuf,
    json: bool,
    mode: String,
    hdkey: Option<String>,
    hdvalue: Option<String>,
}

fn parse_imhead_args(args: impl IntoIterator<Item = String>) -> Result<ImheadArgs, String> {
    let mut path = None;
    let mut json = false;
    let mut mode = "summary".to_string();
    let mut hdkey = None;
    let mut hdvalue = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--mode" => {
                mode = args
                    .next()
                    .ok_or_else(|| "--mode requires a value".to_string())?
                    .to_ascii_lowercase();
            }
            "--hdkey" => {
                hdkey = Some(
                    args.next()
                        .ok_or_else(|| "--hdkey requires a value".to_string())?,
                );
            }
            "--hdvalue" => {
                hdvalue = Some(
                    args.next()
                        .ok_or_else(|| "--hdvalue requires a value".to_string())?,
                );
            }
            _ if path.is_none() => path = Some(PathBuf::from(arg)),
            _ => return Err(imhead_usage()),
        }
    }
    Ok(ImheadArgs {
        path: path.ok_or_else(imhead_usage)?,
        json,
        mode,
        hdkey,
        hdvalue,
    })
}

fn imhead_usage() -> String {
    "usage: imexplore imhead <image-path> [--json] [--mode summary|list|put] [--hdkey bunit --hdvalue <value>]".to_string()
}

struct ImstatArgs {
    path: PathBuf,
    box_pixels: Option<String>,
    region: Option<String>,
    chans: Option<String>,
    json: bool,
}

fn parse_imstat_args(args: impl IntoIterator<Item = String>) -> Result<ImstatArgs, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let mut path = None;
    let mut box_pixels = None;
    let mut region = None;
    let mut chans = None;
    let mut json = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--json" => json = true,
            "--box" => {
                idx += 1;
                box_pixels = Some(args.get(idx).ok_or_else(imstat_usage)?.clone());
            }
            "--region" => {
                idx += 1;
                region = Some(args.get(idx).ok_or_else(imstat_usage)?.clone());
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(imstat_usage)?.clone());
            }
            _ if path.is_none() => path = Some(PathBuf::from(&args[idx])),
            other => {
                return Err(format!(
                    "unknown imstat argument {other:?}\n{}",
                    imstat_usage()
                ));
            }
        }
        idx += 1;
    }
    Ok(ImstatArgs {
        path: path.ok_or_else(imstat_usage)?,
        box_pixels,
        region,
        chans,
        json,
    })
}

fn imstat_usage() -> String {
    "usage: imexplore imstat <image-path> [--box x0,y0,x1,y1] [--region path|box[[x0pix,y0pix],[x1pix,y1pix]]|world CRTF box] [--chans 0~4] [--json]".to_string()
}
