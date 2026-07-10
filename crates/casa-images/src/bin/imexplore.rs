// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - inspect persistent casacore images from the command line.

use std::env;
use std::fmt::Display;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process;

use casa_images::{
    ImageBrowserSession, ImmomentsRequest, ImpvRequest, image_analysis_ui_schema_json,
    imexplore_ui_schema_json, imhead, imhead_put, immoments, impv, imstat,
};
use casars_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserProtocolInfo, ImageBrowserRequestEnvelope,
    ImageBrowserResponseEnvelope, ImageBrowserViewport, PROTOCOL_VERSION, schema_bundle_json,
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
    let raw_args = env::args().skip(1).collect::<Vec<_>>();
    if raw_args.len() == 2
        && raw_args[1] == "--ui-schema"
        && matches!(
            raw_args[0].as_str(),
            "imhead" | "imstat" | "immoments" | "impv"
        )
    {
        print!(
            "{}",
            image_analysis_ui_schema_json(&raw_args[0]).map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let mut args = raw_args.into_iter().peekable();
    if args.peek().is_some_and(|arg| arg == "--json-schema") {
        print!(
            "{}",
            schema_bundle_json()
                .map_err(|error| format!("serialize imexplore schema bundle: {error}"))?
        );
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "--protocol-info") {
        print!(
            "{}",
            serde_json::to_string_pretty(&ImageBrowserProtocolInfo::current())
                .map_err(|error| format!("serialize imexplore protocol info: {error}"))?
        );
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "--ui-schema") {
        print!("{}", imexplore_ui_schema_json("imexplore")?);
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "--session") {
        args.next();
        if args.peek().is_some() {
            return Err("--session does not accept positional arguments".to_string());
        }
        run_session()?;
        return Ok(());
    }
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
    if args.peek().is_some_and(|arg| arg == "immoments") {
        args.next();
        let moment_args = parse_immoments_args(args)?;
        let summary = immoments(&moment_args.request).map_err(|error| {
            image_operation_error("immoments", &moment_args.request.imagename, error)
        })?;
        if moment_args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .map_err(|error| format!("serialize immoments summary: {error}"))?
            );
        } else {
            println!("outfile: {}", summary.outfile);
            println!("moment: {}", summary.moment);
            println!("shape: {:?}", summary.shape);
            println!("valid_profiles: {}", summary.valid_profiles);
            println!("units: {}", summary.units);
        }
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "impv") {
        args.next();
        let pv_args = parse_impv_args(args)?;
        let summary = impv(&pv_args.request)
            .map_err(|error| image_operation_error("impv", &pv_args.request.imagename, error))?;
        if pv_args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .map_err(|error| format!("serialize impv summary: {error}"))?
            );
        } else {
            println!("outfile: {}", summary.outfile);
            println!("shape: {:?}", summary.shape);
            println!("path_pixels: {}", summary.path_pixels);
            println!("width: {}", summary.width);
            println!("units: {}", summary.units);
        }
        return Ok(());
    }

    let image_path = parse_path(args)?;
    run_snapshot(&image_path)
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

struct ImmomentsArgs {
    request: ImmomentsRequest,
    json: bool,
}

struct ImpvArgs {
    request: ImpvRequest,
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

fn parse_immoments_args(args: impl IntoIterator<Item = String>) -> Result<ImmomentsArgs, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let mut path = None;
    let mut outfile = None;
    let mut moments = 0;
    let mut chans = None;
    let mut includepix = None;
    let mut overwrite = false;
    let mut json = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(immoments_usage)?));
            }
            "--moments" => {
                idx += 1;
                moments = args
                    .get(idx)
                    .ok_or_else(immoments_usage)?
                    .parse::<i32>()
                    .map_err(|error| format!("invalid --moments: {error}"))?;
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(immoments_usage)?.clone());
            }
            "--includepix" => {
                idx += 1;
                includepix = Some(parse_range(args.get(idx).ok_or_else(immoments_usage)?)?);
            }
            "--overwrite" => overwrite = true,
            "--json" => json = true,
            _ if path.is_none() => path = Some(PathBuf::from(&args[idx])),
            other => {
                return Err(format!(
                    "unknown immoments argument {other:?}\n{}",
                    immoments_usage()
                ));
            }
        }
        idx += 1;
    }

    Ok(ImmomentsArgs {
        request: ImmomentsRequest {
            imagename: path.ok_or_else(immoments_usage)?,
            outfile: outfile.ok_or_else(immoments_usage)?,
            moments,
            chans,
            includepix,
            mask: None,
            overwrite,
        },
        json,
    })
}

fn immoments_usage() -> String {
    "usage: imexplore immoments <image-path> --outfile <path> [--moments -1|0|1|2|3] [--chans 4~12] [--includepix min,max] [--overwrite] [--json]".to_string()
}

fn parse_impv_args(args: impl IntoIterator<Item = String>) -> Result<ImpvArgs, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let mut path = None;
    let mut outfile = None;
    let mut mode = "coords".to_string();
    let mut start = None;
    let mut end = None;
    let mut width = 1usize;
    let mut chans = None;
    let mut overwrite = false;
    let mut json = false;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--outfile" => {
                idx += 1;
                outfile = Some(PathBuf::from(args.get(idx).ok_or_else(impv_usage)?));
            }
            "--mode" => {
                idx += 1;
                mode = args.get(idx).ok_or_else(impv_usage)?.clone();
            }
            "--start" => {
                idx += 1;
                start = Some(args.get(idx).ok_or_else(impv_usage)?.clone());
            }
            "--end" => {
                idx += 1;
                end = Some(args.get(idx).ok_or_else(impv_usage)?.clone());
            }
            "--width" => {
                idx += 1;
                width = args
                    .get(idx)
                    .ok_or_else(impv_usage)?
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --width: {error}"))?;
            }
            "--chans" => {
                idx += 1;
                chans = Some(args.get(idx).ok_or_else(impv_usage)?.clone());
            }
            "--overwrite" => overwrite = true,
            "--json" => json = true,
            _ if path.is_none() => path = Some(PathBuf::from(&args[idx])),
            other => return Err(format!("unknown impv argument {other:?}\n{}", impv_usage())),
        }
        idx += 1;
    }

    Ok(ImpvArgs {
        request: ImpvRequest {
            imagename: path.ok_or_else(impv_usage)?,
            outfile: outfile.ok_or_else(impv_usage)?,
            mode,
            start: start.ok_or_else(impv_usage)?,
            end: end.ok_or_else(impv_usage)?,
            width,
            chans,
            overwrite,
        },
        json,
    })
}

fn impv_usage() -> String {
    "usage: imexplore impv <image-path> --outfile <path> --start x,y --end x,y [--mode coords] [--width pixels] [--chans 4~12] [--overwrite] [--json]".to_string()
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

fn parse_path(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.len() != 1 {
        return Err(
            "usage: imexplore <image-path> | imexplore imhead <image-path> [--json] [--mode summary|list|put] [--hdkey bunit --hdvalue <value>] | imexplore imstat <image-path> [--box x0,y0,x1,y1] [--chans 0~4] [--json] | imexplore immoments <image-path> --outfile <path> [--json] | imexplore impv <image-path> --outfile <path> --start x,y --end x,y [--json] | imexplore --session | imexplore --json-schema | imexplore --protocol-info | imexplore --ui-schema".into(),
        );
    }
    Ok(PathBuf::from(&args[0]))
}

fn run_snapshot(path: &PathBuf) -> Result<(), String> {
    let mut session = ImageBrowserSession::open(path, ImageBrowserViewport::new(120, 40))
        .map_err(|error| error.to_string())?;
    let snapshot = session.snapshot().map_err(|error| error.to_string())?;
    println!("{}", snapshot.status_line);
    for line in snapshot.inspector_lines {
        println!("{line}");
    }
    if !snapshot.content_lines.is_empty() {
        println!();
        for line in snapshot.content_lines {
            println!("{line}");
        }
    }
    Ok(())
}

fn run_session() -> Result<(), String> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut session: Option<ImageBrowserSession> = None;

    for line_result in stdin.lock().lines() {
        let line = line_result.map_err(|error| format!("read stdin: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }

        let request: ImageBrowserRequestEnvelope = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                let response = ImageBrowserResponseEnvelope::error(
                    "invalid_json",
                    format!("parse request: {error}"),
                );
                writeln!(
                    stdout,
                    "{}",
                    serde_json::to_string(&response)
                        .map_err(|error| format!("serialize response: {error}"))?
                )
                .map_err(|error| format!("write response: {error}"))?;
                stdout
                    .flush()
                    .map_err(|error| format!("flush response: {error}"))?;
                continue;
            }
        };

        let response = if request.version != PROTOCOL_VERSION {
            ImageBrowserResponseEnvelope::error(
                "unsupported_version",
                format!(
                    "expected protocol version {}, received {}",
                    PROTOCOL_VERSION, request.version
                ),
            )
        } else {
            match (&mut session, request.command) {
                (Some(session), ImageBrowserCommand::PreviewOccurrence { request }) => {
                    match session.preview_occurrence(&request) {
                        Ok(preview) => ImageBrowserResponseEnvelope::preview(preview),
                        Err(error) => {
                            ImageBrowserResponseEnvelope::error("command_failed", error.to_string())
                        }
                    }
                }
                (Some(session), command) => match session.handle_command(command) {
                    Ok(snapshot) => ImageBrowserResponseEnvelope::snapshot(snapshot),
                    Err(error) => {
                        ImageBrowserResponseEnvelope::error("command_failed", error.to_string())
                    }
                },
                (
                    None,
                    ImageBrowserCommand::OpenRoot {
                        path,
                        viewport,
                        parameters,
                    },
                ) => {
                    match ImageBrowserSession::open_with_parameters(
                        path,
                        viewport,
                        parameters.as_ref(),
                    ) {
                        Ok(new_session) => {
                            let mut new_session = new_session;
                            let snapshot =
                                new_session.snapshot().map_err(|error| error.to_string());
                            session = Some(new_session);
                            match snapshot {
                                Ok(snapshot) => ImageBrowserResponseEnvelope::snapshot(snapshot),
                                Err(error) => {
                                    ImageBrowserResponseEnvelope::error("open_root_failed", error)
                                }
                            }
                        }
                        Err(error) => ImageBrowserResponseEnvelope::error(
                            "open_root_failed",
                            error.to_string(),
                        ),
                    }
                }
                (None, _) => ImageBrowserResponseEnvelope::error(
                    "session_not_open",
                    "send open_root before any other imexplore command",
                ),
            }
        };

        writeln!(
            stdout,
            "{}",
            serde_json::to_string(&response)
                .map_err(|error| format!("serialize response: {error}"))?
        )
        .map_err(|error| format!("write response: {error}"))?;
        stdout
            .flush()
            .map_err(|error| format!("flush response: {error}"))?;
    }

    Ok(())
}
