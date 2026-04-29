// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - inspect persistent casacore images from the command line.

use std::env;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use casa_images::{ImageBrowserSession, imexplore_ui_schema_json, imhead, imstat};
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
    let mut args = env::args().skip(1).peekable();
    if args.peek().is_some_and(|arg| arg == "--json-schema") {
        print!(
            "{}",
            schema_bundle_json(ui_schema_value()?)
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
        let image_path = parse_path_allowing_json(args)?;
        let summary = imhead(&image_path.path).map_err(|error| error.to_string())?;
        if image_path.json {
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
            stat_args.chans.as_deref(),
            None,
        )
        .map_err(|error| error.to_string())?;
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

    let image_path = parse_path(args)?;
    run_snapshot(&image_path)
}

struct PathJson {
    path: PathBuf,
    json: bool,
}

fn parse_path_allowing_json(args: impl IntoIterator<Item = String>) -> Result<PathJson, String> {
    let mut path = None;
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            _ if path.is_none() => path = Some(PathBuf::from(arg)),
            _ => return Err("usage: imexplore imhead <image-path> [--json]".to_string()),
        }
    }
    Ok(PathJson {
        path: path.ok_or_else(|| "usage: imexplore imhead <image-path> [--json]".to_string())?,
        json,
    })
}

struct ImstatArgs {
    path: PathBuf,
    box_pixels: Option<String>,
    chans: Option<String>,
    json: bool,
}

fn parse_imstat_args(args: impl IntoIterator<Item = String>) -> Result<ImstatArgs, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let mut path = None;
    let mut box_pixels = None;
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
        chans,
        json,
    })
}

fn imstat_usage() -> String {
    "usage: imexplore imstat <image-path> [--box x0,y0,x1,y1] [--chans 0~4] [--json]".to_string()
}

fn parse_path(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.len() != 1 {
        return Err(
            "usage: imexplore <image-path> | imexplore imhead <image-path> [--json] | imexplore imstat <image-path> [--box x0,y0,x1,y1] [--chans 0~4] [--json] | imexplore --session | imexplore --json-schema | imexplore --protocol-info | imexplore --ui-schema".into(),
        );
    }
    Ok(PathBuf::from(&args[0]))
}

fn ui_schema_value() -> Result<serde_json::Value, String> {
    serde_json::from_str(&imexplore_ui_schema_json("imexplore")?)
        .map_err(|error| format!("parse imexplore ui schema: {error}"))
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
