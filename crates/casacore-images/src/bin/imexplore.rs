// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - inspect persistent casacore images from the command line.

use std::env;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use casacore_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserRequestEnvelope, ImageBrowserResponseEnvelope,
    ImageBrowserViewport, PROTOCOL_VERSION,
};
use casacore_images::ImageBrowserSession;
use serde_json::json;

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
    if args.peek().is_some_and(|arg| arg == "--ui-schema") {
        print!("{}", ui_schema_json()?);
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

    let image_path = parse_path(args)?;
    run_snapshot(&image_path)
}

fn parse_path(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.len() != 1 {
        return Err(
            "usage: imexplore <image-path> | imexplore --session | imexplore --ui-schema".into(),
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

fn ui_schema_json() -> Result<String, String> {
    let schema = json!({
        "schema_version": 1,
        "command_id": "imexplore",
        "invocation_name": "imexplore",
        "display_name": "ImExplore",
        "category": "Images",
        "summary": "browse persistent casacore images",
        "usage": "imexplore <image-path>",
        "arguments": [
            {
                "id": "image_path",
                "label": "Image Path",
                "order": 0,
                "parser": {
                    "kind": "positional",
                    "metavar": "image-path"
                },
                "value_kind": "path",
                "required": true,
                "default": null,
                "help": "Path to the casacore image root directory",
                "group": "Input",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "blc",
                "label": "BLC",
                "order": 1,
                "parser": {
                    "kind": "option",
                    "flags": ["--blc"],
                    "metavar": "BLC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive bottom-left-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "trc",
                "label": "TRC",
                "order": 2,
                "parser": {
                    "kind": "option",
                    "flags": ["--trc"],
                    "metavar": "TRC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive top-right-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "inc",
                "label": "INC",
                "order": 3,
                "parser": {
                    "kind": "option",
                    "flags": ["--inc"],
                    "metavar": "INC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated per-axis pixel increments",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "stretch",
                "label": "Stretch",
                "order": 4,
                "parser": {
                    "kind": "option",
                    "flags": ["--stretch"],
                    "metavar": "STRETCH",
                    "choices": ["percentile99", "percentile95", "minmax", "zscale", "manual"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "percentile99",
                "help": "Plane stretch preset",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "autoscale",
                "label": "Autoscale",
                "order": 5,
                "parser": {
                    "kind": "option",
                    "flags": ["--autoscale"],
                    "metavar": "AUTOSCALE",
                    "choices": ["per_plane", "frozen"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "per_plane",
                "help": "Whether clip bounds update per plane or stay frozen while stepping cubes",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_low",
                "label": "Clip Low",
                "order": 6,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-low"],
                    "metavar": "LOW",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual lower clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_high",
                "label": "Clip High",
                "order": 7,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-high"],
                    "metavar": "HIGH",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual upper clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "fps",
                "label": "FPS",
                "order": 8,
                "parser": {
                    "kind": "option",
                    "flags": ["--fps"],
                    "metavar": "FPS",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "1",
                "help": "Movie playback frames per second",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "help",
                "label": "Help",
                "order": 9,
                "parser": {
                    "kind": "action",
                    "flags": ["-h", "--help"],
                    "action": "help"
                },
                "value_kind": "none",
                "required": false,
                "default": null,
                "help": "Print this help message",
                "group": "Input",
                "advanced": true,
                "hidden_in_tui": true
            }
        ],
        "managed_output": null
    });
    serde_json::to_string_pretty(&schema).map_err(|error| format!("serialize ui schema: {error}"))
}
