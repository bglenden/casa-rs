// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - inspect persistent casacore images from the command line.

use std::env;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use casa_images::{ImageBrowserSession, imexplore_ui_schema_json};
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

    let image_path = parse_path(args)?;
    run_snapshot(&image_path)
}

fn parse_path(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.len() != 1 {
        return Err(
            "usage: imexplore <image-path> | imexplore --session | imexplore --json-schema | imexplore --protocol-info | imexplore --ui-schema".into(),
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
