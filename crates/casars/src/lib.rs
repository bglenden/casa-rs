// SPDX-License-Identifier: LGPL-3.0-or-later
//! Headless `casars` task host.
//!
//! `casars run TASK ...` resolves a task's parameters through the shared
//! parameter runtime (Defaults, Last, Last Successful or a profile file, plus
//! command-line overrides), records Last state in the workspace, and runs the
//! task's provider executable from the application catalog. `casars params
//! ...` validates, shows, saves, templates and describes parameter profiles.
//! The Python package drives tasks through `casars run`.

mod execution;
mod parameters_cli;
mod registry;

use std::ffi::OsString;
use std::io::{self, Write as _};

use thiserror::Error;

/// Errors surfaced by the `casars` command.
#[derive(Debug, Error)]
pub enum CasarsError {
    /// The command line was invalid, or a task could not be resolved or run.
    #[error("{0}")]
    Command(String),
    /// Writing command output failed.
    #[error("write output: {0}")]
    Output(#[source] io::Error),
}

/// Run `casars` with its command-line arguments (without the program name).
pub fn run_with_cli_args(args: impl IntoIterator<Item = OsString>) -> Result<(), CasarsError> {
    let args = args.into_iter().collect::<Vec<_>>();
    let Some(first) = args.first() else {
        return print(&usage());
    };
    match first.to_str() {
        Some("-h" | "--help" | "help") => print(&usage()),
        Some("params" | "run") => {
            match parameters_cli::dispatch(&args).map_err(CasarsError::Command)? {
                parameters_cli::ParameterCliDispatch::Print(text) => print(&text),
                parameters_cli::ParameterCliDispatch::Done => Ok(()),
            }
        }
        Some("open") => Err(CasarsError::Command(
            "`casars open` was removed with the interactive browsers (#833); use casars.Image \
             and casars.Table from Python"
                .to_string(),
        )),
        _ => Err(CasarsError::Command(format!(
            "unknown casars command {:?}; expected run, params or --help",
            first.to_string_lossy()
        ))),
    }
}

fn print(text: &str) -> Result<(), CasarsError> {
    let mut stdout = io::stdout();
    stdout
        .write_all(text.as_bytes())
        .map_err(CasarsError::Output)?;
    stdout.flush().map_err(CasarsError::Output)
}

fn usage() -> String {
    format!(
        "casars - run casa-rs tasks through the shared parameter runtime\n\n\
Usage:\n\
  casars run TASK [SOURCE] [OVERRIDES] [RUNTIME CONTROLS]\n\
  casars params validate|show|save|template|describe ...\n\
  casars --help\n\n{}",
        parameters_cli::source_and_control_help()
    )
}
