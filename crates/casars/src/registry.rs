// SPDX-License-Identifier: LGPL-3.0-or-later
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

use casacore_ms::listobs::cli::UiCommandSchema;

#[derive(Debug, Clone)]
pub(crate) struct RegistryApp {
    pub id: &'static str,
    pub category: &'static str,
    pub display_name: &'static str,
    binary_name: &'static str,
    cargo_package: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedCommand {
    program: OsString,
    prefix_args: Vec<OsString>,
}

impl ResolvedCommand {
    pub(crate) fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.prefix_args);
        command
    }
}

impl RegistryApp {
    pub(crate) fn load_schema(&self) -> Result<UiCommandSchema, String> {
        let resolved = self.resolve_command()?;
        let output = resolved
            .command()
            .arg("--ui-schema")
            .output()
            .map_err(|error| format!("spawn {} --ui-schema: {error}", self.binary_name))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "{} --ui-schema exited with {}: {}",
                self.binary_name,
                output.status,
                stderr.trim()
            ));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("parse {} --ui-schema output: {error}", self.binary_name))
    }

    pub(crate) fn resolve_command(&self) -> Result<ResolvedCommand, String> {
        if let Some(path) = env::var_os("CASARS_LISTOBS_BIN") {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = env::var_os(format!("CARGO_BIN_EXE_{}", self.binary_name)) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = sibling_binary(self.binary_name) {
            return Ok(ResolvedCommand {
                program: path.into_os_string(),
                prefix_args: Vec::new(),
            });
        }

        let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
        Ok(ResolvedCommand {
            program: cargo,
            prefix_args: vec![
                OsString::from("run"),
                OsString::from("-q"),
                OsString::from("-p"),
                OsString::from(self.cargo_package),
                OsString::from("--bin"),
                OsString::from(self.binary_name),
                OsString::from("--"),
            ],
        })
    }
}

pub(crate) fn listobs_app() -> RegistryApp {
    RegistryApp {
        id: "listobs",
        category: "MeasurementSet",
        display_name: "ListObs",
        binary_name: "listobs",
        cargo_package: "casacore-ms",
    }
}

fn sibling_binary(binary_name: &str) -> Option<PathBuf> {
    let mut path = env::current_exe().ok()?;
    path.pop();
    path.push(binary_name);
    path.set_extension(env::consts::EXE_EXTENSION);
    if path.exists() { Some(path) } else { None }
}
