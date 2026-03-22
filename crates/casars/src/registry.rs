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
    kind: RegistryAppKind,
}

#[derive(Debug, Clone)]
enum RegistryAppKind {
    Subprocess {
        binary_name: &'static str,
        cargo_package: &'static str,
        override_env: &'static str,
        interaction: AppInteraction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppInteraction {
    OneShot,
    BrowserSession,
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
        match &self.kind {
            RegistryAppKind::Subprocess { binary_name, .. } => {
                let resolved = self.resolve_command()?;
                let output = resolved
                    .command()
                    .arg("--ui-schema")
                    .output()
                    .map_err(|error| format!("spawn {binary_name} --ui-schema: {error}"))?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(format!(
                        "{binary_name} --ui-schema exited with {}: {}",
                        output.status,
                        stderr.trim()
                    ));
                }
                serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse {binary_name} --ui-schema output: {error}"))
            }
        }
    }

    pub(crate) fn resolve_command(&self) -> Result<ResolvedCommand, String> {
        let RegistryAppKind::Subprocess {
            binary_name,
            cargo_package,
            override_env,
            ..
        } = &self.kind;

        if let Some(path) = env::var_os(override_env) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = env::var_os(format!("CARGO_BIN_EXE_{binary_name}")) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = sibling_binary(binary_name) {
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
                OsString::from(cargo_package),
                OsString::from("--bin"),
                OsString::from(binary_name),
                OsString::from("--"),
            ],
        })
    }

    pub(crate) fn is_browser_session(&self) -> bool {
        matches!(
            self.kind,
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession,
                ..
            }
        )
    }

    pub(crate) fn ready_status_line(&self) -> &'static str {
        match self.kind {
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession,
                ..
            } => "Ready. Press r to open the browser session.",
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::OneShot,
                ..
            } => "Ready. Press r to run the selected command.",
        }
    }
}

pub(crate) fn resolve_app(id: Option<&str>) -> Result<RegistryApp, String> {
    match id.unwrap_or("listobs") {
        "listobs" => Ok(listobs_app()),
        "tablebrowser" => Ok(tablebrowser_app()),
        other => Err(format!(
            "unknown casars app {other:?}; expected one of: listobs, tablebrowser"
        )),
    }
}

pub(crate) fn registered_apps() -> Vec<RegistryApp> {
    vec![listobs_app(), tablebrowser_app()]
}

pub(crate) fn listobs_app() -> RegistryApp {
    RegistryApp {
        id: "listobs",
        category: "MeasurementSet",
        display_name: "ListObs",
        kind: RegistryAppKind::Subprocess {
            binary_name: "listobs",
            cargo_package: "casacore-ms",
            override_env: "CASARS_LISTOBS_BIN",
            interaction: AppInteraction::OneShot,
        },
    }
}

pub(crate) fn tablebrowser_app() -> RegistryApp {
    RegistryApp {
        id: "tablebrowser",
        category: "Tables",
        display_name: "Table Browser",
        kind: RegistryAppKind::Subprocess {
            binary_name: "tablebrowser",
            cargo_package: "casacore-tables",
            override_env: "CASARS_TABLEBROWSER_BIN",
            interaction: AppInteraction::BrowserSession,
        },
    }
}

fn sibling_binary(binary_name: &str) -> Option<PathBuf> {
    let mut path = env::current_exe().ok()?;
    path.pop();
    path.push(binary_name);
    path.set_extension(env::consts::EXE_EXTENSION);
    if path.exists() { Some(path) } else { None }
}
