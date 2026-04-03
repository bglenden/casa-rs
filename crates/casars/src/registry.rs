// SPDX-License-Identifier: LGPL-3.0-or-later
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::SystemTime;

#[cfg(test)]
use casacore_ms::listobs::cli::command_schema as listobs_command_schema;
use casacore_ms::listobs::cli::UiCommandSchema;
use casacore_ms::msexplore::cli::command_schema as msexplore_command_schema;

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
pub(crate) enum AppInteraction {
    OneShot,
    BrowserSession(BrowserAppKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserAppKind {
    Table,
    Image,
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

    #[cfg(test)]
    pub(crate) fn direct(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            prefix_args: Vec::new(),
        }
    }
}

impl RegistryApp {
    pub(crate) fn load_schema(&self) -> Result<UiCommandSchema, String> {
        if !self.has_explicit_binary_override() {
            match self.id {
                "msexplore" => return Ok(msexplore_command_schema("msexplore")),
                #[cfg(test)]
                "listobs" => return Ok(listobs_command_schema("listobs")),
                _ => {}
            }
        }
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

    fn has_explicit_binary_override(&self) -> bool {
        let RegistryAppKind::Subprocess {
            binary_name,
            override_env,
            ..
        } = &self.kind;
        env::var_os(override_env).is_some()
            || env::var_os(format!("CARGO_BIN_EXE_{binary_name}")).is_some()
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
            if !self.prefers_cargo_workspace_fallback_for_stale_sibling()
                || !sibling_binary_is_stale_for_current_process(&path)
            {
                return Ok(ResolvedCommand {
                    program: path.into_os_string(),
                    prefix_args: Vec::new(),
                });
            }
        }

        let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
        let manifest_path = workspace_manifest_path();
        Ok(ResolvedCommand {
            program: cargo,
            prefix_args: vec![
                OsString::from("run"),
                OsString::from("--manifest-path"),
                manifest_path.into_os_string(),
                OsString::from("-q"),
                OsString::from("-p"),
                OsString::from(cargo_package),
                OsString::from("--bin"),
                OsString::from(binary_name),
                OsString::from("--"),
            ],
        })
    }

    fn prefers_cargo_workspace_fallback_for_stale_sibling(&self) -> bool {
        matches!(self.id, "listobs" | "msexplore")
    }

    pub(crate) fn is_browser_session(&self) -> bool {
        matches!(
            self.kind,
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession(_),
                ..
            }
        )
    }

    pub(crate) fn browser_kind(&self) -> Option<BrowserAppKind> {
        match self.kind {
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession(kind),
                ..
            } => Some(kind),
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::OneShot,
                ..
            } => None,
        }
    }

    pub(crate) fn browser_path_field_id(&self) -> Option<&'static str> {
        match self.browser_kind()? {
            BrowserAppKind::Table => Some("table_path"),
            BrowserAppKind::Image => Some("image_path"),
        }
    }

    pub(crate) fn ready_status_line(&self) -> &'static str {
        match self.kind {
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession(_),
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
    match id.unwrap_or("msexplore") {
        "msexplore" => Ok(msexplore_app()),
        "tablebrowser" => Ok(tablebrowser_app()),
        "imexplore" => Ok(imexplore_app()),
        other => Err(format!(
            "unknown casars app {other:?}; expected one of: msexplore, tablebrowser, imexplore"
        )),
    }
}

pub(crate) fn registered_apps() -> Vec<RegistryApp> {
    vec![msexplore_app(), tablebrowser_app(), imexplore_app()]
}

#[cfg(test)]
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
            interaction: AppInteraction::BrowserSession(BrowserAppKind::Table),
        },
    }
}

pub(crate) fn imexplore_app() -> RegistryApp {
    RegistryApp {
        id: "imexplore",
        category: "Images",
        display_name: "ImExplore",
        kind: RegistryAppKind::Subprocess {
            binary_name: "imexplore",
            cargo_package: "casacore-images",
            override_env: "CASARS_IMEXPLORE_BIN",
            interaction: AppInteraction::BrowserSession(BrowserAppKind::Image),
        },
    }
}

pub(crate) fn msexplore_app() -> RegistryApp {
    RegistryApp {
        id: "msexplore",
        category: "MeasurementSet",
        display_name: "MSExplore",
        kind: RegistryAppKind::Subprocess {
            binary_name: "msexplore",
            cargo_package: "casacore-ms",
            override_env: "CASARS_MSEXPLORE_BIN",
            interaction: AppInteraction::OneShot,
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

fn workspace_manifest_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .unwrap_or_else(|| {
            panic!("casars manifest dir should live under <workspace>/crates/casars")
        })
        .join("Cargo.toml")
}

fn sibling_binary_is_stale_for_current_process(sibling_path: &std::path::Path) -> bool {
    let current_exe = match env::current_exe() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let current_modified = file_modified_time(&current_exe);
    let sibling_modified = file_modified_time(sibling_path);
    is_binary_stale(sibling_modified, current_modified)
}

fn file_modified_time(path: &std::path::Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

fn is_binary_stale(
    binary_modified: Option<SystemTime>,
    reference_modified: Option<SystemTime>,
) -> bool {
    match (binary_modified, reference_modified) {
        (Some(binary_modified), Some(reference_modified)) => binary_modified < reference_modified,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_ms::MsPlotPreset;
    use casacore_ms::listobs::cli::UiArgumentParser;
    use std::time::Duration;

    #[test]
    fn resolve_app_defaults_and_rejects_unknown_ids() {
        assert_eq!(resolve_app(None).unwrap().id, "msexplore");
        assert_eq!(resolve_app(Some("msexplore")).unwrap().id, "msexplore");
        assert_eq!(
            resolve_app(Some("tablebrowser")).unwrap().id,
            "tablebrowser"
        );
        assert_eq!(resolve_app(Some("imexplore")).unwrap().id, "imexplore");
        assert!(
            resolve_app(Some("bogus"))
                .unwrap_err()
                .contains("unknown casars app")
        );
    }

    #[test]
    fn app_metadata_matches_interaction_kind() {
        let msexplore = msexplore_app();
        assert!(!msexplore.is_browser_session());
        assert_eq!(
            msexplore.ready_status_line(),
            "Ready. Press r to run the selected command."
        );

        let tablebrowser = tablebrowser_app();
        assert!(tablebrowser.is_browser_session());
        assert_eq!(tablebrowser.browser_kind(), Some(BrowserAppKind::Table));
        assert_eq!(tablebrowser.browser_path_field_id(), Some("table_path"));
        assert_eq!(
            tablebrowser.ready_status_line(),
            "Ready. Press r to open the browser session."
        );

        let imexplore = imexplore_app();
        assert!(imexplore.is_browser_session());
        assert_eq!(imexplore.browser_kind(), Some(BrowserAppKind::Image));
        assert_eq!(imexplore.browser_path_field_id(), Some("image_path"));
    }

    #[test]
    fn resolve_command_prefers_override_environment() {
        let _guard = crate::test_env_lock();
        let app = msexplore_app();
        unsafe {
            env::set_var("CASARS_MSEXPLORE_BIN", "/tmp/custom-msexplore");
        }

        let resolved = app.resolve_command().expect("resolve override");
        let command = resolved.command();
        assert_eq!(command.get_program(), "/tmp/custom-msexplore");
        assert_eq!(command.get_args().count(), 0);

        unsafe {
            env::remove_var("CASARS_MSEXPLORE_BIN");
        }
    }

    #[test]
    fn resolve_command_falls_back_to_cargo_run_prefix() {
        let _guard = crate::test_env_lock();
        let app = tablebrowser_app();
        unsafe {
            env::remove_var("CASARS_TABLEBROWSER_BIN");
            env::remove_var("CARGO_BIN_EXE_tablebrowser");
            env::set_var("CARGO", "cargo");
        }
        let resolved = app.resolve_command().expect("resolve cargo fallback");
        let command = resolved.command();
        assert_eq!(command.get_program(), "cargo");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec![
                "run",
                "--manifest-path",
                workspace_manifest_path().to_string_lossy().as_ref(),
                "-q",
                "-p",
                "casacore-tables",
                "--bin",
                "tablebrowser",
                "--"
            ]
        );
        unsafe {
            env::remove_var("CARGO");
        }
    }

    #[test]
    fn msexplore_load_schema_includes_every_shipped_preset() {
        let schema = msexplore_app()
            .load_schema()
            .expect("load msexplore schema");
        let preset = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "preset")
            .expect("preset argument");
        let UiArgumentParser::Option { choices, .. } = &preset.parser else {
            panic!("preset should be an option parser");
        };
        let expected = MsPlotPreset::ALL
            .iter()
            .map(|preset| preset.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(choices, &expected);
    }

    #[test]
    fn stale_binary_detection_requires_binary_older_than_reference() {
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
        assert!(is_binary_stale(Some(older), Some(newer)));
        assert!(!is_binary_stale(Some(newer), Some(older)));
        assert!(!is_binary_stale(Some(newer), Some(newer)));
        assert!(!is_binary_stale(None, Some(newer)));
        assert!(!is_binary_stale(Some(older), None));
    }

    #[test]
    fn workspace_ms_apps_prefer_cargo_fallback_for_stale_siblings() {
        assert!(msexplore_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(!tablebrowser_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(!imexplore_app().prefers_cargo_workspace_fallback_for_stale_sibling());
    }
}
