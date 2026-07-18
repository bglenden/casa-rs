// SPDX-License-Identifier: LGPL-3.0-or-later
//! Executable routing projected from the shared inventory and parameter catalog.

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

use casa_ms::presentation::UiCommandSchema;
use casa_provider_contracts::{
    ApplicationBrowserKind, ApplicationDefinition, ApplicationInteraction, ApplicationKind,
    ApplicationLaunchMode, ApplicationShellKind, builtin_application_catalog,
    builtin_surface_bundle,
};

#[derive(Debug, Clone)]
pub(crate) struct RegistryApp {
    pub id: String,
    pub category: String,
    pub display_name: String,
    shell_kind: AppShellKind,
    kind: RegistryAppKind,
}

#[derive(Debug, Clone)]
enum RegistryAppKind {
    Subprocess {
        binary_name: String,
        cargo_package: String,
        override_env: String,
        interaction: AppInteraction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppInteraction {
    OneShot,
    BrowserSession(BrowserAppKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppShellKind {
    Inspect,
    Browser,
    Workflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserAppKind {
    Table,
    Image,
}

fn registry_app_from_catalog(entry: &ApplicationDefinition) -> Option<RegistryApp> {
    if !entry.show_in_tui || entry.kind != ApplicationKind::Task {
        return None;
    }
    let surface = entry.surface_bundle().ok()??;
    let shell_kind = match entry.shell_kind {
        ApplicationShellKind::Inspect => AppShellKind::Inspect,
        ApplicationShellKind::Browser => AppShellKind::Browser,
        ApplicationShellKind::Workflow => AppShellKind::Workflow,
        ApplicationShellKind::Launcher => return None,
    };
    let interaction = match entry.interaction {
        ApplicationInteraction::OneShot => AppInteraction::OneShot,
        ApplicationInteraction::BrowserSession => {
            let browser_kind = match entry.browser_kind {
                Some(ApplicationBrowserKind::Table) => BrowserAppKind::Table,
                Some(ApplicationBrowserKind::Image) => BrowserAppKind::Image,
                _ => return None,
            };
            AppInteraction::BrowserSession(browser_kind)
        }
        ApplicationInteraction::Launcher => return None,
    };
    Some(RegistryApp {
        id: entry.id.clone(),
        category: surface.surface.category().to_string(),
        display_name: surface.surface.display_name().to_string(),
        shell_kind,
        kind: RegistryAppKind::Subprocess {
            binary_name: entry.launch.executable.clone(),
            cargo_package: entry.launch.cargo_package.clone(),
            override_env: entry.launch.override_env.clone(),
            interaction,
        },
    })
}

fn configured_launch_mode() -> Result<ApplicationLaunchMode, String> {
    match env::var("CASARS_LAUNCH_MODE") {
        Ok(value) if value == "installed_suite" => Ok(ApplicationLaunchMode::InstalledSuite),
        Ok(value) if value == "development_workspace" => {
            Ok(ApplicationLaunchMode::DevelopmentWorkspace)
        }
        Ok(value) => Err(format!(
            "invalid CASARS_LAUNCH_MODE {value:?}; expected installed_suite or development_workspace"
        )),
        Err(env::VarError::NotPresent) if cfg!(debug_assertions) => {
            Ok(ApplicationLaunchMode::DevelopmentWorkspace)
        }
        Err(env::VarError::NotPresent) => Ok(ApplicationLaunchMode::InstalledSuite),
        Err(error) => Err(format!("read CASARS_LAUNCH_MODE: {error}")),
    }
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
    /// Derive the form schema from the canonical built-in surface definition.
    /// Provider subprocesses no longer supply a fallback schema or alias table.
    pub(crate) fn load_schema(&self) -> Result<UiCommandSchema, String> {
        let bundle = builtin_surface_bundle(&self.id)?;
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .map_err(|error| format!("project {} parameter definition: {error}", self.id))
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
        match configured_launch_mode()? {
            ApplicationLaunchMode::InstalledSuite => {
                let path = sibling_binary(binary_name).ok_or_else(|| {
                    format!(
                        "installed-suite executable {binary_name:?} is missing beside the casars launcher; set {override_env} to its installed path"
                    )
                })?;
                Ok(ResolvedCommand {
                    program: path.into_os_string(),
                    prefix_args: Vec::new(),
                })
            }
            ApplicationLaunchMode::DevelopmentWorkspace => {
                let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
                Ok(ResolvedCommand {
                    program: cargo,
                    prefix_args: vec![
                        OsString::from("run"),
                        OsString::from("--manifest-path"),
                        workspace_manifest_path().into_os_string(),
                        OsString::from("-q"),
                        OsString::from("-p"),
                        OsString::from(cargo_package),
                        OsString::from("--bin"),
                        OsString::from(binary_name),
                        OsString::from("--"),
                    ],
                })
            }
        }
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

    pub(crate) fn shell_kind(&self) -> AppShellKind {
        self.shell_kind
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
            BrowserAppKind::Table => Some("table"),
            BrowserAppKind::Image => Some("image"),
        }
    }

    pub(crate) fn ready_status_line(&self) -> &'static str {
        match (self.shell_kind, &self.kind) {
            (
                AppShellKind::Browser,
                RegistryAppKind::Subprocess {
                    interaction: AppInteraction::BrowserSession(_),
                    ..
                },
            ) => "Ready. Press r to open the browser session.",
            (AppShellKind::Inspect, _) => {
                "Ready. Press r to run and refresh the current inspection view."
            }
            (AppShellKind::Workflow, _) => "Ready. Press r to run the selected workflow stage.",
            (_, _) => "Ready. Press r to run the selected command.",
        }
    }
}

pub(crate) fn resolve_app(id: Option<&str>) -> Result<RegistryApp, String> {
    let requested = id.unwrap_or("msexplore");
    registered_apps()
        .into_iter()
        .find(|app| app.id == requested)
        .ok_or_else(|| {
            let expected = registered_apps()
                .into_iter()
                .map(|app| app.id)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown casars app {requested:?}; expected one of: {expected}")
        })
}

pub(crate) fn registered_apps() -> Vec<RegistryApp> {
    builtin_application_catalog()
        .expect("built-in application catalog should validate")
        .applications
        .iter()
        .filter_map(registry_app_from_catalog)
        .collect()
}

#[cfg(test)]
pub(crate) fn calibrate_app() -> RegistryApp {
    resolve_app(Some("calibrate")).expect("calibrate should be in task catalog")
}

#[cfg(test)]
pub(crate) fn importvla_app() -> RegistryApp {
    resolve_app(Some("importvla")).expect("importvla should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imager_app() -> RegistryApp {
    resolve_app(Some("imager")).expect("imager should be in task catalog")
}

#[cfg(test)]
pub(crate) fn tablebrowser_app() -> RegistryApp {
    resolve_app(Some("tablebrowser")).expect("tablebrowser should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imexplore_app() -> RegistryApp {
    resolve_app(Some("imexplore")).expect("imexplore should be in task catalog")
}

#[cfg(test)]
pub(crate) fn msexplore_app() -> RegistryApp {
    resolve_app(Some("msexplore")).expect("msexplore should be in task catalog")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_and_parameter_catalog_resolve_the_same_42_surfaces() {
        let apps = registered_apps();
        assert_eq!(apps.len(), 42);
        for app in apps {
            let schema = app.load_schema().expect("canonical surface projection");
            assert_eq!(schema.command_id, app.id);
        }
    }

    #[test]
    fn schema_projection_uses_casa_names_without_alias_enrichment() {
        let imager = imager_app().load_schema().unwrap();
        assert!(imager.argument("vis").is_some());
        assert!(imager.argument("cell").is_some());
        assert!(imager.argument("ms").is_none());
        assert!(imager.argument("cell_arcsec").is_none());

        // These catalog surfaces intentionally share executable routes without
        // a provider surface selector, so frontend forms are projected directly
        // from each canonical surface rather than inferred from a family binary.
        let split_app = resolve_app(Some("split")).unwrap();
        let mstransform_app = resolve_app(Some("mstransform")).unwrap();
        let RegistryAppKind::Subprocess {
            binary_name: split_binary,
            ..
        } = &split_app.kind;
        let RegistryAppKind::Subprocess {
            binary_name: mstransform_binary,
            ..
        } = &mstransform_app.kind;
        assert_eq!(split_binary, mstransform_binary);
        let split = split_app.load_schema().unwrap();
        let mstransform = mstransform_app.load_schema().unwrap();
        assert_eq!(split.command_id, "split");
        assert_eq!(mstransform.command_id, "mstransform");
        assert!(split.argument("vis").is_some());

        let plotms_app = resolve_app(Some("plotms")).unwrap();
        let msexplore_app = resolve_app(Some("msexplore")).unwrap();
        let RegistryAppKind::Subprocess {
            binary_name: plotms_binary,
            ..
        } = &plotms_app.kind;
        let RegistryAppKind::Subprocess {
            binary_name: msexplore_binary,
            ..
        } = &msexplore_app.kind;
        assert_eq!(plotms_binary, msexplore_binary);
        let plotms = plotms_app.load_schema().unwrap();
        let msexplore = msexplore_app.load_schema().unwrap();
        assert_eq!(plotms.command_id, "plotms");
        assert_eq!(msexplore.command_id, "msexplore");
    }

    #[test]
    fn session_metadata_uses_canonical_path_fields() {
        let table = tablebrowser_app();
        assert!(table.is_browser_session());
        assert_eq!(table.browser_path_field_id(), Some("table"));
        let image = imexplore_app();
        assert_eq!(image.browser_path_field_id(), Some("image"));
    }

    #[test]
    fn command_override_remains_runtime_only() {
        let app = imager_app();
        let variable = "CASARS_IMAGER_BIN";
        let _guard = crate::test_env_lock();
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe { env::set_var("CASARS_LAUNCH_MODE", "installed_suite") };
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe { env::set_var(variable, "/tmp/canonical-imager") };
        let command = app.resolve_command().unwrap().command();
        assert_eq!(command.get_program(), "/tmp/canonical-imager");
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe { env::remove_var(variable) };
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe { env::remove_var("CASARS_LAUNCH_MODE") };
    }

    #[test]
    fn invalid_launch_mode_fails_without_searching_or_switching_modes() {
        let app = imager_app();
        let _guard = crate::test_env_lock();
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe {
            env::remove_var("CASARS_IMAGER_BIN");
            env::set_var("CASARS_LAUNCH_MODE", "search_everywhere");
        }
        let error = app.resolve_command().unwrap_err();
        assert!(error.contains("invalid CASARS_LAUNCH_MODE"), "{error}");
        // SAFETY: the test holds the crate-wide environment lock.
        unsafe { env::remove_var("CASARS_LAUNCH_MODE") };
    }
}
