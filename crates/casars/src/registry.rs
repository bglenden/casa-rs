// SPDX-License-Identifier: LGPL-3.0-or-later
//! Executable routing projected from the shared application catalog.

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

use casa_provider_contracts::{
    ApplicationDefinition, ApplicationKind, ApplicationLaunchMode, builtin_application_catalog,
};

/// A task in the application catalog and the executable that provides it.
#[derive(Debug, Clone)]
pub(crate) struct RegistryApp {
    pub id: String,
    binary_name: String,
    cargo_package: String,
    override_env: String,
}

fn registry_app_from_catalog(entry: &ApplicationDefinition) -> Option<RegistryApp> {
    (entry.kind == ApplicationKind::Task).then(|| RegistryApp {
        id: entry.id.clone(),
        binary_name: entry.launch.executable.clone(),
        cargo_package: entry.launch.cargo_package.clone(),
        override_env: entry.launch.override_env.clone(),
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

/// A program and the arguments that precede the task's own arguments.
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
    /// Resolve the provider executable: the `CASARS_<APP>_BIN` override
    /// first, then the launch mode (an installed suite's sibling binary, or
    /// `cargo run` in a development workspace).
    pub(crate) fn resolve_command(&self) -> Result<ResolvedCommand, String> {
        if let Some(path) = env::var_os(&self.override_env) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }
        match configured_launch_mode()? {
            ApplicationLaunchMode::InstalledSuite => {
                let path = sibling_binary(&self.binary_name).ok_or_else(|| {
                    format!(
                        "installed-suite executable {:?} is missing beside the casars launcher; set {} to its installed path",
                        self.binary_name, self.override_env
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
                        OsString::from(&self.cargo_package),
                        OsString::from("--bin"),
                        OsString::from(&self.binary_name),
                        OsString::from("--"),
                    ],
                })
            }
        }
    }
}

/// The catalog task `id`.
pub(crate) fn resolve_app(id: &str) -> Result<RegistryApp, String> {
    registered_apps()
        .into_iter()
        .find(|app| app.id == id)
        .ok_or_else(|| {
            let expected = registered_apps()
                .into_iter()
                .map(|app| app.id)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown casars task {id:?}; expected one of: {expected}")
        })
}

/// Every task in the built-in application catalog.
pub(crate) fn registered_apps() -> Vec<RegistryApp> {
    builtin_application_catalog()
        .expect("built-in application catalog should validate")
        .applications
        .iter()
        .filter_map(registry_app_from_catalog)
        .collect()
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
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn every_catalog_task_has_a_parameter_surface() {
        let apps = registered_apps();
        assert_eq!(apps.len(), 27);
        for app in apps {
            let bundle = casa_provider_contracts::builtin_surface_bundle(&app.id)
                .unwrap_or_else(|error| panic!("{}: {error}", app.id));
            assert_eq!(bundle.surface.id(), app.id);
        }
    }

    #[test]
    fn surfaces_may_share_one_provider_executable() {
        assert_eq!(
            resolve_app("split").unwrap().binary_name,
            resolve_app("mstransform").unwrap().binary_name
        );
        assert_eq!(
            resolve_app("plotms").unwrap().binary_name,
            resolve_app("msexplore").unwrap().binary_name
        );
        assert_eq!(resolve_app("imstat").unwrap().binary_name, "imexplore");
    }

    #[test]
    fn unknown_tasks_are_rejected_with_the_catalog_list() {
        let error = resolve_app("tablebrowser").unwrap_err();
        assert!(error.contains("unknown casars task"), "{error}");
        assert!(error.contains("imager"), "{error}");
    }

    #[test]
    fn command_override_remains_runtime_only() {
        let app = resolve_app("imager").unwrap();
        let variable = "CASARS_IMAGER_BIN";
        let _guard = env_lock();
        // SAFETY: the test holds the module's environment lock.
        unsafe { env::set_var("CASARS_LAUNCH_MODE", "installed_suite") };
        // SAFETY: the test holds the module's environment lock.
        unsafe { env::set_var(variable, "/tmp/canonical-imager") };
        let command = app.resolve_command().unwrap().command();
        assert_eq!(command.get_program(), "/tmp/canonical-imager");
        // SAFETY: the test holds the module's environment lock.
        unsafe { env::remove_var(variable) };
        // SAFETY: the test holds the module's environment lock.
        unsafe { env::remove_var("CASARS_LAUNCH_MODE") };
    }

    #[test]
    fn invalid_launch_mode_fails_without_searching_or_switching_modes() {
        let app = resolve_app("imager").unwrap();
        let _guard = env_lock();
        // SAFETY: the test holds the module's environment lock.
        unsafe {
            env::remove_var("CASARS_IMAGER_BIN");
            env::set_var("CASARS_LAUNCH_MODE", "search_everywhere");
        }
        let error = app.resolve_command().unwrap_err();
        assert!(error.contains("invalid CASARS_LAUNCH_MODE"), "{error}");
        // SAFETY: the test holds the module's environment lock.
        unsafe { env::remove_var("CASARS_LAUNCH_MODE") };
    }
}
