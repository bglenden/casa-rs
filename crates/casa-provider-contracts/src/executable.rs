// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where a task's executable is found.
//!
//! Python starts each task's executable directly (ADR-0017), so one search
//! decides which file runs. It looks, in order, at:
//!
//! 1. the task's override, `CASARS_<APP>_BIN`
//!    ([`ApplicationLaunchDescriptor::override_env`]);
//! 2. a suite root: `CASARS_SUITE_ROOT`, or else the installed suite the
//!    caller belongs to (see [`installed_suite_root`]), whose `bin/` holds
//!    the executable;
//! 3. a development checkout: the built executable under the workspace this
//!    library was built in, in `$CARGO_TARGET_DIR` or `target/`.
//!
//! The first step that applies decides. A step that applies but finds no
//! file is an error naming what is missing; it never falls through to a
//! later step, so an installed suite never runs a checkout's build. Nothing
//! is built or run through `cargo`: a checkout's missing executable is
//! reported with the `cargo build` that makes it. The caller's own path
//! (`current_exe`) is not consulted; from Python it is the interpreter.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::ApplicationLaunchDescriptor;

/// The environment variable that names an installed suite's root.
pub const SUITE_ROOT_ENV: &str = "CASARS_SUITE_ROOT";

/// The file that marks an installed suite's root (`scripts/install-suite.sh`).
const SUITE_MANIFEST: &str = "bundle-manifest.json";

/// The workspace this library was built in, fixed at build time.
const BUILD_WORKSPACE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

/// Find the executable `launch` names.
///
/// `environment` reads one environment variable; a caller passes its own
/// process environment, or a test its own. `installed_suite` is the root of
/// the installed suite the caller belongs to, when it belongs to one: from
/// Python, [`installed_suite_root`] of `sys.prefix`. The search order is the
/// module's. The path found is absolute: a relative override or suite root
/// names a file in the caller's working directory, wherever the task then
/// runs. The error says which step applied and what it lacked.
pub fn resolve_task_executable(
    launch: &ApplicationLaunchDescriptor,
    environment: &dyn Fn(&str) -> Option<OsString>,
    installed_suite: Option<&Path>,
) -> Result<PathBuf, String> {
    let caller =
        std::env::current_dir().map_err(|error| format!("read the working directory: {error}"))?;
    resolve_in(
        launch,
        environment,
        installed_suite,
        development_workspace(Path::new(BUILD_WORKSPACE)).as_deref(),
        &caller,
    )
}

/// The root of the installed suite whose Python environment is `prefix`, if
/// it is one: the installer puts the environment at `<root>/python`, beside
/// `<root>/bin` and the bundle manifest.
pub fn installed_suite_root(prefix: &Path) -> Option<PathBuf> {
    let root = prefix.parent()?;
    (prefix.file_name()? == "python" && root.join(SUITE_MANIFEST).is_file())
        .then(|| root.to_path_buf())
}

/// [`resolve_task_executable`] with the development checkout and the
/// caller's working directory given.
fn resolve_in(
    launch: &ApplicationLaunchDescriptor,
    environment: &dyn Fn(&str) -> Option<OsString>,
    installed_suite: Option<&Path>,
    development: Option<&Path>,
    caller: &Path,
) -> Result<PathBuf, String> {
    let name = format!("{}{}", launch.executable, std::env::consts::EXE_SUFFIX);
    if let Some(path) = environment(&launch.override_env).filter(|path| !path.is_empty()) {
        let path = caller.join(path);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "{} names {}, which is not a file",
                launch.override_env,
                path.display()
            ))
        };
    }
    let suite = environment(SUITE_ROOT_ENV)
        .filter(|root| !root.is_empty())
        .map(|root| (caller.join(root), SUITE_ROOT_ENV))
        .or_else(|| installed_suite.map(|root| (caller.join(root), "the installed suite")));
    if let Some((root, source)) = suite {
        let path = root.join("bin").join(&name);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "the casa-rs suite at {} ({source}) has no {name} in bin/; reinstall the \
                 suite, or set {} to the executable",
                root.display(),
                launch.override_env
            ))
        };
    }
    if let Some(workspace) = development {
        let target = match environment("CARGO_TARGET_DIR").filter(|dir| !dir.is_empty()) {
            Some(dir) => workspace.join(dir),
            None => workspace.join("target"),
        };
        return newest_build(&target, &name).ok_or_else(|| {
            format!(
                "{name} is not built in this development checkout ({}); build it with \
                 `cargo build -p {} --bin {}`, or set {} to the executable",
                target.display(),
                launch.cargo_package,
                launch.executable,
                launch.override_env
            )
        });
    }
    Err(format!(
        "no {name} for this task: set {SUITE_ROOT_ENV} to an installed casa-rs suite, or {} \
         to the executable (docs/install.md)",
        launch.override_env
    ))
}

/// `workspace`, made absolute, when it is the casa-rs checkout this library
/// was built in.
fn development_workspace(workspace: &Path) -> Option<PathBuf> {
    workspace
        .join("crates/casa-provider-contracts/Cargo.toml")
        .is_file()
        .then(|| workspace.canonicalize().ok())
        .flatten()
}

/// The most recently built of `target`'s debug and release `name`.
fn newest_build(target: &Path, name: &str) -> Option<PathBuf> {
    let modified = |path: &Path| -> Option<SystemTime> {
        let metadata = path.metadata().ok()?;
        metadata
            .is_file()
            .then(|| metadata.modified().ok())
            .flatten()
    };
    ["debug", "release"]
        .into_iter()
        .map(|profile| target.join(profile).join(name))
        .filter_map(|path| Some((modified(&path)?, path)))
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
}

#[cfg(test)]
#[path = "executable_tests.rs"]
mod tests;
