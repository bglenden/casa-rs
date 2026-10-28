// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime};

use super::*;

fn imager() -> ApplicationLaunchDescriptor {
    ApplicationLaunchDescriptor {
        executable: "casars-imager".to_string(),
        cargo_package: "casars-imager".to_string(),
        override_env: "CASARS_IMAGER_BIN".to_string(),
    }
}

fn name() -> String {
    format!("casars-imager{}", std::env::consts::EXE_SUFFIX)
}

/// The imager's executable, for a caller in `/caller`, which the absolute
/// paths these tests name do not depend on.
fn resolve(
    environment: &dyn Fn(&str) -> Option<OsString>,
    installed_suite: Option<&Path>,
    development: Option<&Path>,
) -> Result<PathBuf, String> {
    resolve_in(
        &imager(),
        environment,
        installed_suite,
        development,
        Path::new("/caller"),
    )
}

/// An environment holding `values`.
fn environment(values: &[(&str, &Path)]) -> impl Fn(&str) -> Option<OsString> + use<> {
    let values = values
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.as_os_str().to_os_string()))
        .collect::<BTreeMap<_, _>>();
    move |name| values.get(name).cloned()
}

/// Write an empty file at `path`, with its parents.
fn touch(path: &Path) -> PathBuf {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, "").unwrap();
    path.to_path_buf()
}

/// A suite root as the installer lays it out, with `executables` in `bin/`.
fn suite(root: &Path, executables: &[&str]) -> PathBuf {
    touch(&root.join(SUITE_MANIFEST));
    fs::create_dir_all(root.join("python")).unwrap();
    fs::create_dir_all(root.join("bin")).unwrap();
    for executable in executables {
        touch(&root.join("bin").join(executable));
    }
    root.to_path_buf()
}

#[test]
fn the_override_wins_and_must_name_a_file() {
    let scratch = tempfile::tempdir().unwrap();
    let suite = suite(&scratch.path().join("suite"), &[&name()]);
    let chosen = touch(&scratch.path().join("elsewhere/imager"));
    let env = environment(&[("CASARS_IMAGER_BIN", &chosen), (SUITE_ROOT_ENV, &suite)]);
    assert_eq!(resolve(&env, Some(&suite), None).unwrap(), chosen);

    let missing = scratch.path().join("missing");
    let env = environment(&[("CASARS_IMAGER_BIN", &missing), (SUITE_ROOT_ENV, &suite)]);
    let error = resolve(&env, Some(&suite), None).unwrap_err();
    assert!(error.contains("CASARS_IMAGER_BIN names"), "{error}");
    assert!(error.contains("not a file"), "{error}");
}

#[test]
fn a_relative_override_or_suite_root_names_a_file_where_the_caller_is() {
    // Astra's #974 P1: a relative path was returned as given, and the task,
    // started in its own workspace, then ran another file or none.
    let caller = tempfile::tempdir().unwrap();
    let chosen = touch(&caller.path().join("bin/imager"));
    let suite = suite(&caller.path().join("suite"), &[&name()]);
    let resolve_from = |values: &[(&str, &Path)]| {
        resolve_in(&imager(), &environment(values), None, None, caller.path())
    };

    let found = resolve_from(&[("CASARS_IMAGER_BIN", Path::new("bin/imager"))]).unwrap();
    assert!(found.is_absolute(), "{found:?}");
    assert_eq!(found, chosen);
    // A bare file name is the file in the caller's directory, never one on
    // PATH.
    let error = resolve_from(&[("CASARS_IMAGER_BIN", Path::new("imager"))]).unwrap_err();
    assert!(
        error.contains(&caller.path().display().to_string()),
        "{error}"
    );

    let found = resolve_from(&[(SUITE_ROOT_ENV, Path::new("suite"))]).unwrap();
    assert!(found.is_absolute(), "{found:?}");
    assert_eq!(found, suite.join("bin").join(name()));
}

#[test]
fn a_suite_root_is_searched_alone() {
    let scratch = tempfile::tempdir().unwrap();
    let named = suite(&scratch.path().join("named"), &[&name()]);
    let installed = suite(&scratch.path().join("installed"), &[&name()]);
    let checkout = scratch.path().join("checkout");
    touch(&checkout.join("target/debug").join(name()));

    // CASARS_SUITE_ROOT comes before the suite the caller is installed in.
    let env = environment(&[(SUITE_ROOT_ENV, &named)]);
    assert_eq!(
        resolve(&env, Some(&installed), Some(&checkout)).unwrap(),
        named.join("bin").join(name())
    );
    let none = environment(&[]);
    assert_eq!(
        resolve(&none, Some(&installed), Some(&checkout)).unwrap(),
        installed.join("bin").join(name())
    );

    // A suite that lacks the executable is an error, not a checkout's build.
    let empty = suite(&scratch.path().join("empty"), &[]);
    let error = resolve(&none, Some(&empty), Some(&checkout)).unwrap_err();
    assert!(error.contains("the installed suite"), "{error}");
    assert!(error.contains("reinstall the suite"), "{error}");
    let env = environment(&[(SUITE_ROOT_ENV, &empty)]);
    let error = resolve(&env, None, Some(&checkout)).unwrap_err();
    assert!(error.contains(SUITE_ROOT_ENV), "{error}");
}

#[test]
fn a_checkout_runs_its_newest_build_and_names_the_build_it_lacks() {
    let scratch = tempfile::tempdir().unwrap();
    let checkout = scratch.path().join("checkout");
    let none = environment(&[]);
    let error = resolve(&none, None, Some(&checkout)).unwrap_err();
    assert!(
        error.contains("`cargo build -p casars-imager --bin casars-imager`"),
        "{error}"
    );

    let debug = touch(&checkout.join("target/debug").join(name()));
    assert_eq!(resolve(&none, None, Some(&checkout)).unwrap(), debug);
    let release = touch(&checkout.join("target/release").join(name()));
    let earlier = SystemTime::now() - Duration::from_secs(3600);
    fs::File::options()
        .write(true)
        .open(&debug)
        .unwrap()
        .set_modified(earlier)
        .unwrap();
    assert_eq!(resolve(&none, None, Some(&checkout)).unwrap(), release);

    // CARGO_TARGET_DIR moves the build, as it moves cargo's.
    let shared = scratch.path().join("shared-target");
    let env = environment(&[("CARGO_TARGET_DIR", &shared)]);
    let error = resolve(&env, None, Some(&checkout)).unwrap_err();
    assert!(error.contains("shared-target"), "{error}");
    let built = touch(&shared.join("debug").join(name()));
    assert_eq!(resolve(&env, None, Some(&checkout)).unwrap(), built);
}

#[test]
fn without_a_suite_or_checkout_the_error_names_both_remedies() {
    let error = resolve(&environment(&[]), None, None).unwrap_err();
    assert!(error.contains(SUITE_ROOT_ENV), "{error}");
    assert!(error.contains("CASARS_IMAGER_BIN"), "{error}");
}

#[test]
fn an_installed_suite_is_recognised_from_its_python_prefix() {
    let scratch = tempfile::tempdir().unwrap();
    let root = suite(&scratch.path().join("0.16.0"), &[]);
    assert_eq!(
        installed_suite_root(&root.join("python")),
        Some(root.clone())
    );
    // A virtual environment elsewhere, or the suite root itself, is not one.
    assert_eq!(installed_suite_root(&scratch.path().join("venv")), None);
    assert_eq!(installed_suite_root(&root), None);
}

#[test]
fn this_library_was_built_in_a_checkout() {
    // The tests run from the checkout, so the build workspace is one.
    let workspace = development_workspace(Path::new(BUILD_WORKSPACE)).unwrap();
    assert!(workspace.join("Cargo.toml").is_file());
}
