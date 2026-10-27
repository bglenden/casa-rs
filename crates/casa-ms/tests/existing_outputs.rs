// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MeasurementSet tasks' guards (#931): CASA's parameters are the consent.
//! A product a second run would replace is refused without `--overwrite`, as
//! CASA refuses it, before anything is written or flagged; read-only modes
//! and refused arguments leave the MeasurementSet byte for byte as it was.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::{TIME_BASE_SECONDS, create_msexplore_fixture_ms};

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, std::fs::read(path).unwrap());
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

fn run(binary: &str, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .output()
        .expect("run task binary")
}

fn flag_versions(ms: &Path) -> PathBuf {
    let mut name = ms.as_os_str().to_owned();
    name.push(".flagversions");
    PathBuf::from(name)
}

#[test]
fn mstransform_and_split_refuse_an_existing_output_ms() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms = ms.to_str().unwrap();
    let out = temp.path().join("out.ms");
    let args = [
        "--vis",
        ms,
        "--outputvis",
        out.to_str().unwrap(),
        "--spw",
        "0",
        "--datacolumn",
        "data",
    ];

    let first = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(first.status.success(), "{first:?}");
    let before = snapshot(&out);

    let second = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("already exists - will not overwrite it"),
        "{stderr}"
    );
    assert_eq!(snapshot(&out), before);
}

#[test]
fn msexplore_refuses_an_existing_output_before_it_writes_or_flags_anything() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    // The plot is written last, after any flag edit, so it is the output
    // whose guard must run first.
    let plot = temp.path().join("plot.txt");
    let outputs = [
        "--preset",
        "amplitude_vs_time",
        "--plot-output",
        plot.to_str().unwrap(),
        "--plot-format",
        "txt",
    ];
    let xmin = format!("{}", TIME_BASE_SECONDS - 0.1);
    let xmax = format!("{}", TIME_BASE_SECONDS + 0.1);
    let flag_edit = [
        "--flag-action",
        "flag",
        "--flag-xmin",
        &xmin,
        "--flag-xmax",
        &xmax,
        "--flag-ymin",
        "-0.1",
        "--flag-ymax",
        "0.1",
        "--flag-apply",
    ];
    let ms_arg = ms.to_str().unwrap();

    let mut first = outputs.to_vec();
    first.push(ms_arg);
    let output = run(env!("CARGO_BIN_EXE_msexplore"), &first);
    assert!(output.status.success(), "{output:?}");
    let before = (snapshot(&ms), snapshot(&plot));

    // Asking again, now also to apply a flag edit, without --overwrite.
    let mut second = outputs.to_vec();
    second.extend(flag_edit);
    second.push(ms_arg);
    let refused = run(env!("CARGO_BIN_EXE_msexplore"), &second);
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("refusing to overwrite"), "{stderr}");
    assert_eq!((snapshot(&ms), snapshot(&plot)), before);
    assert!(!flag_versions(&ms).exists());

    second.insert(0, "--overwrite");
    let replaced = run(env!("CARGO_BIN_EXE_msexplore"), &second);
    assert!(replaced.status.success(), "{replaced:?}");
    assert_ne!(snapshot(&ms), before.0, "the flag edit was applied");
}

#[test]
fn read_only_modes_leave_the_measurement_set_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);

    for (binary, args) in [
        (
            env!("CARGO_BIN_EXE_flagdata"),
            vec!["--vis", ms_arg, "--mode", "summary"],
        ),
        (
            env!("CARGO_BIN_EXE_flagmanager"),
            vec!["--vis", ms_arg, "--mode", "list"],
        ),
        (env!("CARGO_BIN_EXE_msexplore"), vec![ms_arg]),
    ] {
        let output = run(binary, &args);
        assert!(output.status.success(), "{binary} {args:?}: {output:?}");
        assert_eq!(snapshot(&ms), before, "{binary} {args:?}");
    }
    assert!(!flag_versions(&ms).exists());
}

#[test]
fn refused_arguments_change_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);
    let out = temp.path().join("out.ms");

    for (binary, args) in [
        (
            env!("CARGO_BIN_EXE_flagdata"),
            vec!["--vis", ms_arg, "--mode", "manual", "--spw", "0:99~100"],
        ),
        (
            env!("CARGO_BIN_EXE_mstransform"),
            vec!["--vis", ms_arg, "--outputvis", out.to_str().unwrap(), "--spw", "9"],
        ),
    ] {
        let output = run(binary, &args);
        assert!(!output.status.success(), "{binary} {args:?} succeeded");
        assert_eq!(snapshot(&ms), before, "{binary} {args:?}");
    }
    assert!(!flag_versions(&ms).exists());
    assert!(!out.exists());
}
