// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `msconvert` binary end to end, on the MSv2/MSv4 trial fixtures:
//! `ms_trial.ms` and XRADIO's conversion of it, `ms_trial.ps.zarr` (one
//! partition of 12 times, 10 baselines, 8 channels and 4 correlations).

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use flate2::read::GzDecoder;
use tar::Archive;

/// The fixture `name` unpacked into `root`.
fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    Archive::new(GzDecoder::new(File::open(archive).expect("fixture")))
        .unpack(root)
        .expect("unpack");
    root.join(name)
}

/// Run `msconvert` in `workspace`.
fn msconvert(workspace: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_msconvert"))
        .current_dir(workspace)
        .arg("--no-save-last")
        .args(args)
        .output()
        .expect("run msconvert")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Assert that `output` is a refusal whose message says `expected`.
fn refused(output: &Output, expected: &str) {
    assert_eq!(output.status.code(), Some(2), "{}", stderr(output));
    assert!(stderr(output).contains(expected), "{}", stderr(output));
}

/// The names in `directory`, sorted.
fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("list")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name != ".casa-rs")
        .collect();
    names.sort();
    names
}

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn walk(root: &Path, path: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).expect("list") {
                walk(root, &entry.expect("entry").path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).expect("inside").to_path_buf();
            files.push((relative, std::fs::read(path).expect("read")));
        }
    }
    let mut files = Vec::new();
    walk(path, path, &mut files);
    files.sort();
    files
}

/// The chunk shape of `dataset`'s `VISIBILITY` in the processing set at
/// `path`.
fn visibility_chunks(path: &Path, dataset: &str) -> Vec<u64> {
    let metadata: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join(dataset).join("VISIBILITY/zarr.json")).expect("zarr.json"),
    )
    .expect("json");
    metadata["chunk_grid"]["configuration"]["chunk_shape"]
        .as_array()
        .expect("chunk shape")
        .iter()
        .map(|extent| extent.as_u64().expect("extent"))
        .collect()
}

#[test]
fn an_msv2_converts_to_an_msv4_and_back_under_default_names() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let rows = casa_ms::MeasurementSet::open(&v2).expect("ms").row_count();

    let converted = msconvert(root.path(), &["--vis", "ms_trial.ms"]);
    assert!(converted.status.success(), "{}", stderr(&converted));
    let v4 = root.path().join("ms_trial.ms4");
    assert!(casa_ms::observation_io::is_measurement_set(&v4));
    assert!(casa_ms::observation_io::is_msv4_store(&v4));
    // XRADIO's chunks: 12 times are far below 128 MiB, so the dataset is
    // one chunk.
    assert_eq!(visibility_chunks(&v4, "ms_trial_0"), [12, 10, 8, 4]);
    let summary = String::from_utf8_lossy(&converted.stdout);
    assert!(
        summary.contains("(msv2)") && summary.contains("(msv4)"),
        "{summary}"
    );
    // What MSv4 does not hold is taken and reported, not refused.
    let log = stderr(&converted);
    for lost in [
        "not carried into msv4: SIGMA is not carried",
        "not carried into msv4: HISTORY is not carried",
    ] {
        assert!(log.contains(lost), "{log}");
    }

    // Back to MSv2: the other format, named for it.
    let back = msconvert(root.path(), &["--vis", "ms_trial.ms4"]);
    assert!(back.status.success(), "{}", stderr(&back));
    let back = casa_ms::MeasurementSet::open(root.path().join("ms_trial.ms2")).expect("back");
    assert_eq!(back.row_count(), rows, "every acquisition, one row each");
    assert_eq!(
        names(root.path()),
        ["ms_trial.ms", "ms_trial.ms2", "ms_trial.ms4"]
    );
}

#[test]
fn a_default_name_that_would_be_the_input_is_refused() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    assert!(
        msconvert(root.path(), &["--vis", "ms_trial.ms"])
            .status
            .success()
    );
    let before = names(root.path());
    refused(
        &msconvert(
            root.path(),
            &[
                "--vis",
                "ms_trial.ms4",
                "--outformat",
                "msv4",
                "--overwrite",
            ],
        ),
        "would be itself",
    );
    assert_eq!(names(root.path()), before);
}

#[test]
fn an_existing_output_is_replaced_only_when_overwrite_is_set() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let args = ["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"];
    assert!(msconvert(root.path(), &args).status.success());
    let first = names(root.path());

    refused(&msconvert(root.path(), &args), "set overwrite=True");

    // Re-chunked in place: the old store replaced, nothing left beside it.
    let replaced = msconvert(
        root.path(),
        &[&args[..], &["--overwrite", "--chunks", "[5, 0, 0, 0]"]].concat(),
    );
    assert!(replaced.status.success(), "{}", stderr(&replaced));
    assert_eq!(names(root.path()), first);
    let v4 = root.path().join("out.ps.zarr");
    assert_eq!(visibility_chunks(&v4, "ms_trial_0"), [5, 10, 8, 4]);
}

#[test]
fn overwrite_replaces_only_a_whole_measurement_set_apart_from_the_input() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let source = snapshot(&v2);
    let convert = |output: &str| {
        msconvert(
            root.path(),
            &["--vis", "ms_trial.ms", "--outputvis", output, "--overwrite"],
        )
    };
    // Inside the input, existing or not: a subtable would be destroyed,
    // and a new store would change it.
    refused(&convert("ms_trial.ms/ANTENNA"), "is vis or lies inside it");
    refused(&convert("ms_trial.ms/new.ms4"), "is vis or lies inside it");
    refused(
        &convert("./ms_trial.ms/../ms_trial.ms"),
        "is vis or lies inside it",
    );
    // Neither a directory, a casacore table that is not a MeasurementSet,
    // nor a link is replaced.
    let junk = root.path().join("junk");
    std::fs::create_dir(&junk).expect("junk");
    std::fs::write(junk.join("keep"), b"kept").expect("keep");
    refused(&convert("junk"), "is not a MeasurementSet");
    let table = root.path().join("antenna.tab");
    std::fs::create_dir(&table).expect("table");
    for entry in std::fs::read_dir(v2.join("ANTENNA")).expect("ANTENNA") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), table.join(entry.file_name())).expect("copy");
    }
    let antenna = snapshot(&table);
    refused(&convert("antenna.tab"), "is not a MeasurementSet");
    // A link to the input is the input; a link to another MeasurementSet
    // is refused, not followed.
    std::os::unix::fs::symlink(&v2, root.path().join("self.ms")).expect("link");
    refused(&convert("self.ms"), "is vis or lies inside it");
    let other = fixture(root.path(), "ms_trial.ps.zarr");
    let other_before = snapshot(&other);
    std::os::unix::fs::symlink(&other, root.path().join("link.ms")).expect("link");
    refused(&convert("link.ms"), "is a symbolic link");
    // A MeasurementSet holding the input is not replaced by its
    // conversion, and anything else holding it is no MeasurementSet.
    assert!(
        msconvert(
            root.path(),
            &["--vis", "ms_trial.ms", "--outputvis", "outer.ms4"]
        )
        .status
        .success()
    );
    fixture(&root.path().join("outer.ms4"), "ms_trial.ms");
    refused(
        &msconvert(
            root.path(),
            &[
                "--vis",
                "outer.ms4/ms_trial.ms",
                "--outputvis",
                "outer.ms4",
                "--overwrite",
            ],
        ),
        "holds vis",
    );
    refused(&convert("."), "is not a MeasurementSet");

    assert_eq!(snapshot(&v2), source, "the input is untouched");
    assert_eq!(snapshot(&table), antenna);
    assert_eq!(snapshot(&other), other_before);
    assert_eq!(std::fs::read(junk.join("keep")).expect("kept"), b"kept");
}

#[test]
fn invalid_parameters_are_refused_at_their_parameter() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let base = ["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"];
    for (args, expected) in [
        (&["--chunks", "[5, 0, 0]"][..], "four extents"),
        (&["--chunks", "[5, -1, 0, 0]"][..], "chunks"),
        (&["--outformat", "msv3"][..], "outformat"),
        (&["--codec", "gzip"][..], "codec"),
        (&["--padding", "dropped"][..], "padding"),
        (&["--nthreads", "-1"][..], "nthreads"),
    ] {
        refused(
            &msconvert(root.path(), &[&base[..], args].concat()),
            expected,
        );
    }
    refused(
        &msconvert(root.path(), &["--vis", "missing.ms"]),
        "does not exist",
    );
    assert_eq!(names(root.path()), ["ms_trial.ms"]);
}

#[test]
fn an_msv4_from_xradio_converts_to_an_msv2() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ps.zarr");
    let converted = msconvert(
        root.path(),
        &["--vis", "ms_trial.ps.zarr", "--padding", "omitted"],
    );
    assert!(converted.status.success(), "{}", stderr(&converted));
    // An MSv2 states SIGMA, and what the MSv4 leaves unstated, itself.
    let log = stderr(&converted);
    assert!(log.contains("SIGMA is derived from WEIGHT"), "{log}");
    assert!(log.contains("MSv2's default is written"), "{log}");
    let ms = casa_ms::MeasurementSet::open(root.path().join("ms_trial.ms2")).expect("ms");
    assert_eq!(ms.row_count(), 120);
}
