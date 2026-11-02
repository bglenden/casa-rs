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

/// What the MSv2 fixture loses into an MSv4: `SIGMA`, `HISTORY` and
/// pointing columns.
const INTO_MSV4: [&str; 4] = ["--acceptloss", "sigma", "--acceptloss", "tables"];

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
fn an_msv2_converts_to_an_msv4_and_back() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let rows = casa_ms::MeasurementSet::open(&v2).expect("ms").row_count();

    let converted = msconvert(
        root.path(),
        &[
            &["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"][..],
            &INTO_MSV4,
        ]
        .concat(),
    );
    assert!(converted.status.success(), "{}", stderr(&converted));
    let v4 = root.path().join("out.ps.zarr");
    assert!(casa_ms::observation_io::is_msv4_store(&v4));
    // XRADIO's default chunks: 12 times are far below 128 MiB, so the
    // dataset is one chunk.
    assert_eq!(visibility_chunks(&v4, "ms_trial_0"), [12, 10, 8, 4]);
    let summary = String::from_utf8_lossy(&converted.stdout);
    assert!(
        summary.contains("(msv2)") && summary.contains("(msv4)"),
        "{summary}"
    );

    // Back to MSv2: the empty outformat is the other format. An MSv2 states
    // what the MSv4 does not, as MSv2's defaults.
    let back = msconvert(
        root.path(),
        &[
            "--vis",
            "out.ps.zarr",
            "--outputvis",
            "back.ms",
            "--acceptloss",
            "sigma,defaulted",
        ],
    );
    assert!(back.status.success(), "{}", stderr(&back));
    let back = casa_ms::MeasurementSet::open(root.path().join("back.ms")).expect("back");
    assert_eq!(back.row_count(), rows, "every acquisition, one row each");
}

#[test]
fn an_unaccepted_loss_refuses_naming_every_kind_and_writes_nothing() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let before = names(root.path());
    let refused = msconvert(
        root.path(),
        &["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"],
    );
    assert_eq!(refused.status.code(), Some(2), "a refusal");
    let message = stderr(&refused);
    for described in [
        "HISTORY is not carried",
        "POINTING",
        "accept tables to convert anyway",
    ] {
        assert!(message.contains(described), "{message}");
    }
    assert!(message.contains("acceptloss is now [sigma]"), "{message}");
    assert_eq!(names(root.path()), before, "nothing is written");
}

#[test]
fn an_existing_output_is_replaced_only_when_overwrite_is_set() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let args = [
        &["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"][..],
        &INTO_MSV4,
    ]
    .concat();
    assert!(msconvert(root.path(), &args).status.success());
    let first = names(root.path());

    let refused = msconvert(root.path(), &args);
    assert_eq!(refused.status.code(), Some(2));
    assert!(
        stderr(&refused).contains("set overwrite=True"),
        "{}",
        stderr(&refused)
    );

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
fn overwrite_replaces_only_a_measurement_set_other_than_the_input() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let junk = root.path().join("junk");
    std::fs::create_dir(&junk).expect("junk");
    std::fs::write(junk.join("keep"), b"kept").expect("keep");
    let refused = msconvert(
        root.path(),
        &[
            &["--vis", "ms_trial.ms", "--outputvis", "junk", "--overwrite"][..],
            &INTO_MSV4,
        ]
        .concat(),
    );
    assert_eq!(refused.status.code(), Some(2));
    assert!(
        stderr(&refused).contains("is not a MeasurementSet"),
        "{}",
        stderr(&refused)
    );
    assert_eq!(std::fs::read(junk.join("keep")).expect("kept"), b"kept");

    let rows = casa_ms::MeasurementSet::open(root.path().join("ms_trial.ms"))
        .expect("ms")
        .row_count();
    let refused = msconvert(
        root.path(),
        &[
            "--vis",
            "ms_trial.ms",
            "--outputvis",
            "ms_trial.ms",
            "--outformat",
            "msv2",
            "--overwrite",
        ],
    );
    assert_eq!(refused.status.code(), Some(2));
    assert!(
        stderr(&refused).contains("is vis or holds it"),
        "{}",
        stderr(&refused)
    );
    let kept = casa_ms::MeasurementSet::open(root.path().join("ms_trial.ms")).expect("kept");
    assert_eq!(kept.row_count(), rows);
}

#[test]
fn invalid_parameters_are_refused_at_their_parameter() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ms");
    let base = ["--vis", "ms_trial.ms", "--outputvis", "out.ps.zarr"];
    for (args, expected) in [
        (&["--chunks", "[5, 0, 0]"][..], "four extents"),
        (&["--chunks", "[5, -1, 0, 0]"][..], "chunks"),
        (&["--acceptloss", "row-flags"][..], "acceptloss"),
        (&["--outformat", "msv3"][..], "outformat"),
        (&["--codec", "gzip"][..], "codec"),
        (&["--nthreads", "-1"][..], "nthreads"),
    ] {
        let refused = msconvert(root.path(), &[&base[..], args].concat());
        assert_eq!(refused.status.code(), Some(2), "{args:?}");
        assert!(
            stderr(&refused).contains(expected),
            "{args:?}: {}",
            stderr(&refused)
        );
    }
    let missing = msconvert(
        root.path(),
        &["--vis", "missing.ms", "--outputvis", "out.ms"],
    );
    assert_eq!(missing.status.code(), Some(2));
    assert!(
        stderr(&missing).contains("does not exist"),
        "{}",
        stderr(&missing)
    );
    assert!(!root.path().join("out.ps.zarr").exists());
}

#[test]
fn an_msv4_from_xradio_converts_to_an_msv2() {
    let root = tempfile::tempdir().expect("root");
    fixture(root.path(), "ms_trial.ps.zarr");
    let refused = msconvert(
        root.path(),
        &["--vis", "ms_trial.ps.zarr", "--outputvis", "out.ms"],
    );
    assert_eq!(refused.status.code(), Some(2));
    assert!(
        stderr(&refused).contains("accept defaulted to convert anyway"),
        "{}",
        stderr(&refused)
    );
    let converted = msconvert(
        root.path(),
        &[
            "--vis",
            "ms_trial.ps.zarr",
            "--outputvis",
            "out.ms",
            "--acceptloss",
            "sigma",
            "--acceptloss",
            "defaulted",
            "--padding",
            "omitted",
        ],
    );
    assert!(converted.status.success(), "{}", stderr(&converted));
    let ms = casa_ms::MeasurementSet::open(root.path().join("out.ms")).expect("ms");
    assert_eq!(ms.row_count(), 120);
}
