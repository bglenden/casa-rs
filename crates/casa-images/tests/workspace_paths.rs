// SPDX-License-Identifier: LGPL-3.0-or-later

//! An image task binary run directly resolves every relative path its run
//! names in its `--workspace`, as a Python call does (#931): inputs,
//! products, and the image a mask expression names, whatever directory the
//! command was run from.

mod support;

use std::path::Path;
use std::process::{Command, Output};

use support::write_image;

/// `binary ARGS --workspace ../workspace`, run from `root/elsewhere`.
fn run_elsewhere(binary: &str, args: &[&str], root: &Path) -> Output {
    Command::new(binary)
        .args(args)
        .args(["--workspace", "../workspace"])
        .current_dir(root.join("elsewhere"))
        .output()
        .expect("run task binary")
}

#[test]
fn a_task_binary_resolves_its_relative_paths_in_its_workspace() {
    // Astra's #956 P1: a run resolved them against the process directory.
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let elsewhere = root.path().join("elsewhere");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&elsewhere).unwrap();
    write_image(&workspace.join("in.image"), 1.0);
    write_image(&workspace.join("mask.image"), 1.0);

    let cases: [(&str, &[&str], &str); 3] = [
        (
            env!("CARGO_BIN_EXE_exportfits"),
            &["in.image", "out.fits"],
            "out.fits",
        ),
        (
            env!("CARGO_BIN_EXE_imsubimage"),
            &["in.image", "out.sub", "--box", "0,0,1,1"],
            "out.sub",
        ),
        (
            env!("CARGO_BIN_EXE_immoments"),
            &[
                "in.image",
                "--outfile",
                "out.mom0",
                "--moments",
                "0",
                "--mask",
                "mask.image>10",
            ],
            "out.mom0",
        ),
    ];
    for (binary, args, product) in cases {
        let output = run_elsewhere(binary, args, root.path());
        assert!(
            output.status.success(),
            "{binary} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            workspace.join(product).exists(),
            "{binary} wrote no {product} in its workspace"
        );
    }
    let written = std::fs::read_dir(&elsewhere).unwrap().count();
    assert_eq!(written, 0, "a run wrote where its command was run");
}
