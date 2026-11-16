// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `synthetic-ms` developer binary: a JSON request in, the generator's
//! result envelope out, as `simobserve --json-run` gave them.

use std::io::Write;
use std::process::{Command, Stdio};

fn synthetic_ms() -> Command {
    Command::new(env!("CARGO_BIN_EXE_synthetic-ms"))
}

/// A one-antenna total-power family of three pointings, as casa-ms's own
/// family test writes it: small enough to run in a moment.
fn family_request(directory: &std::path::Path) -> serde_json::Value {
    let config = directory.join("aca.tp.cfg");
    std::fs::write(
        &config,
        "# observatory=ALMASD\n# coordsys=LOC\n-47.7 178.2 -2.0 12.0 T701\n-42.1 118.7 -2.0 12.0 T702\n",
    )
    .expect("a configuration file");
    serde_json::json!({
        "kind": "family",
        "request": {
            "source_model": {
                "kind": "analytic_components",
                "components": [{
                    "kind": "point",
                    "l_rad": 0.0,
                    "m_rad": 0.0,
                    "spectrum": { "flux_jy": 1.0 }
                }]
            },
            "telescope": "ACA",
            "array_config": config,
            "band": "Band 7",
            "target_ms_size_gib": 0.00001,
            "polarizations": 2,
            "ms_channels": 1,
            "image_channels": 1,
            "pointing_count": 3,
            "imaging_mode": "mosaic",
            "observation_mode": "total_power",
            "output_ms": directory.join("tp-family.ms")
        }
    })
}

#[test]
fn a_family_request_from_a_file_writes_its_measurement_set() {
    let directory = tempfile::tempdir().expect("a scratch directory");
    let request = directory.path().join("request.json");
    std::fs::write(&request, family_request(directory.path()).to_string()).expect("a request");
    let output = synthetic_ms()
        .arg("--json-run")
        .arg(&request)
        .output()
        .expect("synthetic-ms runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).expect("a JSON result");
    assert_eq!(result["kind"], "family");
    assert!(
        directory
            .path()
            .join("tp-family.ms")
            .join("table.dat")
            .exists()
    );
    let manifest = result["result"]["manifest_path"]
        .as_str()
        .expect("a manifest");
    assert!(std::path::Path::new(manifest).exists());
}

#[test]
fn a_request_on_standard_input_is_read_from_it() {
    let directory = tempfile::tempdir().expect("a scratch directory");
    let mut child = synthetic_ms()
        .args(["--json-run", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("synthetic-ms runs");
    child
        .stdin
        .take()
        .expect("standard input")
        .write_all(family_request(directory.path()).to_string().as_bytes())
        .expect("the request written");
    let output = child.wait_with_output().expect("synthetic-ms finishes");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        directory
            .path()
            .join("tp-family.ms")
            .join("table.dat")
            .exists()
    );
}

#[test]
fn help_and_bad_requests_are_answered() {
    let help = synthetic_ms()
        .arg("--help")
        .output()
        .expect("synthetic-ms runs");
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--json-run"));
    let directory = tempfile::tempdir().expect("a scratch directory");
    let request = directory.path().join("request.json");
    std::fs::write(&request, r#"{"kind": "nonsense", "request": {}}"#).expect("a request");
    let bad = synthetic_ms()
        .arg("--json-run")
        .arg(&request)
        .output()
        .expect("synthetic-ms runs");
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("failed to parse the request"));
    let stray = synthetic_ms()
        .arg("--model")
        .output()
        .expect("synthetic-ms runs");
    assert!(!stray.status.success());
}
