// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exercise the primary-beam task against products recorded by CASA 6.7.6.14.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_images::PagedImage;
use serde_json::{Value, json};

fn copy_inputs(source: &Path, workspace: &Path) {
    fs::create_dir_all(workspace).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "expected.json" || entry.file_name() == "table.lock" {
            continue;
        }
        let destination = workspace.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_inputs(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn invoke(workspace: &Path, values: &Value) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_impbcor"))
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let body = json!({"surface": "impbcor", "values": values});
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn task_matches_pinned_casa_products() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/impbcor");
    let recorded: Value = serde_json::from_str(include_str!(
        "../../../libs/casacore/casa-images/tests/data/impbcor/expected.json"
    ))
    .unwrap();
    assert_eq!(recorded["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&fixtures, workspace.path());
    for (name, case) in recorded["cases"].as_object().unwrap() {
        let file = format!("{name}.out");
        let mut values = case["params"].clone();
        values["outfile"] = json!(file);
        let result = invoke(workspace.path(), &values);
        let output = String::from_utf8_lossy(&result.stdout);
        let product = workspace.path().join(file);
        if case.get("error").is_some() {
            assert!(!result.status.success(), "{name}: {output}");
            assert!(!product.exists(), "{name}");
            continue;
        }
        assert!(result.status.success(), "{name}: {output}");
        let actual = PagedImage::<f32>::open(product).unwrap();
        let expected = &case["product"];
        assert_eq!(json!(actual.shape()), expected["shape"], "{name}");
        assert_eq!(json!(actual.axis_names()), expected["names"], "{name}");
        assert_eq!(json!(actual.units()), expected["units"], "{name}");
        assert_eq!(
            actual.default_mask_name().as_deref().unwrap_or(""),
            expected["default_mask"][0].as_str().unwrap(),
            "{name}"
        );
        let data = actual
            .get_slice(&vec![0; actual.ndim()], actual.shape())
            .unwrap();
        let valid = actual.get_mask().unwrap().map_or_else(
            || vec![true; data.len()],
            |mask| mask.view().reversed_axes().iter().copied().collect(),
        );
        for (offset, value) in data.view().reversed_axes().iter().enumerate() {
            assert_eq!(
                json!(valid[offset]),
                expected["mask"][offset],
                "{name} mask at {offset}"
            );
            if let Some(reference) = expected["pixels"][offset].as_f64() {
                assert!(
                    (f64::from(*value) - reference).abs() <= 2e-6 * reference.abs().max(1.0),
                    "{name} pixel {offset}: CASA {reference}, native {value}"
                );
            } else {
                assert!(!value.is_finite(), "{name} pixel {offset}: native {value}");
            }
        }
        let history = actual.history().unwrap();
        let fixture_history: Vec<_> = history
            .iter()
            .filter(|entry| entry.starts_with("input history for "))
            .collect();
        assert_eq!(json!(fixture_history), expected["history"], "{name}");
        assert!(
            history.iter().any(|line| line.starts_with("impbcor(")),
            "{name}"
        );
    }
}

#[test]
fn failed_overwrite_preserves_input_and_existing_output() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/impbcor");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&fixtures, workspace.path());
    let mut values = json!({"imagename": "cube.im", "pbimage": "plane.im", "outfile": "out.im"});
    assert!(invoke(workspace.path(), &values).status.success());
    let outfile = workspace.path().join("out.im");
    let previous = fs::read(outfile.join("table.dat")).unwrap();
    values["pbimage"] = json!("shifted.im");
    values["overwrite"] = json!(true);
    assert!(!invoke(workspace.path(), &values).status.success());
    assert_eq!(fs::read(outfile.join("table.dat")).unwrap(), previous);
    values["pbimage"] = json!("plane.im");
    for invalid in ["cube.im", "cube.im/logtable", "plane.im", "plane.im/new.im"] {
        values["outfile"] = json!(invalid);
        assert!(
            !invoke(workspace.path(), &values).status.success(),
            "{invalid}"
        );
    }
    assert!(!workspace.path().join("plane.im/new.im").exists());
}
