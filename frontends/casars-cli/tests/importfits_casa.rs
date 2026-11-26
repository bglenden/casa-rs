// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 snapshots from tools/casa-compat/importfits_oracle.py.

use casa_images::PagedImage;
use serde_json::Value;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

#[test]
fn importfits_matches_casa_snapshots() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/casa-images/tests/data/importfits");
    let snapshots: Value = serde_json::from_str(include_str!(
        "../../../crates/casa-images/tests/data/importfits/expected.json"
    ))
    .unwrap();
    assert_eq!(snapshots["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    for (name, case) in snapshots["cases"].as_object().unwrap() {
        let source = data.join(case["fitsimage"].as_str().unwrap());
        let product = workspace.path().join(format!("{name}.im"));
        let mut values = case["params"].clone();
        values["fitsimage"] = serde_json::json!(source);
        values["imagename"] = serde_json::json!(product);
        let request = serde_json::json!({"surface": "importfits", "values": values});
        let mut task = Command::new(env!("CARGO_BIN_EXE_importfits"))
            .args(["--json-run", "-"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        task.stdin
            .take()
            .unwrap()
            .write_all(request.to_string().as_bytes())
            .unwrap();
        let output = task.wait_with_output().unwrap();
        if case.get("error").is_some() {
            assert!(!output.status.success(), "{name}");
            assert!(!product.exists(), "{name}");
            continue;
        }
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let image = PagedImage::<f32>::open(&product).unwrap();
        assert_eq!(image.units(), case["unit"].as_str().unwrap(), "{name}");
        assert_eq!(serde_json::json!(image.shape()), case["shape"], "{name}");
        assert_eq!(
            serde_json::json!(image.axis_names()),
            case["names"],
            "{name}"
        );
        let origin = vec![0; image.shape().len()];
        let data = image.get_slice(&origin, image.shape()).unwrap();
        let values = case["pixels"].as_array().unwrap();
        let validity = case["mask"].as_array().unwrap();
        let mask = image.get_mask().unwrap();
        // FITS traverses the first axis fastest. Use logical indices so
        // storage layout differences in a strided block do not hide mistakes.
        for (offset, (value, valid)) in values.iter().zip(validity).enumerate() {
            let mut remaining = offset;
            let index: Vec<_> = image
                .shape()
                .iter()
                .map(|&n| {
                    let pixel = remaining % n;
                    remaining /= n;
                    pixel
                })
                .collect();
            let actual = data[&index[..]];
            if let Some(value) = value.as_f64() {
                assert_eq!(actual, value as f32, "{name} at {index:?}");
            } else {
                assert!(actual.is_nan(), "{name} at {index:?}");
            }
            assert_eq!(
                mask.as_ref().is_none_or(|mask| mask[&index[..]]),
                valid.as_bool().unwrap(),
                "{name} at {index:?}"
            );
        }
        let history = image.history().unwrap();
        assert_eq!(
            history
                .iter()
                .filter(|line| *line == "Fixture origin")
                .count(),
            case["fits_history"].as_array().unwrap().len(),
            "{name}"
        );
        assert!(history.last().unwrap().starts_with("importfits("), "{name}");
    }
}
