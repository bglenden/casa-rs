// SPDX-License-Identifier: LGPL-3.0-or-later
//! Effective diameter and dish filtering on the CASA feather task surface.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_images::PagedImage;
use serde_json::{Value, json};

fn copy_images(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "expected.json" || entry.file_name() == "table.lock" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_images(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn run(workspace: &Path, values: &Value) -> Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_feather"))
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    process
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({"surface":"feather", "values":values})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    process.wait_with_output().unwrap()
}

#[test]
fn dish_filters_match_casa() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/feather");
    let record: Value = serde_json::from_str(include_str!("data/feather/expected.json")).unwrap();
    assert_eq!(record["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_images(&fixtures, workspace.path());
    for (name, case) in record["cases"].as_object().unwrap() {
        let product = format!("{name}.out");
        let path = workspace.path().join(&product);
        let mut values = case["params"].clone();
        values["imagename"] = json!(product);
        let reply = run(workspace.path(), &values);
        let message = String::from_utf8_lossy(&reply.stdout);
        let Some(expected) = case.get("product") else {
            assert!(!reply.status.success(), "{name}: {message}");
            assert!(message.contains("smaller than the original single-dish beam"));
            assert!(!path.exists());
            continue;
        };
        assert!(reply.status.success(), "{name}: {message}");
        let actual = PagedImage::<f32>::open(path).unwrap();
        assert_eq!(json!(actual.shape()), expected["shape"], "{name}");
        assert_eq!(json!(actual.units()), expected["units"], "{name}");
        assert_eq!(
            actual.default_mask_name().unwrap_or_default(),
            expected["default_mask"][0].as_str().unwrap(),
            "{name}"
        );
        let beam = actual.image_info().unwrap().beam_set.single_beam().unwrap();
        for (key, value) in [
            ("major", beam.major_in("arcsec").unwrap()),
            ("minor", beam.minor_in("arcsec").unwrap()),
            ("positionangle", beam.position_angle_in("deg").unwrap()),
        ] {
            let reference = expected["beam"][key][0].as_f64().unwrap();
            assert!(
                (value - reference).abs() < 1e-10,
                "{name} {key}: {value} vs {reference}"
            );
        }
        let data = actual.get().unwrap();
        let mask = actual.get_mask().unwrap().map_or_else(
            || vec![true; data.len()],
            |mask| mask.view().reversed_axes().iter().copied().collect(),
        );
        assert_eq!(json!(mask), expected["mask"], "{name}");
        let (mut squared_error, mut squared_reference) = (0.0, 0.0);
        for (i, &value) in data.view().reversed_axes().iter().enumerate() {
            let reference = expected["pixels"][i].as_f64().unwrap();
            squared_error += (f64::from(value) - reference).powi(2);
            squared_reference += reference.powi(2);
        }
        let nrms = (squared_error / squared_reference).sqrt();
        // At an integer-aligned footprint, CASA can alternate between
        // excluding and extrapolating the same edge by row after its sky
        // coordinate round trip. Use #977's scientific tolerance for this case.
        let tolerance = if name == "integer_aligned_smaller_low" {
            1e-3
        } else {
            2e-6
        };
        assert!(nrms < tolerance, "{name} normalized RMS error {nrms}");
    }
    let output = workspace.path().join("default.out/table.dat");
    let before = fs::read(&output).unwrap();
    let reply = run(
        workspace.path(),
        &json!({
            "imagename":"default.out", "highres":"high.im", "lowres":"low.im",
            "effdishdiam":4200.0, "overwrite":true,
        }),
    );
    assert!(!reply.status.success());
    assert_eq!(fs::read(output).unwrap(), before);
}
