// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 regrid shapes, coordinate frames, masks and samples.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_images::{AnyPagedImage, ImagePixel, PagedImage};
use serde_json::{Value, json};

fn copy_inputs(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "expected.json" || entry.file_name() == "table.lock" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_inputs(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn regrid_products_match_casa() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imregrid");
    let evidence: Value =
        serde_json::from_str(include_str!("data/imregrid/expected.json")).unwrap();
    assert_eq!(evidence["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&directory, workspace.path());
    for (name, case) in evidence["cases"].as_object().unwrap() {
        let output = format!("{name}.out");
        let mut values = case["params"].clone();
        values["output"] = json!(output);
        let mut child = Command::new(env!("CARGO_BIN_EXE_imregrid"))
            .args(["--json-run", "-"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                json!({"surface": "imregrid", "values": values})
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
        let reply = child.wait_with_output().unwrap();
        let message = String::from_utf8_lossy(&reply.stdout);
        let path = workspace.path().join(output);
        let Some(expected) = case.get("product") else {
            assert!(!reply.status.success(), "{name}: {message}");
            assert!(!path.exists(), "{name} published a refused output");
            continue;
        };
        assert!(reply.status.success(), "{name}: {message}");
        let metadata = case.get("preserved_input_metadata").unwrap_or(expected);
        match AnyPagedImage::open(path).unwrap() {
            AnyPagedImage::Float32(image) => compare(name, &image, expected, metadata),
            AnyPagedImage::Float64(image) => compare(name, &image, expected, metadata),
            _ => panic!("{name} returned an unexpected pixel type"),
        }
    }
}

fn compare<T: ImagePixel + Into<f64> + Copy>(
    name: &str,
    image: &PagedImage<T>,
    expected: &Value,
    metadata: &Value,
) {
    assert_eq!(json!(image.shape()), expected["shape"], "{name}");
    assert_eq!(json!(image.axis_names()), expected["names"], "{name}");
    assert_eq!(json!(image.units()), metadata["brightness"], "{name}");
    let origin = vec![0; image.ndim()];
    let world = image
        .coordinates()
        .to_world(&vec![0.0; image.ndim()])
        .unwrap();
    for (index, actual) in world.into_iter().enumerate() {
        let reference = expected["pixel0_world"][index].as_f64().unwrap();
        assert!(
            (actual - reference).abs() < 1e-8 * reference.abs().max(1.0),
            "{name} axis {index}: CASA {reference}, native {actual}"
        );
    }
    let data = image.get_slice(&origin, image.shape()).unwrap();
    let mask = image.get_mask().unwrap().map_or_else(
        || vec![true; data.len()],
        |mask| mask.view().reversed_axes().iter().copied().collect(),
    );
    assert_eq!(json!(mask), expected["mask"], "{name} mask");
    let (mut residual, mut power) = (0.0, 0.0);
    for (pixel, &actual) in data.view().reversed_axes().iter().enumerate() {
        if mask[pixel] {
            let reference = expected["pixels"][pixel].as_f64().unwrap();
            residual += (actual.into() - reference).powi(2);
            power += reference.powi(2);
        }
    }
    let normalized_rms = (residual / power).sqrt();
    assert!(
        normalized_rms < 1e-6,
        "{name} normalized RMS error {normalized_rms}"
    );
    let info = image.image_info().unwrap();
    if let Some(alt) = metadata["beam"].get("positionangle") {
        let measured = info
            .beam_set
            .get_beam()
            .unwrap()
            .position_angle_in("deg")
            .unwrap();
        let reference = alt[0].as_f64().unwrap();
        assert!(
            (measured - reference).abs() < 1e-5,
            "{name} position angle {measured} vs {reference}"
        );
    } else {
        assert!(info.beam_set.is_empty(), "{name} beam");
    }
}
