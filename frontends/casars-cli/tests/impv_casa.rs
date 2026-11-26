// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 direction, offset, and spectra through the impv executable.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_core::{ArrayValue, Value};
use casa_images::PagedImage;
use serde_json::{Value as Json, json};

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

#[test]
fn position_velocity_products_match_casa() {
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/casa-images/tests/data/impv");
    let recording: Json = serde_json::from_str(include_str!(
        "../../../crates/casa-images/tests/data/impv/expected.json"
    ))
    .unwrap();
    assert_eq!(recording["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&fixtures, workspace.path());
    for (name, case) in recording["cases"].as_object().unwrap() {
        let outfile = format!("{name}.out");
        let mut values = case["params"].clone();
        values["outfile"] = json!(outfile);
        let mut child = Command::new(env!("CARGO_BIN_EXE_impv"))
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
                json!({"surface": "impv", "values": values})
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
        let result = child.wait_with_output().unwrap();
        let text = String::from_utf8_lossy(&result.stdout);
        let path = workspace.path().join(outfile);
        let Some(expected) = case.get("product").or_else(|| case.get("padded_reference")) else {
            assert!(!result.status.success(), "{name}: {text}");
            assert!(!path.exists());
            continue;
        };
        assert!(result.status.success(), "{name}: {text}");
        compare(name, &PagedImage::<f32>::open(path).unwrap(), expected);
    }
}

fn compare(name: &str, image: &PagedImage<f32>, expected: &Json) {
    assert_eq!(json!(image.shape()), expected["shape"], "{name}");
    assert_eq!(json!(image.axis_names()), expected["names"], "{name}");
    assert_eq!(json!(image.units()), expected["brightness"], "{name}");
    let axis = vec![0.0; image.ndim()];
    let world = image.coordinates().to_world(&axis).unwrap();
    for (i, value) in world.into_iter().enumerate() {
        let reference = expected["pixel0_world"][i].as_f64().unwrap();
        assert!(
            (value - reference).abs() < 1e-8,
            "{name} world axis {i}: CASA {reference}, native {value}"
        );
    }
    let Some(Value::Record(record)) = image.table().keywords().get("coords") else {
        panic!("{name} has no stored coordinate record");
    };
    for (key, expected) in expected["coordinate_record_maps"].as_object().unwrap() {
        let Some(Value::Array(ArrayValue::Int32(value))) = record.get(key) else {
            panic!("{name} missing {key}");
        };
        assert_eq!(
            json!(value.iter().copied().collect::<Vec<_>>()),
            *expected,
            "{name} {key}"
        );
    }
    let data = image
        .get_slice(&vec![0; image.ndim()], image.shape())
        .unwrap();
    let valid = image.get_mask().unwrap().map_or_else(
        || vec![true; data.len()],
        |mask| {
            mask.view()
                .reversed_axes()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        },
    );
    assert_eq!(json!(valid), expected["mask"], "{name}");
    for (i, &pixel) in data.view().reversed_axes().iter().enumerate() {
        if valid[i] {
            let reference = expected["pixels"][i].as_f64().unwrap();
            assert!(
                (f64::from(pixel) - reference).abs() <= 2e-5 * reference.abs().max(1.0),
                "{name} pixel {i}: CASA {reference}, native {pixel}"
            );
        }
    }
    let misc = image.misc_info();
    let Some(Value::Record(secondary)) = misc.get("secondary_coordinates") else {
        panic!("{name} missing secondary coordinate system");
    };
    let direction = casa_coordinates::CoordinateSystem::record_coordinates(secondary)
        .unwrap()
        .into_iter()
        .find(|entry| entry.field.starts_with("direction"))
        .unwrap();
    let Some(Value::Record(direction)) = secondary.get(&direction.field) else {
        panic!("{name} missing secondary direction coordinate");
    };
    let Some(Value::Array(ArrayValue::Float64(reference_pixel))) = direction.get("crpix") else {
        panic!("{name} missing secondary direction reference pixel");
    };
    assert_eq!(
        reference_pixel[[1]],
        expected["secondary_direction"]["crpix"][1]
            .as_f64()
            .unwrap(),
        "{name} collapsed latitude reference pixel"
    );
    let beams = image.image_info().unwrap().beam_set;
    let expected_beams = &expected["beams"];
    if expected_beams.get("beams").is_some() {
        let channels = expected_beams["nChannels"].as_u64().unwrap() as usize;
        let stokes = expected_beams["nStokes"].as_u64().unwrap() as usize;
        assert_eq!(beams.shape(), (channels, stokes), "{name}");
        for channel in 0..channels {
            for pol in 0..stokes {
                compare_beam(
                    beams.beam(channel, pol),
                    &expected_beams["beams"][format!("*{channel}")][format!("*{pol}")],
                );
            }
        }
    } else if expected_beams.get("major").is_some() {
        compare_beam(beams.get_beam().unwrap(), expected_beams);
    } else {
        assert!(beams.is_empty(), "{name}");
    }
    assert!(
        image
            .history()
            .unwrap()
            .iter()
            .any(|line| line.starts_with("impv("))
    );
}

fn compare_beam(beam: &casa_images::GaussianBeam, expected: &Json) {
    for (field, actual) in [
        ("major", beam.major_in("arcsec").unwrap()),
        ("minor", beam.minor_in("arcsec").unwrap()),
        ("positionangle", beam.position_angle_in("deg").unwrap()),
    ] {
        let reference = expected[field][0].as_f64().unwrap();
        assert!(
            (actual - reference).abs() < 1e-10,
            "{field}: CASA {reference}, native {actual}"
        );
    }
}
