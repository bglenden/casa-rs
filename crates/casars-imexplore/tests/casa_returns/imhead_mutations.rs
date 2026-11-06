// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA comparisons for image header edits and history.

use super::*;

#[test]
fn imhead_edits_spectral_reference_frames() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("reffreq_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("reffreq-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let count = image.history().unwrap().len();
        drop(image);
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let expected = case["history"].as_array().unwrap();
            let tail = &history[count..];
            assert_eq!(tail.len(), expected.len(), "{index}: history count");
            if expected.len() == 3 {
                assert_eq!(tail[0], expected[0], "{index}: metadata history");
            }
            assert_eq!(
                tail[tail.len() - 1],
                expected[expected.len() - 1]
                    .as_str()
                    .unwrap()
                    .replace("<IMAGE>", &path.display().to_string()),
                "{index}: task history"
            );
        }
        if let Some(expected) = case.get("get") {
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": "reffreqtype"}),
            );
            assert_eq!(value.to_plain_json(), *expected, "{index}: get");
        }
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in [
            "axisnames",
            "axisunits",
            "incr",
            "refpix",
            "refval",
            "shape",
        ] {
            assert_header_value_near(&summary[key], &case["after"][key], index);
        }
    }
    for (index, case) in oracle["restfreq_cases"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let path = workspace.path().join(format!("multi-restfreq-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let before = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
        );
        assert_eq!(before.to_plain_json(), json!({"value": 1e9, "unit": "Hz"}));
        let result = returned(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "restfreq", "hdvalue": "1.5GHz"
            }),
        );
        assert_eq!(result, CasaValue::None);
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
        );
        assert_header_value_near(&value.to_plain_json(), &case["get"], index);
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let system = image.coordinates();
        let mut frequencies: Vec<_> = (0..system.n_coordinates())
            .filter_map(|i| match system.coordinate(i) {
                casa_coordinates::CoordinateModel::Spectral(spectral) => {
                    Some(spectral.rest_frequency())
                }
                _ => None,
            })
            .collect();
        frequencies.sort_by(f64::total_cmp);
        let mut expected: Vec<_> = case["frequencies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        expected.sort_by(f64::total_cmp);
        assert_header_value_near(&json!(frequencies), &json!(expected), index);
    }
}

#[test]
fn imhead_undefined_spectral_frame_is_not_mislabeled_by_fits_export() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("undefined.im");
    let fits = workspace.path().join("undefined.fits");
    copy_tree(&fixture, &image);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({
            "imagename": image, "mode": "put", "hdkey": "reffreqtype", "hdvalue": "UNDEFINED"
        }),
    );
    assert_eq!(value, CasaValue::None);
    casa_images::analysis::export_fits(&image, &fits, false, false).unwrap();
    let bytes = std::fs::read(fits).unwrap();
    let cards = bytes.as_chunks::<80>().0;
    assert!(cards.iter().any(|card| card.starts_with(b"RESTFRQ ")));
    assert!(!cards.iter().any(|card| card.starts_with(b"SPECSYS ")));
}

#[test]
fn imhead_edits_axis_units_and_rescales_coordinate_values() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("axis_unit_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("axis-unit-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": case["key"]}),
            );
            assert_header_value_near(&value.to_plain_json(), &case["get"], index);
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            assert_eq!(
                history[history.len() - 3],
                case["history"][0],
                "{index}: history"
            );
        }
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in [
            "axisnames",
            "axisunits",
            "incr",
            "refpix",
            "refval",
            "shape",
        ] {
            assert_header_value_near(&summary[key], &case["after"][key], index);
        }
        if let Some(expected) = case.get("restfreq") {
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
            );
            assert_header_value_near(&value.to_plain_json(), expected, index);
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let system = image.coordinates();
            let frequencies = (0..system.n_coordinates())
                .find_map(|i| match system.coordinate(i) {
                    casa_coordinates::CoordinateModel::Spectral(spectral) => {
                        Some(spectral.rest_frequencies())
                    }
                    _ => None,
                })
                .unwrap();
            assert_header_value_near(&json!(frequencies), &case["frequencies"], index);
        }
    }
}

#[test]
fn imhead_refuses_unit_spellings_not_supported_by_the_coordinate_reader() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("unit-spelling.im");
    copy_tree(&fixture, &path);
    let before = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "summary"}),
    );
    for unit in ["deg^1", "deg**1", "arcsec*1"] {
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "cunit1", "hdvalue": unit
            }),
        );
        assert!(
            matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")),
            "{unit}"
        );
    }
    let after = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "summary"}),
    );
    assert_eq!(before, after);
}

#[test]
fn imhead_validates_unit_axis_before_value_type() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("unit-errors.im");
    copy_tree(&fixture, &path);
    for (key, expected) in [("cunit3", "polarization axis"), ("cunit5", "invalid axis")] {
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": key, "hdvalue": 42
            }),
        );
        assert!(
            matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains(expected)),
            "{key}"
        );
    }
}

#[test]
fn imhead_refuses_non_hz_spectral_units_without_mutation() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let workspace = tempfile::tempdir().unwrap();
    for (source, key, unit) in [
        ("normal", "cunit4", "GHz"),
        ("ghz", "cunit4", "MHz"),
        ("transposed", "cunit2", "GHz"),
    ] {
        let path = workspace.path().join(format!("{source}.im"));
        copy_tree(&fixture.join(format!("{source}.im")), &path);
        let before = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": key, "hdvalue": unit
            }),
        );
        assert!(
            matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")),
            "{source}"
        );
        let after = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        );
        assert_eq!(before, after, "{source}");
    }
}

#[test]
fn imhead_spectral_unit_conversion_to_hz_remains_correct_for_fits_export() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/ghz.im");
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("spectral.im");
    let fits = workspace.path().join("spectral.fits");
    copy_tree(&fixture, &image);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({
            "imagename": image, "mode": "put", "hdkey": "cunit4", "hdvalue": "Hz"
        }),
    );
    assert_eq!(value, CasaValue::None);
    casa_images::analysis::export_fits(&image, &fits, false, false).unwrap();
    let bytes = std::fs::read(fits).unwrap();
    let card = bytes
        .as_chunks::<80>()
        .0
        .iter()
        .find(|card| card.starts_with(b"RESTFRQ "))
        .unwrap();
    let number = std::str::from_utf8(&card[10..])
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .trim()
        .parse::<f64>()
        .unwrap();
    assert_eq!(number, 1.4e9);
}

fn assert_header_value_near(actual: &Value, expected: &Value, case: usize) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            assert!(
                (a - b).abs() <= b.abs().max(1.0) * 5e-14,
                "{case}: {a} != {b}"
            );
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{case}");
            for (a, b) in a.iter().zip(b) {
                assert_header_value_near(a, b, case);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len(), "{case}");
            for (key, value) in a {
                assert_header_value_near(value, &b[key], case);
            }
        }
        _ => assert_eq!(actual, expected, "{case}"),
    }
}

#[test]
fn imhead_edits_coordinate_values_as_casa_quantities() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("axis_quantity_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("axis-quantity-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": case["key"]}),
            );
            assert_header_value_near(&value.to_plain_json(), &case["get"], index);
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            assert_eq!(
                history[history.len() - 3],
                case["history"][0],
                "{index}: history"
            );
        }
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in [
            "axisnames",
            "axisunits",
            "incr",
            "refpix",
            "refval",
            "shape",
        ] {
            assert_header_value_near(&summary[key], &case["after"][key], index);
        }
    }
}

#[test]
fn imhead_refuses_nonzenithal_reference_value_edits_without_mutation() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let workspace = tempfile::tempdir().unwrap();
    for projection in ["car", "sfl", "mer", "ait"] {
        let path = workspace.path().join(format!("{projection}.im"));
        copy_tree(&fixture.join(format!("{projection}.im")), &path);
        let before = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        );
        for key in ["crval1", "crval2", "crval4"] {
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({
                    "imagename": path, "mode": "put", "hdkey": key, "hdvalue": "3deg"
                }),
            );
            assert!(
                matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")),
                "{projection}: {key}"
            );
        }
        let after = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        );
        assert_eq!(before, after, "{projection}");
    }
}

#[test]
fn imhead_refuses_direction_increment_underflow_without_mutation() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("tiny.im");
    copy_tree(&fixture, &path);
    let before = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "summary"}),
    );
    for value in ["1e-322deg", "1e-320deg"] {
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "cdelt1", "hdvalue": value
            }),
        );
        assert!(matches!(reply, Some(TaskReply::Failed { .. })), "{value}");
    }
    let after = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "summary"}),
    );
    assert_eq!(before, after);
}

#[test]
fn imhead_edits_rest_frequency_as_a_casa_quantity() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("restfreq_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("restfreq-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "restfreq", "hdvalue": case["value"]
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            assert_eq!(
                history[history.len() - 3],
                case["history"][0],
                "{index}: history"
            );
        }
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
        );
        assert_eq!(value.to_plain_json(), case["get"], "{index}: get");
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let system = image.coordinates();
        let frequencies = (0..system.n_coordinates())
            .find_map(|i| match system.coordinate(i) {
                casa_coordinates::CoordinateModel::Spectral(spectral) => {
                    Some(spectral.rest_frequencies())
                }
                _ => None,
            })
            .unwrap();
        let expected = case["frequencies"].as_array().unwrap();
        assert_eq!(
            frequencies.len(),
            expected.len(),
            "{index}: frequency count"
        );
        for (actual, expected) in frequencies.iter().zip(expected) {
            let expected = expected.as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= expected.abs().max(1.0) * 1e-15,
                "{index}: rest frequency {actual} != {expected}"
            );
        }
    }
}

#[test]
fn imhead_edits_coordinate_headers_on_normal_and_transposed_images() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("crpix-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]}),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": case["key"]}),
            );
            assert_eq!(value.to_plain_json(), case["get"], "{index}: get");
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let tail = &history[history.len() - 3..];
            assert_eq!(tail[0], case["history"][0], "{index}");
            assert_eq!(
                tail[2],
                case["history"][2]
                    .as_str()
                    .unwrap()
                    .replace("<IMAGE>", &path.display().to_string()),
                "{index}"
            );
        }
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in [
            "axisnames",
            "axisunits",
            "incr",
            "refpix",
            "refval",
            "shape",
        ] {
            assert_eq!(summary[key], case["after"][key], "{index}: {key}");
        }
    }
}

#[test]
fn imhead_refuses_unimplemented_variants_without_mutating_metadata() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/base.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("variants.im");
    copy_tree(&fixture, &path);
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "put", "hdkey": "ctype1", "hdvalue": "a\0b"}),
    );
    assert!(matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")));
    for value in [
        json!(1_i64 << 40),
        json!([1, 2]),
        json!({"unit": "Hz", "value": 1.5}),
    ] {
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": "custom", "hdvalue": value}),
        );
        assert!(
            matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977"))
        );
    }
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "get", "hdkey": "custom"}),
    );
    assert!(matches!(reply, Some(TaskReply::Failed { .. })));
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "put", "hdkey": "object", "hdvalue": 42}),
    );
    assert!(matches!(
        reply,
        Some(TaskReply::Failed {
            kind: casa_provider_contracts::TaskFailureKind::Execution,
            ..
        })
    ));
    returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "put", "hdkey": "object", "hdvalue": "preserved"}),
    );
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "del", "hdkey": "object", "hdvalue": [1, 2]}),
    );
    assert!(matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")));
    assert_eq!(
        returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "object"})
        )
        .to_plain_json(),
        "preserved"
    );
}

#[test]
fn imhead_cli_variant_preserves_literal_numeric_and_empty_strings() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("variant-cli.im");
    write_ramp(&path, false);
    for value in ["42", "true", "", "  "] {
        let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
            .args([
                "imhead",
                path.to_str().unwrap(),
                "--mode",
                "put",
                "--hdkey",
                "object",
                "--hdvalue",
                value,
                "--no-save-last",
            ])
            .current_dir(workspace.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{value:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": "object"})
            )
            .to_plain_json(),
            json!(value)
        );
    }
}

#[test]
fn imhead_does_not_round_out_of_range_json_integers_into_float_values() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/base.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("overflow.im");
    copy_tree(&fixture, &path);
    for value in [
        "9223372036854775808",
        "-9223372036854775809",
        "[0, 9223372036854775808]",
        "{\"nested\": -9223372036854775809}",
    ] {
        let request = format!(
            "{{\"surface\":\"imhead\",\"values\":{{\"imagename\":{},\"mode\":\"put\",\"hdkey\":\"custom\",\"hdvalue\":{value}}}}}",
            serde_json::to_string(&path).unwrap()
        );
        let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
            .args(["imhead", "--json-run", "-"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        write!(child.stdin.take().unwrap(), "{request}").unwrap();
        let output = child.wait_with_output().unwrap();
        let reply: TaskReply = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            matches!(reply, TaskReply::Failed { message, .. } if message.contains("signed 64-bit range")),
            "{value}"
        );
    }
    let request = format!(
        "{{\"surface\":\"imhead\",\"values\":{{\"imagename\":{},\"mode\":\"put\",\"hdkey\":\"zero\",\"hdvalue\":-0}}}}",
        serde_json::to_string(&path).unwrap()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args(["imhead", "--json-run", "-"])
        .current_dir(workspace.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    write!(child.stdin.take().unwrap(), "{request}").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let zero = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "get", "hdkey": "zero"}),
    );
    assert!(matches!(zero, CasaValue::Int { value: 0 }));
}

#[test]
fn imhead_custom_string_keys_follow_casa_casing_and_history() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("custom_expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, sequence) in expected["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("custom-{index}.im"));
        copy_tree(&fixture.join("base.im"), &path);
        for case in sequence.as_array().unwrap() {
            let mode = case["mode"].as_str().unwrap();
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": mode, "hdkey": case["key"], "hdvalue": case["value"]}),
            );
            if case.get("error").is_some() {
                assert!(
                    matches!(
                        reply,
                        Some(TaskReply::Failed {
                            kind: casa_provider_contracts::TaskFailureKind::Execution,
                            ..
                        })
                    ),
                    "{case}: {reply:?}"
                );
            } else {
                let Some(TaskReply::Succeeded { value, .. }) = reply else {
                    panic!("{case}: {reply:?}");
                };
                assert_eq!(value.to_plain_json(), case["return"], "{case}");
                if mode != "get" {
                    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
                    let history = image.history().unwrap();
                    let tail = &history[history.len() - 3..];
                    assert_eq!(tail[0], case["history"][0], "{case}");
                    assert_eq!(
                        tail[2],
                        case["history"][2]
                            .as_str()
                            .unwrap()
                            .replace("<IMAGE>", &path.display().to_string()),
                        "{case}"
                    );
                }
            }
        }
    }
    let numeric = workspace.path().join("numeric.im");
    copy_tree(&fixture.join("numeric_custom.im"), &numeric);
    for key in ["integer", "real"] {
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": numeric, "mode": "get", "hdkey": key})
            )
            .to_plain_json(),
            expected["numeric"][key]
        );
    }
}

#[test]
fn imhead_add_and_delete_string_fields_follow_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("add_del_expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in expected["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("{index}.im"));
        copy_tree(&fixture.join("base.im"), &path);
        let key = case["key"].as_str().unwrap();
        returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": key, "hdvalue": case["initial"]}),
        );
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": case["mode"], "hdkey": key, "hdvalue": case["value"]}),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            assert!(
                matches!(
                    reply,
                    Some(TaskReply::Succeeded {
                        value: CasaValue::None,
                        ..
                    })
                ),
                "{index}: {reply:?}"
            );
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let tail = &history[history.len() - 3..];
            assert_eq!(tail[0], case["history"][0], "{index}");
            assert_eq!(
                tail[2],
                case["history"][2]
                    .as_str()
                    .unwrap()
                    .replace("<IMAGE>", &path.display().to_string()),
                "{index}"
            );
        }
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": key})
            )
            .to_plain_json(),
            case["readback"],
            "{index}"
        );
    }
}

#[test]
fn imhead_put_history_keeps_the_given_relative_image_path() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("relative.im");
    write_ramp(&path, false);
    returned(
        workspace.path(),
        "imhead",
        json!({"imagename": "./relative.im", "mode": "put", "hdkey": "object", "hdvalue": "M31"}),
    );
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    let history = image.history().unwrap();
    assert_eq!(
        history.last().unwrap(),
        "imhead(imagename=\"./relative.im\", mode=\"put\", hdkey=\"object\", hdvalue=\"M31\", verbose=False)"
    );
}

#[test]
fn imhead_uses_casas_default_type_when_imageinfo_is_missing() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("missing.im");
    copy_tree(&fixture.join("without_imageinfo.im"), &path);
    assert_eq!(
        returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "imtype"})
        )
        .to_plain_json(),
        expected["missing_imageinfo"]["initial_imtype"]
    );
    returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode": "put", "hdkey": "imtype", "hdvalue": "Beam"}),
    );
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    let history = image.history().unwrap();
    assert_eq!(
        history[history.len() - 3],
        expected["missing_imageinfo"]["history"]
    );
    let unusual = workspace.path().join("unusual.im");
    copy_tree(&fixture.join("unusual_imtype.im"), &unusual);
    assert_eq!(
        returned(
            workspace.path(),
            "imhead",
            json!({"imagename": unusual, "mode": "get", "hdkey": "imtype"})
        )
        .to_plain_json(),
        expected["unusual_imtype"]
    );
}

#[test]
fn imhead_history_replays_casa_log_messages_with_original_times_and_origins() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/base.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("history.im");
    copy_tree(&fixture, &path);
    let mut expected = casa_images::image::image_history_entries(&path).unwrap();
    assert!(!expected.is_empty());
    assert!(expected[0].location.is_empty());
    for entry in &mut expected {
        entry.location = entry.replay_location();
    }
    let output_root = workspace.path().join("replayed");
    std::fs::create_dir(&output_root).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args(["imhead", "--json-run", "-"])
        .env("CASA_RS_LOG_TABLE", output_root.join("logtable"))
        .current_dir(workspace.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    write!(
        child.stdin.take().unwrap(),
        "{}",
        json!({"surface": "imhead", "values": {"imagename": path, "mode": "history"}})
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: TaskReply = serde_json::from_slice(&output.stdout).unwrap();
    assert!(matches!(
        reply,
        TaskReply::Succeeded {
            value: CasaValue::None,
            ..
        }
    ));
    let replayed = casa_images::image::image_history_entries(&output_root).unwrap();
    assert!(
        replayed
            .windows(expected.len())
            .any(|window| window == expected),
        "{replayed:?}"
    );
}

#[test]
fn imhead_history_does_not_require_summary_metadata_to_be_understood() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/unusual_imtype.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("history.im");
    copy_tree(&fixture, &path);
    let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imhead",
            path.to_str().unwrap(),
            "--mode",
            "history",
            "--log-stderr-priority",
            "INFO",
            "--no-save-last",
        ])
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("Created Paged image"));
}

#[test]
fn imhead_history_is_visible_by_default_and_respects_explicit_filtering() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/base.im");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.parent().unwrap().join("history_expected.json")).unwrap(),
    )
    .unwrap();
    let expected: Vec<&str> = oracle["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|line| line.as_str().unwrap())
        .collect();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("history.im");
    copy_tree(&fixture, &path);
    for config in [
        "default",
        "off",
        "table_priority_only",
        "cli_off",
        "cli_table_priority_only",
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_imexplore"));
        command
            .args(["imhead", "--json-run", "-"])
            .current_dir(workspace.path())
            .env_remove("CASA_RS_LOG_TABLE")
            .env_remove("CASA_RS_LOG_TABLE_PRIORITY")
            .env_remove("CASA_RS_LOG_STDERR_PRIORITY")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if config == "off" {
            command.env("CASA_RS_LOG_STDERR_PRIORITY", "off");
        } else if config == "table_priority_only" {
            command.env("CASA_RS_LOG_TABLE_PRIORITY", "WARN");
        } else if config == "cli_off" {
            command.args(["--log-stderr-priority", "off"]);
        } else if config == "cli_table_priority_only" {
            command.args(["--log-table-priority", "WARN"]);
        }
        let mut child = command.spawn().unwrap();
        write!(
            child.stdin.take().unwrap(),
            "{}",
            json!({"surface":"imhead", "values":{"imagename":path, "mode":"history"}})
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        let reply: TaskReply = serde_json::from_slice(&output.stdout).unwrap();
        assert!(matches!(
            reply,
            TaskReply::Succeeded {
                value: CasaValue::None,
                ..
            }
        ));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.contains("Created Paged image"),
            !["off", "cli_off"].contains(&config),
            "{config}: {stderr}"
        );
        if !["off", "cli_off"].contains(&config) {
            assert_eq!(stderr.lines().collect::<Vec<_>>(), expected, "{config}");
        }
    }
}

#[test]
fn imhead_history_refuses_an_advertised_missing_logtable() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put/base.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("history.im");
    copy_tree(&fixture, &path);
    std::fs::remove_dir_all(path.join("logtable")).unwrap();
    assert!(casa_images::image::image_history_entries(&path).is_err());
}

#[test]
fn imhead_put_preserves_literal_strings_and_unrelated_beam_metadata() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_put");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in expected["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("case-{index}.im"));
        copy_tree(&fixture.join("base.im"), &path);
        let key = case["key"].as_str().unwrap();
        let value = case["value"].as_str().unwrap();
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": key, "hdvalue": value
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}"
            );
        } else {
            let Some(TaskReply::Succeeded { value: result, .. }) = reply else {
                panic!("{index}: {reply:?}")
            };
            assert_eq!(result, CasaValue::None, "{index}");
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let tail = &history[history.len() - 3..];
            assert_eq!(tail[0], case["history"][0], "{index}: metadata history");
            assert!(
                tail[1].starts_with("version: casa-rs "),
                "{index}: {}",
                tail[1]
            );
            let command = case["history"][2]
                .as_str()
                .unwrap()
                .replace("<IMAGE>", &path.display().to_string());
            assert_eq!(tail[2], command, "{index}: task history");
        }
        let actual = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": key}),
        );
        assert_eq!(
            actual.to_plain_json(),
            case["readback"],
            "{index}: {key} {value:?}"
        );
        let summary =
            returned(workspace.path(), "imhead", json!({"imagename": path})).to_plain_json();
        assert_eq!(
            summary["restoringbeam"], case["beam"],
            "{index}: beam metadata"
        );
    }
}
