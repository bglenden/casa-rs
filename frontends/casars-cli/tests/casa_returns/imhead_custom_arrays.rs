// SPDX-License-Identifier: LGPL-3.0-or-later
//! Custom-array writes and deletes compared with CASA's stored metadata.
use super::*;
use casa_types::ArrayValue;

#[test]
fn custom_array_sequences_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("custom_array_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("array-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        for op in case["operations"].as_array().unwrap() {
            let before = casa_images::PagedImage::<f32>::open(&path)
                .unwrap()
                .history()
                .unwrap()
                .len();
            let (output, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": op["mode"], "hdkey": "custom", "hdvalue": op["value"]}),
            );
            if op.get("error").is_some() {
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
                    "{index}: {reply:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let expected = op["history"].as_array().unwrap();
            assert_eq!(
                history.len() - before,
                expected.len(),
                "{index}: history count"
            );
            if !expected.is_empty() {
                assert_eq!(history[before], expected[0], "{index}: history");
            }
            if expected.len() == 3 {
                let task = expected[2]
                    .as_str()
                    .unwrap()
                    .replace("<IMAGE>", path.to_str().unwrap());
                assert_eq!(history[before + 2], task, "{index}: task history");
            }
        }
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let info = image.misc_info();
        if let Some(expected) = case.get("stored") {
            let values = expected["data"].as_array().unwrap();
            let array = match expected["kind"].as_str().unwrap() {
                "b" => {
                    ArrayValue::from_bool_vec(values.iter().map(|v| v.as_bool().unwrap()).collect())
                }
                "i" => ArrayValue::from_i32_vec(
                    values
                        .iter()
                        .map(|v| i32::try_from(v.as_i64().unwrap()).unwrap())
                        .collect(),
                ),
                "f" => {
                    ArrayValue::from_f64_vec(values.iter().map(|v| v.as_f64().unwrap()).collect())
                }
                "U" => ArrayValue::from_string_vec(
                    values
                        .iter()
                        .map(|v| v.as_str().unwrap().to_string())
                        .collect(),
                ),
                other => panic!("unsupported oracle kind {other}"),
            };
            assert_eq!(
                info.get("custom"),
                Some(&casa_types::Value::Array(array)),
                "{index}: stored"
            );
            drop(image);
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": "custom"}),
            );
            let Some(TaskReply::Failed { message, .. }) = reply else {
                panic!("{index}: get {reply:?}")
            };
            assert!(
                message.contains(case["get_error"].as_str().unwrap()),
                "{index}: get {message}"
            );
        } else {
            assert!(info.get("custom").is_none(), "{index}: removed");
        }
    }
}

#[test]
fn casa_created_custom_arrays_are_read_by_the_image_layer() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    for (name, expected) in [
        ("bool", ArrayValue::from_bool_vec(vec![true, false])),
        ("int", ArrayValue::from_i32_vec(vec![1, 2, 3])),
        ("float", ArrayValue::from_f64_vec(vec![1.0, 2.5])),
        (
            "string",
            ArrayValue::from_string_vec(vec!["x".into(), "y".into()]),
        ),
        ("empty", ArrayValue::from_bool_vec(vec![])),
    ] {
        let image =
            casa_images::PagedImage::<f32>::open(root.join(format!("custom_array_{name}.im")))
                .unwrap();
        assert_eq!(
            image.misc_info().get("custom"),
            Some(&casa_types::Value::Array(expected)),
            "{name}"
        );
    }
}
