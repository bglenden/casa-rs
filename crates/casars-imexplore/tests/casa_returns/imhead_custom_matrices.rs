// SPDX-License-Identifier: LGPL-3.0-or-later
//! Custom matrices and matrices in records compared with CASA.
use super::*;

#[test]
fn custom_matrices_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("custom_matrix_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("matrix-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap()
            .len();
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode":case["mode"], "hdkey":"custom", "hdvalue":case["value"]}),
        );
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
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let history = image.history().unwrap();
        let expected = case["history"].as_array().unwrap();
        assert_eq!(
            history.len() - before,
            expected.len(),
            "{index}: history count"
        );
        assert_eq!(history[before], expected[0], "{index}: metadata history");
        let task = expected[2]
            .as_str()
            .unwrap()
            .replace("<IMAGE>", path.to_str().unwrap());
        assert_eq!(history[before + 2], task, "{index}: task history");
        if let Some(expected) = case.get("stored") {
            let info = image.misc_info();
            let stored = info.get("custom").unwrap();
            let value = casa_provider_contracts::casacore_value_to_casa(stored).unwrap();
            let (kind, shape) = match &value {
                CasaValue::BoolArray { shape, .. } => ("b", shape),
                CasaValue::IntArray {
                    dtype: IntDtype::Int64,
                    shape,
                    ..
                } => ("i", shape),
                CasaValue::FloatArray {
                    dtype: FloatDtype::Float64,
                    shape,
                    ..
                } => ("f", shape),
                CasaValue::StrArray { shape, .. } => ("U", shape),
                other => panic!("{index}: unexpected array type {other:?}"),
            };
            assert_eq!(kind, expected["kind"], "{index}: dtype");
            assert_eq!(json!(shape), expected["shape"], "{index}: shape");
            assert_eq!(value.to_plain_json(), expected["data"], "{index}: stored");
            drop(image);
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename":path, "mode":"get", "hdkey":"custom"}),
            );
            assert!(
                matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("Unhandled data type")),
                "{index}: get"
            );
        } else {
            drop(image);
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename":path, "mode":"get", "hdkey":"custom"}),
            );
            assert_eq!(value.to_plain_json(), case["get"], "{index}: record get");
        }
    }
}

#[test]
fn casa_created_standalone_matrices_keep_shape_and_values() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    for (name, expected) in [
        ("int", json!([[1, 2, 3], [4, 5, 6]])),
        ("string", json!([["a", "b", "c"], ["d", "e", "f"]])),
    ] {
        let image =
            casa_images::PagedImage::<f32>::open(root.join(format!("custom_matrix_{name}.im")))
                .unwrap();
        let info = image.misc_info();
        let value =
            casa_provider_contracts::casacore_value_to_casa(info.get("custom").unwrap()).unwrap();
        assert_eq!(value.to_plain_json(), expected, "{name}");
    }
}
