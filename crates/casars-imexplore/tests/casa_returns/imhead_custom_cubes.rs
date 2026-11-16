// SPDX-License-Identifier: LGPL-3.0-or-later
//! Higher-dimensional custom arrays and record fields versus CASA.
use super::*;

#[test]
fn custom_cubes_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("custom_cube_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("cube-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap()
            .len();
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename":path,"mode":"put","hdkey":"custom","hdvalue":case["value"]}),
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
        assert_eq!(
            history[before], case["history"][0],
            "{index}: metadata history"
        );
        let task = case["history"][2]
            .as_str()
            .unwrap()
            .replace("<IMAGE>", path.to_str().unwrap());
        assert_eq!(history[before + 2], task, "{index}: task history");
        let info = image.misc_info();
        let value =
            casa_provider_contracts::casacore_value_to_casa(info.get("custom").unwrap()).unwrap();
        assert_eq!(value.to_plain_json(), case["stored"], "{index}: stored");
        if let Some(shape) = case.get("shape") {
            let (kind, actual) = match &value {
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
                other => panic!("{index}: unexpected array {other:?}"),
            };
            assert_eq!(json!(actual), *shape, "{index}: shape");
            assert_eq!(kind, case["kind"], "{index}: dtype");
        }
        drop(image);
        if let Some(expected) = case.get("get") {
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename":path,"mode":"get","hdkey":"custom"}),
            );
            assert_eq!(value.to_plain_json(), *expected, "{index}: record get");
        }
    }
}

#[test]
fn casa_created_cubes_keep_shape_and_values() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    for (name, expected) in [
        (
            "int",
            json!([
                [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]],
                [[13, 14, 15, 16], [17, 18, 19, 20], [21, 22, 23, 24]]
            ]),
        ),
        (
            "string",
            json!([
                [
                    ["a", "b", "c", "d"],
                    ["e", "f", "g", "h"],
                    ["i", "j", "k", "l"]
                ],
                [
                    ["m", "n", "o", "p"],
                    ["q", "r", "s", "t"],
                    ["u", "v", "w", "x"]
                ]
            ]),
        ),
    ] {
        let image =
            casa_images::PagedImage::<f32>::open(root.join(format!("custom_cube_{name}.im")))
                .unwrap();
        let info = image.misc_info();
        let value =
            casa_provider_contracts::casacore_value_to_casa(info.get("custom").unwrap()).unwrap();
        assert_eq!(value.to_plain_json(), expected, "{name}");
    }
}
