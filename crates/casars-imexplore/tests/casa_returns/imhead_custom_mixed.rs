// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA promotion of mixed numeric and string custom arrays.
use super::*;

#[test]
fn mixed_custom_arrays_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("custom_mixed_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("mixed-{index}.im"));
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
        let stored =
            casa_provider_contracts::casacore_value_to_casa(info.get("custom").unwrap()).unwrap();
        assert_eq!(stored.to_plain_json(), case["stored"], "{index}: stored");
        if let Some(shape) = case.get("shape") {
            assert!(
                matches!(&stored, CasaValue::StrArray { shape: actual, .. } if json!(actual) == *shape),
                "{index}: type and shape {stored:?}"
            );
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
