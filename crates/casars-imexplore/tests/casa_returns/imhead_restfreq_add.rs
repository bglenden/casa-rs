// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rest frequency additions compared with CASA.
use super::*;

#[test]
fn rest_frequency_additions_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("restfreq_add_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("rest-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "add", "hdkey": case["key"], "hdvalue": case["value"]
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
                "{index}: {reply:?}: {}",
                String::from_utf8_lossy(&output.stderr)
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
        assert_eq!(
            value.to_plain_json(),
            case["get"],
            "{index}: rest frequency"
        );
    }
}
