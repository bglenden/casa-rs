// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observation date writes compared with CASA and reopened by the image reader.

use super::*;

#[test]
fn observation_date_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_dates");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("edit_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("date-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: {reply:?}: {}",
                String::from_utf8_lossy(&output.stderr)
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
            assert_eq!(
                image
                    .coordinates()
                    .obs_info()
                    .date
                    .as_ref()
                    .unwrap()
                    .refer(),
                casa_types::measures::EpochRef::UTC
            );
        }
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "date-obs"}),
        );
        assert_eq!(value.to_plain_json(), case["get"], "{index}: date");
    }
}

#[test]
fn extreme_observation_date_does_not_modify_image() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_dates/noon.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("date.im");
    copy_tree(&fixture, &path);
    let before = casa_images::PagedImage::<f32>::open(&path)
        .unwrap()
        .history()
        .unwrap();
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({
            "imagename": path, "mode": "put", "hdkey": "epoch",
            "hdvalue": {"unit": "d", "value": 1.0e30}
        }),
    );
    assert!(matches!(reply, Some(TaskReply::Failed { .. })), "{reply:?}");
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(
        image
            .coordinates()
            .obs_info()
            .date
            .as_ref()
            .unwrap()
            .value()
            .as_mjd(),
        58849.5
    );
    assert_eq!(image.history().unwrap(), before);
}
