// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction projection edits compared with CASA.
use super::*;

#[test]
fn projection_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("projection_edit_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("projection-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap()
            .len();
        let (output, reply) = json_run(
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
                "{index}: {reply:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let history = image.history().unwrap();
        assert_eq!(
            history.len() - before,
            case["history"].as_array().unwrap().len(),
            "{index}: history count"
        );
        if case["history"].as_array().unwrap().len() == 3 {
            assert_eq!(history[before], case["history"][0], "{index}: history");
        }
        let coordinates = image.coordinates();
        let direction = (0..coordinates.n_coordinates())
            .find_map(|i| match coordinates.coordinate(i) {
                casa_coordinates::CoordinateModel::Direction(direction) => Some(direction),
                _ => None,
            })
            .unwrap();
        for (pixel, expected) in [[0.0, 0.0], [2.5, 1.0]]
            .into_iter()
            .zip(case["world"].as_array().unwrap())
        {
            let actual = direction.pixel_to_world(pixel).unwrap();
            for (axis, (actual, expected)) in actual
                .into_iter()
                .zip(expected.as_array().unwrap())
                .enumerate()
            {
                let delta = actual - expected.as_f64().unwrap();
                // Longitudes differing by a full turn describe the same point.
                let delta = if axis == 0 {
                    (delta + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
                        - std::f64::consts::PI
                } else {
                    delta
                };
                assert!(delta.abs() < 1e-12, "{index}: world {actual} != {expected}");
            }
        }
        drop(image);
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "projection"}),
        );
        assert_eq!(value.to_plain_json(), case["get"], "{index}: projection");
    }
}

#[test]
fn unsupported_projection_transitions_leave_the_image_unchanged() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    {
        let target = "MOL";
        let path = workspace.path().join(format!("{target}.im"));
        copy_tree(&fixture, &path);
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap();
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": "projection", "hdvalue": target}),
        );
        assert!(
            matches!(reply, Some(TaskReply::Failed { .. })),
            "{target}: {reply:?}"
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("#977"));
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(image.history().unwrap(), before);
        drop(image);
        let projection = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "projection"}),
        );
        assert_eq!(projection.to_plain_json(), "SIN");
    }
}
