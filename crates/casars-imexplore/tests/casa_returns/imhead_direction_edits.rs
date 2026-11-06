// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction-reference edits, world coordinates and FITS headers versus CASA.
use super::*;

#[test]
fn direction_reference_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("direction_edit_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("direction-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        if let Some(initial) = case.get("initial") {
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename":path,"mode":"put","hdkey":"equinox","hdvalue":initial}),
            );
        }
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap()
            .len();
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename":path,"mode":"put","hdkey":case["key"],"hdvalue":case["value"]}),
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
        let expected = case["history"].as_array().unwrap();
        assert_eq!(
            history.len() - before,
            expected.len(),
            "{index}: history count"
        );
        if expected.len() == 3 {
            assert_eq!(history[before], expected[0], "{index}: metadata history");
        }
        let coords = image.coordinates();
        let direction = (0..coords.n_coordinates())
            .find_map(|i| match coords.coordinate(i) {
                casa_coordinates::CoordinateModel::Direction(direction) => Some(direction),
                _ => None,
            })
            .unwrap();
        for ((pixel, expected), expected_back) in [[0.0, 0.0], [2.5, 1.0]]
            .into_iter()
            .zip(case["world"].as_array().unwrap())
            .zip(case["inverse"].as_array().unwrap())
        {
            let world = direction.pixel_to_world(pixel).unwrap();
            for (actual, expected) in world.into_iter().zip(expected.as_array().unwrap()) {
                assert!(
                    (actual - expected.as_f64().unwrap()).abs() < 5e-11,
                    "{index}: world {actual} != {expected}"
                );
            }
            let back = direction.world_to_pixel(world).unwrap();
            for (actual, expected) in back.into_iter().zip(expected_back.as_array().unwrap()) {
                assert!(
                    (actual - expected.as_f64().unwrap()).abs() < 2e-6,
                    "{index}: inverse {actual} != {expected}"
                );
            }
        }
        if let Some(fits) = case.get("fits") {
            let header = casa_coordinates::fits::to_fits_header(coords, image.shape());
            for key in ["CTYPE1", "CTYPE2", "RADESYS"] {
                assert_eq!(
                    header.get_string(key),
                    fits.get(key).and_then(Value::as_str),
                    "{index}: {key}"
                );
            }
            assert_eq!(
                header.get_float("EQUINOX"),
                fits.get("EQUINOX").and_then(Value::as_f64),
                "{index}: equinox"
            );
        }
        drop(image);
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename":path,"mode":"get","hdkey":"equinox"}),
        );
        assert_eq!(value.to_plain_json(), case["get"], "{index}: frame");
        let position = returned(
            workspace.path(),
            "imhead",
            json!({"imagename":path,"mode":"get","hdkey":"minpos"}),
        );
        assert_eq!(
            position.to_plain_json(),
            case["minpos"],
            "{index}: minimum position"
        );
    }
}
