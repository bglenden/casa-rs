// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA comparisons for non-zenithal direction-coordinate edits.

use super::imhead_mutations::assert_header_value_near;
use super::*;

#[test]
fn nonzenithal_projection_and_reference_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("nonzenithal_expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("nonzenithal-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let before = if case.get("error").is_some() {
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            Some((image.coordinates().to_record(), image.history().unwrap()))
        } else {
            None
        };
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]}),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "case {index}: {reply:?}"
            );
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let (coordinates, history) = before.unwrap();
            assert_eq!(image.coordinates().to_record(), coordinates, "case {index}");
            assert_eq!(image.history().unwrap(), history, "case {index}");
            continue;
        }
        assert!(
            matches!(
                reply,
                Some(TaskReply::Succeeded {
                    value: CasaValue::None,
                    ..
                })
            ),
            "case {index}: {reply:?}"
        );
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in ["axisunits", "refval", "refpix", "incr"] {
            assert_header_value_near(&summary[key], &case[key], index);
        }
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let direction = (0..image.coordinates().n_coordinates())
            .find_map(|i| match image.coordinates().coordinate(i) {
                casa_coordinates::CoordinateModel::Direction(direction) => Some(direction),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            direction.projection().name(),
            case["projection"].as_str().unwrap()
        );
        assert!(
            (direction.longpole().to_degrees() - case["longpole"].as_f64().unwrap()).abs() < 1e-8,
            "case {index}"
        );
        assert!(
            (direction.latpole().to_degrees() - case["latpole"].as_f64().unwrap()).abs() < 1e-8,
            "case {index}"
        );
        for (pixel, expected) in [[0.0, 0.0], [2.5, 1.0]]
            .into_iter()
            .zip(case["worlds"].as_array().unwrap())
        {
            let actual = direction.pixel_to_world(pixel).unwrap();
            for axis in 0..2 {
                let expected = expected[axis].as_f64().unwrap();
                let delta = actual[axis] - expected;
                let delta = if axis == 0 {
                    (delta + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
                        - std::f64::consts::PI
                } else {
                    delta
                };
                assert!(
                    delta.abs() < 1e-10,
                    "case {index}: axis {axis}: {} != {expected}",
                    actual[axis]
                );
            }
        }
    }
}
