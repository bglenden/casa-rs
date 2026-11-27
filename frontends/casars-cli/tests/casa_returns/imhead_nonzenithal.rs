// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA comparisons for non-zenithal direction-coordinate edits.

use super::imhead_mutations::assert_header_value_near;
use super::*;

fn apply_direction_setup(path: &Path, setup: &Value) {
    use casa_core::{ArrayValue, ScalarValue, Value as TableValue};

    let mut image = casa_images::PagedImage::<f32>::open(path).unwrap();
    let mut coordinates = image.coordinates().to_record();
    let Some(TableValue::Record(direction)) = coordinates.get_mut("direction0") else {
        panic!("missing direction coordinate");
    };
    if let Some(frame) = setup["conversionSystem"].as_str() {
        direction.upsert(
            "conversionSystem",
            TableValue::Scalar(ScalarValue::String(frame.into())),
        );
    }
    for field in ["cdelt", "crpix", "projection_parameters"] {
        if let Some(values) = setup[field].as_array() {
            direction.upsert(
                field,
                TableValue::Array(ArrayValue::from_f64_vec(
                    values.iter().map(|value| value.as_f64().unwrap()).collect(),
                )),
            );
        }
    }
    if let Some(axes) = setup["worldmap0"].as_array() {
        let axes = ndarray::arr1(&[
            axes[0].as_i64().unwrap() as i32,
            axes[1].as_i64().unwrap() as i32,
        ]);
        coordinates.upsert(
            "worldmap0",
            TableValue::Array(ArrayValue::Int32(axes.into_dyn())),
        );
    }
    image.set_coordinate_record(coordinates).unwrap();
    image.save().unwrap();
}

fn assert_numbered_header_values(workspace: &Path, path: &Path, case: &Value, index: usize) {
    let Some(expected) = case["axis_get"].as_object() else {
        return;
    };
    for (key, expected) in expected {
        let actual = returned(
            workspace,
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": key}),
        )
        .to_plain_json();
        assert_header_value_near(&actual, expected, index);
    }
    let actual = returned(
        workspace,
        "imhead",
        json!({"imagename": path, "mode": "list"}),
    )
    .to_plain_json();
    for (key, expected) in case["axis_list"].as_object().unwrap() {
        assert_header_value_near(&actual[key], expected, index);
    }
}

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
        if let Some(setup) = case.get("setup") {
            apply_direction_setup(&path, setup);
        }
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
        if let Some(names) = case.get("axisnames") {
            assert_eq!(&summary["axisnames"], names, "case {index}");
        }
        assert_numbered_header_values(workspace.path(), &path, case, index);
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
        if let Some(frame) = case["conversion_system"].as_str() {
            assert_eq!(direction.conversion_direction_ref().as_str(), frame);
        }
        if let Some(parameters) = case.get("projection_parameters") {
            assert_eq!(
                json!(direction.projection().parameters()),
                *parameters,
                "case {index}"
            );
        }
        assert!(
            (direction.longpole().to_degrees() - case["longpole"].as_f64().unwrap()).abs() < 1e-8,
            "case {index}"
        );
        assert!(
            (direction.latpole().to_degrees() - case["latpole"].as_f64().unwrap()).abs() < 1e-8,
            "case {index}"
        );
        // Compare the WCS in its native frame. The CASA readback probe also
        // checks the converted positions after opening the saved table.
        let native_direction;
        let (direction, worlds) = if let Some(worlds) = case.get("native_worlds") {
            native_direction = direction
                .clone()
                .with_conversion_direction_ref(direction.direction_ref());
            (&native_direction, worlds)
        } else {
            (direction, &case["worlds"])
        };
        for (pixel, expected) in [[0.0, 0.0], [2.5, 1.0]]
            .into_iter()
            .zip(worlds.as_array().unwrap())
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
