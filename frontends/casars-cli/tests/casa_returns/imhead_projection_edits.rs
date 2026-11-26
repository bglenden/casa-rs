// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction projection and increment edits compared with CASA.
use super::*;

#[test]
fn tiny_nonzero_direction_increment_matches_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("small-increment.im");
    copy_tree(&fixture, &path);
    assert_eq!(
        returned(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "cdelt1",
                "hdvalue": {"unit": "rad", "value": 1e-311}
            })
        ),
        CasaValue::None
    );
    let actual = returned(
        workspace.path(),
        "imhead",
        json!({
            "imagename": path, "mode": "get", "hdkey": "cdelt1"
        }),
    )
    .to_plain_json();
    assert_eq!(actual["unit"], "'");
    let ratio = actual["value"].as_f64().unwrap() / 3.4377467707847596e-308;
    assert!((ratio - 1.0).abs() < 1e-12, "{actual}");
    assert_eq!(
        returned(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "put", "hdkey": "cdelt1", "hdvalue": "1e-320deg"
            })
        ),
        CasaValue::None
    );
    let actual = returned(
        workspace.path(),
        "imhead",
        json!({
            "imagename": path, "mode": "get", "hdkey": "cdelt1"
        }),
    )
    .to_plain_json();
    let ratio = actual["value"].as_f64().unwrap() / 5.9436e-319;
    assert!((ratio - 1.0).abs() < 1e-3, "{actual}");
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    let casa_coordinates::CoordinateModel::Direction(direction) = image.coordinates().coordinate(0)
    else {
        panic!("direction coordinate")
    };
    assert!(direction.world_to_pixel([1.0, 0.0]).is_err());
}

#[test]
fn projection_edit_preserves_direction_frame_conversion() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("b1950.im");
    copy_tree(&fixture, &path);
    for (key, value) in [("equinox", "B1950"), ("projection", "TAN")] {
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({
                    "imagename": path, "mode": "put", "hdkey": key, "hdvalue": value
                })
            ),
            CasaValue::None
        );
    }
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    let coordinates = image.coordinates();
    let direction_index = coordinates
        .find_coordinate(casa_coordinates::CoordinateType::Direction)
        .unwrap();
    let casa_coordinates::CoordinateModel::Direction(direction) =
        coordinates.coordinate(direction_index)
    else {
        panic!("direction coordinate")
    };
    assert_eq!(direction.direction_ref().as_str(), "B1950");
    assert_eq!(direction.conversion_direction_ref().as_str(), "J2000");
    assert_eq!(direction.projection().name(), "TAN");
    let actual = direction.pixel_to_world([2.5, 1.0]).unwrap();
    let casa_world = [0.011037295753162814, 0.004567223198389743];
    for (actual, expected) in actual.into_iter().zip(casa_world) {
        assert!((actual - expected).abs() < 5e-11, "{actual} != {expected}");
    }
}

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
        if let Some(initial) = case.get("initial") {
            assert_eq!(
                returned(
                    workspace.path(),
                    "imhead",
                    json!({
                        "imagename": path, "mode": "put", "hdkey": "projection", "hdvalue": initial
                    })
                ),
                CasaValue::None
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
        let parameters = case["parameters"].as_array().unwrap();
        assert_eq!(
            direction.projection().parameters().len(),
            parameters.len(),
            "{index}"
        );
        for (actual, expected) in direction.projection().parameters().iter().zip(parameters) {
            assert_eq!(
                *actual,
                expected.as_f64().unwrap(),
                "{index}: projection parameters"
            );
        }
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
fn projection_edits_keep_unused_casa_parameters_across_successive_edits() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("inherited-parameters.im");
    copy_tree(&fixture, &path);
    // CASA 6.7.6.14 retains SZP's third parameter when editing this image
    // to AZP or AIR, even though those transforms do not use it.
    for projection in ["SZP", "AZP", "AIR"] {
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({
                    "imagename": path, "mode": "put", "hdkey": "projection", "hdvalue": projection
                })
            ),
            CasaValue::None
        );
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let casa_coordinates::CoordinateModel::Direction(direction) =
            image.coordinates().coordinate(0)
        else {
            panic!("direction coordinate")
        };
        assert_eq!(direction.projection().name(), projection);
        assert_eq!(direction.projection().parameters(), &[0.0, 0.0, 90.0]);
    }
}

#[test]
fn projection_edits_use_the_existing_celestial_transform() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    for (projection, expected) in [
        ("PAR", [6.280189143337153, 0.0033888896096286457]),
        ("MOL", [6.279857412384074, 0.003195077168228016]),
        ("PCO", [6.28018913976324, 0.0035488202169419547]),
        ("TSC", [6.279370510870037, 0.0045184548898045875]),
        ("CSC", [6.280411656385501, 0.003285284843149124]),
        ("QSC", [6.280669546360773, 0.003081870128923639]),
    ] {
        let path = workspace.path().join(format!("{projection}.im"));
        copy_tree(&fixture, &path);
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({
                    "imagename": path, "mode": "put", "hdkey": "projection", "hdvalue": projection
                })
            ),
            CasaValue::None
        );
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let casa_coordinates::CoordinateModel::Direction(direction) =
            image.coordinates().coordinate(0)
        else {
            panic!("direction coordinate")
        };
        assert_eq!(direction.projection().name(), projection);
        let actual = direction.pixel_to_world([12.3, 14.2]).unwrap();
        let delta = (actual[0] - expected[0] + std::f64::consts::PI)
            .rem_euclid(std::f64::consts::TAU)
            - std::f64::consts::PI;
        assert!(
            delta.abs() < 1e-8 && (actual[1] - expected[1]).abs() < 1e-8,
            "{projection}: {actual:?} != {expected:?}"
        );
    }
}

#[test]
fn invalid_zpn_polynomial_leaves_the_image_unchanged() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    {
        let target = "ZPN";
        let path = workspace.path().join(format!("{target}.im"));
        copy_tree(&fixture, &path);
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap();
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": "projection", "hdvalue": target}),
        );
        assert!(
            matches!(&reply, Some(TaskReply::Failed { message, .. }) if message.contains("Invalid ZPN polynomial coefficients")),
            "{target}: {reply:?}"
        );
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
