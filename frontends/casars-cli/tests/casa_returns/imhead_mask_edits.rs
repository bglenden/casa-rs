// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 `imhead` mask removal through the task executable.

use super::*;
use casa_images::PagedImage;

#[test]
fn imhead_deletes_one_named_mask_or_all_masks() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("masked.im");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["pix".into(), "pix".into()],
    ));
    let mut image = PagedImage::<f32>::create(vec![2, 2], coordinates, &path).unwrap();
    image
        .put_mask("good", &ArrayD::from_elem(IxDyn(&[2, 2]), true))
        .unwrap();
    image
        .put_mask("default", &ArrayD::from_elem(IxDyn(&[2, 2]), false))
        .unwrap();
    image.set_default_mask("default").unwrap();
    image.save().unwrap();
    drop(image);

    let run = |mode, value| {
        json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": mode, "hdkey": "masks", "hdvalue": value}),
        )
        .1
        .unwrap()
    };
    let masks = || {
        let TaskReply::Succeeded { value, .. } = run("get", "") else {
            panic!("get masks")
        };
        value.to_plain_json()
    };
    assert_eq!(masks(), json!(["good", "default"]));
    let TaskReply::Failed { message, .. } = run("del", "missing") else {
        panic!("unknown mask")
    };
    assert!(
        message.contains("No mask named missing found."),
        "{message}"
    );
    assert_eq!(masks(), json!(["good", "default"]));
    assert!(matches!(
        run("del", "default"),
        TaskReply::Succeeded {
            value: CasaValue::None,
            ..
        }
    ));
    assert_eq!(masks(), json!(["good"]));
    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert!(reopened.default_mask_name().is_none());
    assert!(reopened.get_mask().unwrap().is_none());
    drop(reopened);
    assert!(!path.join("default").exists());
    assert!(matches!(
        run("del", ""),
        TaskReply::Succeeded {
            value: CasaValue::None,
            ..
        }
    ));
    assert_eq!(masks(), json!([]));
    assert!(!path.join("good").exists());
}
