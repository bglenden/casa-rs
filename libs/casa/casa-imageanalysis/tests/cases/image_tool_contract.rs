// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imageanalysis::tool::attachment::remove_file;
use casa_imageanalysis::tool::create::{PixelArrayInput, create_from_array};
use casa_imageanalysis::tool::pixels::{ChunkData, PixelChunk, read_chunk_casa, write_chunk_casa};
use casa_imageanalysis::tool::slice::{SliceRecordResult, get_slice_attached};
use casa_imageanalysis::tool::{AttachedImage, no_external_open};
use casa_images::RegridMethod;
use ndarray::{ArrayD, IxDyn};

fn float_image(path: &str, pixels: ArrayD<f32>) -> AttachedImage {
    create_from_array(
        path,
        PixelArrayInput::Float(pixels),
        None,
        false,
        false,
        true,
        true,
        &no_external_open,
    )
    .unwrap()
    .0
}

#[cfg(unix)]
#[test]
fn releasing_unlocked_image_preserves_other_writers_metadata() {
    use std::{path::Path, process::Command};

    use casa_imageanalysis::tool::ImageToolState;
    use casa_imageanalysis::tool::locking::{has_lock, unlock_image};
    use casa_imageanalysis::tool::metadata::{brightness_unit, set_brightness_unit};

    const PEER_PATH: &str = "CASA_RS_TEST_IMAGE_UNLOCK_PEER";
    const PEER_UNIT: &str = "CASA_RS_TEST_IMAGE_UNLOCK_UNIT";
    if let Some(path) = std::env::var_os(PEER_PATH) {
        let image = AttachedImage::open(Path::new(&path)).unwrap();
        if let Ok(unit) = std::env::var(PEER_UNIT) {
            set_brightness_unit(&image, &unit, false).unwrap();
            unlock_image(&image).unwrap();
        }
        assert_eq!(brightness_unit(&image), "Jy/beam");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    for operation in ["unlock", "close", "done"] {
        let path = dir.path().join(format!("{operation}.im"));
        let image = float_image(path.to_str().unwrap(), ArrayD::zeros(IxDyn(&[4, 4])));
        set_brightness_unit(&image, "K", false).unwrap();
        unlock_image(&image).unwrap();
        assert_eq!(has_lock(&image, None).unwrap(), [false, false]);
        let peer = |unit: Option<&str>| {
            let mut cmd = Command::new(std::env::current_exe().unwrap());
            cmd.args([
                "image_tool_contract::releasing_unlocked_image_preserves_other_writers_metadata",
                "--exact",
                "--nocapture",
            ])
            .env(PEER_PATH, &path)
            .env_remove(PEER_UNIT);
            if let Some(unit) = unit {
                cmd.env(PEER_UNIT, unit);
            }
            let out = cmd.output().unwrap();
            assert!(
                out.status.success(),
                "{operation}: {} {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
            assert!(String::from_utf8_lossy(&out.stdout).contains("1 passed"));
        };
        peer(Some("Jy/beam"));
        assert_eq!(brightness_unit(&image), "K");
        match operation {
            "close" => assert!(ImageToolState::attached(image).close().unwrap()),
            "done" => assert!(
                ImageToolState::attached(image)
                    .done(false, false, &no_external_open)
                    .unwrap()
                    .0
            ),
            _ => assert!(unlock_image(&image).unwrap()),
        }
        peer(None);
    }
}

#[test]
fn removefile_refuses_members_of_open_image_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("open.im");
    let image = float_image(
        path.to_str().unwrap(),
        ArrayD::from_elem(IxDyn(&[2, 2]), 4.0),
    );
    let control = path.join("table.dat");
    assert!(!remove_file(&control, &no_external_open).unwrap());
    assert!(control.is_file());
    assert!(remove_file(&path, &no_external_open).is_err());
    drop(image);
    let reopened = AttachedImage::open(&path).unwrap();
    let chunk = read_chunk_casa(&reopened, &[], &[], &[], &[], false, false).unwrap();
    let ChunkData::Floats { values, .. } = chunk else {
        panic!("expected image pixels");
    };
    assert_eq!(values, [4.0; 4]);
}

#[test]
fn chunk_writes_and_cubic_slices_use_image_tool_semantics() {
    let image = float_image("", ArrayD::zeros(IxDyn(&[4, 2])));
    write_chunk_casa(
        &image,
        PixelChunk::Float(ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![7.0, 8.0]).unwrap()),
        &[0, 0],
        &[2, 1],
        false,
        true,
        None,
    )
    .unwrap();
    let chunk = read_chunk_casa(&image, &[], &[], &[], &[], false, false).unwrap();
    let ChunkData::Floats { values, .. } = chunk else {
        panic!("expected image pixels");
    };
    assert_eq!(values, [7.0, 0.0, 8.0, 0.0, 7.0, 0.0, 8.0, 0.0]);

    let image = float_image(
        "",
        ArrayD::from_shape_fn(IxDyn(&[8, 8]), |index| (index[0] * index[0]) as f32),
    );
    let slice = get_slice_attached(
        &image,
        &[2.5, 4.5],
        &[3.0, 3.0],
        [0, 1],
        &[0; 2],
        3,
        RegridMethod::Cubic,
    )
    .unwrap();
    let SliceRecordResult::Float { slice, .. } = slice else {
        panic!("expected float slice");
    };
    assert_eq!(slice.positions, [[2.5, 3.0], [3.5, 3.0], [4.5, 3.0]]);
    assert_eq!(slice.mask, [true; 3]);
    assert_eq!(slice.pixels, [6.25, 12.25, 20.25]);
}
