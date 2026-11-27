// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};

#[test]
fn read_mask_validates_the_target_section_before_mapping_axes() {
    use casa_coordinates::defaults::coordinate_system_for_shape;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.image");
    let shape = [4, 4];
    let source = PagedImage::<f32>::create(
        shape.to_vec(),
        coordinate_system_for_shape(&shape, false),
        &path,
    )
    .unwrap();
    drop(source);
    let expression = format!("\"{}\" >= 0", path.display());
    let target_shape = [4, 4, 3];
    let coordinates = coordinate_system_for_shape(&target_shape, false);
    for (start, stride) in [
        (&[0][..], &[1, 1, 1][..]),
        (&[0, 0, 3][..], &[1, 1, 1][..]),
        (&[0, 0, 0][..], &[1, 1, 0][..]),
    ] {
        assert!(matches!(
            evaluate_image_tool_mask_section(
                &expression,
                &target_shape,
                &coordinates,
                (start, &[2, 2, 1], stride),
                true,
            ),
            Err(ImageError::Lattice(_))
        ));
    }
    let valid = evaluate_image_tool_mask_section(
        &expression,
        &target_shape,
        &coordinates,
        (&[1, 1, 1], &[2, 2, 2], &[1, 1, 1]),
        true,
    )
    .unwrap();
    assert_eq!(valid.shape(), &[2, 2, 2]);
    assert!(valid.iter().all(|&value| value));
}

#[test]
fn converted_operand_keeps_writer_out_between_sections() {
    const CHILD_CASE: &str =
        "analysis::lel_mask::tests::converted_operand_keeps_writer_out_between_sections";
    const PATH_KEY: &str = "CASA_RS_MASK_OPERAND_LOCK_TEST_PATH";
    const WRITE_KEY: &str = "CASA_RS_MASK_OPERAND_LOCK_TEST_WRITE";
    if let Some(path) = std::env::var_os(PATH_KEY) {
        let mut table = Table::open_with_lock(
            TableOptions::new(path),
            LockOptions::new(LockMode::UserLocking),
        )
        .unwrap();
        assert_eq!(
            table.lock(LockType::Write, 1).unwrap(),
            std::env::var_os(WRITE_KEY).is_some()
        );
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mask.image");
    let mut image = PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &path).unwrap();
    let mask = ArrayD::from_elem(ndarray::IxDyn(&[4, 4]), true);
    image.put_mask("selected", &mask).unwrap();
    image.set_default_mask("selected").unwrap();
    image.save().unwrap();
    drop(image);

    let check_peer = |can_write: bool| {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child.args(["--exact", CHILD_CASE]).env(PATH_KEY, &path);
        if can_write {
            child.env(WRITE_KEY, "yes");
        } else {
            child.env_remove(WRITE_KEY);
        }
        let output = child.output().unwrap();
        let report = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && report.contains("1 passed"),
            "{report}"
        );
    };
    for name in [None, Some("nomask".to_owned())] {
        let expect_mask = name.is_none();
        let AnyPagedImage::Float32(image) = open_mask_image(&path).unwrap() else {
            panic!("float image");
        };
        let source = convert_paged_mask_source(image, name, false, f64::from).unwrap();
        {
            let reader = source.as_interface();
            reader.get_slice(&[0, 0], &[2, 2], &[1, 1]).unwrap();
            check_peer(false);
            assert_eq!(
                reader
                    .get_mask_slice(&[2, 2], &[2, 2], &[1, 1])
                    .unwrap()
                    .is_some(),
                expect_mask
            );
            reader.get_slice(&[2, 2], &[2, 2], &[1, 1]).unwrap();
        }
        drop(source);
        check_peer(true);
    }
}
