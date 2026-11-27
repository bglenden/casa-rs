// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn filled_mask_uses_scratch_storage_with_paged_temporary_pixels() {
    let directory = tempfile::tempdir().unwrap();
    let mut image = TempImage::<f32>::new(
        vec![6, 5],
        CoordinateSystem::new(),
        TempStoragePolicy::Paged {
            scratch: ScratchSpace::Directory(directory.path().to_path_buf()),
        },
    )
    .unwrap();
    image.create_filled_mask("mask0", true).unwrap();
    assert!(matches!(
        image.find_named_mask("mask0").unwrap(),
        TempMask::Paged(_)
    ));
    let block = ArrayD::from_elem(IxDyn(&[2, 2]), false);
    image.put_mask_slice("mask0", &block, &[1, 2]).unwrap();
    image.rename_mask("mask0", "valid").unwrap();
    image.set_default_mask("valid").unwrap();

    let selected = image
        .get_mask_slice(&[0, 0], &[3, 3], &[2, 2])
        .unwrap()
        .unwrap();
    assert_eq!(
        selected.iter().copied().collect::<Vec<_>>(),
        vec![true, true, true, true, false, true, true, true, true]
    );

    let saved = image.save_as(directory.path().join("saved.im")).unwrap();
    assert_eq!(saved.default_mask_name().as_deref(), Some("valid"));
    assert_eq!(saved.get_mask().unwrap(), image.get_mask().unwrap());
}

#[test]
fn paged_anonymous_mask_uses_the_active_slice_and_survives_save_as() {
    let directory = tempfile::tempdir().unwrap();
    let mut image = TempImage::<f32>::new(
        vec![6, 5],
        CoordinateSystem::new(),
        TempStoragePolicy::Paged {
            scratch: ScratchSpace::Directory(directory.path().to_path_buf()),
        },
    )
    .unwrap();
    let name = image.create_filled_pixel_mask(false).unwrap();
    assert!(name.is_empty());
    assert!(image.mask_names().is_empty());
    assert!(matches!(image.anonymous_mask, Some(TempMask::Paged(_))));
    let block = ArrayD::from_elem(IxDyn(&[2, 2]), true);
    image.put_mask_slice(&name, &block, &[1, 2]).unwrap();
    let selected = image
        .get_mask_slice(&[0, 0], &[3, 3], &[2, 2])
        .unwrap()
        .unwrap();
    assert_eq!(
        selected.iter().copied().collect::<Vec<_>>(),
        vec![false, false, false, false, true, false, false, false, false]
    );

    let saved = image.save_as(directory.path().join("saved.im")).unwrap();
    assert_eq!(saved.default_mask_name().as_deref(), Some("mask0"));
    assert_eq!(saved.get_mask().unwrap(), image.get_mask().unwrap());
}
