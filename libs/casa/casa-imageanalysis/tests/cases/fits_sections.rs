// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_coordinates::{CoordinateSystem, LinearCoordinate};
use casa_imageanalysis::analysis::{SubimageOptions, SubimagePlan};
use casa_images::{TempImage, TempStoragePolicy, fits_read::fits_section_image};
use ndarray::{ArrayD, IxDyn};
use std::{cell::Cell, path::Path};
#[test]
fn complex_product_reads_fits_masks_in_sections() {
    let shape = vec![1025, 1025];
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["".into(); 2],
    ));
    let metadata =
        TempImage::new(vec![1, 1], coordinates.clone(), TempStoragePolicy::Memory).unwrap();
    let largest = Cell::new(0);
    let image = fits_section_image(
        &shape,
        &metadata,
        Path::new("virtual.fits"),
        |origin, extent, _| {
            largest.set(largest.get().max(extent.iter().product()));
            let mut pixels = ArrayD::from_elem(IxDyn(extent), 1.0);
            if origin == [0, 0] {
                pixels[[0, 0]] = f32::NAN;
            }
            Ok(pixels)
        },
    );
    let plan = SubimagePlan::new(
        &image,
        None,
        SubimageOptions {
            region: None,
            dropdeg: false,
            keepaxes: &[],
        },
    )
    .unwrap();
    let mut product =
        TempImage::new(shape.clone(), coordinates, TempStoragePolicy::Memory).unwrap();
    plan.make_complex_to(&image, &image, &mut product, None)
        .unwrap();
    assert!(largest.get() > 0 && largest.get() < shape.iter().product());
    let mask = product.get_named_mask("mask0").unwrap();
    assert!(!mask[[0, 0]]);
    assert_eq!(mask.iter().filter(|&&valid| !valid).count(), 1);
}
