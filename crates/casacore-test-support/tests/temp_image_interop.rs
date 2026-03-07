// SPDX-License-Identifier: LGPL-3.0-or-later
//! TempImage Rust/C++ interop checks for Wave 13.

use casacore_coordinates::{CoordinateSystem, LinearCoordinate};
use casacore_images::{Image, ImageInfo, ImageType, TempImage};
use casacore_test_support::{
    cpp_backend_available, cpp_create_temp_image_materialized, cpp_read_image_data,
    cpp_read_image_default_mask, cpp_read_image_default_mask_name, cpp_read_image_info_object_name,
    cpp_read_image_info_type, cpp_read_image_shape, cpp_read_image_units,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

fn f32_array(shape: &[usize], data: Vec<f32>) -> ArrayD<f32> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), data).expect("shape/data should match")
}

fn bool_array(shape: &[usize], data: Vec<bool>) -> ArrayD<bool> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), data).expect("shape/data should match")
}

#[test]
fn rc_rust_temp_image_materializes_cpp_reads_metadata() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping rc_rust_temp_image_materializes_cpp_reads_metadata: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_temp_saved.image");
    let shape = vec![2usize, 2];
    let shape_i32 = vec![2i32, 2];
    let data = vec![1.0f32, 2.0, 3.0, 4.0];
    let mask = vec![true, false, true, true];

    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(Box::new(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["m".into(), "m".into()],
    )));

    let mut img = TempImage::<f32>::with_threshold(shape.clone(), coords, Some(1)).unwrap();
    img.put_slice(&f32_array(&shape, data.clone()), &[0, 0])
        .unwrap();
    img.set_units("K").unwrap();
    img.set_image_info(&ImageInfo {
        beam_set: Default::default(),
        image_type: ImageType::Intensity,
        object_name: "RustTemp".into(),
    })
    .unwrap();
    img.make_mask("flags", true, true).unwrap();
    img.put_mask("flags", &bool_array(&shape, mask.clone()))
        .unwrap();

    // Force the paged temp path and verify save_as() can auto-reopen.
    img.temp_close().unwrap();
    let _paged = img.save_as(&path).unwrap();

    assert_eq!(cpp_read_image_shape(&path).unwrap(), shape_i32);
    assert_eq!(cpp_read_image_data(&path, data.len()).unwrap(), data);
    assert_eq!(cpp_read_image_units(&path).unwrap(), "K");
    // Full cross-language CoordinateSystem reconstruction is still a broader
    // repo boundary, so this test focuses on the TempImage behaviors the Rust
    // and C++ backends already share today.
    assert_eq!(cpp_read_image_default_mask_name(&path).unwrap(), "flags");
    assert_eq!(
        cpp_read_image_default_mask(&path, mask.len()).unwrap(),
        mask
    );
    assert_eq!(cpp_read_image_info_object_name(&path).unwrap(), "RustTemp");
    assert_eq!(cpp_read_image_info_type(&path).unwrap(), "Intensity");
}

#[test]
fn cr_cpp_temp_image_materializes_rust_reads_metadata() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping cr_cpp_temp_image_materializes_rust_reads_metadata: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_temp_saved.image");
    let shape_i32 = vec![2i32, 2];
    let shape = vec![2usize, 2];
    let data = vec![10.0f32, 20.0, 30.0, 40.0];

    cpp_create_temp_image_materialized(&path, &shape_i32, &data, "Jy", "CppTemp", "Beam").unwrap();

    let img = Image::open(&path).unwrap();
    assert_eq!(img.shape(), &shape);
    assert_eq!(
        img.get().unwrap().as_slice_memory_order().unwrap().to_vec(),
        data
    );
    assert_eq!(img.units(), "Jy");
    assert_eq!(img.default_mask_name().as_deref(), Some("flags"));
    let got_mask = img
        .get_mask()
        .unwrap()
        .unwrap()
        .as_slice_memory_order()
        .unwrap()
        .to_vec();
    assert_eq!(got_mask, vec![true, true, true, true]);

    let info = img.image_info().unwrap();
    assert_eq!(info.object_name, "CppTemp");
    assert_eq!(info.image_type, ImageType::Beam);
}
