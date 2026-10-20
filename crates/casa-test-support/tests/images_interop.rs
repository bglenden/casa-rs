// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image interop tests: 2×2 cross-matrix (Rust-write/read × C++-write/read).
//!
//! Verifies that Rust `Image` and C++ `PagedImage<Float>` produce identical
//! on-disk formats and can read each other's images.

#![cfg(feature = "cpp-interop-tests")]

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, LinearCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType, TabularCoordinate,
};
use casa_images::{Image, PagedImage};
use casa_tables::{Table, TableOptions};
use casa_test_support::{ImageOracle, casacore_oracle_available, discover_casa_python};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{Complex32, Complex64, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Helper: generate ramp data for a given shape (flat Fortran-order sequence).
///
/// The returned Vec is a flat ramp `[0, 1, 2, ...]` that should be interpreted
/// as column-major (Fortran) storage. This matches C++ casacore's internal
/// array storage order, so `getStorage()` on a C++ Array filled with this data
/// returns the same flat sequence.
fn ramp_data(shape: &[usize]) -> Vec<f32> {
    let n: usize = shape.iter().product();
    (0..n).map(|i| i as f32).collect()
}

/// Create an ndarray in Fortran order from flat ramp data.
fn ramp_array(shape: &[usize]) -> ArrayD<f32> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), ramp_data(shape)).unwrap()
}

fn ramp_data_f64(shape: &[usize]) -> Vec<f64> {
    let n: usize = shape.iter().product();
    (0..n).map(|i| i as f64 * 0.5).collect()
}

fn ramp_data_complex32(shape: &[usize]) -> Vec<Complex32> {
    let n: usize = shape.iter().product();
    (0..n)
        .map(|i| Complex32::new(i as f32, -(i as f32) * 0.25))
        .collect()
}

fn ramp_data_complex64(shape: &[usize]) -> Vec<Complex64> {
    let n: usize = shape.iter().product();
    (0..n)
        .map(|i| Complex64::new(i as f64 * 0.5, -(i as f64) * 0.125))
        .collect()
}

fn direction_coords() -> CoordinateSystem {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, std::f64::consts::FRAC_PI_4],
        [-1.0e-4, 1.0e-4],
        [8.0, 8.0],
    ));
    coords
}

// ---------------------------------------------------------------------------
// RR: Rust-write, Rust-read
// ---------------------------------------------------------------------------

#[test]
fn rr_roundtrip_3d() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rr_test.image");
    let shape = vec![32, 32, 16];
    let data = ramp_data(&shape);

    // Write (Fortran-order array so flat data matches storage order)
    {
        let mut img = Image::create(shape.clone(), Default::default(), &path).unwrap();
        let arr = ramp_array(&shape);
        img.put_slice(&arr, &[0, 0, 0]).unwrap();
        img.set_units("Jy/beam").unwrap();
        img.save().unwrap();
    }

    // Read
    {
        let img = Image::open(&path).unwrap();
        assert_eq!(img.shape(), &shape);
        assert_eq!(img.units(), "Jy/beam");
        let got = img.get().unwrap();
        let got_flat: Vec<f32> = got.as_slice_memory_order().unwrap().to_vec();
        assert_eq!(got_flat, data);
    }
}

// ---------------------------------------------------------------------------
// RC: Rust-write, C++-read
// ---------------------------------------------------------------------------

#[test]
fn rc_rust_write_cpp_read_3d() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_write_cpp_read_3d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_test.image");
    let shape = vec![16, 16, 8];
    let data = ramp_data(&shape);

    // Write with Rust (Fortran-order so C++ getStorage returns same flat data)
    {
        let mut img = Image::create(shape.clone(), Default::default(), &path).unwrap();
        let arr = ramp_array(&shape);
        img.put_slice(&arr, &[0, 0, 0]).unwrap();
        img.set_units("K").unwrap();
        img.save().unwrap();
    }

    // Read with C++
    let cpp_shape = ImageOracle::read_image_shape(&path).unwrap();
    assert_eq!(
        cpp_shape,
        shape.iter().map(|&s| s as i32).collect::<Vec<_>>()
    );

    let cpp_data = ImageOracle::read_image_data(&path, data.len()).unwrap();
    assert_eq!(cpp_data.len(), data.len());
    assert_eq!(cpp_data, data);

    let cpp_units = ImageOracle::read_image_units(&path).unwrap();
    assert_eq!(cpp_units, "K");
}

// ---------------------------------------------------------------------------
// CR: C++-write, Rust-read
// ---------------------------------------------------------------------------

#[test]
fn cr_cpp_write_rust_read_3d() {
    if !casacore_oracle_available() {
        eprintln!("skipping cr_cpp_write_rust_read_3d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_test.image");
    let shape_i32 = vec![16i32, 16, 8];
    let shape_usize: Vec<usize> = shape_i32.iter().map(|&s| s as usize).collect();
    let data = ramp_data(&shape_usize);

    // Write with C++
    ImageOracle::create_image(&path, &shape_i32, &data, "mJy/beam").unwrap();

    // Read with Rust
    let img = Image::open(&path).unwrap();
    assert_eq!(img.shape(), &shape_usize);
    assert_eq!(img.units(), "mJy/beam");

    // C++ stores data in Fortran (column-major) order. The flat ramp was
    // memcpy'd directly into that storage, so compare in memory order.
    let got = img.get().unwrap();
    let got_flat: Vec<f32> = got.as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got_flat, data);
}

#[test]
fn cr_casa_python_write_rust_read_4d_degenerate_cube() {
    let Some(casa) = discover_casa_python() else {
        eprintln!(
            "skipping cr_casa_python_write_rust_read_4d_degenerate_cube: CASA python unavailable"
        );
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_cube_4d.image");
    let shape = vec![8usize, 6, 1, 5];
    let script = r#"
import os
import numpy as np
from casatools import image

shape = (8, 6, 1, 5)
pixels = np.zeros(shape, dtype=np.float32)
for chan in range(shape[3]):
    for y in range(shape[1]):
        for x in range(shape[0]):
            pixels[x, y, 0, chan] = x + 10 * y + 1000 * chan

ia = image()
ia.fromarray(outfile=os.environ["CASA_IMAGE"], pixels=pixels)
ia.done()
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .env("CASA_IMAGE", &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let img = Image::open(&path).unwrap();
    assert_eq!(img.shape(), &shape);

    let got = img.get().unwrap();
    for chan in 0..shape[3] {
        for y in 0..shape[1] {
            for x in 0..shape[0] {
                let expected = x as f32 + 10.0 * y as f32 + 1000.0 * chan as f32;
                assert_eq!(got[[x, y, 0, chan]], expected);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CC: C++-write, C++-read (baseline correctness)
// ---------------------------------------------------------------------------

#[test]
fn cc_cpp_roundtrip_3d() {
    if !casacore_oracle_available() {
        eprintln!("skipping cc_cpp_roundtrip_3d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cc_test.image");
    let shape_i32 = vec![16i32, 16, 8];
    let shape_usize: Vec<usize> = shape_i32.iter().map(|&s| s as usize).collect();
    let data = ramp_data(&shape_usize);

    // Write with C++
    ImageOracle::create_image(&path, &shape_i32, &data, "Jy").unwrap();

    // Read with C++
    let cpp_data = ImageOracle::read_image_data(&path, data.len()).unwrap();
    assert_eq!(cpp_data, data);

    let cpp_units = ImageOracle::read_image_units(&path).unwrap();
    assert_eq!(cpp_units, "Jy");
}

// ---------------------------------------------------------------------------
// Metadata interop
// ---------------------------------------------------------------------------

#[test]
fn metadata_units_rust_to_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping metadata_units_rust_to_cpp: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta_rc.image");
    let shape = vec![4, 4];
    let data = vec![0.0f32; 16];

    {
        let mut img = Image::create(shape, Default::default(), &path).unwrap();
        let arr = ArrayD::from_shape_vec(IxDyn(&[4, 4]), data).unwrap();
        img.put_slice(&arr, &[0, 0]).unwrap();
        img.set_units("Jy/beam").unwrap();
        img.save().unwrap();
    }

    let units = ImageOracle::read_image_units(&path).unwrap();
    assert_eq!(units, "Jy/beam");
}

#[test]
fn rc_rust_direction_image_cpp_read_2d() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_direction_image_cpp_read_2d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_direction.image");
    let shape = vec![16usize, 16usize];
    let data = ramp_data(&shape);

    {
        let mut img = Image::create(shape.clone(), direction_coords(), &path).unwrap();
        let arr = ramp_array(&shape);
        img.put_slice(&arr, &[0, 0]).unwrap();
        img.set_units("Jy/beam").unwrap();
        img.save().unwrap();
    }

    let cpp_shape = ImageOracle::read_image_shape(&path).unwrap();
    assert_eq!(cpp_shape, vec![16, 16]);
    let cpp_data = ImageOracle::read_image_data(&path, data.len()).unwrap();
    assert_eq!(cpp_data, data);
    assert_eq!(ImageOracle::read_image_units(&path).unwrap(), "Jy/beam");
}

#[test]
fn rc_rust_all_coordinate_models_cpp_read() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_all_coordinate_models_cpp_read: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_all_coordinates.image");
    let shape = vec![4, 4, 2, 1, 3, 4];
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, std::f64::consts::FRAC_PI_4],
        [-1.0e-4, 1.0e-4],
        [2.0, 2.0],
    ));
    coordinates.add_coordinate(LinearCoordinate::new(
        1,
        vec!["TIME".into()],
        vec!["s".into()],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.42e9,
        1.0e6,
        0.0,
        1.42040575e9,
    ));
    coordinates.add_coordinate(TabularCoordinate::new(
        vec![0.0, 1.0, 2.0, 3.0],
        vec![10.0, 20.0, 30.0, 40.0],
        "CUSTOM",
        "m",
    ));

    let mut image = Image::create(shape, coordinates, &path).unwrap();
    image.save().unwrap();

    assert_eq!(ImageOracle::read_image_coordinate_count(&path).unwrap(), 5);
}

#[test]
fn cpp_image_opens_without_crash() {
    if !casacore_oracle_available() {
        eprintln!("skipping cpp_image_opens_without_crash: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_open.image");
    let shape_i32 = vec![8i32, 8];
    let data = vec![1.0f32; 64];

    ImageOracle::create_image(&path, &shape_i32, &data, "K").unwrap();

    // Rust can open C++ image without crashing.
    let img = Image::open(&path).unwrap();
    assert_eq!(img.shape(), &[8, 8]);
    // Coordinates exist (C++ wrote them), but we don't crash reading them.
    let _cs = img.coordinates();
}

// ---------------------------------------------------------------------------
// Larger cube interop (512³ if enabled, 64³ default for CI speed)
// ---------------------------------------------------------------------------

#[test]
fn rc_cube_interop() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_cube_interop: C++ casacore not available");
        return;
    }

    // Use a smaller cube for CI (64³ = 262144 pixels ≈ 1 MB).
    // Set CASA_RS_LARGE_INTEROP=1 to run with 128³.
    let size: usize = if std::env::var("CASA_RS_LARGE_INTEROP").is_ok() {
        128
    } else {
        64
    };
    let shape = vec![size, size, size];
    let n: usize = shape.iter().product();
    let data: Vec<f32> = (0..n).map(|i| (i % 1000) as f32 * 0.001).collect();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cube_rc.image");

    // Write with Rust (Fortran order so C++ getStorage matches flat data)
    {
        let mut img = Image::create(shape.clone(), Default::default(), &path).unwrap();
        let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), data.clone()).unwrap();
        img.put_slice(&arr, &[0, 0, 0]).unwrap();
        img.save().unwrap();
    }

    // Read with C++
    let cpp_data = ImageOracle::read_image_data(&path, n).unwrap();
    assert_eq!(cpp_data.len(), n);
    for (i, (&got, &expected)) in cpp_data.iter().zip(data.iter()).enumerate() {
        assert!(
            (got - expected).abs() < 1e-6,
            "pixel {i}: got {got}, expected {expected}"
        );
    }
}

#[test]
fn rc_rust_write_cpp_read_f64() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_write_cpp_read_f64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_f64.image");
    let shape = vec![8, 4, 2];
    let data = ramp_data_f64(&shape);

    let mut img = PagedImage::<f64>::create(shape.clone(), Default::default(), &path).unwrap();
    let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), data.clone()).unwrap();
    img.put_slice(&arr, &[0, 0, 0]).unwrap();
    img.save().unwrap();

    let cpp = ImageOracle::read_image_data_f64(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_f64() {
    if !casacore_oracle_available() {
        eprintln!("skipping cr_cpp_write_rust_read_f64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_f64.image");
    let shape_i32 = vec![8, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_f64(&shape);

    ImageOracle::create_image_f64(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<f64>::open(&path).unwrap();
    let got: Vec<f64> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}

#[test]
fn rc_rust_write_cpp_read_complex32() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_write_cpp_read_complex32: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_c32.image");
    let shape = vec![4, 4, 2];
    let data = ramp_data_complex32(&shape);

    let mut img =
        PagedImage::<Complex32>::create(shape.clone(), Default::default(), &path).unwrap();
    let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), data.clone()).unwrap();
    img.put_slice(&arr, &[0, 0, 0]).unwrap();
    img.save().unwrap();

    let cpp = ImageOracle::read_image_data_complex32(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_complex32() {
    if !casacore_oracle_available() {
        eprintln!("skipping cr_cpp_write_rust_read_complex32: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_c32.image");
    let shape_i32 = vec![4, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_complex32(&shape);

    ImageOracle::create_image_complex32(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<Complex32>::open(&path).unwrap();
    let got: Vec<Complex32> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}

#[test]
fn rc_rust_write_cpp_read_complex64() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_write_cpp_read_complex64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_c64.image");
    let shape = vec![4, 4, 2];
    let data = ramp_data_complex64(&shape);

    let mut img =
        PagedImage::<Complex64>::create(shape.clone(), Default::default(), &path).unwrap();
    let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), data.clone()).unwrap();
    img.put_slice(&arr, &[0, 0, 0]).unwrap();
    img.save().unwrap();

    let cpp = ImageOracle::read_image_data_complex64(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_complex64() {
    if !casacore_oracle_available() {
        eprintln!("skipping cr_cpp_write_rust_read_complex64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_c64.image");
    let shape_i32 = vec![4, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_complex64(&shape);

    ImageOracle::create_image_complex64(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<Complex64>::open(&path).unwrap();
    let got: Vec<Complex64> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}

#[test]
fn rc_rust_write_cpp_reads_default_mask() {
    if !casacore_oracle_available() {
        eprintln!("skipping rc_rust_write_cpp_reads_default_mask: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_mask.image");
    ImageOracle::create_image(&path, &[5, 5], &[0.0; 25], "").unwrap();

    let roi = ArrayD::from_shape_fn(IxDyn(&[5, 5]), |index| {
        (1..=3).contains(&index[0]) && (1..=3).contains(&index[1])
    });
    let mut image = Image::open(&path).unwrap();
    image.put_mask("roi", &roi).unwrap();
    image.set_default_mask("roi").unwrap();
    image.save().unwrap();

    assert_eq!(
        ImageOracle::read_image_default_mask_name(&path).unwrap(),
        "roi"
    );
    let mask = ImageOracle::read_image_default_mask(&path, 25).unwrap();
    assert_eq!(mask.len(), 25);
    assert_eq!(mask.iter().filter(|value| **value).count(), 9);
}

#[test]
fn cr_cpp_write_default_mask_rust_reads_it() {
    if !casacore_oracle_available() {
        eprintln!("skipping cr_cpp_write_default_mask_rust_reads_it: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_mask.image");
    ImageOracle::create_image(&path, &[4, 4], &[0.0; 16], "").unwrap();

    let mask = (0..16).map(|index| index % 2 == 0).collect::<Vec<_>>();
    ImageOracle::write_default_mask(&path, "cppmask", &mask).unwrap();

    let reopened = Image::open(&path).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("cppmask"));
    let rust_mask = reopened.get_named_mask("cppmask").unwrap();
    let rust_flat = rust_mask.as_slice_memory_order().unwrap().to_vec();
    assert_eq!(rust_flat, mask);
}

// ---------------------------------------------------------------------------
// Moved images: table keywords are stored relative to the image (#715)
// ---------------------------------------------------------------------------

/// Moves an image directory one level deeper under a different parent, so a
/// keyword resolved against the wrong anchor (the old location, or the
/// parent directory instead of the image) cannot find the mask by accident.
fn move_image(root: &Path, image: &Path) -> PathBuf {
    let parent = root.join("moved").join("deeper");
    std::fs::create_dir_all(&parent).unwrap();
    let moved = parent.join(image.file_name().unwrap());
    std::fs::rename(image, &moved).unwrap();
    moved
}

/// Writes a 4×4 casa-rs image with default mask `mask0` under `root/written/`.
fn write_rust_masked_image(root: &Path, mask: &ArrayD<bool>) -> PathBuf {
    let path = root.join("written").join("rust_mask.image");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut image = PagedImage::<f32>::create(vec![4, 4], direction_coords(), &path).unwrap();
    image.put_mask("mask0", mask).unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();
    path
}

/// Returns the stored `logtable` and `masks.<mask>.mask` table keywords.
fn stored_table_keywords(image: &Path, mask: &str) -> (Value, Value) {
    let table = Table::open(TableOptions::new(image)).unwrap();
    let keywords = table.keywords();
    let Some(Value::Record(masks)) = keywords.get("masks") else {
        panic!("{} has no masks keyword", image.display());
    };
    let Some(Value::Record(region)) = masks.get(mask) else {
        panic!("{} has no {mask} mask region", image.display());
    };
    (
        keywords.get("logtable").cloned().unwrap(),
        region.get("mask").cloned().unwrap(),
    )
}

#[test]
fn rc_moved_rust_masked_image_opens_with_its_mask() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping rc_moved_rust_masked_image_opens_with_its_mask: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let mask = ArrayD::from_shape_fn(IxDyn(&[4, 4]).f(), |idx| (idx[0] + 2 * idx[1]) % 3 != 0);
    let written = write_rust_masked_image(dir.path(), &mask);
    let moved = move_image(dir.path(), &written);

    assert_eq!(
        ImageOracle::read_image_default_mask_name(&moved).unwrap(),
        "mask0"
    );
    let cpp_mask = ImageOracle::read_image_default_mask(&moved, 16).unwrap();
    assert_eq!(cpp_mask, mask.as_slice_memory_order().unwrap());

    let reopened = PagedImage::<f32>::open(&moved).unwrap();
    assert_eq!(reopened.get_mask().unwrap(), Some(mask));
}

#[test]
fn cr_moved_cpp_masked_image_reads_its_mask_in_rust() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping cr_moved_cpp_masked_image_reads_its_mask_in_rust: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let written = dir.path().join("written").join("cpp_mask.image");
    std::fs::create_dir_all(written.parent().unwrap()).unwrap();
    ImageOracle::create_image(&written, &[4, 4], &[0.0; 16], "").unwrap();
    let mask = (0..16).map(|index| index % 3 != 1).collect::<Vec<_>>();
    ImageOracle::write_default_mask(&written, "mask0", &mask).unwrap();
    let moved = move_image(dir.path(), &written);

    let reopened = PagedImage::<f32>::open(&moved).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("mask0"));
    let rust_mask = reopened.get_named_mask("mask0").unwrap();
    assert_eq!(rust_mask.as_slice_memory_order().unwrap(), mask);
}

#[test]
fn rust_and_cpp_store_image_table_keywords_in_the_same_form() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping rust_and_cpp_store_image_table_keywords_in_the_same_form: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let cpp = dir.path().join("cpp_mask.image");
    ImageOracle::create_image(&cpp, &[4, 4], &[0.0; 16], "").unwrap();
    ImageOracle::write_default_mask(&cpp, "mask0", &[true; 16]).unwrap();
    let rust = write_rust_masked_image(dir.path(), &ArrayD::from_elem(IxDyn(&[4, 4]).f(), true));

    let (cpp_logtable, cpp_mask) = stored_table_keywords(&cpp, "mask0");
    assert_eq!(cpp_logtable, Value::table_ref("././logtable"));
    assert_eq!(cpp_mask, Value::table_ref("././mask0"));
    assert_eq!(
        stored_table_keywords(&rust, "mask0"),
        (cpp_logtable, cpp_mask)
    );
}
