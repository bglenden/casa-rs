// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image interop tests: 2×2 cross-matrix (Rust-write/read × C++-write/read).
//!
//! Verifies that Rust `Image` and C++ `PagedImage<Float>` produce identical
//! on-disk formats and can read each other's images.

use casacore_images::{Image, PagedImage};
use casacore_test_support::cpp_backend_available;
use casacore_types::{Complex32, Complex64};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

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
    if !cpp_backend_available() {
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
    let cpp_shape = casacore_test_support::cpp_read_image_shape(&path).unwrap();
    assert_eq!(
        cpp_shape,
        shape.iter().map(|&s| s as i32).collect::<Vec<_>>()
    );

    let cpp_data = casacore_test_support::cpp_read_image_data(&path, data.len()).unwrap();
    assert_eq!(cpp_data.len(), data.len());
    assert_eq!(cpp_data, data);

    let cpp_units = casacore_test_support::cpp_read_image_units(&path).unwrap();
    assert_eq!(cpp_units, "K");
}

// ---------------------------------------------------------------------------
// CR: C++-write, Rust-read
// ---------------------------------------------------------------------------

#[test]
fn cr_cpp_write_rust_read_3d() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_rust_read_3d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_test.image");
    let shape_i32 = vec![16i32, 16, 8];
    let shape_usize: Vec<usize> = shape_i32.iter().map(|&s| s as usize).collect();
    let data = ramp_data(&shape_usize);

    // Write with C++
    casacore_test_support::cpp_create_image(&path, &shape_i32, &data, "mJy/beam").unwrap();

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

// ---------------------------------------------------------------------------
// CC: C++-write, C++-read (baseline correctness)
// ---------------------------------------------------------------------------

#[test]
fn cc_cpp_roundtrip_3d() {
    if !cpp_backend_available() {
        eprintln!("skipping cc_cpp_roundtrip_3d: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cc_test.image");
    let shape_i32 = vec![16i32, 16, 8];
    let shape_usize: Vec<usize> = shape_i32.iter().map(|&s| s as usize).collect();
    let data = ramp_data(&shape_usize);

    // Write with C++
    casacore_test_support::cpp_create_image(&path, &shape_i32, &data, "Jy").unwrap();

    // Read with C++
    let cpp_data = casacore_test_support::cpp_read_image_data(&path, data.len()).unwrap();
    assert_eq!(cpp_data, data);

    let cpp_units = casacore_test_support::cpp_read_image_units(&path).unwrap();
    assert_eq!(cpp_units, "Jy");
}

// ---------------------------------------------------------------------------
// Metadata interop
// ---------------------------------------------------------------------------

#[test]
fn metadata_units_rust_to_cpp() {
    if !cpp_backend_available() {
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

    let units = casacore_test_support::cpp_read_image_units(&path).unwrap();
    assert_eq!(units, "Jy/beam");
}

#[test]
fn cpp_image_opens_without_crash() {
    if !cpp_backend_available() {
        eprintln!("skipping cpp_image_opens_without_crash: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_open.image");
    let shape_i32 = vec![8i32, 8];
    let data = vec![1.0f32; 64];

    casacore_test_support::cpp_create_image(&path, &shape_i32, &data, "K").unwrap();

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
    if !cpp_backend_available() {
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
    let cpp_data = casacore_test_support::cpp_read_image_data(&path, n).unwrap();
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
    if !cpp_backend_available() {
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

    let cpp = casacore_test_support::cpp_read_image_data_f64(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_f64() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_rust_read_f64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_f64.image");
    let shape_i32 = vec![8, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_f64(&shape);

    casacore_test_support::cpp_create_image_f64(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<f64>::open(&path).unwrap();
    let got: Vec<f64> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}

#[test]
fn rc_rust_write_cpp_read_complex32() {
    if !cpp_backend_available() {
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

    let cpp = casacore_test_support::cpp_read_image_data_complex32(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_complex32() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_rust_read_complex32: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_c32.image");
    let shape_i32 = vec![4, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_complex32(&shape);

    casacore_test_support::cpp_create_image_complex32(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<Complex32>::open(&path).unwrap();
    let got: Vec<Complex32> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}

#[test]
fn rc_rust_write_cpp_read_complex64() {
    if !cpp_backend_available() {
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

    let cpp = casacore_test_support::cpp_read_image_data_complex64(&path, data.len()).unwrap();
    assert_eq!(cpp, data);
}

#[test]
fn cr_cpp_write_rust_read_complex64() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_rust_read_complex64: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_c64.image");
    let shape_i32 = vec![4, 4, 2];
    let shape: Vec<usize> = shape_i32.iter().map(|&v| v as usize).collect();
    let data = ramp_data_complex64(&shape);

    casacore_test_support::cpp_create_image_complex64(&path, &shape_i32, &data, "Jy").unwrap();
    let img = PagedImage::<Complex64>::open(&path).unwrap();
    let got: Vec<Complex64> = img.get().unwrap().as_slice_memory_order().unwrap().to_vec();
    assert_eq!(got, data);
}
