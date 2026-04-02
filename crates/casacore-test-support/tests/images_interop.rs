// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image interop tests: 2×2 cross-matrix (Rust-write/read × C++-write/read).
//!
//! Verifies that Rust `Image` and C++ `PagedImage<Float>` produce identical
//! on-disk formats and can read each other's images.

use casacore_images::{Image, OpenedImageView, PagedImage};
use casacore_test_support::{CppUnsupportedRegionKind, cpp_backend_available};
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

#[test]
fn cr_cpp_write_rust_load_saved_polygon_region() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping cr_cpp_write_rust_load_saved_polygon_region: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_polygon_region.image");
    casacore_test_support::cpp_create_image(&path, &[5, 5], &[0.0; 25], "").unwrap();

    let opened = OpenedImageView::open(&path).unwrap();
    let window = opened.default_window();
    let pixels = [(1usize, 1usize), (3, 1), (2, 3)];
    let vertices = pixels
        .into_iter()
        .map(|pixel_xy| {
            opened
                .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
                .unwrap()
        })
        .collect::<Vec<_>>();
    let x = vertices
        .iter()
        .map(|vertex| vertex.world[0])
        .collect::<Vec<_>>();
    let y = vertices
        .iter()
        .map(|vertex| vertex.world[1])
        .collect::<Vec<_>>();
    casacore_test_support::cpp_write_polygon_region(&path, "cpp_poly", &x, &y).unwrap();

    let loaded = opened.load_saved_region("cpp_poly").unwrap();
    assert_eq!(loaded.label, "cpp_poly");
    assert_eq!(loaded.shapes.len(), 1);
    assert_eq!(loaded.shapes[0].vertices.len(), 3);
    let overlay = opened
        .region_overlay_with_window_and_axes(&loaded, &window, &[])
        .unwrap();
    assert_eq!(overlay.shapes.len(), 1);
    assert_eq!(overlay.shapes[0].vertices.len(), 3);
    assert!(overlay.shapes[0].closed);
}

#[test]
fn cr_cpp_write_rust_load_saved_union_region() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_rust_load_saved_union_region: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_union_region.image");
    casacore_test_support::cpp_create_image(&path, &[6, 6], &[0.0; 36], "").unwrap();

    let opened = OpenedImageView::open(&path).unwrap();
    let window = opened.default_window();
    let tri1 = [(1usize, 1usize), (2, 1), (1, 2)]
        .into_iter()
        .map(|pixel_xy| {
            opened
                .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
                .unwrap()
        })
        .collect::<Vec<_>>();
    let tri2 = [(4usize, 4usize), (5, 4), (4, 5)]
        .into_iter()
        .map(|pixel_xy| {
            opened
                .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
                .unwrap()
        })
        .collect::<Vec<_>>();
    let x1 = tri1
        .iter()
        .map(|vertex| vertex.world[0])
        .collect::<Vec<_>>();
    let y1 = tri1
        .iter()
        .map(|vertex| vertex.world[1])
        .collect::<Vec<_>>();
    let x2 = tri2
        .iter()
        .map(|vertex| vertex.world[0])
        .collect::<Vec<_>>();
    let y2 = tri2
        .iter()
        .map(|vertex| vertex.world[1])
        .collect::<Vec<_>>();
    casacore_test_support::cpp_write_union_region(&path, "cpp_union", &x1, &y1, &x2, &y2).unwrap();

    let loaded = opened.load_saved_region("cpp_union").unwrap();
    assert_eq!(loaded.shapes.len(), 2);
    assert!(loaded.shapes.iter().all(|shape| shape.closed));
}

#[test]
fn rc_rust_write_cpp_reads_saved_region_classes() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping rc_rust_write_cpp_reads_saved_region_classes: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_region_class.image");
    casacore_test_support::cpp_create_image(&path, &[6, 6], &[0.0; 36], "").unwrap();

    let opened = OpenedImageView::open(&path).unwrap();
    let window = opened.default_window();
    let mut region = opened.default_region("Region 1").unwrap();
    region.start_shape().unwrap();
    for pixel_xy in [(1usize, 1usize), (2, 1), (1, 2)] {
        let vertex = opened
            .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
            .unwrap();
        region.append_vertex(vertex).unwrap();
    }
    region.close_active_shape().unwrap();
    let polygon_name = opened.save_region_definition(&region, None).unwrap();
    assert_eq!(
        casacore_test_support::cpp_read_region_class(&path, &polygon_name).unwrap(),
        "WCPolygon"
    );

    region.start_shape().unwrap();
    for pixel_xy in [(4usize, 4usize), (5, 4), (4, 5)] {
        let vertex = opened
            .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
            .unwrap();
        region.append_vertex(vertex).unwrap();
    }
    region.close_active_shape().unwrap();
    let union_name = opened.save_region_definition(&region, None).unwrap();
    assert_eq!(
        casacore_test_support::cpp_read_region_class(&path, &union_name).unwrap(),
        "WCUnion"
    );
    assert_eq!(
        casacore_test_support::cpp_read_region_names(&path).unwrap(),
        vec![polygon_name, union_name]
    );
}

#[test]
fn cr_cpp_write_unsupported_region_reports_useful_error() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping cr_cpp_write_unsupported_region_reports_useful_error: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_box_region.image");
    casacore_test_support::cpp_create_image(&path, &[5, 5], &[0.0; 25], "").unwrap();

    casacore_test_support::cpp_write_box_region(&path, "cpp_box", 0.0, 0.0, 1.0, 1.0).unwrap();
    let opened = OpenedImageView::open(&path).unwrap();
    let error = opened.load_saved_region("cpp_box").unwrap_err();
    assert!(error.to_string().contains("saved region 'cpp_box'"));
    assert!(error.to_string().contains("WCBox"));
}

#[test]
fn cr_cpp_write_unsupported_region_matrix_reports_class_names() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping cr_cpp_write_unsupported_region_matrix_reports_class_names: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_unsupported_regions.image");
    casacore_test_support::cpp_create_image(&path, &[6, 6, 4], &[0.0; 144], "").unwrap();
    let opened = OpenedImageView::open(&path).unwrap();

    let cases = [
        (
            "cpp_ellipsoid",
            CppUnsupportedRegionKind::Ellipsoid,
            "WCEllipsoid",
        ),
        (
            "cpp_intersection",
            CppUnsupportedRegionKind::Intersection,
            "WCIntersection",
        ),
        (
            "cpp_difference",
            CppUnsupportedRegionKind::Difference,
            "WCDifference",
        ),
        (
            "cpp_complement",
            CppUnsupportedRegionKind::Complement,
            "WCComplement",
        ),
        (
            "cpp_concatenation",
            CppUnsupportedRegionKind::Concatenation,
            "WCConcatenation",
        ),
        (
            "cpp_extension",
            CppUnsupportedRegionKind::Extension,
            "WCExtension",
        ),
        (
            "cpp_lelmask",
            CppUnsupportedRegionKind::LelMask,
            "WCLELMask",
        ),
        ("cpp_lcbox", CppUnsupportedRegionKind::LcBox, "LCBox"),
    ];

    for (name, kind, class_name) in cases {
        casacore_test_support::cpp_write_unsupported_region(&path, name, kind).unwrap();
        let error = opened.load_saved_region(name).unwrap_err();
        let error_text = error.to_string();
        assert!(error_text.contains(name), "{name}: {error_text}");
        assert!(error_text.contains(class_name), "{name}: {error_text}");
    }
}

fn region_vertex_worlds(
    opened: &OpenedImageView,
    window: &casacore_images::ImageViewWindow,
    pixels: &[(usize, usize)],
) -> Vec<casacore_images::image_view::ImageRegionVertex> {
    pixels
        .iter()
        .map(|pixel_xy| {
            opened
                .region_vertex_for_pixel_with_window_and_axes(*pixel_xy, window, &[])
                .unwrap()
        })
        .collect()
}

fn assert_region_stats_close(
    rust: &casacore_images::image_view::ImageRegionStats,
    cpp: &casacore_test_support::CppRegionStatistics,
) {
    assert_eq!(rust.pixel_count, cpp.pixel_count);
    for (label, left, right) in [
        ("sum", rust.sum, cpp.sum),
        ("mean", rust.mean, cpp.mean),
        ("median", rust.median, cpp.median),
        ("rms", rust.rms, cpp.rms),
        ("sigma", rust.sigma, cpp.sigma),
        ("min", rust.min, cpp.min),
        ("max", rust.max, cpp.max),
    ] {
        let tolerance = 1e-9_f64.max(left.abs() * 1e-9).max(right.abs() * 1e-9);
        assert!(
            (left - right).abs() <= tolerance,
            "{label} mismatch: rust={left} cpp={right} tol={tolerance}"
        );
    }
}

#[test]
fn rc_rust_region_statistics_match_cpp_for_polygon_and_union() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping rc_rust_region_statistics_match_cpp_for_polygon_and_union: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_region_stats.image");
    let shape = vec![6usize, 6usize];
    casacore_test_support::cpp_create_image(
        &path,
        &[shape[0] as i32, shape[1] as i32],
        &vec![0.0; shape.iter().product()],
        "Jy/beam",
    )
    .unwrap();
    let mut image = Image::open(&path).unwrap();
    let mut data = ramp_array(&shape);
    data[[2, 2]] = f32::NAN;
    data[[4, 1]] = f32::INFINITY;
    image.put_slice(&data, &[0, 0]).unwrap();
    let mut mask = ArrayD::from_elem(IxDyn(&shape).f(), true);
    mask[[1, 1]] = false;
    mask[[4, 4]] = false;
    image.put_mask("quality", &mask).unwrap();
    image.set_default_mask("quality").unwrap();
    image.save().unwrap();

    let opened = OpenedImageView::open(&path).unwrap();
    let window = opened.default_window();

    let mut polygon = opened.default_region("Region 1").unwrap();
    polygon.start_shape().unwrap();
    for vertex in region_vertex_worlds(&opened, &window, &[(1, 1), (4, 1), (2, 4)]) {
        polygon.append_vertex(vertex).unwrap();
    }
    polygon.close_active_shape().unwrap();
    let polygon_name = opened.save_region_definition(&polygon, None).unwrap();
    let rust_polygon = opened
        .region_stats_with_window_and_axes(&polygon, &window, &[])
        .unwrap()
        .unwrap();
    let cpp_polygon =
        casacore_test_support::cpp_read_region_statistics(&path, &polygon_name).unwrap();
    assert_region_stats_close(&rust_polygon, &cpp_polygon);

    polygon.start_shape().unwrap();
    for vertex in region_vertex_worlds(&opened, &window, &[(4, 2), (5, 2), (4, 5)]) {
        polygon.append_vertex(vertex).unwrap();
    }
    polygon.close_active_shape().unwrap();
    let union_name = opened.save_region_definition(&polygon, None).unwrap();
    let rust_union = opened
        .region_stats_with_window_and_axes(&polygon, &window, &[])
        .unwrap()
        .unwrap();
    let cpp_union = casacore_test_support::cpp_read_region_statistics(&path, &union_name).unwrap();
    assert_region_stats_close(&rust_union, &cpp_union);
}

#[test]
fn rc_rust_write_cpp_reads_default_mask() {
    if !cpp_backend_available() {
        eprintln!("skipping rc_rust_write_cpp_reads_default_mask: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_mask.image");
    casacore_test_support::cpp_create_image(&path, &[5, 5], &[0.0; 25], "").unwrap();

    let opened = OpenedImageView::open(&path).unwrap();
    let window = opened.default_window();
    let mut region = opened.default_region("Region 1").unwrap();
    region.start_shape().unwrap();
    for pixel_xy in [(1usize, 1usize), (3, 1), (3, 3), (1, 3)] {
        let vertex = opened
            .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[])
            .unwrap();
        region.append_vertex(vertex).unwrap();
    }
    region.close_active_shape().unwrap();
    opened.write_region_mask(&region, "roi", true).unwrap();

    assert_eq!(
        casacore_test_support::cpp_read_image_default_mask_name(&path).unwrap(),
        "roi"
    );
    let mask = casacore_test_support::cpp_read_image_default_mask(&path, 25).unwrap();
    assert_eq!(mask.len(), 25);
    assert!(mask.iter().any(|value| *value));
}

#[test]
fn cr_cpp_write_default_mask_rust_reads_it() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_cpp_write_default_mask_rust_reads_it: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_mask.image");
    casacore_test_support::cpp_create_image(&path, &[4, 4], &[0.0; 16], "").unwrap();

    let mask = (0..16).map(|index| index % 2 == 0).collect::<Vec<_>>();
    casacore_test_support::cpp_write_default_mask(&path, "cppmask", &mask).unwrap();

    let reopened = Image::open(&path).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("cppmask"));
    let rust_mask = reopened.get_named_mask("cppmask").unwrap();
    let rust_flat = rust_mask.as_slice_memory_order().unwrap().to_vec();
    assert_eq!(rust_flat, mask);
}
