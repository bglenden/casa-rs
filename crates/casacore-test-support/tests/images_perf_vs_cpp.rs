// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison: Rust Image vs C++ PagedImage disk I/O.
//!
//! Each test runs the same workload with both implementations and reports the
//! ratio. Use `cargo test --release` for meaningful comparisons. The 2×
//! threshold triggers a warning (not a hard failure).

use std::collections::HashMap;

use casacore_coordinates::CoordinateSystem;
use casacore_images::expr_file;
use casacore_images::expr_parser::{HashMapResolver, parse_image_expr};
use casacore_images::image::ImageInterface;
use casacore_images::{Image, ImageExpr, ImageIter, PagedImage};
use casacore_test_support::{
    cpp_backend_available, cpp_create_image, cpp_eval_image_expr_closeout_slice, cpp_eval_lel_expr,
    cpp_open_lel_expr_file, cpp_save_lel_expr_file,
};
use casacore_types::Complex32;
use ndarray::{ArrayD, IxDyn};
use std::time::Instant;

fn flatten_fortran<T: Clone>(array: &ArrayD<T>) -> Vec<T> {
    let shape = array.shape();
    let mut out = Vec::with_capacity(array.len());
    for linear in 0..array.len() {
        let mut idx = Vec::with_capacity(shape.len());
        let mut remaining = linear;
        for &dim in shape {
            idx.push(remaining % dim);
            remaining /= dim;
        }
        out.push(array[IxDyn(&idx)].clone());
    }
    out
}

fn assert_float_close(label: &str, actual: &[f32], expected: &[f32], tol: f32) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{label}: output length mismatch"
    );
    for (i, (&got, &want)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (got - want).abs() < tol,
            "{label}: pixel {i}: got={got}, expected={want}"
        );
    }
}

// ---------------------------------------------------------------------------
// Image lifecycle benchmark
// ---------------------------------------------------------------------------

#[test]
fn image_lifecycle_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping image_lifecycle_perf: C++ casacore not available");
        return;
    }

    // Use 64³ by default for CI speed. Set CASA_RS_LARGE_PERF=1 for 128³.
    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        128
    } else {
        64
    };
    let shape = vec![size, size, size];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();

    let dir = tempfile::tempdir().unwrap();

    // -- C++ timing --
    let cpp_path = dir.path().join("cpp_perf.image");
    let data: Vec<f32> = vec![1.0; n];

    let t0 = Instant::now();
    casacore_test_support::cpp_create_image(&cpp_path, &shape_i32, &data, "")
        .expect("C++ create failed");
    let cpp_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let _cpp_data =
        casacore_test_support::cpp_read_image_data(&cpp_path, n).expect("C++ read failed");
    let cpp_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let cpp_total_ms = cpp_write_ms + cpp_read_ms;

    // -- Rust timing --
    let rust_path = dir.path().join("rust_perf.image");

    let t0 = Instant::now();
    let mut img =
        Image::create(shape.clone(), Default::default(), &rust_path).expect("Rust create failed");
    img.set(1.0).expect("Rust set failed");
    img.save().expect("Rust save failed");
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let img2 = Image::open(&rust_path).expect("Rust open failed");
    let _arr = img2.get().expect("Rust get failed");
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let rust_total_ms = rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Image lifecycle ({size}³ = {n} pixels):\n  \
         C++:  write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust image I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust image I/O ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

// ---------------------------------------------------------------------------
// Chunked iteration throughput
// ---------------------------------------------------------------------------

#[test]
fn chunked_iteration_throughput() {
    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        128
    } else {
        64
    };
    let shape = vec![size, size, size];
    let n: usize = shape.iter().product();
    let cursor = vec![32, 32, 32];

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iter_perf.image");

    let mut img = Image::create(shape.clone(), Default::default(), &path).unwrap();
    img.set(1.0).unwrap();
    img.save().unwrap();
    drop(img);

    let img = Image::open(&path).unwrap();

    let t0 = Instant::now();
    let mut total = 0.0f64;
    let mut chunks = 0usize;
    for chunk in ImageIter::new(&img, cursor) {
        let c = chunk.unwrap();
        total += c.data.sum() as f64;
        chunks += 1;
    }
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let pixels_per_sec = n as f64 / t0.elapsed().as_secs_f64();

    assert!((total - n as f64).abs() < 1.0, "total mismatch: {total}");

    eprintln!(
        "Chunked iteration ({size}³, 32³ cursor, {chunks} chunks):\n  \
         {elapsed_ms:.1} ms, {:.0} Mpix/s",
        pixels_per_sec / 1e6,
    );
}

// ---------------------------------------------------------------------------
// Sub-cube slice read
// ---------------------------------------------------------------------------

#[test]
fn subcube_slice_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping subcube_slice_perf: C++ casacore not available");
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        128
    } else {
        64
    };
    let shape = vec![size, size, size];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();

    let dir = tempfile::tempdir().unwrap();

    // Create image with both Rust and C++
    let rust_path = dir.path().join("slice_rust.image");
    let cpp_path = dir.path().join("slice_cpp.image");

    let data: Vec<f32> = (0..n).map(|i| i as f32).collect();

    // Rust write
    {
        let mut img = Image::create(shape.clone(), Default::default(), &rust_path).unwrap();
        let arr = ArrayD::from_shape_vec(IxDyn(&shape), data.clone()).unwrap();
        img.put_slice(&arr, &[0, 0, 0]).unwrap();
        img.save().unwrap();
    }

    // C++ write
    casacore_test_support::cpp_create_image(&cpp_path, &shape_i32, &data, "").unwrap();

    // Slice parameters: quarter-size sub-cube from the center
    let quarter = size / 4;
    let half = size / 2;
    let start = vec![quarter, quarter, quarter];
    let length = vec![half, half, half];
    let start_i32: Vec<i32> = start.iter().map(|&s| s as i32).collect();
    let length_i32: Vec<i32> = length.iter().map(|&s| s as i32).collect();

    // Rust slice
    let t0 = Instant::now();
    let rust_slice = Image::open(&rust_path)
        .unwrap()
        .get_slice(&start, &length)
        .unwrap();
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // C++ slice
    let t0 = Instant::now();
    let cpp_slice =
        casacore_test_support::cpp_read_image_slice(&cpp_path, &start_i32, &length_i32).unwrap();
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let slice_n = rust_slice.len();
    let ratio = rust_ms / cpp_ms.max(0.001);

    // Verify data matches
    let rust_flat: Vec<f32> = rust_slice.iter().copied().collect();
    assert_eq!(rust_flat.len(), cpp_slice.len());

    eprintln!(
        "Sub-cube slice ({half}³ from {size}³, {slice_n} pixels):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust slice I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(ratio <= 2.0, "Rust slice ratio {ratio:.2}× exceeds 2.0×");
    }
}

#[test]
fn complex32_lifecycle_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping complex32_lifecycle_perf: C++ casacore not available");
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        96
    } else {
        48
    };
    let shape = vec![size, size, size];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let data: Vec<Complex32> = (0..n)
        .map(|i| Complex32::new((i % 1024) as f32, -((i % 257) as f32)))
        .collect();

    let dir = tempfile::tempdir().unwrap();

    let cpp_path = dir.path().join("cpp_c32_perf.image");
    let t0 = Instant::now();
    casacore_test_support::cpp_create_image_complex32(&cpp_path, &shape_i32, &data, "")
        .expect("C++ complex create failed");
    let cpp_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let _cpp_data = casacore_test_support::cpp_read_image_data_complex32(&cpp_path, n)
        .expect("C++ complex read failed");
    let cpp_read_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let cpp_total_ms = cpp_write_ms + cpp_read_ms;

    let rust_path = dir.path().join("rust_c32_perf.image");
    let t0 = Instant::now();
    let mut img =
        PagedImage::<Complex32>::create(shape.clone(), Default::default(), &rust_path).unwrap();
    let arr = ArrayD::from_shape_vec(IxDyn(&shape), data.clone()).unwrap();
    img.put_slice(&arr, &[0, 0, 0]).unwrap();
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let img = PagedImage::<Complex32>::open(&rust_path).unwrap();
    let _arr = img.get().unwrap();
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rust_total_ms = rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Complex32 lifecycle ({size}³ = {n} pixels):\n  \
         C++:  write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust Complex32 image I/O ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

#[test]
fn lazy_image_expr_closeout_slice_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping lazy_image_expr_closeout_slice_perf_vs_cpp: C++ casacore not available"
        );
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        192
    } else {
        96
    };
    let shape = vec![size, size];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let start = vec![(size / 4) as i32, (size / 4) as i32];
    let length = vec![(size / 2) as i32, (size / 2) as i32];
    let start_usize: Vec<usize> = start.iter().map(|&v| v as usize).collect();
    let length_usize: Vec<usize> = length.iter().map(|&v| v as usize).collect();
    let slice_n: usize = length.iter().map(|&v| v as usize).product();
    let passes = 25usize;

    let dir = tempfile::tempdir().unwrap();
    let rust_path = dir.path().join("rust_expr_perf.image");
    let cpp_path = dir.path().join("cpp_expr_perf.image");
    let data: Vec<f32> = (0..n).map(|i| 0.05 + (i as f32) * 0.001).collect();

    {
        let mut img = Image::create(shape.clone(), CoordinateSystem::new(), &rust_path).unwrap();
        let arr = ArrayD::from_shape_vec(IxDyn(&shape), data.clone()).unwrap();
        img.put_slice(&arr, &[0, 0]).unwrap();
        img.save().unwrap();
    }
    cpp_create_image(&cpp_path, &shape_i32, &data, "").unwrap();

    let image = Image::open(&rust_path).unwrap();
    let expr = ImageExpr::from_image(&image)
        .unwrap()
        .add_scalar(1.0)
        .sqrt()
        .atan2_expr(
            ImageExpr::from_image(&image)
                .unwrap()
                .add_scalar(0.5)
                .pow_scalar(2.0)
                .fmod_scalar(3.0)
                .add_scalar(0.25),
        )
        .unwrap()
        .max_scalar(0.5);

    let t0 = Instant::now();
    let mut rust_total = 0.0f64;
    for _ in 0..passes {
        let slice = expr
            .get_slice(&start_usize, &length_usize)
            .expect("Rust expr slice failed");
        rust_total += slice.sum() as f64;
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let mut cpp_total = 0.0f64;
    for _ in 0..passes {
        let slice = cpp_eval_image_expr_closeout_slice(&cpp_path, &start, &length)
            .expect("C++ expr slice failed");
        cpp_total += slice.iter().map(|&v| v as f64).sum::<f64>();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    let delta = (rust_total - cpp_total).abs();
    assert!(
        delta < 1.0e-2 * passes as f64,
        "closeout expression sums diverged: rust={rust_total}, cpp={cpp_total}, delta={delta}"
    );

    eprintln!(
        "Lazy ImageExpr closeout slice ({size}x{size}, slice {}x{}, {passes} passes, {slice_n} pixels/pass):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        length[0], length[1],
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust lazy ImageExpr slice is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust lazy ImageExpr slice ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

// ---------------------------------------------------------------------------
// Parser perf: parse LEL string + evaluate small slice (Rust vs C++)
// ---------------------------------------------------------------------------

#[test]
fn parsed_lel_expr_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping parsed_lel_expr_perf_vs_cpp: C++ casacore not available");
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        128
    } else {
        64
    };
    let n = size * size;
    let shape_i32 = [size as i32, size as i32];
    let passes = 50usize;

    let dir = tempfile::tempdir().unwrap();
    let img_path = dir.path().join("lel_perf.image");
    let data: Vec<f32> = (0..n).map(|i| 0.5 + (i as f32) * 0.01).collect();
    cpp_create_image(&img_path, &shape_i32, &data, "").unwrap();

    // LEL expression exercising arithmetic, transcendental, and 2-arg functions
    let lel = format!(
        "sqrt(abs('{}' * 2.0 + 1.0)) + max(sin('{}'), 0.0)",
        img_path.display(),
        img_path.display(),
    );

    // --- Rust: parse + full read ---
    let image = PagedImage::<f32>::open(&img_path).unwrap();
    let mut images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    images.insert(img_path.display().to_string(), &image);
    let resolver = HashMapResolver(images);

    let t0 = Instant::now();
    let mut rust_sum = 0.0f64;
    for _ in 0..passes {
        let expr = parse_image_expr(&lel, &resolver).unwrap();
        let arr = expr.get().unwrap();
        rust_sum += arr.iter().map(|&v| v as f64).sum::<f64>();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++: parse + full read ---
    let t0 = Instant::now();
    let mut cpp_sum = 0.0f64;
    for _ in 0..passes {
        let (vals, _shape) = cpp_eval_lel_expr(&lel, n).unwrap();
        cpp_sum += vals.iter().map(|&v| v as f64).sum::<f64>();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    let delta = (rust_sum - cpp_sum).abs();
    assert!(
        delta < 1.0e-1 * passes as f64,
        "parsed LEL sums diverged: rust={rust_sum}, cpp={cpp_sum}, delta={delta}"
    );

    eprintln!(
        "Parsed LEL expr perf ({size}x{size}, {passes} passes, parse+full-read per pass):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust parsed LEL expr is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust parsed LEL ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

#[test]
fn parsed_two_image_virtual_expr_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping parsed_two_image_virtual_expr_perf_vs_cpp: C++ casacore not available");
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        96
    } else {
        48
    };
    let n = size * size;
    let shape_i32 = [size as i32, size as i32];
    let passes = 30usize;

    let dir = tempfile::tempdir().unwrap();
    let lhs_path = dir.path().join("lhs_perf.image");
    let rhs_path = dir.path().join("rhs_perf.image");

    let lhs_data: Vec<f32> = (0..n).map(|i| 1.0 + (i as f32) * 0.01).collect();
    let rhs_data: Vec<f32> = (0..n)
        .map(|i| 0.25 + ((i % (size + 3)) as f32) * 0.02)
        .collect();
    cpp_create_image(&lhs_path, &shape_i32, &lhs_data, "").unwrap();
    cpp_create_image(&rhs_path, &shape_i32, &rhs_data, "").unwrap();

    let lhs_str = lhs_path.to_str().unwrap();
    let rhs_str = rhs_path.to_str().unwrap();
    let lel = format!(
        "sqrt(abs('{lhs}' * 1.5 - '{rhs}' / 2.0)) + max('{lhs}', '{rhs}')",
        lhs = lhs_str,
        rhs = rhs_str,
    );
    let expected: Vec<f32> = lhs_data
        .iter()
        .zip(rhs_data.iter())
        .map(|(&lhs, &rhs)| ((lhs * 1.5 - rhs / 2.0).abs().sqrt()) + lhs.max(rhs))
        .collect();

    let lhs = PagedImage::<f32>::open(&lhs_path).unwrap();
    let rhs = PagedImage::<f32>::open(&rhs_path).unwrap();
    let mut images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    images.insert(lhs_str.to_string(), &lhs);
    images.insert(rhs_str.to_string(), &rhs);
    let resolver = HashMapResolver(images);

    let rust_expr = parse_image_expr(&lel, &resolver).unwrap();
    let rust_once = rust_expr.get().unwrap();
    assert_float_close(
        "Rust two-image virtual expr",
        &flatten_fortran(&rust_once),
        &expected,
        1.0e-5,
    );

    let (cpp_once, cpp_shape) = cpp_eval_lel_expr(&lel, n).unwrap();
    assert_eq!(cpp_shape, shape_i32.to_vec());
    assert_float_close("C++ two-image virtual expr", &cpp_once, &expected, 1.0e-4);

    let t0 = Instant::now();
    let mut rust_sum = 0.0f64;
    for _ in 0..passes {
        let expr = parse_image_expr(&lel, &resolver).unwrap();
        let arr = expr.get().unwrap();
        rust_sum += arr.iter().map(|&v| v as f64).sum::<f64>();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let mut cpp_sum = 0.0f64;
    for _ in 0..passes {
        let (vals, _shape) = cpp_eval_lel_expr(&lel, n).unwrap();
        cpp_sum += vals.iter().map(|&v| v as f64).sum::<f64>();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    let delta = (rust_sum - cpp_sum).abs();
    assert!(
        delta < 1.0e-1 * passes as f64,
        "two-image virtual expr sums diverged: rust={rust_sum}, cpp={cpp_sum}, delta={delta}"
    );

    eprintln!(
        "Two-image parsed virtual LEL perf ({size}x{size}, {passes} passes, parse+full-read per pass):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust two-image parsed virtual LEL is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust two-image parsed virtual LEL ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

// ---------------------------------------------------------------------------
// Expression file save/open benchmark
// ---------------------------------------------------------------------------

#[test]
fn imgexpr_save_open_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping imgexpr_save_open_perf_vs_cpp: C++ casacore not available");
        return;
    }

    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        128
    } else {
        64
    };
    let n = size * size;
    let shape_i32 = [size as i32, size as i32];
    let passes = 20usize;

    let dir = tempfile::tempdir().unwrap();
    let img_path = dir.path().join("src.image");
    let data: Vec<f32> = (0..n).map(|i| 0.5 + (i as f32) * 0.01).collect();
    cpp_create_image(&img_path, &shape_i32, &data, "").unwrap();

    let img_str = img_path.to_str().unwrap();
    let expr_str = format!("'{img_str}' * 2.0 + 1.0");

    // --- Rust: save + open + read ---
    let image = PagedImage::<f32>::open(&img_path).unwrap();
    let mut images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    images.insert(img_str.to_string(), &image);
    let resolver = HashMapResolver(images);
    let parsed = parse_image_expr(&expr_str, &resolver).unwrap();

    let t0 = Instant::now();
    let mut rust_sum = 0.0f64;
    for i in 0..passes {
        let save_path = dir.path().join(format!("rust_expr_{i}.imgexpr"));
        parsed.save_expr(&save_path).unwrap();
        let opened = expr_file::open::<f32>(&save_path).unwrap();
        let arr = opened.get().unwrap();
        rust_sum += arr.iter().map(|&v| v as f64).sum::<f64>();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++: save + open + read ---
    let t0 = Instant::now();
    let mut cpp_sum = 0.0f64;
    for i in 0..passes {
        let save_path = dir.path().join(format!("cpp_expr_{i}.imgexpr"));
        cpp_save_lel_expr_file(&expr_str, &save_path).unwrap();
        let (vals, _shape) = cpp_open_lel_expr_file(&save_path, n).unwrap();
        cpp_sum += vals.iter().map(|&v| v as f64).sum::<f64>();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    let delta = (rust_sum - cpp_sum).abs();
    assert!(
        delta < 1.0 * passes as f64,
        "imgexpr save/open sums diverged: rust={rust_sum}, cpp={cpp_sum}, delta={delta}"
    );

    eprintln!(
        "imgexpr save+open+read perf ({size}x{size}, {passes} passes):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 3.0 {
        eprintln!(
            "WARNING: Rust imgexpr save/open is {ratio:.1}× slower than C++ (threshold: 3.0×)"
        );
    }
}

// =========================================================================
// Wave 14 perf: reduction, conditional, type projection
// =========================================================================

#[test]
fn perf_wave14_reduction_64_cube() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let size = 64usize;
    let n = size * size * size;
    let shape_i32 = [size as i32, size as i32, size as i32];
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cube.image");
    let data: Vec<f32> = (0..n).map(|i| (i as f32) * 0.001).collect();
    cpp_create_image(&path, &shape_i32, &data, "").unwrap();

    let a = PagedImage::<f32>::open(&path).unwrap();
    let a_str = path.to_str().unwrap();
    let resolver = {
        let mut map = HashMap::new();
        map.insert(a_str.to_string(), &a as &dyn ImageInterface<f32>);
        HashMapResolver(map)
    };

    // Rust: sum + mean embedded in shape-preserving expression
    let passes = 5;
    let expr_sum = format!("'{a_str}' * 0.0 + sum('{a_str}')");
    let expr_mean = format!("'{a_str}' * 0.0 + mean('{a_str}')");

    let t0 = Instant::now();
    for _ in 0..passes {
        let e = parse_image_expr(&expr_sum, &resolver).unwrap();
        let _ = e.get_at(&[0, 0, 0]).unwrap();
        let e = parse_image_expr(&expr_mean, &resolver).unwrap();
        let _ = e.get_at(&[0, 0, 0]).unwrap();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    for _ in 0..passes {
        let _ = cpp_eval_lel_expr(&expr_sum, n).unwrap();
        let _ = cpp_eval_lel_expr(&expr_mean, n).unwrap();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    eprintln!(
        "wave14 reduction perf ({size}³, {passes} passes sum+mean):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust reduction is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
}

#[test]
fn perf_wave14_iif_64_cube() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let size = 64usize;
    let n = size * size * size;
    let shape_i32 = [size as i32, size as i32, size as i32];
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cube.image");
    let data: Vec<f32> = (0..n).map(|i| (i as f32) * 0.001 - 128.0).collect();
    cpp_create_image(&path, &shape_i32, &data, "").unwrap();

    let a = PagedImage::<f32>::open(&path).unwrap();
    let a_str = path.to_str().unwrap();
    let resolver = {
        let mut map = HashMap::new();
        map.insert(a_str.to_string(), &a as &dyn ImageInterface<f32>);
        HashMapResolver(map)
    };

    let expr_str = format!("iif('{a_str}' > 0.0, '{a_str}' * 2.0, '{a_str}' * -1.0)");
    let passes = 5;

    let t0 = Instant::now();
    for _ in 0..passes {
        let e = parse_image_expr(&expr_str, &resolver).unwrap();
        let _ = e.get().unwrap();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    for _ in 0..passes {
        let _ = cpp_eval_lel_expr(&expr_str, n).unwrap();
    }
    let cpp_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ratio = rust_ms / cpp_ms.max(0.001);
    eprintln!(
        "wave14 iif perf ({size}³, {passes} passes):\n  \
         C++:  {cpp_ms:.1} ms\n  \
         Rust: {rust_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust iif is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
}

#[test]
fn perf_wave14_type_projection_48_cube() {
    // Type projection (real_part) is typed-API-only — no C++ LEL comparison.
    // Just measure Rust performance to establish a baseline.
    let size = 48usize;
    let shape = vec![size, size, size];
    let mut img =
        casacore_images::TempImage::<Complex32>::new(shape, CoordinateSystem::new()).unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(&[size, size, size]), |idx| {
        Complex32::new((idx[0] + idx[1]) as f32, idx[2] as f32)
    });
    img.put_slice(&data, &[0, 0, 0]).unwrap();

    let passes = 5;
    let t0 = Instant::now();
    for _ in 0..passes {
        let expr = ImageExpr::from_image(&img).unwrap().real_part();
        let _ = expr.get().unwrap();
    }
    let rust_ms = t0.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "wave14 real_part perf ({size}³ Complex32, {passes} passes):\n  \
         Rust: {rust_ms:.1} ms ({:.1} ms/pass)",
        rust_ms / passes as f64
    );
}

// ---------------------------------------------------------------------------
// Plane-by-plane I/O with tile-aware TiledFileIO
// ---------------------------------------------------------------------------

#[test]
fn plane_by_plane_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping plane_by_plane_perf: C++ casacore not available");
        return;
    }

    // Use 256³ by default. Set CASA_RS_LARGE_PERF=1 for 1024³.
    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        1024
    } else {
        256
    };
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    let dir = tempfile::tempdir().unwrap();

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        casacore_test_support::cpp_bench_image_plane_by_plane(&cpp_path, &shape_i32, &tile_i32, 0)
            .expect("C++ plane-by-plane benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    // --- Rust benchmark ---
    let rust_path = dir.path().join("rust_pbp.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
    )
    .expect("Rust create_with_tile_shape failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Write plane by plane (z-planes).
    // Pre-allocate a single plane array (matching C++ which reuses one Array).
    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        // Fill using raw slice access — matches C++ getStorage/putStorage pattern.
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                slice[x * size + y] = (x + y * size + z * plane_size) as f32;
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = Image::open(&rust_path).expect("Rust open failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        // Spot-check a few pixels per plane.
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let expected = (x + y * size + z * plane_size) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane ({size}³ = {n} pixels, {tile}³ tiles):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust plane-by-plane is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust plane-by-plane ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

// ---------------------------------------------------------------------------
// Plane-by-plane with bounded tile cache (forces real disk I/O)
// ---------------------------------------------------------------------------

#[test]
fn plane_by_plane_bounded_cache_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping plane_by_plane_bounded_cache_perf: C++ casacore not available");
        return;
    }

    // 1024³ only — smaller sizes fit in cache anyway.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    // 128 MiB cache: 32³ f32 tiles = 128 KB each → 1024 tiles fit → one z-plane.
    let cache_bytes: usize = 128 * 1024 * 1024;
    let cache_mib: i32 = 128;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_pbp_bounded.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                slice[x * size + y] = (x + y * size + z * plane_size) as f32;
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = Image::open_with_cache(&rust_path, cache_bytes).expect("Rust open_with_cache failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let expected = (x + y * size + z * plane_size) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp_bounded.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        casacore_test_support::cpp_bench_image_plane_by_plane(
            &cpp_path, &shape_i32, &tile_i32, cache_mib,
        )
        .expect("C++ bounded-cache benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust plane-by-plane bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}

// ---------------------------------------------------------------------------
// Plane-by-plane Complex32 with bounded tile cache
// ---------------------------------------------------------------------------

#[test]
fn plane_by_plane_complex32_bounded_cache_perf() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping plane_by_plane_complex32_bounded_cache_perf: C++ casacore not available"
        );
        return;
    }

    // 1024³ — same size as f32 bounded cache test.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    // 256 MiB cache: 32³ Complex32 tiles = 256 KB each → 1024 tiles fit → one z-plane.
    let cache_bytes: usize = 256 * 1024 * 1024;
    let cache_mib: i32 = 256;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_pbp_c32.image");

    let t0 = Instant::now();
    let mut img = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                let val = (x + y * size + z * plane_size) as f32;
                slice[x * size + y] = Complex32::new(val, -val);
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = PagedImage::<Complex32>::open_with_cache(&rust_path, cache_bytes)
        .expect("Rust open_with_cache failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let val = (x + y * size + z * plane_size) as f32;
                    let expected = Complex32::new(val, -val);
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp_c32.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        casacore_test_support::cpp_bench_image_plane_by_plane_complex(
            &cpp_path, &shape_i32, &tile_i32, cache_mib,
        )
        .expect("C++ complex32 plane-by-plane benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;
    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane Complex32 bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust Complex32 plane-by-plane bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}

// ---------------------------------------------------------------------------
// Spectrum-by-spectrum with bounded tile cache (forces real disk I/O)
// ---------------------------------------------------------------------------

#[test]
fn spectrum_by_spectrum_bounded_cache_perf() {
    if !cpp_backend_available() {
        eprintln!("skipping spectrum_by_spectrum_bounded_cache_perf: C++ casacore not available");
        return;
    }

    // 1024³ only — smaller sizes fit in cache anyway.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();
    let nx = size;
    let ny = size;
    let nz = size;
    let plane_size = nx * ny;

    // 128 MiB cache: holds 1024 tiles → covers one full y-strip of z-columns.
    let cache_bytes: usize = 128 * 1024 * 1024;
    let cache_mib: i32 = 128;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_sbs_bounded.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Write spectrum by spectrum: iterate y then x (matching C++).
    let t0 = Instant::now();
    let mut spectrum = ArrayD::zeros(IxDyn(&[1, 1, nz]));
    for y in 0..ny {
        for x in 0..nx {
            let slice = spectrum.as_slice_mut().unwrap();
            let base = x + y * nx;
            for (z, val) in slice.iter_mut().enumerate() {
                *val = (base + z * plane_size) as f32;
            }
            img.put_slice(&spectrum, &[x, y, 0]).unwrap();
        }
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify spectrum by spectrum.
    let t0 = Instant::now();
    let img = Image::open_with_cache(&rust_path, cache_bytes).expect("Rust open_with_cache failed");
    for y in 0..ny {
        for x in 0..nx {
            let spectrum = img.get_slice(&[x, y, 0], &[1, 1, nz]).unwrap();
            // Verify a few values per spectrum at the corners of the (y, x) grid.
            if (y == 0 || y == ny - 1) && (x == 0 || x == nx - 1) {
                for z in [0, nz / 2, nz - 1] {
                    let expected = (x + y * nx + z * plane_size) as f32;
                    assert_eq!(spectrum[[0, 0, z]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_sbs_bounded.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        casacore_test_support::cpp_bench_image_spectrum_by_spectrum(
            &cpp_path, &shape_i32, &tile_i32, cache_mib,
        )
        .expect("C++ spectrum-by-spectrum benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Spectrum-by-spectrum bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust spectrum-by-spectrum bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}
