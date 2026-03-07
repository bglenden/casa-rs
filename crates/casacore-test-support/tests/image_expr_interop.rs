// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ interop checks for lazy `ImageExpr` (Waves 11a + 11b + 12a parser + 12b persistence).

use std::collections::HashMap;

use casacore_coordinates::CoordinateSystem;
use casacore_images::expr_parser::{HashMapResolver, parse_image_expr, parse_mask_expr};
use casacore_images::image::ImageInterface;
use casacore_images::{ImageExpr, ImageExprBinaryOp, ImageExprUnaryOp, PagedImage};
use casacore_lattices::Lattice;
use casacore_test_support::{
    CppImageExprBinaryOp, CppImageExprCompareOp, CppImageExprUnaryOp, CppMaskLogicalOp,
    cpp_backend_available, cpp_create_image, cpp_eval_image_expr_binary,
    cpp_eval_image_expr_scalar, cpp_eval_image_expr_unary, cpp_eval_image_mask_range,
    cpp_eval_lel_expr, cpp_eval_lel_expr_mask, cpp_open_lel_expr_file, cpp_read_image_data,
    cpp_save_lel_expr_file,
};
use casacore_types::ArrayD;
use ndarray::{IxDyn, ShapeBuilder};

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

fn make_image(
    path: &std::path::Path,
    shape: &[usize],
    values: Vec<f32>,
) -> Result<PagedImage<f32>, casacore_images::ImageError> {
    let mut image = PagedImage::<f32>::create(shape.to_vec(), CoordinateSystem::new(), path)?;
    let data = ArrayD::from_shape_vec(IxDyn(shape).f(), values).unwrap();
    image.put_slice(&data, &vec![0; shape.len()])?;
    image.save()?;
    Ok(image)
}

#[test]
fn rust_lazy_binary_add_matches_cpp_expr() {
    if !cpp_backend_available() {
        eprintln!("skipping rust_lazy_binary_add_matches_cpp_expr: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let lhs_path = dir.path().join("lhs.image");
    let rhs_path = dir.path().join("rhs.image");
    let shape = [3usize, 4usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &lhs_path,
        &shape_i32,
        &(0..n).map(|i| i as f32).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    cpp_create_image(
        &rhs_path,
        &shape_i32,
        &(0..n).map(|i| (i as f32) * 0.5).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let lhs = PagedImage::<f32>::open(&lhs_path).unwrap();
    let rhs = PagedImage::<f32>::open(&rhs_path).unwrap();

    let rust = ImageExpr::from_image(&lhs)
        .unwrap()
        .add_image(&rhs)
        .unwrap()
        .get()
        .unwrap();
    let cpp =
        cpp_eval_image_expr_binary(&lhs_path, &rhs_path, CppImageExprBinaryOp::Add, n).unwrap();

    assert_eq!(flatten_fortran(&rust), cpp);
}

#[test]
fn rust_lazy_scalar_multiply_matches_cpp_expr() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping rust_lazy_scalar_multiply_matches_cpp_expr: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scalar.image");
    let shape = [4usize, 3usize];
    let n: usize = shape.iter().product();

    let image = make_image(&path, &shape, (0..n).map(|i| i as f32 - 2.0).collect()).unwrap();
    let rust = ImageExpr::from_image(&image)
        .unwrap()
        .multiply_scalar(3.0)
        .get()
        .unwrap();
    let cpp = cpp_eval_image_expr_scalar(&path, 3.0, CppImageExprBinaryOp::Multiply, n).unwrap();

    assert_eq!(flatten_fortran(&rust), cpp);
}

#[test]
fn rust_lazy_unary_and_transcendental_match_cpp_expr() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping rust_lazy_unary_and_transcendental_match_cpp_expr: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unary.image");
    let shape = [2usize, 5usize];
    let n: usize = shape.iter().product();

    let image = make_image(&path, &shape, (0..n).map(|i| 0.25 * i as f32).collect()).unwrap();

    let rust_neg = ImageExpr::from_image(&image)
        .unwrap()
        .negate()
        .get()
        .unwrap();
    let cpp_neg = cpp_eval_image_expr_unary(&path, CppImageExprUnaryOp::Negate, n).unwrap();
    assert_eq!(flatten_fortran(&rust_neg), cpp_neg);

    let rust_exp = ImageExpr::from_image(&image)
        .unwrap()
        .unary(ImageExprUnaryOp::Exp)
        .get()
        .unwrap();
    let cpp_exp = cpp_eval_image_expr_unary(&path, CppImageExprUnaryOp::Exp, n).unwrap();

    for (rust_value, cpp_value) in flatten_fortran(&rust_exp).iter().zip(cpp_exp.iter()) {
        assert!((rust_value - cpp_value).abs() < 1.0e-5);
    }
}

#[test]
fn rust_lazy_mask_range_matches_cpp_expr() {
    if !cpp_backend_available() {
        eprintln!("skipping rust_lazy_mask_range_matches_cpp_expr: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mask.image");
    let shape = [3usize, 3usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| i as f32).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();
    let rust = ImageExpr::from_image(&image)
        .unwrap()
        .gt_scalar(1.5)
        .and(ImageExpr::from_image(&image).unwrap().lt_scalar(6.5))
        .unwrap()
        .get()
        .unwrap();
    let cpp = cpp_eval_image_mask_range(
        &path,
        CppImageExprCompareOp::GreaterThan,
        1.5,
        CppMaskLogicalOp::And,
        CppImageExprCompareOp::LessThan,
        6.5,
        n,
    )
    .unwrap();

    assert_eq!(flatten_fortran(&rust), cpp);
}

#[test]
fn saved_lazy_expr_is_cpp_readable() {
    if !cpp_backend_available() {
        eprintln!("skipping saved_lazy_expr_is_cpp_readable: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let in_path = dir.path().join("source.image");
    let out_path = dir.path().join("expr.image");
    let shape = [3usize, 2usize];
    let n: usize = shape.iter().product();

    let image = make_image(&in_path, &shape, (0..n).map(|i| i as f32 + 1.0).collect()).unwrap();
    let expr = ImageExpr::from_image(&image)
        .unwrap()
        .multiply_scalar(2.0)
        .add_scalar(1.0);
    let rust = expr.get().unwrap();
    expr.save_as(&out_path).unwrap();

    let cpp = cpp_read_image_data(&out_path, n).unwrap();
    assert_eq!(flatten_fortran(&rust), cpp);
}

// ---- Wave 11b: exhaustive operator interop tests ----

#[test]
fn rust_lazy_transcendental_unary_ops_match_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trig.image");
    let shape = [3usize, 4usize];
    let n: usize = shape.iter().product();

    // Values in [0.05, 0.6] to stay in domain for all trig inverses
    let image = make_image(
        &path,
        &shape,
        (0..n).map(|i| 0.05 + (i as f32) * 0.05).collect(),
    )
    .unwrap();

    let cases: &[(ImageExprUnaryOp, CppImageExprUnaryOp)] = &[
        (ImageExprUnaryOp::Sin, CppImageExprUnaryOp::Sin),
        (ImageExprUnaryOp::Cos, CppImageExprUnaryOp::Cos),
        (ImageExprUnaryOp::Tan, CppImageExprUnaryOp::Tan),
        (ImageExprUnaryOp::Asin, CppImageExprUnaryOp::Asin),
        (ImageExprUnaryOp::Acos, CppImageExprUnaryOp::Acos),
        (ImageExprUnaryOp::Atan, CppImageExprUnaryOp::Atan),
        (ImageExprUnaryOp::Sinh, CppImageExprUnaryOp::Sinh),
        (ImageExprUnaryOp::Cosh, CppImageExprUnaryOp::Cosh),
        (ImageExprUnaryOp::Tanh, CppImageExprUnaryOp::Tanh),
        (ImageExprUnaryOp::Log, CppImageExprUnaryOp::Log),
        (ImageExprUnaryOp::Log10, CppImageExprUnaryOp::Log10),
        (ImageExprUnaryOp::Sqrt, CppImageExprUnaryOp::Sqrt),
        (ImageExprUnaryOp::Abs, CppImageExprUnaryOp::Abs),
        (ImageExprUnaryOp::Ceil, CppImageExprUnaryOp::Ceil),
        (ImageExprUnaryOp::Floor, CppImageExprUnaryOp::Floor),
        (ImageExprUnaryOp::Round, CppImageExprUnaryOp::Round),
        (ImageExprUnaryOp::Sign, CppImageExprUnaryOp::Sign),
        // Conj is complex-only in C++ LEL; tested in Rust unit tests for real types.
    ];

    for (rust_op, cpp_op) in cases {
        let rust = ImageExpr::from_image(&image)
            .unwrap()
            .unary(*rust_op)
            .get()
            .unwrap();
        let cpp = cpp_eval_image_expr_unary(&path, *cpp_op, n).unwrap();

        for (i, (r, c)) in flatten_fortran(&rust).iter().zip(cpp.iter()).enumerate() {
            assert!(
                (r - c).abs() < 1e-5,
                "{rust_op:?}: pixel {i}: rust={r}, cpp={c}"
            );
        }
    }
}

#[test]
fn rust_lazy_binary_subtract_divide_match_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let lhs_path = dir.path().join("lhs.image");
    let rhs_path = dir.path().join("rhs.image");
    let shape = [3usize, 4usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &lhs_path,
        &shape_i32,
        &(0..n).map(|i| i as f32 + 1.0).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    cpp_create_image(
        &rhs_path,
        &shape_i32,
        &(0..n).map(|i| (i as f32) * 0.3 + 0.5).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let lhs = PagedImage::<f32>::open(&lhs_path).unwrap();
    let rhs = PagedImage::<f32>::open(&rhs_path).unwrap();

    let cases: &[(ImageExprBinaryOp, CppImageExprBinaryOp)] = &[
        (ImageExprBinaryOp::Subtract, CppImageExprBinaryOp::Subtract),
        (ImageExprBinaryOp::Divide, CppImageExprBinaryOp::Divide),
        (ImageExprBinaryOp::Pow, CppImageExprBinaryOp::Pow),
        (ImageExprBinaryOp::Fmod, CppImageExprBinaryOp::Fmod),
        (ImageExprBinaryOp::Atan2, CppImageExprBinaryOp::Atan2),
        (ImageExprBinaryOp::Min, CppImageExprBinaryOp::Min),
        (ImageExprBinaryOp::Max, CppImageExprBinaryOp::Max),
    ];

    for (rust_op, cpp_op) in cases {
        let rust = ImageExpr::from_image(&lhs)
            .unwrap()
            .binary_image(&rhs, *rust_op)
            .unwrap()
            .get()
            .unwrap();
        let cpp = cpp_eval_image_expr_binary(&lhs_path, &rhs_path, *cpp_op, n).unwrap();

        for (i, (r, c)) in flatten_fortran(&rust).iter().zip(cpp.iter()).enumerate() {
            assert!(
                (r - c).abs() < 1e-4,
                "{rust_op:?}: pixel {i}: rust={r}, cpp={c}"
            );
        }
    }
}

#[test]
fn rust_lazy_scalar_subtract_divide_match_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scalar_ops.image");
    let shape = [4usize, 3usize];
    let n: usize = shape.iter().product();

    let image = make_image(&path, &shape, (0..n).map(|i| i as f32 + 1.0).collect()).unwrap();

    let cases: &[(ImageExprBinaryOp, CppImageExprBinaryOp, f32)] = &[
        (
            ImageExprBinaryOp::Subtract,
            CppImageExprBinaryOp::Subtract,
            2.5,
        ),
        (ImageExprBinaryOp::Divide, CppImageExprBinaryOp::Divide, 3.0),
        (ImageExprBinaryOp::Pow, CppImageExprBinaryOp::Pow, 2.0),
        (ImageExprBinaryOp::Fmod, CppImageExprBinaryOp::Fmod, 2.5),
        (ImageExprBinaryOp::Atan2, CppImageExprBinaryOp::Atan2, 3.0),
        (ImageExprBinaryOp::Min, CppImageExprBinaryOp::Min, 5.0),
        (ImageExprBinaryOp::Max, CppImageExprBinaryOp::Max, 5.0),
    ];

    for (rust_op, cpp_op, scalar) in cases {
        let rust = ImageExpr::from_image(&image)
            .unwrap()
            .binary_scalar(*scalar, *rust_op)
            .get()
            .unwrap();
        let cpp = cpp_eval_image_expr_scalar(&path, *scalar, *cpp_op, n).unwrap();

        for (i, (r, c)) in flatten_fortran(&rust).iter().zip(cpp.iter()).enumerate() {
            assert!(
                (r - c).abs() < 1e-4,
                "{rust_op:?} scalar={scalar}: pixel {i}: rust={r}, cpp={c}"
            );
        }
    }
}

#[test]
fn rust_lazy_extended_comparisons_match_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cmp.image");
    let shape = [3usize, 3usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| i as f32).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();

    // ge(3.0) && le(6.0)
    let rust = ImageExpr::from_image(&image)
        .unwrap()
        .ge_scalar(3.0)
        .and(ImageExpr::from_image(&image).unwrap().le_scalar(6.0))
        .unwrap()
        .get()
        .unwrap();
    let cpp = cpp_eval_image_mask_range(
        &path,
        CppImageExprCompareOp::GreaterEqual,
        3.0,
        CppMaskLogicalOp::And,
        CppImageExprCompareOp::LessEqual,
        6.0,
        n,
    )
    .unwrap();
    assert_eq!(flatten_fortran(&rust), cpp);

    // eq(4.0) || ne(4.0) should always be true
    let rust = ImageExpr::from_image(&image)
        .unwrap()
        .eq_scalar(4.0)
        .or(ImageExpr::from_image(&image).unwrap().ne_scalar(4.0))
        .unwrap()
        .get()
        .unwrap();
    let cpp = cpp_eval_image_mask_range(
        &path,
        CppImageExprCompareOp::Equal,
        4.0,
        CppMaskLogicalOp::Or,
        CppImageExprCompareOp::NotEqual,
        4.0,
        n,
    )
    .unwrap();
    assert_eq!(flatten_fortran(&rust), cpp);
}

// ---- Wave 12a: parser interop tests ----

fn make_resolver_for_paths<'a>(images: &[(&str, &'a PagedImage<f32>)]) -> HashMapResolver<'a, f32> {
    let mut map = HashMap::new();
    for &(name, img) in images {
        map.insert(name.to_string(), img as &dyn ImageInterface<f32>);
    }
    HashMapResolver(map)
}

#[test]
fn parsed_arithmetic_expr_matches_cpp_lel() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let a_path = dir.path().join("a.image");
    let b_path = dir.path().join("b.image");
    let shape = [3usize, 4usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &a_path,
        &shape_i32,
        &(0..n).map(|i| i as f32 + 1.0).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    cpp_create_image(
        &b_path,
        &shape_i32,
        &(0..n).map(|i| (i as f32) * 0.5 + 0.5).collect::<Vec<_>>(),
        "",
    )
    .unwrap();

    let a = PagedImage::<f32>::open(&a_path).unwrap();
    let b = PagedImage::<f32>::open(&b_path).unwrap();

    let a_str = a_path.to_str().unwrap();
    let b_str = b_path.to_str().unwrap();

    // Test: 'a' + 'b' * 2.0
    let expr_str = format!("'{a_str}' + '{b_str}' * 2.0");
    let resolver = make_resolver_for_paths(&[(a_str, &a), (b_str, &b)]);
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    let rust = rust_expr.get().unwrap();

    let (cpp, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();
    assert_eq!(flatten_fortran(&rust), cpp);
}

#[test]
fn parsed_transcendental_expr_matches_cpp_lel() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trig.image");
    let shape = [3usize, 4usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| 0.1 + (i as f32) * 0.05).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();

    let path_str = path.to_str().unwrap();
    let resolver = make_resolver_for_paths(&[(path_str, &image)]);

    // sin('img')
    let expr_str = format!("sin('{path_str}')");
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    let rust = rust_expr.get().unwrap();
    let (cpp, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();

    for (i, (r, c)) in flatten_fortran(&rust).iter().zip(cpp.iter()).enumerate() {
        assert!((r - c).abs() < 1e-5, "sin: pixel {i}: rust={r}, cpp={c}");
    }

    // sqrt(abs('img'))
    let expr_str2 = format!("sqrt(abs('{path_str}'))");
    let rust_expr2 = parse_image_expr(&expr_str2, &resolver).unwrap();
    let rust2 = rust_expr2.get().unwrap();
    let (cpp2, _) = cpp_eval_lel_expr(&expr_str2, n).unwrap();

    for (i, (r, c)) in flatten_fortran(&rust2).iter().zip(cpp2.iter()).enumerate() {
        assert!(
            (r - c).abs() < 1e-5,
            "sqrt(abs): pixel {i}: rust={r}, cpp={c}"
        );
    }
}

#[test]
fn parsed_mask_expr_matches_cpp_lel() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mask.image");
    let shape = [3usize, 3usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| i as f32).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();

    let path_str = path.to_str().unwrap();
    let resolver = make_resolver_for_paths(&[(path_str, &image)]);

    // 'img' > 1.5 && 'img' < 6.5
    let expr_str = format!("'{path_str}' > 1.5 && '{path_str}' < 6.5");
    let rust_mask = parse_mask_expr(&expr_str, &resolver).unwrap();
    let rust = rust_mask.get().unwrap();

    let (cpp, _) = cpp_eval_lel_expr_mask(&expr_str, n).unwrap();

    let rust_flat: Vec<bool> = flatten_fortran(&rust);
    assert_eq!(rust_flat, cpp);
}

#[test]
fn parsed_composite_expr_matches_cpp_lel() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("comp.image");
    let shape = [4usize, 3usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    // Use C++ to create the image so coordinates are C++-compatible
    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| i as f32 + 1.0).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();

    let path_str = path.to_str().unwrap();
    let resolver = make_resolver_for_paths(&[(path_str, &image)]);

    // max(atan2(sqrt('img' + 1.0), fmod(pow('img' + 0.5, 2.0), 3.0) + 0.25), 0.5)
    let expr_str = format!(
        "max(atan2(sqrt('{p}' + 1.0), fmod(pow('{p}' + 0.5, 2.0), 3.0) + 0.25), 0.5)",
        p = path_str
    );
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    let rust = rust_expr.get().unwrap();
    let (cpp, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();

    for (i, (r, c)) in flatten_fortran(&rust).iter().zip(cpp.iter()).enumerate() {
        assert!(
            (r - c).abs() < 1e-4,
            "composite: pixel {i}: rust={r}, cpp={c}"
        );
    }
}

#[test]
fn parsed_quoted_path_with_special_chars_matches_cpp_lel() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    // Use a path with dots and dashes that must be quoted
    let path = dir.path().join("my-test.image");
    let shape = [2usize, 3usize];
    let n: usize = shape.iter().product();
    let shape_i32: Vec<i32> = shape.iter().map(|&v| v as i32).collect();

    cpp_create_image(
        &path,
        &shape_i32,
        &(0..n).map(|i| i as f32 * 2.0).collect::<Vec<_>>(),
        "",
    )
    .unwrap();
    let image = PagedImage::<f32>::open(&path).unwrap();

    let path_str = path.to_str().unwrap();
    let resolver = make_resolver_for_paths(&[(path_str, &image)]);

    // Must quote the path due to the dash
    let expr_str = format!("'{path_str}' + 1.0");
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    let rust = rust_expr.get().unwrap();
    let (cpp, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();

    assert_eq!(flatten_fortran(&rust), cpp);
}

// ===========================================================================
// Wave 12b — Expression file persistence interop
// ===========================================================================

/// Rust saves `.imgexpr` → C++ opens it and reads matching pixel values.
#[test]
fn rust_save_imgexpr_cpp_opens() {
    if !cpp_backend_available() {
        eprintln!("skipping rust_save_imgexpr_cpp_opens: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [8, 8];
    let n = 64usize;

    // Create a source image via C++ for coordinate compatibility.
    let img_path = dir.path().join("src.image");
    let data: Vec<f32> = (0..n).map(|i| 1.0 + i as f32 * 0.5).collect();
    cpp_create_image(&img_path, &shape_i32, &data, "").unwrap();

    // Build LEL expression string with the image path.
    let img_path_str = img_path.to_str().unwrap();
    let expr_str = format!("'{img_path_str}' * 2.0 + 1.0");

    // Open the image in Rust and parse the expression.
    let image = PagedImage::<f32>::open(&img_path).unwrap();
    let mut images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    images.insert(img_path_str.to_string(), &image);
    let resolver = HashMapResolver(images);
    let parsed = parse_image_expr(&expr_str, &resolver).unwrap();

    // Save as .imgexpr using Rust.
    let expr_path = dir.path().join("rust_expr.imgexpr");
    parsed.save_expr(&expr_path).unwrap();

    // Verify the directory and JSON exist.
    assert!(expr_path.join("imageexpr.json").is_file());

    // Open with C++ and read pixel data.
    // Note: C++ LEL promotes floating-point literals to Double, so the opened
    // image may be ImageExpr<Double>; the C++ wrapper handles this conversion.
    let (cpp_data, cpp_shape) = cpp_open_lel_expr_file(&expr_path, n).unwrap();
    assert_eq!(cpp_shape, vec![8, 8]);

    // Evaluate in Rust for comparison.
    let rust_data = flatten_fortran(&parsed.get().unwrap());
    for (i, (&r, &c)) in rust_data.iter().zip(cpp_data.iter()).enumerate() {
        assert!((r - c).abs() < 1e-5, "pixel {i}: rust={r}, cpp={c}");
    }
}

/// C++ saves `.imgexpr` → Rust opens it and reads matching pixel values.
#[test]
fn cpp_save_imgexpr_rust_opens() {
    if !cpp_backend_available() {
        eprintln!("skipping cpp_save_imgexpr_rust_opens: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [8, 8];
    let n = 64usize;

    // Create source image via C++.
    let img_path = dir.path().join("src.image");
    let data: Vec<f32> = (0..n).map(|i| 2.0 + i as f32 * 0.1).collect();
    cpp_create_image(&img_path, &shape_i32, &data, "").unwrap();

    // Build LEL expression.
    let img_path_str = img_path.to_str().unwrap();
    let expr_str = format!("sqrt('{img_path_str}') + 1.0");

    // Save as .imgexpr using C++.
    let expr_path = dir.path().join("cpp_expr.imgexpr");
    cpp_save_lel_expr_file(&expr_str, &expr_path).unwrap();

    // Open with Rust.
    let owned = casacore_images::expr_file::open::<f32>(&expr_path).unwrap();
    let rust_data = owned.get().unwrap();

    // Evaluate the same expression directly with C++ for reference.
    let (cpp_data, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();

    let rust_flat = flatten_fortran(&rust_data);
    for (i, (&r, &c)) in rust_flat.iter().zip(cpp_data.iter()).enumerate() {
        assert!((r - c).abs() < 1e-5, "pixel {i}: rust={r}, cpp={c}");
    }
}

#[test]
fn two_image_imgexpr_cross_matrix_matches_expected_pixels() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping two_image_imgexpr_cross_matrix_matches_expected_pixels: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape = [4usize, 5usize];
    let shape_i32 = [4i32, 5i32];
    let n: usize = shape.iter().product();

    let lhs_path = dir.path().join("lhs.image");
    let rhs_path = dir.path().join("rhs.image");

    let lhs_values: Vec<f32> = (0..n).map(|i| 1.0 + (i as f32) * 0.25).collect();
    let rhs_values: Vec<f32> = (0..n).map(|i| 0.5 + ((i % 7) as f32) * 0.4).collect();
    cpp_create_image(&lhs_path, &shape_i32, &lhs_values, "").unwrap();
    cpp_create_image(&rhs_path, &shape_i32, &rhs_values, "").unwrap();
    let lhs = PagedImage::<f32>::open(&lhs_path).unwrap();
    let rhs = PagedImage::<f32>::open(&rhs_path).unwrap();

    let lhs_str = lhs_path.to_str().unwrap();
    let rhs_str = rhs_path.to_str().unwrap();
    let expr_str = format!(
        "sqrt(abs('{lhs}' * 1.5 - '{rhs}' / 2.0)) + max('{lhs}', '{rhs}')",
        lhs = lhs_str,
        rhs = rhs_str,
    );
    let expected: Vec<f32> = lhs_values
        .iter()
        .zip(rhs_values.iter())
        .map(|(&lhs, &rhs)| ((lhs * 1.5 - rhs / 2.0).abs().sqrt()) + lhs.max(rhs))
        .collect();

    let resolver = make_resolver_for_paths(&[(lhs_str, &lhs), (rhs_str, &rhs)]);
    let parsed = parse_image_expr(&expr_str, &resolver).unwrap();

    let rust_expr_path = dir.path().join("rust_matrix.imgexpr");
    parsed.save_expr(&rust_expr_path).unwrap();

    let rr = casacore_images::expr_file::open::<f32>(&rust_expr_path)
        .unwrap()
        .get()
        .unwrap();
    assert_float_close("RR", &flatten_fortran(&rr), &expected, 1.0e-5);

    let (rc, rc_shape) = cpp_open_lel_expr_file(&rust_expr_path, n).unwrap();
    assert_eq!(rc_shape, shape_i32.to_vec());
    assert_float_close("RC", &rc, &expected, 1.0e-4);

    let cpp_expr_path = dir.path().join("cpp_matrix.imgexpr");
    cpp_save_lel_expr_file(&expr_str, &cpp_expr_path).unwrap();

    let cr = casacore_images::expr_file::open::<f32>(&cpp_expr_path)
        .unwrap()
        .get()
        .unwrap();
    assert_float_close("CR", &flatten_fortran(&cr), &expected, 1.0e-4);

    let (cc, cc_shape) = cpp_open_lel_expr_file(&cpp_expr_path, n).unwrap();
    assert_eq!(cc_shape, shape_i32.to_vec());
    assert_float_close("CC", &cc, &expected, 1.0e-4);
}

/// Nested expression: save an expression referencing another .imgexpr.
#[test]
fn nested_imgexpr_round_trip() {
    if !cpp_backend_available() {
        eprintln!("skipping nested_imgexpr_round_trip: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [4, 4];
    let n = 16usize;

    // Create source image via C++.
    let img_path = dir.path().join("base.image");
    let data: Vec<f32> = (0..n).map(|i| 1.0 + i as f32).collect();
    cpp_create_image(&img_path, &shape_i32, &data, "").unwrap();

    // Create first expression: base + 10
    let img_path_str = img_path.to_str().unwrap();
    let expr1_str = format!("'{img_path_str}' + 10.0");
    let expr1_path = dir.path().join("expr1.imgexpr");
    cpp_save_lel_expr_file(&expr1_str, &expr1_path).unwrap();

    // Create second expression referencing the first: expr1 * 2
    let expr1_path_str = expr1_path.to_str().unwrap();
    let expr2_str = format!("'{expr1_path_str}' * 2.0");
    let expr2_path = dir.path().join("expr2.imgexpr");
    cpp_save_lel_expr_file(&expr2_str, &expr2_path).unwrap();

    // Open the nested expression with C++.
    let (cpp_data, _) = cpp_open_lel_expr_file(&expr2_path, n).unwrap();

    // Verify: (base + 10) * 2
    let expected: Vec<f32> = data.iter().map(|&v| (v + 10.0) * 2.0).collect();
    for (i, (&e, &c)) in expected.iter().zip(cpp_data.iter()).enumerate() {
        assert!((e - c).abs() < 1e-5, "pixel {i}: expected={e}, cpp={c}");
    }
}

/// Negative case: Rust open of non-existent .imgexpr fails gracefully.
#[test]
fn open_nonexistent_imgexpr_errors() {
    let result = casacore_images::expr_file::open::<f32>("/nonexistent/path.imgexpr");
    assert!(result.is_err());
}

/// Negative case: Rust open of .imgexpr with missing source image fails.
#[test]
fn open_imgexpr_with_missing_source_errors() {
    let dir = tempfile::tempdir().unwrap();
    let expr_path = dir.path().join("bad.imgexpr");
    casacore_images::expr_file::save(
        &expr_path,
        "'nonexistent.image' + 1.0",
        casacore_types::PrimitiveType::Float32,
        &casacore_types::RecordValue::default(),
    )
    .unwrap();

    let result = casacore_images::expr_file::open::<f32>(&expr_path);
    assert!(result.is_err());
}

/// Rust save_expr requires an expression string to be set.
#[test]
fn save_expr_without_string_errors() {
    let img = casacore_images::TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
    let expr = ImageExpr::from_image(&img).unwrap();
    // Programmatic expression — no expr_string set.
    let dir = tempfile::tempdir().unwrap();
    let result = expr.save_expr(dir.path().join("should_fail.imgexpr"));
    assert!(result.is_err());
}

// =========================================================================
// Wave 14 interop: Rust-parsed LEL vs C++ evaluated LEL
// =========================================================================

#[test]
fn wave14_isnan_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [2i32, 2];
    let n = 4usize;
    // Pixel at col-major position 1 is NaN.
    let data = vec![1.0f32, f32::NAN, 3.0, 4.0];
    let (a, path) = make_cpp_image(dir.path(), "nan.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("isnan('{a_str}')");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_mask = parse_mask_expr(&expr_str, &resolver).unwrap();
    let rust = casacore_lattices::Lattice::get(&rust_mask).unwrap();

    let (cpp, _) = cpp_eval_lel_expr_mask(&expr_str, n).unwrap();
    let rust_flat: Vec<bool> = flatten_fortran(&rust);
    assert_eq!(
        rust_flat, cpp,
        "isnan mismatch: Rust={rust_flat:?} vs C++={cpp:?}"
    );
}

#[test]
fn wave14_mask_and_replace_on_masked_derived_expr_match_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape = [2usize, 2usize];
    let shape_i32 = [2i32, 2i32];
    let n = 4usize;
    let path = dir.path().join("masked.image");
    let data = vec![1.0f32, f32::INFINITY, 3.0, 4.0];
    cpp_create_image(&path, &shape_i32, &data, "").unwrap();

    let mut image = PagedImage::<f32>::open(&path).unwrap();
    image.make_mask("quality", true, true).unwrap();
    let mask = ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![true, false, true, true]).unwrap();
    image.put_mask("quality", &mask).unwrap();
    image.set_default_mask("quality").unwrap();
    image.save().unwrap();

    let path_str = path.to_str().unwrap();
    let resolver = make_resolver_for_paths(&[(path_str, &image)]);

    let mask_expr_str = format!("mask('{path_str}' + 1.0)");
    let rust_mask = parse_mask_expr(&mask_expr_str, &resolver).unwrap();
    let rust_mask = flatten_fortran(&rust_mask.get().unwrap());
    let (cpp_mask, cpp_mask_shape) = cpp_eval_lel_expr_mask(&mask_expr_str, n).unwrap();
    assert_eq!(cpp_mask_shape, shape_i32.to_vec());
    assert_eq!(rust_mask, vec![true, false, true, true]);
    assert_eq!(rust_mask, cpp_mask);

    let replace_expr_str = format!("replace('{path_str}' + 1.0, 42.0)");
    let rust_expr = parse_image_expr(&replace_expr_str, &resolver).unwrap();
    let rust = flatten_fortran(&rust_expr.get().unwrap());
    let expected = vec![2.0f32, 42.0, 4.0, 5.0];
    assert_float_close("replace-masked-derived-rust", &rust, &expected, 1e-5);

    let (cpp, cpp_shape) = cpp_eval_lel_expr(&replace_expr_str, n).unwrap();
    assert_eq!(cpp_shape, shape_i32.to_vec());
    assert_float_close("replace-masked-derived-cpp", &cpp, &expected, 1e-5);
    assert_float_close("replace-masked-derived-cross", &rust, &cpp, 1e-5);
}

/// Helper: create a C++ image and return the opened Rust PagedImage + path string.
fn make_cpp_image(
    dir: &std::path::Path,
    name: &str,
    shape_i32: &[i32],
    data: &[f32],
) -> (PagedImage<f32>, std::path::PathBuf) {
    let path = dir.join(name);
    cpp_create_image(&path, shape_i32, data, "").unwrap();
    let img = PagedImage::<f32>::open(&path).unwrap();
    (img, path)
}

#[test]
fn wave14_sum_scalar_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [3i32, 4];
    let n: usize = 12;
    let data: Vec<f32> = (0..n).map(|i| i as f32 + 1.0).collect();
    let (a, path) = make_cpp_image(dir.path(), "vals.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("sum('{a_str}')");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    assert!(rust_expr.shape().is_empty());
    let rust = rust_expr.get().unwrap()[IxDyn(&[])];

    let (cpp, cpp_shape) = cpp_eval_lel_expr(&expr_str, 1).unwrap();
    assert!(cpp_shape.is_empty());
    assert_eq!(cpp.len(), 1);
    assert!(
        (rust - cpp[0]).abs() < 1e-2,
        "sum scalar mismatch: rust={rust}, cpp={}",
        cpp[0]
    );
}

#[test]
fn wave14_mean_scalar_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [2i32, 3];
    let n: usize = 6;
    let data: Vec<f32> = (0..n).map(|i| i as f32 + 1.0).collect();
    let (a, path) = make_cpp_image(dir.path(), "vals.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("mean('{a_str}')");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    assert!(rust_expr.shape().is_empty());
    let rust = rust_expr.get().unwrap()[IxDyn(&[])];

    let (cpp, cpp_shape) = cpp_eval_lel_expr(&expr_str, 1).unwrap();
    assert!(cpp_shape.is_empty());
    assert_eq!(cpp.len(), 1);
    assert!(
        (rust - cpp[0]).abs() < 1e-5,
        "mean scalar mismatch: rust={rust}, cpp={}",
        cpp[0]
    );
}

#[test]
fn wave14_iif_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [2i32, 2];
    let n = 4usize;
    let data = vec![1.0f32, 5.0, 3.0, 7.0];
    let (a, path) = make_cpp_image(dir.path(), "cond.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("iif('{a_str}' > 4.0, '{a_str}' + 100.0, '{a_str}')");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    let rust = flatten_fortran(&rust_expr.get().unwrap());
    let (cpp, _) = cpp_eval_lel_expr(&expr_str, n).unwrap();
    assert_float_close("iif", &rust, &cpp, 1e-5);
}

#[test]
fn wave14_ntrue_scalar_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [2i32, 2];
    let data = vec![1.0f32, 5.0, 3.0, 7.0];
    let (a, path) = make_cpp_image(dir.path(), "cnt.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("ntrue('{a_str}' > 4.0)");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_expr = parse_image_expr(&expr_str, &resolver).unwrap();
    assert!(rust_expr.shape().is_empty());
    let rust = rust_expr.get().unwrap()[IxDyn(&[])];
    let (cpp, cpp_shape) = cpp_eval_lel_expr(&expr_str, 1).unwrap();
    assert!(cpp_shape.is_empty());
    assert_eq!(cpp.len(), 1);
    assert!(
        (rust - cpp[0]).abs() < 1e-5,
        "ntrue scalar mismatch: rust={rust}, cpp={}",
        cpp[0]
    );
}

#[test]
fn wave14_all_scalar_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let shape_i32 = [2i32, 2];
    let data = vec![1.0f32, 5.0, 3.0, 7.0];
    let (a, path) = make_cpp_image(dir.path(), "all.image", &shape_i32, &data);
    let a_str = path.to_str().unwrap();

    let expr_str = format!("all('{a_str}' > 0.0)");
    let resolver = make_resolver_for_paths(&[(a_str, &a)]);
    let rust_mask = parse_mask_expr(&expr_str, &resolver).unwrap();
    assert!(rust_mask.shape().is_empty());
    let rust = rust_mask.get().unwrap()[IxDyn(&[])];
    let (cpp, cpp_shape) = cpp_eval_lel_expr_mask(&expr_str, 1).unwrap();
    assert!(cpp_shape.is_empty());
    assert_eq!(cpp, vec![rust]);
}
