// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of `ImageExpr` — lazy image expressions.
//!
//! Corresponds to C++ `tImageExpr.cc`.  All operations evaluate lazily;
//! pixels are computed only when read.
//!
//! Wave 12a adds parsing of LEL expression strings into the lazy DAG.
//! Wave 12b adds `.imgexpr` persistence (save/open cycle).

use std::collections::HashMap;

use casacore_coordinates::CoordinateSystem;
use casacore_images::expr_file;
use casacore_images::expr_parser::{HashMapResolver, parse_image_expr, parse_mask_expr};
use casacore_images::image::ImageInterface;
use casacore_images::{ImageExpr, ImageExprUnaryOp, TempImage};

fn main() {
    let mut lhs = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
    let mut rhs = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
    lhs.set(1.5).unwrap();
    rhs.set(2.0).unwrap();

    // --- Programmatic API (Wave 11) ---

    // Arithmetic + unary negate
    let expr = ImageExpr::from_image(&lhs)
        .unwrap()
        .add_image(&rhs)
        .unwrap()
        .unary(ImageExprUnaryOp::Negate);
    println!("-(lhs + rhs)[0,0] = {}", expr.get_at(&[0, 0]).unwrap());

    // Transcendental chain: sqrt(lhs^2 + rhs^2)
    let magnitude = ImageExpr::from_image(&lhs)
        .unwrap()
        .pow_scalar(2.0)
        .add_expr(ImageExpr::from_image(&rhs).unwrap().pow_scalar(2.0))
        .unwrap()
        .sqrt();
    println!(
        "sqrt(lhs^2 + rhs^2)[0,0] = {}",
        magnitude.get_at(&[0, 0]).unwrap()
    );

    // Clamp via min/max
    let clamped = ImageExpr::from_image(&lhs)
        .unwrap()
        .multiply_scalar(10.0)
        .min_scalar(12.0)
        .max_scalar(5.0);
    println!(
        "clamp(lhs*10, 5, 12)[0,0] = {}",
        clamped.get_at(&[0, 0]).unwrap()
    );

    // Comparison mask: which pixels > 1.0 && <= 2.0?
    let mask = ImageExpr::from_image(&lhs)
        .unwrap()
        .gt_scalar(1.0)
        .and(ImageExpr::from_image(&lhs).unwrap().le_scalar(2.0))
        .unwrap();
    println!(
        "mask (1.0 < lhs <= 2.0)[0,0] = {}",
        mask.get_at(&[0, 0]).unwrap()
    );

    // Persist
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("expr.image");
    let persisted = expr.save_as(&out).unwrap();
    println!(
        "Persisted expression image to {:?}",
        persisted.name().unwrap()
    );

    // --- Parser API (Wave 12a) ---
    println!("\n--- LEL expression parser ---");

    let mut images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    images.insert("lhs".to_string(), &lhs);
    images.insert("rhs".to_string(), &rhs);
    let resolver = HashMapResolver(images);

    // Parse arithmetic expression from string
    let parsed = parse_image_expr("-('lhs' + 'rhs')", &resolver).unwrap();
    println!(
        "parsed: -('lhs' + 'rhs')[0,0] = {}",
        parsed.get_at(&[0, 0]).unwrap()
    );

    // Parse transcendental chain from string
    let parsed_mag = parse_image_expr("sqrt('lhs' ^ 2.0 + 'rhs' ^ 2.0)", &resolver).unwrap();
    println!(
        "parsed: sqrt(lhs^2 + rhs^2)[0,0] = {}",
        parsed_mag.get_at(&[0, 0]).unwrap()
    );

    // Parse min/max clamp from string
    let parsed_clamp = parse_image_expr("max(min('lhs' * 10.0, 12.0), 5.0)", &resolver).unwrap();
    println!(
        "parsed: clamp(lhs*10, 5, 12)[0,0] = {}",
        parsed_clamp.get_at(&[0, 0]).unwrap()
    );

    // Parse comparison mask from string
    let parsed_mask = parse_mask_expr("'lhs' > 1.0 && 'lhs' <= 2.0", &resolver).unwrap();
    println!(
        "parsed mask: (1.0 < lhs <= 2.0)[0,0] = {}",
        parsed_mask.get_at(&[0, 0]).unwrap()
    );

    // Constants
    let pi_expr = parse_image_expr("'lhs' * pi()", &resolver).unwrap();
    println!(
        "parsed: lhs * pi()[0,0] = {}",
        pi_expr.get_at(&[0, 0]).unwrap()
    );

    // --- Expression file persistence (Wave 12b) ---
    println!("\n--- .imgexpr save/open cycle ---");

    // Save source images to disk so .imgexpr can reference them.
    let lhs_path = dir.path().join("lhs.image");
    let rhs_path = dir.path().join("rhs.image");
    let _lhs_paged = lhs.save_as(&lhs_path).unwrap();
    let _rhs_paged = rhs.save_as(&rhs_path).unwrap();

    // Parse expression with on-disk image paths.
    let lhs_disk = casacore_images::PagedImage::<f32>::open(&lhs_path).unwrap();
    let rhs_disk = casacore_images::PagedImage::<f32>::open(&rhs_path).unwrap();
    let lhs_s = lhs_path.to_str().unwrap();
    let rhs_s = rhs_path.to_str().unwrap();
    let mut disk_images: HashMap<String, &dyn ImageInterface<f32>> = HashMap::new();
    disk_images.insert(lhs_s.to_string(), &lhs_disk);
    disk_images.insert(rhs_s.to_string(), &rhs_disk);
    let disk_resolver = HashMapResolver(disk_images);
    let disk_expr = parse_image_expr(&format!("-('{lhs_s}' + '{rhs_s}')"), &disk_resolver).unwrap();

    // Save as .imgexpr directory.
    let expr_dir = dir.path().join("negated.imgexpr");
    disk_expr.save_expr(&expr_dir).unwrap();
    println!("Saved .imgexpr to {expr_dir:?}");

    // Check that it's recognized as an expression image.
    assert!(expr_file::is_image_expr(&expr_dir));
    println!("is_image_expr = true");

    // Read metadata without evaluating.
    let info = expr_file::read_info(&expr_dir).unwrap();
    println!(
        "Expression: {:?}, DataType: {:?}",
        info.expr_string, info.data_type
    );

    // Open and evaluate the expression from disk.
    let owned = expr_file::open::<f32>(&expr_dir).unwrap();
    println!(
        "Opened .imgexpr pixel[0,0] = {} (expected {})",
        owned.get_at(&[0, 0]).unwrap(),
        disk_expr.get_at(&[0, 0]).unwrap()
    );
}
