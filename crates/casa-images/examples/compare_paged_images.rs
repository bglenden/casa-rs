// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compare pixel and mask topology for two matching casacore image prefixes.

use std::{env, ffi::OsString, path::PathBuf};

use casa_images::PagedImage;
use serde_json::{Map, Value, json};

const PRODUCTS: &[&str] = &[
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".mask",
    ".model.tt0",
    ".model.tt1",
    ".pb.tt0",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
];

fn product_path(prefix: &PathBuf, suffix: &str) -> PathBuf {
    let mut path = OsString::from(prefix.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: compare_paged_images LEFT_PREFIX RIGHT_PREFIX".into());
    }
    let left_prefix = PathBuf::from(&args[0]);
    let right_prefix = PathBuf::from(&args[1]);
    let mut products = Map::new();
    for &suffix in PRODUCTS {
        let left = PagedImage::<f32>::open(product_path(&left_prefix, suffix))?;
        let right = PagedImage::<f32>::open(product_path(&right_prefix, suffix))?;
        let shape_equal = left.shape() == right.shape();
        let units_equal = left.units() == right.units();
        let mask_names_equal = left.mask_names() == right.mask_names();
        let default_mask_name_equal = left.default_mask_name() == right.default_mask_name();
        let left_pixels = left.get()?;
        let right_pixels = right.get()?;
        if left_pixels.shape() != right_pixels.shape() {
            return Err(format!(
                "{suffix} pixel shapes differ: {:?} versus {:?}",
                left_pixels.shape(),
                right_pixels.shape()
            )
            .into());
        }
        let mut exact_pixels = 0usize;
        let mut left_nonzero_pixels = 0usize;
        let mut right_nonzero_pixels = 0usize;
        let mut finite_topology_mismatches = 0usize;
        let mut finite_overlap = 0usize;
        let mut squared_difference = 0.0f64;
        let mut squared_right = 0.0f64;
        let mut max_abs_difference = 0.0f32;
        let mut max_abs_right = 0.0f32;
        for (&left_value, &right_value) in left_pixels.iter().zip(right_pixels.iter()) {
            left_nonzero_pixels += usize::from(left_value != 0.0);
            right_nonzero_pixels += usize::from(right_value != 0.0);
            if left_value.to_bits() == right_value.to_bits() {
                exact_pixels += 1;
            }
            let left_finite = left_value.is_finite();
            let right_finite = right_value.is_finite();
            if left_finite != right_finite {
                finite_topology_mismatches += 1;
            }
            if left_finite && right_finite {
                finite_overlap += 1;
                let difference = left_value - right_value;
                squared_difference += f64::from(difference) * f64::from(difference);
                squared_right += f64::from(right_value) * f64::from(right_value);
                max_abs_difference = max_abs_difference.max(difference.abs());
                max_abs_right = max_abs_right.max(right_value.abs());
            }
        }
        let left_mask = left.get_mask()?;
        let right_mask = right.get_mask()?;
        let mask_topology_equal = left_mask.is_some() == right_mask.is_some();
        let mask_mismatches = match (left_mask, right_mask) {
            (Some(left), Some(right)) => {
                if left.shape() != right.shape() {
                    return Err(format!(
                        "{suffix} mask shapes differ: {:?} versus {:?}",
                        left.shape(),
                        right.shape()
                    )
                    .into());
                }
                left.iter()
                    .zip(right.iter())
                    .filter(|(left, right)| left != right)
                    .count()
            }
            (None, None) => 0,
            _ => left_pixels.len(),
        };
        let diff_rms = if finite_overlap == 0 {
            0.0
        } else {
            (squared_difference / finite_overlap as f64).sqrt()
        };
        let right_rms = if finite_overlap == 0 {
            0.0
        } else {
            (squared_right / finite_overlap as f64).sqrt()
        };
        products.insert(
            suffix.to_string(),
            json!({
                "pixels": left_pixels.len(),
                "exact_pixels": exact_pixels,
                "left_nonzero_pixels": left_nonzero_pixels,
                "right_nonzero_pixels": right_nonzero_pixels,
                "finite_overlap": finite_overlap,
                "finite_topology_mismatches": finite_topology_mismatches,
                "mask_topology_equal": mask_topology_equal,
                "mask_mismatches": mask_mismatches,
                "shape_equal": shape_equal,
                "units_equal": units_equal,
                "mask_names_equal": mask_names_equal,
                "default_mask_name_equal": default_mask_name_equal,
                "diff_rms": diff_rms,
                "right_rms": right_rms,
                "diff_rms_over_right_rms": if right_rms == 0.0 { 0.0 } else { diff_rms / right_rms },
                "max_abs_difference": max_abs_difference,
                "max_abs_right": max_abs_right,
                "max_abs_difference_over_right_peak": if max_abs_right == 0.0 {
                    0.0
                } else {
                    max_abs_difference / max_abs_right
                },
            }),
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "left_prefix": left_prefix,
            "right_prefix": right_prefix,
            "products": Value::Object(products),
        }))?
    );
    Ok(())
}
