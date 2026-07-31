// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded frozen-final-state product-formation diagnostic for the VLASS row.
//!
//! This example implements only phase A of the diagnostic: it consumes frozen
//! CASA model and residual terms, runs casa-rs restoration and MT-MFS spectral
//! product arithmetic, and writes a cloned 19-product tree for the unchanged
//! comparison harness. It never opens a MeasurementSet or convolution-function
//! cache and cannot enter prediction, gridding, or deconvolution.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use casa_images::PagedImage;
use casa_imaging::{BeamFit, restore_standard_mfs_model};
use ndarray::{Array2, Axis};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const IMAGE_SIDE: usize = 4096;
const CELL_ARCSEC: f64 = 0.6;
const BEAM_MAJOR_ARCSEC: f64 = 3.202_949_762_344_360_4;
const BEAM_MINOR_ARCSEC: f64 = 2.157_604_455_947_876;
const BEAM_POSITION_ANGLE_DEG: f64 = 70.553_497_314_453_12;
const PRINCIPAL_INVERSE: [[f32; 2]; 2] = [[1.008_975_6, -0.420_447_83], [-0.420_447_83, 19.695_16]];
const CASA_ORACLE_MANIFEST_SHA256: &str =
    "59f03bad4d43b79ee7c2d8ead4cb10a53d9b3fc76cf1a300e5251551c3db2c02";
const V6_NATIVE_COMPONENT_RECEIPT_SHA256: &str =
    "35c3281f882fbe61cc512b4e489f25253904575be1deb027226e4017020c37b7";
const PRODUCT_SUFFIXES: [&str; 19] = [
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

fn product_path(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", prefix.display()))
}

fn plane(prefix: &Path, suffix: &str) -> Result<Array2<f32>, String> {
    let path = product_path(prefix, suffix);
    let image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    let shape = image.shape();
    if shape != [IMAGE_SIDE, IMAGE_SIDE, 1, 1] {
        return Err(format!(
            "{} has shape {shape:?}, expected [{IMAGE_SIDE}, {IMAGE_SIDE}, 1, 1]",
            path.display()
        ));
    }
    let values = image
        .get_slice(&[0, 0, 0, 0], shape)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), values.iter().copied().collect())
        .map_err(|error| format!("reshape {}: {error}", path.display()))
}

fn default_mask(prefix: &Path, suffix: &str) -> Result<Array2<bool>, String> {
    let path = product_path(prefix, suffix);
    let image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    let shape = image.shape();
    let values = image
        .get_mask_slice(&[0, 0, 0, 0], shape, &[1, 1, 1, 1])
        .map_err(|error| format!("read default mask {}: {error}", path.display()))?
        .ok_or_else(|| format!("{} has no default pixel mask", path.display()))?;
    Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), values.iter().copied().collect())
        .map_err(|error| format!("reshape default mask {}: {error}", path.display()))
}

fn clone_product_tree(source_prefix: &Path, output_prefix: &Path) -> Result<(), String> {
    if let Some(parent) = output_prefix.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create output parent {}: {error}", parent.display()))?;
    }
    for suffix in PRODUCT_SUFFIXES {
        let source = product_path(source_prefix, suffix);
        let output = product_path(output_prefix, suffix);
        if !source.is_dir() {
            return Err(format!(
                "frozen source product is missing: {}",
                source.display()
            ));
        }
        if output.exists() {
            return Err(format!(
                "refusing to overwrite diagnostic product: {}",
                output.display()
            ));
        }
        let status = Command::new("/bin/cp")
            .args(["-cR"])
            .arg(&source)
            .arg(&output)
            .status()
            .map_err(|error| format!("clone {}: {error}", source.display()))?;
        if !status.success() {
            return Err(format!(
                "clone {} to {} exited {status}",
                source.display(),
                output.display()
            ));
        }
    }
    Ok(())
}

fn write_plane(prefix: &Path, suffix: &str, values: &Array2<f32>) -> Result<(), String> {
    let path = product_path(prefix, suffix);
    let mut image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open output {}: {error}", path.display()))?;
    let expanded = values.view().insert_axis(Axis(2)).insert_axis(Axis(3));
    image
        .put_slice_view(expanded.into_dyn(), &[0, 0, 0, 0])
        .map_err(|error| format!("write output {}: {error}", path.display()))
}

fn write_default_mask(prefix: &Path, suffix: &str, values: &Array2<bool>) -> Result<(), String> {
    let path = product_path(prefix, suffix);
    let mut image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open mask output {}: {error}", path.display()))?;
    let name = image
        .default_mask_name()
        .ok_or_else(|| format!("{} has no default mask name", path.display()))?;
    let expanded = values
        .view()
        .insert_axis(Axis(2))
        .insert_axis(Axis(3))
        .to_owned()
        .into_dyn();
    image
        .put_mask(&name, &expanded)
        .map_err(|error| format!("write mask output {}: {error}", path.display()))
}

fn principal_terms(raw: &[Array2<f32>; 2]) -> [Array2<f32>; 2] {
    let mut first = Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE));
    let mut second = Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE));
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            let rhs0 = raw[0][(x, y)];
            let rhs1 = raw[1][(x, y)];
            first[(x, y)] = PRINCIPAL_INVERSE[0][0] * rhs0 + PRINCIPAL_INVERSE[0][1] * rhs1;
            second[(x, y)] = PRINCIPAL_INVERSE[1][0] * rhs0 + PRINCIPAL_INVERSE[1][1] * rhs1;
        }
    }
    [first, second]
}

fn alpha_products(
    image_terms: &[Array2<f32>; 2],
    principal_residual_terms: &[Array2<f32>; 2],
) -> (Array2<f32>, Array2<f32>, Array2<bool>, f32) {
    let threshold = principal_residual_terms[0]
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max)
        / 10.0;
    let mut alpha = Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE));
    let mut alpha_error = Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE));
    let mut alpha_mask = Array2::<bool>::from_elem((IMAGE_SIDE, IMAGE_SIDE), false);
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            let image0 = image_terms[0][(x, y)];
            if image0 <= threshold {
                continue;
            }
            alpha_mask[(x, y)] = true;
            let image1 = image_terms[1][(x, y)];
            if image0 == 0.0 || image1 == 0.0 {
                continue;
            }
            let value = image1 / image0;
            alpha[(x, y)] = value;
            let term0 = principal_residual_terms[0][(x, y)] / image0;
            let term1 = principal_residual_terms[1][(x, y)] / image1;
            alpha_error[(x, y)] = value.abs() * (term0 * term0 + term1 * term1).sqrt();
        }
    }
    (alpha, alpha_error, alpha_mask, threshold)
}

fn sha256_f32(values: &Array2<f32>) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update(value.to_bits().to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_bool(values: &Array2<bool>) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update([u8::from(*value)]);
    }
    format!("{:x}", hasher.finalize())
}

fn nonzero_support(values: &Array2<f32>) -> Value {
    let mut hasher = Sha256::new();
    let mut count = 0_usize;
    let mut minimum = f32::INFINITY;
    let mut maximum = f32::NEG_INFINITY;
    for ((x, y), value) in values.indexed_iter() {
        if *value == 0.0 {
            continue;
        }
        count += 1;
        minimum = minimum.min(*value);
        maximum = maximum.max(*value);
        hasher.update(x.to_le_bytes());
        hasher.update(y.to_le_bytes());
        hasher.update(value.to_bits().to_le_bytes());
    }
    json!({
        "count": count,
        "sha256": format!("{:x}", hasher.finalize()),
        "minimum": (count != 0).then_some(minimum),
        "maximum": (count != 0).then_some(maximum),
    })
}

fn ordered_f32_bits(value: f32) -> i32 {
    let bits = value.to_bits() as i32;
    if bits < 0 { i32::MIN - bits } else { bits }
}

fn ulp_distance(left: f32, right: f32) -> u32 {
    ordered_f32_bits(left).abs_diff(ordered_f32_bits(right))
}

fn numeric_metrics(
    candidate: &Array2<f32>,
    reference: &Array2<f32>,
    valid: Option<&Array2<bool>>,
) -> Value {
    let mut count = 0_usize;
    let mut exact = 0_usize;
    let mut difference_sum_squares = 0.0_f64;
    let mut reference_sum_squares = 0.0_f64;
    let mut reference_peak = 0.0_f32;
    let mut max_absolute_difference = 0.0_f32;
    let mut max_ulp = 0_u32;
    let mut first_mismatch = None::<Value>;
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            if valid.is_some_and(|mask| !mask[(x, y)]) {
                continue;
            }
            let left = candidate[(x, y)];
            let right = reference[(x, y)];
            count += 1;
            if left.to_bits() == right.to_bits() {
                exact += 1;
            } else if first_mismatch.is_none() {
                first_mismatch = Some(json!({
                    "location": [x, y],
                    "candidate": left,
                    "reference": right,
                    "candidate_bits": left.to_bits(),
                    "reference_bits": right.to_bits(),
                    "ulp_distance": ulp_distance(left, right),
                }));
            }
            let difference = left - right;
            difference_sum_squares += f64::from(difference) * f64::from(difference);
            reference_sum_squares += f64::from(right) * f64::from(right);
            reference_peak = reference_peak.max(right.abs());
            max_absolute_difference = max_absolute_difference.max(difference.abs());
            max_ulp = max_ulp.max(ulp_distance(left, right));
        }
    }
    let difference_rms = (difference_sum_squares / count as f64).sqrt();
    let reference_rms = (reference_sum_squares / count as f64).sqrt();
    json!({
        "count": count,
        "bitwise_equal_count": exact,
        "bitwise_mismatch_count": count - exact,
        "candidate_sha256": sha256_f32(candidate),
        "reference_sha256": sha256_f32(reference),
        "difference_rms": difference_rms,
        "reference_rms": reference_rms,
        "difference_rms_over_reference_rms": difference_rms / reference_rms,
        "maximum_absolute_difference": max_absolute_difference,
        "maximum_absolute_difference_over_reference_peak":
            f64::from(max_absolute_difference) / f64::from(reference_peak),
        "maximum_ulp_distance": max_ulp,
        "first_mismatch": first_mismatch,
    })
}

fn topology_metrics(candidate: &Array2<bool>, reference: &Array2<bool>) -> Value {
    let mut mismatch_count = 0_usize;
    let mut first_mismatches = Vec::new();
    let mut candidate_valid = 0_usize;
    let mut reference_valid = 0_usize;
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            let left = candidate[(x, y)];
            let right = reference[(x, y)];
            candidate_valid += usize::from(left);
            reference_valid += usize::from(right);
            if left != right {
                mismatch_count += 1;
                if first_mismatches.len() < 16 {
                    first_mismatches.push(json!({
                        "location": [x, y],
                        "candidate": left,
                        "reference": right,
                    }));
                }
            }
        }
    }
    json!({
        "mismatch_count": mismatch_count,
        "candidate_valid_count": candidate_valid,
        "reference_valid_count": reference_valid,
        "candidate_sha256": sha256_bool(candidate),
        "reference_sha256": sha256_bool(reference),
        "first_mismatches": first_mismatches,
    })
}

fn main() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let casa_prefix = args.next().map(PathBuf::from).ok_or_else(|| {
        "usage: vlass_final_state_sandwich CASA_PREFIX OUTPUT_PREFIX RECEIPT_JSON".to_string()
    })?;
    let output_prefix = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing OUTPUT_PREFIX".to_string())?;
    let receipt_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing RECEIPT_JSON".to_string())?;
    if args.next().is_some() {
        return Err("unexpected trailing argument".to_string());
    }
    if receipt_path.exists() {
        return Err(format!(
            "refusing to overwrite receipt: {}",
            receipt_path.display()
        ));
    }

    let model = [
        plane(&casa_prefix, ".model.tt0")?,
        plane(&casa_prefix, ".model.tt1")?,
    ];
    let residual = [
        plane(&casa_prefix, ".residual.tt0")?,
        plane(&casa_prefix, ".residual.tt1")?,
    ];
    let reference_image = [
        plane(&casa_prefix, ".image.tt0")?,
        plane(&casa_prefix, ".image.tt1")?,
    ];
    let reference_alpha = plane(&casa_prefix, ".alpha")?;
    let reference_alpha_error = plane(&casa_prefix, ".alpha.error")?;
    let reference_alpha_mask = default_mask(&casa_prefix, ".alpha")?;
    let reference_alpha_error_mask = default_mask(&casa_prefix, ".alpha.error")?;

    let principal_residual = principal_terms(&residual);
    let cell_rad = CELL_ARCSEC.to_radians() / 3600.0;
    let beam = BeamFit {
        major_fwhm_rad: BEAM_MAJOR_ARCSEC.to_radians() / 3600.0,
        minor_fwhm_rad: BEAM_MINOR_ARCSEC.to_radians() / 3600.0,
        position_angle_rad: BEAM_POSITION_ANGLE_DEG.to_radians(),
    };
    let image = [
        &restore_standard_mfs_model(&model[0], [-cell_rad, cell_rad], Some(beam))
            + &principal_residual[0],
        &restore_standard_mfs_model(&model[1], [-cell_rad, cell_rad], Some(beam))
            + &principal_residual[1],
    ];
    let (alpha, alpha_error, alpha_mask, spectral_threshold) =
        alpha_products(&image, &principal_residual);

    clone_product_tree(&casa_prefix, &output_prefix)?;
    write_plane(&output_prefix, ".image.tt0", &image[0])?;
    write_plane(&output_prefix, ".image.tt1", &image[1])?;
    write_plane(&output_prefix, ".alpha", &alpha)?;
    write_plane(&output_prefix, ".alpha.error", &alpha_error)?;
    write_default_mask(&output_prefix, ".alpha", &alpha_mask)?;
    write_default_mask(&output_prefix, ".alpha.error", &alpha_mask)?;

    let alpha_topology = topology_metrics(&alpha_mask, &reference_alpha_mask);
    let alpha_error_topology = topology_metrics(&alpha_mask, &reference_alpha_error_mask);
    let receipt = json!({
        "schema": "casa-rs-vlass-frozen-final-state-sandwich-v1",
        "phase": "phase-a-product-only-closure",
        "classification": "phase-a-computed-awaiting-unchanged-contract-comparison",
        "casa_prefix": casa_prefix,
        "output_prefix": output_prefix,
        "frozen_identity": {
            "casa_oracle_manifest_sha256": CASA_ORACLE_MANIFEST_SHA256,
            "v6_native_component_receipt_sha256": V6_NATIVE_COMPONENT_RECEIPT_SHA256,
            "image_side": IMAGE_SIDE,
            "cell_arcsec": CELL_ARCSEC,
            "field": 1525,
            "spws": [2, 7, 12, 17],
            "nterms": 2,
            "scales": [0, 5, 12],
        },
        "execution_boundary": {
            "phase_a_entered": true,
            "visibility_operator_entered": false,
            "residual_refresh_entered": false,
            "response_surrogate_entered": false,
            "controller_entered": false,
            "beam_fit_entered": false,
            "major_cycles_entered": 0,
            "minor_iterations_entered": 0,
            "measurement_set_opened": false,
            "convolution_function_cache_opened": false,
        },
        "restoration": {
            "beam": {
                "major_arcsec": BEAM_MAJOR_ARCSEC,
                "minor_arcsec": BEAM_MINOR_ARCSEC,
                "position_angle_deg": BEAM_POSITION_ANGLE_DEG,
            },
            "principal_inverse": PRINCIPAL_INVERSE,
            "spectral_threshold": spectral_threshold,
            "spectral_threshold_bits": spectral_threshold.to_bits(),
        },
        "frozen_inputs": {
            "model_tt0_sha256": sha256_f32(&model[0]),
            "model_tt1_sha256": sha256_f32(&model[1]),
            "residual_tt0_sha256": sha256_f32(&residual[0]),
            "residual_tt1_sha256": sha256_f32(&residual[1]),
            "model_tt0_nonzero_support": nonzero_support(&model[0]),
            "model_tt1_nonzero_support": nonzero_support(&model[1]),
        },
        "products": {
            ".image.tt0": numeric_metrics(&image[0], &reference_image[0], None),
            ".image.tt1": numeric_metrics(&image[1], &reference_image[1], None),
            ".alpha": {
                "numeric": numeric_metrics(
                    &alpha,
                    &reference_alpha,
                    Some(&reference_alpha_mask),
                ),
                "topology": alpha_topology,
            },
            ".alpha.error": {
                "numeric": numeric_metrics(
                    &alpha_error,
                    &reference_alpha_error,
                    Some(&reference_alpha_error_mask),
                ),
                "topology": alpha_error_topology,
            },
        },
        "next_gate": "unchanged-19-product-comparison-contract",
    });
    if let Some(parent) = receipt_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create receipt parent {}: {error}", parent.display()))?;
    }
    fs::write(
        &receipt_path,
        serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("write {}: {error}", receipt_path.display()))?;
    println!(
        "{}",
        serde_json::to_string(&receipt).map_err(|error| error.to_string())?
    );
    Ok(())
}
