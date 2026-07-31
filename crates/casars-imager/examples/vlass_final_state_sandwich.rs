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
const HYBRID_OBSERVED_SHA256: &str =
    "3601b5c6ebf749d58c80bc16b329db68a94557e5d7cbb477034b061ef89f2172";
const HYBRID_CONTROL_PREDICTION_SHA256: &str =
    "68d6dc8c6b4ec45b8cad8d17ee44cdc1a1220e0ae261c251a35b75899ecb0bf9";
const HYBRID_CONTROL_RESIDUAL_SHA256: &str =
    "3ab0ed020a6b75ed54aadd91606c7d6e0fc8424575f77f931654e1addb3b6f98";
const HYBRID_CANDIDATE_PREDICTION_SHA256: &str =
    "2c6a3072a7f5556c81cc5b691a8d0ac2d7b055010bb8f171ed207d5d1a5d1e5d";
const HYBRID_CANDIDATE_RESIDUAL_SHA256: &str =
    "4db5487bff286e841718aec4a600f3b5c1ebf3aa602c5120a0796832355ad6d9";
const HYBRID_PREDICTION_TO_TILE_READY_LIMIT_MS: f64 = 62.690_729;
const HYBRID_RAW_BUFFER_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
const HYBRID_RELATIVE_RMS_LIMIT: f64 = 2.0e-7;
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

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn sha256_hybrid_plane(term: usize, values: &Array2<f32>) -> String {
    let mut hasher = Sha256::new();
    for word in [term as u64, values.nrows() as u64, values.ncols() as u64] {
        hasher.update(word.to_le_bytes());
    }
    for value in values {
        hasher.update(value.to_bits().to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn hybrid_residual_terms(receipt: &Value) -> Result<[Array2<f32>; 2], String> {
    if receipt["schema"] != "casa-rs-vlass-aw-hybrid-normalized-residual-v1"
        || receipt["residual_fft"]["precision"] != "f64"
        || receipt["stop_boundary"]["normalized_residual_tt0_tt1_complete"] != true
        || receipt["stop_boundary"]["product_tree_entered"] != false
        || receipt["stop_boundary"]["controller_entered"] != false
        || receipt["stop_boundary"]["minor_cycle_after_refresh"] != false
        || receipt["stop_boundary"]["additional_major_cycle"] != false
    {
        return Err(
            "hybrid normalized-residual receipt failed its execution-boundary contract".to_string(),
        );
    }
    let planes = receipt["planes"]
        .as_array()
        .ok_or_else(|| "hybrid normalized-residual receipt has no planes".to_string())?;
    if planes.len() != 2 {
        return Err(format!(
            "hybrid normalized-residual receipt has {} planes, expected 2",
            planes.len(),
        ));
    }
    let mut output = Vec::with_capacity(2);
    for (term, record) in planes.iter().enumerate() {
        if record["term"].as_u64() != Some(term as u64)
            || record["rows"].as_u64() != Some(IMAGE_SIDE as u64)
            || record["columns"].as_u64() != Some(IMAGE_SIDE as u64)
            || record["element_count"].as_u64() != Some((IMAGE_SIDE * IMAGE_SIDE) as u64)
        {
            return Err(format!(
                "hybrid normalized-residual term {term} topology differs",
            ));
        }
        let path = PathBuf::from(
            record["path"]
                .as_str()
                .ok_or_else(|| format!("hybrid term {term} path is missing"))?,
        );
        let bytes = fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
        if bytes.len() != IMAGE_SIDE * IMAGE_SIDE * std::mem::size_of::<f32>() {
            return Err(format!(
                "hybrid term {term} byte count differs: {}",
                bytes.len(),
            ));
        }
        let file_hash = format!("{:x}", Sha256::digest(&bytes));
        if record["file_sha256"].as_str() != Some(file_hash.as_str()) {
            return Err(format!("hybrid term {term} file hash differs: {file_hash}",));
        }
        let values = bytes
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|word| f32::from_bits(u32::from_le_bytes(word.try_into().expect("f32 word"))))
            .collect::<Vec<_>>();
        let plane = Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), values)
            .map_err(|error| format!("reshape hybrid term {term}: {error}"))?;
        let value_hash = sha256_hybrid_plane(term, &plane);
        if record["value_sha256"].as_str() != Some(value_hash.as_str()) {
            return Err(format!(
                "hybrid term {term} value hash differs: {value_hash}",
            ));
        }
        output.push(plane);
    }
    output
        .try_into()
        .map_err(|_| "hybrid residual term count changed".to_string())
}

fn validate_hybrid_prediction_receipt(receipt: &Value) -> Result<Value, String> {
    let hashes = &receipt["hashes"];
    let expected_hashes = [
        ("observed", HYBRID_OBSERVED_SHA256),
        ("control_prediction", HYBRID_CONTROL_PREDICTION_SHA256),
        ("control_residual", HYBRID_CONTROL_RESIDUAL_SHA256),
        ("candidate_prediction", HYBRID_CANDIDATE_PREDICTION_SHA256),
        ("candidate_residual", HYBRID_CANDIDATE_RESIDUAL_SHA256),
        ("tile_ingress_residual", HYBRID_CANDIDATE_RESIDUAL_SHA256),
    ];
    if receipt["schema"] != "casa-rs-vlass-aw-hybrid-residual-prediction-v1"
        || receipt["sample_count"].as_u64() != Some(98_239)
        || receipt["source_role_count"].as_u64() != Some(196_478)
        || receipt["complex_division_count"].as_u64() != Some(392_956)
        || receipt["execution"]["prediction_dispatch_count"].as_u64() != Some(1)
        || receipt["execution"]["residual_refresh_count"].as_u64() != Some(1)
        || receipt["execution"]["tile_grid_dispatch_count"].as_u64() != Some(1)
        || receipt["execution"]["raw_frame_taylor"] != true
        || receipt["execution"]["source_phase_once_after_taylor"] != true
        || receipt["execution"]["residual_fft_precision"] != "f64"
        || receipt["execution"]["product_formation_entered"] != false
        || receipt["execution"]["controller_entered"] != false
        || receipt["execution"]["minor_cycle_after_refresh"] != false
        || receipt["execution"]["additional_major_cycle"] != false
    {
        return Err(
            "hybrid prediction receipt failed its topology or execution contract".to_string(),
        );
    }
    for (name, expected) in expected_hashes {
        if hashes[name].as_str() != Some(expected) {
            return Err(format!(
                "hybrid prediction receipt {name} differs: {:?} != {expected}",
                hashes[name],
            ));
        }
    }
    let prediction_to_tile_ready_ms = receipt["timings_ms"]["prediction_to_tile_ready"]
        .as_f64()
        .ok_or_else(|| "hybrid prediction-to-tile-ready timing is missing".to_string())?;
    let raw_bytes = receipt["memory_bytes"]["raw_wide_division_logical"]
        .as_u64()
        .ok_or_else(|| "hybrid raw-buffer byte count is missing".to_string())?;
    Ok(json!({
        "prediction_to_tile_ready_ms": prediction_to_tile_ready_ms,
        "prediction_to_tile_ready_limit_ms": HYBRID_PREDICTION_TO_TILE_READY_LIMIT_MS,
        "prediction_to_tile_ready_pass": prediction_to_tile_ready_ms
            <= HYBRID_PREDICTION_TO_TILE_READY_LIMIT_MS,
        "raw_wide_division_bytes": raw_bytes,
        "raw_wide_division_limit_bytes": HYBRID_RAW_BUFFER_LIMIT_BYTES,
        "raw_wide_division_pass": raw_bytes <= HYBRID_RAW_BUFFER_LIMIT_BYTES,
        "eligible": prediction_to_tile_ready_ms <= HYBRID_PREDICTION_TO_TILE_READY_LIMIT_MS
            && raw_bytes <= HYBRID_RAW_BUFFER_LIMIT_BYTES,
    }))
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
    let first = args.next().ok_or_else(|| {
        "usage: vlass_final_state_sandwich CASA_PREFIX OUTPUT_PREFIX RECEIPT_JSON\n\
         or: vlass_final_state_sandwich --hybrid-residual CASA_PREFIX \
         PREDICTION_RECEIPT NORMALIZED_RECEIPT RECEIPT_JSON"
            .to_string()
    })?;
    let hybrid_mode = first.to_string_lossy() == "--hybrid-residual";
    let (
        casa_prefix,
        output_prefix,
        prediction_receipt_path,
        normalized_receipt_path,
        receipt_path,
    ) = if hybrid_mode {
        (
            args.next()
                .map(PathBuf::from)
                .ok_or_else(|| "missing CASA_PREFIX".to_string())?,
            None,
            Some(
                args.next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "missing PREDICTION_RECEIPT".to_string())?,
            ),
            Some(
                args.next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "missing NORMALIZED_RECEIPT".to_string())?,
            ),
            args.next()
                .map(PathBuf::from)
                .ok_or_else(|| "missing RECEIPT_JSON".to_string())?,
        )
    } else {
        (
            PathBuf::from(first),
            Some(
                args.next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "missing OUTPUT_PREFIX".to_string())?,
            ),
            None,
            None,
            args.next()
                .map(PathBuf::from)
                .ok_or_else(|| "missing RECEIPT_JSON".to_string())?,
        )
    };
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
    let reference_residual = [
        plane(&casa_prefix, ".residual.tt0")?,
        plane(&casa_prefix, ".residual.tt1")?,
    ];
    let (residual, prediction_receipt, normalized_receipt, performance_gate) =
        if let (Some(prediction_path), Some(normalized_path)) = (
            prediction_receipt_path.as_deref(),
            normalized_receipt_path.as_deref(),
        ) {
            let prediction: Value = serde_json::from_slice(
                &fs::read(prediction_path)
                    .map_err(|error| format!("read {}: {error}", prediction_path.display()))?,
            )
            .map_err(|error| format!("parse {}: {error}", prediction_path.display()))?;
            let normalized: Value = serde_json::from_slice(
                &fs::read(normalized_path)
                    .map_err(|error| format!("read {}: {error}", normalized_path.display()))?,
            )
            .map_err(|error| format!("parse {}: {error}", normalized_path.display()))?;
            let performance = validate_hybrid_prediction_receipt(&prediction)?;
            let residual = hybrid_residual_terms(&normalized)?;
            if residual.iter().flatten().any(|value| !value.is_finite()) {
                return Err("hybrid normalized residual contains non-finite values".to_string());
            }
            (residual, Some(prediction), Some(normalized), performance)
        } else {
            (
                reference_residual.clone(),
                None,
                None,
                json!({"eligible": true}),
            )
        };
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

    if let Some(output_prefix) = output_prefix.as_deref() {
        clone_product_tree(&casa_prefix, output_prefix)?;
        write_plane(output_prefix, ".image.tt0", &image[0])?;
        write_plane(output_prefix, ".image.tt1", &image[1])?;
        write_plane(output_prefix, ".alpha", &alpha)?;
        write_plane(output_prefix, ".alpha.error", &alpha_error)?;
        write_default_mask(output_prefix, ".alpha", &alpha_mask)?;
        write_default_mask(output_prefix, ".alpha.error", &alpha_mask)?;
    }

    let alpha_topology = topology_metrics(&alpha_mask, &reference_alpha_mask);
    let alpha_error_topology = topology_metrics(&alpha_mask, &reference_alpha_error_mask);
    let residual_metrics = [
        numeric_metrics(&residual[0], &reference_residual[0], None),
        numeric_metrics(&residual[1], &reference_residual[1], None),
    ];
    let image_metrics = [
        numeric_metrics(&image[0], &reference_image[0], None),
        numeric_metrics(&image[1], &reference_image[1], None),
    ];
    let residual_numeric_pass = residual_metrics.iter().all(|metrics| {
        metrics["difference_rms_over_reference_rms"]
            .as_f64()
            .is_some_and(|value| value <= HYBRID_RELATIVE_RMS_LIMIT)
    });
    let image_numeric_pass = image_metrics.iter().all(|metrics| {
        metrics["difference_rms_over_reference_rms"]
            .as_f64()
            .is_some_and(|value| value <= HYBRID_RELATIVE_RMS_LIMIT)
    });
    let product_topology_pass = alpha_topology["mismatch_count"].as_u64() == Some(0)
        && alpha_error_topology["mismatch_count"].as_u64() == Some(0);
    let performance_eligible = performance_gate["eligible"].as_bool().unwrap_or(false);
    let classification = if !hybrid_mode {
        "phase-a-computed-awaiting-unchanged-contract-comparison"
    } else if !residual_numeric_pass {
        "prediction-exact-downstream-residual-fails"
    } else if !image_numeric_pass || !product_topology_pass {
        "residual-passes-product-topology-fails"
    } else if !performance_eligible {
        "bounded-correctness-pass-performance-ineligible"
    } else {
        "bounded-hybrid-closure"
    };
    let alpha_cliff_p = (673, 2447);
    let alpha_cliff_q = (1341, 3274);
    let receipt = json!({
        "schema": if hybrid_mode {
            "casa-rs-vlass-aw-hybrid-residual-closure-v1"
        } else {
            "casa-rs-vlass-frozen-final-state-sandwich-v1"
        },
        "phase": if hybrid_mode {
            "hybrid-residual-read-only-product-closure"
        } else {
            "phase-a-product-only-closure"
        },
        "classification": classification,
        "casa_prefix": casa_prefix,
        "output_prefix": output_prefix,
        "hybrid_inputs": if hybrid_mode {
            json!({
                "prediction_receipt": prediction_receipt_path.as_deref(),
                "prediction_receipt_sha256": sha256_file(
                    prediction_receipt_path.as_deref().expect("hybrid prediction receipt")
                )?,
                "normalized_receipt": normalized_receipt_path.as_deref(),
                "normalized_receipt_sha256": sha256_file(
                    normalized_receipt_path.as_deref().expect("hybrid normalized receipt")
                )?,
                "prediction_contract": prediction_receipt,
                "normalized_contract": normalized_receipt,
            })
        } else {
            Value::Null
        },
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
            "visibility_operator_entered": hybrid_mode,
            "residual_refresh_entered": hybrid_mode,
            "response_surrogate_entered": false,
            "controller_entered": false,
            "beam_fit_entered": false,
            "major_cycles_entered": usize::from(hybrid_mode),
            "minor_iterations_entered": 0,
            "measurement_set_opened_by_closure": false,
            "convolution_function_cache_opened_by_closure": false,
            "diagnostic_product_tree_written": !hybrid_mode,
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
            ".residual.tt0": residual_metrics[0].clone(),
            ".residual.tt1": residual_metrics[1].clone(),
            ".image.tt0": image_metrics[0].clone(),
            ".image.tt1": image_metrics[1].clone(),
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
        "alpha_cliff": {
            "p": {
                "location": [alpha_cliff_p.0, alpha_cliff_p.1],
                "candidate_image_tt0": image[0][alpha_cliff_p],
                "candidate_image_tt0_bits": image[0][alpha_cliff_p].to_bits(),
                "casa_image_tt0": reference_image[0][alpha_cliff_p],
                "casa_image_tt0_bits": reference_image[0][alpha_cliff_p].to_bits(),
                "candidate_alpha_valid": alpha_mask[alpha_cliff_p],
                "casa_alpha_valid": reference_alpha_mask[alpha_cliff_p],
            },
            "q": {
                "location": [alpha_cliff_q.0, alpha_cliff_q.1],
                "candidate_principal_residual_tt0": principal_residual[0][alpha_cliff_q],
                "candidate_principal_residual_tt0_bits": principal_residual[0][alpha_cliff_q].to_bits(),
            },
        },
        "gates": {
            "relative_rms_limit": HYBRID_RELATIVE_RMS_LIMIT,
            "residual_numeric_pass": residual_numeric_pass,
            "image_numeric_pass": image_numeric_pass,
            "product_topology_pass": product_topology_pass,
            "performance": performance_gate,
        },
        "next_gate": if classification == "bounded-hybrid-closure" {
            "exactly-one-private-4096-four-spw-clean-candidate-authorized"
        } else if hybrid_mode {
            "no-clean-candidate-authorized"
        } else {
            "unchanged-19-product-comparison-contract"
        },
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
