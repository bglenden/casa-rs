// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only causal certificate for the frozen VLASS alpha-mask cliff.
//!
//! This analyzer consumes already-written CASA and casa-rs products. It does
//! not open a MeasurementSet and cannot enter prediction, gridding, FFT,
//! restoration, deconvolution, or product-formation code.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use ndarray::Array2;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const IMAGE_SIDE: usize = 4096;
const MISMATCH_PIXEL: (usize, usize) = (673, 2447);
const PRINCIPAL_INVERSE: [[f32; 2]; 2] = [[1.008_975_6, -0.420_447_83], [-0.420_447_83, 19.695_16]];
const CASA_ORACLE_MANIFEST_SHA256: &str =
    "59f03bad4d43b79ee7c2d8ead4cb10a53d9b3fc76cf1a300e5251551c3db2c02";
const PHASE_A_RECEIPT_SHA256: &str =
    "ddd6b4e42c8d40987eae14854a3fddb5877a4907743b48ae72d9e48dff924c6c";
const PHASE_B_LOG_SHA256: &str = "8eb30ce404a4226cd530f1cb22544d20cc140b44ad20cc9d37dff69af31d4a77";
const PHASE_B_COMPARISON_SHA256: &str =
    "dfd8ce2834beecb9ea0329890b25b699b148c10fd1d0943a8f78540ce9c51953";
const MODEL_TT0_SHA256: &str = "8c8528281b075cb9b73aa848e44b3e3566aef6bb28b0c378b7e258d499075625";
const MODEL_TT1_SHA256: &str = "9ca50e60d9264bb5c8d2576bd212dacbda008697daaae3b5d5d18fe14c17b1d5";
const CASA_IMAGE_TT0_SHA256: &str =
    "e88e3343e0fd9ee0956bec8b2e53aa3d1472a583153ec609c5a1dedf69429dc6";
const CASA_RESIDUAL_TT0_SHA256: &str =
    "09517f85cec3310eba80587b308f4dab73a33fa21db6850f2996207d64db238a";
const CASA_RESIDUAL_TT1_SHA256: &str =
    "c3add19a1ce0819245cc159fbe7a25298f1f036b54ecdf4251840b846b0ad429";
const CASA_ALPHA_MASK_SHA256: &str =
    "4b790869264d8f5652c6e377a10b0f2189d43480ca11d94141ae968cf644726a";
const PHASE_B_IMAGE_TT0_SHA256: &str =
    "bc33c3fc1737a7097c265fc6103a564736f94e23ba358c00f380900c456473fd";
const PHASE_B_RESIDUAL_TT0_SHA256: &str =
    "f28303c7b269fa5f2ce1ed0a371f9fcc25d205d3e57b1fbbd6977fa0d7612684";
const PHASE_B_RESIDUAL_TT1_SHA256: &str =
    "70f65d2391f28bb36d999eae7fc9e4e701212e0ba7bc508940284ac507692ec8";
const PHASE_B_ALPHA_MASK_SHA256: &str =
    "29a8344e6864fd6db934bf49f929342307fd92737a42f1c6fa6eedca29eeaa8c";
const CASA_WEIGHT_TT0_SHA256: &str =
    "fff9ca9513bd9115e907cdb24e667638cf99210c90942a13b5290567b319a1a8";

#[derive(Debug)]
struct FrozenPlane {
    values: Array2<f32>,
    valid: Array2<bool>,
    shape: Vec<usize>,
    units: String,
    coordinates_sha256: String,
    image_info_sha256: String,
    values_sha256: String,
    valid_sha256: String,
    valid_count: usize,
    default_mask: Option<String>,
}

#[derive(Debug)]
struct ProducerState {
    image: FrozenPlane,
    residual0: FrozenPlane,
    residual1: FrozenPlane,
    alpha_mask: Array2<bool>,
    alpha_error_mask: Array2<bool>,
    principal_residual0: Array2<f32>,
    principal_valid: Array2<bool>,
    residual_max: f32,
    residual_max_position: (usize, usize),
    residual_max_tie_count: usize,
    threshold: f32,
    recomputed_alpha_mask: Array2<bool>,
}

fn product_path(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", prefix.display()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(|bytes| sha256_bytes(&bytes))
        .map_err(|error| format!("read {}: {error}", path.display()))
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

fn load_plane(prefix: &Path, suffix: &str) -> Result<FrozenPlane, String> {
    let path = product_path(prefix, suffix);
    let image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    let shape = image.shape().to_vec();
    if shape != [IMAGE_SIDE, IMAGE_SIDE, 1, 1] {
        return Err(format!(
            "{} has shape {shape:?}, expected [{IMAGE_SIDE}, {IMAGE_SIDE}, 1, 1]",
            path.display()
        ));
    }
    let raw = image
        .get_slice(&[0, 0, 0, 0], &shape)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let values = Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), raw.iter().copied().collect())
        .map_err(|error| format!("reshape {}: {error}", path.display()))?;
    let default_mask = image.default_mask_name();
    let valid = match image
        .get_mask_slice(&[0, 0, 0, 0], &shape, &[1, 1, 1, 1])
        .map_err(|error| format!("read mask {}: {error}", path.display()))?
    {
        Some(raw) => {
            Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), raw.iter().copied().collect())
                .map_err(|error| format!("reshape mask {}: {error}", path.display()))?
        }
        None => Array2::from_elem((IMAGE_SIDE, IMAGE_SIDE), true),
    };
    let coordinates_sha256 =
        sha256_bytes(format!("{:?}", image.coordinates().to_record()).as_bytes());
    let image_info = image
        .image_info()
        .map_err(|error| format!("read image info {}: {error}", path.display()))?;
    let image_info_sha256 = sha256_bytes(format!("{:?}", image_info.to_record()).as_bytes());
    let values_sha256 = sha256_f32(&values);
    let valid_sha256 = sha256_bool(&valid);
    let valid_count = valid.iter().filter(|&&value| value).count();
    Ok(FrozenPlane {
        values,
        valid,
        shape,
        units: image.units().to_string(),
        coordinates_sha256,
        image_info_sha256,
        values_sha256,
        valid_sha256,
        valid_count,
        default_mask,
    })
}

fn load_required_default_mask(prefix: &Path, suffix: &str) -> Result<Array2<bool>, String> {
    let plane = load_plane(prefix, suffix)?;
    if plane.default_mask.is_none() {
        return Err(format!(
            "{} has no default validity mask",
            product_path(prefix, suffix).display()
        ));
    }
    Ok(plane.valid)
}

fn principal_term_zero(residual0: &Array2<f32>, residual1: &Array2<f32>) -> Array2<f32> {
    let mut result = Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE));
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            result[(x, y)] = PRINCIPAL_INVERSE[0][0] * residual0[(x, y)]
                + PRINCIPAL_INVERSE[0][1] * residual1[(x, y)];
        }
    }
    result
}

fn positive_maximum(
    values: &Array2<f32>,
    valid: &Array2<bool>,
) -> Result<(f32, (usize, usize), usize), String> {
    let mut maximum = f32::NEG_INFINITY;
    let mut first_position = None;
    let mut ties = 0_usize;
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            if !valid[(x, y)] {
                continue;
            }
            let value = values[(x, y)];
            if !value.is_finite() {
                return Err(format!("nonfinite valid residual at ({x}, {y})"));
            }
            match value.total_cmp(&maximum) {
                std::cmp::Ordering::Greater => {
                    maximum = value;
                    first_position = Some((x, y));
                    ties = 1;
                }
                std::cmp::Ordering::Equal if value.to_bits() == maximum.to_bits() => {
                    ties += 1;
                }
                _ => {}
            }
        }
    }
    let position = first_position.ok_or_else(|| "no valid residual pixel".to_string())?;
    Ok((maximum, position, ties))
}

fn recompute_alpha_mask(
    image: &Array2<f32>,
    image_valid: &Array2<bool>,
    threshold: f32,
) -> Array2<bool> {
    let mut result = Array2::from_elem((IMAGE_SIDE, IMAGE_SIDE), false);
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            result[(x, y)] = image_valid[(x, y)] && image[(x, y)] > threshold;
        }
    }
    result
}

fn load_producer(prefix: &Path) -> Result<ProducerState, String> {
    let image = load_plane(prefix, ".image.tt0")?;
    let residual0 = load_plane(prefix, ".residual.tt0")?;
    let residual1 = load_plane(prefix, ".residual.tt1")?;
    let alpha_mask = load_required_default_mask(prefix, ".alpha")?;
    let alpha_error_mask = load_required_default_mask(prefix, ".alpha.error")?;
    if alpha_mask != alpha_error_mask {
        return Err(format!(
            "{} alpha and alpha.error default masks differ",
            prefix.display()
        ));
    }
    let principal_valid = &residual0.valid & &residual1.valid;
    let principal_residual0 = principal_term_zero(&residual0.values, &residual1.values);
    let (residual_max, residual_max_position, residual_max_tie_count) =
        positive_maximum(&principal_residual0, &principal_valid)?;
    let threshold = residual_max / 10.0_f32;
    if !threshold.is_finite() {
        return Err(format!("{} threshold is nonfinite", prefix.display()));
    }
    let recomputed_alpha_mask = recompute_alpha_mask(&image.values, &image.valid, threshold);
    if recomputed_alpha_mask != alpha_mask {
        return Err(format!(
            "{} exact principal-residual threshold arithmetic does not reproduce the stored alpha mask",
            prefix.display()
        ));
    }
    Ok(ProducerState {
        image,
        residual0,
        residual1,
        alpha_mask,
        alpha_error_mask,
        principal_residual0,
        principal_valid,
        residual_max,
        residual_max_position,
        residual_max_tie_count,
        threshold,
        recomputed_alpha_mask,
    })
}

fn mismatch_positions(left: &Array2<bool>, right: &Array2<bool>) -> Vec<(usize, usize)> {
    let mut result = Vec::new();
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            if left[(x, y)] != right[(x, y)] {
                result.push((x, y));
            }
        }
    }
    result
}

fn ordered_f32_bits(value: f32) -> i64 {
    let bits = value.to_bits() as i32;
    if bits < 0 {
        i64::from(i32::MIN - bits)
    } else {
        i64::from(bits)
    }
}

fn ulp_distance(left: f32, right: f32) -> u64 {
    ordered_f32_bits(left).abs_diff(ordered_f32_bits(right))
}

fn upward_steps_to_strictly_exceed(value: f32, threshold: f32) -> u64 {
    if value > threshold {
        0
    } else {
        ordered_f32_bits(threshold)
            .abs_diff(ordered_f32_bits(value))
            .saturating_add(1)
    }
}

fn downward_steps_to_make_strictly_below(threshold: f32, value: f32) -> u64 {
    if value > threshold {
        0
    } else {
        ordered_f32_bits(threshold)
            .abs_diff(ordered_f32_bits(value))
            .saturating_add(1)
    }
}

fn scalar(value: f32) -> Value {
    json!({
        "value": value,
        "bits": value.to_bits(),
    })
}

fn plane_identity(plane: &FrozenPlane) -> Value {
    json!({
        "shape": plane.shape,
        "axis_order": "x,y,stokes,spectral",
        "data_type": "f32",
        "units": plane.units,
        "coordinates_sha256": plane.coordinates_sha256,
        "image_info_sha256": plane.image_info_sha256,
        "values_sha256": plane.values_sha256,
        "default_mask_name": plane.default_mask,
        "valid_mask_sha256": plane.valid_sha256,
        "valid_count": plane.valid_count,
    })
}

fn require_hash(path: &Path, expected: &str, role: &str) -> Result<(), String> {
    let actual = sha256_file(path)?;
    if actual != expected {
        return Err(format!(
            "{role} hash mismatch for {}: expected {expected}, got {actual}",
            path.display()
        ));
    }
    Ok(())
}

fn require_plane_hash(actual: &str, expected: &str, role: &str) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "{role} frozen value hash mismatch: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn verify_phase_b_comparison(
    path: &Path,
    casa_prefix: &Path,
    phase_b_prefix: &Path,
) -> Result<Value, String> {
    let receipt: Value = serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", path.display()))?;
    if receipt["status"] != "comparison_failed"
        || receipt["reason"] != "product comparison failed for .alpha, .alpha.error"
        || receipt["request_binding"]["require_metadata_parity"] != true
        || receipt["request_binding"]["right_prefix"] != casa_prefix.to_string_lossy().as_ref()
        || receipt["request_binding"]["left_prefix"] != phase_b_prefix.to_string_lossy().as_ref()
    {
        return Err("Phase-B comparison receipt does not describe the frozen sandwich".to_string());
    }
    for suffix in [".image.tt0", ".residual.tt0", ".alpha", ".alpha.error"] {
        if receipt["products"][suffix]["metadata"]["parity"] != true {
            return Err(format!(
                "Phase-B comparison metadata parity failed for {suffix}"
            ));
        }
    }
    if receipt["products"][".alpha"]["topology_parity"] != false
        || receipt["products"][".alpha.error"]["topology_parity"] != false
    {
        return Err("Phase-B comparison no longer contains both topology failures".to_string());
    }
    Ok(json!({
        "status": receipt["status"],
        "reason": receipt["reason"],
        "metadata_parity_required": true,
        "metadata_parity_products": [".image.tt0", ".residual.tt0", ".alpha", ".alpha.error"],
    }))
}

fn receipt_principal_inverse_matches(receipt: &Value) -> bool {
    (0..2).all(|row| {
        (0..2).all(|column| {
            receipt["restoration"]["principal_inverse"][row][column]
                .as_f64()
                .is_some_and(|value| {
                    (value as f32).to_bits() == PRINCIPAL_INVERSE[row][column].to_bits()
                })
        })
    })
}

fn main() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let casa_prefix = args.next().map(PathBuf::from).ok_or_else(|| {
        "usage: vlass_alpha_cliff_causality CASA_PREFIX PHASE_B_PREFIX \
         PHASE_A_RECEIPT PHASE_B_LOG PHASE_B_COMPARISON OUTPUT_RECEIPT"
            .to_string()
    })?;
    let phase_b_prefix = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_B_PREFIX".to_string())?;
    let phase_a_receipt = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_A_RECEIPT".to_string())?;
    let phase_b_log = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_B_LOG".to_string())?;
    let phase_b_comparison = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_B_COMPARISON".to_string())?;
    let output_receipt = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing OUTPUT_RECEIPT".to_string())?;
    if args.next().is_some() {
        return Err("unexpected trailing argument".to_string());
    }
    if output_receipt.exists() {
        return Err(format!(
            "refusing to overwrite receipt: {}",
            output_receipt.display()
        ));
    }

    require_hash(&phase_a_receipt, PHASE_A_RECEIPT_SHA256, "Phase-A receipt")?;
    require_hash(&phase_b_log, PHASE_B_LOG_SHA256, "Phase-B log")?;
    require_hash(
        &phase_b_comparison,
        PHASE_B_COMPARISON_SHA256,
        "Phase-B comparison",
    )?;
    let phase_b_comparison_control =
        verify_phase_b_comparison(&phase_b_comparison, &casa_prefix, &phase_b_prefix)?;

    let phase_a: Value = serde_json::from_slice(
        &fs::read(&phase_a_receipt)
            .map_err(|error| format!("read {}: {error}", phase_a_receipt.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", phase_a_receipt.display()))?;
    if phase_a["frozen_identity"]["casa_oracle_manifest_sha256"] != CASA_ORACLE_MANIFEST_SHA256
        || phase_a["frozen_inputs"]["model_tt0_sha256"] != MODEL_TT0_SHA256
        || phase_a["frozen_inputs"]["model_tt1_sha256"] != MODEL_TT1_SHA256
        || !receipt_principal_inverse_matches(&phase_a)
    {
        return Err("Phase-A frozen identity or principal inverse changed".to_string());
    }

    let phase_b_text = fs::read_to_string(&phase_b_log)
        .map_err(|error| format!("read {}: {error}", phase_b_log.display()))?;
    for required in [
        "awproject_frozen_model_support positions=2166 source=imported-nonzero-union",
        "awproject_frozen_final_state_model_grid term=0 rows=4096 columns=4096 sha256=2cc338fcd624042ece5727245d51182f990f78fef85200b8fd7ca4011c745289",
        "awproject_frozen_final_state_model_grid term=1 rows=4096 columns=4096 sha256=2db70c3da68a8c17b04801302aee72e2cd00388a1efc47959b82bab4735e825d",
        "samples=98239 observed_sha256=3601b5c6ebf749d58c80bc16b329db68a94557e5d7cbb477034b061ef89f2172 predicted_sha256=68d6dc8c6b4ec45b8cad8d17ee44cdc1a1220e0ae261c251a35b75899ecb0bf9 residual_sha256=3ab0ed020a6b75ed54aadd91606c7d6e0fc8424575f77f931654e1addb3b6f98",
        "representation_sha256=e2207715b644881fb519bd434a766ce1adc960acff82eaf8d5fdde0323282f01 value_sha256=7dad10d7fdf07fe59d83ff22f34d9db5e2d5231e27d84ce74adae0f70f36256e",
        "representation_sha256=e07195cbbc657bbcea0379995e2fdd004e9fb3d1c8b8adc7266abdc2fff719de value_sha256=6cd13f9bfa9cbe8886184570243e4bf7f653ee53a2a5c18f840714042b617f3b",
        "awproject_frozen_restoring_beam source=frozen-casa major_arcsec=3.20294976234436035e0 minor_arcsec=2.15760445594787598e0 position_angle_deg=7.05534973144531250e1 beam_fit_entered=false",
    ] {
        if !phase_b_text.contains(required) {
            return Err(format!(
                "Phase-B log is missing required provenance: {required}"
            ));
        }
    }

    let casa = load_producer(&casa_prefix)?;
    let phase_b = load_producer(&phase_b_prefix)?;
    for (actual, expected, role) in [
        (
            casa.image.values_sha256.as_str(),
            CASA_IMAGE_TT0_SHA256,
            "CASA image.tt0",
        ),
        (
            casa.residual0.values_sha256.as_str(),
            CASA_RESIDUAL_TT0_SHA256,
            "CASA residual.tt0",
        ),
        (
            casa.residual1.values_sha256.as_str(),
            CASA_RESIDUAL_TT1_SHA256,
            "CASA residual.tt1",
        ),
        (
            phase_b.image.values_sha256.as_str(),
            PHASE_B_IMAGE_TT0_SHA256,
            "Phase-B image.tt0",
        ),
        (
            phase_b.residual0.values_sha256.as_str(),
            PHASE_B_RESIDUAL_TT0_SHA256,
            "Phase-B residual.tt0",
        ),
        (
            phase_b.residual1.values_sha256.as_str(),
            PHASE_B_RESIDUAL_TT1_SHA256,
            "Phase-B residual.tt1",
        ),
    ] {
        require_plane_hash(actual, expected, role)?;
    }
    require_plane_hash(
        &sha256_bool(&casa.alpha_mask),
        CASA_ALPHA_MASK_SHA256,
        "CASA alpha mask",
    )?;
    require_plane_hash(
        &sha256_bool(&phase_b.alpha_mask),
        PHASE_B_ALPHA_MASK_SHA256,
        "Phase-B alpha mask",
    )?;
    let alpha_mismatches = mismatch_positions(&phase_b.alpha_mask, &casa.alpha_mask);
    let alpha_error_mismatches =
        mismatch_positions(&phase_b.alpha_error_mask, &casa.alpha_error_mask);
    if alpha_mismatches != [MISMATCH_PIXEL] || alpha_error_mismatches != [MISMATCH_PIXEL] {
        return Err(format!(
            "frozen topology mismatch sets changed: alpha={alpha_mismatches:?}, \
             alpha.error={alpha_error_mismatches:?}"
        ));
    }

    let (x, y) = MISMATCH_PIXEL;
    if !(casa.image.valid[(x, y)]
        && casa.principal_valid[(x, y)]
        && phase_b.image.valid[(x, y)]
        && phase_b.principal_valid[(x, y)])
    {
        return Err("mismatch pixel is invalid in a required input domain".to_string());
    }
    let casa_image = casa.image.values[(x, y)];
    let phase_b_image = phase_b.image.values[(x, y)];
    let casa_principal_residual = casa.principal_residual0[(x, y)];
    let phase_b_principal_residual = phase_b.principal_residual0[(x, y)];
    let cc = casa_image > casa.threshold;
    let bb = phase_b_image > phase_b.threshold;
    let bc = phase_b_image > casa.threshold;
    let cb = casa_image > phase_b.threshold;
    if !cc || bb {
        return Err(format!(
            "stored decision controls changed: CASA={cc}, Phase-B={bb}"
        ));
    }
    let classification = match (bc, cb) {
        (false, true) => "local-value-only",
        (true, false) => "threshold-only",
        (false, false) => "both-independently-sufficient",
        (true, true) => "joint-cliff",
    };

    let weight = load_plane(&casa_prefix, ".weight.tt0")?;
    require_plane_hash(
        &weight.values_sha256,
        CASA_WEIGHT_TT0_SHA256,
        "CASA weight.tt0",
    )?;
    let executable =
        env::current_exe().map_err(|error| format!("resolve analyzer executable: {error}"))?;
    let executable_sha256 = sha256_file(&executable)?;
    let receipt = json!({
        "schema": "casa-rs-vlass-frozen-alpha-cliff-causality-v1",
        "role": "read-only-frozen-product-causal-certificate",
        "analyzer": {
            "executable": executable,
            "executable_sha256": executable_sha256,
        },
        "execution_boundary": {
            "formed_grid": false,
            "entered_fft": false,
            "formed_image": false,
            "formed_product": false,
            "entered_prediction": false,
            "entered_clean": false,
            "opened_measurement_set": false,
            "modified_frozen_artifact": false,
        },
        "frozen_identity": {
            "frozen_casa_manifest_sha256": CASA_ORACLE_MANIFEST_SHA256,
            "phase_a_receipt_sha256": PHASE_A_RECEIPT_SHA256,
            "phase_b_log_sha256": PHASE_B_LOG_SHA256,
            "phase_b_comparison_sha256": PHASE_B_COMPARISON_SHA256,
            "model_tt0_sha256": MODEL_TT0_SHA256,
            "model_tt1_sha256": MODEL_TT1_SHA256,
            "principal_inverse": PRINCIPAL_INVERSE,
            "principal_inverse_f32_bits": [
                [PRINCIPAL_INVERSE[0][0].to_bits(), PRINCIPAL_INVERSE[0][1].to_bits()],
                [PRINCIPAL_INVERSE[1][0].to_bits(), PRINCIPAL_INVERSE[1][1].to_bits()],
            ],
            "weight_tt0": plane_identity(&weight),
            "restoring_beam": {
                "major_arcsec": 3.202_949_762_344_360_4_f64,
                "minor_arcsec": 2.157_604_455_947_876_f64,
                "position_angle_deg": 70.553_497_314_453_12_f64,
            },
            "phase_b_comparison_control": phase_b_comparison_control,
        },
        "phase_b_operator_hashes": {
            "model_grid_tt0": "2cc338fcd624042ece5727245d51182f990f78fef85200b8fd7ca4011c745289",
            "model_grid_tt1": "2db70c3da68a8c17b04801302aee72e2cd00388a1efc47959b82bab4735e825d",
            "observed_visibilities": "3601b5c6ebf749d58c80bc16b329db68a94557e5d7cbb477034b061ef89f2172",
            "predicted_visibilities": "68d6dc8c6b4ec45b8cad8d17ee44cdc1a1220e0ae261c251a35b75899ecb0bf9",
            "residual_visibilities": "3ab0ed020a6b75ed54aadd91606c7d6e0fc8424575f77f931654e1addb3b6f98",
            "prefft_grid_tt0_representation": "e2207715b644881fb519bd434a766ce1adc960acff82eaf8d5fdde0323282f01",
            "prefft_grid_tt0_values": "7dad10d7fdf07fe59d83ff22f34d9db5e2d5231e27d84ce74adae0f70f36256e",
            "prefft_grid_tt1_representation": "e07195cbbc657bbcea0379995e2fdd004e9fb3d1c8b8adc7266abdc2fff719de",
            "prefft_grid_tt1_values": "6cd13f9bfa9cbe8886184570243e4bf7f653ee53a2a5c18f840714042b617f3b",
            "used_as_cross_producer_comparisons": false,
        },
        "context_only_artifacts": [{
            "role": "different-full-16-spw-model-prediction-traces",
            "used_for_classification": false,
        }],
        "mismatch_pixel": [x, y],
        "topology_control": {
            "alpha_mismatches": alpha_mismatches,
            "alpha_error_mismatches": alpha_error_mismatches,
            "casa_alpha_mask_sha256": sha256_bool(&casa.alpha_mask),
            "phase_b_alpha_mask_sha256": sha256_bool(&phase_b.alpha_mask),
            "casa_recomputed_mask_sha256": sha256_bool(&casa.recomputed_alpha_mask),
            "phase_b_recomputed_mask_sha256": sha256_bool(&phase_b.recomputed_alpha_mask),
            "casa_full_mask_reproduced": casa.recomputed_alpha_mask == casa.alpha_mask,
            "phase_b_full_mask_reproduced": phase_b.recomputed_alpha_mask == phase_b.alpha_mask,
        },
        "inputs": {
            "casa": {
                "image_tt0": plane_identity(&casa.image),
                "residual_tt0": plane_identity(&casa.residual0),
                "residual_tt1": plane_identity(&casa.residual1),
            },
            "phase_b": {
                "image_tt0": plane_identity(&phase_b.image),
                "residual_tt0": plane_identity(&phase_b.residual0),
                "residual_tt1": plane_identity(&phase_b.residual1),
            },
        },
        "casa": {
            "image_value": scalar(casa_image),
            "raw_residual_tt0_value": scalar(casa.residual0.values[(x, y)]),
            "raw_residual_tt1_value": scalar(casa.residual1.values[(x, y)]),
            "principal_residual_tt0_value": scalar(casa_principal_residual),
            "principal_residual_max": scalar(casa.residual_max),
            "principal_residual_max_position": [
                casa.residual_max_position.0,
                casa.residual_max_position.1,
            ],
            "principal_residual_max_tie_count": casa.residual_max_tie_count,
            "threshold": scalar(casa.threshold),
            "stored_decision": cc,
            "recomputed_decision": cc,
            "full_mask_reproduced": true,
        },
        "phase_b": {
            "image_value": scalar(phase_b_image),
            "raw_residual_tt0_value": scalar(phase_b.residual0.values[(x, y)]),
            "raw_residual_tt1_value": scalar(phase_b.residual1.values[(x, y)]),
            "principal_residual_tt0_value": scalar(phase_b_principal_residual),
            "principal_residual_max": scalar(phase_b.residual_max),
            "principal_residual_max_position": [
                phase_b.residual_max_position.0,
                phase_b.residual_max_position.1,
            ],
            "principal_residual_max_tie_count": phase_b.residual_max_tie_count,
            "threshold": scalar(phase_b.threshold),
            "stored_decision": bb,
            "recomputed_decision": bb,
            "full_mask_reproduced": true,
        },
        "cross_locations": {
            "at_casa_max": {
                "position": [casa.residual_max_position.0, casa.residual_max_position.1],
                "casa_principal_residual": scalar(casa.principal_residual0[casa.residual_max_position]),
                "phase_b_principal_residual": scalar(phase_b.principal_residual0[casa.residual_max_position]),
            },
            "at_phase_b_max": {
                "position": [phase_b.residual_max_position.0, phase_b.residual_max_position.1],
                "casa_principal_residual": scalar(casa.principal_residual0[phase_b.residual_max_position]),
                "phase_b_principal_residual": scalar(phase_b.principal_residual0[phase_b.residual_max_position]),
            },
        },
        "counterfactuals": {
            "casa_image_casa_threshold": cc,
            "phase_b_image_phase_b_threshold": bb,
            "phase_b_image_casa_threshold": bc,
            "casa_image_phase_b_threshold": cb,
        },
        "margins": {
            "casa": f64::from(casa_image) - f64::from(casa.threshold),
            "phase_b": f64::from(phase_b_image) - f64::from(phase_b.threshold),
            "local_image_shift": f64::from(phase_b_image) - f64::from(casa_image),
            "principal_residual_shift_at_mismatch_pixel":
                f64::from(phase_b_principal_residual) - f64::from(casa_principal_residual),
            "raw_residual_tt0_shift_at_mismatch_pixel":
                f64::from(phase_b.residual0.values[(x, y)])
                    - f64::from(casa.residual0.values[(x, y)]),
            "threshold_shift": f64::from(phase_b.threshold) - f64::from(casa.threshold),
        },
        "ulp_distances": {
            "image_value": ulp_distance(casa_image, phase_b_image),
            "principal_residual_at_mismatch_pixel":
                ulp_distance(casa_principal_residual, phase_b_principal_residual),
            "principal_residual_max": ulp_distance(casa.residual_max, phase_b.residual_max),
            "threshold": ulp_distance(casa.threshold, phase_b.threshold),
        },
        "diagnostic_only_f32_step_counts": {
            "phase_b_image_upward_steps_to_exceed_casa_threshold":
                upward_steps_to_strictly_exceed(phase_b_image, casa.threshold),
            "casa_threshold_downward_steps_to_fall_below_phase_b_image":
                downward_steps_to_make_strictly_below(casa.threshold, phase_b_image),
            "casa_image_upward_steps_to_exceed_phase_b_threshold":
                upward_steps_to_strictly_exceed(casa_image, phase_b.threshold),
            "phase_b_threshold_downward_steps_to_fall_below_casa_image":
                downward_steps_to_make_strictly_below(phase_b.threshold, casa_image),
            "phase_b_image_upward_steps_to_exceed_phase_b_threshold":
                upward_steps_to_strictly_exceed(phase_b_image, phase_b.threshold),
        },
        "classification": classification,
        "scope_limit": "identifies the causal scalar target but does not distinguish prediction, gridding, FFT, or normalization",
    });

    if let Some(parent) = output_receipt.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create receipt parent {}: {error}", parent.display()))?;
    }
    fs::write(
        &output_receipt,
        serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("write {}: {error}", output_receipt.display()))?;
    println!(
        "{}",
        serde_json::to_string(&receipt).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        downward_steps_to_make_strictly_below, ulp_distance, upward_steps_to_strictly_exceed,
    };

    #[test]
    fn strict_step_counts_include_the_equality_cliff() {
        let value = 1.0_f32;
        assert_eq!(upward_steps_to_strictly_exceed(value, value), 1);
        assert_eq!(downward_steps_to_make_strictly_below(value, value), 1);
        assert_eq!(
            upward_steps_to_strictly_exceed(value, f32::from_bits(value.to_bits() + 1)),
            2
        );
        assert_eq!(
            downward_steps_to_make_strictly_below(f32::from_bits(value.to_bits() + 1), value),
            2
        );
    }

    #[test]
    fn ulp_distance_tracks_adjacent_positive_values() {
        let value = 1.0_f32;
        assert_eq!(ulp_distance(value, f32::from_bits(value.to_bits() + 1)), 1);
    }
}
