// SPDX-License-Identifier: LGPL-3.0-or-later
//! Offline final-model term causality cases for the frozen VLASS clean row.
//!
//! This diagnostic never opens a MeasurementSet, predicts visibilities, grids,
//! transforms, or deconvolves. It reuses the validated Phase-A restoration and
//! MT-MFS product arithmetic with frozen CASA residuals while substituting only
//! the final CASA and casa-rs model terms. A Python driver applies the existing
//! structured-difference implementation and conditionally requests the term
//! hybrids only when the complete casa-rs model fails.

#[allow(dead_code)]
#[path = "vlass_final_state_sandwich.rs"]
mod phase_a;

use std::env;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use casa_imaging::{BeamFit, restore_standard_mfs_model};
use ndarray::Array2;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const CANDIDATE_COMMIT: &str = "778a1ba4344823398e639421915a52a266892f6a";
const PHASE_A_RECEIPT_SHA256: &str =
    "ddd6b4e42c8d40987eae14854a3fddb5877a4907743b48ae72d9e48dff924c6c";
const PHASE_A_COMPARISON_SHA256: &str =
    "ba3ce70cbbf7c4fd6f387eb0d8f89d537d4bef761bf2abff8f2b8be877547191";
const CLEAN_LOG_SHA256: &str = "99fefee3f1fdd251fa651c70c165511517fc81a7849aa40f611fc2f2f7a4a0f3";
const CONTROL_TRACE_SHA256: &str =
    "979242b44469a8101da1f5dd9932614ca1a946fc2de1d4354c0956bc676b66ef";
const CLEAN_COMPARISON_SHA256: &str =
    "03f8a4d8027479559749202abdfd99fe2f1bbdae905dc3a0b56d2bce272b93ab";
const NUMERIC_RMS_LIMIT: f64 = 1.0e-3;
const NUMERIC_PEAK_LIMIT: f64 = 5.0e-3;
const STRUCTURED_AMPLITUDE_GOOD_LIMIT: f64 = 1.0e-4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Batch {
    Primary,
    TermHybrids,
}

impl Batch {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "primary" => Ok(Self::Primary),
            "term-hybrids" => Ok(Self::TermHybrids),
            _ => Err(format!(
                "unknown batch {value:?}; expected primary or term-hybrids"
            )),
        }
    }

    fn labels(self) -> &'static [&'static str] {
        match self {
            Self::Primary => &["control-a", "complete-rust-model"],
            Self::TermHybrids => &["tt0-rust-only", "tt1-rust-only"],
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::TermHybrids => "term-hybrids",
        }
    }
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", path.display()))
}

fn require_hash(path: &Path, expected: &str, label: &str) -> Result<(), String> {
    let actual = phase_a::sha256_file(path)?;
    if actual != expected {
        return Err(format!(
            "{label} hash differs for {}: {actual} != {expected}",
            path.display()
        ));
    }
    Ok(())
}

fn expected_mismatch_coordinates(receipt: &Value, suffix: &str) -> Result<Vec<[usize; 2]>, String> {
    let topology = &receipt["products"][suffix]["full_array"]["topology"];
    if topology["mask_mismatch_count"].as_u64() != Some(16) {
        return Err(format!(
            "{suffix} does not contain the frozen 16-pixel failure"
        ));
    }
    let samples = topology["mask_mismatch_samples"]
        .as_array()
        .ok_or_else(|| format!("{suffix} mismatch samples are missing"))?;
    if samples.len() != 16 {
        return Err(format!(
            "{suffix} retained {} mismatch samples, expected 16",
            samples.len()
        ));
    }
    let mut coordinates = Vec::with_capacity(samples.len());
    for sample in samples {
        let location = sample["location"]
            .as_array()
            .ok_or_else(|| format!("{suffix} mismatch location is missing"))?;
        if location.len() != 4 || location[2].as_u64() != Some(0) || location[3].as_u64() != Some(0)
        {
            return Err(format!(
                "{suffix} mismatch location is not on the frozen plane"
            ));
        }
        coordinates.push([
            location[0]
                .as_u64()
                .ok_or_else(|| format!("{suffix} mismatch x is missing"))? as usize,
            location[1]
                .as_u64()
                .ok_or_else(|| format!("{suffix} mismatch y is missing"))? as usize,
        ]);
    }
    coordinates.sort_unstable();
    coordinates.dedup();
    if coordinates.len() != 16 {
        return Err(format!("{suffix} mismatch coordinates are not unique"));
    }
    Ok(coordinates)
}

fn coordinate_hash(coordinates: &[[usize; 2]]) -> String {
    let mut ordered = coordinates.to_vec();
    ordered.sort_unstable();
    let mut hasher = Sha256::new();
    for [x, y] in ordered {
        hasher.update((x as u64).to_le_bytes());
        hasher.update((y as u64).to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

struct ValidatedInputs {
    phase_a_receipt: Value,
    phase_a_comparison: Value,
    clean_comparison: Value,
    failure_coordinates: Vec<[usize; 2]>,
}

fn validate_inputs(
    casa_prefix: &Path,
    rust_prefix: &Path,
    phase_a_receipt_path: &Path,
    phase_a_comparison_path: &Path,
    clean_log_path: &Path,
    control_trace_path: &Path,
    clean_comparison_path: &Path,
) -> Result<ValidatedInputs, String> {
    require_hash(
        phase_a_receipt_path,
        PHASE_A_RECEIPT_SHA256,
        "Phase-A receipt",
    )?;
    require_hash(
        phase_a_comparison_path,
        PHASE_A_COMPARISON_SHA256,
        "Phase-A comparison",
    )?;
    require_hash(clean_log_path, CLEAN_LOG_SHA256, "hybrid clean log")?;
    require_hash(
        control_trace_path,
        CONTROL_TRACE_SHA256,
        "hybrid clean control trace",
    )?;
    require_hash(
        clean_comparison_path,
        CLEAN_COMPARISON_SHA256,
        "hybrid clean comparison",
    )?;

    let phase_a_receipt = read_json(phase_a_receipt_path)?;
    let phase_a_comparison = read_json(phase_a_comparison_path)?;
    let control_trace = read_json(control_trace_path)?;
    let clean_comparison = read_json(clean_comparison_path)?;
    if phase_a_receipt["schema"] != "casa-rs-vlass-frozen-final-state-sandwich-v1"
        || phase_a_receipt["phase"] != "phase-a-product-only-closure"
        || phase_a_receipt["execution_boundary"]["measurement_set_opened"] != false
        || phase_a_receipt["execution_boundary"]["visibility_operator_entered"] != false
        || phase_a_receipt["execution_boundary"]["residual_refresh_entered"] != false
        || phase_a_receipt["execution_boundary"]["controller_entered"] != false
        || phase_a_receipt["execution_boundary"]["minor_iterations_entered"].as_u64() != Some(0)
    {
        return Err("Phase-A receipt does not preserve the product-only boundary".to_string());
    }
    if phase_a_comparison["status"] != "completed"
        || phase_a_comparison["product_inventory"]["observed_match"] != true
        || phase_a_comparison["product_inventory"]["expected"]
            .as_array()
            .is_none_or(|products| products.len() != 19)
        || phase_a_comparison["right_prefix"].as_str()
            != Some(casa_prefix.to_string_lossy().as_ref())
    {
        return Err(
            "Phase-A comparison does not preserve the exact 19-product contract".to_string(),
        );
    }
    if control_trace["minor"]["candidate_count"].as_u64() != Some(171)
        || control_trace["minor"]["reference_count"].as_u64() != Some(171)
        || control_trace["minor"]["discrete_parity"] != true
        || control_trace["refresh"]["candidate_count"].as_u64() != Some(170)
        || control_trace["refresh"]["discrete_parity"] != true
        || control_trace["final_refresh_iteration_parity"] != true
        || control_trace["candidate_final_refresh"]["reported_iterations"].as_u64() != Some(2_000)
    {
        return Err("hybrid clean control trajectory is not the frozen exact trace".to_string());
    }
    if clean_comparison["status"] != "comparison_failed"
        || clean_comparison["left_prefix"].as_str() != Some(rust_prefix.to_string_lossy().as_ref())
        || clean_comparison["right_prefix"].as_str() != Some(casa_prefix.to_string_lossy().as_ref())
        || clean_comparison["product_inventory"]["observed_match"] != true
        || clean_comparison["product_inventory"]["expected"]
            .as_array()
            .is_none_or(|products| products.len() != 19)
    {
        return Err("hybrid clean comparison is not the frozen 19-product failure".to_string());
    }
    let alpha_coordinates = expected_mismatch_coordinates(&clean_comparison, ".alpha")?;
    let alpha_error_coordinates = expected_mismatch_coordinates(&clean_comparison, ".alpha.error")?;
    if alpha_coordinates != alpha_error_coordinates {
        return Err("frozen alpha and alpha-error mismatch signatures differ".to_string());
    }
    Ok(ValidatedInputs {
        phase_a_receipt,
        phase_a_comparison,
        clean_comparison,
        failure_coordinates: alpha_coordinates,
    })
}

fn sparse_term(values: &Array2<f32>) -> Value {
    let mut coordinate_hasher = Sha256::new();
    let mut value_hasher = Sha256::new();
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
        for hasher in [&mut coordinate_hasher, &mut value_hasher] {
            hasher.update((x as u64).to_le_bytes());
            hasher.update((y as u64).to_le_bytes());
        }
        value_hasher.update(value.to_bits().to_le_bytes());
    }
    json!({
        "nonzero_count": count,
        "ordered_support_coordinate_sha256": format!("{:x}", coordinate_hasher.finalize()),
        "ordered_coordinate_value_sha256": format!("{:x}", value_hasher.finalize()),
        "raw_f32_sha256": phase_a::sha256_f32(values),
        "minimum": (count != 0).then_some(minimum),
        "maximum": (count != 0).then_some(maximum),
    })
}

fn sparse_cross(casa: &Array2<f32>, rust: &Array2<f32>) -> Value {
    assert_eq!(casa.dim(), rust.dim(), "model terms must have equal shapes");
    let mut support_mismatches = Vec::new();
    let mut support_mismatch_hasher = Sha256::new();
    let mut support_mismatch_count = 0_usize;
    let mut value_mismatch_count = 0_usize;
    let mut maximum_ulp_distance = 0_u32;
    let mut first_value_mismatch = None::<Value>;
    for x in 0..casa.nrows() {
        for y in 0..casa.ncols() {
            let casa_value = casa[(x, y)];
            let rust_value = rust[(x, y)];
            let casa_nonzero = casa_value != 0.0;
            let rust_nonzero = rust_value != 0.0;
            if casa_nonzero != rust_nonzero {
                support_mismatch_count += 1;
                support_mismatch_hasher.update((x as u64).to_le_bytes());
                support_mismatch_hasher.update((y as u64).to_le_bytes());
                if support_mismatches.len() < 16 {
                    support_mismatches.push(json!({
                        "location": [x, y],
                        "casa_nonzero": casa_nonzero,
                        "rust_nonzero": rust_nonzero,
                        "casa_bits": casa_value.to_bits(),
                        "rust_bits": rust_value.to_bits(),
                    }));
                }
            }
            if (casa_nonzero || rust_nonzero) && casa_value.to_bits() != rust_value.to_bits() {
                value_mismatch_count += 1;
                let ulp = phase_a::ulp_distance(casa_value, rust_value);
                maximum_ulp_distance = maximum_ulp_distance.max(ulp);
                if first_value_mismatch.is_none() {
                    first_value_mismatch = Some(json!({
                        "location": [x, y],
                        "casa": casa_value,
                        "rust": rust_value,
                        "casa_bits": casa_value.to_bits(),
                        "rust_bits": rust_value.to_bits(),
                        "ulp_distance": ulp,
                    }));
                }
            }
        }
    }
    json!({
        "support_mismatch_count": support_mismatch_count,
        "ordered_support_mismatch_sha256": format!(
            "{:x}",
            support_mismatch_hasher.finalize()
        ),
        "first_support_mismatches": support_mismatches,
        "value_mismatch_count_on_support_union": value_mismatch_count,
        "first_value_mismatch": first_value_mismatch,
        "maximum_ulp_distance": maximum_ulp_distance,
    })
}

fn topology_with_signature(candidate: &Array2<bool>, reference: &Array2<bool>) -> Value {
    assert_eq!(
        candidate.dim(),
        reference.dim(),
        "topology planes must have equal shapes"
    );
    let mut metrics = phase_a::topology_metrics(candidate, reference);
    let mut coordinates = Vec::new();
    for x in 0..candidate.nrows() {
        for y in 0..candidate.ncols() {
            if candidate[(x, y)] != reference[(x, y)] {
                coordinates.push([x, y]);
            }
        }
    }
    metrics["ordered_mismatch_coordinate_sha256"] = json!(coordinate_hash(&coordinates));
    metrics["all_mismatch_coordinates"] = json!(coordinates);
    metrics
}

fn numerical_gate(metrics: &Value) -> bool {
    metrics["difference_rms_over_reference_rms"]
        .as_f64()
        .is_some_and(|value| value <= NUMERIC_RMS_LIMIT)
        && metrics["maximum_absolute_difference_over_reference_peak"]
            .as_f64()
            .is_some_and(|value| value <= NUMERIC_PEAK_LIMIT)
}

fn non_spatial_structure_gate(metrics: &Value) -> bool {
    metrics["difference_rms_over_reference_rms"]
        .as_f64()
        .is_some_and(|value| value < STRUCTURED_AMPLITUDE_GOOD_LIMIT)
}

fn write_f32_plane(path: &Path, values: &Array2<f32>) -> Result<Value, String> {
    if path.exists() {
        return Err(format!(
            "refusing to overwrite raw plane: {}",
            path.display()
        ));
    }
    let file =
        fs::File::create(path).map_err(|error| format!("create {}: {error}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha256::new();
    for value in values {
        let bytes = value.to_bits().to_le_bytes();
        writer
            .write_all(&bytes)
            .map_err(|error| format!("write {}: {error}", path.display()))?;
        hasher.update(bytes);
    }
    writer
        .flush()
        .map_err(|error| format!("flush {}: {error}", path.display()))?;
    Ok(json!({
        "path": path,
        "dtype": "little-endian-f32",
        "shape": [phase_a::IMAGE_SIDE, phase_a::IMAGE_SIDE],
        "bytes": phase_a::IMAGE_SIDE * phase_a::IMAGE_SIDE * size_of::<f32>(),
        "sha256": format!("{:x}", hasher.finalize()),
    }))
}

struct References<'a> {
    residual: &'a [Array2<f32>; 2],
    image: &'a [Array2<f32>; 2],
    alpha: &'a Array2<f32>,
    alpha_error: &'a Array2<f32>,
    alpha_mask: &'a Array2<bool>,
    alpha_error_mask: &'a Array2<bool>,
    current_failure_coordinates: &'a [[usize; 2]],
}

fn evaluate_case(
    label: &str,
    model_sources: [&str; 2],
    model: [&Array2<f32>; 2],
    references: &References<'_>,
    scratch: &Path,
) -> Result<Value, String> {
    let principal_residual = phase_a::principal_terms(references.residual);
    let cell_rad = phase_a::CELL_ARCSEC.to_radians() / 3600.0;
    let beam = BeamFit {
        major_fwhm_rad: phase_a::BEAM_MAJOR_ARCSEC.to_radians() / 3600.0,
        minor_fwhm_rad: phase_a::BEAM_MINOR_ARCSEC.to_radians() / 3600.0,
        position_angle_rad: phase_a::BEAM_POSITION_ANGLE_DEG.to_radians(),
    };
    let image = [
        &restore_standard_mfs_model(model[0], [-cell_rad, cell_rad], Some(beam))
            + &principal_residual[0],
        &restore_standard_mfs_model(model[1], [-cell_rad, cell_rad], Some(beam))
            + &principal_residual[1],
    ];
    let (alpha, alpha_error, alpha_mask, threshold) =
        phase_a::alpha_products(&image, &principal_residual);
    let alpha_topology = topology_with_signature(&alpha_mask, references.alpha_mask);
    let alpha_error_topology = topology_with_signature(&alpha_mask, references.alpha_error_mask);
    let image_metrics = [
        phase_a::numeric_metrics(&image[0], &references.image[0], None),
        phase_a::numeric_metrics(&image[1], &references.image[1], None),
    ];
    let alpha_metrics =
        phase_a::numeric_metrics(&alpha, references.alpha, Some(references.alpha_mask));
    let alpha_error_metrics = phase_a::numeric_metrics(
        &alpha_error,
        references.alpha_error,
        Some(references.alpha_error_mask),
    );
    let mut margins = Vec::with_capacity(references.current_failure_coordinates.len());
    for [x, y] in references.current_failure_coordinates {
        let location = (*x, *y);
        margins.push(json!({
            "location": [x, y],
            "candidate_image_tt0": image[0][location],
            "candidate_image_tt0_bits": image[0][location].to_bits(),
            "reference_image_tt0": references.image[0][location],
            "reference_image_tt0_bits": references.image[0][location].to_bits(),
            "threshold": threshold,
            "threshold_bits": threshold.to_bits(),
            "candidate_margin": image[0][location] - threshold,
            "candidate_margin_bits": (image[0][location] - threshold).to_bits(),
            "candidate_valid": alpha_mask[location],
            "reference_alpha_valid": references.alpha_mask[location],
            "reference_alpha_error_valid": references.alpha_error_mask[location],
        }));
    }
    let raw = [
        write_f32_plane(&scratch.join(format!("{label}.image.tt0.f32")), &image[0])?,
        write_f32_plane(&scratch.join(format!("{label}.image.tt1.f32")), &image[1])?,
    ];
    let finite = image
        .iter()
        .chain([&alpha, &alpha_error])
        .all(|plane| plane.iter().all(|value| value.is_finite()));
    let numerical_pass = image_metrics.iter().all(numerical_gate)
        && numerical_gate(&alpha_metrics)
        && numerical_gate(&alpha_error_metrics);
    let topology_pass = alpha_topology["mismatch_count"].as_u64() == Some(0)
        && alpha_error_topology["mismatch_count"].as_u64() == Some(0);
    let non_spatial_structure_pass = non_spatial_structure_gate(&alpha_metrics)
        && non_spatial_structure_gate(&alpha_error_metrics);
    let expected_signature = coordinate_hash(references.current_failure_coordinates);
    let alpha_signature_exact = alpha_topology["mismatch_count"].as_u64()
        == Some(references.current_failure_coordinates.len() as u64)
        && alpha_topology["ordered_mismatch_coordinate_sha256"].as_str()
            == Some(expected_signature.as_str());
    let alpha_error_signature_exact = alpha_error_topology["mismatch_count"].as_u64()
        == Some(references.current_failure_coordinates.len() as u64)
        && alpha_error_topology["ordered_mismatch_coordinate_sha256"].as_str()
            == Some(expected_signature.as_str());
    Ok(json!({
        "label": label,
        "model_sources": {
            "tt0": model_sources[0],
            "tt1": model_sources[1],
        },
        "restoration": {
            "beam_major_arcsec": phase_a::BEAM_MAJOR_ARCSEC,
            "beam_minor_arcsec": phase_a::BEAM_MINOR_ARCSEC,
            "beam_position_angle_deg": phase_a::BEAM_POSITION_ANGLE_DEG,
            "principal_inverse": phase_a::PRINCIPAL_INVERSE,
            "spectral_threshold": threshold,
            "spectral_threshold_bits": threshold.to_bits(),
        },
        "products": {
            ".image.tt0": {
                "numeric": image_metrics[0].clone(),
                "raw": raw[0].clone(),
                "structured_difference": {"status": "pending-python-driver"},
            },
            ".image.tt1": {
                "numeric": image_metrics[1].clone(),
                "raw": raw[1].clone(),
                "structured_difference": {"status": "pending-python-driver"},
            },
            ".alpha": {
                "numeric": alpha_metrics.clone(),
                "topology": alpha_topology,
                "structured_difference": {
                    "status": "derived-non-spatial",
                    "overall": if non_spatial_structure_gate(&alpha_metrics) {
                        "good"
                    } else {
                        "not-good"
                    },
                },
            },
            ".alpha.error": {
                "numeric": alpha_error_metrics.clone(),
                "topology": alpha_error_topology,
                "structured_difference": {
                    "status": "derived-non-spatial",
                    "overall": if non_spatial_structure_gate(&alpha_error_metrics) {
                        "good"
                    } else {
                        "not-good"
                    },
                },
            },
        },
        "gates_before_image_structure": {
            "finite": finite,
            "numerical": numerical_pass,
            "topology": topology_pass,
            "non_spatial_structure": non_spatial_structure_pass,
            "pass": finite && numerical_pass && topology_pass && non_spatial_structure_pass,
        },
        "current_failure_signature": {
            "expected_count": references.current_failure_coordinates.len(),
            "expected_ordered_coordinate_sha256": expected_signature,
            "alpha_exact": alpha_signature_exact,
            "alpha_error_exact": alpha_error_signature_exact,
            "decision_margins": margins,
        },
    }))
}

fn main() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let batch = Batch::parse(
        &args
            .next()
            .ok_or_else(|| {
                "usage: vlass_model_term_causality BATCH CASA_PREFIX RUST_PREFIX \
                 PHASE_A_RECEIPT PHASE_A_COMPARISON CLEAN_LOG CONTROL_TRACE \
                 CLEAN_COMPARISON SCRATCH_DIR RECEIPT_JSON"
                    .to_string()
            })?
            .to_string_lossy(),
    )?;
    let casa_prefix = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing CASA_PREFIX".to_string())?;
    let rust_prefix = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing RUST_PREFIX".to_string())?;
    let phase_a_receipt_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_A_RECEIPT".to_string())?;
    let phase_a_comparison_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing PHASE_A_COMPARISON".to_string())?;
    let clean_log_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing CLEAN_LOG".to_string())?;
    let control_trace_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing CONTROL_TRACE".to_string())?;
    let clean_comparison_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing CLEAN_COMPARISON".to_string())?;
    let scratch = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing SCRATCH_DIR".to_string())?;
    let receipt_path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing RECEIPT_JSON".to_string())?;
    if args.next().is_some() {
        return Err("unexpected trailing argument".to_string());
    }
    if scratch.exists() || receipt_path.exists() {
        return Err("refusing to overwrite scratch directory or receipt".to_string());
    }
    fs::create_dir_all(&scratch)
        .map_err(|error| format!("create {}: {error}", scratch.display()))?;

    let validated = validate_inputs(
        &casa_prefix,
        &rust_prefix,
        &phase_a_receipt_path,
        &phase_a_comparison_path,
        &clean_log_path,
        &control_trace_path,
        &clean_comparison_path,
    )?;
    let casa_model = [
        phase_a::plane(&casa_prefix, ".model.tt0")?,
        phase_a::plane(&casa_prefix, ".model.tt1")?,
    ];
    let rust_model = [
        phase_a::plane(&rust_prefix, ".model.tt0")?,
        phase_a::plane(&rust_prefix, ".model.tt1")?,
    ];
    let residual = [
        phase_a::plane(&casa_prefix, ".residual.tt0")?,
        phase_a::plane(&casa_prefix, ".residual.tt1")?,
    ];
    let reference_image = [
        phase_a::plane(&casa_prefix, ".image.tt0")?,
        phase_a::plane(&casa_prefix, ".image.tt1")?,
    ];
    let reference_alpha = phase_a::plane(&casa_prefix, ".alpha")?;
    let reference_alpha_error = phase_a::plane(&casa_prefix, ".alpha.error")?;
    let reference_alpha_mask = phase_a::default_mask(&casa_prefix, ".alpha")?;
    let reference_alpha_error_mask = phase_a::default_mask(&casa_prefix, ".alpha.error")?;
    let references = References {
        residual: &residual,
        image: &reference_image,
        alpha: &reference_alpha,
        alpha_error: &reference_alpha_error,
        alpha_mask: &reference_alpha_mask,
        alpha_error_mask: &reference_alpha_error_mask,
        current_failure_coordinates: &validated.failure_coordinates,
    };
    let reference_raw = [
        write_f32_plane(
            &scratch.join("reference.image.tt0.f32"),
            &reference_image[0],
        )?,
        write_f32_plane(
            &scratch.join("reference.image.tt1.f32"),
            &reference_image[1],
        )?,
    ];
    let cases = match batch {
        Batch::Primary => vec![
            evaluate_case(
                "control-a",
                ["casa", "casa"],
                [&casa_model[0], &casa_model[1]],
                &references,
                &scratch,
            )?,
            evaluate_case(
                "complete-rust-model",
                ["casa-rs", "casa-rs"],
                [&rust_model[0], &rust_model[1]],
                &references,
                &scratch,
            )?,
        ],
        Batch::TermHybrids => vec![
            evaluate_case(
                "tt0-rust-only",
                ["casa-rs", "casa"],
                [&rust_model[0], &casa_model[1]],
                &references,
                &scratch,
            )?,
            evaluate_case(
                "tt1-rust-only",
                ["casa", "casa-rs"],
                [&casa_model[0], &rust_model[1]],
                &references,
                &scratch,
            )?,
        ],
    };
    let receipt = json!({
        "schema": "casa-rs-vlass-final-model-term-causality-case-batch-v1",
        "batch": batch.as_str(),
        "case_labels": batch.labels(),
        "candidate_commit": CANDIDATE_COMMIT,
        "frozen_identity": {
            "phase_a_receipt": phase_a_receipt_path,
            "phase_a_receipt_sha256": PHASE_A_RECEIPT_SHA256,
            "phase_a_comparison": phase_a_comparison_path,
            "phase_a_comparison_sha256": PHASE_A_COMPARISON_SHA256,
            "clean_log": clean_log_path,
            "clean_log_sha256": CLEAN_LOG_SHA256,
            "control_trace": control_trace_path,
            "control_trace_sha256": CONTROL_TRACE_SHA256,
            "clean_comparison": clean_comparison_path,
            "clean_comparison_sha256": CLEAN_COMPARISON_SHA256,
            "casa_prefix": casa_prefix,
            "rust_prefix": rust_prefix,
            "phase_a_contract": validated.phase_a_receipt,
            "phase_a_comparison_status": validated.phase_a_comparison["status"],
            "clean_comparison_request_sha256":
                validated.clean_comparison["request_sha256"],
        },
        "model_ledger": {
            "tt0": {
                "casa": sparse_term(&casa_model[0]),
                "rust": sparse_term(&rust_model[0]),
                "cross": sparse_cross(&casa_model[0], &rust_model[0]),
            },
            "tt1": {
                "casa": sparse_term(&casa_model[1]),
                "rust": sparse_term(&rust_model[1]),
                "cross": sparse_cross(&casa_model[1], &rust_model[1]),
            },
        },
        "reference_raw": {
            ".image.tt0": reference_raw[0].clone(),
            ".image.tt1": reference_raw[1].clone(),
        },
        "cases": cases,
        "execution_boundary": {
            "measurement_set_opened": false,
            "prediction_entered": false,
            "residual_refresh_entered": false,
            "grid_allocated": false,
            "fft_entered": false,
            "controller_entered": false,
            "minor_cycle_entered": false,
            "clean_entered": false,
            "beam_fit_entered": false,
            "response_cache_entered": false,
            "product_tree_written": false,
            "transient_raw_image_planes_written": 2 + 2 * batch.labels().len(),
        },
        "gates": {
            "numeric_rms_limit": NUMERIC_RMS_LIMIT,
            "numeric_peak_limit": NUMERIC_PEAK_LIMIT,
            "structured_allowed_labels": ["good"],
            "topology_mismatch_limit": 0,
        },
        "next_boundary": "python-structured-difference-and-conditional-classification",
    });
    if let Some(parent) = receipt_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn sparse_ledger_distinguishes_support_from_values() {
        let casa = array![[0.0_f32, 1.0], [2.0, 0.0]];
        let same_support = array![[0.0_f32, 1.0], [3.0, 0.0]];
        let changed_support = array![[4.0_f32, 1.0], [0.0, 0.0]];

        let value_cross = sparse_cross(&casa, &same_support);
        assert_eq!(value_cross["support_mismatch_count"], 0);
        assert_eq!(value_cross["value_mismatch_count_on_support_union"], 1);

        let support_cross = sparse_cross(&casa, &changed_support);
        assert_eq!(support_cross["support_mismatch_count"], 2);
        assert_eq!(support_cross["value_mismatch_count_on_support_union"], 2);
    }

    #[test]
    fn mismatch_hash_is_order_independent() {
        let first = coordinate_hash(&[[3, 7], [1, 9], [2, 4]]);
        let second = coordinate_hash(&[[2, 4], [3, 7], [1, 9]]);
        assert_eq!(first, second);
    }
}
