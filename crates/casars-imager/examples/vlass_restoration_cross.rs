// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frozen-final-state restoration cross matrix for the VLASS 4096 full-band row.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use casa_imaging::{BeamFit, restore_standard_mfs_model};
use ndarray::Array2;
use serde_json::{Value, json};

const IMAGE_SIDE: usize = 4096;
const CELL_ARCSEC: f64 = 0.6;
const BEAM_MAJOR_ARCSEC: f64 = 3.108_743_429_183_96;
const BEAM_MINOR_ARCSEC: f64 = 2.114_210_367_202_759;
const BEAM_POSITION_ANGLE_DEG: f64 = 72.612_525_939_941_4;
const PRINCIPAL_INVERSE: [[f32; 2]; 2] = [[1.050_109_4, -1.224_721_3], [-1.224_721_3, 29.933_35]];
const SPECTRAL_THRESHOLD: f32 = 0.000_744_950_55;
const MISMATCH_PIXELS: [(usize, usize); 2] = [(2837, 3114), (309, 3290)];

fn plane(prefix: &Path, suffix: &str) -> Result<Array2<f32>, String> {
    let path = PathBuf::from(format!("{}{suffix}", prefix.display()));
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

fn alpha_mask(prefix: &Path) -> Result<Array2<bool>, String> {
    let path = PathBuf::from(format!("{}.alpha", prefix.display()));
    let image = PagedImage::<f32>::open(&path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    let shape = image.shape();
    let values = image
        .get_mask_slice(&[0, 0, 0, 0], shape, &[1, 1, 1, 1])
        .map_err(|error| format!("read alpha mask {}: {error}", path.display()))?
        .ok_or_else(|| format!("{} has no default pixel mask", path.display()))?;
    Array2::from_shape_vec((IMAGE_SIDE, IMAGE_SIDE), values.iter().copied().collect())
        .map_err(|error| format!("reshape alpha mask {}: {error}", path.display()))
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

fn metrics(candidate: &Array2<f32>, reference: &Array2<f32>) -> Value {
    let mut difference_sum_squares = 0.0_f64;
    let mut reference_sum_squares = 0.0_f64;
    let mut max_absolute_difference = 0.0_f32;
    let mut max_location = [0_usize; 2];
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            let reference_value = reference[(x, y)];
            let difference = candidate[(x, y)] - reference_value;
            difference_sum_squares += f64::from(difference) * f64::from(difference);
            reference_sum_squares += f64::from(reference_value) * f64::from(reference_value);
            if difference.abs() > max_absolute_difference {
                max_absolute_difference = difference.abs();
                max_location = [x, y];
            }
        }
    }
    json!({
        "relative_l2": (difference_sum_squares / reference_sum_squares).sqrt(),
        "difference_rms": (difference_sum_squares / (IMAGE_SIDE * IMAGE_SIDE) as f64).sqrt(),
        "max_absolute_difference": max_absolute_difference,
        "max_location": max_location,
    })
}

fn cross_case(
    model: &[Array2<f32>; 2],
    raw_residual: &[Array2<f32>; 2],
    reference_images: &[Array2<f32>; 2],
    reference_alpha_mask: &Array2<bool>,
) -> Value {
    let cell_rad = CELL_ARCSEC.to_radians() / 3600.0;
    let beam = BeamFit {
        major_fwhm_rad: BEAM_MAJOR_ARCSEC.to_radians() / 3600.0,
        minor_fwhm_rad: BEAM_MINOR_ARCSEC.to_radians() / 3600.0,
        position_angle_rad: BEAM_POSITION_ANGLE_DEG.to_radians(),
    };
    let principal = principal_terms(raw_residual);
    let mut images = [
        Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE)),
        Array2::<f32>::zeros((IMAGE_SIDE, IMAGE_SIDE)),
    ];
    for term in 0..2 {
        let restored = restore_standard_mfs_model(&model[term], [-cell_rad, cell_rad], Some(beam));
        images[term] = &restored + &principal[term];
    }
    let mut topology_mismatches = 0_usize;
    let mut mismatch_samples = Vec::new();
    for x in 0..IMAGE_SIDE {
        for y in 0..IMAGE_SIDE {
            let included = images[0][(x, y)] > SPECTRAL_THRESHOLD;
            if included != reference_alpha_mask[(x, y)] {
                topology_mismatches += 1;
                if mismatch_samples.len() < 16 {
                    mismatch_samples.push(json!({
                        "location": [x, y],
                        "candidate_value": images[0][(x, y)],
                        "reference_value": reference_images[0][(x, y)],
                        "candidate_included": included,
                        "reference_included": reference_alpha_mask[(x, y)],
                    }));
                }
            }
        }
    }
    let decisions = MISMATCH_PIXELS
        .iter()
        .map(|&(x, y)| {
            json!({
                "location": [x, y],
                "candidate_image_tt0": images[0][(x, y)],
                "reference_image_tt0": reference_images[0][(x, y)],
                "candidate_image_tt0_bits": images[0][(x, y)].to_bits(),
                "reference_image_tt0_bits": reference_images[0][(x, y)].to_bits(),
                "threshold_delta": images[0][(x, y)] - SPECTRAL_THRESHOLD,
                "included": images[0][(x, y)] > SPECTRAL_THRESHOLD,
                "reference_included": reference_alpha_mask[(x, y)],
            })
        })
        .collect::<Vec<_>>();
    json!({
        "image_tt0": metrics(&images[0], &reference_images[0]),
        "image_tt1": metrics(&images[1], &reference_images[1]),
        "alpha_topology_mismatches": topology_mismatches,
        "alpha_topology_mismatch_samples": mismatch_samples,
        "decisions": decisions,
    })
}

fn main() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let left_prefix = args.next().map(PathBuf::from).ok_or_else(|| {
        "usage: vlass_restoration_cross LEFT_PREFIX RIGHT_PREFIX OUTPUT_JSON".to_string()
    })?;
    let right_prefix = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing RIGHT_PREFIX".to_string())?;
    let output = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing OUTPUT_JSON".to_string())?;
    if args.next().is_some() {
        return Err("unexpected trailing argument".to_string());
    }

    let left_model = [
        plane(&left_prefix, ".model.tt0")?,
        plane(&left_prefix, ".model.tt1")?,
    ];
    let right_model = [
        plane(&right_prefix, ".model.tt0")?,
        plane(&right_prefix, ".model.tt1")?,
    ];
    let left_residual = [
        plane(&left_prefix, ".residual.tt0")?,
        plane(&left_prefix, ".residual.tt1")?,
    ];
    let right_residual = [
        plane(&right_prefix, ".residual.tt0")?,
        plane(&right_prefix, ".residual.tt1")?,
    ];
    let right_images = [
        plane(&right_prefix, ".image.tt0")?,
        plane(&right_prefix, ".image.tt1")?,
    ];
    let right_alpha_mask = alpha_mask(&right_prefix)?;

    let result = json!({
        "kind": "vlass_4096_full16_restoration_cross_matrix",
        "role": "bounded_frozen_final_state_correctness_diagnostic",
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "spectral_threshold": SPECTRAL_THRESHOLD,
        "spectral_threshold_bits": SPECTRAL_THRESHOLD.to_bits(),
        "principal_inverse": PRINCIPAL_INVERSE,
        "geometry": {
            "image_side": IMAGE_SIDE,
            "cell_arcsec": CELL_ARCSEC,
            "beam_major_arcsec": BEAM_MAJOR_ARCSEC,
            "beam_minor_arcsec": BEAM_MINOR_ARCSEC,
            "beam_position_angle_deg": BEAM_POSITION_ANGLE_DEG,
        },
        "cases": {
            "casa_model_casa_residual": cross_case(
                &right_model,
                &right_residual,
                &right_images,
                &right_alpha_mask,
            ),
            "casa_rs_model_casa_residual": cross_case(
                &left_model,
                &right_residual,
                &right_images,
                &right_alpha_mask,
            ),
            "casa_model_casa_rs_residual": cross_case(
                &right_model,
                &left_residual,
                &right_images,
                &right_alpha_mask,
            ),
            "casa_rs_model_casa_rs_residual": cross_case(
                &left_model,
                &left_residual,
                &right_images,
                &right_alpha_mask,
            ),
        },
    });
    fs::write(
        &output,
        serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("write {}: {error}", output.display()))?;
    println!(
        "{}",
        serde_json::to_string(&result).map_err(|error| error.to_string())?
    );
    Ok(())
}
