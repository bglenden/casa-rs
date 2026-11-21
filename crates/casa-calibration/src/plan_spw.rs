// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traditional and CLPatchPanel spectral-window map rules.

use super::*;

pub(super) fn resolve_calibration_spw_mapping(
    spec: &ApplyCalibrationTableSpec,
    subtype: &str,
    available: &[i32],
    calibration_window_count: usize,
    selected: &[i32],
) -> Result<Vec<ApplySpwMapping>, ApplyPlanError> {
    let effective_spec;
    let spec_for_spw_mapping = match subtype {
        "KAntPos Jones" => {
            effective_spec = k_antpos_spwmap_spec(spec, selected);
            &effective_spec
        }
        "F Jones" if spec.callibrary.is_none() => {
            effective_spec = ApplyCalibrationTableSpec {
                spwmap: vec![0],
                ..spec.clone()
            };
            &effective_spec
        }
        _ => spec,
    };
    resolve_spw_mapping(
        spec_for_spw_mapping,
        available,
        calibration_window_count,
        selected,
    )
}

fn k_antpos_spwmap_spec(
    spec: &ApplyCalibrationTableSpec,
    applicable_data_spw_ids: &[i32],
) -> ApplyCalibrationTableSpec {
    let mut spec = spec.clone();
    let len = applicable_data_spw_ids
        .iter()
        .filter_map(|spw| usize::try_from(*spw).ok())
        .max()
        .map(|spw| spw + 1)
        .unwrap_or_default();
    spec.spwmap = vec![0; len];
    spec
}

pub(crate) fn resolve_spw_mapping(
    spec: &ApplyCalibrationTableSpec,
    available: &[i32],
    calibration_window_count: usize,
    selected: &[i32],
) -> Result<Vec<ApplySpwMapping>, ApplyPlanError> {
    if spec.callibrary.is_some() {
        return Ok(selected
            .iter()
            .map(|&data_spw_id| {
                let calibration_spw_id = spec
                    .spwmap
                    .get(data_spw_id as usize)
                    .or_else(|| spec.spwmap.last())
                    .copied()
                    .filter(|id| *id >= 0)
                    .unwrap_or(data_spw_id);
                ApplySpwMapping {
                    data_spw_id,
                    calibration_spw_id,
                }
            })
            .collect());
    }
    let automatic = spec.spwmap.first() == Some(&-999);
    let identity = spec.spwmap.is_empty() || spec.spwmap == [-1];
    if !automatic
        && !identity
        && let Some(&invalid) = spec
            .spwmap
            .iter()
            .find(|&&window| window < 0 || window as usize >= calibration_window_count)
    {
        return Err(ApplyPlanError::MissingCalibrationSpectralWindow {
            path: spec.path.display().to_string(),
            calibration_spw_id: invalid,
        });
    }
    let first = if automatic {
        Some(
            *available
                .first()
                .ok_or_else(|| ApplyPlanError::UnsupportedCalibrationTable {
                    path: spec.path.display().to_string(),
                    reason: "no calibration window is available for automatic fanout".to_string(),
                })?,
        )
    } else {
        None
    };
    Ok(selected
        .iter()
        .map(|&data_spw_id| {
            let calibration_spw_id = if let Some(first) = first {
                first
            } else if identity {
                data_spw_id
            } else {
                spec.spwmap
                    .get(if spec.spwmap.len() == 1 {
                        0
                    } else {
                        data_spw_id as usize
                    })
                    .copied()
                    .unwrap_or(data_spw_id)
            };
            ApplySpwMapping {
                data_spw_id,
                calibration_spw_id,
            }
        })
        .collect())
}
