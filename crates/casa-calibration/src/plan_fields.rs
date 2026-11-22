// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select calibration fields or map the nearest direction on sky.

use super::*;

pub(crate) fn resolve_gainfield(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    selected_field_ids: &[i32],
) -> Result<(Vec<ResolvedGainField>, Vec<ResolvedNearestGainField>), ApplyPlanError> {
    let Some(selector) = spec.gainfield.as_ref() else {
        return Ok((Vec::new(), Vec::new()));
    };

    match selector {
        GainFieldSelector::FieldId(field_id) => {
            if !summary.field_ids.contains(field_id) {
                return Err(ApplyPlanError::ResolveGainField {
                    path: spec.path.display().to_string(),
                    selector: field_id.to_string(),
                    reason: "FIELD_ID is not referenced by any MAIN row".to_string(),
                });
            }
            Ok((
                vec![ResolvedGainField {
                    selector: selector.clone(),
                    field_id: *field_id,
                    field_name: field_name_by_id(summary, *field_id)?,
                }],
                Vec::new(),
            ))
        }
        GainFieldSelector::Expression(expression) => {
            resolve_gainfield_expression(spec, summary, expression)
                .map(|resolved| (resolved, Vec::new()))
        }
        GainFieldSelector::Nearest => {
            resolve_gainfield_nearest(ms, spec, summary, selected_field_ids)
                .map(|resolved| (Vec::new(), resolved))
        }
    }
}

fn resolve_gainfield_expression(
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    expression: &str,
) -> Result<Vec<ResolvedGainField>, ApplyPlanError> {
    let table = open_summary_subtable(summary, "FIELD")?;
    let field = casa_ms::MsField::new(&table);
    let failure = |source: MsError| ApplyPlanError::ResolveGainField {
        path: spec.path.display().to_string(),
        selector: expression.to_string(),
        reason: source.to_string(),
    };
    let rows = (0..table.row_count())
        .map(|index| {
            Ok((
                field.name(index)?,
                field.code(index)?,
                field.flag_row(index)?,
            ))
        })
        .collect::<Result<Vec<_>, MsError>>()
        .map_err(failure)?;
    let borrowed = rows
        .iter()
        .map(|(name, code, flagged)| casa_ms::FieldSelectorRow {
            name,
            code: Some(code),
            flagged: *flagged,
        })
        .collect::<Vec<_>>();
    let ids = casa_ms::resolve_field_selector_rows(&borrowed, expression).map_err(failure)?;
    let resolved = ids
        .into_iter()
        // Library field expressions resolve against FIELD metadata. A
        // mapped field with no solutions is handled by SPW availability.
        .filter(|id| spec.callibrary.is_some() || summary.field_ids.contains(id))
        .map(|field_id| ResolvedGainField {
            selector: GainFieldSelector::Expression(expression.to_string()),
            field_id,
            field_name: Some(rows[field_id as usize].0.clone()),
        })
        .collect::<Vec<_>>();
    if resolved.is_empty() {
        Err(ApplyPlanError::ResolveGainField {
            path: spec.path.display().to_string(),
            selector: expression.to_string(),
            reason: "selected fields have no calibration rows".to_string(),
        })
    } else {
        Ok(resolved)
    }
}

pub(super) fn selected_spectral_windows(
    summary: &CalibrationTableSummary,
    selected: &[ResolvedGainField],
) -> Result<Vec<i32>, ApplyPlanError> {
    if selected.is_empty() {
        return Ok(summary.spectral_window_ids.clone());
    }
    let table = Table::open(TableOptions::new(&summary.path)).map_err(|source| {
        ApplyPlanError::OpenSubtable {
            path: summary.path.display().to_string(),
            subtable: "MAIN",
            source,
        }
    })?;
    let mut windows = BTreeSet::new();
    for first in (0..table.row_count()).step_by(512) {
        let indices = (first..table.row_count().min(first + 512)).collect::<Vec<_>>();
        let fields = load_i32_column_rows(&table, "FIELD_ID", &indices)?;
        let spws = load_i32_column_rows(&table, "SPECTRAL_WINDOW_ID", &indices)?;
        for (field, window) in fields.into_iter().zip(spws) {
            if field.is_some_and(|field| selected.iter().any(|selected| selected.field_id == field))
                && let Some(window) = window
            {
                windows.insert(window);
            }
        }
    }
    Ok(windows.into_iter().collect())
}

#[derive(Debug, Clone)]
struct FieldDirectionInfo {
    field_id: i32,
    field_name: Option<String>,
    ra_rad: f64,
    dec_rad: f64,
}

fn resolve_gainfield_nearest(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    selected_field_ids: &[i32],
) -> Result<Vec<ResolvedNearestGainField>, ApplyPlanError> {
    let all_fields = (0..ms
        .field()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count())
        .map(|id| id as i32)
        .collect::<Vec<_>>();
    let ms_fields = load_ms_field_directions(ms, &all_fields)?;
    let cal_fields = load_caltable_field_directions(summary)?;
    if cal_fields.is_empty() {
        return Err(ApplyPlanError::ResolveGainField {
            path: spec.path.display().to_string(),
            selector: "nearest".to_string(),
            reason: "no usable FIELD directions were found in the calibration table".to_string(),
        });
    }

    let mut resolved = Vec::new();
    for ms_field in ms_fields {
        let mut best = None::<(&FieldDirectionInfo, f64)>;
        let mut tied = false;
        for cal_field in &cal_fields {
            let separation = angular_separation_rad(
                ms_field.ra_rad,
                ms_field.dec_rad,
                cal_field.ra_rad,
                cal_field.dec_rad,
            );
            match best {
                Some((_, best_separation)) if separation == best_separation => {
                    tied = true;
                }
                Some((_, best_separation)) if separation > best_separation => {}
                _ => {
                    best = Some((cal_field, separation));
                    tied = false;
                }
            }
        }
        let Some((cal_field, separation)) = best else {
            return Err(ApplyPlanError::ResolveGainField {
                path: spec.path.display().to_string(),
                selector: "nearest".to_string(),
                reason: format!(
                    "no caltable FIELD rows were available for MS field {}",
                    ms_field.field_id
                ),
            });
        };
        if tied {
            return Err(ApplyPlanError::ResolveGainField {
                path: spec.path.display().to_string(),
                selector: "nearest".to_string(),
                reason: "more than one calibration field is at the minimum angular distance"
                    .to_string(),
            });
        }
        if !selected_field_ids.contains(&ms_field.field_id) {
            continue;
        }
        resolved.push(ResolvedNearestGainField {
            measurement_set_field_id: ms_field.field_id,
            measurement_set_field_name: ms_field.field_name.clone(),
            calibration_field_id: cal_field.field_id,
            calibration_field_name: cal_field.field_name.clone(),
            angular_separation_rad: separation,
        });
    }
    Ok(resolved)
}

fn load_ms_field_directions(
    ms: &MeasurementSet,
    selected_field_ids: &[i32],
) -> Result<Vec<FieldDirectionInfo>, ApplyPlanError> {
    let field = ms
        .field()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let mut out = Vec::new();
    for &field_id in selected_field_ids {
        let row = usize::try_from(field_id).map_err(|_| ApplyPlanError::ResolveGainField {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            selector: "nearest".to_string(),
            reason: format!("MS FIELD_ID {field_id} does not fit in usize"),
        })?;
        if row >= field.row_count() {
            return Err(ApplyPlanError::ResolveGainField {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                selector: "nearest".to_string(),
                reason: format!("MS FIELD_ID {field_id} is outside the FIELD subtable"),
            });
        }
        let (ra_rad, dec_rad) = phase_direction_to_radec(
            field
                .phase_dir(row)
                .map_err(|source| ApplyPlanError::Selection { source })?,
        )
        .map_err(|reason| ApplyPlanError::ResolveGainField {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            selector: "nearest".to_string(),
            reason: format!("MS FIELD_ID {field_id}: {reason}"),
        })?;
        out.push(FieldDirectionInfo {
            field_id,
            field_name: field.name(row).ok(),
            ra_rad,
            dec_rad,
        });
    }
    Ok(out)
}

fn load_caltable_field_directions(
    summary: &CalibrationTableSummary,
) -> Result<Vec<FieldDirectionInfo>, ApplyPlanError> {
    let table = open_summary_subtable(summary, "FIELD")?;
    let mut out = Vec::new();
    for row_index in 0..table.row_count() {
        let field_id = row_index as i32;
        if !summary.field_ids.contains(&field_id) {
            continue;
        }
        let phase_dir = table
            .cell_accessor(row_index, "PHASE_DIR")
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyPlanError::OpenSubtable {
                path: summary.path.display().to_string(),
                subtable: "FIELD",
                source,
            })?;
        let (ra_rad, dec_rad) = phase_direction_to_radec(phase_dir).map_err(|reason| {
            ApplyPlanError::ResolveGainField {
                path: summary.path.display().to_string(),
                selector: "nearest".to_string(),
                reason: format!("caltable FIELD_ID {field_id}: {reason}"),
            }
        })?;
        out.push(FieldDirectionInfo {
            field_id,
            field_name: get_string(&table, row_index, "NAME").ok(),
            ra_rad,
            dec_rad,
        });
    }
    Ok(out)
}

fn phase_direction_to_radec(value: &ArrayValue) -> Result<(f64, f64), String> {
    let ArrayValue::Float64(values) = value else {
        return Err("PHASE_DIR was not a Float64 array".to_string());
    };
    if values.ndim() != 2 {
        return Err(format!("PHASE_DIR had shape {:?}", values.shape()));
    }
    let shape = values.shape();
    if shape[0] < 2 || shape[1] == 0 {
        return Err(format!("PHASE_DIR had unsupported shape {:?}", shape));
    }
    Ok((values[[0, 0]], values[[1, 0]]))
}

fn angular_separation_rad(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    let cos_sep = dec1.sin() * dec2.sin() + dec1.cos() * dec2.cos() * (ra1 - ra2).cos();
    cos_sep.clamp(-1.0, 1.0).acos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    #[test]
    fn direction_helpers_validate_shape_and_separation() {
        let phase_dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![1.25_f64, -0.5]).unwrap(),
        );
        assert_eq!(phase_direction_to_radec(&phase_dir).unwrap(), (1.25, -0.5));
        assert!(
            phase_direction_to_radec(&ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![1_i32, 2]).unwrap()
            ))
            .unwrap_err()
            .contains("Float64")
        );
        assert!(
            phase_direction_to_radec(&ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f64, 2.0]).unwrap()
            ))
            .unwrap_err()
            .contains("unsupported shape")
        );

        assert!(angular_separation_rad(0.0, 0.0, 0.0, 0.0) < 1.0e-12);
        let pi = angular_separation_rad(0.0, 0.0, std::f64::consts::PI, 0.0);
        assert!((pi - std::f64::consts::PI).abs() < 1.0e-12);
    }
}
