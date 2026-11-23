// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve CLPatchPanel data slices against the globally selected MS.
use super::*;
use crate::session::resolve_calibration_selection;
use std::collections::HashMap;
#[path = "plan_callibrary_keys.rs"]
mod keys;
pub(super) use keys::PatchGroup;
pub(super) type PlannedTable = (ApplyCalibrationTablePlan, Vec<PatchGroup>);

/// VI1 queries CLPatchPanel with wildcard scan and intent IDs. A library
/// registration contributes only the first matching result, even if several
/// physical slices expand to this visibility buffer.
pub(super) fn choose_legacy_slices(
    tables: &mut [PlannedTable],
    rows: &[ApplyRowPlan],
    registrations: &[crate::execute::LibraryApplyRegistration],
) {
    for registration in registrations
        .iter()
        .filter(|library| library.tables.len() > 1)
    {
        let slices = &mut tables[registration.tables.clone()];
        let mut assigned = vec![Vec::new(); slices.len()];
        let mut priorities = HashMap::new();
        for row in rows {
            let mut present = slices
                .iter()
                .enumerate()
                .filter(|(_, (table, _))| table.applies_to(row));
            let Some(first) = present.next() else {
                continue;
            };
            let winner = if let Some(second) = present.next() {
                std::iter::once(first)
                    .chain(std::iter::once(second))
                    .chain(present)
                    .filter_map(|(index, (_, groups))| {
                        let context = (
                            index,
                            row.calibration_observation_id,
                            row.field_id,
                            row.data_spw_id,
                        );
                        priorities
                            .entry(context)
                            .or_insert_with(|| legacy_priority(row, groups))
                            .map(|key| (index, key))
                    })
                    .min_by(|(_, a), (_, b)| compare_legacy_keys(a, b))
                    .map_or(first.0, |(index, _)| index)
            } else {
                first.0
            };
            assigned[winner].push(row.row_index);
        }
        for ((table, _), mut rows) in slices.iter_mut().zip(assigned) {
            rows.sort_unstable();
            table.applicable_selected_row_count = rows.len();
            table.applicable_row_indices = Some(rows);
        }
    }
}

type LegacyKey = [i32; 5];

fn legacy_priority(row: &ApplyRowPlan, groups: &[PatchGroup]) -> Option<LegacyKey> {
    let matching = |axis: &[i32], id| {
        if axis.contains(&id) {
            Some(id)
        } else if axis.contains(&-1) {
            Some(-1)
        } else {
            None
        }
    };
    groups
        .iter()
        .filter_map(|keys| {
            Some([
                matching(&keys[0], row.calibration_observation_id)?,
                *keys[1].iter().min()?,
                matching(&keys[2], row.field_id)?,
                *keys[3].iter().min()?,
                matching(&keys[4], row.data_spw_id)?,
            ])
        })
        .min_by(compare_legacy_keys)
}

fn compare_legacy_keys(left: &LegacyKey, right: &LegacyKey) -> std::cmp::Ordering {
    left.iter()
        .zip(right)
        .find_map(|(&left, &right)| {
            (left >= 0 && right >= 0 && left != right).then(|| left.cmp(&right))
        })
        .unwrap_or(std::cmp::Ordering::Equal)
}

pub(super) fn selected_rows(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    selected: &[ApplyRowPlan],
    legacy_corruption: bool,
) -> Result<Option<Vec<usize>>, ApplyPlanError> {
    let Some(library) = spec
        .callibrary
        .as_ref()
        .filter(|library| library.selection != MsSelection::new())
    else {
        return Ok(None);
    };
    if let Some(spw) = &library.selection.spw {
        ms.resolve_spw_selector(spw)
            .map_err(|source| ApplyPlanError::Selection { source })?;
    }
    let resolved = resolve_calibration_selection(ms, &library.selection).map_err(|error| {
        ApplyPlanError::Selection {
            source: MsError::InvalidInput(error.to_string()),
        }
    })?;
    let mut rows = resolved.row_indices().collect::<Vec<_>>();
    rows.sort_unstable();
    rows.dedup();
    if legacy_corruption && (library.selection.scan.is_some() || library.selection.intent.is_some())
    {
        // VI1 syncs observation, field, and SPW, while CLPatchPanel looks
        // up scan and intent with wildcard keys. Retain the contexts that
        // the slice registered and admit data from other scans and intents.
        return Ok(Some(legacy_slice_rows(library, selected, &rows)));
    }
    if library.selection.intent.is_some() || library.selection.observation.is_some() {
        // Calibrater::correct2 sorts ARRAY/SCAN/FIELD/DDID/TIME. VisCal::syncMeta2
        // uses the first row's STATE_ID and observation for the entire buffer.
        let indices = selected.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let arrays = load_i32_column_rows(ms.main_table(), "ARRAY_ID", &indices)?;
        let mut groups = HashMap::new();
        let mut grouped = Vec::new();
        for (index, row) in selected.iter().enumerate() {
            let key = (
                value_at_i32(&arrays, index, "ARRAY_ID")?,
                row.scan_number,
                row.field_id,
                row.data_desc_id,
                row.time_seconds.to_bits(),
            );
            let applies = *groups
                .entry(key)
                .or_insert_with(|| rows.binary_search(&row.row_index).is_ok());
            if applies {
                grouped.push(row.row_index);
            }
        }
        rows = grouped;
        rows.sort_unstable();
    }
    Ok(Some(rows))
}

fn legacy_slice_rows(
    library: &crate::CallibraryOptions,
    selected: &[ApplyRowPlan],
    matched_rows: &[usize],
) -> Vec<usize> {
    let context = |row: &ApplyRowPlan| {
        (
            library
                .selection
                .observation
                .as_ref()
                .map(|_| row.calibration_observation_id),
            library.selection.field.as_ref().map(|_| row.field_id),
            row.data_spw_id,
        )
    };
    let registered = selected
        .iter()
        .filter(|row| matched_rows.binary_search(&row.row_index).is_ok())
        .map(context)
        .collect::<BTreeSet<_>>();
    let mut rows = selected
        .iter()
        .filter(|row| registered.contains(&context(row)))
        .map(|row| row.row_index)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

/// CLPatchPanel distinguishes absent context axes from absent antennas: the
/// former prevent the buffer's correction, while an entirely unlinked antenna
/// group passes through this table without adding flags.
pub(super) fn arrange_contexts(
    ms: &MeasurementSet,
    plan: &mut ApplyCalibrationTablePlan,
    selected: &[ApplyRowPlan],
) -> Result<Vec<PatchGroup>, ApplyPlanError> {
    let Some(library) = &plan.spec.callibrary else {
        return Ok(Vec::new());
    };
    if plan.summary.table_subtype == "BPOLY" {
        return Ok(Vec::new());
    }
    let table = Table::open(TableOptions::new(&plan.spec.path)).map_err(|source| {
        ApplyPlanError::OpenSubtable {
            path: plan.spec.path.display().to_string(),
            subtable: "MAIN",
            source,
        }
    })?;
    let partition_field =
        !library.field_map.is_empty() || !plan.resolved_nearest_gainfields.is_empty();
    let mut contexts = HashMap::<_, BTreeSet<i32>>::new();
    for row in 0..table.row_count() {
        let field = get_i32(&table, row, "FIELD_ID")?;
        if !plan.resolved_gainfields.is_empty()
            && !plan.resolved_gainfields.iter().any(|f| f.field_id == field)
        {
            continue;
        }
        let observation = if library.observation_map.active() {
            get_i32(&table, row, "OBSERVATION_ID")?
        } else {
            -1
        };
        let scan = if library.scan_map.active() {
            get_i32(&table, row, "SCAN_NUMBER")?
        } else {
            -1
        };
        let key = (
            observation,
            scan,
            if partition_field { field } else { -1 },
            get_i32(&table, row, "SPECTRAL_WINDOW_ID")?,
        );
        contexts
            .entry(key)
            .or_default()
            .insert(get_i32(&table, row, "ANTENNA1")?);
    }
    let antennas = ms
        .antenna()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count();
    let requested_antennas = (0..antennas as i32)
        .map(|id| {
            library
                .antenna_map
                .get(id as usize)
                .or_else(|| library.antenna_map.last())
                .copied()
                .filter(|id| *id >= 0)
                .unwrap_or(id)
        })
        .collect::<BTreeSet<_>>();
    let groups = keys::groups(ms, plan, selected, &contexts)?;
    let mut active = Vec::new();
    let mut unavailable = Vec::new();
    for row in selected.iter().filter(|row| plan.applies_to(row)) {
        let field = library
            .field_map
            .get(row.field_id as usize)
            .copied()
            .unwrap_or_else(|| {
                plan.resolved_nearest_gainfields
                    .iter()
                    .find(|f| f.measurement_set_field_id == row.field_id)
                    .map_or(-1, |f| f.calibration_field_id)
            });
        let window = plan
            .spw_mapping
            .iter()
            .find(|m| m.data_spw_id == row.data_spw_id)
            .expect("mapped applicable row")
            .calibration_spw_id;
        let key = (
            library.observation_map.map(row.calibration_observation_id),
            library.scan_map.map(row.scan_number),
            field,
            window,
        );
        match contexts.get(&key) {
            None => {
                unavailable.push(row.row_index);
                active.push(row.row_index);
            }
            Some(ants) if !ants.is_disjoint(&requested_antennas) => active.push(row.row_index),
            Some(_) => {}
        }
    }
    active.sort_unstable();
    unavailable.sort_unstable();
    plan.unavailable_row_indices = unavailable;
    plan.applicable_selected_row_count = active.len();
    plan.applicable_row_indices = Some(active);
    Ok(groups)
}

/// Multiple slices of one table may cover disjoint data, but CLPatchPanel
/// refuses to assign the same calibration table to the same MS context twice.
pub(super) fn validate_overlaps(tables: &[PlannedTable]) -> Result<(), ApplyPlanError> {
    let mut deployed = HashMap::<String, Vec<&PatchGroup>>::new();
    for (table, groups) in tables {
        let Some(library) = &table.spec.callibrary else {
            continue;
        };
        let literal = if library.table_name.is_empty() {
            table.spec.path.display().to_string()
        } else {
            library.table_name.clone()
        };
        let previous = deployed.entry(literal).or_default();
        for group in groups {
            if previous.iter().any(|prior| keys::overlap(prior, group)) {
                return Err(ApplyPlanError::Callibrary { message: "Attempted duplicate MSCalPatchKey: overlapping wildcard contexts of one calibration table".into() });
            }
        }
        previous.extend(groups);
    }
    Ok(())
}

/// Keep physical IDs in the row plan, alongside the scalar metadata VisCal sees.
pub(super) fn buffer_observations(
    ms: &MeasurementSet,
    rows: &mut [ApplyRowPlan],
) -> Result<(), ApplyPlanError> {
    let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let arrays = load_i32_column_rows(ms.main_table(), "ARRAY_ID", &ids)?;
    let mut first = HashMap::new();
    for (index, row) in rows.iter_mut().enumerate() {
        let key = (
            value_at_i32(&arrays, index, "ARRAY_ID")?,
            row.scan_number,
            row.field_id,
            row.data_desc_id,
            row.time_seconds.to_bits(),
        );
        row.calibration_observation_id = *first.entry(key).or_insert(row.observation_id);
    }
    Ok(())
}
