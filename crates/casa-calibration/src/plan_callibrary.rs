// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve CLPatchPanel data slices against the globally selected MS.
use super::*;
use std::collections::HashMap;

pub(super) fn selected_rows(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    selected: &[ApplyRowPlan],
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

/// CLPatchPanel distinguishes absent context axes from absent antennas: the
/// former prevent the buffer's correction, while an entirely unlinked antenna
/// group passes through this table without adding flags.
pub(super) fn arrange_contexts(
    ms: &MeasurementSet,
    plan: &mut ApplyCalibrationTablePlan,
    selected: &[ApplyRowPlan],
) -> Result<(), ApplyPlanError> {
    let Some(library) = &plan.spec.callibrary else {
        return Ok(());
    };
    if plan.summary.table_subtype == "BPOLY" {
        return Ok(());
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
            library.observation_map.map(row.observation_id),
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
    Ok(())
}

/// Multiple slices of one table may cover disjoint data, but CLPatchPanel
/// refuses to assign the same calibration table to the same MS context twice.
pub(super) fn validate_overlaps(
    tables: &[ApplyCalibrationTablePlan],
    rows: &[ApplyRowPlan],
) -> Result<(), ApplyPlanError> {
    let mut seen = std::collections::HashSet::new();
    for table in tables
        .iter()
        .filter(|table| table.spec.callibrary.is_some())
    {
        for row in rows.iter().filter(|row| table.applies_to(row)) {
            if table
                .unavailable_row_indices
                .binary_search(&row.row_index)
                .is_err()
                && !seen.insert((&table.spec.path, row.row_index))
            {
                return Err(ApplyPlanError::Callibrary { message: "Attempted duplicate MSCalPatchKey: overlapping slices of one calibration table".into() });
            }
        }
    }
    Ok(())
}
