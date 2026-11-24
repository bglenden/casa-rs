// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve CLPatchPanel data slices against the globally selected MS.
use super::*;
use crate::session::resolve_calibration_selection;
use std::collections::HashMap;
#[path = "plan_callibrary_keys.rs"]
mod keys;
pub(super) use keys::PatchGroup;
pub(super) type PlannedTable = (ApplyCalibrationTablePlan, Vec<PatchGroup>);

/// The physical MS scan to use for a registered slice's calibration lookup.
pub(crate) type LegacyScanOverrides = HashMap<(usize, usize), i32>;

/// VI1 queries CLPatchPanel with wildcard scan and intent IDs. A library
/// contributes one registered context for each visibility buffer.
pub(super) fn choose_legacy_slices(
    tables: &mut [PlannedTable],
    rows: &[ApplyRowPlan],
    registrations: &[crate::execute::LibraryApplyRegistration],
) -> LegacyScanOverrides {
    let mut overrides = LegacyScanOverrides::new();
    for registration in registrations
        .iter()
        .filter(|library| library.tables.len() > 1)
    {
        let slices = &mut tables[registration.tables.clone()];
        let mut assigned = vec![Vec::new(); slices.len()];
        let mut unavailable = vec![Vec::new(); slices.len()];
        let mut priorities = HashMap::new();
        for row in rows {
            let context = (
                row.calibration_observation_id,
                row.observation_id,
                row.field_id,
                row.data_spw_id,
            );
            let preferred = *priorities
                .entry(context)
                .or_insert_with(|| legacy_priority(row, slices));
            if let Some((index, [scan, _])) = preferred {
                assigned[index].push(row.row_index);
                if scan >= 0
                    && scan != row.scan_number
                    && slices[index]
                        .0
                        .spec
                        .callibrary
                        .as_ref()
                        .is_some_and(|library| library.scan_map.active())
                {
                    overrides.insert((registration.tables.start + index, row.row_index), scan);
                }
            } else if let Some((index, (fallback, _))) = slices
                .iter()
                .enumerate()
                .find(|(_, (table, _))| table.applies_to(row))
            {
                assigned[index].push(row.row_index);
                if fallback
                    .unavailable_row_indices
                    .binary_search(&row.row_index)
                    .is_ok()
                {
                    unavailable[index].push(row.row_index);
                }
            }
        }
        for (((table, _), mut rows), mut missing) in
            slices.iter_mut().zip(assigned).zip(unavailable)
        {
            rows.sort_unstable();
            missing.sort_unstable();
            table.applicable_selected_row_count = rows.len();
            table.applicable_row_indices = Some(rows);
            table.unavailable_row_indices = missing;
        }
    }
    overrides
}

// Observation, field and SPW already match the buffer. Only the scan and
// intent contribute to ordering the candidate keys.
type LegacyPriority = (usize, [i32; 2]);

fn legacy_priority(row: &ApplyRowPlan, slices: &[PlannedTable]) -> Option<LegacyPriority> {
    let matches = |values: &[i32], id| values.contains(&id) || values.contains(&-1);
    let mut candidates = Vec::new();
    for (index, (table, groups)) in slices.iter().enumerate() {
        if !table.spec.apply_to.matches(row) {
            continue;
        }
        let before = candidates.len();
        candidates.extend(groups.iter().filter_map(|keys| {
            if matches(&keys[0], row.calibration_observation_id)
                && matches(&keys[2], row.field_id)
                && matches(&keys[4], row.data_spw_id)
            {
                Some((index, [*keys[1].iter().min()?, *keys[3].iter().min()?]))
            } else {
                None
            }
        }));
        candidates[before..].sort_unstable();
    }
    let has_wildcard_scan = candidates.iter().any(|(_, key)| key[0] < 0);
    let has_concrete_scan = candidates.iter().any(|(_, key)| key[0] >= 0);
    let has_intent = candidates.iter().any(|(_, key)| key[1] >= 0);
    if has_wildcard_scan && has_concrete_scan && has_intent {
        first_registered_key(&candidates)
    } else {
        candidates
            .into_iter()
            .min_by(|(_, left), (_, right)| compare_legacy_keys(left, right))
    }
}

fn first_registered_key(candidates: &[LegacyPriority]) -> Option<LegacyPriority> {
    // A wildcard in a stored key can make the comparator non-transitive.
    // Respect registration order when constructing a single buffer lookup.
    // A CASA index with cyclic keys can also update more than one result.
    let mut children = vec![[None; 2]; candidates.len()];
    for (index, (_, key)) in candidates.iter().enumerate().skip(1) {
        let mut node = 0;
        loop {
            let right = !compare_legacy_keys(key, &candidates[node].1).is_lt();
            let next = &mut children[node][usize::from(right)];
            match *next {
                Some(child) => node = child,
                None => {
                    *next = Some(index);
                    break;
                }
            }
        }
    }
    let mut first = 0;
    while let Some(child) = children[first][0] {
        first = child;
    }
    candidates.get(first).copied()
}

fn compare_legacy_keys(left: &[i32; 2], right: &[i32; 2]) -> std::cmp::Ordering {
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
    rows: &mut [ApplyRowPlan],
    array_id: impl Fn(usize) -> Result<i32, ApplyPlanError>,
) -> Result<(), ApplyPlanError> {
    let mut first = HashMap::new();
    for (index, row) in rows.iter_mut().enumerate() {
        let key = (
            array_id(index)?,
            row.scan_number,
            row.field_id,
            row.data_desc_id,
            row.time_seconds.to_bits(),
        );
        row.calibration_observation_id = *first.entry(key).or_insert(row.observation_id);
    }
    Ok(())
}
