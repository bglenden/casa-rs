// SPDX-License-Identifier: LGPL-3.0-or-later
//! CLPatchPanel arranges Cartesian context axes; negative IDs are wildcards.
use super::*;
use crate::callib::CallibraryContextMap;
use crate::session::resolve_calibration_selection;

pub(crate) type PatchGroup = [Vec<i32>; 6];

pub(super) fn groups(
    ms: &MeasurementSet,
    plan: &ApplyCalibrationTablePlan,
    selected: &[ApplyRowPlan],
    contexts: &HashMap<(i32, i32, i32, i32), BTreeSet<i32>>,
) -> Result<Vec<PatchGroup>, ApplyPlanError> {
    let library = plan.spec.callibrary.as_ref().expect("library");
    let raw = resolve_calibration_selection(ms, &library.selection).map_err(|error| {
        ApplyPlanError::Callibrary {
            message: error.to_string(),
        }
    })?;
    let mut indices = raw.row_indices().collect::<Vec<_>>();
    indices.sort_unstable();
    let rows = selected
        .iter()
        .filter(|row| indices.binary_search(&row.row_index).is_ok())
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let requested = [
        library
            .selection
            .observation
            .as_ref()
            .map(|_| unique_i32(rows.iter().map(|row| row.observation_id))),
        library
            .selection
            .scan
            .as_ref()
            .map(|_| unique_i32(rows.iter().map(|row| row.scan_number))),
        library
            .selection
            .field
            .as_ref()
            .map(|_| unique_i32(rows.iter().map(|row| row.field_id))),
        if library.selection.intent.is_some() {
            Some(unique_i32(
                rows.iter()
                    .map(|row| get_i32(ms.main_table(), row.row_index, "STATE_ID"))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        } else {
            None
        },
    ];
    let observation_count = ms
        .observation()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count() as i32;
    let field_count = ms
        .field()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count() as i32;
    let antenna_count = ms
        .antenna()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count() as i32;
    let scan_count = selected
        .iter()
        .map(|row| row.scan_number)
        .max()
        .unwrap_or(-1)
        + 1;
    let windows = unique_i32(rows.iter().map(|row| row.data_spw_id));
    let mappings = resolve_spw_mapping(&plan.spec, &[], 0, &windows)?;
    let mut groups = Vec::new();
    for (&(observation, scan, field, window), antennas) in contexts {
        let fields = if !library.field_map.is_empty() {
            restrict(
                library
                    .field_map
                    .iter()
                    .enumerate()
                    .filter(|(_, target)| **target == field)
                    .map(|(id, _)| id as i32)
                    .collect(),
                requested[2].as_deref(),
            )
        } else if !plan.resolved_nearest_gainfields.is_empty() {
            restrict(
                plan.resolved_nearest_gainfields
                    .iter()
                    .filter(|mapping| mapping.calibration_field_id == field)
                    .map(|mapping| mapping.measurement_set_field_id)
                    .collect(),
                requested[2].as_deref(),
            )
        } else if plan.resolved_gainfields.len() == 1 && plan.spec.gainfield.is_some() {
            restrict((0..field_count).collect(), requested[2].as_deref())
        } else {
            requested[2].clone().unwrap_or_else(|| vec![-1])
        };
        let group = [
            reverse(
                &library.observation_map,
                observation,
                observation_count,
                requested[0].as_deref(),
            ),
            reverse(&library.scan_map, scan, scan_count, requested[1].as_deref()),
            fields,
            requested[3].clone().unwrap_or_else(|| vec![-1]),
            mappings
                .iter()
                .filter(|mapping| mapping.calibration_spw_id == window)
                .map(|mapping| mapping.data_spw_id)
                .collect(),
            (0..antenna_count)
                .filter(|&id| {
                    let target = library
                        .antenna_map
                        .get(id as usize)
                        .or_else(|| library.antenna_map.last())
                        .copied()
                        .filter(|id| *id >= 0)
                        .unwrap_or(id);
                    antennas.contains(&target)
                })
                .collect(),
        ];
        if group.iter().all(|axis| !axis.is_empty()) {
            groups.push(group);
        }
    }
    Ok(groups)
}

fn reverse(
    map: &CallibraryContextMap,
    target: i32,
    count: i32,
    requested: Option<&[i32]>,
) -> Vec<i32> {
    match map {
        CallibraryContextMap::All => requested.map_or_else(|| vec![-1], <[i32]>::to_vec),
        CallibraryContextMap::SelfMap => restrict(
            if (0..count).contains(&target) {
                vec![target]
            } else {
                vec![]
            },
            requested,
        ),
        CallibraryContextMap::Indices(ids) => restrict(
            ids.iter()
                .enumerate()
                .filter(|(_, id)| **id == target)
                .map(|(id, _)| id as i32)
                .collect(),
            requested,
        ),
    }
}

fn restrict(mut ids: Vec<i32>, requested: Option<&[i32]>) -> Vec<i32> {
    if let Some(requested) = requested {
        ids.retain(|id| requested.contains(id));
    }
    ids
}

pub(super) fn overlap(left: &PatchGroup, right: &PatchGroup) -> bool {
    left.iter().zip(right).all(|(a, b)| {
        a.iter().any(|id| *id < 0)
            || b.iter().any(|id| *id < 0)
            || a.iter().any(|id| b.contains(id))
    })
}
