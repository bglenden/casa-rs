// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Validate channel ranges before mutating flags. Public CASA-style calls
/// have already saved any requested backup by this stage.
pub(super) fn check_channel_selections(
    ms: &MeasurementSet,
    selections: &BTreeMap<i32, ChannelSelection>,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    let spectral_window = ms
        .spectral_window()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open SPECTRAL_WINDOW: {source}"),
        })?;
    for (&spw, selection) in selections {
        let resolved = usize::try_from(spw)
            .map_err(|_| format!("SPW {spw} is negative"))
            .and_then(|row| {
                let count = spectral_window.num_chan(row).map_err(|source| {
                    format!("read SPECTRAL_WINDOW.NUM_CHAN row {spw}: {source}")
                })?;
                usize::try_from(count).map_err(|_| format!("SPW {spw} has a negative NUM_CHAN"))
            })
            .and_then(|channels| {
                selection
                    .indices(channels)
                    .map(|_| ())
                    .map_err(|source| format!("resolve SPW {spw} channels: {source}"))
            });
        resolved.map_err(|reason| FlaggingError::MutateFlags {
            path: path.clone(),
            reason,
        })?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SelectedFlagRowPolicy {
    ChangedSamples,
    AllSelectedRows,
    FullySelectedRows,
    Preserve,
}

pub(super) fn selected_flag_row_policy(
    correlations: Option<&CorrelationSelection>,
    unfiltered: SelectedFlagRowPolicy,
) -> SelectedFlagRowPolicy {
    match correlations {
        Some(selection) if selection.decorated_stored_products() => {
            SelectedFlagRowPolicy::FullySelectedRows
        }
        Some(_) => SelectedFlagRowPolicy::Preserve,
        None => unfiltered,
    }
}

pub(super) fn set_selected_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    flag: bool,
    row_policy: SelectedFlagRowPolicy,
) -> Result<ChangeSet, FlaggingError> {
    let row_policy = if is_calibration_table(ms) {
        SelectedFlagRowPolicy::Preserve
    } else {
        row_policy
    };
    let loaded = load_samples(
        ms,
        selected_rows,
        channel_selections,
        None,
        correlations,
        false,
        None,
    )?;
    let mut updates = loaded.flags_by_row;
    let mut changed = ChangeSet::default();
    let mut touched_rows = std::collections::BTreeSet::new();
    let mut coverage = BTreeMap::<usize, usize>::new();
    for sample in loaded.samples {
        if row_policy == SelectedFlagRowPolicy::FullySelectedRows {
            *coverage.entry(sample.row).or_default() += 1;
        }
        if sample.flag != flag
            && let Some(flags) = updates.get_mut(&sample.row)
            && let Some(value) = flags.get_mut(IxDyn(&[sample.corr, sample.chan]))
            && *value != flag
        {
            *value = flag;
            changed.changed_samples += 1;
            touched_rows.insert(sample.row);
        }
    }
    if row_policy == SelectedFlagRowPolicy::AllSelectedRows {
        let path = display_ms_path(ms);
        let row_flags = load_bool_cells(ms.main_table(), selected_rows, FLAG_ROW_COLUMN, &path)?;
        for (&row, flag_row) in selected_rows.iter().zip(row_flags) {
            if !flag_row && updates.get(&row).is_some_and(|flags| !flags.is_empty()) {
                touched_rows.insert(row);
            }
        }
    }
    let fully_selected = coverage
        .into_iter()
        .filter_map(|(row, count)| (updates[&row].len() == count).then_some(row))
        .collect::<std::collections::BTreeSet<_>>();
    if !fully_selected.is_empty() {
        let rows = fully_selected.iter().copied().collect::<Vec<_>>();
        let path = display_ms_path(ms);
        let row_flags = load_bool_cells(ms.main_table(), &rows, FLAG_ROW_COLUMN, &path)?;
        touched_rows.extend(
            rows.into_iter()
                .zip(row_flags)
                .filter_map(|(row, row_flag)| (row_flag != flag).then_some(row)),
        );
    }
    write_touched_flag_updates(
        ms,
        updates,
        touched_rows,
        |row| match row_policy {
            SelectedFlagRowPolicy::FullySelectedRows => fully_selected.contains(&row),
            SelectedFlagRowPolicy::Preserve => false,
            _ => true,
        },
        &mut changed,
    )?;
    Ok(changed)
}

pub(super) fn apply_flag_mask_with_preloaded(
    samples: &[Sample],
    mask: &[bool],
    mut updates: BTreeMap<usize, ArrayD<bool>>,
    ms: &mut MeasurementSet,
    update_flag_rows: bool,
    flag_value: bool,
) -> Result<ChangeSet, FlaggingError> {
    let (touched_rows, changed_samples) = stage_flag_mask(samples, mask, &mut updates, flag_value);
    let mut changed = ChangeSet {
        changed_samples,
        ..ChangeSet::default()
    };
    write_touched_flag_updates(
        ms,
        updates,
        touched_rows,
        |_| update_flag_rows,
        &mut changed,
    )?;
    Ok(changed)
}

pub(super) fn stage_flag_mask(
    samples: &[Sample],
    mask: &[bool],
    updates: &mut BTreeMap<usize, ArrayD<bool>>,
    flag_value: bool,
) -> (std::collections::BTreeSet<usize>, usize) {
    let coordinates = samples
        .iter()
        .zip(mask)
        .filter(|(_, flag)| **flag)
        .map(|(sample, _)| (sample.row, sample.corr, sample.chan));
    stage_flag_coordinates(coordinates, updates, flag_value)
}

pub(super) fn stage_flag_coordinates(
    coordinates: impl Iterator<Item = (usize, usize, usize)>,
    updates: &mut BTreeMap<usize, ArrayD<bool>>,
    flag_value: bool,
) -> (std::collections::BTreeSet<usize>, usize) {
    let mut changed_samples = 0;
    let mut touched_rows = std::collections::BTreeSet::<usize>::new();
    for (row, corr, chan) in coordinates {
        let Some(flags) = updates.get_mut(&row) else {
            continue;
        };
        if let Some(value) = flags.get_mut(IxDyn(&[corr, chan]))
            && *value != flag_value
        {
            *value = flag_value;
            changed_samples += 1;
            touched_rows.insert(row);
        }
    }
    (touched_rows, changed_samples)
}

pub(super) fn write_touched_flag_updates(
    ms: &mut MeasurementSet,
    mut updates: std::collections::BTreeMap<usize, ArrayD<bool>>,
    touched_rows: std::collections::BTreeSet<usize>,
    update_flag_row: impl Fn(usize) -> bool,
    changed: &mut ChangeSet,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    let row_flags = !is_calibration_table(ms);
    let mut prepared = Vec::with_capacity(touched_rows.len());
    for row in touched_rows {
        let Some(flags) = updates.remove(&row) else {
            continue;
        };
        prepared.push((row, flags));
    }
    let flag_row_updates = {
        let selected = prepared
            .iter()
            .filter(|(row, _)| row_flags && update_flag_row(*row));
        let rows = selected.clone().map(|(row, _)| *row).collect::<Vec<_>>();
        let current_flag_rows = if row_flags {
            load_bool_cells(ms.main_table(), &rows, FLAG_ROW_COLUMN, &path)?
        } else {
            Vec::new()
        };
        selected
            .zip(current_flag_rows)
            .filter_map(|((row, flags), current)| {
                let flag_row = flags.iter().all(|flag| *flag);
                (current != flag_row).then_some((*row, flag_row))
            })
            .collect::<Vec<_>>()
    };
    ms.main_table_mut()
        .reserve_array_cell_updates(FLAG_COLUMN, prepared.len());
    {
        let columns: &[&str] = if row_flags {
            &[FLAG_COLUMN, FLAG_ROW_COLUMN]
        } else {
            &[FLAG_COLUMN]
        };
        let mut writer = ms
            .main_table_mut()
            .row_accessor_mut()
            .prepare(columns)
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("prepare flag updates: {source}"),
            })?;
        let flag_slot = writer
            .column_index(FLAG_COLUMN)
            .expect("prepared FLAG slot");
        let flag_row_slot = writer.column_index(FLAG_ROW_COLUMN);
        for (row, flags) in &prepared {
            writer
                .seek(*row)
                .and_then(|()| {
                    writer.set_value_at(flag_slot, Value::Array(ArrayValue::Bool(flags.clone())))
                })
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG row {row}: {source}"),
                })?;
        }
        for (row, flag_row) in &flag_row_updates {
            let flag_row_slot = flag_row_slot.expect("prepared FLAG_ROW slot");
            writer
                .seek(*row)
                .and_then(|()| {
                    writer.set_value_at(flag_row_slot, Value::Scalar(ScalarValue::Bool(*flag_row)))
                })
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG_ROW row {row}: {source}"),
                })?;
        }
    }
    changed.changed_flag_row_rows += flag_row_updates.len();
    for (row, _) in prepared {
        changed.changed_rows += 1;
        changed.changed_row_indices.push(row);
    }
    Ok(())
}
