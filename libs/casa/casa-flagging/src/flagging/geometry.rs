// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, mem::size_of};

use casa_core::measures::direction::MDirection;

use casa_ms::columns::direction_columns::DirectionColumn;
use casa_ms::derived::engine::{MsCalEngine, raw_field_phase_direction};
use casa_ms::selection::syntax::ChannelSelection;
use casa_ms::subtables::SubTable;
use casa_ms::{MeasurementSet, MsResult};

use super::{
    ChangeSet, CorrelationSelection, FlaggingError, Sample, SelectedFlagRowPolicy, flag_read_plan,
    selected_flag_row_policy, set_selected_flags,
};

pub(super) fn apply_geometric_flags(
    ms: &mut MeasurementSet,
    rows: &[usize],
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
) -> Result<ChangeSet, FlaggingError> {
    let mut changed = ChangeSet::default();
    if rows.is_empty() {
        return Ok(changed);
    }
    let plan = flag_read_plan(ms, rows.len(), size_of::<Sample>(), true)?;
    let row_policy = if channels.is_some() {
        SelectedFlagRowPolicy::Preserve
    } else {
        selected_flag_row_policy(correlations, SelectedFlagRowPolicy::AllSelectedRows)
    };
    for rows in rows.chunks(plan.rows_per_block.max(1)) {
        let block = set_selected_flags(ms, rows, channels, correlations, true, row_policy)?;
        changed.changed_rows += block.changed_rows;
        changed.changed_samples += block.changed_samples;
        changed.changed_flag_row_rows += block.changed_flag_row_rows;
        changed
            .changed_row_indices
            .extend(block.changed_row_indices);
    }
    Ok(changed)
}

pub(super) fn phase_center(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    field_id: usize,
    time: f64,
) -> MsResult<MDirection> {
    let field = ms.field()?;
    let raw = raw_field_phase_direction(ms, field_id)?;
    if field.num_poly(field_id)? > 0 {
        let mut evaluated = DirectionColumn::phase_dir(field.table())
            .get_direction_at(field_id, time - field.time(field_id)?)?;
        evaluated.set_reference(raw.refer());
        return Ok(evaluated);
    }
    Ok(engine
        .ephemeris_phase_direction(field_id, time)?
        .unwrap_or(raw))
}
