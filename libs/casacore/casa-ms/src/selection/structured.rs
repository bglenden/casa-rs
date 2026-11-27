// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn apply_structured_selection(
    sel: &CompiledMsSelection,
    ms: &MeasurementSet,
    effective_ddids: &[i32],
    rows_per_block: Option<usize>,
    exclude_autocorrelations: bool,
    first_only: bool,
) -> MsResult<Vec<usize>> {
    let setup_started_at = Instant::now();
    if sel.field_ids.as_ref().is_some_and(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let field_ids: HashSet<i32> = sel.field_ids.iter().flatten().copied().collect();
    let spw_ddids: HashSet<i32> = sel
        .spw_data_desc_ids
        .as_deref()
        .unwrap_or(effective_ddids)
        .iter()
        .copied()
        .collect();
    let data_desc_ids: HashSet<i32> = sel.data_desc_ids.iter().copied().collect();
    let antenna_ids: HashSet<i32> = sel.antenna_ids.iter().copied().collect();
    let baselines: HashSet<(i32, i32)> = sel.baselines.iter().copied().collect();
    let scan_numbers: HashSet<i32> = sel.scan_numbers.iter().copied().collect();
    let state_ids: HashSet<i32> = sel.state_ids.iter().copied().collect();
    let observation_ids: HashSet<i32> = sel.observation_ids.iter().copied().collect();
    let array_ids: HashSet<i32> = sel.array_ids.iter().copied().collect();
    let uv_lambda_by_ddid = if sel
        .uv_ranges
        .iter()
        .any(|range| matches!(range.unit, UvUnit::Lambda(_)))
    {
        parser::ddid_to_lambda_map(ms)?
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };

    if (!sel.spw_ids.is_empty() || sel.spw_data_desc_ids.is_some()) && spw_ddids.is_empty() {
        return Ok(Vec::new());
    }

    let table = ms.main_table();
    let needs_antennas = exclude_autocorrelations
        || !antenna_ids.is_empty()
        || !baselines.is_empty()
        || sel.casa_baseline.is_some();
    let column_request = StructuredSelectionColumnRequest {
        field: !field_ids.is_empty(),
        data_desc: !spw_ddids.is_empty()
            || !data_desc_ids.is_empty()
            || !uv_lambda_by_ddid.is_empty()
            || sel
                .casa_uv
                .as_ref()
                .is_some_and(|uv| uv.needs_data_desc_id()),
        antenna1: needs_antennas,
        antenna2: needs_antennas,
        time: sel.time_range.is_some() || sel.casa_time.is_some(),
        scan: !scan_numbers.is_empty() || sel.casa_scan_ranges.is_some(),
        state: !state_ids.is_empty(),
        observation: !observation_ids.is_empty() || sel.casa_observation_ranges.is_some(),
        array: !array_ids.is_empty() || sel.casa_array_ranges.is_some(),
        feed: sel.casa_feed.is_some(),
        uvw: !sel.uv_ranges.is_empty() || sel.casa_uv.is_some(),
    };
    let rows_per_block = rows_per_block.unwrap_or_else(|| table.row_count().max(1));
    let mut load_columns_ns = 0u64;
    let mut filter_rows_ns = 0u64;
    let mut rows = Vec::new();
    for start_row in (0..table.row_count()).step_by(rows_per_block) {
        let end_row = start_row
            .saturating_add(rows_per_block)
            .min(table.row_count());
        let block_rows = (start_row..end_row).collect::<Vec<_>>();
        let load_columns_started_at = Instant::now();
        let columns = load_structured_selection_columns(table, column_request, &block_rows)?;
        load_columns_ns =
            load_columns_ns.saturating_add(load_columns_started_at.elapsed().as_nanos() as u64);
        let filter_rows_started_at = Instant::now();
        for (slot, row) in block_rows.into_iter().enumerate() {
            if exclude_autocorrelations
                && columns.antenna1.as_ref().expect("requested antennas")[slot]
                    == columns.antenna2.as_ref().expect("requested antennas")[slot]
            {
                continue;
            }
            if structured_row_matches(
                slot,
                sel,
                &columns,
                &field_ids,
                &spw_ddids,
                &data_desc_ids,
                &antenna_ids,
                &baselines,
                &scan_numbers,
                &state_ids,
                &observation_ids,
                &array_ids,
                &uv_lambda_by_ddid,
            ) {
                rows.push(row);
                if first_only {
                    break;
                }
            }
        }
        filter_rows_ns =
            filter_rows_ns.saturating_add(filter_rows_started_at.elapsed().as_nanos() as u64);
        if first_only && !rows.is_empty() {
            break;
        }
    }

    log_selection_profile(
        "structured_selection",
        setup_started_at.elapsed().as_nanos() as u64,
        Some(format!(
            "rows={} row_count={} load_columns={:.3}s filter_rows={:.3}s requested=field:{} data_desc:{} antenna:{} time:{} scan:{} state:{} observation:{} array:{} feed:{} uvrange:{}",
            rows.len(),
            table.row_count(),
            load_columns_ns as f64 / 1_000_000_000.0,
            filter_rows_ns as f64 / 1_000_000_000.0,
            !field_ids.is_empty(),
            column_request.data_desc,
            needs_antennas,
            column_request.time,
            column_request.scan,
            !state_ids.is_empty(),
            column_request.observation,
            column_request.array,
            column_request.feed,
            column_request.uvw,
        )),
    );

    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn structured_row_matches(
    slot: usize,
    sel: &CompiledMsSelection,
    columns: &StructuredSelectionColumns,
    field_ids: &HashSet<i32>,
    spw_ddids: &HashSet<i32>,
    data_desc_ids: &HashSet<i32>,
    antenna_ids: &HashSet<i32>,
    baselines: &HashSet<(i32, i32)>,
    scan_numbers: &HashSet<i32>,
    state_ids: &HashSet<i32>,
    observation_ids: &HashSet<i32>,
    array_ids: &HashSet<i32>,
    uv_lambda_by_ddid: &BTreeMap<i32, f64>,
) -> bool {
    if columns
        .field
        .as_ref()
        .is_some_and(|column| !field_ids.contains(&column[slot]))
    {
        return false;
    }
    if let Some(column) = columns.data_desc.as_ref() {
        let data_desc_id = column[slot];
        if (!spw_ddids.is_empty() && !spw_ddids.contains(&data_desc_id))
            || (!data_desc_ids.is_empty() && !data_desc_ids.contains(&data_desc_id))
        {
            return false;
        }
    }
    if let (Some(antenna1), Some(antenna2)) = (columns.antenna1.as_ref(), columns.antenna2.as_ref())
    {
        let pair = (antenna1[slot], antenna2[slot]);
        if (!antenna_ids.is_empty()
            && !antenna_ids.contains(&pair.0)
            && !antenna_ids.contains(&pair.1))
            || (!baselines.is_empty() && !baselines.contains(&pair))
            || sel.casa_baseline.as_ref().is_some_and(|selection| {
                // Existing and out-of-table IDs have distinct predicate
                // slots. Offset signed IDs into out-of-table slots without
                // changing equality between negative values.
                !selection.holds(pair.0 as usize, pair.1 as usize)
            })
        {
            return false;
        }
    }
    if let (Some((start, end)), Some(column)) = (sel.time_range, columns.time.as_ref())
        && !(start..=end).contains(&column[slot])
    {
        return false;
    }
    if let (Some(time), Some(column)) = (&sel.casa_time, columns.time.as_ref())
        && !time.holds(column[slot])
    {
        return false;
    }
    if let Some(uvw) = columns.uvw.as_ref() {
        let data_desc_id = columns
            .data_desc
            .as_ref()
            .and_then(|values| values.get(slot))
            .copied();
        if (!sel.uv_ranges.is_empty()
            && !sel
                .uv_ranges
                .iter()
                .any(|range| uv_range_matches(*range, uvw[slot], data_desc_id, uv_lambda_by_ddid)))
            || sel
                .casa_uv
                .as_ref()
                .is_some_and(|uv| !uv.holds(uvw[slot], data_desc_id))
        {
            return false;
        }
    }
    if let (Some(selection), Some((first, second))) = (&sel.casa_feed, &columns.feed)
        && !selection.holds(first[slot], second[slot])
    {
        return false;
    }
    columns
        .scan
        .as_ref()
        .is_none_or(|column| row_id_matches(column[slot], scan_numbers, &sel.casa_scan_ranges))
        && columns
            .state
            .as_ref()
            .is_none_or(|column| state_ids.contains(&column[slot]))
        && columns.observation.as_ref().is_none_or(|column| {
            row_id_matches(column[slot], observation_ids, &sel.casa_observation_ranges)
        })
        && columns
            .array
            .as_ref()
            .is_none_or(|column| row_id_matches(column[slot], array_ids, &sel.casa_array_ranges))
}

pub(super) fn row_id_matches(
    id: i32,
    ids: &HashSet<i32>,
    ranges: &Option<Vec<(i32, i32)>>,
) -> bool {
    (ids.is_empty() || ids.contains(&id))
        && ranges.as_ref().is_none_or(|ranges| {
            ranges
                .iter()
                .any(|&(start, end)| (start..=end).contains(&id))
        })
}

pub(super) fn uv_range_matches(
    range: UvSelectionRange,
    uvw_m: [f64; 3],
    data_desc_id: Option<i32>,
    uv_lambda_by_ddid: &BTreeMap<i32, f64>,
) -> bool {
    let unit_to_meters = match range.unit {
        UvUnit::Meters(scale) => scale,
        UvUnit::Lambda(scale) => {
            let Some(lambda_m) = data_desc_id
                .and_then(|id| uv_lambda_by_ddid.get(&id))
                .copied()
            else {
                return false;
            };
            lambda_m * scale
        }
    };
    let distance_squared_m2 = uvw_m[0] * uvw_m[0] + uvw_m[1] * uvw_m[1];
    if !(unit_to_meters.is_finite() && unit_to_meters > 0.0 && distance_squared_m2.is_finite()) {
        return false;
    }
    let compare = |bound: UvBound| {
        let bound_squared_m2 = (bound.value * unit_to_meters).powi(2);
        match bound.op {
            UvBoundOp::Greater => distance_squared_m2 > bound_squared_m2,
            UvBoundOp::GreaterEqual => distance_squared_m2 >= bound_squared_m2,
            UvBoundOp::Less => distance_squared_m2 < bound_squared_m2,
            UvBoundOp::LessEqual => distance_squared_m2 <= bound_squared_m2,
        }
    };
    range.lower.is_none_or(compare) && range.upper.is_none_or(compare)
}

/// The `tracing` target of the selection profile (`debug`).
pub(super) const SELECTION_PROFILE: &str = "casa_ms::selection_profile";

/// Whether the logging configuration takes the selection profile.
pub(super) fn selection_profile_enabled() -> bool {
    tracing::enabled!(target: SELECTION_PROFILE, tracing::Level::DEBUG)
}

pub(super) fn log_selection_profile(phase: &str, total_ns: u64, detail: Option<String>) {
    if !selection_profile_enabled() {
        return;
    }
    let mut line = format!(
        "[casa-ms selection] phase={phase} dt={:.3}s",
        total_ns as f64 / 1_000_000_000.0
    );
    if let Some(detail) = detail
        && !detail.is_empty()
    {
        line.push(' ');
        line.push_str(&detail);
    }
    tracing::debug!(target: SELECTION_PROFILE, "{line}");
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StructuredSelectionColumnRequest {
    pub(super) field: bool,
    pub(super) data_desc: bool,
    pub(super) antenna1: bool,
    pub(super) antenna2: bool,
    pub(super) time: bool,
    pub(super) scan: bool,
    pub(super) state: bool,
    pub(super) observation: bool,
    pub(super) array: bool,
    pub(super) feed: bool,
    pub(super) uvw: bool,
}

#[derive(Debug, Default)]
pub(super) struct StructuredSelectionColumns {
    pub(super) field: Option<Vec<i32>>,
    pub(super) data_desc: Option<Vec<i32>>,
    pub(super) antenna1: Option<Vec<i32>>,
    pub(super) antenna2: Option<Vec<i32>>,
    pub(super) time: Option<Vec<f64>>,
    pub(super) scan: Option<Vec<i32>>,
    pub(super) state: Option<Vec<i32>>,
    pub(super) observation: Option<Vec<i32>>,
    pub(super) array: Option<Vec<i32>>,
    pub(super) feed: Option<(Vec<i32>, Vec<i32>)>,
    pub(super) uvw: Option<Vec<[f64; 3]>>,
}

pub(super) fn load_structured_selection_columns(
    table: &Table,
    request: StructuredSelectionColumnRequest,
    rows: &[usize],
) -> MsResult<StructuredSelectionColumns> {
    let mut columns = StructuredSelectionColumns::default();

    if rows.is_empty() {
        if request.field {
            columns.field = Some(Vec::new());
        }
        if request.data_desc {
            columns.data_desc = Some(Vec::new());
        }
        if request.antenna1 {
            columns.antenna1 = Some(Vec::new());
        }
        if request.antenna2 {
            columns.antenna2 = Some(Vec::new());
        }
        if request.time {
            columns.time = Some(Vec::new());
        }
        if request.scan {
            columns.scan = Some(Vec::new());
        }
        if request.state {
            columns.state = Some(Vec::new());
        }
        if request.observation {
            columns.observation = Some(Vec::new());
        }
        if request.array {
            columns.array = Some(Vec::new());
        }
        if request.feed {
            columns.feed = Some((Vec::new(), Vec::new()));
        }
        if request.uvw {
            columns.uvw = Some(Vec::new());
        }
        return Ok(columns);
    }

    if request.field {
        columns.field = Some(load_field_ids(table, rows)?);
    }
    if request.data_desc {
        columns.data_desc = Some(load_i32_column(table, "DATA_DESC_ID", rows)?);
    }
    if request.antenna1 {
        columns.antenna1 = Some(load_i32_column(table, "ANTENNA1", rows)?);
    }
    if request.antenna2 {
        columns.antenna2 = Some(load_i32_column(table, "ANTENNA2", rows)?);
    }
    if request.time {
        columns.time = Some(load_f64_column(table, "TIME", rows)?);
    }
    if request.scan {
        columns.scan = Some(load_i32_column(table, "SCAN_NUMBER", rows)?);
    }
    if request.state {
        columns.state = Some(load_i32_column(table, "STATE_ID", rows)?);
    }
    if request.observation {
        columns.observation = Some(load_i32_column(table, "OBSERVATION_ID", rows)?);
    }
    if request.array {
        columns.array = Some(load_i32_column(table, "ARRAY_ID", rows)?);
    }
    if request.feed {
        columns.feed = Some((
            load_i32_column(table, "FEED1", rows)?,
            load_i32_column(table, "FEED2", rows)?,
        ));
    }
    if request.uvw {
        columns.uvw = Some(load_uvw_column(table, rows)?);
    }

    Ok(columns)
}

pub(super) fn int_list(ids: &[i32]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn append_csv(target: &mut Option<String>, value: &str) {
    if value.is_empty() {
        return;
    }
    match target {
        Some(existing) if !existing.is_empty() => {
            existing.push(',');
            existing.push_str(value);
        }
        _ => *target = Some(value.to_string()),
    }
}

/// Resolve antenna names to IDs using the ANTENNA subtable.
pub(super) fn resolve_antenna_names(ms: &MeasurementSet, names: &[String]) -> MsResult<Vec<i32>> {
    let ant = ms.antenna()?;
    let mut ids = Vec::new();
    for row in 0..ant.row_count() {
        let name = ant.name(row)?;
        if names.iter().any(|n| n == &name) {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::MissingColumn {
            column: format!("antenna names {names:?}"),
            table: "ANTENNA".to_string(),
        });
    }
    Ok(ids)
}

/// Resolve spectral window IDs to DATA_DESC_IDs using the DATA_DESCRIPTION subtable.
pub(super) fn resolve_spw_to_ddid(ms: &MeasurementSet, spw_ids: &[i32]) -> MsResult<Vec<i32>> {
    let dd = ms.data_description()?;
    let mut ddids = Vec::new();
    for row in 0..dd.row_count() {
        let spw_id = dd.spectral_window_id(row)?;
        if spw_ids.contains(&spw_id) {
            ddids.push(row as i32);
        }
    }
    Ok(ddids)
}
