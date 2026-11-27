// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn build_scans(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
    field_name_lookup: &HashMap<i32, String>,
    dd_to_spw: &HashMap<i32, i32>,
    state_intent_lookup: &HashMap<i32, String>,
    unflagged_fractions: Option<&[f64]>,
    group_scan_by_field: bool,
) -> MsResult<Vec<ScanSummary>> {
    #[derive(Debug, Clone)]
    struct MainRowInfo {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        field_id: i32,
        data_desc_id: i32,
        state_id: i32,
        time: f64,
        exposure_seconds: f64,
        report_interval_seconds: f64,
        unflagged_row_fraction: Option<f64>,
    }

    #[derive(Debug, Default, Clone)]
    struct TimeGroup {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        time: f64,
        row_count: usize,
        field_ids: BTreeSet<i32>,
        field_names: BTreeSet<String>,
        data_description_ids: BTreeSet<i32>,
        spectral_window_ids: BTreeSet<i32>,
        state_ids: BTreeSet<i32>,
        report_interval_by_spw: BTreeMap<i32, (f64, usize)>,
        exposure_sum_seconds: f64,
        unflagged_row_sum: f64,
    }

    #[derive(Debug, Default)]
    struct SubScanAccumulator {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        row_count: usize,
        field_ids: BTreeSet<i32>,
        field_names: BTreeSet<String>,
        data_description_ids: BTreeSet<i32>,
        spectral_window_ids: BTreeSet<i32>,
        state_ids: BTreeSet<i32>,
        report_interval_by_spw: BTreeMap<i32, (f64, usize)>,
        exposure_sum_seconds: f64,
        unflagged_row_sum: f64,
    }

    impl TimeGroup {
        fn from_row(
            row: &MainRowInfo,
            field_name_lookup: &HashMap<i32, String>,
            dd_to_spw: &HashMap<i32, i32>,
        ) -> Self {
            let mut group = Self {
                observation_id: row.observation_id,
                array_id: row.array_id,
                scan_number: row.scan_number,
                time: row.time,
                ..Self::default()
            };
            group.add_row(row, field_name_lookup, dd_to_spw);
            group
        }

        fn add_row(
            &mut self,
            row: &MainRowInfo,
            field_name_lookup: &HashMap<i32, String>,
            dd_to_spw: &HashMap<i32, i32>,
        ) {
            self.row_count += 1;
            self.field_ids.insert(row.field_id);
            self.field_names.insert(
                field_name_lookup
                    .get(&row.field_id)
                    .cloned()
                    .unwrap_or_else(|| format!("FIELD_{}", row.field_id)),
            );
            self.data_description_ids.insert(row.data_desc_id);
            if let Some(spw_id) = dd_to_spw.get(&row.data_desc_id).copied() {
                self.spectral_window_ids.insert(spw_id);
                let entry = self
                    .report_interval_by_spw
                    .entry(spw_id)
                    .or_insert((0.0, 0));
                entry.0 += row.report_interval_seconds;
                entry.1 += 1;
            }
            self.state_ids.insert(row.state_id);
            self.exposure_sum_seconds += row.exposure_seconds;
            self.unflagged_row_sum += row.unflagged_row_fraction.unwrap_or(0.0);
        }
    }

    impl SubScanAccumulator {
        fn new(group: &TimeGroup) -> Self {
            Self {
                observation_id: group.observation_id,
                array_id: group.array_id,
                scan_number: group.scan_number,
                row_count: group.row_count,
                field_ids: group.field_ids.clone(),
                field_names: group.field_names.clone(),
                data_description_ids: group.data_description_ids.clone(),
                spectral_window_ids: group.spectral_window_ids.clone(),
                state_ids: group.state_ids.clone(),
                report_interval_by_spw: group.report_interval_by_spw.clone(),
                exposure_sum_seconds: group.exposure_sum_seconds,
                unflagged_row_sum: group.unflagged_row_sum,
            }
        }

        fn absorb(&mut self, group: &TimeGroup) {
            self.row_count += group.row_count;
            self.exposure_sum_seconds += group.exposure_sum_seconds;
            self.field_ids.extend(group.field_ids.iter().copied());
            self.field_names.extend(group.field_names.iter().cloned());
            self.data_description_ids
                .extend(group.data_description_ids.iter().copied());
            self.spectral_window_ids
                .extend(group.spectral_window_ids.iter().copied());
            self.state_ids.extend(group.state_ids.iter().copied());
            for (spw_id, (sum, count)) in &group.report_interval_by_spw {
                let entry = self
                    .report_interval_by_spw
                    .entry(*spw_id)
                    .or_insert((0.0, 0));
                entry.0 += *sum;
                entry.1 += *count;
            }
            self.unflagged_row_sum += group.unflagged_row_sum;
        }

        fn matches_group(&self, group: &TimeGroup, group_scan_by_field: bool) -> bool {
            self.observation_id == group.observation_id
                && self.array_id == group.array_id
                && self.scan_number == group.scan_number
                && self.field_ids == group.field_ids
                && (group_scan_by_field || self.data_description_ids == group.data_description_ids)
        }
    }

    let table = ms.main_table();
    let has_exposure = has_column(table, "EXPOSURE");
    let mut columns = vec![
        "OBSERVATION_ID",
        "ARRAY_ID",
        "SCAN_NUMBER",
        "FIELD_ID",
        "DATA_DESC_ID",
        "STATE_ID",
        "TIME",
        "INTERVAL",
    ];
    if has_exposure {
        columns.push("EXPOSURE");
    }
    let mut rows = Vec::with_capacity(table.row_count());
    let mut scan_time_ranges = BTreeMap::<(i32, i32, i32), (f64, f64)>::new();
    let row_numbers = selected_rows
        .map(|rows| rows.to_vec())
        .unwrap_or_else(|| (0..table.row_count()).collect());
    let row_chunk_rows = planned_scalar_scan_rows(row_numbers.len(), 6 * 4 + 3 * 8)?;
    let mut row_offset = 0;
    for row_chunk in row_numbers.chunks(row_chunk_rows) {
        let mut values = table.required_scalar_columns_owned_for_rows(&columns, row_chunk)?;
        let observation_values = take_integer(&mut values, "OBSERVATION_ID")?;
        let array_values = take_integer(&mut values, "ARRAY_ID")?;
        let scan_values = take_integer(&mut values, "SCAN_NUMBER")?;
        let field_values = take_integer(&mut values, "FIELD_ID")?;
        let data_desc_values = take_integer(&mut values, "DATA_DESC_ID")?;
        let state_values = take_integer(&mut values, "STATE_ID")?;
        let time_values = take_double(&mut values, "TIME")?;
        let interval_values = take_double(&mut values, "INTERVAL")?;
        let exposure_values = if has_exposure {
            Some(take_double(&mut values, "EXPOSURE")?)
        } else {
            None
        };
        for row_slot in 0..row_chunk.len() {
            let observation_id = observation_values[row_slot];
            let array_id = array_values[row_slot];
            let scan_number = scan_values[row_slot];
            let field_id = field_values[row_slot];
            let data_desc_id = data_desc_values[row_slot];
            let state_id = state_values[row_slot];
            let time = time_values[row_slot];
            let interval = interval_values[row_slot];
            let exposure = exposure_values
                .as_ref()
                .map(|values| values[row_slot])
                .unwrap_or(interval);
            let unflagged_row_fraction =
                unflagged_fractions.map(|fractions| fractions[row_offset + row_slot]);
            rows.push(MainRowInfo {
                observation_id,
                array_id,
                scan_number,
                field_id,
                data_desc_id,
                state_id,
                time,
                exposure_seconds: exposure,
                report_interval_seconds: if group_scan_by_field {
                    interval
                } else {
                    exposure
                },
                unflagged_row_fraction,
            });
            let begin = time - interval / 2.0;
            let end = time + interval / 2.0;
            let entry = scan_time_ranges
                .entry((observation_id, array_id, scan_number))
                .or_insert((begin, end));
            entry.0 = entry.0.min(begin);
            entry.1 = entry.1.max(end);
        }
        row_offset += row_chunk.len();
    }

    rows.sort_by(|left, right| {
        left.observation_id
            .cmp(&right.observation_id)
            .then_with(|| left.array_id.cmp(&right.array_id))
            .then_with(|| left.scan_number.cmp(&right.scan_number))
            .then_with(|| {
                if group_scan_by_field {
                    left.field_id.cmp(&right.field_id)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| left.field_id.cmp(&right.field_id))
            .then_with(|| left.data_desc_id.cmp(&right.data_desc_id))
            .then_with(|| left.state_id.cmp(&right.state_id))
    });

    let mut time_groups = Vec::<TimeGroup>::new();
    for row in &rows {
        let should_extend = time_groups.last().is_some_and(|group| {
            group.observation_id == row.observation_id
                && group.array_id == row.array_id
                && group.scan_number == row.scan_number
                && group.time == row.time
                && (!group_scan_by_field || group.field_ids.contains(&row.field_id))
        });
        if should_extend {
            if let Some(group) = time_groups.last_mut() {
                group.add_row(row, field_name_lookup, dd_to_spw);
            }
            continue;
        }

        time_groups.push(TimeGroup::from_row(row, field_name_lookup, dd_to_spw));
    }

    let finish_scan_summary = |scan: SubScanAccumulator| {
        let field_id = scan.field_ids.iter().copied().next().unwrap_or(-1);
        let field_name = field_name_lookup
            .get(&field_id)
            .cloned()
            .or_else(|| scan.field_names.iter().next().cloned())
            .unwrap_or_else(|| format!("FIELD_{field_id}"));
        let spectral_window_ids: Vec<i32> = scan.spectral_window_ids.iter().copied().collect();
        let mut scan_intents = Vec::new();
        for state_id in &scan.state_ids {
            if let Some(intent) = state_intent_lookup.get(state_id) {
                push_unique_string(&mut scan_intents, intent);
            }
        }
        let (start_mjd_seconds, end_mjd_seconds) = scan_time_ranges
            .get(&(scan.observation_id, scan.array_id, scan.scan_number))
            .copied()
            .unwrap_or((0.0, 0.0));

        ScanSummary {
            observation_id: scan.observation_id,
            array_id: scan.array_id,
            scan_number: scan.scan_number,
            row_count: scan.row_count,
            field_id,
            field_name,
            field_ids: scan.field_ids.into_iter().collect(),
            field_names: scan.field_names.into_iter().collect(),
            data_description_ids: scan.data_description_ids.into_iter().collect(),
            spectral_window_ids: spectral_window_ids.clone(),
            state_ids: scan.state_ids.into_iter().collect(),
            scan_intents,
            start_mjd_seconds,
            end_mjd_seconds,
            mean_interval_seconds: if scan.row_count == 0 {
                0.0
            } else {
                scan.exposure_sum_seconds / scan.row_count as f64
            },
            mean_interval_seconds_by_spw: spectral_window_ids
                .iter()
                .map(|spw_id| {
                    scan.report_interval_by_spw
                        .get(spw_id)
                        .map(|(sum, count)| {
                            if *count == 0 {
                                0.0
                            } else {
                                *sum / *count as f64
                            }
                        })
                        .unwrap_or(0.0)
                })
                .collect(),
            unflagged_row_count: unflagged_fractions.map(|_| scan.unflagged_row_sum),
        }
    };

    let mut summaries = Vec::with_capacity(time_groups.len());
    let mut current = None::<SubScanAccumulator>;
    for group in &time_groups {
        let same_subscan = current
            .as_ref()
            .is_some_and(|scan| scan.matches_group(group, group_scan_by_field));

        if !same_subscan {
            if let Some(scan) = current.take() {
                summaries.push(finish_scan_summary(scan));
            }
            current = Some(SubScanAccumulator::new(group));
            continue;
        }

        if let Some(scan) = current.as_mut() {
            scan.absorb(group);
        }
    }

    if let Some(scan) = current {
        summaries.push(finish_scan_summary(scan));
    }
    Ok(summaries)
}

impl UnflaggedRowContext {
    pub(super) fn new(ms: &MeasurementSet, use_row_flag: bool) -> MsResult<Self> {
        let dd = ms.data_description()?;
        let spw = ms.spectral_window()?;
        let mut ddid_to_bandwidth_hz = HashMap::new();
        let mut ddid_to_channel_widths_hz = HashMap::new();
        for row in 0..dd.row_count() {
            let spw_id = dd.spectral_window_id(row)?;
            if spw_id < 0 {
                continue;
            }
            let mut channel_widths = spw.chan_width(spw_id as usize)?;
            if channel_widths.is_empty() {
                channel_widths = spw.effective_bw(spw_id as usize)?;
            }
            channel_widths
                .iter_mut()
                .for_each(|width| *width = width.abs());
            let bandwidth_hz = spw
                .total_bandwidth(spw_id as usize)
                .map(f64::abs)
                .unwrap_or_else(|_| channel_widths.iter().sum());
            ddid_to_bandwidth_hz.insert(row as i32, bandwidth_hz);
            ddid_to_channel_widths_hz.insert(row as i32, channel_widths);
        }
        let polarization = ms.polarization()?;
        let mut max_correlations = 0;
        for row in 0..polarization.row_count() {
            max_correlations = max_correlations.max(polarization.num_corr(row)?.max(0) as usize);
        }
        let max_channels = ddid_to_channel_widths_hz
            .values()
            .map(Vec::len)
            .max()
            .unwrap_or(0);
        let flag_bytes_per_row = std::mem::size_of::<Option<ArrayValue>>()
            .saturating_add(max_correlations.saturating_mul(max_channels));
        Ok(Self {
            use_row_flag,
            ddid_to_bandwidth_hz,
            ddid_to_channel_widths_hz,
            flag_bytes_per_row,
        })
    }

    fn fractions(
        &self,
        table: &Table,
        rows: &[usize],
        data_desc_ids: &[i32],
    ) -> MsResult<Vec<f64>> {
        let flags = table.column_accessor("FLAG")?.array_cells_owned(rows)?;
        let row_flags = if self.use_row_flag {
            table
                .column_accessor("FLAG_ROW")
                .and_then(|column| column.scalar_cells_owned_for_rows(rows))
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        flags
            .into_iter()
            .enumerate()
            .map(|(slot, value)| {
                let flags = value.ok_or_else(|| MsError::MissingColumn {
                    column: format!("FLAG[row={}]", rows[slot]),
                    table: "MAIN".into(),
                })?;
                let row_flagged =
                    matches!(row_flags.get(slot), Some(Some(ScalarValue::Bool(true))));
                compute_unflagged_row_fraction(flags, row_flagged, data_desc_ids[slot], self)
            })
            .collect()
    }
}

pub(super) fn compute_unflagged_row_fraction(
    flags: ArrayValue,
    row_flagged: bool,
    data_desc_id: i32,
    context: &UnflaggedRowContext,
) -> MsResult<f64> {
    let bandwidth_hz = context
        .ddid_to_bandwidth_hz
        .get(&data_desc_id)
        .copied()
        .unwrap_or(0.0);
    let channel_widths = context
        .ddid_to_channel_widths_hz
        .get(&data_desc_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if bandwidth_hz <= 0.0 {
        return Ok(0.0);
    }
    if row_flagged {
        return Ok(0.0);
    }
    match flags {
        ArrayValue::Bool(values) => {
            let shape = values.shape();
            let slice = values.iter().copied().collect::<Vec<_>>();
            let (n_corr, n_chan) = match shape {
                [] => (1, 1),
                [only] => (1, *only),
                [corr, chan, ..] => (*corr, *chan),
            };
            if slice.iter().all(|flag| !*flag) {
                return Ok(1.0);
            }
            if slice.iter().all(|flag| *flag) {
                return Ok(0.0);
            }
            let denom = bandwidth_hz * n_corr as f64;
            if denom == 0.0 {
                return Ok(0.0);
            }
            let mut bandwidth_sum_hz = 0.0;
            for corr in 0..n_corr {
                let start = corr * n_chan;
                let end = start + n_chan;
                let corr_slice = &slice[start..end];
                if corr_slice.iter().all(|flag| !*flag) {
                    bandwidth_sum_hz += bandwidth_hz;
                    continue;
                }
                if corr_slice.iter().all(|flag| *flag) {
                    continue;
                }
                for (channel_index, flagged) in corr_slice.iter().enumerate() {
                    if !*flagged {
                        bandwidth_sum_hz += channel_widths
                            .get(channel_index)
                            .copied()
                            .unwrap_or_else(|| bandwidth_hz / n_chan as f64);
                    }
                }
            }
            Ok(bandwidth_sum_hz / denom)
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "FLAG".to_string(),
            table: "MAIN".to_string(),
            expected: "Bool array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

pub(super) fn main_time_reference(table: &Table) -> Option<String> {
    let desc = TableMeasDesc::reconstruct(table, "TIME")?;
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => Some(refer.clone()),
        MeasRefDesc::VariableInt { ref_column, .. } => Some(format!("variable:{ref_column}")),
        MeasRefDesc::VariableString { ref_column } => Some(format!("variable:{ref_column}")),
    }
}

pub(super) fn column_measure_reference_for_row(
    table: &Table,
    column: &str,
    row: usize,
) -> MsResult<Option<String>> {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return Ok(None);
    };
    let reference = match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => refer.clone(),
        MeasRefDesc::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => {
            let code = match table.cell_accessor(row, ref_column)?.scalar()? {
                ScalarValue::Int32(value) => *value,
                ScalarValue::Int64(value) => *value as i32,
                ScalarValue::UInt32(value) => *value as i32,
                other => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: "listobs".to_string(),
                        expected: "Int scalar".to_string(),
                        found: format!("{other:?}"),
                    });
                }
            };
            tab_ref_codes
                .iter()
                .position(|candidate| *candidate == code)
                .and_then(|index| tab_ref_types.get(index).cloned())
                .ok_or_else(|| MsError::InvalidMeasureCode {
                    table: "listobs".to_string(),
                    column: ref_column.clone(),
                    code,
                })?
        }
        MeasRefDesc::VariableString { ref_column } => {
            match table.cell_accessor(row, ref_column)?.scalar()? {
                ScalarValue::String(value) => value.clone(),
                other => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: "listobs".to_string(),
                        expected: "String scalar".to_string(),
                        found: format!("{other:?}"),
                    });
                }
            }
        }
    };
    Ok(reference
        .parse::<DirectionRef>()
        .map(|reference| reference.as_str().to_string())
        .ok()
        .or(Some(reference)))
}

pub(super) fn analyze_main_table(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
    unflagged_context: Option<&UnflaggedRowContext>,
) -> MsResult<MainUsage> {
    const COLUMNS: [&str; 8] = [
        "FIELD_ID",
        "OBSERVATION_ID",
        "ARRAY_ID",
        "ANTENNA1",
        "ANTENNA2",
        "DATA_DESC_ID",
        "TIME",
        "INTERVAL",
    ];
    let table = ms.main_table();
    let mut usage = MainUsage {
        selection_applied: selected_rows.is_some(),
        unflagged_row_fractions: unflagged_context.map(|_| Vec::new()),
        ..MainUsage::default()
    };
    let mut start = f64::INFINITY;
    let mut end = f64::NEG_INFINITY;
    let row_numbers = selected_rows
        .map(|rows| rows.to_vec())
        .unwrap_or_else(|| (0..table.row_count()).collect());
    let flag_bytes = unflagged_context.map_or(0, |context| context.flag_bytes_per_row);
    let row_chunk_rows =
        planned_scalar_scan_rows(row_numbers.len(), flag_bytes.saturating_add(6 * 4 + 2 * 8))?;
    for row_chunk in row_numbers.chunks(row_chunk_rows) {
        let mut values = table.required_scalar_columns_owned_for_rows(&COLUMNS, row_chunk)?;
        let field_values = take_integer(&mut values, "FIELD_ID")?;
        let observation_values = take_integer(&mut values, "OBSERVATION_ID")?;
        let array_values = take_integer(&mut values, "ARRAY_ID")?;
        let antenna1_values = take_integer(&mut values, "ANTENNA1")?;
        let antenna2_values = take_integer(&mut values, "ANTENNA2")?;
        let data_desc_values = take_integer(&mut values, "DATA_DESC_ID")?;
        let time_values = take_double(&mut values, "TIME")?;
        let interval_values = take_double(&mut values, "INTERVAL")?;
        let fractions = unflagged_context
            .map(|context| context.fractions(table, row_chunk, &data_desc_values))
            .transpose()?;
        for row_slot in 0..row_chunk.len() {
            let field_id = field_values[row_slot];
            if field_id >= 0 {
                usage.used_field_ids.insert(field_id as usize);
                *usage.field_row_counts.entry(field_id).or_insert(0) += 1;
                if let Some(fractions) = fractions.as_ref() {
                    *usage
                        .field_unflagged_row_counts
                        .entry(field_id)
                        .or_insert(0.0) += fractions[row_slot];
                }
            }
            let observation_id = observation_values[row_slot];
            if observation_id >= 0 {
                usage.used_observation_ids.insert(observation_id as usize);
            }
            let array_id = array_values[row_slot];
            usage.used_array_ids.insert(array_id);

            let antenna1 = antenna1_values[row_slot];
            if antenna1 >= 0 {
                usage.used_antenna_ids.insert(antenna1 as usize);
            }
            let antenna2 = antenna2_values[row_slot];
            if antenna2 >= 0 {
                usage.used_antenna_ids.insert(antenna2 as usize);
            }

            let data_desc_id = data_desc_values[row_slot];
            if data_desc_id >= 0 {
                usage
                    .used_data_description_ids
                    .insert(data_desc_id as usize);
            }

            let time = time_values[row_slot];
            let interval = interval_values[row_slot];
            start = start.min(time - interval / 2.0);
            end = end.max(time + interval / 2.0);
            usage.selected_row_count += 1;
        }
        if let (Some(all), Some(fractions)) = (&mut usage.unflagged_row_fractions, fractions) {
            all.extend(fractions);
        }
    }

    if usage.selected_row_count > 0 {
        usage.time_range = Some((start, end));
    }
    Ok(usage)
}

pub(super) fn planned_scalar_scan_rows(
    row_count: usize,
    requested_bytes_per_row: usize,
) -> MsResult<usize> {
    if row_count == 0 {
        return Ok(1);
    }
    let budget = MsSelectionIoBudget::from_system_memory(1, requested_bytes_per_row, None)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    MsReadPlan::new(row_count, budget)
        .map(|plan| plan.rows_per_block)
        .map_err(|error| MsError::InvalidInput(error.to_string()))
}

pub(super) fn build_state_intent_lookup(ms: &MeasurementSet) -> MsResult<HashMap<i32, String>> {
    let state = ms.state()?;
    let mut lookup = HashMap::with_capacity(state.row_count());
    for row in 0..state.row_count() {
        let intent = state.string(row, "OBS_MODE")?;
        if !intent.is_empty() {
            lookup.insert(row as i32, intent);
        }
    }
    Ok(lookup)
}

pub(super) fn collect_used_ids(
    table: &Table,
    rows: &[usize],
    column: &str,
) -> MsResult<BTreeSet<i32>> {
    let mut ids = BTreeSet::new();
    for &row in rows {
        let id = get_i32(table, row, column)?;
        if id >= 0 {
            ids.insert(id);
        }
    }
    Ok(ids)
}
