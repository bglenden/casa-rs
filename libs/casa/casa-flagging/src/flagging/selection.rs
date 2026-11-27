// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct SelectedFlagRows {
    pub(super) rows: Vec<usize>,
    pub(super) channel_ranges: BTreeMap<i32, ChannelSelection>,
}

pub(super) fn flagging_channels(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    resolved_ranges: BTreeMap<i32, ChannelSelection>,
) -> Result<Option<BTreeMap<i32, ChannelSelection>>, FlaggingError> {
    let Some(text) = request.spw.as_deref() else {
        return Ok(None);
    };
    let retain_overlaps = request.casa_row_selection && request.mode == FlagDataMode::Extend;
    let ranges =
        if request.casa_row_selection && !retain_overlaps && request.spw == request.selection.spw {
            Ok(resolved_ranges)
        } else if request.casa_row_selection {
            ms.resolve_flagdata_spw_selector(text).and_then(|spw| {
                casa_ms::selection::flagdata_channel_ranges(ms, &spw, retain_overlaps)
            })
        } else {
            parse_channel_selections(text)
        }
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: source.to_string(),
        })?;
    check_channel_selections(ms, &ranges)?;
    Ok(Some(ranges))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FlagTimeSelection {
    Global,
    GlobalAndAgent,
    Agent { initial_default_row: usize },
}

pub(super) fn time_on_globally_selected_rows(
    text: &str,
    rows: &[ResolvedMsSelectionRow],
) -> casa_ms::MsResult<TimeSelection> {
    let Some(first) = rows.first() else {
        return Ok(TimeSelection::all());
    };
    // Exposure affects the global predicate and the width in getTimeList(),
    // but only the endpoints are used by the list agent.
    let defaults = |row: &ResolvedMsSelectionRow| TimeDefaults {
        time_mjd_s: row.time_mjd_seconds,
        exposure_s: 0.0,
    };
    let first_unflagged = rows.iter().find(|row| !row.flag_row).unwrap_or(first);
    resolve_time_selector(
        text,
        TimeSelectorDefaults {
            first_term: defaults(first_unflagged),
            later_terms: defaults(first),
        },
    )
}

pub(super) fn selected_rows(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    time_selection: FlagTimeSelection,
) -> Result<SelectedFlagRows, FlaggingError> {
    let selection_error = |source| FlaggingError::MutateFlags {
        path: display_ms_path(ms),
        reason: format!("select rows: {source}"),
    };
    let timerange = request
        .selection
        .timerange
        .as_deref()
        .filter(|_| request.selection.selectdata)
        .map(flagdata_c_string)
        .filter(|text| !text.is_empty());
    let agent_default_row = match time_selection {
        FlagTimeSelection::Agent {
            initial_default_row,
        } => Some(initial_default_row),
        _ => None,
    };
    let (rows, channel_ranges, missing_rows) = if request.casa_row_selection {
        let resolved = resolve_flagdata_selection_with_system_budget(
            ms,
            &request.selection,
            agent_default_row,
        )
        .map_err(selection_error)?;
        let missing_rows = resolved.selected_rows.is_empty();
        // CASA parses a one-command list's agent on the globally selected
        // table, so omitted dates can differ from the initial global parse.
        let agent_time = timerange
            .filter(|_| time_selection == FlagTimeSelection::GlobalAndAgent)
            .map(|text| time_on_globally_selected_rows(text, &resolved.selected_rows))
            .transpose()
            .map_err(selection_error)?;
        (
            resolved
                .selected_rows
                .into_iter()
                .filter(|row| {
                    agent_time
                        .as_ref()
                        .is_none_or(|time| time.holds_agent_time(row.time_mjd_seconds))
                })
                .map(|row| row.row_index)
                .collect(),
            resolved.channel_selections,
            missing_rows,
        )
    } else {
        let rows = resolve_row_indices_with_system_budget(ms, &request.selection)
            .map_err(selection_error)?;
        let missing_rows = rows.is_empty();
        (rows, BTreeMap::new(), missing_rows)
    };
    let field = request
        .selection
        .field
        .as_deref()
        .filter(|text| !text.trim().is_empty());
    let explicit_spw_filter = missing_rows
        && request.casa_row_selection
        && request
            .selection
            .spw
            .as_deref()
            .map(|text| {
                ms.resolve_flagdata_spw_selector(text)
                    .map(|spw| spw.rows_restricted())
            })
            .transpose()
            .map_err(selection_error)?
            .unwrap_or(false);
    let explicit_row_filter = field.is_some()
        || explicit_spw_filter
        || (request.casa_row_selection
            && [
                request.selection.scan.as_deref(),
                request.selection.antenna.as_deref(),
                request.selection.timerange.as_deref(),
                request.selection.observation.as_deref(),
                request.selection.array.as_deref(),
                request.selection.feed.as_deref(),
                request.selection.intent.as_deref(),
                request.selection.uvrange.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|text| !text.is_empty() && !text.starts_with('\0')));
    if missing_rows && request.selection.selectdata && explicit_row_filter {
        // An empty result can mean no matching FIELD metadata or simply no
        // selected MAIN rows. Keep separate diagnostics for the two cases.
        if let Some(field) = field {
            ms.resolve_field_selector(field).map_err(selection_error)?;
        }
        // An agent can have valid local time intervals that match no
        // records. Keep it in the list, including its empty summary.
        if !(request.casa_row_selection && timerange.is_some() && agent_default_row.is_some()) {
            return Err(FlaggingError::MutateFlags {
                path: display_ms_path(ms),
                reason: "MSSelectionNullSelection : The selected table has zero rows.".to_string(),
            });
        }
    }
    Ok(SelectedFlagRows {
        rows,
        channel_ranges,
    })
}

pub(super) fn flag_read_plan(
    ms: &MeasurementSet,
    row_count: usize,
    primitive_bytes: usize,
    include_data_description: bool,
) -> Result<MsReadPlan, FlaggingError> {
    let path = display_ms_path(ms);
    let maximum_elements = maximum_flag_elements(ms)?;
    let requested_bytes_per_row = maximum_elements
        .checked_mul(primitive_bytes.saturating_add(2))
        .and_then(|bytes| bytes.checked_add(usize::from(include_data_description) * 4))
        .ok_or_else(|| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: "flag row byte accounting overflowed".to_string(),
        })?;
    let budget = MsSelectionIoBudget::from_system_memory(1, requested_bytes_per_row, None)
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: source.to_string(),
        })?;
    MsReadPlan::new(row_count, budget).map_err(|source| FlaggingError::MutateFlags {
        path,
        reason: source.to_string(),
    })
}

pub(super) fn maximum_flag_elements(ms: &MeasurementSet) -> Result<usize, FlaggingError> {
    let path = display_ms_path(ms);
    let data_description = ms
        .data_description()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open DATA_DESCRIPTION: {source}"),
        })?;
    let spectral_window = ms
        .spectral_window()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open SPECTRAL_WINDOW: {source}"),
        })?;
    let polarization = ms
        .polarization()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open POLARIZATION: {source}"),
        })?;
    let mut maximum_elements = 0usize;
    for row in 0..data_description.row_count() {
        let spw_id = data_description.spectral_window_id(row).map_err(|source| {
            FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read DATA_DESCRIPTION.SPECTRAL_WINDOW_ID row {row}: {source}"),
            }
        })?;
        let polarization_id =
            data_description
                .polarization_id(row)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read DATA_DESCRIPTION.POLARIZATION_ID row {row}: {source}"),
                })?;
        let channel_count = usize::try_from(spectral_window.num_chan(spw_id as usize).map_err(
            |source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read SPECTRAL_WINDOW.NUM_CHAN row {spw_id}: {source}"),
            },
        )?)
        .map_err(|_| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("SPECTRAL_WINDOW.NUM_CHAN row {spw_id} is negative"),
        })?;
        let correlation_count = usize::try_from(
            polarization
                .num_corr(polarization_id as usize)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read POLARIZATION.NUM_CORR row {polarization_id}: {source}"),
                })?,
        )
        .map_err(|_| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("POLARIZATION.NUM_CORR row {polarization_id} is negative"),
        })?;
        maximum_elements =
            maximum_elements.max(channel_count.checked_mul(correlation_count).ok_or_else(
                || FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: "flag row shape byte accounting overflowed".to_string(),
                },
            )?);
    }
    Ok(maximum_elements)
}

#[derive(Default)]
pub(super) struct AveragedAgentInput<'a> {
    pub(super) time: Option<&'a TimeSelection>,
    pub(super) uv: Option<&'a UvDistanceSelection>,
    pub(super) view: Option<&'a mut SharedFlagView>,
}

pub(super) fn parse_channel_selections(
    value: &str,
) -> Result<BTreeMap<i32, ChannelSelection>, MsError> {
    let mut selections = BTreeMap::new();
    for selector in parse_spw_selector(value)? {
        if let Some(channels) = selector.channels {
            selections.insert(selector.spw_id, channels);
        }
    }
    Ok(selections)
}

pub(super) fn data_description_spw_map(
    ms: &MeasurementSet,
) -> Result<BTreeMap<i32, i32>, FlaggingError> {
    let table = ms
        .data_description()
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("open DATA_DESCRIPTION: {source}"),
        })?;
    let mut result = BTreeMap::new();
    for row in 0..table.row_count() {
        result.insert(
            row as i32,
            table
                .spectral_window_id(row)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("read DATA_DESCRIPTION row {row}: {source}"),
                })?,
        );
    }
    Ok(result)
}
