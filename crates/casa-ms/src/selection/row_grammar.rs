// SPDX-License-Identifier: LGPL-3.0-or-later
//! Connect the CASA selector grammars to the bounded row selection API.

use super::array_expr::ArraySelection;
use super::feed_expr::FeedSelection;
use super::syntax::{ChannelSelection, ChannelSelectionSegment, parse_numeric_row_id_ranges};
use super::uv_expr::UvDistanceSelection;
use std::collections::{BTreeMap, BTreeSet};

use super::spw_expr::SpwSelection;
use super::{
    BaselineSelection, CompiledMsSelection, TimeCondition, TimeDefaults, TimeSelectorDefaults,
    default_selection_budget, int_list, resolve_time_selector, rows_per_block_for_budget,
};
use crate::subtables::has_column;
use crate::{ListObsOptions, MeasurementSet, MsError, MsResult};
use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::ScalarValue;

pub(super) fn apply(
    ms: &MeasurementSet,
    mut compiled: CompiledMsSelection,
    options: &ListObsOptions,
    flagdata: bool,
) -> MsResult<CompiledMsSelection> {
    if let Some(text) = options.spw.as_deref().filter(|text| !text.is_empty()) {
        let spw = if flagdata {
            ms.resolve_flagdata_spw_selector(text)?
        } else {
            ms.resolve_spw_selector(text)?
        };
        compiled.spw_data_desc_ids = spw
            .row_data_descriptions()
            .map(|ids| ids.iter().map(|&id| id as i32).collect());
        compiled.spw_channel_selections = if flagdata {
            for warning in spw.warnings() {
                tracing::warn!(%warning, "SPW selection");
            }
            flagdata_channel_ranges(ms, &spw, false)?
        } else {
            selected_channel_ranges(ms, &spw)?
        };
    }
    if let Some(text) = options.antenna.as_deref().filter(|text| !text.is_empty()) {
        let baseline = if flagdata {
            ms.resolve_antenna_selector_with_warnings(text)?
        } else {
            ms.resolve_antenna_selector(text)?
        };
        compiled.casa_taql_exprs.push(baseline_predicate(&baseline));
        compiled.casa_baseline = Some(baseline);
    }
    for (column, label, text, target) in [
        (
            "SCAN_NUMBER",
            "scan",
            options.scan.as_deref(),
            &mut compiled.casa_scan_ranges,
        ),
        (
            "OBSERVATION_ID",
            "observation",
            options.observation.as_deref(),
            &mut compiled.casa_observation_ranges,
        ),
    ] {
        let text = if flagdata {
            text.map(flagdata_c_string)
        } else {
            text
        };
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            let text = if flagdata {
                flagdata_lexer_prefix(text)
            } else {
                text
            };
            let ranges = parse_numeric_row_id_ranges(text, label)?;
            compiled
                .casa_taql_exprs
                .push(row_ids_predicate(column, &ranges));
            *target = Some(ranges);
        }
    }
    Ok(compiled)
}

pub(crate) fn selected_channel_ranges(
    ms: &MeasurementSet,
    spw: &SpwSelection,
) -> MsResult<BTreeMap<i32, ChannelSelection>> {
    channel_ranges(ms, spw, false)
}

fn channel_ranges(
    ms: &MeasurementSet,
    spw: &SpwSelection,
    retain_full_segments: bool,
) -> MsResult<BTreeMap<i32, ChannelSelection>> {
    let windows = ms.spectral_window()?;
    let mut full_windows = BTreeSet::new();
    let mut selected = BTreeMap::<_, ChannelSelection>::new();
    for range in spw.channel_list() {
        let id = range.spw_id as i32;
        if !retain_full_segments
            && range.start == 0
            && range.step <= 1
            && range.stop == windows.num_chan(range.spw_id)? - 1
        {
            full_windows.insert(id);
            selected.remove(&id);
        } else if !full_windows.contains(&id) {
            selected
                .entry(id)
                .or_insert_with(|| ChannelSelection {
                    segments: Vec::new(),
                })
                .segments
                .push(ChannelSelectionSegment {
                    start: range.start,
                    end: range.stop,
                    stride: range.step.max(1) as usize,
                });
        }
    }
    Ok(selected)
}

pub(crate) fn flagdata_channel_ranges(
    ms: &MeasurementSet,
    spw: &SpwSelection,
    retain_full_segments: bool,
) -> MsResult<BTreeMap<i32, ChannelSelection>> {
    let mut ranges = channel_ranges(ms, spw, retain_full_segments)?;
    let named = spw
        .channel_list()
        .iter()
        .map(|range| range.spw_id)
        .collect::<BTreeSet<_>>();
    // Keep windows with no matching channels distinct from named windows
    // whose full channel axes were selected.
    for window in 0..ms.spectral_window()?.row_count() {
        if !named.contains(&window) {
            ranges.insert(
                window as i32,
                ChannelSelection {
                    segments: Vec::new(),
                },
            );
        }
    }
    Ok(ranges)
}

pub(crate) fn flagdata_c_string(text: &str) -> &str {
    text.split_once('\0').map_or(text, |(prefix, _)| prefix)
}

/// CASA's flagdata row lexers stop at the first non-ASCII byte.
pub(super) fn flagdata_lexer_prefix(text: &str) -> &str {
    text.split_once(|character: char| !character.is_ascii())
        .map_or(text, |(prefix, _)| prefix)
}

pub(super) fn apply_array_feed_uv(
    ms: &MeasurementSet,
    mut compiled: CompiledMsSelection,
    options: &ListObsOptions,
    flagdata: bool,
) -> MsResult<CompiledMsSelection> {
    let array = options.array.as_deref();
    let array = if flagdata {
        array.map(flagdata_c_string)
    } else {
        array
    };
    if let Some(text) = array.filter(|text| !text.is_empty()) {
        let ranges = if flagdata {
            // Flagging preserves the stored ARRAY_IDs and needs only their row predicate.
            parse_numeric_row_id_ranges(flagdata_lexer_prefix(text), "array")?
        } else {
            ArraySelection::parse(text)?.ranges
        };
        compiled
            .casa_taql_exprs
            .push(row_ids_predicate("ARRAY_ID", &ranges));
        compiled.casa_array_ranges = Some(ranges);
    }
    if let Some(text) = options.feed.as_deref().filter(|text| !text.is_empty()) {
        let selection = FeedSelection::parse(ms, text)?;
        compiled.casa_taql_exprs.push(selection.to_taql());
        compiled.casa_feed = Some(selection);
    }
    let uvrange = options.uvrange.as_deref();
    let uvrange = if flagdata {
        uvrange.map(flagdata_c_string)
    } else {
        uvrange
    };
    if let Some(text) = uvrange.filter(|text| !text.is_empty()) {
        let text = if flagdata {
            flagdata_lexer_prefix(text)
        } else {
            text
        };
        let selection = UvDistanceSelection::parse(ms, text)?;
        compiled.casa_taql_exprs.push(selection.to_taql());
        compiled.casa_uv = Some(selection);
    }
    Ok(compiled)
}

fn row_ids_predicate(column: &str, ranges: &[(i32, i32)]) -> String {
    disjunction(
        ranges
            .iter()
            .map(|&(start, end)| {
                if start == end {
                    format!("{column}=={start}")
                } else {
                    format!("({column}>={start} AND {column}<={end})")
                }
            })
            .collect(),
    )
}

pub(super) fn with_time(
    ms: &MeasurementSet,
    selection: CompiledMsSelection,
    text: &str,
    rows_per_block: Option<usize>,
    agent_time_default_row: Option<usize>,
) -> MsResult<CompiledMsSelection> {
    if text.is_empty() {
        return Ok(selection);
    }
    let first = if let Some(original_first) = agent_time_default_row {
        flagdata_agent_time_row(ms, &selection, rows_per_block, original_first)?
    } else if selection.where_clauses().is_empty() {
        first_unflagged_time_row(ms)?
    } else {
        selection.first_match(ms, rows_per_block)?.unwrap_or(0)
    };
    let mut time = resolve_time_selector(text, time_defaults(ms.main_table(), first)?)?;
    // AgentFlagger tests the closed endpoints of getTimeList().
    if agent_time_default_row.is_some() {
        time.use_agent_intervals();
    }
    let terms = time.terms().iter().map(|term| match term.condition {
        TimeCondition::Near { time_s, tolerance_s } =>
            format!("ABS(TIME - {time_s}) <= {tolerance_s}"),
        TimeCondition::AtOrAfter { time_s } => format!("TIME >= {time_s}"),
        TimeCondition::AtOrBefore { time_s } => format!("TIME <= {time_s}"),
        TimeCondition::Within { lower_s, upper_s } =>
            format!("(TIME >= {lower_s} AND TIME <= {upper_s})"),
        TimeCondition::EdgeInclusive { lower_s, upper_s, edge_s } => {
            let edge = f64::from(edge_s);
            format!("((TIME > {lower_s} OR ABS(TIME - {lower_s}) < {edge}) AND (TIME < {upper_s} OR ABS(TIME - {upper_s}) < {edge}))")
        }
    }).collect();
    let mut selection = selection;
    selection.casa_taql_exprs.push(disjunction(terms));
    selection.casa_time = Some(time);
    Ok(selection)
}

fn time_defaults(table: &Table, first: usize) -> MsResult<TimeSelectorDefaults> {
    let rows = [first, 0];
    let rows = if first == 0 { &rows[..1] } else { &rows[..] };
    let has_exposure = has_column(table, "EXPOSURE");
    let columns: &[&str] = if has_exposure {
        &["TIME", "EXPOSURE"]
    } else {
        &["TIME"]
    };
    let values = table.required_scalar_columns_owned_for_rows(columns, rows)?;
    let float_column = |column: &str| match values.get(column) {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values.as_slice()),
        _ => Err(MsError::InvalidInput(format!(
            "MAIN {column} must contain double-precision scalars"
        ))),
    };
    let times = float_column("TIME")?;
    let exposures = if has_exposure {
        Some(float_column("EXPOSURE")?)
    } else {
        None
    };
    let at = |index| TimeDefaults {
        time_mjd_s: times[index],
        exposure_s: exposures.map_or(0.1, |values| values[index]),
    };
    Ok(TimeSelectorDefaults {
        first_term: at(0),
        later_terms: at(rows.len() - 1),
    })
}

pub(crate) fn first_unflagged_time_row(ms: &MeasurementSet) -> MsResult<usize> {
    let table = ms.main_table();
    if !has_column(table, "FLAG_ROW") {
        return Ok(0);
    }
    let budget = default_selection_budget()?;
    let limit = rows_per_block_for_budget(budget)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let flags = table.column_accessor("FLAG_ROW")?;
    let mut start = 0_usize;
    let mut block = 1_usize;
    while start < table.row_count() {
        let end = start.saturating_add(block).min(table.row_count());
        let rows = (start..end).collect::<Vec<_>>();
        for (row, flag) in rows
            .iter()
            .copied()
            .zip(flags.scalar_cells_owned_for_rows(&rows)?)
        {
            match flag {
                Some(ScalarValue::Bool(false)) => return Ok(row),
                Some(ScalarValue::Bool(true)) => {}
                _ => {
                    return Err(MsError::InvalidInput(format!(
                        "MAIN FLAG_ROW must contain a Boolean at row {row}"
                    )));
                }
            }
        }
        start = end;
        block = block.saturating_mul(2).min(limit);
    }
    Ok(0)
}

fn flagdata_agent_time_row(
    ms: &MeasurementSet,
    selection: &CompiledMsSelection,
    rows_per_block: Option<usize>,
    original_first: usize,
) -> MsResult<usize> {
    // FlagAgentBase adds array, field, and scan to MSSelection before parsing
    // timerange. The remaining agent selectors are added afterwards.
    if selection.field_ids.is_none()
        && selection.casa_array_ranges.is_none()
        && selection.casa_scan_ranges.is_none()
    {
        return Ok(original_first);
    }
    let prefix = CompiledMsSelection {
        field_ids: selection.field_ids.clone(),
        casa_array_ranges: selection.casa_array_ranges.clone(),
        casa_scan_ranges: selection.casa_scan_ranges.clone(),
        ..CompiledMsSelection::default()
    };
    Ok(prefix.first_match(ms, rows_per_block)?.unwrap_or(0))
}

fn disjunction(terms: Vec<String>) -> String {
    if terms.is_empty() {
        "FALSE".to_string()
    } else {
        terms.join(" OR ")
    }
}

/// One compact condition per first antenna. The full grammar distinguishes
/// equal and unequal out-of-range IDs, so keep those cases in the predicate.
pub(super) fn baseline_predicate(selection: &BaselineSelection) -> String {
    let count = selection.antenna_count();
    let outside = |column| format!("({column}<0 OR {column}>={count})");
    let mut terms = Vec::new();
    for first in 0..=count {
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for second in 0..count {
            let ids = if selection.holds(first, second) {
                &mut accepted
            } else {
                &mut rejected
            };
            ids.push(second as i32);
        }
        let mut second_terms = Vec::new();
        if accepted.len() > rejected.len() {
            let existing = format!("(ANTENNA2>=0 AND ANTENNA2<{count})");
            second_terms.push(if rejected.is_empty() {
                existing
            } else {
                format!(
                    "({existing} AND NOT (ANTENNA2 IN [{}]))",
                    int_list(&rejected)
                )
            });
        } else if !accepted.is_empty() {
            second_terms.push(format!("ANTENNA2 IN [{}]", int_list(&accepted)));
        }

        let same = first == count && selection.holds(first, count);
        let different = selection.holds(first, count + 1);
        match (same, different, first == count) {
            (true, true, _) | (false, true, false) => second_terms.push(outside("ANTENNA2")),
            (true, false, _) => second_terms.push("ANTENNA2==ANTENNA1".into()),
            (false, true, true) => {
                second_terms.push(format!("({} AND ANTENNA2!=ANTENNA1)", outside("ANTENNA2")));
            }
            _ => {}
        }
        if !second_terms.is_empty() {
            let first_term = if first == count {
                outside("ANTENNA1")
            } else {
                format!("ANTENNA1=={first}")
            };
            terms.push(format!(
                "({first_term} AND ({}))",
                disjunction(second_terms)
            ));
        }
    }
    disjunction(terms)
}
