// SPDX-License-Identifier: LGPL-3.0-or-later
//! Connect the CASA selector grammars to the bounded row selection API.

use super::array_expr::ArraySelection;
use super::feed_expr::FeedSelection;
use super::syntax::{ChannelSelection, ChannelSelectionSegment, parse_numeric_row_id_ranges};
use super::uv_expr::UvDistanceSelection;
use super::{
    BaselineSelection, CompiledMsSelection, TimeCondition, TimeDefaults, TimeSelectorDefaults,
    int_list, resolve_time_selector,
};
use crate::subtables::{get_bool, get_f64, has_column};
use crate::{ListObsOptions, MeasurementSet, MsResult};

pub(super) fn apply(
    ms: &MeasurementSet,
    mut compiled: CompiledMsSelection,
    options: &ListObsOptions,
    flagdata: bool,
) -> MsResult<CompiledMsSelection> {
    if !flagdata && let Some(text) = options.spw.as_deref() {
        let spw = ms.resolve_spw_selector(text)?;
        compiled.spw_data_desc_ids = spw
            .row_data_descriptions()
            .map(|ids| ids.iter().map(|&id| id as i32).collect());
        let windows = ms.spectral_window()?;
        let mut full_windows = std::collections::BTreeSet::new();
        for range in spw.channel_list() {
            if range.start == 0
                && range.step <= 1
                && range.stop == windows.num_chan(range.spw_id)? - 1
            {
                full_windows.insert(range.spw_id);
                compiled
                    .spw_channel_selections
                    .remove(&(range.spw_id as i32));
            }
            if full_windows.contains(&range.spw_id) {
                continue;
            }
            compiled
                .spw_channel_selections
                .entry(range.spw_id as i32)
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
    if let Some(text) = options.antenna.as_deref().filter(|text| !text.is_empty()) {
        let baseline = if flagdata {
            ms.resolve_flagdata_antenna_selector(text)?
        } else {
            ms.resolve_antenna_selector(text)?
        };
        compiled.casa_taql_exprs.push(baseline_predicate(&baseline));
        compiled.casa_baseline = Some(baseline);
    }
    let observation = if flagdata {
        None
    } else {
        options.observation.as_deref()
    };
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
            observation,
            &mut compiled.casa_observation_ranges,
        ),
    ] {
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            let ranges = parse_numeric_row_id_ranges(text, label)?;
            compiled
                .casa_taql_exprs
                .push(row_ids_predicate(column, &ranges));
            *target = Some(ranges);
        }
    }
    Ok(compiled)
}

pub(super) fn apply_transform_selectors(
    ms: &MeasurementSet,
    mut compiled: CompiledMsSelection,
    options: &ListObsOptions,
) -> MsResult<CompiledMsSelection> {
    if let Some(text) = options.array.as_deref().filter(|text| !text.is_empty()) {
        let ranges = ArraySelection::parse(text)?.ranges;
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
    if let Some(text) = options.uvrange.as_deref().filter(|text| !text.is_empty()) {
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
) -> MsResult<CompiledMsSelection> {
    if text.is_empty() {
        return Ok(selection);
    }
    let table = ms.main_table();
    let first = if selection.where_clauses().is_empty() {
        let mut first = 0;
        if has_column(table, "FLAG_ROW") {
            for row in 0..table.row_count() {
                if !get_bool(table, row, "FLAG_ROW")? {
                    first = row;
                    break;
                }
            }
        }
        first
    } else {
        selection.first_match(ms, rows_per_block)?.unwrap_or(0)
    };
    let defaults = |row| -> MsResult<TimeDefaults> {
        Ok(TimeDefaults {
            time_mjd_s: get_f64(table, row, "TIME")?,
            exposure_s: if has_column(table, "EXPOSURE") {
                get_f64(table, row, "EXPOSURE")?
            } else {
                0.1
            },
        })
    };
    let time = resolve_time_selector(
        text,
        TimeSelectorDefaults {
            first_term: defaults(first)?,
            later_terms: defaults(0)?,
        },
    )?;
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

fn disjunction(terms: Vec<String>) -> String {
    if terms.is_empty() {
        "FALSE".to_string()
    } else {
        terms.join(" OR ")
    }
}

/// One compact condition per first antenna. The full grammar distinguishes
/// equal and unequal out-of-range IDs, so keep those cases in the predicate.
fn baseline_predicate(selection: &BaselineSelection) -> String {
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
