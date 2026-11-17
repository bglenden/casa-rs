// SPDX-License-Identifier: LGPL-3.0-or-later
//! Connect the CASA selector grammars to the bounded row selection API.

use super::syntax::{ChannelSelection, ChannelSelectionSegment, parse_numeric_row_id_ranges};
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
) -> MsResult<CompiledMsSelection> {
    if let Some(text) = options.spw.as_deref() {
        let spw = ms.resolve_spw_selector(text)?;
        compiled.spw_data_desc_ids = spw
            .row_data_descriptions()
            .map(|ids| ids.iter().map(|&id| id as i32).collect());
        for range in spw.channel_list() {
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
        let baseline = ms.resolve_antenna_selector(text)?;
        compiled = compiled.taql(&baseline_predicate(&baseline));
    }
    for (column, label, text) in [
        ("SCAN_NUMBER", "scan", &options.scan),
        ("OBSERVATION_ID", "observation", &options.observation),
    ] {
        if let Some(text) = text.as_deref().filter(|text| !text.is_empty()) {
            let ranges = parse_numeric_row_id_ranges(text, label)?;
            let terms = ranges
                .into_iter()
                .map(|(start, end)| {
                    if start == end {
                        format!("{column}=={start}")
                    } else {
                        format!("({column}>={start} AND {column}<={end})")
                    }
                })
                .collect::<Vec<_>>();
            compiled = compiled.taql(&disjunction(terms));
        }
    }
    Ok(compiled)
}

pub(super) fn with_time(
    ms: &MeasurementSet,
    selection: CompiledMsSelection,
    text: &str,
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
        selection.apply(ms)?.first().copied().unwrap_or(0)
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
    Ok(selection.taql(&disjunction(terms)))
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
