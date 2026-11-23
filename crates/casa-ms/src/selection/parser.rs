// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal selector parsing and TaQL construction for listobs-style selection.

use std::{borrow::Cow, collections::BTreeMap};

use casa_types::quanta::MvTime;

use crate::selection::antenna_expr::regex::FieldRegex;
use crate::selection::syntax::{canonical_numeric_ranges, dedup_i32, parse_numeric_id_selector};
use crate::selection::{CompiledMsSelection, UvBound, UvBoundOp, UvSelectionRange, UvUnit};
use crate::subtables::{MsField, get_f64, has_column, optional_string};
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

use crate::listobs::{ListObsOptions, format_float_compact};

pub(crate) fn selection_from_options(
    ms: &MeasurementSet,
    options: &ListObsOptions,
    grammar: super::RowGrammar,
    rows_per_block: Option<usize>,
) -> MsResult<CompiledMsSelection> {
    use super::RowGrammar;

    let full_row_grammar = matches!(grammar, RowGrammar::Casa | RowGrammar::Transform);
    let flagdata = matches!(grammar, RowGrammar::FlagData { .. });
    let casa_time_grammar = full_row_grammar || flagdata;
    let mut selection = CompiledMsSelection::new();

    if let Some(field) = options.field.as_deref() {
        selection = selection.field(resolve_field_predicate(ms, field)?);
    }

    if full_row_grammar || flagdata {
        selection = super::row_grammar::apply(ms, selection, options, flagdata)?;
    }
    if !full_row_grammar {
        if !flagdata && let Some(spw) = options.spw.as_deref() {
            selection = selection.spw_selector(spw)?;
        }
        if !flagdata && let Some(scan) = options.scan.as_deref() {
            selection = selection.scan(&parse_numeric_id_selector(scan, "scan")?);
        }
        if !flagdata && let Some(observation) = options.observation.as_deref() {
            selection =
                selection.observation(&parse_numeric_id_selector(observation, "observation")?);
        }
        if !flagdata && let Some(antenna) = options.antenna.as_deref() {
            selection = apply_antenna_selector(ms, selection, antenna)?;
        }
    }
    if grammar == RowGrammar::Transform || flagdata {
        selection = super::row_grammar::apply_array_feed_uv(ms, selection, options, flagdata)?;
    } else if let Some(array) = options.array.as_deref() {
        selection = selection.array(&parse_numeric_id_selector(array, "array")?);
    }
    if let Some(intent) = options.intent.as_deref() {
        let intent = if flagdata {
            super::row_grammar::flagdata_c_string(intent)
        } else {
            intent
        };
        if !flagdata || !intent.is_empty() {
            selection = apply_intent_selector(ms, selection, intent, flagdata)?;
        }
    }
    if let Some(correlation) = options.correlation.as_deref() {
        selection = apply_correlation_selector(ms, selection, correlation)?;
    }
    if grammar != RowGrammar::Transform
        && !flagdata
        && let Some(uvrange) = options.uvrange.as_deref()
    {
        selection = apply_uvrange_selector(ms, selection, uvrange)?;
    }
    if !casa_time_grammar && let Some(timerange) = options.timerange.as_deref() {
        selection = apply_timerange_selector(ms, selection, timerange)?;
    }
    if let Some(msselect) = options.msselect.as_deref() {
        let trimmed = msselect.trim();
        if !trimmed.is_empty() {
            selection = selection.taql(trimmed);
        }
    }
    if casa_time_grammar && let Some(timerange) = options.timerange.as_deref() {
        let timerange = if flagdata {
            super::row_grammar::flagdata_c_string(timerange)
        } else {
            timerange
        };
        let agent_time = grammar == RowGrammar::FlagData { agent_time: true };
        selection =
            super::row_grammar::with_time(ms, selection, timerange, rows_per_block, agent_time)?;
    }

    Ok(selection)
}

/// One FIELD row as the field selector reads it.
/// Row `i` of a slice of these is `FIELD_ID` `i`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldSelectorRow<'a> {
    /// The stored name; the selector compares it trimmed.
    pub name: &'a str,
    /// The stored CODE, used as a fallback when no NAME matches.
    pub code: Option<&'a str>,
    /// Whether the row is flagged: it matches no name, pattern or bound,
    /// following `MSFieldIndex`, though a numeric ID still selects it.
    pub flagged: bool,
}

/// Resolve a field selector over `fields`, a FIELD table's rows in row
/// order, as [`MeasurementSet::resolve_field_selector`] does over its own
/// FIELD table. An observation catalogue states the same rows, so a
/// consumer that holds one resolves fields without the MeasurementSet.
///
/// # Errors
///
/// As [`MeasurementSet::resolve_field_selector`], without its FIELD read.
pub fn resolve_field_selector_rows(
    fields: &[FieldSelectorRow<'_>],
    value: &str,
) -> MsResult<Vec<i32>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let ids = resolve_field_predicate_rows(fields, value)?;
    if ids.is_empty() {
        return Err(MsError::InvalidInput(format!(
            "field selector {value:?} did not match any existing fields"
        )));
    }
    Ok(ids)
}

impl MeasurementSet {
    /// Resolve a field selector using FIELD metadata, without scanning MAIN.
    ///
    /// Comma-separated IDs, inclusive numeric ranges, exact names, shell
    /// patterns, slash-delimited regexes and
    /// strict ID comparisons form a union as in CASA's `MSFieldGram` index list.
    /// The returned IDs are sorted and unique. Empty text returns an empty
    /// list, the selection builder's convention for selecting every field.
    ///
    /// # Errors
    ///
    /// Unreadable FIELD metadata, invalid numeric ranges, a name/pattern matching
    /// no FIELD row, or a nonempty selection resolving to no existing fields.
    /// Slash regexes support positive and negative lookahead. Escapes
    /// outside a delimited pattern are refused.
    /// An out-of-range numeric ID tries its decimal text as a name or code.
    /// Only the FIELD rows themselves bound numeric range enumeration.
    /// Name matching trims stored names and
    /// excludes flagged FIELD rows, following `MSFieldIndex`.
    pub fn resolve_field_selector(&self, value: &str) -> MsResult<Vec<i32>> {
        if value.trim().is_empty() {
            return Ok(Vec::new());
        }
        let rows = field_selector_rows(self)?;
        resolve_field_selector_rows(&borrow_field_rows(&rows), value)
    }
}

/// The FIELD table's metadata in row order.
fn field_selector_rows(ms: &MeasurementSet) -> MsResult<Vec<(String, Option<String>, bool)>> {
    let table = ms
        .subtable(SubtableId::Field)
        .ok_or_else(|| MsError::MissingSubtable("FIELD".to_string()))?;
    let field = MsField::new(table);
    (0..field.row_count())
        .map(|row| {
            Ok((
                field.name(row)?,
                optional_string(table, row, "CODE")?,
                field.flag_row(row)?,
            ))
        })
        .collect()
}

fn borrow_field_rows(rows: &[(String, Option<String>, bool)]) -> Vec<FieldSelectorRow<'_>> {
    rows.iter()
        .map(|(name, code, flagged)| FieldSelectorRow {
            name,
            code: code.as_deref(),
            flagged: *flagged,
        })
        .collect()
}

/// Preserve the difference between no predicate and an explicit empty match.
/// Generic MS consumers allow empty results; imaging's public wrapper refuses them.
fn resolve_field_predicate(ms: &MeasurementSet, value: &str) -> MsResult<Option<Vec<i32>>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let rows = field_selector_rows(ms)?;
    resolve_field_predicate_rows(&borrow_field_rows(&rows), value).map(Some)
}

/// The FIELD rows of `fields` that the nonempty selector `value` names,
/// sorted and unique; possibly none.
fn resolve_field_predicate_rows(
    fields: &[FieldSelectorRow<'_>],
    value: &str,
) -> MsResult<Vec<i32>> {
    let mut ranges = Vec::new();
    let mut ids = Vec::new();
    // MSFieldGram permits a single pair of parentheses around the list.
    let list = if let Some(inside) = value.strip_prefix('(') {
        inside
            .strip_suffix(')')
            .ok_or_else(|| MsError::InvalidInput(format!("unclosed FIELD list {value:?}")))?
    } else {
        value
    };
    let mut delimiter = None;
    let parts = list.split(|character| {
        match delimiter {
            Some(close) if character == close => delimiter = None,
            None if matches!(character, '"' | '/') => delimiter = Some(character),
            _ => {}
        }
        character == ',' && delimiter.is_none()
    });
    for raw_part in parts {
        if raw_part.is_empty() {
            return Err(MsError::InvalidInput(format!(
                "empty term in field selector {value:?}"
            )));
        }
        let term = raw_part.trim_matches([' ', '\t', '\n']);
        if let Some(delimiter @ ('"' | '/')) = term.chars().next() {
            let pattern = field_delimited_pattern(raw_part, delimiter)?;
            ids.extend(resolve_field_pattern(fields, &pattern, delimiter == '/')?);
            continue;
        }
        if term.starts_with(['<', '>']) {
            append_field_comparison(fields, raw_part, &mut ids)?;
            continue;
        }
        if let Some(range) = field_id_range(term)? {
            ranges.push(range);
            continue;
        }

        let name = raw_part.trim_matches(' ');
        if !raw_part.bytes().all(field_name_byte) {
            // SIDENTIFIER has a different alphabet and may consume leading
            // or trailing tabs and newlines. stripWhite then removes spaces
            // only, including when it leaves an empty identifier.
            if (term.is_empty() && !raw_part.contains(' ')) || !term.bytes().all(field_pattern_byte)
            {
                return Err(MsError::InvalidInput(format!(
                    "invalid bare FIELD term {raw_part:?}"
                )));
            }
            ids.extend(resolve_field_pattern(fields, name, false)?);
            continue;
        }
        let matching = field_ids_by_name_or_code(
            fields,
            |value| Ok(value == name),
            |code| Ok(code.trim_matches(' ') == name),
        )?;
        if matching.is_empty() {
            return Err(MsError::VersionError(format!(
                "field selector name {name:?} did not match any FIELD rows"
            )));
        }
        ids.extend(matching);
    }
    append_numeric_fields(fields, ranges, &mut ids);
    Ok(dedup_i32(ids))
}

/// MSFieldGram reads the full IDENTIFIER before SIDENTIFIER on a tie.
fn field_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"_.{}+-=;@#%:! ".contains(&byte)
}

fn field_pattern_byte(byte: u8) -> bool {
    byte != b'.' && (field_name_byte(byte) || b"$'*?".contains(&byte))
}

fn field_delimited_pattern(raw: &str, delimiter: char) -> MsResult<Cow<'_, str>> {
    let invalid = || {
        if delimiter == '"' {
            MsError::InvalidInput(format!(
                "invalid or unterminated quoted field selector {raw:?}"
            ))
        } else {
            MsError::InvalidInput(format!("invalid slash-delimited FIELD regex {raw:?}"))
        }
    };
    let body = raw
        .strip_prefix(delimiter)
        .and_then(|text| text.strip_suffix(delimiter))
        .filter(|text| !text.contains(delimiter))
        .ok_or_else(invalid)?;
    // In QS and RS, the lexer's default rule consumes these characters
    // without appending them to the token.
    if body.contains(['\\', '\n']) {
        Ok(Cow::Owned(body.replace(['\\', '\n'], "")))
    } else {
        Ok(Cow::Borrowed(body))
    }
}

/// Check the unsigned decimal tokens before attempting a FIELD name. A sign
/// is part of an identifier, and an overflowing decimal is an invalid ID.
fn field_id_range(term: &str) -> MsResult<Option<(i32, i32)>> {
    let decimal = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    let (start, end) = if let Some((start, end)) = term.split_once('~') {
        (
            start.trim_end_matches([' ', '\t', '\n']),
            end.trim_start_matches([' ', '\t', '\n']),
        )
    } else if decimal(term) {
        (term, term)
    } else {
        return Ok(None);
    };
    if !decimal(start) || !decimal(end) {
        return Err(MsError::InvalidInput(format!(
            "invalid FIELD ID range {term:?}"
        )));
    }
    let integer = |text: &str| {
        text.parse::<i32>().map_err(|_| {
            MsError::InvalidInput(format!(
                "FIELD ID {text} does not fit in a signed 32-bit integer"
            ))
        })
    };
    let (start, end) = (integer(start)?, integer(end)?);
    if end < start {
        return Err(MsError::InvalidInput(format!(
            "descending FIELD ID range {term:?}"
        )));
    }
    Ok(Some((start, end)))
}

/// MSFieldGram accepts LT INT, GT INT and GT INT AMPERSAND LT INT.
fn append_field_comparison(
    fields: &[FieldSelectorRow<'_>],
    text: &str,
    ids: &mut Vec<i32>,
) -> MsResult<()> {
    let integer = |digits: &str| {
        let digits = digits.trim_matches([' ', '\t', '\n']);
        digits
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| digits.parse::<i32>().ok())
            .flatten()
    };
    let bounds = match text.as_bytes().first() {
        Some(b'<') => integer(&text[1..]).map(|upper| (-1, Some(upper))),
        Some(b'>') => {
            let tail = &text[1..];
            match tail.split_once("&<") {
                Some((low, high)) => integer(low).zip(integer(high)).map(|(l, h)| (l, Some(h))),
                None => integer(tail).map(|lower| (lower, None)),
            }
        }
        _ => None,
    };
    let (lower, upper) = bounds.ok_or_else(|| {
        MsError::InvalidInput(format!("invalid FIELD ID comparison {text:?} (#54)"))
    })?;
    let start = ids.len();
    ids.extend(fields.iter().enumerate().filter_map(|(row, field)| {
        let row = i32::try_from(row).ok()?;
        (!field.flagged && row > lower && upper.is_none_or(|upper| row < upper)).then_some(row)
    }));
    if ids.len() == start {
        return Err(MsError::InvalidInput(format!(
            "FIELD ID comparison {text:?} did not match any unflagged FIELD rows"
        )));
    }
    Ok(())
}

/// MSFieldIndex keeps an in-range ID. For other numbers, it uses the first
/// unflagged decimal NAME, then the first unflagged decimal CODE. Search the
/// stored labels in the requested intervals, so their numeric width does
/// not control how much work the fallback does.
fn append_numeric_fields(
    fields: &[FieldSelectorRow<'_>],
    ranges: Vec<(i32, i32)>,
    ids: &mut Vec<i32>,
) {
    let Some(last) = fields.len().checked_sub(1) else {
        return;
    };
    let last = i32::try_from(last).unwrap_or(i32::MAX);
    let ranges = canonical_numeric_ranges(ranges);
    for &(start, end) in &ranges {
        ids.extend(start..=end.min(last));
    }
    if ranges.last().is_none_or(|&(_, end)| end <= last) {
        return;
    }

    let rows = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.flagged);
    let names = rows
        .clone()
        .map(|(row, field)| (row, field.name.trim_matches(' ')));
    let codes = rows.filter_map(|(row, field)| Some((row, field.code?.trim_matches(' '))));
    let mut aliases = BTreeMap::new();
    for (row, text) in names.chain(codes) {
        if !text.bytes().all(|byte| byte.is_ascii_digit())
            || (text.len() > 1 && text.starts_with('0'))
        {
            continue;
        }
        let Ok(number) = text.parse::<i32>() else {
            continue;
        };
        if number <= last {
            continue;
        }
        let index = ranges.partition_point(|&(_, end)| end < number);
        if ranges.get(index).is_some_and(|&(start, _)| start <= number) {
            aliases.entry(number).or_insert(row as i32);
        }
    }
    ids.extend(aliases.into_values());
}

/// A FIELD pattern matches a complete trimmed NAME or, if none match,
/// a complete CODE as stored.
fn resolve_field_pattern(
    fields: &[FieldSelectorRow<'_>],
    pattern: &str,
    regex: bool,
) -> MsResult<Vec<i32>> {
    let matcher = FieldRegex::compile(pattern, regex).map_err(MsError::InvalidInput)?;
    let matches = |text: &str| {
        matcher.matches(text).map_err(|error| {
            MsError::InvalidInput(format!("matching FIELD regex {pattern:?}: {error}"))
        })
    };
    let ids = field_ids_by_name_or_code(fields, matches, matches)?;
    if ids.is_empty() {
        return Err(MsError::VersionError(format!(
            "field selector pattern {pattern:?} did not match any field names or codes"
        )));
    }
    Ok(ids)
}

fn field_ids_by_name_or_code(
    fields: &[FieldSelectorRow<'_>],
    matches_name: impl Fn(&str) -> MsResult<bool>,
    matches_code: impl Fn(&str) -> MsResult<bool>,
) -> MsResult<Vec<i32>> {
    let rows = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.flagged);
    let mut ids = Vec::new();
    for (row, field) in rows.clone() {
        if matches_name(field.name.trim_matches(' '))? {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        for (row, field) in rows {
            if let Some(code) = field.code
                && matches_code(code)?
            {
                ids.push(row as i32);
            }
        }
    }
    Ok(ids)
}

fn apply_antenna_selector(
    ms: &MeasurementSet,
    mut selection: CompiledMsSelection,
    value: &str,
) -> MsResult<CompiledMsSelection> {
    let mut antenna_ids = Vec::new();
    let mut antenna_names = Vec::new();
    let mut baselines = Vec::new();

    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((left, right)) = part.split_once("&&") {
            baselines.push((
                resolve_antenna_ref(ms, left.trim())?,
                resolve_antenna_ref(ms, right.trim())?,
            ));
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            antenna_ids.push(id);
            continue;
        }
        antenna_names.push(part.to_string());
    }

    if !antenna_ids.is_empty() {
        selection = selection.antenna(&dedup_i32(antenna_ids));
    }
    if !antenna_names.is_empty() {
        let names = antenna_names.iter().map(String::as_str).collect::<Vec<_>>();
        selection = selection.antenna_name(&names);
    }
    if !baselines.is_empty() {
        selection = selection.baseline(&baselines);
    }

    Ok(selection)
}

fn apply_intent_selector(
    ms: &MeasurementSet,
    mut selection: CompiledMsSelection,
    value: &str,
    flagdata: bool,
) -> MsResult<CompiledMsSelection> {
    let state_ids = parse_state_selector_impl(ms, value, flagdata)?;
    if state_ids.is_empty() {
        return Err(MsError::VersionError(format!(
            "intent selector {value:?} did not match any STATE rows"
        )));
    }
    selection = selection.state(&state_ids);
    Ok(selection)
}

fn apply_correlation_selector(
    ms: &MeasurementSet,
    mut selection: CompiledMsSelection,
    value: &str,
) -> MsResult<CompiledMsSelection> {
    let requested_codes = parse_correlation_selector(value)?;
    selection.correlation_types = requested_codes.clone();
    let polarization = ms.polarization()?;
    let data_description = ms.data_description()?;
    let mut matching_ddids = Vec::new();
    for row in 0..data_description.row_count() {
        let pol_id = data_description.polarization_id(row)?;
        if pol_id < 0 {
            continue;
        }
        let corr_types = polarization.corr_type(pol_id as usize)?;
        if requested_codes
            .iter()
            .all(|code| corr_types.iter().any(|candidate| candidate == code))
        {
            matching_ddids.push(row as i32);
        }
    }
    if matching_ddids.is_empty() {
        return Err(MsError::VersionError(format!(
            "correlation selector {value:?} did not match any DATA_DESCRIPTION rows"
        )));
    }
    selection = selection.data_description(&dedup_i32(matching_ddids));
    Ok(selection)
}

fn apply_uvrange_selector(
    ms: &MeasurementSet,
    mut selection: CompiledMsSelection,
    value: &str,
) -> MsResult<CompiledMsSelection> {
    let ranges = parse_uvrange_selector(value)?;
    let taql = build_uvrange_taql(ms, value)?;
    selection.uv_ranges.extend(ranges);
    selection.uv_taql_exprs.push(taql);
    Ok(selection)
}

fn apply_timerange_selector(
    ms: &MeasurementSet,
    mut selection: CompiledMsSelection,
    value: &str,
) -> MsResult<CompiledMsSelection> {
    let taql = build_timerange_taql(ms, &selection, value)?;
    selection = selection.taql(&taql);
    Ok(selection)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TimeSelectionDefaults {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
    exposure_seconds: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct PartialTimeSpec {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ResolvedTimeSpec {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
}

fn build_timerange_taql(
    ms: &MeasurementSet,
    selection: &CompiledMsSelection,
    value: &str,
) -> MsResult<String> {
    let defaults = timerange_defaults(ms, selection)?;
    let clauses = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| build_timerange_clause(part, defaults))
        .collect::<MsResult<Vec<_>>>()?;
    if clauses.is_empty() {
        return Err(MsError::VersionError(
            "timerange selector was empty".to_string(),
        ));
    }
    Ok(format!("({})", clauses.join(" OR ")))
}

fn timerange_defaults(
    ms: &MeasurementSet,
    selection: &CompiledMsSelection,
) -> MsResult<TimeSelectionDefaults> {
    let rows = selection.apply(ms)?;
    let row = rows.first().copied().ok_or_else(|| {
        MsError::VersionError("timerange selection produced an empty row set".to_string())
    })?;
    let table = ms.main_table();
    let time = get_f64(table, row, "TIME")?;
    let exposure_seconds = if has_column(table, "EXPOSURE") {
        get_f64(table, row, "EXPOSURE")?
    } else {
        0.1
    };
    let calendar = MvTime::from_mjd_seconds(time);
    let time_of_day = time.rem_euclid(86_400.0);
    let hour = (time_of_day / 3_600.0).floor() as u32;
    let minute = ((time_of_day / 60.0).floor() as u32) % 60;
    let second = time_of_day - f64::from(hour) * 3_600.0 - f64::from(minute) * 60.0;

    Ok(TimeSelectionDefaults {
        year: calendar.year(),
        month: calendar.month(),
        day: calendar.month_day(),
        hour,
        minute,
        second,
        exposure_seconds,
    })
}

fn build_timerange_clause(value: &str, defaults: TimeSelectionDefaults) -> MsResult<String> {
    if let Some(rest) = value.strip_prefix(">=") {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME >= {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix("<=") {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME <= {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix('>') {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME > {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix('<') {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME < {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some((left, right)) = value.split_once('~') {
        let start = resolve_time_spec(parse_partial_time_spec(left)?, defaults, None)?;
        let end = resolve_time_spec(parse_partial_time_spec(right)?, defaults, Some(start))?;
        if end.as_mjd_seconds() < start.as_mjd_seconds() {
            return Err(MsError::VersionError(format!(
                "timerange selector {value:?} has an end before its start"
            )));
        }
        return Ok(format!(
            "(TIME >= {} AND TIME <= {})",
            format_float_compact(start.as_mjd_seconds(), 6),
            format_float_compact(end.as_mjd_seconds(), 6)
        ));
    }
    if let Some((left, right)) = value.split_once('+') {
        let start = resolve_time_spec(parse_partial_time_spec(left)?, defaults, None)?;
        let duration_seconds = parse_duration_seconds(right)?;
        let end = start.as_mjd_seconds() + duration_seconds;
        return Ok(format!(
            "(TIME >= {} AND TIME <= {})",
            format_float_compact(start.as_mjd_seconds(), 6),
            format_float_compact(end, 6)
        ));
    }

    let center = resolve_time_spec(parse_partial_time_spec(value)?, defaults, None)?;
    let tolerance = defaults.exposure_seconds / 2.0;
    Ok(format!(
        "(ABS(TIME - {}) <= {})",
        format_float_compact(center.as_mjd_seconds(), 6),
        format_float_compact(tolerance, 6)
    ))
}

fn parse_partial_time_spec(value: &str) -> MsResult<PartialTimeSpec> {
    let trimmed = value.trim().trim_matches('\'').trim_matches('"');
    if trimmed.is_empty() {
        return Err(MsError::VersionError(
            "timerange selector contains an empty time expression".to_string(),
        ));
    }

    let mut spec = PartialTimeSpec::default();
    if let Some((date_part, time_part)) = split_date_time_parts(trimmed) {
        parse_date_part(date_part, &mut spec)?;
        if let Some(time_part) = time_part {
            parse_time_part(time_part, &mut spec)?;
        }
        return Ok(spec);
    }

    parse_time_part(trimmed, &mut spec)?;
    Ok(spec)
}

fn split_date_time_parts(value: &str) -> Option<(&str, Option<&str>)> {
    if let Some((date, time)) = value.split_once('T') {
        return Some((date.trim(), Some(time.trim())));
    }
    if let Some((date, time)) = value.split_once(' ')
        && looks_like_date(date.trim())
    {
        return Some((date.trim(), Some(time.trim())));
    }
    let slash_count = value.matches('/').count();
    if slash_count >= 2 && !value.contains(':') {
        return Some((value.trim(), None));
    }
    if slash_count >= 2 {
        let (date, time) = value.rsplit_once('/')?;
        if looks_like_date(date.trim()) {
            return Some((date.trim(), Some(time.trim())));
        }
    }
    if looks_like_date(value) {
        return Some((value.trim(), None));
    }
    None
}

fn looks_like_date(value: &str) -> bool {
    let trimmed = value.trim();
    (trimmed.contains('/') && trimmed.split('/').count() >= 3)
        || (trimmed.contains('-')
            && trimmed.split('-').filter(|part| !part.is_empty()).count() >= 3
            && trimmed
                .chars()
                .any(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit()))
}

fn parse_date_part(value: &str, spec: &mut PartialTimeSpec) -> MsResult<()> {
    let trimmed = value.trim();
    if trimmed.contains('/') {
        let parts = trimmed.split('/').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(MsError::VersionError(format!(
                "timerange date {value:?} is not a supported yyyy/mm/dd value"
            )));
        }
        spec.year = Some(parse_i32_component(parts[0], "year", value)?);
        spec.month = Some(parse_u32_component(parts[1], "month", value)?);
        spec.day = Some(parse_u32_component(parts[2], "day", value)?);
        return Ok(());
    }

    let normalized = trimmed.replace(' ', "-");
    let parts = normalized.split('-').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(MsError::VersionError(format!(
            "timerange date {value:?} is not a supported date expression"
        )));
    }
    if parts[1].chars().all(|ch| ch.is_ascii_digit()) {
        spec.year = Some(parse_i32_component(parts[0], "year", value)?);
        spec.month = Some(parse_u32_component(parts[1], "month", value)?);
        spec.day = Some(parse_u32_component(parts[2], "day", value)?);
    } else {
        spec.day = Some(parse_u32_component(parts[0], "day", value)?);
        spec.month = Some(parse_month_component(parts[1], value)?);
        spec.year = Some(parse_two_or_four_digit_year(parts[2], value)?);
    }
    Ok(())
}

fn parse_time_part(value: &str, spec: &mut PartialTimeSpec) -> MsResult<()> {
    let trimmed = value.trim();
    let parts = trimmed.split(':').map(str::trim).collect::<Vec<_>>();
    match parts.len() {
        2 => {
            spec.hour = Some(parse_u32_component(parts[0], "hour", value)?);
            spec.minute = Some(parse_u32_component(parts[1], "minute", value)?);
        }
        3 => {
            spec.hour = Some(parse_u32_component(parts[0], "hour", value)?);
            spec.minute = Some(parse_u32_component(parts[1], "minute", value)?);
            spec.second = Some(parse_f64_component(parts[2], "second", value)?);
        }
        _ => {
            return Err(MsError::VersionError(format!(
                "timerange time {value:?} is not a supported hh:mm[:ss] value"
            )));
        }
    }
    Ok(())
}

fn parse_duration_seconds(value: &str) -> MsResult<f64> {
    let trimmed = value.trim().trim_matches('\'').trim_matches('"');
    if trimmed.is_empty() {
        return Err(MsError::VersionError(
            "timerange interval is empty".to_string(),
        ));
    }
    if trimmed.contains(':') {
        let parts = trimmed.split(':').map(str::trim).collect::<Vec<_>>();
        let seconds = match parts.len() {
            2 => {
                let hours = parse_u32_component(parts[0], "hour", value)?;
                let minutes = parse_u32_component(parts[1], "minute", value)?;
                f64::from(hours) * 3_600.0 + f64::from(minutes) * 60.0
            }
            3 => {
                let hours = parse_u32_component(parts[0], "hour", value)?;
                let minutes = parse_u32_component(parts[1], "minute", value)?;
                let seconds = parse_f64_component(parts[2], "second", value)?;
                f64::from(hours) * 3_600.0 + f64::from(minutes) * 60.0 + seconds
            }
            _ => {
                return Err(MsError::VersionError(format!(
                    "timerange interval {value:?} is not a supported hh:mm[:ss] value"
                )));
            }
        };
        return Ok(seconds);
    }

    trimmed.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange interval {value:?} is not a supported numeric duration"
        ))
    })
}

fn resolve_time_spec(
    spec: PartialTimeSpec,
    defaults: TimeSelectionDefaults,
    reference: Option<ResolvedTimeSpec>,
) -> MsResult<ResolvedTimeSpec> {
    let reference = reference.unwrap_or(ResolvedTimeSpec {
        year: defaults.year,
        month: defaults.month,
        day: defaults.day,
        hour: defaults.hour,
        minute: defaults.minute,
        second: defaults.second,
    });
    let resolved = ResolvedTimeSpec {
        year: spec.year.unwrap_or(reference.year),
        month: spec.month.unwrap_or(reference.month),
        day: spec.day.unwrap_or(reference.day),
        hour: spec.hour.unwrap_or(reference.hour),
        minute: spec.minute.unwrap_or(reference.minute),
        second: spec.second.unwrap_or(reference.second),
    };
    if MvTime::from_ymd_hms_utc(
        resolved.year,
        resolved.month,
        resolved.day,
        resolved.hour,
        resolved.minute,
        resolved.second,
    )
    .is_none()
    {
        return Err(MsError::VersionError(format!(
            "timerange resolved to an invalid UTC timestamp: {:04}-{:02}-{:02} {:02}:{:02}:{:06.3}",
            resolved.year,
            resolved.month,
            resolved.day,
            resolved.hour,
            resolved.minute,
            resolved.second
        )));
    }
    Ok(resolved)
}

impl ResolvedTimeSpec {
    fn as_mjd_seconds(self) -> f64 {
        MvTime::from_ymd_hms_utc(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
        )
        .expect("validated time")
        .as_mjd_seconds()
    }
}

fn parse_i32_component(value: &str, label: &str, original: &str) -> MsResult<i32> {
    value.parse::<i32>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid integer"
        ))
    })
}

fn parse_u32_component(value: &str, label: &str, original: &str) -> MsResult<u32> {
    value.parse::<u32>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid integer"
        ))
    })
}

fn parse_f64_component(value: &str, label: &str, original: &str) -> MsResult<f64> {
    value.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid number"
        ))
    })
}

fn parse_month_component(value: &str, original: &str) -> MsResult<u32> {
    match value.to_ascii_lowercase().as_str() {
        "jan" | "january" => Ok(1),
        "feb" | "february" => Ok(2),
        "mar" | "march" => Ok(3),
        "apr" | "april" => Ok(4),
        "may" => Ok(5),
        "jun" | "june" => Ok(6),
        "jul" | "july" => Ok(7),
        "aug" | "august" => Ok(8),
        "sep" | "sept" | "september" => Ok(9),
        "oct" | "october" => Ok(10),
        "nov" | "november" => Ok(11),
        "dec" | "december" => Ok(12),
        _ => Err(MsError::VersionError(format!(
            "timerange month in {original:?} is not a supported month name"
        ))),
    }
}

fn parse_two_or_four_digit_year(value: &str, original: &str) -> MsResult<i32> {
    let year = parse_i32_component(value, "year", original)?;
    Ok(if value.len() < 3 { 2000 + year } else { year })
}

pub(crate) fn parse_state_selector(ms: &MeasurementSet, value: &str) -> MsResult<Vec<i32>> {
    parse_state_selector_impl(ms, value, false)
}

fn parse_state_selector_impl(
    ms: &MeasurementSet,
    value: &str,
    flagdata: bool,
) -> MsResult<Vec<i32>> {
    let state = ms.state()?;
    let modes = (0..state.row_count())
        .map(|row| Ok((state.string(row, "OBS_MODE")?, state.bool(row, "FLAG_ROW")?)))
        .collect::<MsResult<Vec<_>>>()?;
    super::intent::resolve(value, &modes, flagdata)
}

pub(crate) fn parse_correlation_selector(value: &str) -> MsResult<Vec<i32>> {
    let mut codes = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        let normalized = part
            .trim_matches('\'')
            .trim_matches('"')
            .to_ascii_uppercase();
        let Some(code) = stokes_code(&normalized) else {
            return Err(MsError::VersionError(format!(
                "correlation selector {part:?} is not a supported Stokes/correlation name"
            )));
        };
        codes.push(code);
    }
    if codes.is_empty() {
        return Err(MsError::VersionError(
            "correlation selector was empty".to_string(),
        ));
    }
    Ok(dedup_i32(codes))
}

fn build_uvrange_taql(ms: &MeasurementSet, value: &str) -> MsResult<String> {
    let ranges = parse_uvrange_selector(value)?;
    let ddid_lambda_map = ddid_to_lambda_map(ms)?;
    let clauses = ranges
        .iter()
        .map(|range| uvrange_clause(range, &ddid_lambda_map))
        .collect::<MsResult<Vec<_>>>()?;
    if clauses.is_empty() {
        return Err(MsError::VersionError(
            "uvrange selector was empty".to_string(),
        ));
    }
    Ok(format!("({})", clauses.join(" OR ")))
}

pub(crate) fn parse_uvrange_selector(value: &str) -> MsResult<Vec<UvSelectionRange>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(parse_uvrange_part)
        .collect()
}

fn parse_uvrange_part(value: &str) -> MsResult<UvSelectionRange> {
    if let Some(rest) = value.strip_prefix(">=") {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: parsed,
                op: UvBoundOp::GreaterEqual,
            }),
            upper: None,
            unit: unit.unwrap_or(UvUnit::Meters(1.0)),
        });
    }
    if let Some(rest) = value.strip_prefix("<=") {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: None,
            upper: Some(UvBound {
                value: parsed,
                op: UvBoundOp::LessEqual,
            }),
            unit: unit.unwrap_or(UvUnit::Meters(1.0)),
        });
    }
    if let Some(rest) = value.strip_prefix('>') {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: parsed,
                op: UvBoundOp::Greater,
            }),
            upper: None,
            unit: unit.unwrap_or(UvUnit::Meters(1.0)),
        });
    }
    if let Some(rest) = value.strip_prefix('<') {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: None,
            upper: Some(UvBound {
                value: parsed,
                op: UvBoundOp::Less,
            }),
            unit: unit.unwrap_or(UvUnit::Meters(1.0)),
        });
    }
    if let Some((start_raw, end_raw)) = value.split_once('~') {
        let (start, start_unit) = parse_uv_bound_value(start_raw)?;
        let (end, end_unit) = parse_uv_bound_value(end_raw)?;
        let unit = merge_uv_units(start_unit, end_unit)?;
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: start,
                op: UvBoundOp::GreaterEqual,
            }),
            upper: Some(UvBound {
                value: end,
                op: UvBoundOp::LessEqual,
            }),
            unit,
        });
    }

    let (parsed, unit) = parse_uv_bound_value(value)?;
    Ok(UvSelectionRange {
        lower: Some(UvBound {
            value: parsed,
            op: UvBoundOp::GreaterEqual,
        }),
        upper: Some(UvBound {
            value: parsed,
            op: UvBoundOp::LessEqual,
        }),
        unit: unit.unwrap_or(UvUnit::Meters(1.0)),
    })
}

fn parse_uv_bound_value(value: &str) -> MsResult<(f64, Option<UvUnit>)> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.' || ch == '+' || ch == '-'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(MsError::VersionError(format!(
            "uvrange selector {value:?} is missing a numeric bound"
        )));
    }
    let parsed = number.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "uvrange selector {value:?} has an invalid numeric bound"
        ))
    })?;
    let unit = if suffix.trim().is_empty() {
        None
    } else {
        Some(parse_uv_unit(suffix.trim())?)
    };
    Ok((parsed, unit))
}

fn parse_uv_unit(value: &str) -> MsResult<UvUnit> {
    match value.to_ascii_lowercase().as_str() {
        "m" => Ok(UvUnit::Meters(1.0)),
        "km" => Ok(UvUnit::Meters(1_000.0)),
        "lambda" => Ok(UvUnit::Lambda(1.0)),
        "klambda" => Ok(UvUnit::Lambda(1_000.0)),
        "mlambda" => Ok(UvUnit::Lambda(1_000_000.0)),
        "glambda" => Ok(UvUnit::Lambda(1_000_000_000.0)),
        other => Err(MsError::VersionError(format!(
            "uvrange unit {other:?} is not supported; use m, km, lambda, klambda, mlambda, or glambda"
        ))),
    }
}

fn merge_uv_units(left: Option<UvUnit>, right: Option<UvUnit>) -> MsResult<UvUnit> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(MsError::VersionError(
            "uvrange bounds must use the same unit".to_string(),
        )),
        (Some(unit), _) | (_, Some(unit)) => Ok(unit),
        (None, None) => Ok(UvUnit::Meters(1.0)),
    }
}

pub(super) fn ddid_to_lambda_map(ms: &MeasurementSet) -> MsResult<Vec<(i32, f64)>> {
    let dd = ms.data_description()?;
    let spw = ms.spectral_window()?;
    let mut mapping = Vec::new();
    for row in 0..dd.row_count() {
        let spw_id = dd.spectral_window_id(row)?;
        if spw_id < 0 {
            continue;
        }
        let ref_frequency_hz = spw.ref_frequency(spw_id as usize)?;
        if ref_frequency_hz <= 0.0 {
            continue;
        }
        mapping.push((row as i32, 299_792_458.0 / ref_frequency_hz));
    }
    Ok(mapping)
}

fn uvrange_clause(range: &UvSelectionRange, ddid_lambda_map: &[(i32, f64)]) -> MsResult<String> {
    match range.unit {
        UvUnit::Meters(scale) => Ok(build_uvdist_condition(range, Some(scale))),
        UvUnit::Lambda(scale) => {
            let mut clauses = Vec::new();
            for &(ddid, lambda_m) in ddid_lambda_map {
                let scaled = lambda_m * scale;
                clauses.push(format!(
                    "(DATA_DESC_ID=={ddid} AND {})",
                    build_uvdist_condition(range, Some(scaled))
                ));
            }
            if clauses.is_empty() {
                return Err(MsError::VersionError(
                    "uvrange lambda selection could not resolve any DATA_DESCRIPTION rows"
                        .to_string(),
                ));
            }
            Ok(format!("({})", clauses.join(" OR ")))
        }
    }
}

fn build_uvdist_condition(range: &UvSelectionRange, lambda_m: Option<f64>) -> String {
    let distance_expr = "SQUARE(UVW[1]) + SQUARE(UVW[2])";
    let mut terms = Vec::new();
    if let Some(lower) = range.lower {
        let bound = scale_uv_bound(lower.value, lambda_m);
        let op = match lower.op {
            UvBoundOp::Greater => ">",
            UvBoundOp::GreaterEqual => ">=",
            UvBoundOp::Less => "<",
            UvBoundOp::LessEqual => "<=",
        };
        terms.push(format!(
            "{distance_expr} {op} {}",
            format_float_compact(bound, 6)
        ));
    }
    if let Some(upper) = range.upper {
        let bound = scale_uv_bound(upper.value, lambda_m);
        let op = match upper.op {
            UvBoundOp::Greater => ">",
            UvBoundOp::GreaterEqual => ">=",
            UvBoundOp::Less => "<",
            UvBoundOp::LessEqual => "<=",
        };
        terms.push(format!(
            "{distance_expr} {op} {}",
            format_float_compact(bound, 6)
        ));
    }
    format!("({})", terms.join(" AND "))
}

fn scale_uv_bound(value: f64, lambda_m: Option<f64>) -> f64 {
    let meters = lambda_m.map(|scale| value * scale).unwrap_or(value);
    meters * meters
}

fn resolve_antenna_ref(ms: &MeasurementSet, value: &str) -> MsResult<i32> {
    if let Ok(id) = value.parse::<i32>() {
        return Ok(id);
    }
    let antenna = ms.antenna()?;
    for row in 0..antenna.row_count() {
        if antenna.name(row)? == value {
            return Ok(row as i32);
        }
    }
    Err(MsError::VersionError(format!(
        "antenna selector {value:?} did not match any ANTENNA rows"
    )))
}

pub(super) fn matches_simple_glob(pattern: &str, candidate: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == candidate;
    }
    let mut rest = candidate;
    let parts = pattern.split('*').collect::<Vec<_>>();
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');

    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 && anchored_start {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            continue;
        }
        if index == parts.len() - 1 && anchored_end {
            return rest.ends_with(part);
        }
        let Some(position) = rest.find(part) else {
            return false;
        };
        rest = &rest[position + part.len()..];
    }

    true
}

fn stokes_code(name: &str) -> Option<i32> {
    match name {
        "I" => Some(1),
        "Q" => Some(2),
        "U" => Some(3),
        "V" => Some(4),
        "RR" => Some(5),
        "RL" => Some(6),
        "LR" => Some(7),
        "LL" => Some(8),
        "XX" => Some(9),
        "XY" => Some(10),
        "YX" => Some(11),
        "YY" => Some(12),
        _ => None,
    }
}
