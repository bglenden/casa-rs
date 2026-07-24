// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal selector parsing and TaQL construction for listobs-style selection.

use std::collections::BTreeSet;

use casa_types::quanta::MvTime;

use crate::selection::syntax::{dedup_i32, parse_numeric_id_selector, parse_numeric_range};
use crate::selection::{CompiledMsSelection, UvBound, UvBoundOp, UvSelectionRange, UvUnit};
use crate::subtables::{get_f64, has_column};
use crate::{MeasurementSet, MsError, MsResult};

use crate::listobs::{ListObsOptions, format_float_compact};

pub(crate) fn selection_from_options(
    ms: &MeasurementSet,
    options: &ListObsOptions,
) -> MsResult<CompiledMsSelection> {
    let mut selection = CompiledMsSelection::new();

    if let Some(field) = options.field.as_deref() {
        let (field_ids, field_names) = parse_field_selector(ms, field)?;
        if !field_ids.is_empty() {
            selection = selection.field(&field_ids);
        }
        if !field_names.is_empty() {
            let names = field_names.iter().map(String::as_str).collect::<Vec<_>>();
            let resolved_ids = resolve_field_names(ms, &names)?;
            selection = selection.field(&resolved_ids);
        }
    }

    if let Some(spw) = options.spw.as_deref() {
        selection = selection.spw_selector(spw)?;
    }
    if let Some(scan) = options.scan.as_deref() {
        selection = selection.scan(&parse_numeric_id_selector(scan, "scan")?);
    }
    if let Some(observation) = options.observation.as_deref() {
        selection = selection.observation(&parse_numeric_id_selector(observation, "observation")?);
    }
    if let Some(array) = options.array.as_deref() {
        selection = selection.array(&parse_numeric_id_selector(array, "array")?);
    }
    if let Some(antenna) = options.antenna.as_deref() {
        selection = apply_antenna_selector(ms, selection, antenna)?;
    }
    if let Some(intent) = options.intent.as_deref() {
        selection = apply_intent_selector(ms, selection, intent)?;
    }
    if let Some(correlation) = options.correlation.as_deref() {
        selection = apply_correlation_selector(ms, selection, correlation)?;
    }
    if let Some(uvrange) = options.uvrange.as_deref() {
        selection = apply_uvrange_selector(ms, selection, uvrange)?;
    }
    if let Some(timerange) = options.timerange.as_deref() {
        selection = apply_timerange_selector(ms, selection, timerange)?;
    }
    if let Some(msselect) = options.msselect.as_deref() {
        let trimmed = msselect.trim();
        if !trimmed.is_empty() {
            selection = selection.taql(trimmed);
        }
    }

    Ok(selection)
}

fn parse_field_selector(ms: &MeasurementSet, value: &str) -> MsResult<(Vec<i32>, Vec<String>)> {
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if part.contains('*') {
            let matched = collect_matching_field_names(ms, part)?;
            if matched.is_empty() {
                return Err(MsError::VersionError(format!(
                    "field selector {part:?} did not match any field names"
                )));
            }
            names.extend(matched);
            continue;
        }
        if let Some((start, end)) = parse_numeric_range(part) {
            ids.extend(start..=end);
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            ids.push(id);
            continue;
        }
        names.push(part.to_string());
    }
    Ok((dedup_i32(ids), dedup_strings(names)))
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
) -> MsResult<CompiledMsSelection> {
    let state_ids = parse_state_selector(ms, value)?;
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
    if let Some((date, time)) = value.split_once(' ') {
        if looks_like_date(date.trim()) {
            return Some((date.trim(), Some(time.trim())));
        }
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

fn resolve_field_names(ms: &MeasurementSet, names: &[&str]) -> MsResult<Vec<i32>> {
    let field = ms.field()?;
    let mut ids = Vec::new();
    for row in 0..field.row_count() {
        let name = field.name(row)?;
        if names.iter().any(|candidate| candidate == &name) {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::VersionError(format!(
            "field selector names {names:?} did not match any FIELD rows"
        )));
    }
    Ok(dedup_i32(ids))
}

fn collect_matching_field_names(ms: &MeasurementSet, pattern: &str) -> MsResult<Vec<String>> {
    let field = ms.field()?;
    let mut matches = Vec::new();
    for row in 0..field.row_count() {
        let name = field.name(row)?;
        if matches_simple_glob(pattern, &name) {
            matches.push(name);
        }
    }
    Ok(dedup_strings(matches))
}

fn parse_state_selector(ms: &MeasurementSet, value: &str) -> MsResult<Vec<i32>> {
    let state = ms.state()?;
    let mut ids = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = parse_numeric_range(part) {
            ids.extend(start..=end);
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            ids.push(id);
            continue;
        }
        let mut matched = false;
        for row in 0..state.row_count() {
            let obs_mode = state.string(row, "OBS_MODE")?;
            let is_match = if part.contains('*') {
                matches_simple_glob(part, &obs_mode)
            } else {
                obs_mode == part
            };
            if is_match {
                ids.push(row as i32);
                matched = true;
            }
        }
        if !matched {
            return Err(MsError::VersionError(format!(
                "intent selector {part:?} did not match any STATE rows"
            )));
        }
    }
    Ok(dedup_i32(ids))
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

fn parse_uvrange_selector(value: &str) -> MsResult<Vec<UvSelectionRange>> {
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

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn matches_simple_glob(pattern: &str, candidate: &str) -> bool {
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
