// SPDX-License-Identifier: LGPL-3.0-or-later
//! Route current-format calibration tables through the native flagging operations.

use crate::selection::spw_expr::{
    DataDescriptionRow, SpectralWindowRow, SpwSelection, resolve_spw_selector,
};
use crate::selection::syntax::parse_numeric_id_selector;
use crate::selection::time_expr::{TimeDefaults, TimeSelector, TimeSelectorDefaults};

use super::*;

pub(super) fn run(
    table: &mut MeasurementSet,
    request: &FlagDataRequest,
    persist: bool,
) -> Result<FlagDataReport, FlaggingError> {
    let path = display_ms_path(table);
    let backup_version = (persist && request.flagbackup && request.mode != FlagDataMode::Summary)
        .then(|| save_next_casa_flag_backup(table, "flagdata"))
        .transpose()?;
    let selected = select(table, request)?;
    let correlation = request.correlation.as_deref().unwrap_or_default();
    if request.mode == FlagDataMode::Summary {
        if !correlation.is_empty() {
            return Err(FlaggingError::MissingCorrelationSummary);
        }
        return summary::report(
            table,
            &selected.rows,
            selected.channels.as_ref(),
            None,
            request.summary_options,
        );
    }
    if !matches!(request.mode, FlagDataMode::Manual | FlagDataMode::Clip) {
        return Err(FlaggingError::MutateFlags {
            path,
            reason: format!(
                "calibration-table {:?} is unfinished under #1527",
                request.mode
            ),
        });
    }
    if request.mode == FlagDataMode::Clip {
        check_clip_request(table, request)?;
    }
    let mut changes = ChangeSet::default();
    let mut report = FlagDataReport {
        mode: request.mode,
        selected_rows: selected.rows.len(),
        changed_rows: 0,
        changed_samples: 0,
        flagged_samples: 0,
        total_samples: 0,
        backup_version,
        timedev: None,
        freqdev: None,
        summary: None,
    };
    for &row in &selected.rows {
        let mut flags = clone_flag_matrix(table, row)?;
        let changed = edit_row(table, request, &selected, row, &path, &mut flags)?;
        report.flagged_samples += flags.iter().filter(|&&value| value).count();
        report.total_samples += flags.len();
        if changed > 0 {
            changes.changed_row_indices.push(row);
            report.changed_samples += changed;
            if persist {
                table
                    .main_table_mut()
                    .cell_accessor_mut(row, FLAG_COLUMN)
                    .and_then(|mut cell| cell.set(Value::Array(ArrayValue::Bool(flags))))
                    .map_err(|error| flag_error(&path, error.to_string()))?;
            }
        }
    }
    report.changed_rows = changes.changed_row_indices.len();
    if persist && report.changed_rows > 0 {
        persist_changed_flags(table, &changes, &path)?;
    }
    Ok(report)
}

fn check_clip_request(
    table: &MeasurementSet,
    request: &FlagDataRequest,
) -> Result<(), FlaggingError> {
    if !matches!(
        request.data_column,
        FlagDataColumn::Cparam
            | FlagDataColumn::Fparam
            | FlagDataColumn::ParamErr
            | FlagDataColumn::Snr
    ) || !table
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(request.data_column.name()))
    {
        return Err(FlaggingError::MissingClipColumn);
    }
    if request.clip_channel_bins.is_some() || request.clip_time_bin.is_some() {
        return Err(flag_error(
            &display_ms_path(table),
            "calibration-table clip averaging is unfinished under #1527",
        ));
    }
    Ok(())
}

fn edit_row(
    table: &MeasurementSet,
    request: &FlagDataRequest,
    selected: &SelectedFlagRows,
    row: usize,
    path: &str,
    flags: &mut ArrayD<bool>,
) -> Result<usize, FlaggingError> {
    let index = |name| table_i32(table.main_table(), row, name, table.path());
    let &[receptors, count] = flags.shape() else {
        return Err(flag_error(path, format!("FLAG row {row} must be rank-2")));
    };
    let window = index("SPECTRAL_WINDOW_ID")?;
    let channels = selected
        .channels
        .as_ref()
        .and_then(|windows| windows.get(&window))
        .map(|range| range.indices(count))
        .transpose()
        .map_err(selection_error)?;
    if request.mode == FlagDataMode::Manual {
        if request
            .correlation
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        {
            return Ok(0);
        }
        if request.autocorr && request.action == FlagDataAction::Flag {
            let antenna2 = index("ANTENNA2")?;
            if antenna2 >= 0 && index("ANTENNA1")? != antenna2 {
                return Ok(0);
            }
        }
        let value = request.action == FlagDataAction::Flag;
        let channels = channels.unwrap_or_else(|| (0..count).collect());
        let mut changed = 0;
        for receptor in 0..receptors {
            for &channel in &channels {
                let flag = &mut flags[[receptor, channel]];
                changed += usize::from(*flag != value);
                *flag = value;
            }
        }
        return Ok(changed);
    }
    let data = table
        .main_table()
        .column_accessor(request.data_column.name())
        .and_then(|column| column.array_cell(row))
        .map_err(|error| flag_error(path, error.to_string()))?;
    let mut changed = Vec::new();
    apply_clip_array(
        data,
        flags,
        channels.as_deref(),
        ClipOptions {
            zeros: request.clipzeros,
            bounds: request.clipminmax,
            outside: request.clipoutside.unwrap_or(true),
            projections: None,
            metric: clip_metric(request.correlation.as_deref())?,
        },
        path,
        row,
        &mut changed,
    )?;
    for &(receptor, channel) in &changed {
        flags[[receptor, channel]] = true;
    }
    Ok(changed.len())
}

fn clip_metric(value: Option<&str>) -> Result<Option<correlation::ClipMetric>, FlaggingError> {
    use correlation::ClipMetric;
    let metric = match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "" | "ABS_ALL" | "ABS" | "ALL" => ClipMetric::Amplitude,
        "REAL_ALL" => ClipMetric::Real,
        "IMAG_ALL" => ClipMetric::Imaginary,
        "ARG_ALL" => ClipMetric::Phase,
        "NORM_ALL" => ClipMetric::SquaredMagnitude,
        _ => return Err(FlaggingError::MissingClipColumn),
    };
    Ok(Some(metric))
}

struct SelectedFlagRows {
    rows: Vec<usize>,
    channels: Option<BTreeMap<i32, ChannelSelection>>,
}

fn select(
    table: &MeasurementSet,
    request: &FlagDataRequest,
) -> Result<SelectedFlagRows, FlaggingError> {
    let input = &request.selection;
    if [
        input.array.as_deref(),
        input.uvrange.as_deref(),
        input.intent.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| !value.is_empty())
    {
        return Err(selection_error(
            "MSSelection::toTableExprNode(MSSelectableTable*): Only field-, spw-, scan-, time- and antenna-selection is supported for CalTables",
        ));
    }
    let field = table
        .resolve_field_selector(input.field.as_deref().unwrap_or_default())
        .map_err(selection_error)?;
    let antenna = table
        .resolve_antenna_selector(input.antenna.as_deref().unwrap_or_default())
        .map_err(antenna_selection_error)?;
    let scan = parse_numeric_id_selector(input.scan.as_deref().unwrap_or_default(), "scan")
        .map_err(selection_error)?;
    let observation = parse_numeric_id_selector(
        input.observation.as_deref().unwrap_or_default(),
        "observation",
    )
    .map_err(selection_error)?;
    let spw = spectral_selection(table, input.spw.as_deref().unwrap_or_default())?;
    let mut rows = Vec::new();
    let main = table.main_table();
    let path = table.path();
    for row in 0..main.row_count() {
        let id = |column| table_i32(main, row, column, path);
        let includes = |values: &[i32], column| -> Result<bool, FlaggingError> {
            Ok(values.is_empty() || values.contains(&id(column)?))
        };
        let antenna_id = |column| id(column).map(|id| usize::try_from(id).unwrap_or(usize::MAX));
        if includes(&field, "FIELD_ID")?
            && includes(&scan, "SCAN_NUMBER")?
            && includes(&observation, "OBSERVATION_ID")?
            && (spw.selects_all()
                || usize::try_from(id("SPECTRAL_WINDOW_ID")?)
                    .is_ok_and(|id| spw.spw_ids().contains(&id)))
            && (antenna.is_unconstrained()
                || antenna.holds(antenna_id("ANTENNA1")?, antenna_id("ANTENNA2")?))
        {
            rows.push(row);
        }
    }
    if let Some(text) = input.timerange.as_deref().filter(|text| !text.is_empty())
        && let Some(&first) = rows.first()
    {
        let time = |row| table_f64(main, row, "TIME", path);
        let defaults = |row| -> Result<TimeDefaults, FlaggingError> {
            Ok(TimeDefaults {
                time_mjd_s: time(row)?,
                exposure_s: 0.1,
            })
        };
        let selection = TimeSelector::parse(text)
            .map_err(selection_error)?
            .resolve(TimeSelectorDefaults {
                first_term: defaults(first)?,
                later_terms: defaults(0)?,
            })
            .map_err(selection_error)?;
        let mut selected = Vec::with_capacity(rows.len());
        for row in rows {
            if selection.holds(time(row)?) {
                selected.push(row);
            }
        }
        rows = selected;
    }
    if rows.is_empty() && main.row_count() > 0 {
        return Err(selection_error(
            "MSSelectionNullSelection : The selected table has zero rows.",
        ));
    }
    let channels = if spw.selects_all() {
        None
    } else {
        Some(
            crate::selection::flagdata_channel_ranges(table, &spw, false)
                .map_err(selection_error)?,
        )
    };
    Ok(SelectedFlagRows { rows, channels })
}

fn spectral_selection(
    table: &MeasurementSet,
    expression: &str,
) -> Result<SpwSelection, FlaggingError> {
    if expression.is_empty() {
        return Ok(SpwSelection::all());
    }
    let metadata = table.spectral_window().map_err(selection_error)?;
    let storage = (0..metadata.row_count())
        .map(|row| {
            Ok::<_, MsError>((
                metadata.name(row)?,
                metadata.chan_freq(row)?,
                metadata.chan_width(row)?,
                metadata.ref_frequency(row)?,
                metadata.total_bandwidth(row)?,
                metadata.flag_row(row)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(selection_error)?;
    let windows = storage
        .iter()
        .map(
            |(name, frequencies, widths, reference, bandwidth, flagged)| SpectralWindowRow {
                name,
                chan_freq_hz: frequencies,
                chan_width_hz: widths,
                ref_frequency_hz: *reference,
                total_bandwidth_hz: *bandwidth,
                flag_row: *flagged,
            },
        )
        .collect::<Vec<_>>();
    let descriptions = (0..windows.len())
        .map(|id| DataDescriptionRow {
            spectral_window_id: id as i32,
            flag_row: false,
        })
        .collect::<Vec<_>>();
    resolve_spw_selector(&windows, &descriptions, expression).map_err(selection_error)
}

fn selection_error(reason: impl std::fmt::Display) -> FlaggingError {
    FlaggingError::CalibrationSelection(reason.to_string())
}

fn antenna_selection_error(error: MsError) -> FlaggingError {
    if let MsError::InvalidInput(detail) = &error
        && let Some((_, message)) = detail.split_once(": Antenna Expression: ")
        && message.starts_with("No match found for token(s)")
    {
        return selection_error(format!("Antenna Expression: {message}  "));
    }
    selection_error(error)
}

fn flag_error(path: &str, reason: impl Into<String>) -> FlaggingError {
    FlaggingError::MutateFlags {
        path: path.to_string(),
        reason: reason.into(),
    }
}
