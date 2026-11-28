// SPDX-License-Identifier: LGPL-3.0-or-later
//! Route current-format calibration tables through the native flagging operations.

use std::borrow::Cow;

use casa_ms::selection::spw_expr::{
    DataDescriptionRow, SpectralWindowRow, SpwSelection, resolve_spw_selector,
};
use casa_ms::selection::syntax::parse_numeric_row_id_ranges;
use casa_ms::selection::time_expr::{TimeDefaults, TimeSelector, TimeSelectorDefaults};

use super::*;

pub(super) fn run(
    table: &mut MeasurementSet,
    request: &FlagDataRequest,
    persist: bool,
) -> Result<FlagDataReport, FlaggingError> {
    let path = display_ms_path(table);
    request.check_cancelled()?;
    let backup_version = (persist && request.flagbackup && request.mode != FlagDataMode::Summary)
        .then(|| save_next_casa_flag_backup(table, "flagdata"))
        .transpose()?;
    let (mut report, changes) = stage(table, request, !persist, false, None)?;
    request.check_cancelled()?;
    if persist && !changes.changed_row_indices.is_empty() {
        persist_changed_flags(table, &changes, &path, request.cancellation.as_ref())?;
    }
    report.backup_version = backup_version;
    Ok(report)
}

pub(super) struct ProgramOptions<'a> {
    pub(super) write_flags: bool,
    pub(super) sequential: bool,
    pub(super) local_selection: bool,
    pub(super) selected_rows: Option<&'a [usize]>,
}

pub(super) fn run_program(
    table: &mut MeasurementSet,
    requests: &[FlagDataRequest],
    options: ProgramOptions<'_>,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    let cancellation = requests
        .iter()
        .find_map(|request| request.cancellation.as_ref());
    let mut execution_order: Vec<_> = (0..requests.len()).collect();
    if !options.sequential {
        // In a combined CASA run the unflag agents clear their samples
        // before the other agents observe the shared buffer.
        execution_order.sort_by_key(|&index| requests[index].action != FlagDataAction::Unflag);
    }
    let mut reports: Vec<_> = (0..requests.len()).map(|_| None).collect();
    let mut touched = std::collections::BTreeSet::new();
    for index in execution_order {
        check_cancelled(cancellation)?;
        reports[index] = match stage(
            table,
            &requests[index],
            !options.write_flags,
            options.local_selection,
            options.selected_rows,
        ) {
            Ok((report, changes)) => {
                touched.extend(changes.changed_row_indices);
                Some(report)
            }
            Err(
                FlaggingError::UnsupportedCalibrationAgent(_)
                | FlaggingError::MissingClipColumn
                | FlaggingError::MissingTfcropColumn
                | FlaggingError::MissingCorrelationSummary,
            ) => None,
            Err(error) => return Err(error),
        };
    }
    check_cancelled(cancellation)?;
    if options.write_flags && !touched.is_empty() {
        let path = display_ms_path(table);
        let changes = ChangeSet {
            changed_row_indices: touched.into_iter().collect(),
            ..ChangeSet::default()
        };
        persist_changed_flags(table, &changes, &path, cancellation)?;
    }
    Ok(reports)
}

fn stage(
    table: &mut MeasurementSet,
    request: &FlagDataRequest,
    calculate: bool,
    local_selection: bool,
    base_rows: Option<&[usize]>,
) -> Result<(FlagDataReport, ChangeSet), FlaggingError> {
    request.check_cancelled()?;
    let path = display_ms_path(table);
    let selected = select(
        table,
        &request.selection,
        local_selection,
        base_rows,
        request.cancellation.as_ref(),
    )?;
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
            request.cancellation.as_ref(),
        )
        .map(|report| (report, ChangeSet::default()));
    }
    match request.mode {
        FlagDataMode::Quack | FlagDataMode::Extend | FlagDataMode::Tfcrop | FlagDataMode::Rflag => {
            return grouped_flags(table, request, selected, calculate);
        }
        FlagDataMode::Elevation => {
            return Err(FlaggingError::UnsupportedCalibrationAgent("elevation"));
        }
        FlagDataMode::Shadow => return Err(FlaggingError::UnsupportedCalibrationAgent("shadow")),
        // CASA does not run the antint worker on an empty local selection.
        // A selected calibration row would enter its unsupported buffer path.
        FlagDataMode::Antint
            if !selected.rows.is_empty() && has_calibration_column(table, request.data_column) =>
        {
            return Err(FlaggingError::CalibrationAntint);
        }
        FlagDataMode::Antint => return Err(FlaggingError::UnsupportedCalibrationAgent("antint")),
        _ => {}
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
    let clip = if request.mode == FlagDataMode::Clip {
        CalibrationClip::prepare(table, request, selected.rows.first().copied())?
    } else {
        None
    };
    let mut changes = ChangeSet::default();
    let mut report = FlagDataReport {
        mode: request.mode,
        selected_rows: selected.rows.len(),
        changed_rows: 0,
        changed_samples: 0,
        flagged_samples: 0,
        total_samples: 0,
        backup_version: None,
        timedev: None,
        freqdev: None,
        rflag_report: None,
        summary: None,
    };
    for rows in selected.rows.chunks(256) {
        request.check_cancelled()?;
        let main = table.main_table();
        let flags = clip_time::read_flags_uncached(main, rows, &path)?;
        let data = clip
            .as_ref()
            .map(|_| {
                main.column_accessor(request.data_column.name())?
                    .array_cells_owned_uncached(rows)
            })
            .transpose()
            .map_err(|error| flag_error(&path, error.to_string()))?;
        let mut updates = Vec::new();
        for (slot, (&row, mut flags)) in rows.iter().zip(flags).enumerate() {
            let clip_input = clip
                .as_ref()
                .zip(data.as_ref())
                .map(|(clip, values)| {
                    values[slot]
                        .as_ref()
                        .map(|value| (clip, value))
                        .ok_or_else(|| {
                            flag_error(
                                &path,
                                format!("missing {} at row {row}", request.data_column.name()),
                            )
                        })
                })
                .transpose()?;
            let changed = edit_row(
                table, request, &selected, row, &path, &mut flags, clip_input,
            )?;
            report.flagged_samples += flags.iter().filter(|&&value| value).count();
            report.total_samples += flags.len();
            if changed > 0 {
                changes.changed_row_indices.push(row);
                report.changed_samples += changed;
                updates.push((row, flags));
            }
        }
        if !updates.is_empty() {
            stage_flag_updates(table, updates)
                .map_err(|error| flag_error(&path, error.to_string()))?;
        }
    }
    report.changed_rows = changes.changed_row_indices.len();
    Ok((report, changes))
}

fn stage_flag_updates(
    table: &mut MeasurementSet,
    updates: Vec<(usize, ArrayD<bool>)>,
) -> Result<(), TableError> {
    // Set the bound FLAG cells without materializing unrelated solution columns.
    let mut writer = table
        .main_table_mut()
        .row_accessor_mut()
        .prepare(&[FLAG_COLUMN])?;
    let slot = writer
        .column_index(FLAG_COLUMN)
        .expect("prepared FLAG column");
    for (row, flags) in updates {
        writer.seek(row)?;
        writer.set_value_at(slot, Value::Array(ArrayValue::Bool(flags)))?;
    }
    writer.flush()
}

fn grouped_flags(
    table: &mut MeasurementSet,
    request: &FlagDataRequest,
    selected: SelectedFlagRows,
    calculate: bool,
) -> Result<(FlagDataReport, ChangeSet), FlaggingError> {
    let path = display_ms_path(table);
    let channels = selected.channels.as_ref();
    let changes = if matches!(request.mode, FlagDataMode::Tfcrop | FlagDataMode::Rflag) {
        let valid = has_calibration_column(table, request.data_column);
        if valid && let Some(parameter) = request.automatic_average_vector {
            return Err(FlaggingError::InvalidAutomaticAverageVector(parameter));
        }
        match (request.mode, valid) {
            (FlagDataMode::Tfcrop, true) => apply_tfcrop_flags(
                table,
                &selected.rows,
                channels,
                request,
                None,
                super::AveragedAgentInput::default(),
            )?,
            (FlagDataMode::Rflag, true) => apply_rflag_flags(
                table,
                &selected.rows,
                channels,
                request,
                request.rflag_calculate.unwrap_or(calculate),
                None,
                super::AveragedAgentInput::default(),
            )?,
            (FlagDataMode::Tfcrop, false) => return Err(FlaggingError::MissingTfcropColumn),
            _ => return Err(FlaggingError::UnsupportedCalibrationAgent("rflag")),
        }
    } else if request
        .correlation
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        ChangeSet::default()
    } else if request.mode == FlagDataMode::Quack {
        apply_quack_flags(table, &selected.rows, channels, None, request)?
    } else {
        extend::apply(table, &selected.rows, channels, None, request, &path)?
    };
    let mut report = summary::report(
        table,
        &selected.rows,
        channels,
        None,
        FlagSummaryOptions::default(),
        request.cancellation.as_ref(),
    )?;
    report.mode = request.mode;
    report.changed_rows = changes.changed_rows;
    report.changed_samples = changes.changed_samples;
    report.timedev = changes.thresholds.map(|values| values.0);
    report.freqdev = changes.thresholds.map(|values| values.1);
    report.rflag_report = changes.rflag_report.clone();
    report.summary = None;
    Ok((report, changes))
}

fn edit_row(
    table: &MeasurementSet,
    request: &FlagDataRequest,
    selected: &SelectedFlagRows,
    row: usize,
    path: &str,
    flags: &mut ArrayD<bool>,
    clip: Option<(&CalibrationClip, &ArrayValue)>,
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
    let Some((clip, data)) = clip else {
        return Ok(0);
    };
    if data.shape() != flags.shape() {
        return Err(flag_error(
            path,
            format!(
                "{} and FLAG have different shapes at row {row}",
                request.data_column.name()
            ),
        ));
    }
    let value = request.action.value();
    let unflag_mask = (!value).then(|| ArrayD::from_elem(flags.raw_dim(), false));
    let mut selected = Vec::new();
    apply_clip_array(
        data,
        unflag_mask.as_ref().unwrap_or(flags),
        channels.as_deref(),
        ClipOptions {
            zeros: request.clipzeros,
            signed_zeros: true,
            bounds: clip.bounds,
            outside: clip.outside,
            projections: clip.projections.as_deref(),
            metric: Some(clip.metric),
        },
        path,
        row,
        &mut selected,
    )?;
    let mut changed = 0;
    for (receptor, channel) in selected {
        let flag = &mut flags[[receptor, channel]];
        changed += usize::from(*flag != value);
        *flag = value;
    }
    Ok(changed)
}

struct CalibrationClip {
    outside: bool,
    bounds: Option<[f64; 2]>,
    metric: correlation::ClipMetric,
    projections: Option<Vec<ClipProjection>>,
}

fn has_calibration_column(table: &MeasurementSet, column: FlagDataColumn) -> bool {
    matches!(
        column,
        FlagDataColumn::Cparam | FlagDataColumn::Fparam | FlagDataColumn::Snr
    ) && table
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column.name()))
}

impl CalibrationClip {
    fn prepare(
        table: &MeasurementSet,
        request: &FlagDataRequest,
        first_row: Option<usize>,
    ) -> Result<Option<Self>, FlaggingError> {
        if !has_calibration_column(table, request.data_column) {
            return Err(FlaggingError::MissingClipColumn);
        }
        let (Some(outside), Some(first_row)) = (request.clipoutside, first_row) else {
            return Ok(None);
        };
        let flags = clone_flag_matrix(table, first_row)?;
        let planes = flags.shape().first().copied().unwrap_or_default();
        let expression = request
            .correlation
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_uppercase();
        let Some(projection) = projection(&expression, planes, request.data_column.is_real())?
        else {
            return Ok(None);
        };
        // The pinned calibration agent uses stored cells when averaging
        // options are supplied to the task.
        Ok(Some(Self {
            outside,
            bounds: request
                .clipminmax
                .map(|bounds| bounds.map(|value| f64::from(value as f32))),
            metric: projection.metric,
            projections: projection
                .planes
                .map(|planes| planes.into_iter().map(ClipProjection::Stored).collect()),
        }))
    }
}

pub(super) struct CalibrationProjection {
    metric: correlation::ClipMetric,
    planes: Option<Vec<usize>>,
}

impl CalibrationProjection {
    pub(super) fn includes(&self, plane: usize) -> bool {
        self.planes
            .as_ref()
            .is_none_or(|planes| planes.contains(&plane))
    }

    pub(super) fn scalar(&self, sample: &Sample) -> f64 {
        f64::from(
            self.metric
                .apply32(Complex32::new(sample.real as f32, sample.imag as f32)),
        )
    }
}

pub(super) fn automatic_projection(
    request: &FlagDataRequest,
    samples: &[Sample],
) -> Result<CalibrationProjection, FlaggingError> {
    use correlation::{CLIP_OPERATORS, ClipMetric, RECEPTOR_PRODUCTS, STOKES_PRODUCTS};

    let mut expression = request
        .correlation
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let available = samples
        .iter()
        .map(|sample| sample.corr + 1)
        .max()
        .unwrap_or_default();
    let real = request.data_column.is_real();
    let requested_metric = |expression: &str| {
        CLIP_OPERATORS
            .iter()
            .find_map(|(name, metric)| expression.contains(name).then_some(*metric))
    };
    if expression.is_empty() {
        expression = if real { "REAL ALL" } else { "ABS ALL" }.to_owned();
    } else if real {
        match requested_metric(&expression) {
            Some(ClipMetric::Real)
                if !CLIP_OPERATORS[1..]
                    .iter()
                    .any(|(name, _)| expression.contains(name)) => {}
            Some(_) => {
                let products = expression
                    .split_once('_')
                    .or_else(|| expression.split_once(' '))
                    .map_or("", |(_, products)| products);
                expression = format!("REAL {products}");
            }
            None => expression = format!("REAL {expression}"),
        }
    } else if request.mode == FlagDataMode::Tfcrop && requested_metric(&expression).is_none() {
        expression = format!("ABS {expression}");
    }
    let metric = requested_metric(&expression);
    let projection = |planes| CalibrationProjection {
        metric: metric.unwrap_or(ClipMetric::Amplitude),
        planes: Some(planes),
    };
    let products = ["SOL1", "SOL2", "SOL3", "SOL4"];
    if expression.contains("ALL")
        && let Some(metric) = metric
    {
        return Ok(projection(if metric == ClipMetric::Imaginary {
            Vec::new()
        } else {
            (0..available.min(products.len())).collect()
        }));
    }
    // The agent checks visibility correlations and Stokes names before
    // calibration solution names. The calibration mapper has no such products.
    if RECEPTOR_PRODUCTS
        .into_iter()
        .chain(STOKES_PRODUCTS)
        .any(|name| expression.contains(name))
    {
        return Ok(projection(Vec::new()));
    }
    let requested = products
        .iter()
        .enumerate()
        .filter(|(_, name)| expression.contains(*name))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if requested.is_empty() && available != 0 {
        return Err(unknown_calibration_polarization(&expression));
    }
    Ok(projection(
        requested
            .into_iter()
            .filter(|&index| index < available)
            .collect(),
    ))
}

fn unknown_calibration_polarization(expression: &str) -> FlaggingError {
    selection_error(format!(
        "Unknown polarization requested, ({expression}) supported types are: XX,YY,XY,YX,RR,LL,RL,LR,I,Q,U,V"
    ))
}

fn projection(
    expression: &str,
    planes: usize,
    real: bool,
) -> Result<Option<CalibrationProjection>, FlaggingError> {
    use correlation::ClipMetric;
    let operator = |name| match name {
        "ABS" => Some(ClipMetric::Amplitude),
        "REAL" => Some(ClipMetric::Real),
        "IMAG" => Some(ClipMetric::Imaginary),
        "ARG" => Some(ClipMetric::Phase),
        "NORM" => Some(ClipMetric::SquaredMagnitude),
        _ => None,
    };
    let explicit = expression
        .split_once(['_', ' '])
        .and_then(|(name, product)| operator(name).map(|metric| (metric, product.trim())))
        .or_else(|| operator(expression).map(|metric| (metric, "")));
    let fallback = if real {
        ClipMetric::Real
    } else {
        ClipMetric::Amplitude
    };
    let (requested, products) = explicit.unwrap_or((fallback, expression));
    let metric = if real { fallback } else { requested };
    let error = || {
        let text = if real && requested != ClipMetric::Real {
            format!("REAL {products}")
        } else {
            expression.to_string()
        };
        unknown_calibration_polarization(&text)
    };
    if explicit.is_some() && products.is_empty() {
        return Err(error());
    }
    if metric == ClipMetric::Imaginary {
        return Ok(None);
    }
    if matches!(products, "" | "ALL") {
        return Ok(Some(CalibrationProjection {
            metric,
            planes: None,
        }));
    }
    let mut selected = std::collections::BTreeSet::new();
    for product in products.split(',').map(str::trim) {
        let plane = product
            .strip_prefix("SOL")
            .and_then(|id| id.parse::<usize>().ok())
            .and_then(|id| id.checked_sub(1))
            .filter(|&id| id < planes);
        if let Some(plane) = plane {
            selected.insert(plane);
        } else if explicit.is_some() {
            return Err(error());
        } else {
            return Ok(None);
        }
    }
    Ok(Some(CalibrationProjection {
        metric,
        planes: Some(selected.into_iter().collect()),
    }))
}

struct SelectedFlagRows {
    rows: Vec<usize>,
    channels: Option<BTreeMap<i32, ChannelSelection>>,
}

pub(super) fn selected_rows(
    table: &MeasurementSet,
    selection: &MsSelection,
) -> Result<Vec<usize>, FlaggingError> {
    if selection
        .correlation
        .as_ref()
        .is_some_and(|selection| !selection.is_empty())
    {
        return Err(unsupported_selection());
    }
    select(table, selection, false, None, None).map(|selection| selection.rows)
}

fn unsupported_selection() -> FlaggingError {
    selection_error(
        "MSSelection::toTableExprNode(MSSelectableTable*): Only field-, spw-, scan-, time- and antenna-selection is supported for CalTables",
    )
}

fn select(
    table: &MeasurementSet,
    input: &MsSelection,
    local_selection: bool,
    base_rows: Option<&[usize]>,
    cancellation: Option<&Cancellation>,
) -> Result<SelectedFlagRows, FlaggingError> {
    if [
        input.array.as_deref(),
        input.uvrange.as_deref(),
        input.intent.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| !value.is_empty())
    {
        return Err(unsupported_selection());
    }
    let field = table
        .resolve_field_selector(input.field.as_deref().unwrap_or_default())
        .map_err(selection_error)?;
    let antenna = input.antenna.as_deref().unwrap_or_default();
    let antenna = if local_selection {
        table.resolve_calibration_agent_antenna_selector(antenna)
    } else {
        table.resolve_antenna_selector(antenna)
    }
    .map_err(antenna_selection_error)?;
    let scan = row_id_selection(input.scan.as_deref())?;
    let observation = row_id_selection(input.observation.as_deref())?;
    let spw = spectral_selection(table, input.spw.as_deref().unwrap_or_default())?;
    let mut rows = Vec::new();
    let main = table.main_table();
    let path = table.path();
    let unselected_count = if base_rows.is_none() {
        main.row_count()
    } else {
        0
    };
    let candidates = (0..unselected_count).chain(base_rows.into_iter().flatten().copied());
    for row in candidates {
        check_cancelled(cancellation)?;
        let id = |column| table_i32(main, row, column, path);
        let includes = |values: &[i32], column| -> Result<bool, FlaggingError> {
            Ok(values.is_empty() || values.contains(&id(column)?))
        };
        let includes_range = |ranges: Option<&[(i32, i32)]>, column| {
            ranges.map_or(Ok(true), |ranges| {
                let value = id(column)?;
                Ok(ranges
                    .iter()
                    .any(|&(first, last)| (first..=last).contains(&value)))
            })
        };
        let antenna_id = |column| id(column).map(|id| usize::try_from(id).unwrap_or(usize::MAX));
        if includes(&field, "FIELD_ID")?
            && includes_range(scan.as_deref(), "SCAN_NUMBER")?
            && includes_range(observation.as_deref(), "OBSERVATION_ID")?
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
                exposure_s: if local_selection {
                    table_f64(main, row, "INTERVAL", path)?
                } else {
                    0.1
                },
            })
        };
        let selection = TimeSelector::parse(text)
            .map_err(selection_error)?
            .resolve(TimeSelectorDefaults {
                first_term: defaults(first)?,
                later_terms: defaults(0)?,
            })
            .map_err(selection_error)?;
        rows = if local_selection {
            select_buffer_times(
                table,
                base_rows,
                rows,
                |seconds| selection.holds(seconds),
                cancellation,
            )?
        } else {
            let mut selected = Vec::with_capacity(rows.len());
            for row in rows {
                if selection.holds(time(row)?) {
                    selected.push(row);
                }
            }
            selected
        };
    }
    if rows.is_empty() && main.row_count() > 0 && !local_selection {
        return Err(selection_error(
            "MSSelectionNullSelection : The selected table has zero rows.",
        ));
    }
    let channels = if spw.selects_all() {
        None
    } else {
        Some(
            casa_ms::selection::flagdata_channel_ranges(table, &spw, false)
                .map_err(selection_error)?,
        )
    };
    Ok(SelectedFlagRows { rows, channels })
}

fn select_buffer_times(
    table: &MeasurementSet,
    base_rows: Option<&[usize]>,
    mut rows: Vec<usize>,
    holds: impl Fn(f64) -> bool,
    cancellation: Option<&Cancellation>,
) -> Result<Vec<usize>, FlaggingError> {
    let count = table.main_table().row_count();
    let all = base_rows.map_or_else(|| Cow::Owned((0..count).collect::<Vec<_>>()), Cow::Borrowed);
    let timed = clip_time::ordered_baseline_chunks(
        table,
        &all,
        None,
        false,
        &display_ms_path(table),
        cancellation,
    )?;
    let mut selected = vec![false; count];
    for chunk in timed.chunk_by(|left, right| left.chunk_key() == right.chunk_key()) {
        check_cancelled(cancellation)?;
        let first = chunk
            .iter()
            .min_by_key(|row| row.row)
            .expect("nonempty buffer");
        if holds(first.time()) {
            for row in chunk {
                selected[row.row] = true;
            }
        }
    }
    rows.retain(|&row| selected[row]);
    Ok(rows)
}

fn row_id_selection(text: Option<&str>) -> Result<Option<Vec<(i32, i32)>>, FlaggingError> {
    let Some(text) = text.filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    let invalid = || {
        let mut offset = 0;
        for term in text.split(',') {
            if parse_numeric_row_id_ranges(term, "scan").is_err() {
                if let Some((low, high)) = term.split_once('~')
                    && let (Ok(low), Ok(high)) =
                        (low.trim().parse::<i32>(), high.trim().parse::<i32>())
                    && low >= high
                {
                    return selection_error(format!(
                        "Scan Expression: Malformed range bounds {low} (lower bound) and {high} (upper bound)\n(near char. {} in string {text:?})",
                        offset + term.len(),
                    ));
                }
                let token = if term.trim().is_empty() {
                    text.get(offset + term.len()..)
                        .and_then(|rest| rest.chars().next())
                        .map_or(String::new(), |c| c.to_string())
                } else {
                    term.trim().to_string()
                };
                let position = (offset + term.len() + usize::from(token == ",")).max(1);
                return selection_error(format!(
                    "Scan Expression: Parse error at or near '{token}'\n(near char. {position} in string {text:?})",
                ));
            }
            offset += term.len() + 1;
        }
        selection_error(format!("Scan Expression: malformed selector {text:?}"))
    };
    parse_numeric_row_id_ranges(text, "scan")
        .map(Some)
        .map_err(|_| invalid())
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
