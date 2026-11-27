// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn apply_tfcrop_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
    model_factory: Option<FlagDataModelFactory>,
    averaging: AveragedAgentInput<'_>,
) -> Result<ChangeSet, FlaggingError> {
    let AveragedAgentInput {
        time: averaged_time,
        uv: averaged_uv,
        mut view,
    } = averaging;
    let started = Instant::now();
    let column = request.data_column;
    let calibration = is_calibration_table(ms);
    let public_ms = request.casa_row_selection && !calibration;
    let time_average = request
        .clip_time_bin
        .as_deref()
        .filter(|_| public_ms)
        .map(time_bin_seconds);
    let correlations = public_ms
        .then(|| TfcropCorrelations::read(ms, request.correlation.as_deref(), column))
        .transpose()?;
    let corr_admitted = correlations
        .as_ref()
        .is_none_or(TfcropCorrelations::has_setup);
    let options = request.tfcrop.filter(|options| {
        (1..=9).contains(&options.max_pieces)
            && (1..=3).contains(&options.half_window)
            && time_average != Some(None)
            && corr_admitted
    });
    let widths = request.clip_channel_bins.as_deref().filter(|_| public_ms);
    let average = view.is_some()
        || time_average.flatten().is_some()
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let (mut columns, configures_averaging) = if public_ms {
        tfcrop_columns(
            ms,
            column,
            view.as_deref()
                .map_or(selected_rows, SharedFlagView::input_rows),
            model_factory,
            options.is_some(),
            average,
        )?
    } else {
        (None, false)
    };
    let options =
        options.filter(|_| !request.casa_row_selection || calibration || columns.is_some());
    if (public_ms && !configures_averaging) || (options.is_none() && !request.extendflags) {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let configures_averaging = configures_averaging && corr_admitted && time_average != Some(None);
    let widths = widths.filter(|_| configures_averaging);
    if let Some(widths) = widths {
        check_channel_bins(ms, widths, &display_ms_path(ms))?;
    }
    let loaded = load_automatic_samples(
        ms,
        selected_rows,
        channel_selections,
        request,
        AutomaticSampleSource {
            columns: columns.as_mut(),
            fit: options.is_some(),
            seconds: time_average.flatten().filter(|_| configures_averaging),
            widths,
            input: AveragedAgentInput {
                time: averaged_time,
                uv: averaged_uv,
                view: view.as_deref_mut(),
            },
        },
    )?;
    let mut samples = loaded.samples;
    let calibration_projection = calibration
        .then(|| calibration::automatic_projection(request, &samples))
        .transpose()?;
    if let Some(projection) = &calibration_projection {
        for sample in &mut samples {
            sample.amp = projection.scalar(sample);
        }
    }
    let loaded_ms = started.elapsed().as_millis();
    let mut modified = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let groups: Vec<_> = if request.casa_row_selection || calibration_projection.is_some() {
        let ordered = if let Some(view) = view.as_deref() {
            view.groups(&samples)
        } else {
            ordered_autoflag_groups(ms, selected_rows, &samples, request)?
        };
        ordered
            .into_iter()
            .map(|(([_, _, _, _, description], _), group)| (Some(description), group))
            .collect()
    } else {
        group_samples(&samples)
            .into_values()
            .map(|group| (None, group))
            .collect()
    };
    if let Some(options) = &options {
        let fit_groups = time_selected_autoflag_groups(&groups, &loaded.axes);
        fit_tfcrop_groups(
            &fit_groups,
            options,
            request,
            correlations.as_ref(),
            calibration_projection.as_ref(),
            &loaded.model_roundoff,
            &mut TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            },
        )?;
    }
    if request.extendflags && view.is_none() {
        let fit_flags = loaded
            .axes
            .flags_by_bin
            .as_ref()
            .unwrap_or(&loaded.flags_by_row);
        extend_tfcrop_groups(
            &groups,
            &samples,
            fit_flags,
            request.casa_row_selection && channel_selections.is_some() && widths.is_none(),
            correlations.as_ref().filter(|_| corr_admitted),
            &mut modified,
        );
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "tfcrop",
            "implementation": "casa-rs-native-current",
            "selected_rows": selected_rows.len(),
            "samples": samples.len(),
            "groups": groups.len(),
            "freqcutoff": request.freqcutoff,
            "timecutoff": request.timecutoff,
            "freq_candidate_samples": count_mask(&freq_flags),
            "time_candidate_samples": count_mask(&time_flags),
            "union_candidate_samples": union_candidates,
            "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
            "timing_ms": {
                "load": loaded_ms,
                "plan": planned_ms.saturating_sub(loaded_ms),
            },
        })
    });
    let mut changes = if let Some(view) = view {
        view.stage(&samples, &modified, request.action.value())
    } else {
        apply_tfcrop_mask(
            &samples,
            &modified,
            loaded.flags_by_row,
            &loaded.axes,
            ms,
            !request.casa_row_selection,
            request.action.value(),
        )?
    };
    changes.selected_rows = loaded.axes.time_selected_rows(averaged_time);
    let applied_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "tfcrop",
            "implementation": "casa-rs-casa-shaped-tfcrop",
            "timing_ms": {
                "apply": applied_ms.saturating_sub(planned_ms),
                "total_before_save": applied_ms,
            },
        })
    });
    Ok(changes)
}

pub(super) struct TfcropFitMasks<'a> {
    pub(super) modified: &'a mut [bool],
    pub(super) freq_flags: &'a mut [bool],
    pub(super) time_flags: &'a mut [bool],
}

pub(super) fn fit_tfcrop_groups(
    groups: &[(Option<i32>, Vec<Sample>)],
    options: &TfcropOptions,
    request: &FlagDataRequest,
    correlations: Option<&TfcropCorrelations>,
    calibration_projection: Option<&calibration::CalibrationProjection>,
    model_roundoff: &[f32],
    masks: &mut TfcropFitMasks<'_>,
) -> Result<(), FlaggingError> {
    let mut roundoff = if model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; masks.modified.len()]
    };
    for (description, group) in groups {
        let planes = samples_by_corr(group);
        let selection = correlations.zip(*description);
        let default_projections;
        let (projections, metric) = if let Some((selection, ddid)) = selection {
            (selection.for_description(ddid)?, Some(selection.metric))
        } else {
            default_projections = planes
                .iter()
                .filter(|plane| calibration_projection.is_none_or(|p| p.includes(plane[0].corr)))
                .map(|plane| ClipProjection::Stored(plane[0].corr))
                .collect::<Vec<_>>();
            (default_projections.as_slice(), None)
        };
        for &projection in projections {
            let Some(series) = tfcrop_series(
                &planes,
                projection,
                metric,
                masks.modified,
                model_roundoff,
                &mut roundoff,
            ) else {
                continue;
            };
            for &direction in options.passes.directions() {
                let (cutoff, candidates) = match direction {
                    TfcropDirection::Freq => (request.freqcutoff, &mut *masks.freq_flags),
                    TfcropDirection::Time => (request.timecutoff, &mut *masks.time_flags),
                };
                tfcrop_fit_base_and_flag(
                    &series.samples,
                    direction,
                    cutoff,
                    options,
                    TfcropArithmetic {
                        casa_arithmetic: request.casa_row_selection,
                        model_roundoff: &roundoff,
                    },
                    masks.modified,
                    candidates,
                );
            }
            for (first, second) in series.pairs {
                masks.modified[second] |= masks.modified[first];
                masks.freq_flags[second] |= masks.freq_flags[first];
                masks.time_flags[second] |= masks.time_flags[first];
            }
        }
    }
    Ok(())
}

pub(super) fn extend_tfcrop_groups(
    groups: &[(Option<i32>, Vec<Sample>)],
    samples: &[Sample],
    flags: &BTreeMap<usize, ArrayD<bool>>,
    selected_channels: bool,
    correlations: Option<&TfcropCorrelations>,
    modified: &mut [bool],
) {
    let mut restricted_mask = correlations
        .filter(|selection| selection.restrict_extension)
        .map(|_| modified.to_vec());
    let mask = restricted_mask.as_deref_mut().unwrap_or(modified);
    if selected_channels {
        for (_, group) in groups {
            extend_tfcrop_selected_channels(group, flags, mask);
        }
    } else {
        extend_autoflag_grouped(
            groups.iter().map(|(_, group)| group.as_slice()),
            samples,
            mask,
        );
    }
    if let Some((selection, extended)) = correlations.zip(restricted_mask) {
        for (description, group) in groups {
            let planes = selection
                .extension_planes(description.expect("public TFCrop group has a data description"));
            for sample in group {
                if planes.binary_search(&sample.corr).is_ok() {
                    modified[sample.index] = extended[sample.index];
                }
            }
        }
    }
}

pub(super) struct TfcropSeries {
    pub(super) samples: Vec<Sample>,
    pub(super) pairs: Vec<(usize, usize)>,
}

pub(super) fn tfcrop_series(
    planes: &[Vec<Sample>],
    projection: ClipProjection,
    metric: Option<ClipMetric>,
    modified: &[bool],
    model_roundoff: &[f32],
    roundoff: &mut [f32],
) -> Option<TfcropSeries> {
    let plane = |plane| planes.iter().find(|samples| samples[0].corr == plane);
    let (first, second) = match projection {
        ClipProjection::Stored(first) => (plane(first)?, None),
        ClipProjection::Stokes(first, second, combine) => {
            (plane(first)?, Some((plane(second)?, combine)))
        }
    };
    let mut series = TfcropSeries {
        samples: Vec::with_capacity(first.len()),
        pairs: Vec::new(),
    };
    let second = second.map(|(samples, combine)| {
        let lookup = samples
            .iter()
            .map(|sample| ((sample.row, sample.chan), sample))
            .collect::<HashMap<_, _>>();
        (lookup, combine)
    });
    for original in first {
        let mut sample = *original;
        sample.flag = sample_is_modified(original, modified);
        let mut value = Complex64::new(sample.real, sample.imag);
        let mut uncertainty = model_roundoff.get(sample.index).copied().unwrap_or(0.0);
        if let Some((lookup, combine)) = &second {
            let Some(other) = lookup.get(&(sample.row, sample.chan)) else {
                continue;
            };
            value = combine.apply64(value, Complex64::new(other.real, other.imag));
            sample.real = value.re;
            sample.imag = value.im;
            sample.flag |= sample_is_modified(other, modified);
            uncertainty =
                (uncertainty + model_roundoff.get(other.index).copied().unwrap_or(0.0)) * 0.5;
            series.pairs.push((sample.index, other.index));
        }
        if let Some(metric) = metric {
            sample.amp = metric.apply64(value);
            if let Some(bound) = roundoff.get_mut(sample.index) {
                *bound = tfcrop_metric_roundoff(metric, value, uncertainty);
            }
        } else if let Some(bound) = roundoff.get_mut(sample.index) {
            *bound = uncertainty;
        }
        series.samples.push(sample);
    }
    Some(series)
}

pub(super) fn tfcrop_metric_roundoff(
    metric: ClipMetric,
    value: Complex64,
    uncertainty: f32,
) -> f32 {
    if uncertainty == 0.0 {
        return 0.0;
    }
    let radius = f64::from(uncertainty);
    match metric {
        ClipMetric::SquaredMagnitude => (radius * (2.0 * value.norm() + radius)) as f32,
        ClipMetric::Phase
            if radius < value.norm() && (value.re >= 0.0 || radius < value.im.abs()) =>
        {
            (radius / value.norm()).asin() as f32
        }
        // The fit compares principal arguments on either side of ±π.
        ClipMetric::Phase => std::f32::consts::TAU,
        _ => uncertainty,
    }
}

pub(super) fn tfcrop_columns(
    ms: &MeasurementSet,
    column: FlagDataColumn,
    selected_rows: &[usize],
    model_factory: Option<FlagDataModelFactory>,
    active: bool,
    average: bool,
) -> Result<(Option<ClipColumns>, bool), FlaggingError> {
    autoflag_columns(
        ms,
        column,
        selected_rows,
        model_factory,
        active,
        average,
        FlagDataMode::Tfcrop,
    )
}

pub(super) fn autoflag_columns(
    ms: &MeasurementSet,
    column: FlagDataColumn,
    selected_rows: &[usize],
    model_factory: Option<FlagDataModelFactory>,
    active: bool,
    average: bool,
    mode: FlagDataMode,
) -> Result<(Option<ClipColumns>, bool), FlaggingError> {
    let missing = || {
        if mode == FlagDataMode::Rflag {
            FlaggingError::MissingRflagColumn
        } else {
            FlaggingError::MissingTfcropColumn
        }
    };
    let table = ms.main_table();
    let exists = |name| {
        table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let is_residual = matches!(
        column,
        FlagDataColumn::Residual | FlagDataColumn::ResidualData
    );
    let required_sources_exist = if is_residual {
        exists("MODEL_DATA") || !casa_ms::virtual_model_fields(ms).is_empty()
    } else {
        exists(column.name())
    };
    if !required_sources_exist {
        return Err(missing());
    }
    if column == FlagDataColumn::Residual && !exists("CORRECTED_DATA") {
        let agent = if mode == FlagDataMode::Rflag {
            "Rflag"
        } else {
            "Tfcrop"
        };
        tracing::warn!(
            "Cannot create agent {agent}: necessary CORRECTED_DATA column is not available"
        );
        return Ok((None, false));
    }
    // An admitted agent configures the iterator before validating its fit.
    // A surviving extension agent uses that iterator. Only a running fit
    // opens a virtual recipe's image.
    if !active {
        return Ok((None, true));
    }
    let path = display_ms_path(ms);
    let models =
        clip_models::ClipModels::for_selection(ms, column, selected_rows, model_factory, &path)?;
    match ClipColumns::bind(table, column, average, models) {
        Err(FlaggingError::MissingClipColumn) => Err(missing()),
        result => result.map(|columns| (columns, true)),
    }
}

pub(super) fn extend_tfcrop_selected_channels(
    group: &[Sample],
    flags: &BTreeMap<usize, ArrayD<bool>>,
    modified: &mut [bool],
) {
    // The extension agent counts a whole window. Publish the results only
    // for samples in the explicit channel selection.
    let mut full = Vec::new();
    let mut indices = Vec::with_capacity(group.len());
    for row in sample_row_runs(group) {
        let sample = row[0];
        let flags = &flags[&sample.row];
        let channels = flags.shape()[1];
        let start = full.len();
        push_flag_samples(
            flags,
            &(0..channels).collect::<Vec<_>>(),
            None,
            RowSampleMetadata {
                row: sample.row,
                field: sample.field,
                spw: sample.spw,
                scan: sample.scan,
                ant1: sample.ant1,
                ant2: sample.ant2,
            },
            &mut full,
        );
        indices.extend(
            row.iter()
                .map(|sample| (sample.index, start + sample.corr * channels + sample.chan)),
        );
    }
    let mut extended = vec![false; full.len()];
    for &(selected, all) in &indices {
        extended[all] = modified[selected];
    }
    extend_autoflag_grouped([full.as_slice()], &full, &mut extended);
    for (selected, all) in indices {
        modified[selected] = extended[all];
    }
}

pub(super) type AutoflagChunk = ([i32; 5], usize);

pub(super) fn ordered_autoflag_groups(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    samples: &[Sample],
    request: &FlagDataRequest,
) -> Result<Vec<(AutoflagChunk, Vec<Sample>)>, FlaggingError> {
    let timed = clip_time::ordered_baseline_chunks(
        ms,
        selected_rows,
        request.time_chunk,
        request.combinescans,
        &display_ms_path(ms),
    )?;
    let runs: HashMap<_, _> = sample_row_runs(samples)
        .into_iter()
        .map(|row| (row[0].row, row))
        .collect();
    Ok(timed
        .chunk_by(|left, right| left.baseline_key() == right.baseline_key())
        .map(|baseline| {
            let samples = baseline
                .iter()
                .filter_map(|row| runs.get(&row.row).copied())
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            (baseline[0].chunk_key(), samples)
        })
        .filter(|(_, group)| !group.is_empty())
        .collect())
}

pub(super) fn apply_tfcrop_mask(
    samples: &[Sample],
    mask: &[bool],
    mut flags: BTreeMap<usize, ArrayD<bool>>,
    axes: &AveragedSampleAxes,
    ms: &mut MeasurementSet,
    update_flag_rows: bool,
    flag_value: bool,
) -> Result<ChangeSet, FlaggingError> {
    if axes.channels_by_row.is_empty() && axes.rows_by_bin.is_empty() {
        let mut changes =
            apply_flag_mask_with_preloaded(samples, mask, flags, ms, update_flag_rows, flag_value)?;
        changes.summary = Some(summary_after_flag_mask(
            samples,
            changes.changed_samples,
            flag_value,
        ));
        return Ok(changes);
    }

    // A flag on a bin is written back to each of its selected physical
    // channels. Preview and apply both count flags on that original axis.
    let coordinates = |sample| tfcrop_flag_coordinates(axes, sample);
    let mut summary = FlagSummary {
        flagged_samples: 0,
        total_samples: 0,
    };
    for (row, corr, chan) in samples.iter().flat_map(coordinates) {
        summary.flagged_samples += usize::from(flags[&row][[corr, chan]]);
        summary.total_samples += 1;
    }
    let coordinates = samples
        .iter()
        .zip(mask)
        .filter(|(_, flag)| **flag)
        .flat_map(|(sample, _)| coordinates(sample));
    let (touched, changed_samples) = stage_flag_coordinates(coordinates, &mut flags, flag_value);
    if flag_value {
        summary.flagged_samples += changed_samples;
    } else {
        summary.flagged_samples = summary.flagged_samples.saturating_sub(changed_samples);
    }
    let mut changed = ChangeSet {
        changed_samples,
        summary: Some(summary),
        ..ChangeSet::default()
    };
    let complete_rows = touched
        .iter()
        .copied()
        .filter(|row| !axes.rows_by_bin.is_empty() && flags[row].iter().all(|flag| *flag))
        .collect::<std::collections::BTreeSet<_>>();
    write_touched_flag_updates(
        ms,
        flags,
        touched,
        |row| update_flag_rows || complete_rows.contains(&row),
        &mut changed,
    )?;
    Ok(changed)
}

pub(super) fn tfcrop_flag_coordinates<'a>(
    axes: &'a AveragedSampleAxes,
    sample: &'a Sample,
) -> impl Iterator<Item = (usize, usize, usize)> + 'a {
    let rows = axes
        .rows_by_bin
        .get(&sample.row)
        .map_or_else(|| std::slice::from_ref(&sample.row), Vec::as_slice);
    let channels = axes.channels_by_row.get(&sample.row).map_or_else(
        || std::slice::from_ref(&sample.chan),
        |average| bin_members(&average.channels, average.width, sample.chan),
    );
    rows.iter()
        .flat_map(move |&row| channels.iter().map(move |&chan| (row, sample.corr, chan)))
}

#[derive(Debug, Clone, Copy)]
pub(super) enum TfcropDirection {
    Freq,
    Time,
}

/// Model fitted to the selected time or frequency axis.
#[derive(Debug, Clone, Copy)]
pub enum TfcropFit {
    /// A single line.
    Line,
    /// A piecewise polynomial.
    Poly,
}

/// Ordered TFCrop axis passes.
#[derive(Debug, Clone, Copy)]
pub enum TfcropPasses {
    /// Frequency alone.
    Frequency,
    /// Time alone.
    Time,
    /// Frequency followed by time.
    FrequencyThenTime,
    /// Time followed by frequency.
    TimeThenFrequency,
}

impl TfcropPasses {
    fn directions(self) -> &'static [TfcropDirection] {
        use TfcropDirection::{Freq, Time};
        match self {
            Self::Frequency => &[Freq],
            Self::Time => &[Time],
            Self::FrequencyThenTime => &[Freq, Time],
            Self::TimeThenFrequency => &[Time, Freq],
        }
    }
}

/// Additional statistics evaluated around each candidate sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TfcropWindowStats {
    /// Skip the neighborhood pass.
    None,
    /// Mean absolute deviation in the window.
    Sum,
    /// Root mean square deviation in the window.
    Std,
    /// Both statistics.
    Both,
}

/// Model fitting, axis order and optional neighborhood tests for TFCrop.
#[derive(Debug, Clone, Copy)]
pub struct TfcropOptions {
    /// Model along time.
    pub time_fit: TfcropFit,
    /// Model along frequency.
    pub frequency_fit: TfcropFit,
    /// Axis passes in the order they run.
    pub passes: TfcropPasses,
    /// Sliding-window statistics.
    pub window_stats: TfcropWindowStats,
    /// Maximum piece count for a polynomial fit.
    pub max_pieces: usize,
    /// Number of neighbors per side in a sliding window.
    pub half_window: usize,
}

impl Default for TfcropOptions {
    fn default() -> Self {
        Self {
            time_fit: TfcropFit::Line,
            frequency_fit: TfcropFit::Poly,
            passes: TfcropPasses::FrequencyThenTime,
            window_stats: TfcropWindowStats::None,
            max_pieces: 7,
            half_window: 1,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct TfcropArithmetic<'a> {
    pub(super) casa_arithmetic: bool,
    pub(super) model_roundoff: &'a [f32],
}

pub(super) fn tfcrop_fit_base_and_flag(
    samples: &[Sample],
    direction: TfcropDirection,
    cutoff: f64,
    options: &TfcropOptions,
    arithmetic: TfcropArithmetic<'_>,
    modified: &mut [bool],
    direction_flags: &mut [bool],
) {
    let TfcropArithmetic {
        casa_arithmetic,
        model_roundoff,
    } = arithmetic;
    if samples.is_empty() {
        return;
    }
    let rows = unique_sample_rows(samples);
    let chans = unique_sample_chans(samples);
    let (n0, n1) = match direction {
        TfcropDirection::Freq => (chans.len(), rows.len()),
        TfcropDirection::Time => (rows.len(), chans.len()),
    };

    let dense = rows.len().checked_mul(chans.len()) == Some(samples.len())
        && rows.windows(2).all(|pair| pair[0] < pair[1])
        && samples
            .chunks_exact(chans.len())
            .zip(&rows)
            .all(|(cells, row)| {
                cells
                    .iter()
                    .zip(&chans)
                    .all(|(cell, chan)| cell.row == *row && cell.chan == *chan)
            });
    let sparse = (!dense).then(|| {
        samples
            .iter()
            .map(|sample| ((sample.row, sample.chan), sample))
            .collect::<HashMap<_, _>>()
    });
    let sample_at = |i0, i1| {
        let (row, chan) = tfcrop_row_chan(direction, i0, i1);
        if let Some(sparse) = &sparse {
            sparse.get(&(rows[row], chans[chan])).copied()
        } else {
            samples.get(row * chans.len() + chan)
        }
    };

    let mut avg_dat = vec![0.0f32; n0];
    let mut avg_flag = vec![false; n0];
    let mut avg_roundoff = (!model_roundoff.is_empty()).then(|| vec![0.0; n0]);
    let mut all_zeros = true;
    for i0 in 0..n0 {
        let mut sum = 0.0f32;
        let mut count = 0usize;
        for i1 in 0..n1 {
            if let Some(sample) = sample_at(i0, i1)
                && !sample_is_modified(sample, modified)
            {
                sum += tfcrop_value(sample);
                if let Some(roundoff) = &mut avg_roundoff {
                    roundoff[i0] += model_roundoff.get(sample.index).copied().unwrap_or(0.0);
                }
                count += 1;
            }
        }
        if count > 0 {
            avg_dat[i0] = sum / count as f32;
            if let Some(roundoff) = &mut avg_roundoff {
                roundoff[i0] /= count as f32;
            }
            all_zeros = false;
        } else {
            avg_flag[i0] = true;
        }
    }
    if all_zeros {
        return;
    }

    let fit = match direction {
        TfcropDirection::Freq => options.frequency_fit,
        TfcropDirection::Time => options.time_fit,
    };
    let avg_fit = match fit {
        TfcropFit::Line => {
            tfcrop_fit_piecewise_poly(&avg_dat, &mut avg_flag, 1, 1, casa_arithmetic)
        }
        TfcropFit::Poly => tfcrop_fit_piecewise_poly(
            &avg_dat,
            &mut avg_flag,
            options.max_pieces,
            4,
            casa_arithmetic,
        ),
    };

    let mut normalized_roundoff = avg_roundoff.as_ref().map(|_| vec![0.0; n0]);
    // The fitted baseline shares information across the axis, including
    // channels at which the model has no spectral coverage.
    let fit_roundoff = avg_roundoff.as_ref().map_or(0.0, |bounds| {
        bounds
            .iter()
            .copied()
            .filter(|bound| bound.is_finite())
            .fold(0.0, f32::max)
    });
    for i1 in 0..n1 {
        let mut normalized = vec![0.0f32; n0];
        let mut flags = vec![false; n0];
        if let Some(roundoff) = &mut normalized_roundoff {
            roundoff.fill(0.0);
        }
        for i0 in 0..n0 {
            if let Some(sample) = sample_at(i0, i1) {
                flags[i0] = sample_is_modified(sample, modified);
                if !flags[i0] && (casa_arithmetic || avg_fit[i0].abs() > f32::EPSILON) {
                    let fit = avg_fit[i0];
                    normalized[i0] = tfcrop_value(sample) / fit;
                    if let Some(roundoff) = &mut normalized_roundoff {
                        // The fitted baseline also uses residuals made with the
                        // reconstructed model. Compare above both uncertainties.
                        let measured = model_roundoff.get(sample.index).copied().unwrap_or(0.0);
                        let uncertainty = (measured + fit_roundoff) / fit.abs();
                        if uncertainty.is_finite() {
                            roundoff[i0] = uncertainty;
                        }
                    }
                }
            } else {
                flags[i0] = true;
            }
        }

        let mut previous_sd = 0.0f32;
        let mut sd = 0.0;
        for _ in 0..5 {
            sd = tfcrop_std_about_mean(&normalized, &flags, 1.0);
            if !casa_arithmetic && (!sd.is_finite() || sd <= f32::EPSILON) {
                break;
            }
            for i0 in 0..n0 {
                let deviation = (normalized[i0] - 1.0).abs();
                if !flags[i0]
                    && deviation > cutoff as f32 * sd
                    && normalized_roundoff
                        .as_ref()
                        .is_none_or(|bounds| bounds[i0] == 0.0 || deviation > bounds[i0])
                {
                    flags[i0] = true;
                }
            }
            if (previous_sd - sd).abs() < 0.1 {
                break;
            }
            previous_sd = sd;
        }

        tfcrop_window_flags(
            &normalized,
            normalized_roundoff.as_deref(),
            &mut flags,
            options,
            cutoff as f32 * sd,
        );

        for (i0, &flagged) in flags.iter().enumerate() {
            if !flagged {
                continue;
            }
            if let Some(sample) = sample_at(i0, i1)
                && !modified[sample.index]
            {
                modified[sample.index] = true;
                direction_flags[sample.index] = true;
            }
        }
    }
}

pub(super) fn tfcrop_window_flags(
    normalized: &[f32],
    roundoff: Option<&[f32]>,
    flags: &mut [bool],
    options: &TfcropOptions,
    threshold: f32,
) {
    let half_width = options.half_window;
    let stats = options.window_stats;
    if stats == TfcropWindowStats::None || half_width == 0 {
        return;
    }
    let width = half_width.saturating_mul(2).saturating_add(1);
    for (start, samples) in normalized.windows(width).enumerate() {
        let deviations = || {
            samples.iter().enumerate().map(|(offset, value)| {
                let deviation = (*value - 1.0).abs();
                roundoff.map_or(deviation, |bounds| {
                    let bound = bounds[start + offset];
                    if deviation <= bound {
                        0.0
                    } else {
                        deviation - bound
                    }
                })
            })
        };
        let exceeds_mean = matches!(stats, TfcropWindowStats::Sum | TfcropWindowStats::Both)
            && deviations().sum::<f32>() / width as f32 > threshold;
        let exceeds_rms = matches!(stats, TfcropWindowStats::Std | TfcropWindowStats::Both)
            && (deviations().map(|value| value * value).sum::<f32>() / width as f32).sqrt()
                > threshold;
        flags[start + half_width] |= exceeds_mean || exceeds_rms;
    }
}

pub(super) fn tfcrop_row_chan(
    direction: TfcropDirection,
    axis0: usize,
    axis1: usize,
) -> (usize, usize) {
    match direction {
        TfcropDirection::Freq => (axis1, axis0),
        TfcropDirection::Time => (axis0, axis1),
    }
}

pub(super) fn tfcrop_value(sample: &Sample) -> f32 {
    sample.amp as f32
}

pub(super) fn sample_is_modified(sample: &Sample, modified: &[bool]) -> bool {
    sample.flag || modified.get(sample.index).copied().unwrap_or(false)
}

pub(super) fn tfcrop_fit_piecewise_poly(
    data: &[f32],
    flag: &mut [bool],
    max_pieces: usize,
    max_degree: usize,
    casa_arithmetic: bool,
) -> Vec<f32> {
    let mut tdata = data.to_vec();
    for i in 0..tdata.len() {
        if tdata[i] != 0.0 {
            continue;
        }
        if i == 0 {
            if let Some((index, _)) = tdata
                .iter()
                .enumerate()
                .skip(1)
                .find(|(_, value)| **value != 0.0)
            {
                tdata[i] = tdata[index];
            }
        } else {
            let left = (0..=i).rev().find(|index| tdata[*index] != 0.0);
            let right = (i + 1..tdata.len()).find(|index| tdata[*index] != 0.0);
            match (left, right) {
                (Some(left), Some(right)) => tdata[i] = (tdata[left] + tdata[right]) / 2.0,
                (Some(left), None) => tdata[i] = tdata[left],
                (None, Some(right)) => tdata[i] = tdata[right],
                (None, None) => {}
            }
        }
    }
    for (index, value) in tdata.iter().enumerate() {
        if *value == 0.0 {
            flag[index] = true;
        }
    }

    let mut fitted = tdata.clone();
    for iteration in 0..5usize {
        let npieces = (2 * iteration + 1).min(max_pieces);
        let mut degree = 1usize;
        if iteration > 1 {
            degree = 2;
        }
        if iteration > 2 {
            degree = 3;
        }
        degree = degree.min(max_degree);
        let piece_size = tdata.len() / npieces;
        let leftover = tdata.len() % npieces;
        let leftover_front = leftover / 2;
        for piece in 0..npieces {
            let (left, right) = if npieces > 1 {
                let mut left = leftover_front + piece * piece_size;
                let mut right = left + piece_size;
                if piece == 0 {
                    left = 0;
                    right = leftover_front + piece_size;
                }
                if piece == npieces - 1 {
                    right = tdata.len() - 1;
                }
                (left, right)
            } else {
                (0, tdata.len() - 1)
            };
            if degree == 1 {
                tfcrop_line_fit(&tdata, flag, &mut fitted, left, right, casa_arithmetic);
            } else {
                tfcrop_poly_fit(
                    &tdata,
                    flag,
                    &mut fitted,
                    left,
                    right,
                    degree,
                    casa_arithmetic,
                );
            }
        }

        for i in 2..tdata.len().saturating_sub(2) {
            let win_start = i - 2;
            let win_end = (i + 2).min(tdata.len() - 1);
            if win_end <= win_start {
                break;
            }
            let sum = (win_start..=win_end)
                .map(|index| fitted[index])
                .sum::<f32>();
            fitted[i] = sum / (win_end - win_start + 1) as f32;
        }

        let sd = tfcrop_std_about_fit(&tdata, flag, &fitted);
        if !sd.is_finite() {
            continue;
        }
        let tolerance = if iteration >= 2 { 2.0 } else { 3.0 };
        for i in 0..tdata.len() {
            if tdata[i] - fitted[i] > tolerance * sd {
                flag[i] = true;
            }
        }
    }
    fitted
}

pub(super) fn tfcrop_line_fit(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
    casa_arithmetic: bool,
) {
    let mean = tfcrop_mean(data, flag);
    let sd = tfcrop_std_about_mean(data, flag, mean);
    if !casa_arithmetic && (!sd.is_finite() || sd <= f32::EPSILON) {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let mut sum_w = 0.0f32;
    let mut sum_x = 0.0f32;
    let mut sum_y = 0.0f32;
    let mut sum_xx = 0.0f32;
    let mut sum_xy = 0.0f32;
    let variance = sd * sd;
    let weight = 1.0f32 / variance;
    // CASA lets a degenerate fit produce IEEE non-finite values. The
    // normalized pass then leaves the flags alone in that fit direction.
    let weighted = |term| {
        if casa_arithmetic {
            term / variance
        } else {
            term * weight
        }
    };
    for i in left..=right {
        if !flag[i] {
            let x = i as f32;
            sum_w += weight;
            sum_x += weighted(x);
            sum_y += weighted(data[i]);
            sum_xx += weighted(x * x);
            sum_xy += weighted(x * data[i]);
        }
    }
    let denom = sum_w * sum_xx - sum_x * sum_x;
    if !casa_arithmetic && denom.abs() <= f32::EPSILON {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let intercept = (sum_xx * sum_y - sum_x * sum_xy) / denom;
    let slope = (sum_w * sum_xy - sum_x * sum_y) / denom;
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        *fitted = intercept + slope * i as f32;
    }
}

pub(super) fn tfcrop_poly_fit(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
    degree: usize,
    casa_arithmetic: bool,
) {
    // CASA's LinearFit forms a small normal system before solving it. The
    // degrees used by the CASA-shaped fit need only three or four terms.
    if casa_arithmetic
        && match degree {
            2 => tfcrop_casa_poly_fit::<3>(data, flag, fit, left, right),
            3 => tfcrop_casa_poly_fit::<4>(data, flag, fit, left, right),
            _ => false,
        }
    {
        return;
    }
    let rows = (left..=right)
        .filter(|index| !flag[*index])
        .map(|index| {
            let x = index as f64 + 1.0;
            let basis = (0..=degree)
                .map(|power| x.powi(power as i32))
                .collect::<Vec<_>>();
            (basis, f64::from(data[index]), 1.0)
        })
        .collect::<Vec<_>>();
    if rows.len() <= degree {
        tfcrop_line_fit(data, flag, fit, left, right, casa_arithmetic);
        return;
    }
    let Some(solution) = solve_weighted_least_squares(&rows, degree + 1) else {
        tfcrop_line_fit(data, flag, fit, left, right, casa_arithmetic);
        return;
    };
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        let x = i as f64 + 1.0;
        *fitted = solution
            .iter()
            .enumerate()
            .map(|(power, coefficient)| coefficient * x.powi(power as i32))
            .sum::<f64>() as f32;
    }
}

pub(super) fn tfcrop_casa_poly_fit<const N: usize>(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
) -> bool {
    let mut normal = [[0.0; N]; N];
    let mut known = [0.0; N];
    let mut count = 0;
    for index in (left..=right).filter(|&index| !flag[index]) {
        let sample = f64::from(data[index]);
        let x = index as f64 + 1.0;
        let powers = std::array::from_fn::<_, N, _>(|power| x.powi(power as i32));
        for (column, &value) in powers.iter().enumerate() {
            known[column] += value * sample;
            for (other, &factor) in powers.iter().enumerate().skip(column) {
                normal[column][other] += value * factor;
            }
        }
        count += 1;
    }
    if count < N {
        return false;
    }
    // Non-SVD LinearFit uses zero collinearity tolerance and returns its
    // initial zero coefficients if inversion fails.
    let solution =
        solve_symmetric_ldlt_casacore_with_precision(normal, known, 0.0).unwrap_or([0.0; N]);
    for (index, value) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        let x = index as f64 + 1.0;
        // FlagAgentTimeFreqCrop::polyFit rounds each sum to Float.
        *value = solution
            .iter()
            .enumerate()
            .fold(0.0f32, |sum, (power, coefficient)| {
                (f64::from(sum) + coefficient * x.powi(power as i32)) as f32
            });
    }
    true
}

pub(super) fn tfcrop_mean(values: &[f32], flags: &[bool]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            sum += value;
            count += 1;
        }
    }
    if count == 0 { 0.0 } else { sum / count as f32 }
}

pub(super) fn tfcrop_std_about_fit(values: &[f32], flags: &[bool], fit: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for ((value, flag), fitted) in values.iter().zip(flags.iter()).zip(fit.iter()) {
        if !*flag {
            count += 1;
            sum += (value - fitted) * (value - fitted);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

pub(super) fn tfcrop_std_about_mean(values: &[f32], flags: &[bool], mean: f32) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            count += 1;
            sum += (value - mean) * (value - mean);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

pub(super) fn median(mut values: Vec<f64>) -> f64 {
    median_in_place(&mut values)
}

pub(super) fn median_in_place(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}
