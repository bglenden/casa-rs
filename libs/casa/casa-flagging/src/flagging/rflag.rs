// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn apply_rflag_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
    calculate: bool,
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
    let casa_rflag = request.casa_row_selection || calibration;
    let time_average = request
        .clip_time_bin
        .as_deref()
        .filter(|_| public_ms)
        .map(time_bin_seconds);
    let widths = request.clip_channel_bins.as_deref().filter(|_| public_ms);
    let average = view.is_some()
        || time_average.flatten().is_some()
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let correlations = public_ms
        .then(|| correlation::RflagCorrelations::read(ms, request.correlation.as_deref(), column))
        .transpose()?;
    let corr_admitted = correlations.as_ref().is_none_or(|c| c.admitted);
    let active = request.rflag_window.is_some() && time_average != Some(None) && corr_admitted;
    let (mut columns, configures_averaging) = if public_ms {
        autoflag_columns(
            ms,
            column,
            view.as_deref()
                .map_or(selected_rows, SharedFlagView::input_rows),
            model_factory,
            active,
            average,
            FlagDataMode::Rflag,
        )?
    } else {
        (None, false)
    };
    let active = active && (!public_ms || columns.is_some());
    if (public_ms && !configures_averaging) || (!active && !request.extendflags) {
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
            fit: active,
            seconds: time_average.flatten().filter(|_| configures_averaging),
            widths,
            input: AveragedAgentInput {
                time: averaged_time,
                uv: averaged_uv,
                view: view.as_deref_mut(),
            },
        },
    )?;
    let samples = loaded.samples;
    let projection = calibration
        .then(|| calibration::automatic_projection(request, &samples))
        .transpose()?;
    let context = RflagContext {
        casa_arithmetic: casa_rflag,
        correlations: correlations.as_ref(),
        calibration: projection.as_ref(),
        model_roundoff: &loaded.model_roundoff,
    };
    let loaded_ms = started.elapsed().as_millis();
    let groups = if let Some(view) = view.as_deref() {
        view.groups(&samples)
    } else if casa_rflag {
        ordered_autoflag_groups(ms, selected_rows, &samples, request)?
    } else {
        group_samples(&samples)
            .into_values()
            .map(|samples| (AutoflagChunk::default(), samples))
            .collect()
    };
    let fit_groups = time_selected_autoflag_groups(&groups, &loaded.axes);
    if !calculate && active && casa_rflag {
        validate_rflag_matrix_scales(&fit_groups, request, context)?;
    }
    if calculate {
        let thresholds = if active {
            rflag_threshold_maps_for_groups(&fit_groups, request, context)?
        } else {
            RFlagThresholds::default()
        };
        return Ok(ChangeSet {
            selected_rows: loaded.axes.time_selected_rows(averaged_time),
            rflag_report: (active || request.extendflags)
                .then(|| thresholds.public_report(request, active)),
            ..ChangeSet::default()
        });
    }

    let mut modified = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    let RflagFitOutcome {
        thresholds,
        buffer_cutoffs,
        threshold_ms,
    } = if active {
        fit_rflag_chunks(
            &fit_groups,
            request,
            context,
            &mut TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            },
        )?
    } else {
        RflagFitOutcome::default()
    };
    if request.extendflags && view.is_none() {
        let fit_flags = loaded
            .axes
            .flags_by_bin
            .as_ref()
            .unwrap_or(&loaded.flags_by_row);
        if casa_rflag && channel_selections.is_some() && loaded.axes.channels_by_row.is_empty() {
            for (_, group) in &groups {
                request.check_cancelled()?;
                extend_tfcrop_selected_channels(group, fit_flags, &mut modified);
            }
        } else {
            extend_autoflag_grouped(
                groups.iter().map(|(_, samples)| samples.as_slice()),
                &samples,
                &mut modified,
            );
        }
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "rflag",
            "implementation": "casa-rs-casa-shaped-rflag",
            "selected_rows": selected_rows.len(),
            "samples": samples.len(),
            "groups": groups.len(),
            "timedev_by_field_spw": format_threshold_map(&thresholds.timedev),
            "freqdev_by_field_spw": format_threshold_map(&thresholds.freqdev),
            "time_candidate_samples": count_mask(&time_flags),
            "freq_candidate_samples": count_mask(&freq_flags),
            "union_candidate_samples": union_candidates,
            "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
            "timing_ms": {
                "load": loaded_ms,
                "threshold": threshold_ms,
                "plan": planned_ms.saturating_sub(loaded_ms + threshold_ms),
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
    changes.thresholds = if casa_rflag {
        buffer_cutoffs
            .first()
            .copied()
            .filter(|cutoff| buffer_cutoffs.iter().all(|candidate| candidate == cutoff))
    } else {
        Some(thresholds.representative_pair())
    };
    trace_flagdata(|| {
        json!({
            "mode": "rflag",
            "implementation": "casa-rs-casa-shaped-rflag",
            "timing_ms": {
                "apply": applied_ms.saturating_sub(planned_ms),
                "total_before_save": applied_ms,
            },
        })
    });
    Ok(changes)
}

pub(super) struct AutomaticSampleSource<'a> {
    pub(super) columns: Option<&'a mut ClipColumns>,
    pub(super) fit: bool,
    pub(super) seconds: Option<f64>,
    pub(super) widths: Option<&'a [u32]>,
    pub(super) input: AveragedAgentInput<'a>,
}

pub(super) fn load_automatic_samples(
    ms: &MeasurementSet,
    rows: &[usize],
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
    source: AutomaticSampleSource<'_>,
) -> Result<LoadedSamples, FlaggingError> {
    let AutomaticSampleSource {
        mut columns,
        fit,
        seconds,
        widths,
        input,
    } = source;
    let column = request.data_column;
    if let Some(view) = input.view {
        let path = display_ms_path(ms);
        if let Some(columns) = &mut columns {
            view.prepare_column(ms, column, columns, &path)?;
        }
        return view.load_samples(
            SharedAgentSelection {
                rows: Some(rows),
                channels,
                correlations: None,
                time: input.time,
                uv: input.uv,
            },
            columns.map(|_| column),
            request,
            &path,
        );
    }
    if let Some(seconds) = seconds {
        return load_time_averaged_tfcrop(
            ms,
            rows,
            channels,
            columns,
            request,
            clip_time::TimeAveraging {
                seconds,
                filter: input.time,
            },
            widths,
        );
    }
    load_samples(
        ms,
        rows,
        channels,
        fit.then(|| {
            columns.map_or(SampleData::Stored(column), |operands| {
                SampleData::Visibility(column, operands)
            })
        }),
        None,
        SampleLoadOptions {
            widths,
            cancellation: request.cancellation.as_ref(),
            ..SampleLoadOptions::default()
        },
    )
}

pub(super) fn time_selected_autoflag_groups<'a, K: Copy>(
    groups: &'a [(K, Vec<Sample>)],
    axes: &AveragedSampleAxes,
) -> Cow<'a, [(K, Vec<Sample>)]> {
    if axes.rows_outside_time.is_empty() {
        return Cow::Borrowed(groups);
    }
    Cow::Owned(
        groups
            .iter()
            .filter_map(|(key, samples)| {
                let selected = samples
                    .iter()
                    .copied()
                    .filter(|sample| !axes.rows_outside_time.contains(&sample.row))
                    .collect::<Vec<_>>();
                (!selected.is_empty()).then_some((*key, selected))
            })
            .collect(),
    )
}

#[derive(Clone, Copy, Default)]
pub(super) struct RflagContext<'a> {
    pub(super) casa_arithmetic: bool,
    pub(super) correlations: Option<&'a correlation::RflagCorrelations>,
    pub(super) calibration: Option<&'a calibration::CalibrationProjection>,
    pub(super) model_roundoff: &'a [f32],
}

pub(super) fn rflag_group_projections(
    group: &[Sample],
    description: Option<i32>,
    context: RflagContext<'_>,
) -> Result<Vec<ClipProjection>, FlaggingError> {
    if let Some((selection, ddid)) = context.correlations.zip(description) {
        return selection
            .for_description(ddid)
            .map(<[ClipProjection]>::to_vec);
    }
    Ok(samples_by_corr(group)
        .into_iter()
        .filter(|plane| {
            context
                .calibration
                .is_none_or(|projection| projection.includes(plane[0].corr))
        })
        .map(|plane| ClipProjection::Stored(plane[0].corr))
        .collect())
}

pub(super) fn validate_rflag_matrix_scales(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
) -> Result<(), FlaggingError> {
    for (scale_name, scale, thresholds) in [
        (
            "timedevscale",
            request.timedevscale,
            &request.timedev_by_field_spw,
        ),
        (
            "freqdevscale",
            request.freqdevscale,
            &request.freqdev_by_field_spw,
        ),
    ] {
        if scale > 0.0 || thresholds.is_empty() {
            continue;
        }
        for (chunk, group) in groups {
            let Some(first) = group.first() else {
                continue;
            };
            if !thresholds
                .get(&(first.field, first.spw))
                .is_some_and(|&value| value > 0.0)
            {
                continue;
            }
            let ddid = context.correlations.map(|_| chunk.0[4]);
            if !rflag_group_projections(group, ddid, context)?.is_empty() {
                return Err(FlaggingError::InvalidRFlagScale(scale_name));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct RflagFitOutcome {
    pub(super) thresholds: RFlagThresholds,
    pub(super) buffer_cutoffs: Vec<(f64, f64)>,
    pub(super) threshold_ms: u128,
}

pub(super) fn fit_rflag_chunks(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    masks: &mut TfcropFitMasks<'_>,
) -> Result<RflagFitOutcome, FlaggingError> {
    let mut thresholds = RFlagThresholds::default();
    let mut buffer_cutoffs = Vec::new();
    let mut threshold_ms = 0;
    let mut roundoff_buf = if context.model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; masks.modified.len()]
    };
    // RFlag applies each buffer's own fitted thresholds. The calculate report
    // instead accumulates all buffers for each field/SPW.
    for chunk in groups.chunk_by(|(left, _), (right, _)| left == right) {
        request.check_cancelled()?;
        let threshold_started = Instant::now();
        let chunk_thresholds =
            rflag_threshold_maps_with_roundoff(chunk, request, context, &mut roundoff_buf)?
                .scaled(request, context.casa_arithmetic);
        threshold_ms += threshold_started.elapsed().as_millis();
        for (chunk_key, group) in chunk {
            let ddid = context.correlations.map(|_| chunk_key.0[4]);
            rflag_group_flags(
                ddid,
                group,
                &chunk_thresholds,
                request,
                context,
                &mut roundoff_buf,
                masks,
            )?;
        }
        if context.casa_arithmetic {
            let ([observation, array, scan, field, window], _) = chunk[0].0;
            let cutoff = chunk_thresholds.representative_pair();
            buffer_cutoffs.push(cutoff);
            trace_flagdata(|| {
                json!({
                    "mode": "rflag",
                    "calibration_buffer": {
                        "observation": observation,
                        "array": array,
                        "scan": scan,
                        "field": field,
                        "spw": window,
                    },
                    "timedev": cutoff.0,
                    "freqdev": cutoff.1,
                })
            });
        } else {
            thresholds = chunk_thresholds;
        }
    }
    Ok(RflagFitOutcome {
        thresholds,
        buffer_cutoffs,
        threshold_ms,
    })
}

pub(super) fn extend_autoflag_grouped<'a>(
    groups: impl IntoIterator<Item = &'a [Sample]>,
    samples: &[Sample],
    modified: &mut [bool],
) {
    let mut grown = modified.to_vec();
    for group in groups {
        for by_corr in samples_by_corr(group) {
            for by_row in sample_row_runs(&by_corr) {
                if percent_modified(by_row, modified) >= 80.0 {
                    for sample in by_row {
                        grown[sample.index] = true;
                    }
                }
            }
            for by_chan in samples_by_chan_dense(&by_corr) {
                if percent_modified(&by_chan, modified) >= 50.0 {
                    for sample in &by_chan {
                        grown[sample.index] = true;
                    }
                }
            }
        }
    }
    modified.copy_from_slice(&grown);
    for group in samples_by_row_chan(samples).values() {
        if group
            .iter()
            .any(|sample| sample.flag || modified[sample.index])
        {
            for sample in group {
                modified[sample.index] = true;
            }
        }
    }
}

pub(super) fn rflag_threshold_maps_for_groups(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
) -> Result<RFlagThresholds, FlaggingError> {
    if request.rflag_window.is_none() {
        return Ok(RFlagThresholds::default());
    }
    let mut roundoff_buf = if context.model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; context.model_roundoff.len()]
    };
    rflag_threshold_maps_with_roundoff(groups, request, context, &mut roundoff_buf)
}

pub(super) fn rflag_threshold_maps_with_roundoff(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
) -> Result<RFlagThresholds, FlaggingError> {
    let Some(window_size) = request.rflag_window else {
        return Ok(RFlagThresholds::default());
    };
    let mut histograms = RFlagHistograms::default();
    for (chunk_key, group) in groups {
        request.check_cancelled()?;
        let ddid = context.correlations.map(|_| chunk_key.0[4]);
        rflag_accumulate_group_histograms(
            ddid,
            group,
            &mut histograms,
            window_size,
            context,
            roundoff_buf,
        )?;
    }
    Ok(histograms.into_thresholds(request, context.casa_arithmetic))
}

#[derive(Debug, Default)]
pub(super) struct RFlagHistograms {
    pub(super) time: BTreeMap<FieldSpwKey, ChannelHistogram>,
    pub(super) freq: BTreeMap<FieldSpwKey, ChannelHistogram>,
}

impl RFlagHistograms {
    /// Unscaled thresholds for the public calculate report and flagging.
    /// CASA applies the scale factors only when testing for flags.
    fn into_thresholds(self, request: &FlagDataRequest, casa_arithmetic: bool) -> RFlagThresholds {
        let cutoffs = |histograms: BTreeMap<FieldSpwKey, ChannelHistogram>,
                       scalar: Option<f64>,
                       supplied: &BTreeMap<FieldSpwKey, f64>| {
            histograms
                .into_iter()
                .map(|(key, histogram)| {
                    let threshold = supplied
                        .get(&key)
                        .copied()
                        .filter(|&value| value > 0.0)
                        .or(scalar.filter(|&value| !casa_arithmetic || value > 0.0))
                        .unwrap_or_else(|| histogram.threshold());
                    (key, threshold)
                })
                .collect()
        };
        RFlagThresholds {
            timedev: cutoffs(self.time, request.timedev, &request.timedev_by_field_spw),
            freqdev: cutoffs(self.freq, request.freqdev, &request.freqdev_by_field_spw),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RFlagThresholds {
    pub(super) timedev: BTreeMap<FieldSpwKey, f64>,
    pub(super) freqdev: BTreeMap<FieldSpwKey, f64>,
}

impl RFlagThresholds {
    pub(super) fn scaled(mut self, request: &FlagDataRequest, casa_arithmetic: bool) -> Self {
        // CASA uses -1 for an unavailable applied cutoff. A negative scalar
        // skips the fitted cutoff, and a negative scale skips the prepass
        // histogram. CASA scales its sentinel together with defined cutoffs.
        let applied = |threshold: f64, scalar: Option<f64>, supplied: Option<f64>, scale: f64| {
            if let Some(value) = supplied {
                return if value > 0.0 { value * scale } else { -scale };
            }
            if casa_arithmetic
                && !scalar.is_some_and(|value| value > 0.0)
                && (threshold == 0.0 || scale < 0.0 || scalar.is_some_and(|value| value < 0.0))
            {
                -scale
            } else {
                threshold * scale
            }
        };
        for (key, value) in &mut self.timedev {
            *value = applied(
                *value,
                request.timedev,
                request.timedev_by_field_spw.get(key).copied(),
                request.timedevscale,
            );
        }
        for (key, value) in &mut self.freqdev {
            *value = applied(
                *value,
                request.freqdev,
                request.freqdev_by_field_spw.get(key).copied(),
                request.freqdevscale,
            );
        }
        self
    }

    fn public_report(&self, request: &FlagDataRequest, active: bool) -> RFlagThresholdReport {
        let rows = |values: &BTreeMap<FieldSpwKey, f64>,
                    scalar: Option<f64>,
                    supplied: &BTreeMap<FieldSpwKey, f64>| {
            if !active || request.rflag_window.is_none() {
                return Vec::new();
            }
            let mut report = supplied.clone();
            if !scalar.is_some_and(|value| value > 0.0) {
                report.extend(values);
            }
            report
                .into_iter()
                .map(|((field, spw), value)| [f64::from(field), f64::from(spw), value])
                .collect()
        };
        RFlagThresholdReport {
            timedev: rows(
                &self.timedev,
                request.timedev,
                &request.timedev_by_field_spw,
            ),
            freqdev: rows(
                &self.freqdev,
                request.freqdev,
                &request.freqdev_by_field_spw,
            ),
        }
    }

    pub(super) fn representative_pair(&self) -> (f64, f64) {
        (
            representative_threshold(&self.timedev),
            representative_threshold(&self.freqdev),
        )
    }
}

#[derive(Debug, Clone)]
pub(super) struct ChannelHistogram {
    pub(super) sum: Vec<f64>,
    pub(super) counts: Vec<f64>,
}

impl ChannelHistogram {
    pub(super) fn new(nchan: usize) -> Self {
        Self {
            sum: vec![0.0; nchan],
            counts: vec![0.0; nchan],
        }
    }

    pub(super) fn add(&mut self, chan: usize, value: f64) {
        if let (Some(sum), Some(count)) = (self.sum.get_mut(chan), self.counts.get_mut(chan)) {
            *sum += value;
            *count += 1.0;
        }
    }

    pub(super) fn threshold(&self) -> f64 {
        let samples = self
            .sum
            .iter()
            .zip(self.counts.iter())
            .map(|(sum, count)| if *count > 0.0 { *sum / *count } else { 0.0 })
            .collect::<Vec<_>>();
        let med = median(samples.clone());
        let mad = median(samples.iter().map(|value| (*value - med).abs()).collect());
        med + 1.4826 * mad
    }
}

pub(super) fn representative_threshold(thresholds: &BTreeMap<FieldSpwKey, f64>) -> f64 {
    if thresholds.is_empty() {
        return f64::INFINITY;
    }
    median(thresholds.values().copied().collect())
}

pub(super) fn rflag_accumulate_group_histograms(
    description: Option<i32>,
    group: &[Sample],
    histograms: &mut RFlagHistograms,
    window_size: usize,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
) -> Result<(), FlaggingError> {
    let Some(first) = group.first() else {
        return Ok(());
    };
    let planes = samples_by_corr(group);
    let projections = rflag_group_projections(group, description, context)?;
    if projections.is_empty() {
        return Ok(());
    }
    let key = (first.field, first.spw);
    // CASA histograms have one bin per selected channel.
    let channels = context.casa_arithmetic.then(|| {
        let mut channels = unique_sample_chans(group);
        channels.sort_unstable();
        channels
    });
    let nchan = channels.as_ref().map_or_else(
        || group.iter().map(|sample| sample.chan).max().unwrap_or(0) + 1,
        Vec::len,
    );
    let index = |channel| {
        channels.as_ref().map_or(channel, |channels| {
            channels.binary_search(&channel).expect("loaded channel")
        })
    };
    histograms
        .time
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    histograms
        .freq
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    for projection in projections {
        let Some(series) = tfcrop_series(
            &planes,
            projection,
            None,
            &[],
            context.model_roundoff,
            roundoff_buf,
        ) else {
            continue;
        };
        let time_hist = histograms
            .time
            .get_mut(&key)
            .expect("initialized time histogram");
        for by_chan in samples_by_chan_dense(&series.samples) {
            for (start, stop) in rflag_time_windows(by_chan.len(), window_size) {
                let window = &by_chan[start..=stop];
                if let Some(value) = rflag_time_std_total(
                    window,
                    context.casa_arithmetic,
                    RflagRoundoff::Slice(roundoff_buf),
                ) {
                    time_hist.add(index(by_chan[0].chan), value);
                }
            }
        }
        let freq_hist = histograms
            .freq
            .get_mut(&key)
            .expect("initialized frequency histogram");
        for by_row in sample_row_runs(&series.samples) {
            let stats = rflag_spectral_stats(
                by_row,
                context.casa_arithmetic,
                RflagRoundoff::Slice(roundoff_buf),
            );
            let row_roundoff = rflag_row_roundoff(by_row, RflagRoundoff::Slice(roundoff_buf));
            for sample in by_row {
                if sample.flag {
                    continue;
                }
                let bound = f64::from(
                    roundoff_buf.get(sample.index).copied().unwrap_or(0.0) + row_roundoff,
                );
                if stats.sum_weight_real > 0.0 {
                    freq_hist.add(
                        index(sample.chan),
                        rflag_clamped_deviation((sample.real - stats.average_real).abs(), bound),
                    );
                }
                if stats.sum_weight_imag > 0.0 {
                    freq_hist.add(
                        index(sample.chan),
                        rflag_clamped_deviation((sample.imag - stats.average_imag).abs(), bound),
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum RflagRoundoff<'a> {
    Slice(&'a [f32]),
    Sparse(&'a HashMap<usize, f32>),
}

impl RflagRoundoff<'_> {
    fn is_empty(self) -> bool {
        match self {
            Self::Slice(slice) => slice.is_empty(),
            Self::Sparse(map) => map.is_empty(),
        }
    }

    fn get(self, index: usize) -> f32 {
        match self {
            Self::Slice(slice) => slice.get(index).copied().unwrap_or(0.0),
            Self::Sparse(map) => map.get(&index).copied().unwrap_or(0.0),
        }
    }
}

pub(super) struct RflagProjectionSeries {
    pub(super) rows: Vec<Vec<Sample>>,
    pub(super) chans: Vec<Vec<Sample>>,
    pub(super) partners: HashMap<usize, usize>,
    pub(super) roundoff: HashMap<usize, f32>,
}

pub(super) fn rflag_projection_series(
    planes: &[Vec<Sample>],
    projection: ClipProjection,
    model_roundoff: &[f32],
    roundoff_buf: &mut [f32],
) -> Option<RflagProjectionSeries> {
    let series = tfcrop_series(planes, projection, None, &[], model_roundoff, roundoff_buf)?;
    let rows = sample_row_runs(&series.samples)
        .into_iter()
        .map(<[Sample]>::to_vec)
        .collect::<Vec<_>>();
    let chans = samples_by_chan_dense(&series.samples);
    let roundoff = if roundoff_buf.is_empty() {
        HashMap::new()
    } else {
        series
            .samples
            .iter()
            .map(|sample| {
                (
                    sample.index,
                    roundoff_buf.get(sample.index).copied().unwrap_or(0.0),
                )
            })
            .collect()
    };
    Some(RflagProjectionSeries {
        rows,
        chans,
        partners: series.pairs.into_iter().collect(),
        roundoff,
    })
}

pub(super) fn is_rflag_flagged(
    sample: &Sample,
    partners: &HashMap<usize, usize>,
    modified: &[bool],
) -> bool {
    sample.flag
        || modified[sample.index]
        || partners
            .get(&sample.index)
            .is_some_and(|&partner| modified[partner])
}

pub(super) fn mark_rflag_flagged(
    partners: &HashMap<usize, usize>,
    index: usize,
    axis_flags: &mut [bool],
    modified: &mut [bool],
) {
    axis_flags[index] = true;
    modified[index] = true;
    if let Some(&partner) = partners.get(&index) {
        axis_flags[partner] = true;
        modified[partner] = true;
    }
}

pub(super) fn rflag_group_flags(
    description: Option<i32>,
    group: &[Sample],
    thresholds: &RFlagThresholds,
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
    masks: &mut TfcropFitMasks<'_>,
) -> Result<(), FlaggingError> {
    let Some(window_size) = request.rflag_window else {
        return Ok(());
    };
    let Some(first) = group.first() else {
        return Ok(());
    };
    let key = (first.field, first.spw);
    let timedev = thresholds
        .timedev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    let freqdev = thresholds
        .freqdev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    let planes = samples_by_corr(group);
    let mut projections = Vec::new();
    for projection in rflag_group_projections(group, description, context)? {
        if let Some(series) =
            rflag_projection_series(&planes, projection, context.model_roundoff, roundoff_buf)
        {
            projections.push(series);
        }
    }
    let row_count = projections.first().map_or(0, |series| series.rows.len());
    let do_time = timedev >= 0.0 || !context.casa_arithmetic;
    let do_freq = freqdev >= 0.0 || !context.casa_arithmetic;
    for timestep in 0..row_count {
        if do_time
            && let Some((start, stop)) = rflag_time_window_at(row_count, window_size, timestep)
        {
            for series in &projections {
                let roundoff = RflagRoundoff::Sparse(&series.roundoff);
                for by_chan in &series.chans {
                    let Some(window) = by_chan.get(start..=stop) else {
                        continue;
                    };
                    if rflag_time_std_total(window, context.casa_arithmetic, roundoff)
                        .is_some_and(|deviation| deviation > timedev)
                    {
                        for sample in window {
                            if !is_rflag_flagged(sample, &series.partners, masks.modified) {
                                mark_rflag_flagged(
                                    &series.partners,
                                    sample.index,
                                    masks.time_flags,
                                    masks.modified,
                                );
                            }
                        }
                    }
                }
            }
        }
        if do_freq {
            for series in &projections {
                let Some(by_row) = series.rows.get(timestep) else {
                    continue;
                };
                let roundoff = RflagRoundoff::Sparse(&series.roundoff);
                let stats = rflag_spectral_stats(by_row, context.casa_arithmetic, roundoff);
                let row_roundoff = rflag_row_roundoff(by_row, roundoff);
                let flag_all = stats.std_real > request.spectralmax
                    || stats.std_imag > request.spectralmax
                    || stats.std_real < request.spectralmin
                    || stats.std_imag < request.spectralmin;
                for sample in by_row {
                    if is_rflag_flagged(sample, &series.partners, masks.modified) {
                        continue;
                    }
                    let bound = f64::from(roundoff.get(sample.index) + row_roundoff);
                    let dev_real =
                        rflag_clamped_deviation((sample.real - stats.average_real).abs(), bound);
                    let dev_imag =
                        rflag_clamped_deviation((sample.imag - stats.average_imag).abs(), bound);
                    if flag_all || dev_real > freqdev || dev_imag > freqdev {
                        mark_rflag_flagged(
                            &series.partners,
                            sample.index,
                            masks.freq_flags,
                            masks.modified,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn rflag_time_window_at(
    len: usize,
    window_size: usize,
    center: usize,
) -> Option<(usize, usize)> {
    let effective = len.min(window_size);
    let delta = effective.saturating_sub(1) / 2;
    if delta == 0 {
        (center < len).then_some((center, center))
    } else if center >= delta && center < len - delta {
        Some((center - delta, center + delta))
    } else {
        None
    }
}

pub(super) fn rflag_time_windows(
    len: usize,
    window_size: usize,
) -> impl Iterator<Item = (usize, usize)> {
    (0..len).filter_map(move |center| rflag_time_window_at(len, window_size, center))
}

pub(super) fn rflag_time_std_total(
    samples: &[Sample],
    casa_arithmetic: bool,
    roundoff: RflagRoundoff<'_>,
) -> Option<f64> {
    // CASA's visibility mapper supplies Float values and sums v and v * v in Double.
    let term = |value: f64| {
        if casa_arithmetic {
            let value = value as f32;
            (f64::from(value), f64::from(value * value))
        } else {
            (value, value * value)
        }
    };
    let mut count = 0usize;
    let mut sum_real = 0.0;
    let mut sum_imag = 0.0;
    let mut sumsq_real = 0.0;
    let mut sumsq_imag = 0.0;
    for sample in samples {
        if sample.flag {
            continue;
        }
        count += 1;
        let (real, real_sq) = term(sample.real);
        let (imag, imag_sq) = term(sample.imag);
        sum_real += real;
        sum_imag += imag;
        sumsq_real += real_sq;
        sumsq_imag += imag_sq;
    }
    if count == 0 {
        return (!casa_arithmetic).then_some(0.0);
    }
    let sum_weight = count as f64;
    let avg_real = sum_real / sum_weight;
    let avg_imag = sum_imag / sum_weight;
    if !roundoff.is_empty()
        && samples.iter().filter(|sample| !sample.flag).all(|sample| {
            let bound = f64::from(roundoff.get(sample.index)) * 2.0;
            (sample.real - avg_real).abs() <= bound && (sample.imag - avg_imag).abs() <= bound
        })
    {
        return Some(0.0);
    }
    let var_real = sumsq_real / sum_weight - avg_real * avg_real;
    let var_imag = sumsq_imag / sum_weight - avg_imag * avg_imag;
    Some((var_real.max(0.0) + var_imag.max(0.0)).sqrt())
}

pub(super) fn rflag_row_roundoff(samples: &[Sample], roundoff: RflagRoundoff<'_>) -> f32 {
    if roundoff.is_empty() {
        return 0.0;
    }
    samples
        .iter()
        .filter(|sample| !sample.flag)
        .map(|sample| roundoff.get(sample.index))
        .filter(|bound| bound.is_finite())
        .fold(0.0, f32::max)
}

pub(super) fn rflag_clamped_deviation(deviation: f64, roundoff_bound: f64) -> f64 {
    if roundoff_bound > 0.0 && deviation <= roundoff_bound {
        0.0
    } else {
        deviation
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RFlagSpectralStats {
    pub(super) average_real: f64,
    pub(super) average_imag: f64,
    pub(super) std_real: f64,
    pub(super) std_imag: f64,
    pub(super) sum_weight_real: f64,
    pub(super) sum_weight_imag: f64,
}

pub(super) fn rflag_spectral_stats(
    samples: &[Sample],
    casa_arithmetic: bool,
    roundoff: RflagRoundoff<'_>,
) -> RFlagSpectralStats {
    let mut real_values = Vec::with_capacity(samples.len());
    let mut imag_values = Vec::with_capacity(samples.len());
    for sample in samples {
        if sample.flag {
            continue;
        }
        real_values.push(sample.real);
        imag_values.push(sample.imag);
    }
    if real_values.is_empty() {
        return RFlagSpectralStats::default();
    }
    let average_real = median_in_place(&mut real_values);
    let average_imag = median_in_place(&mut imag_values);
    let row_bound = f64::from(rflag_row_roundoff(samples, roundoff)) * 2.0;
    let residual = |value: f64, reference: f64| {
        let delta = value - reference;
        if row_bound > 0.0 && delta.abs() <= row_bound {
            0.0
        } else if casa_arithmetic {
            delta
        } else {
            delta.abs()
        }
    };
    for value in &mut real_values {
        *value = residual(*value, average_real);
    }
    for value in &mut imag_values {
        *value = residual(*value, average_imag);
    }
    let std_real = 1.4826 * median_in_place(&mut real_values);
    let std_imag = 1.4826 * median_in_place(&mut imag_values);
    let weight = real_values.len() as f64;
    RFlagSpectralStats {
        average_real,
        average_imag,
        std_real,
        std_imag,
        sum_weight_real: weight,
        sum_weight_imag: weight,
    }
}

pub(super) fn format_threshold_map(
    thresholds: &BTreeMap<FieldSpwKey, f64>,
) -> BTreeMap<String, f64> {
    thresholds
        .iter()
        .map(|((field, spw), value)| (format!("field{field}_spw{spw}"), *value))
        .collect()
}
