// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn load_bool_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<bool>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Bool(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Bool scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

pub(super) fn load_i32_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<i32>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Int32(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Int32 scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

pub(super) enum SampleData<'a> {
    Stored(FlagDataColumn),
    Visibility(FlagDataColumn, &'a mut ClipColumns),
}

pub(super) fn load_time_averaged_tfcrop(
    ms: &MeasurementSet,
    selected: &[usize],
    selections: Option<&BTreeMap<i32, ChannelSelection>>,
    mut columns: Option<&mut ClipColumns>,
    request: &FlagDataRequest,
    averaging: clip_time::TimeAveraging<'_>,
    widths: Option<&[u32]>,
) -> Result<LoadedSamples, FlaggingError> {
    let path = display_ms_path(ms);
    let ddid_to_spw = data_description_spw_map(ms)?;
    let timed = clip_time::ordered_baseline_chunks(
        ms,
        selected,
        request.time_chunk,
        request.combinescans,
        &path,
        request.cancellation.as_ref(),
    )?;
    let flags_by_row = selected
        .iter()
        .copied()
        .zip(clip_time::read_flags(ms.main_table(), selected, &path)?)
        .collect::<BTreeMap<_, _>>();
    let spw_for = |ddid: i32| {
        ddid_to_spw
            .get(&ddid)
            .copied()
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
            })
    };
    let rows_per_block = if let Some(columns) = columns.as_mut() {
        if columns.uses_weight_spectrum() {
            let mut first_rows = BTreeMap::<i32, usize>::new();
            for row in &timed {
                first_rows
                    .entry(spw_for(row.description())?)
                    .and_modify(|first| *first = (*first).min(row.row))
                    .or_insert(row.row);
            }
            columns.prepare_spectrum_fallbacks(ms.main_table(), &first_rows, &path)?;
        }
        let bytes_per_sample = columns.bytes_per_sample(ms.main_table(), &path)?;
        flag_read_plan(ms, selected.len(), bytes_per_sample, true)?
            .rows_per_block
            .max(1)
    } else {
        1
    };
    let mut axes = AveragedSampleAxes::default();
    let mut samples = Vec::new();
    let mut model_roundoff = Vec::new();
    let mut flags_by_bin = BTreeMap::new();
    let mut channel_layouts = HashMap::<(i32, usize), Arc<[usize]>>::new();
    for group in averaging.groups(&timed) {
        request.check_cancelled()?;
        if averaging.filter.is_some() {
            let last_unflagged = group
                .iter()
                .rfind(|entry| flags_by_row[&entry.row].iter().any(|&flag| !flag))
                .map(clip_time::ClipTimedRow::time);
            if !averaging.holds(group, last_unflagged) {
                if !request.extendflags {
                    continue;
                }
                // The implicit Extend agent has no local timerange.
                axes.rows_outside_time.insert(group[0].row);
            }
        }
        let first = &group[0];
        let ddid = first.description();
        let spw = spw_for(ddid)?;
        let row = first.row;
        let nchan = flags_by_row[&row].shape()[1];
        let (channels, width) =
            clip_channel_selection(Some(ddid), &ddid_to_spw, selections, widths, nchan, &path)?;
        let channels = channels.unwrap_or_else(|| (0..nchan).collect());
        let [ant1, ant2] = first.antennas();
        let metadata = RowSampleMetadata {
            row,
            field: first.field(),
            spw,
            scan: first.chunk_key().0[2],
            ant1,
            ant2,
        };
        let mut average = columns
            .as_mut()
            .map(|columns| {
                clip_time::collect_time_bin(
                    ms,
                    group,
                    rows_per_block,
                    columns,
                    &ddid_to_spw,
                    &path,
                    request.cancellation.as_ref(),
                )
            })
            .transpose()?
            .map(|(average, _, _)| average);
        let combined = average
            .as_mut()
            .map(|average| {
                average.reduced_visibility(request.data_column, Some(&channels), width, row, &path)
            })
            .transpose()?;
        let averaged_flags;
        let flag_only_bins;
        let (flags, bins) = if let Some(combined) = &combined {
            (combined.flags, combined.bins.as_ref())
        } else {
            averaged_flags = time_bin_flags(group, &flags_by_row, &path)?;
            flag_only_bins = ChannelBins::new(width, &channels, &averaged_flags);
            (&averaged_flags, flag_only_bins.as_ref())
        };
        let binned_channels;
        let (fit_flags, fit_channels) = if let Some(bins) = bins {
            binned_channels = (0..bins.flags().shape()[1]).collect::<Vec<_>>();
            (bins.flags(), binned_channels.as_slice())
        } else {
            (flags, channels.as_slice())
        };
        let first_sample = samples.len();
        if let Some(combined) = &combined {
            push_row_samples(
                &combined.data,
                fit_flags,
                fit_channels,
                metadata,
                &path,
                request.data_column.name(),
                &mut samples,
            )?;
            if let Some(roundoff) = combined.fourier_roundoff {
                model_roundoff.resize(first_sample, 0.0);
                model_roundoff.extend(samples[first_sample..].iter().map(|sample| {
                    if let Some(bins) = bins {
                        bins.original_channels(sample.chan)
                            .iter()
                            .filter(|&&chan| flags[[sample.corr, chan]] == sample.flag)
                            .map(|&chan| roundoff[[sample.corr, chan]])
                            .fold(0.0, f32::max)
                    } else {
                        roundoff[[sample.corr, sample.chan]]
                    }
                }));
            }
        } else {
            push_flag_samples(fit_flags, fit_channels, None, metadata, &mut samples);
        }
        if selections.is_some() && bins.is_none() {
            flags_by_bin.insert(row, flags.clone());
        }
        if let Some(width) = bins.map(ChannelBins::width) {
            let channels = channel_layouts
                .entry((spw, nchan))
                .or_insert_with(|| channels.into())
                .clone();
            axes.channels_by_row
                .insert(row, AveragedChannels { channels, width });
        }
        axes.rows_by_bin
            .insert(row, group.iter().map(|entry| entry.row).collect());
    }
    axes.flags_by_bin = (!flags_by_bin.is_empty()).then_some(flags_by_bin);
    Ok(LoadedSamples {
        samples,
        flags_by_row,
        model_roundoff,
        axes,
    })
}

pub(super) fn time_bin_flags(
    rows: &[clip_time::ClipTimedRow],
    flags_by_row: &BTreeMap<usize, ArrayD<bool>>,
    path: &str,
) -> Result<ArrayD<bool>, FlaggingError> {
    let mut flags = flags_by_row[&rows[0].row].clone();
    for row in &rows[1..] {
        let next = &flags_by_row[&row.row];
        if flags.shape() != next.shape() {
            return Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "TFCrop time bin contains an inconsistent FLAG shape at row {}",
                    row.row
                ),
            });
        }
        ndarray::Zip::from(&mut flags)
            .and(next)
            .for_each(|flag, &next| *flag &= next);
    }
    Ok(flags)
}

/// The selected rows' samples and flags. Each sample carries its selected
/// input value, or zero when the caller reads the flags alone (`None`):
/// manual flagging and extension, as CASA's, need no data column.
pub(super) fn load_samples(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    data: Option<SampleData<'_>>,
    flag_correlations: Option<&CorrelationSelection>,
    options: SampleLoadOptions<'_>,
) -> Result<LoadedSamples, FlaggingError> {
    let SampleLoadOptions {
        retain_channel_overlaps,
        widths,
        cancellation,
    } = options;
    check_cancelled(cancellation)?;
    let path = display_ms_path(ms);
    let calibration = is_calibration_table(ms);
    let ddid_to_spw = if calibration {
        BTreeMap::new()
    } else {
        data_description_spw_map(ms)?
    };
    let table = ms.main_table();
    let stored = if let Some(SampleData::Stored(column)) = data.as_ref() {
        Some(
            table
                .column_accessor(column.name())
                .and_then(|accessor| accessor.array_cells_owned(selected_rows))
                .map(|cells| (column.name(), cells))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read selected {} rows: {source}", column.name()),
                })?,
        )
    } else {
        None
    };
    check_cancelled(cancellation)?;
    let flag_cells = load_sample_flags(table, selected_rows, &path)?;
    let row_window = if calibration {
        "SPECTRAL_WINDOW_ID"
    } else {
        "DATA_DESC_ID"
    };
    let ddids = load_i32_cells(table, selected_rows, row_window, &path)?;
    check_cancelled(cancellation)?;
    let visibility = if let Some(SampleData::Visibility(column, columns)) = data {
        Some((
            column.name(),
            columns.read(
                ms,
                selected_rows,
                &flag_cells,
                Some(&ddids),
                &ddid_to_spw,
                &path,
            )?,
        ))
    } else {
        None
    };
    let fields = load_i32_cells(table, selected_rows, "FIELD_ID", &path)?;
    let scans = load_i32_cells(table, selected_rows, "SCAN_NUMBER", &path)?;
    let ant1s = load_i32_cells(table, selected_rows, "ANTENNA1", &path)?;
    let ant2s = load_i32_cells(table, selected_rows, "ANTENNA2", &path)?;
    let mut samples = Vec::new();
    let mut flags_by_row = BTreeMap::new();
    let mut model_roundoff = Vec::new();
    let mut averaged_channels = BTreeMap::new();
    let mut channel_layouts = HashMap::<(i32, usize), Arc<[usize]>>::new();
    for (row_slot, (&row, flags)) in selected_rows.iter().zip(flag_cells).enumerate() {
        check_cancelled(cancellation)?;
        let ddid = ddids[row_slot];
        let spw = if calibration {
            ddid
        } else {
            ddid_to_spw
                .get(&ddid)
                .copied()
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                })?
        };
        let nchan = flags.shape()[1];
        let (channels, width) = if widths.is_some() {
            clip_channel_selection(
                Some(ddid),
                &ddid_to_spw,
                channel_selections,
                widths,
                nchan,
                &path,
            )?
        } else {
            let channels = channel_selections
                .and_then(|selectors| selectors.get(&spw))
                .map(|selection| {
                    if retain_channel_overlaps {
                        selection.indices_with_overlaps(nchan)
                    } else {
                        selection.indices(nchan)
                    }
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("resolve SPW {spw} channels: {source}"),
                    })
                })
                .transpose()?;
            (channels, 0)
        };
        let channels = channels.unwrap_or_else(|| (0..nchan).collect());
        let bins = ChannelBins::new(width, &channels, &flags);
        let data = if let Some((column, cells)) = &stored {
            let value = checked_row_data(cells.get(row_slot), &flags, row, column, &path)?;
            Some((*column, Cow::Borrowed(value)))
        } else if let Some((column, cells)) = &visibility {
            Some((
                *column,
                cells.row(row_slot, &flags, bins.as_ref(), row, &path)?,
            ))
        } else {
            None
        };
        let binned_channels;
        let (fit_flags, fit_channels) = if let Some(bins) = &bins {
            binned_channels = (0..bins.flags().shape()[1]).collect::<Vec<_>>();
            (bins.flags(), binned_channels.as_slice())
        } else {
            (&flags, channels.as_slice())
        };
        let metadata = RowSampleMetadata {
            row,
            field: fields[row_slot],
            spw,
            scan: scans[row_slot],
            ant1: ant1s[row_slot],
            ant2: ant2s[row_slot],
        };
        let first_sample = samples.len();
        match data {
            Some((column, value)) => push_row_samples(
                value.as_ref(),
                fit_flags,
                fit_channels,
                metadata,
                &path,
                column,
                &mut samples,
            )?,
            None => push_flag_samples(
                fit_flags,
                fit_channels,
                flag_correlations.map(|selection| selection.planes(ddid)),
                metadata,
                &mut samples,
            ),
        }
        if let Some((_, cells)) = &visibility
            && let Some(roundoff) = cells.fourier_residual_roundoff(row_slot, &flags, row, &path)?
        {
            model_roundoff.resize(first_sample, 0.0);
            model_roundoff.extend(samples[first_sample..].iter().map(|sample| {
                if let Some(bins) = &bins {
                    bins.original_channels(sample.chan)
                        .iter()
                        .filter(|&&chan| flags[[sample.corr, chan]] == sample.flag)
                        .map(|&chan| roundoff[[sample.corr, chan]])
                        .fold(0.0, f32::max)
                } else {
                    roundoff[[sample.corr, sample.chan]]
                }
            }));
        }
        if let Some(width) = bins.map(|bins| bins.width()) {
            // The selection and effective width are fixed for this window
            // and input channel count. Reuse the physical axis across rows.
            let channels = channel_layouts
                .entry((spw, nchan))
                .or_insert_with(|| channels.into())
                .clone();
            averaged_channels.insert(row, AveragedChannels { channels, width });
        }
        flags_by_row.insert(row, flags);
    }
    Ok(LoadedSamples {
        samples,
        flags_by_row,
        model_roundoff,
        axes: AveragedSampleAxes {
            channels_by_row: averaged_channels,
            ..Default::default()
        },
    })
}

/// `row`'s cell of `column`: a numeric array of its flags' shape.
#[derive(Default)]
pub(super) struct SampleLoadOptions<'a> {
    pub(super) retain_channel_overlaps: bool,
    pub(super) widths: Option<&'a [u32]>,
    pub(super) cancellation: Option<&'a Cancellation>,
}

fn load_sample_flags(
    table: &Table,
    selected_rows: &[usize],
    path: &str,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    let fail = |reason| FlaggingError::MutateFlags {
        path: path.to_owned(),
        reason,
    };
    table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| column.array_cells_owned(selected_rows))
        .map_err(|source| fail(format!("read selected FLAG rows: {source}")))?
        .into_iter()
        .zip(selected_rows)
        .map(|(cell, row)| match cell {
            Some(ArrayValue::Bool(flags)) if flags.ndim() == 2 => Ok(flags),
            Some(ArrayValue::Bool(_)) => Err(fail(format!("FLAG row {row} is not rank-2"))),
            Some(other) => Err(fail(format!(
                "FLAG row {row} must be bool array, found {other:?}"
            ))),
            None => Err(fail(format!("FLAG row {row} is undefined"))),
        })
        .collect()
}

pub(super) fn checked_row_data<'a>(
    cell: Option<&'a Option<ArrayValue>>,
    flags: &ArrayD<bool>,
    row: usize,
    column: &str,
    path: &str,
) -> Result<&'a ArrayValue, FlaggingError> {
    let invalid = |reason: String| FlaggingError::MutateFlags {
        path: path.to_string(),
        reason,
    };
    let value = cell
        .and_then(Option::as_ref)
        .ok_or_else(|| invalid(format!("{column} row {row} is undefined")))?;
    let shape = numeric_array_value_shape(value).ok_or_else(|| {
        invalid(format!(
            "{column} row {row} must be numeric array, found {value:?}"
        ))
    })?;
    if shape != flags.shape() {
        return Err(invalid(format!(
            "{column} and FLAG shapes differ on row {row}: {shape:?} vs {:?}",
            flags.shape()
        )));
    }
    Ok(value)
}

/// The samples of a row read for its flags alone, each value zero.
pub(super) fn push_flag_samples(
    flags: &ArrayD<bool>,
    channels: &[usize],
    planes: Option<&[usize]>,
    metadata: RowSampleMetadata,
    samples: &mut Vec<Sample>,
) {
    for corr in 0..flags.shape()[0] {
        if planes.is_some_and(|indices| indices.binary_search(&corr).is_err()) {
            continue;
        }
        for &chan in channels {
            push_sample(samples, metadata, corr, chan, 0.0, 0.0, flags[[corr, chan]]);
        }
    }
}

/// Whether clip mode with no clip range flags the amplitude `value`: non-finite,
/// and with `zeros` a value at or below `f32::EPSILON` too, as CASA's
/// `FlagAgentBase::isNaN` and `isNaNOrZero` test it.
pub(super) fn is_casa_clip(value: f64, zeros: bool) -> bool {
    !value.is_finite() || (zeros && value <= f64::from(f32::EPSILON))
}

pub(super) fn is_casa_clip_complex32(value: Complex32, zeros: bool) -> bool {
    !value.norm().is_finite()
        || (zeros
            && f64::from(value.re * value.re + value.im * value.im)
                <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON))
}

pub(super) fn is_casa_clip_complex64(value: Complex64, zeros: bool) -> bool {
    !value.norm().is_finite()
        || (zeros
            && value.re * value.re + value.im * value.im
                <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON))
}

pub(super) fn numeric_array_value_shape(value: &ArrayValue) -> Option<&[usize]> {
    match value {
        ArrayValue::Complex32(values) => Some(values.shape()),
        ArrayValue::Complex64(values) => Some(values.shape()),
        ArrayValue::Float32(values) => Some(values.shape()),
        ArrayValue::Float64(values) => Some(values.shape()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RowSampleMetadata {
    pub(super) row: usize,
    pub(super) field: i32,
    pub(super) spw: i32,
    pub(super) scan: i32,
    pub(super) ant1: i32,
    pub(super) ant2: i32,
}

pub(super) fn push_row_samples(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    channels: &[usize],
    metadata: RowSampleMetadata,
    path: &str,
    data_column: &str,
    samples: &mut Vec<Sample>,
) -> Result<(), FlaggingError> {
    let flag_view = flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("FLAG row {} must be rank-2: {source}", metadata.row),
        })?;
    match data {
        ArrayValue::Complex32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(value.re),
                        f64::from(value.im),
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Complex64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        value.re,
                        value.im,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(values[(corr, chan)]),
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        values[(corr, chan)],
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn push_sample(
    samples: &mut Vec<Sample>,
    metadata: RowSampleMetadata,
    corr: usize,
    chan: usize,
    real: f64,
    imag: f64,
    flag: bool,
) {
    samples.push(Sample {
        index: samples.len(),
        row: metadata.row,
        corr,
        chan,
        amp: real.hypot(imag),
        real,
        imag,
        flag,
        field: metadata.field,
        spw: metadata.spw,
        scan: metadata.scan,
        ant1: metadata.ant1,
        ant2: metadata.ant2,
    });
}

pub(super) type GroupKey = (i32, i32, i32, i32, i32);
pub(super) type FieldSpwKey = (i32, i32);

pub(super) fn group_samples(samples: &[Sample]) -> BTreeMap<GroupKey, Vec<Sample>> {
    let mut groups = BTreeMap::<GroupKey, Vec<Sample>>::new();
    for sample in samples {
        let ant_low = sample.ant1.min(sample.ant2);
        let ant_high = sample.ant1.max(sample.ant2);
        groups
            .entry((sample.field, sample.spw, sample.scan, ant_low, ant_high))
            .or_default()
            .push(*sample);
    }
    groups
}

pub(super) fn samples_by_corr(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_corr) = samples.iter().map(|sample| sample.corr).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_corr + 1];
    for sample in samples {
        groups[sample.corr].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

pub(super) fn samples_by_chan_dense(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_chan) = samples.iter().map(|sample| sample.chan).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_chan + 1];
    for sample in samples {
        groups[sample.chan].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

pub(super) fn sample_row_runs(samples: &[Sample]) -> Vec<&[Sample]> {
    let mut runs = Vec::new();
    let mut start = 0usize;
    while start < samples.len() {
        let row = samples[start].row;
        let mut end = start + 1;
        while end < samples.len() && samples[end].row == row {
            end += 1;
        }
        runs.push(&samples[start..end]);
        start = end;
    }
    runs
}

pub(super) fn unique_sample_rows(samples: &[Sample]) -> Vec<usize> {
    let mut rows = Vec::new();
    let mut previous = None;
    for sample in samples {
        if previous != Some(sample.row) {
            rows.push(sample.row);
            previous = Some(sample.row);
        }
    }
    rows
}

pub(super) fn unique_sample_chans(samples: &[Sample]) -> Vec<usize> {
    let mut seen = HashSet::new();
    let mut chans = Vec::new();
    for sample in samples {
        if seen.insert(sample.chan) {
            chans.push(sample.chan);
        }
    }
    chans
}

pub(super) fn samples_by_row_chan(samples: &[Sample]) -> BTreeMap<(usize, usize), Vec<Sample>> {
    let mut groups = BTreeMap::<(usize, usize), Vec<Sample>>::new();
    for sample in samples {
        groups
            .entry((sample.row, sample.chan))
            .or_default()
            .push(*sample);
    }
    groups
}

pub(super) fn table_flag_matrix(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<ArrayD<bool>, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.array_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ArrayValue::Bool(flags) => Ok(flags.clone()),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool array, found {other:?}"),
        }),
    }
}

pub(super) fn table_bool(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<bool, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Bool(value) => Ok(*value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool scalar, found {other:?}"),
        }),
    }
}

pub(super) fn table_i32(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<i32, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Int32 scalar, found {other:?}"),
        }),
    }
}

pub(super) fn table_f64(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<f64, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Float scalar, found {other:?}"),
        }),
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Sample {
    pub(super) index: usize,
    pub(super) row: usize,
    pub(super) corr: usize,
    pub(super) chan: usize,
    pub(super) amp: f64,
    pub(super) real: f64,
    pub(super) imag: f64,
    pub(super) flag: bool,
    pub(super) field: i32,
    pub(super) spw: i32,
    pub(super) scan: i32,
    pub(super) ant1: i32,
    pub(super) ant2: i32,
}

pub(super) struct LoadedSamples {
    pub(super) samples: Vec<Sample>,
    pub(super) flags_by_row: BTreeMap<usize, ArrayD<bool>>,
    pub(super) model_roundoff: Vec<f32>,
    pub(super) axes: AveragedSampleAxes,
}

#[derive(Default)]
pub(super) struct AveragedSampleAxes {
    pub(super) channels_by_row: BTreeMap<usize, AveragedChannels>,
    pub(super) rows_by_bin: BTreeMap<usize, Vec<usize>>,
    pub(super) flags_by_bin: Option<BTreeMap<usize, ArrayD<bool>>>,
    pub(super) rows_outside_time: std::collections::BTreeSet<usize>,
}

impl AveragedSampleAxes {
    pub(super) fn time_selected_rows(&self, filter: Option<&TimeSelection>) -> Option<usize> {
        filter.map(|_| {
            self.rows_by_bin
                .iter()
                .filter(|(row, _)| !self.rows_outside_time.contains(row))
                .map(|(_, members)| members.len())
                .sum()
        })
    }
}

pub(super) struct AveragedChannels {
    pub(super) channels: Arc<[usize]>,
    pub(super) width: usize,
}
