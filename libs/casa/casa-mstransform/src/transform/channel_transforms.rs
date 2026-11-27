// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Clone)]
pub(super) struct TransformChannelSelection {
    pub(super) channels: Vec<usize>,
    pub(super) metadata_ends: Vec<usize>,
    pub(super) ranges: Vec<ChannelRange>,
}

impl TransformChannelSelection {
    pub(super) fn metadata_for_width(&self, width: usize) -> Vec<spectral_metadata::ChannelWindow> {
        let width = width.min(self.channels.len());
        let mut windows = Vec::new();
        for range in &self.ranges {
            let start = range.start as usize;
            let stop = range.stop as usize;
            let span = (range.step.max(1) as usize).saturating_mul(width);
            for first in (start..=stop).step_by(span) {
                let nominal_last = first.saturating_add(span - 1);
                windows.push(spectral_metadata::ChannelWindow {
                    first,
                    last: nominal_last.min(stop),
                    dropped: nominal_last > stop,
                });
            }
        }
        windows
    }
}

pub(super) fn resolve_transform_channels(
    ms: &MeasurementSet,
    selection: &SpwSelection,
    spw: &str,
) -> Result<BTreeMap<i32, TransformChannelSelection>, MsTransformError> {
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: source.to_string(),
            })?;
    let mut by_spw = BTreeMap::new();
    for row in 0..spectral_window.row_count() {
        if selection.rows_restricted() && !selection.spw_ids().contains(&row) {
            continue;
        }
        let num_chan =
            spectral_window
                .num_chan(row)
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                    reason: source.to_string(),
                })?;
        if num_chan < 0 {
            return Err(MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: format!("spectral-window {row} has negative NUM_CHAN {num_chan}",),
            });
        }
        let num_chan = num_chan as usize;
        let mut ranges = selection
            .channel_list()
            .iter()
            .filter(|range| range.spw_id == row)
            .copied()
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            ranges.push(ChannelRange {
                spw_id: row,
                start: 0,
                stop: num_chan as i32 - 1,
                step: 1,
            });
        }
        let mut channels = Vec::new();
        let mut metadata_ends = Vec::new();
        for range in &ranges {
            let indices = range
                .channels()
                .ok_or_else(|| MsTransformError::InvalidSpw {
                    selector: spw.to_string(),
                    reason: format!("spectral-window {row} range starts before its first channel"),
                })?;
            for channel in indices {
                channels.push(channel);
                metadata_ends
                    .push((channel + range.step.max(1) as usize - 1).min(range.stop as usize));
            }
        }
        if channels.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!("spectral-window {row} selection produced no channels",),
            });
        }
        by_spw.insert(
            row as i32,
            TransformChannelSelection {
                channels,
                metadata_ends,
                ranges,
            },
        );
    }
    Ok(by_spw)
}

pub(super) fn normalized_width(width: usize) -> usize {
    width.max(1)
}

pub(super) fn selected_spw_order<T>(
    windows: &BTreeMap<i32, T>,
    selection: &SpwSelection,
) -> Vec<i32> {
    let mut seen = BTreeSet::new();
    let mut order = selection
        .channel_list()
        .iter()
        .map(|range| range.spw_id as i32)
        .filter(|spw| windows.contains_key(spw) && seen.insert(*spw))
        .collect::<Vec<_>>();
    order.extend(windows.keys().copied().filter(|spw| seen.insert(*spw)));
    order
}

pub(super) fn channel_widths_by_spw(
    channels: &BTreeMap<i32, TransformChannelSelection>,
    spw_selection: &SpwSelection,
    scalar: usize,
    vector: Option<&[usize]>,
) -> Result<BTreeMap<i32, usize>, MsTransformError> {
    let simple = |width| {
        channels
            .keys()
            .map(|&spw| (spw, normalized_width(width)))
            .collect()
    };
    match vector {
        None => Ok(simple(scalar)),
        Some(&[width]) => Ok(simple(width)),
        Some(vector) => {
            let order = selected_spw_order(channels, spw_selection);
            if order.len() != vector.len() {
                return Err(MsTransformError::ChannelWidthCount);
            }
            Ok(order
                .into_iter()
                .zip(vector.iter().copied().map(normalized_width))
                .collect())
        }
    }
}

pub(super) fn channel_bins_by_spw(
    channel_selection: &BTreeMap<i32, TransformChannelSelection>,
    widths: &BTreeMap<i32, usize>,
    average_metadata: Option<&BTreeMap<i32, Vec<spectral_metadata::ChannelWindow>>>,
) -> Result<BTreeMap<i32, Vec<Vec<usize>>>, MsTransformError> {
    let mut by_spw = BTreeMap::new();
    for (&spw_id, selection) in channel_selection {
        let width = widths[&spw_id].min(selection.channels.len()).max(1);
        let bins = if let Some(windows) = average_metadata {
            let count = windows[&spw_id]
                .iter()
                .filter(|window| !window.dropped)
                .count();
            if count == 0 {
                return Err(MsTransformError::NoCompleteChannelBins);
            }
            // CASA averages the selected visibility stripe in sequence. It
            // drops the trailing samples according to the output SPW shape.
            selection
                .channels
                .chunks_exact(width)
                .take(count)
                .map(<[usize]>::to_vec)
                .collect::<Vec<_>>()
        } else {
            selection
                .channels
                .chunks(width)
                .map(<[usize]>::to_vec)
                .collect::<Vec<_>>()
        };
        if bins.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: format!("{spw_id}"),
                reason: format!("spectral-window {spw_id} selection produced no channel bins"),
            });
        }
        by_spw.insert(spw_id, bins);
    }
    Ok(by_spw)
}

pub(super) fn data_description_spw_map(
    ms: &MeasurementSet,
) -> Result<BTreeMap<i32, i32>, MsTransformError> {
    let data_description =
        ms.data_description()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            })?;
    let mut map = BTreeMap::new();
    for row in 0..data_description.row_count() {
        let spw = data_description.spectral_window_id(row).map_err(|source| {
            MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            }
        })?;
        map.insert(row as i32, spw);
    }
    Ok(map)
}

pub(super) fn select_channels(
    value: ArrayValue,
    channels: &[usize],
) -> Result<ArrayValue, TableError> {
    match value {
        ArrayValue::Complex32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "visibility DATA arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Complex32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Complex32(values));
                }
                return Ok(ArrayValue::Complex32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Complex32(values.select(Axis(1), channels)))
        }
        ArrayValue::Bool(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "FLAG arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Bool(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Bool(values));
                }
                return Ok(ArrayValue::Bool(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Bool(values.select(Axis(1), channels)))
        }
        ArrayValue::Float32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "WEIGHT_SPECTRUM arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Float32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Float32(values));
                }
                return Ok(ArrayValue::Float32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Float32(values.select(Axis(1), channels)))
        }
        other => Err(TableError::Schema(format!(
            "unsupported rank-2 channel selection for {:?}",
            other.primitive_type()
        ))),
    }
}

pub(super) fn transform_data_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    match value {
        ArrayValue::Complex32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Complex32)
        }
        ArrayValue::Float32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Float32)
        }
        _ => Err(TableError::Schema(
            "visibility arrays must be Complex32 or Float32".to_string(),
        )),
    }
}

pub(super) fn average_visibility<T>(
    values: ArrayD<T>,
    flags: &ArrayD<bool>,
    bins: &[Vec<usize>],
) -> Result<ArrayD<T>, TableError>
where
    T: Copy + Default + std::ops::AddAssign + std::ops::Div<f32, Output = T>,
{
    if values.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "DATA and FLAG arrays must have matching rank-2 [corr, chan] shapes".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::from_elem((corr_count, bins.len()), T::default());
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = T::default();
            let mut count = 0usize;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                    count += 1;
                }
            }
            if count == 0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
                count = bin.len();
            }
            output[[corr, out_chan]] = sum / count as f32;
        }
    }
    Ok(output.into_dyn())
}

pub(super) fn transform_flag_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 {
        return Err(TableError::Schema(
            "FLAG arrays must be rank-2 [corr, chan]".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<bool>::from_elem((corr_count, bins.len()), false);
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            output[[corr, out_chan]] = bin.iter().copied().all(|chan| values[[corr, chan]]);
        }
    }
    Ok(ArrayValue::Bool(output.into_dyn()))
}

pub(super) fn transform_flag_category_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be Bool rank-3 [corr, chan, category]".to_string(),
        ));
    };
    if values.ndim() != 3 {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be rank-3 [corr, chan, category]".to_string(),
        ));
    }
    let correlation_count = values.shape()[0];
    let channel_count = values.shape()[1];
    let category_count = values.shape()[2];
    if channel_count == 1 {
        return Ok(ArrayValue::Bool(values));
    }
    validate_channel_bins(bins, channel_count)?;
    let mut output = ArrayD::from_elem(
        IxDyn(&[correlation_count, bins.len(), category_count]).f(),
        false,
    );
    for correlation in 0..correlation_count {
        for (output_channel, bin) in bins.iter().enumerate() {
            for category in 0..category_count {
                output[[correlation, output_channel, category]] = bin
                    .iter()
                    .copied()
                    .all(|channel| values[[correlation, channel, category]]);
            }
        }
    }
    Ok(ArrayValue::Bool(output))
}

pub(super) fn transform_weight_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM and FLAG arrays must have matching rank-2 [corr, chan] shapes"
                .to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<f32>::zeros((corr_count, bins.len()));
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = 0.0f32;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                }
            }
            if sum == 0.0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
            }
            output[[corr, out_chan]] = sum;
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

pub(super) fn transform_sigma_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM and FLAG arrays must have matching rank-2 shapes".to_string(),
        ));
    }
    validate_channel_bins(bins, values.shape()[1])?;
    let mut output = ndarray::Array2::<f32>::zeros((values.shape()[0], bins.len()));
    for correlation in 0..values.shape()[0] {
        for (output_channel, bin) in bins.iter().enumerate() {
            let mut inverse_variance = 0.0_f64;
            for &channel in bin {
                let sigma = values[[correlation, channel]];
                if !flags[[correlation, channel]] && sigma.is_finite() && sigma > 0.0 {
                    inverse_variance += 1.0 / f64::from(sigma).powi(2);
                }
            }
            output[[correlation, output_channel]] = if inverse_variance > 0.0 {
                (1.0 / inverse_variance.sqrt()) as f32
            } else {
                0.0
            };
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

pub(super) fn bins_are_single_channel(bins: &[Vec<usize>]) -> bool {
    bins.iter().all(|bin| bin.len() == 1)
}

pub(super) fn validate_channel_bins(
    bins: &[Vec<usize>],
    channel_count: usize,
) -> Result<(), TableError> {
    if bins.is_empty() {
        return Err(TableError::Schema(
            "channel averaging produced no output channels".to_string(),
        ));
    }
    for bin in bins {
        if bin.is_empty() {
            return Err(TableError::Schema(
                "channel averaging bin was empty".to_string(),
            ));
        }
        for &channel in bin {
            if channel >= channel_count {
                return Err(TableError::Schema(format!(
                    "channel {channel} is outside array with {channel_count} channels"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn select_fortran_contiguous_axis1<T: Clone>(
    values: ndarray::ArrayViewD<'_, T>,
    start: usize,
    end: usize,
) -> Result<Option<ArrayD<T>>, TableError> {
    let shape = values.shape();
    if shape.len() != 2 || start > end || end > shape[1] {
        return Err(TableError::Schema(format!(
            "channel range {start}..{end} is outside array shape {shape:?}"
        )));
    }
    let corr_count = shape[0];
    if (corr_count > 1 && values.strides()[0] != 1)
        || (shape[1] > 1 && values.strides()[1] != corr_count as isize)
    {
        return Ok(None);
    }
    let Some(input) = values.as_slice_memory_order() else {
        return Ok(None);
    };
    let output_shape = [corr_count, end - start];
    let start_offset = start * corr_count;
    let end_offset = end * corr_count;
    ArrayD::from_shape_vec(
        IxDyn(&output_shape).f(),
        input[start_offset..end_offset].to_vec(),
    )
    .map(Some)
    .map_err(|error| TableError::Schema(format!("channel slice shape mismatch: {error}")))
}

pub(super) fn contiguous_channel_range(channels: &[usize]) -> Option<(usize, usize)> {
    let (&start, rest) = channels.split_first()?;
    for (offset, &channel) in rest.iter().enumerate() {
        if channel != start + offset + 1 {
            return None;
        }
    }
    Some((start, start + channels.len()))
}

pub(super) fn all_channels_selected(channels: &[usize], channel_count: usize) -> bool {
    channels.len() == channel_count && channels.iter().copied().enumerate().all(|(i, ch)| ch == i)
}

pub(super) fn update_spectral_window_metadata(
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    output_ms: &Path,
) -> Result<(), MsTransformError> {
    let spw_path = output_ms.join(SubtableId::SpectralWindow.name());
    let mut spectral_window = Table::open(TableOptions::new(&spw_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for (&spw_id, bins) in channel_bins {
        let row = spw_id as usize;
        update_f64_frequency_column(&mut spectral_window, row, "CHAN_FREQ", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "CHAN_WIDTH", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "EFFECTIVE_BW", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "RESOLUTION", bins, &spw_path)?;
        spectral_window
            .column_accessor_mut("NUM_CHAN")
            .and_then(|mut column| {
                column.set(row, Value::Scalar(ScalarValue::Int32(bins.len() as i32)))
            })
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            })?;
        let chan_freq = f64_vector_cell(&spectral_window, row, "CHAN_FREQ", &spw_path)?;
        if let (Some(first), Some(last)) = (chan_freq.first(), chan_freq.last()) {
            spectral_window
                .column_accessor_mut("REF_FREQUENCY")
                .and_then(|mut column| column.set(row, Value::Scalar(ScalarValue::Float64(*first))))
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
            let total_bw = f64_vector_cell(&spectral_window, row, "CHAN_WIDTH", &spw_path)
                .map(|widths| widths.iter().map(|width| width.abs()).sum())
                .unwrap_or_else(|_| {
                    if chan_freq.len() > 1 {
                        (last - first).abs()
                    } else {
                        0.0
                    }
                });
            spectral_window
                .column_accessor_mut("TOTAL_BANDWIDTH")
                .and_then(|mut column| {
                    column.set(row, Value::Scalar(ScalarValue::Float64(total_bw)))
                })
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
        }
    }
    spectral_window
        .prepare_write()
        .save(TableOptions::new(&spw_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

pub(super) fn update_f64_frequency_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
                / bin.len() as f64
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

pub(super) fn update_f64_width_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

pub(super) fn validate_bins_for_vector(
    bins: &[Vec<usize>],
    len: usize,
    column: &str,
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    for bin in bins {
        for &channel in bin {
            if channel >= len {
                return Err(MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: format!("{column} channel {channel} is outside vector length {len}"),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn f64_vector_cell(
    table: &Table,
    row: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, MsTransformError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = value else {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    Ok(values.iter().copied().collect())
}

pub(super) fn scalar_i32(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<i32, MsTransformError> {
    match value {
        Some(ScalarValue::Int32(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

pub(super) fn scalar_bool(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ScalarValue::Bool(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

pub(super) fn bool_array_all_true(
    value: Option<&ArrayValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ArrayValue::Bool(values)) => Ok(values.iter().all(|value| *value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

pub(super) fn scalar_f64(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<f64, MsTransformError> {
    match value {
        Some(ScalarValue::Float64(value)) => Ok(*value),
        Some(ScalarValue::Float32(value)) => Ok(f64::from(*value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

pub(super) fn missing_column_error(
    path: &Path,
    row_index: usize,
    column: &str,
) -> MsTransformError {
    MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    }
}

pub(super) fn prepare_output_root(path: &Path) -> Result<(), MsTransformError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| MsTransformError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}
