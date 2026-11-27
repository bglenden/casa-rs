// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub struct ChannelBin {
    pub start: usize,
    pub end: usize,
    pub(super) stride: usize,
    pub(super) ordinal: usize,
    pub(super) averaged: bool,
    pub(super) selection: Option<Arc<[usize]>>,
    pub(super) selection_width: usize,
}

impl ChannelBin {
    pub(super) fn channels(&self) -> impl Iterator<Item = usize> + '_ {
        let (range, stride) = if self.selection.is_some() {
            let start = self.ordinal * self.selection_width;
            (start..start + self.selection_width, 1)
        } else {
            (self.start..self.end, self.stride)
        };
        range.step_by(stride).map(|index| {
            self.selection
                .as_ref()
                .map_or(index, |channels| channels[index])
        })
    }

    pub fn display_channel(&self) -> usize {
        if self.averaged {
            self.ordinal
        } else {
            self.start
        }
    }
}

pub(super) fn channel_bins(
    chan_count: usize,
    avgchannel: Option<usize>,
) -> Result<Vec<ChannelBin>, String> {
    if chan_count == 0 {
        return Ok(Vec::new());
    }
    let width = avgchannel.unwrap_or(1);
    if width == 0 {
        return Err("avgchannel must be a positive integer".to_string());
    }
    let width = width.min(chan_count);
    Ok((0..chan_count / width)
        .map(|ordinal| ChannelBin {
            start: ordinal * width,
            end: (ordinal + 1) * width,
            stride: 1,
            ordinal,
            averaged: width > 1,
            selection: None,
            selection_width: 0,
        })
        .collect())
}

pub(super) fn plot_channel_bins_for_window<'a>(
    chan_count: usize,
    spec: &MsPlotSpec,
    spw_id: i32,
    calibration: Option<&PlotMsInput<'_>>,
    cache: &'a mut HashMap<(i32, usize), Vec<ChannelBin>>,
) -> Result<Cow<'a, [ChannelBin]>, String> {
    let Some(ranges) = calibration.and_then(|input| input.channels.get(&spw_id)) else {
        return plot_channel_bins(chan_count, spec).map(Cow::Owned);
    };
    let key = (spw_id, chan_count);
    let bins = match cache.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(plot_selected_channel_bins(chan_count, spec, ranges)?)
        }
    };
    Ok(Cow::Borrowed(bins))
}

pub fn plot_selected_channel_bins(
    chan_count: usize,
    spec: &MsPlotSpec,
    channels: &[ChannelRange],
) -> Result<Vec<ChannelBin>, String> {
    let mut ranges = channels
        .iter()
        .filter_map(|range| {
            let start = usize::try_from(range.start).ok()?;
            let end = (usize::try_from(range.stop).ok()? + 1).min(chan_count);
            (start < end).then_some((start, end, range.step.max(1) as usize))
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    ranges.dedup();
    let requested_width = spec.averaging.avgchannel.unwrap_or(1);
    if requested_width == 0 {
        return Err("avgchannel must be a positive integer".into());
    }
    if requested_width == 1 {
        let channels = ranges
            .into_iter()
            .flat_map(|(start, end, stride)| (start..end).step_by(stride))
            .collect::<std::collections::BTreeSet<_>>();
        return Ok(channels
            .into_iter()
            .enumerate()
            .map(|(ordinal, start)| ChannelBin {
                start,
                end: start + 1,
                stride: 1,
                ordinal,
                averaged: false,
                selection: None,
                selection_width: 0,
            })
            .collect());
    }

    let selection: Option<Arc<[usize]>> = (ranges.len() > 1).then(|| {
        ranges
            .iter()
            .flat_map(|&(start, end, stride)| (start..end).step_by(stride))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    });
    let selected_count = selection.as_ref().map_or_else(
        || {
            ranges
                .first()
                .map_or(0, |&(start, end, stride)| (end - start).div_ceil(stride))
        },
        |channels| channels.len(),
    );
    if selected_count == 0 {
        return Ok(Vec::new());
    }
    let width = requested_width.min(selected_count);
    let mut bins = Vec::new();
    for (start, end, stride) in ranges {
        let span = width.saturating_mul(stride);
        for offset in 0..(end - start) / span {
            if (bins.len() + 1).saturating_mul(width) > selected_count {
                break;
            }
            let start = start + offset * span;
            // CASA constructs frequency bins from the physical selection ranges
            // and averages the compressed visibility buffer in index order.
            bins.push(ChannelBin {
                start,
                end: start + span,
                stride,
                ordinal: bins.len(),
                averaged: true,
                selection: selection.clone(),
                selection_width: width,
            });
        }
    }
    Ok(bins)
}

pub(super) fn plot_channel_bins(
    chan_count: usize,
    spec: &MsPlotSpec,
) -> Result<Vec<ChannelBin>, String> {
    if spec.x_axis.uses_channel_bins() || spec.y_axes.iter().copied().any(MsAxis::uses_channel_bins)
    {
        channel_bins(chan_count, spec.averaging.avgchannel)
    } else if chan_count == 0 {
        Ok(Vec::new())
    } else {
        Ok(vec![ChannelBin {
            start: 0,
            end: chan_count,
            stride: 1,
            ordinal: 0,
            averaged: chan_count > 1,
            selection: None,
            selection_width: 0,
        }])
    }
}

#[derive(Debug, Clone)]
pub(super) struct SpectralContext {
    pub(super) channel_frequencies_hz: Vec<f64>,
    pub(super) rest_frequency_hz: f64,
    pub(super) doppler_ref: DopplerRef,
}

pub(super) struct TimePlotSpectrum {
    pub(super) frame: FrequencyRef,
    pub(super) source_channels: usize,
    pub(super) frequencies_hz: Vec<f64>,
}

impl TimePlotSpectrum {
    pub(super) fn read(
        column: &ChanFreqColumn<'_>,
        spw_id: i32,
        bins: &[ChannelBin],
    ) -> Result<Self, String> {
        let window = usize::try_from(spw_id)
            .map_err(|_| format!("invalid DATA_DESCRIPTION::SPECTRAL_WINDOW_ID {spw_id}"))?;
        let frame = column
            .frequency_reference(window)
            .map_err(|error| error.to_string())?;
        let frequencies = column
            .get_frequencies(window)
            .map_err(|error| error.to_string())?;
        let frequencies = frequencies.iter().map(MFrequency::hz).collect::<Vec<_>>();
        let frequencies_hz = bins
            .iter()
            .map(|bin| {
                mean_bin_frequency_hz(&frequencies, bin)
                    .ok_or("Missing selected channel frequency for PlotMS time averaging")
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            frame,
            source_channels: frequencies.len(),
            frequencies_hz,
        })
    }
}

impl SpectralContext {
    pub(super) fn mean_frequency_hz(&self, bin: &ChannelBin) -> Option<f64> {
        mean_bin_frequency_hz(&self.channel_frequencies_hz, bin)
    }
}

pub(super) fn mean_bin_frequency_hz(frequencies_hz: &[f64], bin: &ChannelBin) -> Option<f64> {
    // PlotMS takes frequencies from the full physical output span, including
    // the channels between strided samples. Visibility uses the cache members.
    let span = frequencies_hz.get(bin.start..bin.end)?;
    (!span.is_empty()).then(|| span.iter().sum::<f64>() / span.len() as f64)
}

pub(super) fn parse_frequency_frame(value: Option<&str>) -> Result<Option<FrequencyRef>, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<FrequencyRef>()
                .map_err(|error| error.to_string())
        })
        .transpose()
}

pub(super) fn parse_velocity_definition(value: &str) -> Result<DopplerRef, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "RADIO" => Ok(DopplerRef::RADIO),
        "OPTICAL" | "Z" => Ok(DopplerRef::Z),
        "TRUE" | "RELATIVISTIC" | "BETA" => Ok(DopplerRef::BETA),
        "RATIO" => Ok(DopplerRef::RATIO),
        "GAMMA" => Ok(DopplerRef::GAMMA),
        other => Err(format!(
            "unsupported msexplore velocity definition {other:?}"
        )),
    }
}

pub(super) fn resolve_spectral_context(
    spec: &MsPlotSpec,
    spw_id: i32,
    chan_count: usize,
    (field_id_value, row_time_value): (i32, f64),
    chan_freq: Option<&ChanFreqColumn<'_>>,
    derived_engine: Option<&MsCalEngine>,
    plotms_rest_bin: Option<&ChannelBin>,
) -> Result<Option<SpectralContext>, String> {
    let Some(chan_freq) = chan_freq else {
        return Ok(None);
    };

    let spw_index = usize::try_from(spw_id)
        .map_err(|_| format!("invalid DATA_DESCRIPTION::SPECTRAL_WINDOW_ID {spw_id}"))?;
    let source_channel_frequencies = chan_freq
        .get_frequencies(spw_index)
        .map_err(|error| error.to_string())?;
    if source_channel_frequencies.len() != chan_count {
        return Err(format!(
            "SPECTRAL_WINDOW row {spw_id} reported {} channel frequencies for {} data channels",
            source_channel_frequencies.len(),
            chan_count
        ));
    }

    let source_ref = match source_channel_frequencies.first() {
        Some(frequency) => frequency.refer(),
        None => chan_freq
            .frequency_reference(spw_index)
            .map_err(|error| error.to_string())?,
    };
    let target_ref =
        parse_frequency_frame(spec.transforms.freqframe.as_deref())?.unwrap_or(source_ref);
    let channel_frequencies_hz = if target_ref == source_ref {
        source_channel_frequencies
            .iter()
            .map(MFrequency::hz)
            .collect::<Vec<_>>()
    } else {
        let derived_engine = derived_engine.ok_or_else(|| {
            "internal error: missing derived engine for spectral-frame conversion".to_string()
        })?;
        let field_id = usize::try_from(field_id_value)
            .map_err(|_| format!("invalid FIELD_ID {field_id_value} for spectral transform"))?;
        source_channel_frequencies
            .iter()
            .map(|frequency| {
                convert_frequency_to_frame(
                    source_ref,
                    target_ref,
                    frequency.hz(),
                    row_time_value,
                    field_id,
                    derived_engine,
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let rest_frequency_hz = spec
        .transforms
        .restfreq
        .as_deref()
        .map(parse_rest_frequency_hz)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| {
            // The public PlotMS task defaults to the middle selected buffer channel.
            // The msexplore request defaults to the arithmetic centre of the window.
            plotms_rest_bin
                .and_then(|bin| mean_bin_frequency_hz(&channel_frequencies_hz, bin))
                .unwrap_or_else(|| {
                    channel_frequencies_hz.iter().sum::<f64>() / channel_frequencies_hz.len() as f64
                })
        });
    let doppler_ref = parse_velocity_definition(&spec.transforms.veldef)?;
    Ok(Some(SpectralContext {
        channel_frequencies_hz,
        rest_frequency_hz,
        doppler_ref,
    }))
}

pub(super) fn time_reporting_frame(
    time_wavelengths: bool,
    requested_freqframe: Option<FrequencyRef>,
    row_numbers: &[usize],
    time_bins: Option<&PlotTimeGroups>,
    selected_data_desc_ids: &[i32],
    data_description: &MsDataDescription<'_>,
    chan_freq: Option<&ChanFreqColumn<'_>>,
) -> Result<Option<FrequencyRef>, String> {
    let has_rows = !row_numbers.is_empty();
    Ok(
        if time_wavelengths && requested_freqframe.is_none() && has_rows {
            let first_slot = time_bins
                .expect("time wavelengths use the PlotMS iterator plan")
                .first_row_slot(0);
            let first_ddid = usize::try_from(selected_data_desc_ids[first_slot])
                .map_err(|_| "Invalid DATA_DESC_ID for the first PlotMS buffer")?;
            let first_window = data_description
                .spectral_window_id(first_ddid)
                .map_err(|error| error.to_string())?;
            let first_window = usize::try_from(first_window)
                .map_err(|_| "Invalid SPECTRAL_WINDOW_ID for the first PlotMS buffer")?;
            Some(
                chan_freq
                    .expect("wavelength coordinates use channel frequencies")
                    .frequency_reference(first_window)
                    .map_err(|error| error.to_string())?,
            )
        } else {
            requested_freqframe
        },
    )
}
