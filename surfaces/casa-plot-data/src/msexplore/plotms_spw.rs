// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral coordinates carried by PlotMS spectral-window averaging groups.

use std::collections::HashMap;
use std::sync::Arc;

use casa_core::measures::frequency::MFrequency;

use super::ChannelBin;
use super::plotms_ordinates::ScalarSpanPlan;
use super::plotms_time_baselines::TimeBaselinePlan;
use casa_ms::columns::frequency_columns::ChanFreqColumn;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct SpectralChannel {
    pub(super) number: usize,
    pub(super) frequency_hz: f64,
}

/// The displayed channels and frequencies for an averaged visibility buffer.
/// PlotMS takes the window ID from the first buffer and the grid from the
/// buffer still attached to the iterator when it caches the average.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPlotMsAverageSpw {
    /// Spectral-window ID of the first input visibility buffer.
    pub spectral_window: i32,
    /// Optional scan number and field ID restricting where this spectral metadata applies.
    pub scan_field: Option<(i32, i32)>,
    channels: Arc<[SpectralChannel]>,
}

impl MsScatterPlotMsAverageSpw {
    pub(super) fn channel(&self, ordinal: usize) -> &SpectralChannel {
        &self.channels[ordinal]
    }

    /// Looks up the frequency in hertz associated with a plotted channel index.
    pub fn frequency_hz(&self, display_channel: usize) -> Option<f64> {
        let index = self
            .channels
            .binary_search_by_key(&display_channel, |channel| channel.number)
            .ok()?;
        Some(self.channels[index].frequency_hz)
    }
}

#[derive(Default)]
pub(super) struct SpectralAverageGroups {
    averages: Vec<Option<MsScatterPlotMsAverageSpw>>,
    windows: HashMap<i32, SpectralWindowChannels>,
}

struct SpectralWindowChannels {
    input_channels: usize,
    display_channels: Arc<[SpectralChannel]>,
}

impl SpectralAverageGroups {
    pub(super) fn output_window(&self, group: usize) -> Option<i32> {
        self.averages
            .get(group)?
            .as_ref()
            .map(|average| average.spectral_window)
    }

    pub(super) fn observe(
        &mut self,
        group: usize,
        spw: i32,
        input_channels: usize,
        bins: &[ChannelBin],
        frequencies: &ChanFreqColumn<'_>,
        starts_buffer: bool,
    ) -> Result<(), String> {
        let window = match self.windows.entry(spw) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let frequencies = frequencies
                    .get_frequencies(spw as usize)
                    .map_err(|error| error.to_string())?;
                check_input_channels(spw, frequencies.len(), input_channels)?;
                let display_channels = bins
                    .iter()
                    .map(|bin| {
                        // The iterator averages visibility samples from the
                        // compressed selection but advertises physical bins.
                        let physical_bin = &frequencies[bin.start..bin.end];
                        SpectralChannel {
                            number: bin.display_channel(),
                            frequency_hz: physical_bin.iter().map(MFrequency::hz).sum::<f64>()
                                / physical_bin.len() as f64,
                        }
                    })
                    .collect();
                entry.insert(SpectralWindowChannels {
                    input_channels,
                    display_channels,
                })
            }
        };
        check_input_channels(spw, window.input_channels, input_channels)?;
        if group >= self.averages.len() {
            self.averages.resize_with(group + 1, || None);
        }
        let average = self.averages[group].get_or_insert_with(|| MsScatterPlotMsAverageSpw {
            spectral_window: spw,
            scan_field: None,
            channels: window.display_channels.clone(),
        });
        if starts_buffer {
            average.channels.clone_from(&window.display_channels);
        }
        Ok(())
    }

    pub(super) fn into_averages(
        mut self,
        spans: Option<&ScalarSpanPlan>,
        time_plan: Option<&TimeBaselinePlan>,
    ) -> Arc<[Option<MsScatterPlotMsAverageSpw>]> {
        if let Some(plan) = time_plan {
            return plan
                .spectral_window_buffers()
                .map(|(first, last, scan_field)| {
                    let mut average = self.averages.get(first)?.as_ref()?.clone();
                    let last = self.averages.get(last)?.as_ref()?;
                    average.channels.clone_from(&last.channels);
                    average.scan_field = Some(scan_field);
                    Some(average)
                })
                .collect();
        }
        if let Some(spans) = spans {
            for (group, average) in self.averages.iter_mut().enumerate() {
                if let Some(average) = average {
                    let span = spans.metadata(group);
                    average.scan_field = Some((span.scan, span.field));
                }
            }
        }
        self.averages.into()
    }
}

fn check_input_channels(spw: i32, frequencies: usize, input_channels: usize) -> Result<(), String> {
    if frequencies != input_channels {
        return Err(format!(
            "SPECTRAL_WINDOW row {spw} reported {frequencies} channel frequencies for {input_channels} data channels"
        ));
    }
    Ok(())
}
