// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral coordinates carried by PlotMS spectral-window averaging groups.

use std::collections::HashMap;
use std::sync::Arc;

use casa_types::measures::frequency::MFrequency;

use super::ChannelBin;
use crate::columns::frequency_columns::ChanFreqColumn;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct SpectralChannel {
    pub(super) number: usize,
    pub(super) frequency_hz: f64,
}

/// The displayed channels and frequencies for an averaged visibility buffer.
/// PlotMS takes these coordinates from the first selected input window.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPlotMsAverageSpw {
    /// Spectral-window ID of the first input visibility buffer.
    pub spectral_window: i32,
    channels: Arc<[SpectralChannel]>,
}

impl MsScatterPlotMsAverageSpw {
    pub(super) fn channel(&self, ordinal: usize) -> &SpectralChannel {
        &self.channels[ordinal]
    }

    pub(super) fn frequency_hz(&self, display_channel: usize) -> Option<f64> {
        let index = self
            .channels
            .binary_search_by_key(&display_channel, |channel| channel.number)
            .ok()?;
        Some(self.channels[index].frequency_hz)
    }
}

#[derive(Default)]
pub(super) struct SpectralAverageGroups {
    averages: Vec<MsScatterPlotMsAverageSpw>,
    windows: HashMap<i32, SpectralWindowChannels>,
}

struct SpectralWindowChannels {
    input_channels: usize,
    display_channels: Arc<[SpectralChannel]>,
}

impl SpectralAverageGroups {
    pub(super) fn observe(
        &mut self,
        group: usize,
        spw: i32,
        input_channels: usize,
        bins: &[ChannelBin],
        frequencies: &ChanFreqColumn<'_>,
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
        if group == self.averages.len() {
            self.averages.push(MsScatterPlotMsAverageSpw {
                spectral_window: spw,
                channels: window.display_channels.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn into_averages(self) -> Arc<[MsScatterPlotMsAverageSpw]> {
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
