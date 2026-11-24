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
    channels: HashMap<i32, Arc<[SpectralChannel]>>,
}

impl SpectralAverageGroups {
    pub(super) fn observe(
        &mut self,
        group: usize,
        spw: i32,
        bins: &[ChannelBin],
        frequencies: &ChanFreqColumn<'_>,
    ) -> Result<(), String> {
        if group == self.averages.len() {
            let channels = match self.channels.entry(spw) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let frequencies = frequencies
                        .get_frequencies(spw as usize)
                        .map_err(|error| error.to_string())?;
                    let channels = bins
                        .iter()
                        .map(|bin| {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for channel in bin.channels() {
                                let frequency = frequencies.get(channel).ok_or_else(|| {
                                    format!("missing channel {channel} frequency for window {spw}")
                                })?;
                                sum += MFrequency::hz(frequency);
                                count += 1;
                            }
                            Ok(SpectralChannel {
                                number: bin.display_channel(),
                                frequency_hz: sum / f64::from(count),
                            })
                        })
                        .collect::<Result<Arc<[_]>, String>>()?;
                    entry.insert(channels)
                }
            };
            self.averages.push(MsScatterPlotMsAverageSpw {
                spectral_window: spw,
                channels: channels.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn into_averages(self) -> Arc<[MsScatterPlotMsAverageSpw]> {
        self.averages.into()
    }
}
