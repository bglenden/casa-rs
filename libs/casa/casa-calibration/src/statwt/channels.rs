// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel bins and the independent channel selection for variance estimates.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::{MeasurementSet, MsError, MsResult, ResolvedMsSelectionRow};

use super::{StatWtChannelBin, StatWtOptions};

pub(super) struct Channels {
    pub(super) bins: Vec<Range<usize>>,
    pub(super) fit: Vec<bool>,
}

pub(super) fn prepare(
    ms: &MeasurementSet,
    selection: &[ResolvedMsSelectionRow],
    options: &StatWtOptions,
) -> MsResult<BTreeMap<i32, Channels>> {
    let fitted = ms.resolve_spw_selector(&options.fit_spw)?;
    let windows = ms.spectral_window()?;
    let mut channels = BTreeMap::new();
    for row in selection {
        let spw = row.spectral_window_id;
        let std::collections::btree_map::Entry::Vacant(entry) = channels.entry(spw) else {
            continue;
        };
        let index = usize::try_from(spw)
            .map_err(|_| MsError::InvalidInput(format!("invalid spectral window {spw}")))?;
        let count = windows.num_chan(index)? as usize;
        let bins = match options.channel_bin {
            StatWtChannelBin::Window => std::iter::once(0..count).collect(),
            StatWtChannelBin::Channels(width) => (0..count)
                .step_by(width)
                .map(|start| start..start.saturating_add(width).min(count))
                .collect(),
            StatWtChannelBin::Hertz(width) => {
                let frequencies = windows.chan_freq(index)?;
                let mut bins = Vec::new();
                let mut start = 0;
                for (channel, &frequency) in frequencies.iter().enumerate() {
                    if (frequency - frequencies[start]).abs() > width {
                        bins.push(start..channel);
                        start = channel;
                    }
                }
                bins.push(start..frequencies.len());
                bins
            }
        };
        let fit = if let Some(fitted) = fitted.channels_of(index)? {
            let mut mask = vec![options.exclude_channels; count];
            for channel in fitted {
                if let Some(value) = mask.get_mut(channel) {
                    *value = !options.exclude_channels;
                }
            }
            mask
        } else {
            vec![true; count]
        };
        entry.insert(Channels { bins, fit });
    }
    Ok(channels)
}
