// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selection `savemodel='virtual'` writes its model for, from the run's
//! own resolved selection: the fields and windows CASA clears
//! (`VisModelData::clearModel(ms, field, spw)`), the channel ranges its
//! visibility iterator reports (`getChannelInformation`), and the
//! combinations and first buffer of the rows it selects. Every expression
//! has been resolved once by the grammars the run images with
//! ([`super::selection`], [`super::time_selection`]); nothing is parsed
//! again.

use std::collections::BTreeSet;

use casa_imaging_model::RowSelection;
use casa_ms::observation_io::Msv2Observation;
use casa_ms::{VirtualModelCombinations, VirtualModelRow};
use casa_observation::StateId;

use super::PrepareError;
use super::opening::OpenedStore;
use super::selection::{WindowChannels, WindowChoice};
use super::time_selection::OtherSelections;
use crate::MeasurementSetSelection;
use crate::virtual_model::CompiledSelection;

/// The virtual model's selection of `selection`, one MeasurementSet of the
/// request, on `observation`, with its windows, channels, rows and states
/// as the run resolved them.
///
/// # Errors
///
/// A MAIN or DATA_DESCRIPTION column that cannot be read.
pub(super) fn resolve(
    selection: &MeasurementSetSelection,
    (store, observation): (&OpenedStore, &Msv2Observation),
    (windows, channels): (&WindowChoice, &WindowChannels),
    (rows, states): (&RowSelection, &BTreeSet<StateId>),
) -> Result<CompiledSelection, PrepareError> {
    let ms = observation.measurement_set();
    let catalogue = store.catalogue();
    let clear_fields = if selection.field.is_some() {
        (0..catalogue.fields.len())
            .filter_map(|field| i32::try_from(field).ok())
            .filter(|field| rows.fields().holds(i64::from(*field)))
            .collect()
    } else {
        Vec::new()
    };
    let window_count = ms.spectral_window()?.row_count();
    let named_windows: Vec<i32> = match windows {
        WindowChoice::All => (0..window_count)
            .filter_map(|window| i32::try_from(window).ok())
            .collect(),
        WindowChoice::Windows(set) => set.iter().filter_map(|w| i32::try_from(*w).ok()).collect(),
        WindowChoice::DataDescriptions(ddids) => {
            let table = ms.data_description()?;
            let mut set = BTreeSet::new();
            for &ddid in ddids {
                set.insert(table.spectral_window_id(ddid as usize)?);
            }
            set.into_iter().collect()
        }
    };
    let spw_given = selection
        .spw
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty());
    let clear_spws = if spw_given {
        named_windows.clone()
    } else {
        Vec::new()
    };
    let windows_table = ms.spectral_window()?;
    let mut channel_selection = Vec::new();
    for &window in &named_windows {
        let index = usize::try_from(window).map_err(|_| {
            casa_ms::MsError::InvalidInput(format!("spectral window {window} is negative"))
        })?;
        match channels.get(&index) {
            Some(selected) => channel_selection.extend(channel_runs(window, selected)),
            None => channel_selection.push([window, 0, windows_table.num_chan(index)?, 1]),
        }
    }
    let others = OtherSelections::new(observation, store, windows, (rows, states))?;
    let mut combinations = VirtualModelCombinations::new(ms)?;
    let mut failure = None;
    observation.for_each_main_row(|keys| {
        if others.hold(keys)? && rows.times().holds(keys.time_s) {
            let row = VirtualModelRow {
                field: keys.field,
                data_description: keys.data_description,
                scan: keys.scan,
                state: keys.state,
                array: keys.array,
                time_s: keys.time_s,
            };
            if let Err(error) = combinations.add(row) {
                failure.get_or_insert(error);
            }
        }
        Ok(())
    })?;
    if let Some(error) = failure {
        return Err(error.into());
    }
    Ok(CompiledSelection {
        clear_fields,
        clear_spws,
        channel_selection,
        channels: channels.clone(),
        rows: combinations.finish(),
    })
}

/// Window `window`'s selected channels as the iterator reports them:
/// `(spw, first, channels, increment)` runs of evenly spaced channels, in
/// ascending order.
fn channel_runs(window: i32, channels: &[usize]) -> Vec<[i32; 4]> {
    let mut runs: Vec<[i32; 4]> = Vec::new();
    let mut index = 0;
    while index < channels.len() {
        let start = channels[index];
        let stride = channels.get(index + 1).map_or(1, |next| next - start);
        let mut count = 1;
        while index + count < channels.len()
            && channels[index + count] - channels[index + count - 1] == stride
        {
            count += 1;
        }
        // A lone channel after a run starts its own run of one.
        let stride = if count == 1 { 1 } else { stride };
        runs.push([
            window,
            i32::try_from(start).unwrap_or(i32::MAX),
            i32::try_from(count).unwrap_or(i32::MAX),
            i32::try_from(stride).unwrap_or(i32::MAX),
        ]);
        index += count;
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::channel_runs;

    /// CASA's channel ranges come back from the channels they select:
    /// `0~4`, `0~10^2`, `0~3;8~9` and a lone channel.
    #[test]
    fn selected_channels_return_to_casas_ranges() {
        assert_eq!(channel_runs(3, &[0, 1, 2, 3, 4]), [[3, 0, 5, 1]]);
        assert_eq!(channel_runs(0, &[0, 2, 4, 6, 8, 10]), [[0, 0, 6, 2]]);
        assert_eq!(
            channel_runs(1, &[0, 1, 2, 3, 8, 9]),
            [[1, 0, 4, 1], [1, 8, 2, 1]]
        );
        assert_eq!(channel_runs(2, &[7]), [[2, 7, 1, 1]]);
    }
}
