// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::mem::size_of;

use casa_types::ArrayValue;
use casa_types::quanta::format::{YMD, format_time};

use crate::selection::syntax::ChannelSelection;
use crate::{MeasurementSet, MsResult};

use super::clip_time::{ClipTimedRow, integration_blocks, read_clip_times, sort_iterator_rows};
use super::geometry::apply_geometric_flags;
use super::{
    ChangeSet, CorrelationSelection, FLAG_COLUMN, FlaggingError, data_description_spw_map,
    flag_read_plan,
};

/// Controls propagation from a reference antenna's baselines to its integrations.
#[derive(Clone, Debug)]
pub struct AntennaIntegrationOptions {
    /// Exact ANTENNA.NAME of the reference dish.
    pub reference_antenna: String,
    /// Required fraction of channels flagged on all reference baselines.
    pub min_channel_fraction: f64,
    /// Log the integration times and fractions selected by the agent.
    pub verbose: bool,
}

impl Default for AntennaIntegrationOptions {
    fn default() -> Self {
        Self {
            reference_antenna: String::new(),
            min_channel_fraction: 0.6,
            verbose: false,
        }
    }
}

pub(super) fn apply(
    ms: &mut MeasurementSet,
    selected: &[usize],
    options: &AntennaIntegrationOptions,
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    path: &str,
) -> Result<ChangeSet, FlaggingError> {
    let fail = |reason: String| FlaggingError::MutateFlags {
        path: path.to_owned(),
        reason,
    };
    let (reference, antenna_count) = reference_antenna(ms, &options.reference_antenna)
        .map_err(|error| fail(error.to_string()))?;
    let Some(reference) = reference else {
        let name = &options.reference_antenna;
        tracing::warn!(
            "Cannot create agent Antint: The antenna requested ({name}) could not be found in the input dataset"
        );
        tracing::error!("There is no agent to run in list");
        return Ok(ChangeSet::default());
    };
    if selected.is_empty() || antenna_count < 2 {
        return Ok(ChangeSet::default());
    }

    let ddid_to_spw = data_description_spw_map(ms)?;
    let windows = ms
        .spectral_window()
        .map_err(|error| fail(error.to_string()))?;
    let mut timed = read_clip_times(ms, selected, path)?;
    sort_iterator_rows(&mut timed);
    let mut affected = Vec::new();
    let plan = flag_read_plan(ms, timed.len(), size_of::<Option<ArrayValue>>(), false)?;
    let batch_rows = plan.rows_per_block.max(1);
    for block in integration_blocks(&timed, batch_rows) {
        let mut readback = BTreeMap::new();
        for batch in block.chunks(batch_rows) {
            let indices: Vec<_> = batch
                .iter()
                .filter(|row| reference_baseline(row, reference).is_some())
                .map(|row| row.row)
                .collect();
            let cells = ms
                .main_table()
                .column_accessor(FLAG_COLUMN)
                .and_then(|column| column.array_cells_owned_uncached(&indices))
                .map_err(|error| fail(error.to_string()))?;
            readback.extend(indices.into_iter().zip(cells));
        }
        for integration in block.chunk_by(ClipTimedRow::shares_integration) {
            let ddid = integration[0].description();
            let window = ddid_to_spw
                .get(&ddid)
                .and_then(|&id| usize::try_from(id).ok())
                .ok_or_else(|| fail(format!("DATA_DESC_ID {ddid} has no SPW mapping")))?;
            let channel_count = usize::try_from(
                windows
                    .num_chan(window)
                    .map_err(|error| fail(error.to_string()))?,
            )
            .map_err(|_| fail(format!("negative channel count in SPW {window}")))?;
            if channel_count == 0 {
                continue;
            }
            let common = flagged_channels(
                &readback,
                integration,
                reference,
                antenna_count,
                channel_count,
                fail,
            )?;
            let fraction = common as f64 / channel_count as f64;
            let reaches = if channel_count == 1 {
                common == 1
            } else {
                fraction
                    .partial_cmp(&options.min_channel_fraction)
                    .is_none_or(|order| order.is_gt())
            };
            if reaches {
                affected.extend(integration.iter().map(|row| row.row));
                if options.verbose {
                    let time = format_time(integration[0].time() / 86_400.0, YMD, 7);
                    if channel_count == 1 {
                        tracing::info!("Flagging integration at time:{time}");
                    } else {
                        tracing::info!(
                            "Flagging integration at time: {time} (fraction of flagged channels found: {fraction})"
                        );
                    }
                }
            }
        }
    }
    affected.sort_unstable();
    apply_geometric_flags(ms, &affected, channels, correlations)
}

fn reference_antenna(ms: &MeasurementSet, name: &str) -> MsResult<(Option<i32>, usize)> {
    let antennas = ms.antenna()?;
    let count = antennas.row_count();
    for row in 0..count {
        if antennas.name(row)? == name {
            return Ok((i32::try_from(row).ok(), count));
        }
    }
    Ok((None, count))
}

fn reference_baseline(row: &ClipTimedRow, reference: i32) -> Option<i32> {
    let [first, second] = row.antennas();
    match (first == reference, second == reference) {
        (true, false) => Some(second),
        (false, true) => Some(first),
        _ => None,
    }
}

fn flagged_channels(
    flags_by_row: &BTreeMap<usize, Option<ArrayValue>>,
    rows: &[ClipTimedRow],
    reference: i32,
    antenna_count: usize,
    channel_count: usize,
    fail: impl Fn(String) -> FlaggingError,
) -> Result<usize, FlaggingError> {
    let mut flags_by_antenna = BTreeMap::new();
    for row in rows {
        let Some(other) = reference_baseline(row, reference) else {
            continue;
        };
        let row = row.row;
        let Some(Some(ArrayValue::Bool(flags))) = flags_by_row.get(&row) else {
            return Err(fail(format!("FLAG row {row} must be a Boolean array")));
        };
        let [correlation_count, channels] = flags.shape() else {
            return Err(fail(format!(
                "FLAG row {row} must have correlation and channel axes"
            )));
        };
        if *channels != channel_count {
            return Err(fail(format!(
                "FLAG row {row} has an inconsistent channel count"
            )));
        }
        let baseline = flags_by_antenna
            .entry(other)
            .or_insert_with(|| vec![false; channel_count]);
        for (channel, flagged) in baseline.iter_mut().enumerate() {
            *flagged |= (0..*correlation_count).any(|corr| flags[[corr, channel]]);
        }
    }
    if flags_by_antenna.len() < antenna_count - 1 {
        return Ok(0);
    }
    Ok((0..channel_count)
        .filter(|&channel| flags_by_antenna.values().all(|baseline| baseline[channel]))
        .count())
}
