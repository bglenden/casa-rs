// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::mem::size_of;

use casa_types::ArrayValue;
use casa_types::quanta::format::{YMD, format_time};

use crate::selection::syntax::ChannelSelection;
use crate::{MeasurementSet, MsReadPlan, MsResult, MsSelectionIoBudget};

use super::clip_time::{ClipTimedRow, read_clip_times, sort_iterator_rows};
use super::geometry::apply_geometric_flags;
use super::{
    ChangeSet, CorrelationSelection, FLAG_COLUMN, FlaggingError, data_description_spw_map,
    maximum_flag_elements,
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
    let affected = affected_rows(ms, selected, options, path)?;
    apply_affected(ms, affected, channels, correlations)
}

pub(super) fn apply_affected(
    ms: &mut MeasurementSet,
    affected: Option<Vec<usize>>,
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
) -> Result<ChangeSet, FlaggingError> {
    if let Some(affected) = affected {
        apply_geometric_flags(ms, &affected, channels, correlations)
    } else {
        Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        })
    }
}

pub(super) fn affected_rows(
    ms: &MeasurementSet,
    selected: &[usize],
    options: &AntennaIntegrationOptions,
    path: &str,
) -> Result<Option<Vec<usize>>, FlaggingError> {
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
        return Ok(None);
    };
    if selected.is_empty() || antenna_count < 2 {
        return Ok(Some(Vec::new()));
    }

    let ddid_to_spw = data_description_spw_map(ms)?;
    let windows = ms
        .spectral_window()
        .map_err(|error| fail(error.to_string()))?;
    let mut timed = read_clip_times(ms, selected, path)?;
    sort_iterator_rows(&mut timed);
    let mut channels_by_description = BTreeMap::new();
    for row in &timed {
        let ddid = row.description();
        if let std::collections::btree_map::Entry::Vacant(slot) =
            channels_by_description.entry(ddid)
        {
            let window = ddid_to_spw
                .get(&ddid)
                .and_then(|&id| usize::try_from(id).ok())
                .ok_or_else(|| fail(format!("DATA_DESC_ID {ddid} has no SPW mapping")))?;
            let count = usize::try_from(
                windows
                    .num_chan(window)
                    .map_err(|error| fail(error.to_string()))?,
            )
            .map_err(|_| fail(format!("negative channel count in SPW {window}")))?;
            slot.insert(count);
        }
    }
    let bytes_per_row = maximum_flag_elements(ms)?
        .checked_add(size_of::<Option<ArrayValue>>() + size_of::<usize>())
        .ok_or_else(|| fail("flag row byte accounting overflowed".to_owned()))?;
    let budget = MsSelectionIoBudget::from_system_memory(2, bytes_per_row, None)
        .map_err(|error| fail(error.to_string()))?;
    let plan = MsReadPlan::new(timed.len(), budget).map_err(|error| fail(error.to_string()))?;
    let column = ms
        .main_table()
        .column_accessor(FLAG_COLUMN)
        .map_err(|error| fail(error.to_string()))?;
    let mut affected = IntegrationScan {
        rows: &timed,
        reference,
        antenna_count,
        channels_by_description: &channels_by_description,
        options,
    }
    .affected_rows(plan.rows_per_block.max(1), fail, |indices| {
        column
            .array_cells_owned_uncached(indices)
            .map_err(|error| fail(error.to_string()))
    })?;
    affected.sort_unstable();
    Ok(Some(affected))
}

struct IntegrationScan<'a> {
    rows: &'a [ClipTimedRow],
    reference: i32,
    antenna_count: usize,
    channels_by_description: &'a BTreeMap<i32, usize>,
    options: &'a AntennaIntegrationOptions,
}

impl IntegrationScan<'_> {
    fn affected_rows(
        &self,
        batch_rows: usize,
        fail: impl Fn(String) -> FlaggingError,
        mut read_flags: impl FnMut(&[usize]) -> Result<Vec<Option<ArrayValue>>, FlaggingError>,
    ) -> Result<Vec<usize>, FlaggingError> {
        let mut affected = Vec::new();
        let mut start = 0;
        let mut flags = BTreeMap::new();
        for (batch_index, batch) in self.rows.chunks(batch_rows).enumerate() {
            let indices: Vec<_> = batch
                .iter()
                .filter(|row| reference_baseline(row, self.reference).is_some())
                .map(|row| row.row)
                .collect();
            let mut cells = read_flags(&indices)?.into_iter();
            for (offset, row) in batch.iter().enumerate() {
                if !self.rows[start].shares_integration(row) {
                    let end = batch_index * batch_rows + offset;
                    self.finish_integration(&self.rows[start..end], &flags, &mut affected);
                    start = end;
                    flags.clear();
                }
                if let Some(other) = reference_baseline(row, self.reference) {
                    let count = self.channels_by_description[&row.description()];
                    add_baseline_flags(&mut flags, row.row, other, count, cells.next().flatten())
                        .map_err(&fail)?;
                }
            }
        }
        if start < self.rows.len() {
            self.finish_integration(&self.rows[start..], &flags, &mut affected);
        }
        Ok(affected)
    }

    fn finish_integration(
        &self,
        rows: &[ClipTimedRow],
        flags: &BTreeMap<i32, Vec<bool>>,
        affected: &mut Vec<usize>,
    ) {
        let count = self.channels_by_description[&rows[0].description()];
        if count == 0 {
            return;
        }
        let common = common_flagged_channels(flags, self.antenna_count, count);
        let fraction = common as f64 / count as f64;
        let reaches = if count == 1 {
            common == 1
        } else {
            fraction
                .partial_cmp(&self.options.min_channel_fraction)
                .is_none_or(|order| order.is_gt())
        };
        if reaches {
            affected.extend(rows.iter().map(|row| row.row));
            if self.options.verbose {
                let time = format_time(rows[0].time() / 86_400.0, YMD, 7);
                if count == 1 {
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

fn add_baseline_flags(
    flags_by_antenna: &mut BTreeMap<i32, Vec<bool>>,
    row: usize,
    other: i32,
    channel_count: usize,
    cell: Option<ArrayValue>,
) -> Result<(), String> {
    let Some(ArrayValue::Bool(flags)) = cell else {
        return Err(format!("FLAG row {row} must be a Boolean array"));
    };
    let [correlation_count, channels] = flags.shape() else {
        return Err(format!(
            "FLAG row {row} must have correlation and channel axes"
        ));
    };
    if *channels != channel_count {
        return Err(format!("FLAG row {row} has an inconsistent channel count"));
    }
    let baseline = flags_by_antenna
        .entry(other)
        .or_insert_with(|| vec![false; channel_count]);
    for (channel, flagged) in baseline.iter_mut().enumerate() {
        *flagged |= (0..*correlation_count).any(|corr| flags[[corr, channel]]);
    }
    Ok(())
}

fn common_flagged_channels(
    flags_by_antenna: &BTreeMap<i32, Vec<bool>>,
    antenna_count: usize,
    channel_count: usize,
) -> usize {
    if flags_by_antenna.len() < antenna_count - 1 {
        return 0;
    }
    (0..channel_count)
        .filter(|&channel| flags_by_antenna.values().all(|baseline| baseline[channel]))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_decisions_span_physical_flag_batches() {
        let rows: Vec<_> = [
            (10.0, [0, 1]),
            (10.0, [0, 2]),
            (10.0, [1, 2]),
            (20.0, [0, 1]),
            (20.0, [0, 2]),
            (20.0, [1, 2]),
        ]
        .into_iter()
        .enumerate()
        .map(|(row, (time, baseline))| ClipTimedRow::integration_fixture_row(row, time, baseline))
        .collect();
        let scan = IntegrationScan {
            rows: &rows,
            reference: 0,
            antenna_count: 3,
            channels_by_description: &BTreeMap::from([(0, 3)]),
            options: &AntennaIntegrationOptions::default(),
        };
        for batch_size in [1, 2, 16] {
            let affected = scan
                .affected_rows(
                    batch_size,
                    |reason| FlaggingError::MutateFlags {
                        path: "fixture".to_owned(),
                        reason,
                    },
                    |indices| {
                        assert!(indices.len() <= batch_size);
                        Ok(indices
                            .iter()
                            .map(|row| {
                                let values = match row {
                                    0 => [[true, true, false], [false, false, false]],
                                    1 | 3 => [[false, false, false], [true, true, false]],
                                    4 => [[true, false, false], [false, false, false]],
                                    _ => panic!("read a non-reference FLAG row"),
                                };
                                Some(ArrayValue::Bool(ndarray::arr2(&values).into_dyn()))
                            })
                            .collect())
                    },
                )
                .expect("reduce flag batches");
            assert_eq!(affected, [0, 1, 2]);
        }
    }
}
