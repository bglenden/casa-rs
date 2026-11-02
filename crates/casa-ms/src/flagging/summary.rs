// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's flagdata summary (#931): the flagged and total samples of a
//! selection, overall and per antenna, array, correlation, field,
//! observation, scan and spectral window.
//!
//! The counts follow CASA 6.7.6.14's summary agent, as its own runs show:
//! - a sample is one correlation of one channel of one row;
//! - a channel selection (`spw='0:0~3'`) counts only the channels it
//!   selects, in every breakdown and in the totals;
//! - a cross-correlation counts for both of its antennas, an
//!   autocorrelation once for its one;
//! - antennas, fields and correlations are keyed by name, the others by id.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ChannelSelection, FlagDataMode, FlagDataReport, FlaggingError, clone_flag_matrix,
    data_description_spw_map, display_ms_path, load_i32_cells,
};
use crate::listobs::stokes_name;
use crate::ms::MeasurementSet;

/// Rows read at once; the summary holds a block's ids, never all of them.
const BLOCK_ROWS: usize = 65_536;

/// The flagged and total samples of one summary entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FlagCounts {
    /// Flagged samples.
    pub flagged: u64,
    /// All samples.
    pub total: u64,
}

impl FlagCounts {
    fn add(&mut self, other: Self) {
        self.flagged += other.flagged;
        self.total += other.total;
    }
}

/// CASA's flagdata summary of a selection: the counts overall and broken
/// down by each axis CASA's summary agent reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FlagSummaryBreakdown {
    /// Per antenna name.
    pub antenna: BTreeMap<String, FlagCounts>,
    /// Per ARRAY_ID.
    pub array: BTreeMap<String, FlagCounts>,
    /// Per correlation, by its Stokes name (`XX`, `RR`, `I`...).
    pub correlation: BTreeMap<String, FlagCounts>,
    /// Per field name.
    pub field: BTreeMap<String, FlagCounts>,
    /// Per OBSERVATION_ID.
    pub observation: BTreeMap<String, FlagCounts>,
    /// Per SCAN_NUMBER.
    pub scan: BTreeMap<String, FlagCounts>,
    /// Per spectral window id.
    pub spw: BTreeMap<String, FlagCounts>,
    /// The whole selection.
    pub overall: FlagCounts,
}

/// The names and windows a row's ids resolve to.
struct Lookup {
    antenna: Vec<String>,
    field: Vec<String>,
    ddid_to_spw: BTreeMap<i32, i32>,
    ddid_correlations: BTreeMap<i32, Vec<String>>,
}

impl Lookup {
    fn read(ms: &MeasurementSet) -> Result<Self, FlaggingError> {
        let failed = |what: &str, source: crate::MsError| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("read {what}: {source}"),
        };
        let antennas = ms.antenna().map_err(|error| failed("ANTENNA", error))?;
        let antenna = (0..antennas.row_count())
            .map(|row| antennas.name(row))
            .collect::<Result<_, _>>()
            .map_err(|error| failed("ANTENNA names", error))?;
        let fields = ms.field().map_err(|error| failed("FIELD", error))?;
        let field = (0..fields.row_count())
            .map(|row| fields.name(row))
            .collect::<Result<_, _>>()
            .map_err(|error| failed("FIELD names", error))?;
        let polarization = ms
            .polarization()
            .map_err(|error| failed("POLARIZATION", error))?;
        let descriptions = ms
            .data_description()
            .map_err(|error| failed("DATA_DESCRIPTION", error))?;
        let mut ddid_correlations = BTreeMap::new();
        for row in 0..descriptions.row_count() {
            let polarization_id = descriptions
                .polarization_id(row)
                .map_err(|error| failed("DATA_DESCRIPTION", error))?;
            let names = polarization
                .corr_type(polarization_id as usize)
                .map_err(|error| failed("POLARIZATION CORR_TYPE", error))?
                .into_iter()
                .map(|code| stokes_name(code).to_string())
                .collect();
            ddid_correlations.insert(row as i32, names);
        }
        Ok(Self {
            antenna,
            field,
            ddid_to_spw: data_description_spw_map(ms)?,
            ddid_correlations,
        })
    }
}

/// The ids of a block of rows.
struct BlockIds {
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    array: Vec<i32>,
    field: Vec<i32>,
    observation: Vec<i32>,
    scan: Vec<i32>,
    ddid: Vec<i32>,
}

impl BlockIds {
    fn read(ms: &MeasurementSet, rows: &[usize]) -> Result<Self, FlaggingError> {
        let table = ms.main_table();
        let path = display_ms_path(ms);
        let column = |name: &str| load_i32_cells(table, rows, name, &path);
        Ok(Self {
            antenna1: column("ANTENNA1")?,
            antenna2: column("ANTENNA2")?,
            array: column("ARRAY_ID")?,
            field: column("FIELD_ID")?,
            observation: column("OBSERVATION_ID")?,
            scan: column("SCAN_NUMBER")?,
            ddid: column("DATA_DESC_ID")?,
        })
    }
}

/// The summary-mode report of `rows`: it changes nothing, and carries the
/// summary with its overall counts.
pub(super) fn report(
    ms: &MeasurementSet,
    rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
) -> Result<FlagDataReport, FlaggingError> {
    let summary = summarize(ms, rows, channel_selections)?;
    let report = FlagDataReport {
        mode: FlagDataMode::Summary,
        selected_rows: rows.len(),
        changed_rows: 0,
        changed_samples: 0,
        flagged_samples: summary.overall.flagged as usize,
        total_samples: summary.overall.total as usize,
        backup_version: None,
        timedev: None,
        freqdev: None,
        summary: Some(summary),
    };
    tracing::info!(
        path = %display_ms_path(ms),
        selected_rows = report.selected_rows,
        flagged_samples = report.flagged_samples,
        total_samples = report.total_samples,
        "flagdata summary completed"
    );
    Ok(report)
}

/// CASA's summary of the flags of `rows`, counting the channels
/// `channel_selections` selects in each window, or all of them.
fn summarize(
    ms: &MeasurementSet,
    rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
) -> Result<FlagSummaryBreakdown, FlaggingError> {
    let lookup = Lookup::read(ms)?;
    let mut summary = FlagSummaryBreakdown::default();
    for block in rows.chunks(BLOCK_ROWS) {
        let ids = BlockIds::read(ms, block)?;
        for (slot, &row) in block.iter().enumerate() {
            add_row(
                ms,
                &lookup,
                &ids,
                slot,
                row,
                channel_selections,
                &mut summary,
            )?;
        }
    }
    Ok(summary)
}

fn add_row(
    ms: &MeasurementSet,
    lookup: &Lookup,
    ids: &BlockIds,
    slot: usize,
    row: usize,
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    summary: &mut FlagSummaryBreakdown,
) -> Result<(), FlaggingError> {
    let flags = clone_flag_matrix(ms, row)?;
    let shape = flags.shape();
    let (correlations, channels) = (shape[0], shape.get(1).copied().unwrap_or(1));
    let ddid = ids.ddid[slot];
    let spw = lookup.ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
        invalid(
            ms,
            format!("row {row}: DATA_DESC_ID {ddid} has no spectral window"),
        )
    })?;
    let selected = match channel_selections.and_then(|selections| selections.get(&spw)) {
        Some(selection) => selection
            .indices(channels)
            .map_err(|source| invalid(ms, format!("resolve SPW {spw} channels: {source}")))?,
        None => (0..channels).collect(),
    };
    let names = lookup.ddid_correlations.get(&ddid);
    let mut row_counts = FlagCounts::default();
    for correlation in 0..correlations {
        let flagged = selected
            .iter()
            .filter(|&&channel| flags[[correlation, channel]])
            .count() as u64;
        let counts = FlagCounts {
            flagged,
            total: selected.len() as u64,
        };
        let name = names
            .and_then(|names| names.get(correlation))
            .map_or("??", String::as_str);
        summary
            .correlation
            .entry(name.to_string())
            .or_default()
            .add(counts);
        row_counts.add(counts);
    }
    let antenna = |id: i32| -> Result<String, FlaggingError> {
        lookup
            .antenna
            .get(usize::try_from(id).unwrap_or(usize::MAX))
            .cloned()
            .ok_or_else(|| invalid(ms, format!("row {row}: no antenna {id}")))
    };
    let (antenna1, antenna2) = (ids.antenna1[slot], ids.antenna2[slot]);
    let add = |axis: &mut BTreeMap<String, FlagCounts>, key: String| {
        axis.entry(key).or_default().add(row_counts);
    };
    add(&mut summary.antenna, antenna(antenna1)?);
    if antenna2 != antenna1 {
        add(&mut summary.antenna, antenna(antenna2)?);
    }
    let field = ids.field[slot];
    let field_name = lookup
        .field
        .get(usize::try_from(field).unwrap_or(usize::MAX))
        .cloned()
        .ok_or_else(|| invalid(ms, format!("row {row}: no field {field}")))?;
    add(&mut summary.field, field_name);
    add(&mut summary.array, ids.array[slot].to_string());
    add(&mut summary.observation, ids.observation[slot].to_string());
    add(&mut summary.scan, ids.scan[slot].to_string());
    add(&mut summary.spw, spw.to_string());
    summary.overall.add(row_counts);
    Ok(())
}

fn invalid(ms: &MeasurementSet, reason: String) -> FlaggingError {
    FlaggingError::MutateFlags {
        path: display_ms_path(ms),
        reason,
    }
}
