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

use casa_core::ArrayValue;
use ndarray::ArrayD;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ChannelSelection, CorrelationSelection, FLAG_COLUMN, FlagDataMode, FlagDataReport,
    FlaggingError, data_description_spw_map, display_ms_path, load_i32_cells,
};
use crate::listobs::stokes_name;
use crate::ms::MeasurementSet;

/// Rows read at once; the summary holds a block's ids, never all of them.
const BLOCK_ROWS: usize = 65_536;
// Read smaller FLAG batches because each row may contain many channels.
const FLAG_BATCH_ROWS: usize = 512;

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

/// Breakdowns and filters requested by CASA's flagdata summary task.
#[derive(Debug, Clone, Copy)]
pub struct FlagSummaryOptions {
    /// Report counts for each selected channel in its spectral window.
    pub spwchan: bool,
    /// Report counts for each correlation in its spectral window.
    pub spwcorr: bool,
    /// Report baseline and antenna/scan counts.
    pub basecnt: bool,
    /// Report a separate breakdown for each field.
    pub fieldcnt: bool,
    /// Minimum flagged fraction of a reported entry.
    pub minrel: f64,
    /// Maximum flagged fraction of a reported entry.
    pub maxrel: f64,
    /// Minimum flagged samples of a reported entry.
    pub minabs: i64,
    /// Maximum flagged samples of a reported entry. Negative values are unbounded.
    pub maxabs: i64,
}

impl Default for FlagSummaryOptions {
    fn default() -> Self {
        Self {
            spwchan: false,
            spwcorr: false,
            basecnt: false,
            fieldcnt: false,
            minrel: 0.0,
            maxrel: 1.0,
            minabs: 0,
            maxabs: -1,
        }
    }
}

impl FlagSummaryOptions {
    fn includes(self, counts: FlagCounts) -> bool {
        let fraction = counts.flagged as f64 / counts.total as f64;
        !((self.minabs > 0 && counts.flagged < self.minabs as u64)
            || (self.maxabs >= 0 && counts.flagged > self.maxabs as u64)
            || fraction < self.minrel
            || fraction > self.maxrel)
    }

    fn has_bounds(self) -> bool {
        self.minrel != 0.0 || self.maxrel != 1.0 || self.minabs > 0 || self.maxabs >= 0
    }
}

/// CASA's flagdata summary of a selection: the counts overall and broken
/// down by each axis CASA's summary agent reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FlagSummaryBreakdown {
    /// Per antenna name.
    pub antenna: BTreeMap<String, FlagCounts>,
    /// Per antenna and scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub antenna_scan: Option<BTreeMap<String, FlagCounts>>,
    /// Per ARRAY_ID.
    pub array: BTreeMap<String, FlagCounts>,
    /// Per baseline in the stored antenna order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BTreeMap<String, FlagCounts>>,
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
    /// Per spectral window and selected channel index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spw_channel: Option<BTreeMap<String, FlagCounts>>,
    /// Per spectral window and correlation name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spw_correlation: Option<BTreeMap<String, FlagCounts>>,
    /// The whole selection.
    pub overall: FlagCounts,
    /// Separate summaries keyed by field name when fieldcnt is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by_field: Option<BTreeMap<String, FlagSummaryBreakdown>>,
}

impl FlagSummaryBreakdown {
    fn with_options(options: FlagSummaryOptions) -> Self {
        Self {
            antenna_scan: options.basecnt.then(BTreeMap::new),
            baseline: options.basecnt.then(BTreeMap::new),
            spw_channel: options.spwchan.then(BTreeMap::new),
            spw_correlation: options.spwcorr.then(BTreeMap::new),
            ..Self::default()
        }
    }

    fn filter_entries(&mut self, options: FlagSummaryOptions) {
        if !options.has_bounds() {
            return;
        }
        let axes = [
            &mut self.antenna,
            &mut self.array,
            &mut self.correlation,
            &mut self.field,
            &mut self.observation,
            &mut self.scan,
            &mut self.spw,
        ];
        for axis in axes
            .into_iter()
            .chain(&mut self.antenna_scan)
            .chain(&mut self.baseline)
            .chain(&mut self.spw_channel)
            .chain(&mut self.spw_correlation)
        {
            axis.retain(|_, counts| options.includes(*counts));
        }
    }
}

/// The names and windows a row's ids resolve to.
struct Lookup<'a> {
    antenna: Vec<String>,
    field: Vec<String>,
    ddid_to_spw: BTreeMap<i32, i32>,
    ddid_correlations: BTreeMap<i32, Vec<String>>,
    correlation_selection: Option<&'a CorrelationSelection>,
    calibration: bool,
}

impl<'a> Lookup<'a> {
    fn read(
        ms: &MeasurementSet,
        correlation_selection: Option<&'a CorrelationSelection>,
    ) -> Result<Self, FlaggingError> {
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
        let calibration = ms.main_table().info().table_type == "Calibration";
        if calibration {
            return Ok(Self {
                antenna,
                field,
                ddid_to_spw: BTreeMap::new(),
                ddid_correlations: BTreeMap::new(),
                correlation_selection,
                calibration,
            });
        }
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
            correlation_selection,
            calibration,
        })
    }

    fn field_name(&self, ms: &MeasurementSet, row: usize, id: i32) -> Result<&str, FlaggingError> {
        if self.calibration && id == -1 {
            return Ok("-1");
        }
        self.field
            .get(usize::try_from(id).unwrap_or(usize::MAX))
            .map(String::as_str)
            .ok_or_else(|| invalid(ms, format!("row {row}: no field {id}")))
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
    fn read(ms: &MeasurementSet, rows: &[usize], calibration: bool) -> Result<Self, FlaggingError> {
        let table = ms.main_table();
        let path = display_ms_path(ms);
        let column = |name: &str| load_i32_cells(table, rows, name, &path);
        Ok(Self {
            antenna1: column("ANTENNA1")?,
            antenna2: column("ANTENNA2")?,
            array: if calibration {
                vec![-1; rows.len()]
            } else {
                column("ARRAY_ID")?
            },
            field: column("FIELD_ID")?,
            // CASA's pre-v4.1 calibration table reader supplies observation 0.
            observation: if calibration
                && table
                    .schema()
                    .is_some_and(|schema| !schema.contains_column("OBSERVATION_ID"))
            {
                vec![0; rows.len()]
            } else {
                column("OBSERVATION_ID")?
            },
            scan: column("SCAN_NUMBER")?,
            ddid: column(if calibration {
                "SPECTRAL_WINDOW_ID"
            } else {
                "DATA_DESC_ID"
            })?,
        })
    }
}

/// The summary-mode report of `rows`: it changes nothing, and carries the
/// summary with its overall counts.
pub(super) fn report(
    ms: &MeasurementSet,
    rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    options: FlagSummaryOptions,
) -> Result<FlagDataReport, FlaggingError> {
    if correlations.is_some_and(|selection| !selection.has_setup()) {
        // With no matching polarization setup CASA cannot create the agent.
        return Err(FlaggingError::MissingCorrelationSummary);
    }
    let summary = summarize(ms, rows, channel_selections, correlations, options)?;
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
        rflag_report: None,
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
    correlations: Option<&CorrelationSelection>,
    options: FlagSummaryOptions,
) -> Result<FlagSummaryBreakdown, FlaggingError> {
    let lookup = Lookup::read(ms, correlations)?;
    let mut summary = FlagSummaryBreakdown::with_options(options);
    let mut fields = options.fieldcnt.then(BTreeMap::new);
    let flags = ms
        .main_table()
        .column_accessor(FLAG_COLUMN)
        .map_err(|source| invalid(ms, source.to_string()))?;
    for block in rows.chunks(BLOCK_ROWS) {
        let ids = BlockIds::read(ms, block, lookup.calibration)?;
        for (batch_index, batch) in block.chunks(FLAG_BATCH_ROWS).enumerate() {
            let start = batch_index * FLAG_BATCH_ROWS;
            let (slots, selected): (Vec<_>, Vec<_>) = batch
                .iter()
                .copied()
                .enumerate()
                .map(|(offset, row)| (start + offset, row))
                .filter(|&(slot, _)| {
                    correlations
                        .is_none_or(|selection| !selection.planes(ids.ddid[slot]).is_empty())
                })
                .unzip();
            if selected.is_empty() {
                continue;
            }
            let cells = flags
                .array_cells_owned_uncached(&selected)
                .map_err(|source| invalid(ms, source.to_string()))?;
            for ((slot, row), cell) in slots.into_iter().zip(selected).zip(cells) {
                let Some(ArrayValue::Bool(flags)) = cell else {
                    return Err(invalid(
                        ms,
                        format!("FLAG row {row} must be a Boolean array"),
                    ));
                };
                let target = match &mut fields {
                    Some(fields) => fields
                        .entry(lookup.field_name(ms, row, ids.field[slot])?)
                        .or_insert_with(|| FlagSummaryBreakdown::with_options(options)),
                    None => &mut summary,
                };
                add_row(
                    ms,
                    &lookup,
                    &ids,
                    (slot, row),
                    &flags,
                    channel_selections,
                    target,
                )?;
            }
        }
    }
    if let Some(fields) = fields {
        if let Some(reserved) = ["type", "name"]
            .into_iter()
            .find(|name| fields.contains_key(name))
        {
            return Err(invalid(
                ms,
                format!(
                    "RecordRep::defineDataField - incorrect data type used for field {reserved}"
                ),
            ));
        }
        let mut by_field = BTreeMap::new();
        for (name, field) in fields {
            summary.overall.add(field.overall);
            // CASA filters the whole field by its totals and keeps its
            // nested breakdowns intact when that field passes.
            if options.includes(field.overall) {
                by_field.insert(name.to_string(), field);
            }
        }
        summary.by_field = Some(by_field);
    } else {
        summary.filter_entries(options);
    }
    Ok(summary)
}

fn add_row(
    ms: &MeasurementSet,
    lookup: &Lookup<'_>,
    ids: &BlockIds,
    (slot, row): (usize, usize),
    flags: &ArrayD<bool>,
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    summary: &mut FlagSummaryBreakdown,
) -> Result<(), FlaggingError> {
    let shape = flags.shape();
    let (correlations, channels) = (shape[0], shape.get(1).copied().unwrap_or(1));
    let ddid = ids.ddid[slot];
    let spw = if lookup.calibration {
        ddid
    } else {
        lookup.ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
            invalid(
                ms,
                format!("row {row}: DATA_DESC_ID {ddid} has no spectral window"),
            )
        })?
    };
    let selected = match channel_selections.and_then(|selections| selections.get(&spw)) {
        Some(selection) => selection
            .indices(channels)
            .map_err(|source| invalid(ms, format!("resolve SPW {spw} channels: {source}")))?,
        None => (0..channels).collect(),
    };
    let names = lookup.ddid_correlations.get(&ddid);
    let planes = lookup
        .correlation_selection
        .map(|selection| selection.planes(ddid));
    let selected_planes = || {
        (0..correlations)
            .filter(|plane| planes.is_none_or(|planes| planes.binary_search(plane).is_ok()))
    };
    let mut row_counts = FlagCounts::default();
    for correlation in selected_planes() {
        let flagged = selected
            .iter()
            .filter(|&&channel| flags[[correlation, channel]])
            .count() as u64;
        let counts = FlagCounts {
            flagged,
            total: selected.len() as u64,
        };
        let name = if lookup.calibration {
            format!("Sol{}", correlation + 1)
        } else {
            names
                .and_then(|names| names.get(correlation))
                .cloned()
                .unwrap_or_else(|| "??".to_string())
        };
        summary
            .correlation
            .entry(name.clone())
            .or_default()
            .add(counts);
        if let Some(spw_correlation) = &mut summary.spw_correlation {
            spw_correlation
                .entry(format!("{spw}:{name}"))
                .or_default()
                .add(counts);
        }
        row_counts.add(counts);
    }
    if let Some(spw_channel) = &mut summary.spw_channel {
        let selected_correlation_count = selected_planes().count() as u64;
        // CASA indexes the selected channel vector within each window.
        for (index, &channel) in selected.iter().enumerate() {
            let flagged = selected_planes()
                .filter(|&correlation| flags[[correlation, channel]])
                .count() as u64;
            spw_channel
                .entry(format!("{spw}:{index}"))
                .or_default()
                .add(FlagCounts {
                    flagged,
                    total: selected_correlation_count,
                });
        }
    }
    let antenna = |id: i32| -> Result<&str, FlaggingError> {
        lookup
            .antenna
            .get(usize::try_from(id).unwrap_or(usize::MAX))
            .map(String::as_str)
            .ok_or_else(|| invalid(ms, format!("row {row}: no antenna {id}")))
    };
    let (antenna1, mut antenna2) = (ids.antenna1[slot], ids.antenna2[slot]);
    if lookup.calibration && antenna2 < 0 {
        antenna2 = antenna1;
    }
    let add = |axis: &mut BTreeMap<String, FlagCounts>, key: String| {
        axis.entry(key).or_default().add(row_counts);
    };
    let first = antenna(antenna1)?;
    let second = antenna(antenna2)?;
    add(&mut summary.antenna, first.to_string());
    if antenna2 != antenna1 {
        add(&mut summary.antenna, second.to_string());
    }
    let scan = ids.scan[slot];
    if let Some(antenna_scan) = &mut summary.antenna_scan {
        add(antenna_scan, format!("{first}:{scan}"));
        if antenna2 != antenna1 {
            add(antenna_scan, format!("{second}:{scan}"));
        }
    }
    if let Some(baseline) = &mut summary.baseline {
        add(baseline, format!("{first}&&{second}"));
    }
    add(
        &mut summary.field,
        lookup.field_name(ms, row, ids.field[slot])?.to_string(),
    );
    add(&mut summary.array, ids.array[slot].to_string());
    add(&mut summary.observation, ids.observation[slot].to_string());
    add(&mut summary.scan, scan.to_string());
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
