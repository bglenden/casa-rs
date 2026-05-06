// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native CASA-style flagging primitives for MeasurementSets.
//!
//! CASA implements `flagdata` through the agentflagger framework. This module
//! follows the same user-facing task families that matter in the VLA flagging
//! tutorial: manual edits, clip-zero edits, quack, TFCrop, RFlag, extension,
//! summaries, and `flagmanager` flag-version snapshots. The automatic RFI
//! modes are native robust-statistics implementations of the CASA algorithm
//! families rather than a binding to CASA C++.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::MsError;
use crate::ms::MeasurementSet;
use crate::schema::main_table::VisibilityDataColumn;
use crate::selection::MsSelection;
use crate::selection_syntax::{ChannelSelection, parse_spw_selector};

const FLAG_COLUMN: &str = "FLAG";
const FLAG_ROW_COLUMN: &str = "FLAG_ROW";
const FLAG_VERSION_LIST: &str = "FLAG_VERSION_LIST";

/// Input visibility column used by automatic flagging modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataColumn {
    /// MAIN.DATA.
    #[default]
    Data,
    /// MAIN.CORRECTED_DATA.
    CorrectedData,
}

impl FlagDataColumn {
    fn name(self) -> &'static str {
        match self {
            Self::Data => VisibilityDataColumn::Data.name(),
            Self::CorrectedData => VisibilityDataColumn::CorrectedData.name(),
        }
    }
}

/// Native `flagdata` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataMode {
    /// Manual selected-row/sample flagging.
    Manual,
    /// Clip mode, currently including the tutorial's `clipzeros=True` path.
    Clip,
    /// CASA quack-style scan-edge flagging.
    Quack,
    /// CASA TFCrop-family robust time/frequency outlier flagging.
    Tfcrop,
    /// CASA RFlag-family two-pass robust time/frequency flagging.
    Rflag,
    /// Extend existing flags in polarization, time, or frequency.
    Extend,
    /// Summarize flags without mutating the MS.
    Summary,
}

/// Flag or unflag action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataAction {
    /// Set selected samples to flagged.
    Flag,
    /// Set selected samples to unflagged.
    Unflag,
}

impl FlagDataAction {
    fn value(self) -> bool {
        matches!(self, Self::Flag)
    }
}

/// Quack scan-edge mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuackMode {
    /// Flag the first interval in each scan.
    #[default]
    Beg,
    /// Flag the last interval in each scan.
    End,
}

/// Request for native `flagdata`.
#[derive(Debug, Clone)]
pub struct FlagDataRequest {
    /// Structured row selection.
    pub selection: MsSelection,
    /// CASA-style SPW/channel selector.
    pub spw: Option<String>,
    /// Mode to execute.
    pub mode: FlagDataMode,
    /// Manual action.
    pub action: FlagDataAction,
    /// Data column for automatic modes.
    pub data_column: FlagDataColumn,
    /// Create a `flagdata_N` backup before mutating flags.
    pub flagbackup: bool,
    /// Clip exact zeros in [`FlagDataMode::Clip`].
    pub clipzeros: bool,
    /// Quack interval in seconds.
    pub quackinterval: f64,
    /// Quack scan edge.
    pub quackmode: QuackMode,
    /// TFCrop time cutoff in robust sigma units.
    pub timecutoff: f64,
    /// TFCrop frequency cutoff in robust sigma units.
    pub freqcutoff: f64,
    /// RFlag time threshold scale.
    pub timedevscale: f64,
    /// RFlag spectral threshold scale.
    pub freqdevscale: f64,
    /// Optional RFlag time threshold. `None` computes one from the data.
    pub timedev: Option<f64>,
    /// Optional RFlag spectral threshold. `None` computes one from the data.
    pub freqdev: Option<f64>,
    /// Extend flags across correlations/polarizations.
    pub extendpols: bool,
    /// If > 0, flag full channel time columns whose flagged percentage meets
    /// this threshold.
    pub growtime: f64,
    /// If > 0, flag full row spectra whose flagged percentage meets this
    /// threshold.
    pub growfreq: f64,
}

impl Default for FlagDataRequest {
    fn default() -> Self {
        Self {
            selection: MsSelection::new(),
            spw: None,
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Flag,
            data_column: FlagDataColumn::Data,
            flagbackup: true,
            clipzeros: false,
            quackinterval: 0.0,
            quackmode: QuackMode::Beg,
            timecutoff: 4.0,
            freqcutoff: 3.0,
            timedevscale: 5.0,
            freqdevscale: 5.0,
            timedev: None,
            freqdev: None,
            extendpols: false,
            growtime: 0.0,
            growfreq: 0.0,
        }
    }
}

/// Report returned by native `flagdata`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FlagDataReport {
    /// Mode executed.
    pub mode: FlagDataMode,
    /// Number of selected MAIN rows.
    pub selected_rows: usize,
    /// Number of changed MAIN rows.
    pub changed_rows: usize,
    /// Number of changed correlation/channel samples.
    pub changed_samples: usize,
    /// Number of flagged samples in the selected rows after execution.
    pub flagged_samples: usize,
    /// Number of total samples in the selected rows after execution.
    pub total_samples: usize,
    /// Auto-created backup version.
    pub backup_version: Option<String>,
    /// RFlag time threshold actually used.
    pub timedev: Option<f64>,
    /// RFlag spectral threshold actually used.
    pub freqdev: Option<f64>,
}

/// Flag-version merge behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagMerge {
    /// Replace destination flags.
    #[default]
    Replace,
    /// Logical OR source and destination flags.
    Or,
    /// Logical AND source and destination flags.
    And,
}

/// One `flagmanager` list entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FlagVersionEntry {
    /// Version name.
    pub name: String,
    /// Version comment.
    pub comment: String,
}

/// Native flagging errors.
#[derive(Debug, Error)]
pub enum FlaggingError {
    /// Opening an MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Saving an MS failed.
    #[error("failed to save MeasurementSet {path}: {source}")]
    SaveMeasurementSet {
        /// Path being saved.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// A flag mutation failed.
    #[error("failed to mutate flags for {path}: {reason}")]
    MutateFlags {
        /// Path being mutated.
        path: String,
        /// Error context.
        reason: String,
    },

    /// Flag-version management failed.
    #[error("failed to manage flag version for {path}: {reason}")]
    FlagVersion {
        /// MeasurementSet path.
        path: String,
        /// Error context.
        reason: String,
    },
}

/// Run native `flagdata` on an opened MeasurementSet.
pub fn flagdata(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
) -> Result<FlagDataReport, FlaggingError> {
    let ms_path = display_ms_path(ms);
    let selected_rows = selected_rows(ms, request)?;
    let channel_selections = request
        .spw
        .as_deref()
        .map(parse_channel_selections)
        .transpose()
        .map_err(|source| FlaggingError::MutateFlags {
            path: ms_path.clone(),
            reason: source.to_string(),
        })?;

    if request.mode == FlagDataMode::Summary {
        return report_after(
            ms,
            request,
            selected_rows.len(),
            ChangeSet::default(),
            None,
            None,
        );
    }

    let backup_version = if request.flagbackup && !selected_rows.is_empty() {
        Some(save_next_flagdata_backup(ms)?)
    } else {
        None
    };

    let changes = match request.mode {
        FlagDataMode::Manual => apply_manual_flags(
            ms,
            &selected_rows,
            channel_selections.as_ref(),
            request.action,
        )?,
        FlagDataMode::Clip => {
            apply_clip_flags(ms, &selected_rows, channel_selections.as_ref(), request)?
        }
        FlagDataMode::Quack => {
            apply_quack_flags(ms, &selected_rows, channel_selections.as_ref(), request)?
        }
        FlagDataMode::Tfcrop => {
            apply_tfcrop_flags(ms, &selected_rows, channel_selections.as_ref(), request)?
        }
        FlagDataMode::Rflag => {
            apply_rflag_flags(ms, &selected_rows, channel_selections.as_ref(), request)?
        }
        FlagDataMode::Extend => {
            apply_extend_flags(ms, &selected_rows, channel_selections.as_ref(), request)?
        }
        FlagDataMode::Summary => ChangeSet::default(),
    };

    if !changes.changed_row_indices.is_empty() {
        ms.main_table_mut()
            .save_selected_rows_in_place_assuming_valid(
                &[FLAG_COLUMN, FLAG_ROW_COLUMN],
                &changes.changed_row_indices,
            )
            .map_err(|source| FlaggingError::MutateFlags {
                path: ms_path,
                reason: format!("save changed MAIN flag rows: {source}"),
            })?;
    }
    let thresholds = if request.mode == FlagDataMode::Rflag {
        rflag_thresholds(ms, &selected_rows, channel_selections.as_ref(), request).ok()
    } else {
        None
    };
    report_after(
        ms,
        request,
        selected_rows.len(),
        changes,
        backup_version,
        thresholds,
    )
}

/// Open an MS, run native `flagdata`, and save the changed MAIN table.
pub fn flagdata_path(
    ms_path: impl AsRef<Path>,
    request: &FlagDataRequest,
) -> Result<FlagDataReport, FlaggingError> {
    let path = ms_path.as_ref();
    let mut ms =
        MeasurementSet::open(path).map_err(|source| FlaggingError::OpenMeasurementSet {
            path: path.display().to_string(),
            source,
        })?;
    flagdata(&mut ms, request)
}

fn report_after(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    selected_row_count: usize,
    changes: ChangeSet,
    backup_version: Option<String>,
    thresholds: Option<(f64, f64)>,
) -> Result<FlagDataReport, FlaggingError> {
    let summary_rows = selected_rows(ms, request)?;
    let summary = match changes.summary {
        Some(summary) => summary,
        None => summarize_flags(ms, &summary_rows)?,
    };
    Ok(FlagDataReport {
        mode: request.mode,
        selected_rows: selected_row_count,
        changed_rows: changes.changed_rows,
        changed_samples: changes.changed_samples,
        flagged_samples: summary.flagged_samples,
        total_samples: summary.total_samples,
        backup_version,
        timedev: thresholds.map(|pair| pair.0),
        freqdev: thresholds.map(|pair| pair.1),
    })
}

fn selected_rows(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
) -> Result<Vec<usize>, FlaggingError> {
    request
        .selection
        .apply(ms)
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("select rows: {source}"),
        })
}

fn apply_manual_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    action: FlagDataAction,
) -> Result<ChangeSet, FlaggingError> {
    let new_flag = action.value();
    mutate_selected_samples(ms, selected_rows, channel_selections, |_amp, old| {
        if old != new_flag {
            Some(new_flag)
        } else {
            None
        }
    })
}

fn apply_clip_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    if !request.clipzeros {
        return Ok(ChangeSet::default());
    }
    let path = display_ms_path(ms);
    let ddid_to_spw = data_description_spw_map(ms)?;
    let mut updates = std::collections::BTreeMap::<usize, ArrayD<bool>>::new();
    let mut changed = ChangeSet::default();
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    for &row in selected_rows {
        let table = ms.main_table();
        let ddid = table_i32(table, row, "DATA_DESC_ID", ms.path())?;
        let spw = ddid_to_spw
            .get(&ddid)
            .copied()
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
            })?;
        let old_flags = clone_flag_matrix(ms, row)?;
        let data_value = table
            .cell_accessor(row, request.data_column.name())
            .and_then(|cell| cell.value().map(|value| value.cloned()))
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read {} row {row}: {source}", request.data_column.name()),
            })?
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("{} row {row} is undefined", request.data_column.name()),
            })?;
        let data_shape =
            numeric_array_shape(&data_value).ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "{} row {row} must be numeric array, found {data_value:?}",
                    request.data_column.name()
                ),
            })?;
        if data_shape != old_flags.shape() {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "{} and FLAG shapes differ on row {row}: {:?} vs {:?}",
                    request.data_column.name(),
                    data_shape,
                    old_flags.shape()
                ),
            });
        }
        if data_shape.len() != 2 {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("{} row {row} is not rank-2", request.data_column.name()),
            });
        }
        total_samples += old_flags.len();
        flagged_samples += old_flags.iter().filter(|flag| **flag).count();
        let channels =
            match channel_selections.and_then(|selectors| selectors.get(&spw)) {
                Some(selection) => selection.indices(data_shape[1]).map_err(|source| {
                    FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("resolve SPW {spw} channels: {source}"),
                    }
                })?,
                None => (0..data_shape[1]).collect(),
            };
        let mut flags = old_flags.clone();
        let mut row_changed = false;
        for corr in 0..data_shape[0] {
            for &chan in &channels {
                let index = IxDyn(&[corr, chan]);
                if !old_flags[index.clone()] && is_casa_clip_zero_at(&data_value, &index)? {
                    flags[index] = true;
                    changed.changed_samples += 1;
                    flagged_samples += 1;
                    row_changed = true;
                }
            }
        }
        if row_changed {
            updates.insert(row, flags);
        }
    }
    write_flag_updates(ms, updates, &mut changed)?;
    changed.summary = Some(FlagSummary {
        flagged_samples,
        total_samples,
    });
    Ok(changed)
}

fn apply_quack_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let table = ms.main_table();
    let mut by_scan = BTreeMap::<i32, Vec<(usize, f64)>>::new();
    for &row in selected_rows {
        let scan = table_i32(table, row, "SCAN_NUMBER", ms.path())?;
        let time = table_f64(table, row, "TIME", ms.path())?;
        by_scan.entry(scan).or_default().push((row, time));
    }
    let mut quack_rows = Vec::new();
    for rows in by_scan.values_mut() {
        rows.sort_by(|left, right| left.1.total_cmp(&right.1));
        let Some(edge_time) = (match request.quackmode {
            QuackMode::Beg => rows.first().map(|row| row.1),
            QuackMode::End => rows.last().map(|row| row.1),
        }) else {
            continue;
        };
        for &(row, time) in rows.iter() {
            let in_edge = match request.quackmode {
                QuackMode::Beg => time - edge_time < request.quackinterval,
                QuackMode::End => edge_time - time < request.quackinterval,
            };
            if in_edge {
                quack_rows.push(row);
            }
        }
    }
    apply_manual_flags(ms, &quack_rows, channel_selections, FlagDataAction::Flag)
}

fn apply_tfcrop_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let samples = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let mut to_flag = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut freq_flags = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut time_flags = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    for group in group_samples(&samples).values() {
        for by_corr in samples_by_corr(group) {
            for by_row in samples_by_row(&by_corr).values() {
                flag_outliers(by_row, request.freqcutoff, &mut freq_flags);
            }
            for by_chan in samples_by_chan(&by_corr).values() {
                flag_outliers(by_chan, request.timecutoff, &mut time_flags);
            }
        }
    }
    to_flag.extend(freq_flags.iter().copied());
    to_flag.extend(time_flags.iter().copied());
    trace_flagdata(json!({
        "mode": "tfcrop",
        "implementation": "casa-rs-native-current",
        "selected_rows": selected_rows.len(),
        "samples": samples.len(),
        "groups": group_samples(&samples).len(),
        "freqcutoff": request.freqcutoff,
        "timecutoff": request.timecutoff,
        "freq_candidate_samples": freq_flags.len(),
        "time_candidate_samples": time_flags.len(),
        "union_candidate_samples": to_flag.len(),
        "union_by_spw_corr": flag_set_counts_by_spw_corr(&to_flag, &samples),
    }));
    apply_flag_set(ms, &to_flag)
}

fn apply_rflag_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let samples = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let (timedev, freqdev) = rflag_thresholds_for_samples(&samples, request);
    let mut to_flag = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut time_flags = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut freq_flags = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    for group in group_samples(&samples).values() {
        for by_corr in samples_by_corr(group) {
            for by_chan in samples_by_chan(&by_corr).values() {
                if robust_sigma(by_chan.iter().map(|sample| sample.amp)) > timedev {
                    for sample in by_chan {
                        time_flags.insert((sample.row, sample.corr, sample.chan));
                    }
                }
            }
            for by_row in samples_by_row(&by_corr).values() {
                let med = median(by_row.iter().map(|sample| sample.amp).collect());
                for sample in by_row {
                    if (sample.amp - med).abs() > freqdev {
                        freq_flags.insert((sample.row, sample.corr, sample.chan));
                    }
                }
            }
        }
    }
    to_flag.extend(time_flags.iter().copied());
    to_flag.extend(freq_flags.iter().copied());
    trace_flagdata(json!({
        "mode": "rflag",
        "implementation": "casa-rs-native-current",
        "selected_rows": selected_rows.len(),
        "samples": samples.len(),
        "groups": group_samples(&samples).len(),
        "timedev": timedev,
        "freqdev": freqdev,
        "time_candidate_samples": time_flags.len(),
        "freq_candidate_samples": freq_flags.len(),
        "union_candidate_samples": to_flag.len(),
        "union_by_spw_corr": flag_set_counts_by_spw_corr(&to_flag, &samples),
    }));
    apply_flag_set(ms, &to_flag)
}

fn apply_extend_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let samples = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let mut to_flag = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    if request.extendpols {
        for group in samples_by_row_chan(&samples).values() {
            if group.iter().any(|sample| sample.flag) {
                for sample in group {
                    to_flag.insert((sample.row, sample.corr, sample.chan));
                }
            }
        }
    }
    if request.growtime > 0.0 {
        for group in group_samples(&samples).values() {
            for by_chan in samples_by_chan(group).values() {
                let frac = percent_flagged(by_chan);
                if frac >= request.growtime {
                    for sample in by_chan {
                        to_flag.insert((sample.row, sample.corr, sample.chan));
                    }
                }
            }
        }
    }
    if request.growfreq > 0.0 {
        for by_row in samples_by_row(&samples).values() {
            let frac = percent_flagged(by_row);
            if frac >= request.growfreq {
                for sample in by_row {
                    to_flag.insert((sample.row, sample.corr, sample.chan));
                }
            }
        }
    }
    apply_flag_set(ms, &to_flag)
}

fn mutate_selected_samples<F>(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    mut edit: F,
) -> Result<ChangeSet, FlaggingError>
where
    F: FnMut(f64, bool) -> Option<bool>,
{
    let samples = load_samples(ms, selected_rows, channel_selections, FlagDataColumn::Data)?;
    let mut updates = std::collections::BTreeMap::<usize, ArrayD<bool>>::new();
    let mut changed = ChangeSet::default();
    for sample in samples {
        let Some(new_flag) = edit(sample.amp, sample.flag) else {
            continue;
        };
        let flags = updates
            .entry(sample.row)
            .or_insert(clone_flag_matrix(ms, sample.row)?);
        if let Some(value) = flags.get_mut(IxDyn(&[sample.corr, sample.chan])) {
            if *value != new_flag {
                *value = new_flag;
                changed.changed_samples += 1;
            }
        }
    }
    write_flag_updates(ms, updates, &mut changed)?;
    Ok(changed)
}

fn apply_flag_set(
    ms: &mut MeasurementSet,
    to_flag: &std::collections::BTreeSet<(usize, usize, usize)>,
) -> Result<ChangeSet, FlaggingError> {
    let mut updates = std::collections::BTreeMap::<usize, ArrayD<bool>>::new();
    let mut changed = ChangeSet::default();
    for &(row, corr, chan) in to_flag {
        let flags = updates.entry(row).or_insert(clone_flag_matrix(ms, row)?);
        if let Some(value) = flags.get_mut(IxDyn(&[corr, chan])) {
            if !*value {
                *value = true;
                changed.changed_samples += 1;
            }
        }
    }
    write_flag_updates(ms, updates, &mut changed)?;
    Ok(changed)
}

fn write_flag_row_update(
    ms: &mut MeasurementSet,
    row: usize,
    flags: ArrayD<bool>,
    flag_row: bool,
    changed: &mut ChangeSet,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    ms.main_table_mut()
        .cell_accessor_mut(row, FLAG_COLUMN)
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open FLAG row {row}: {source}"),
        })?
        .set(Value::Array(ArrayValue::Bool(flags)))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("write FLAG row {row}: {source}"),
        })?;
    ms.main_table_mut()
        .cell_accessor_mut(row, FLAG_ROW_COLUMN)
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open FLAG_ROW row {row}: {source}"),
        })?
        .set(Value::Scalar(ScalarValue::Bool(flag_row)))
        .map_err(|source| FlaggingError::MutateFlags {
            path,
            reason: format!("write FLAG_ROW row {row}: {source}"),
        })?;
    changed.changed_rows += 1;
    changed.changed_row_indices.push(row);
    Ok(())
}

fn write_flag_updates(
    ms: &mut MeasurementSet,
    updates: std::collections::BTreeMap<usize, ArrayD<bool>>,
    changed: &mut ChangeSet,
) -> Result<(), FlaggingError> {
    for (row, flags) in updates {
        let old = clone_flag_matrix(ms, row)?;
        if old == flags {
            continue;
        }
        let flag_row = flags.iter().all(|flag| *flag);
        write_flag_row_update(ms, row, flags, flag_row, changed)?;
    }
    Ok(())
}

fn load_samples(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    data_column: FlagDataColumn,
) -> Result<Vec<Sample>, FlaggingError> {
    let path = display_ms_path(ms);
    let ddid_to_spw = data_description_spw_map(ms)?;
    let mut samples = Vec::new();
    for &row in selected_rows {
        let table = ms.main_table();
        let ddid = table_i32(table, row, "DATA_DESC_ID", ms.path())?;
        let spw = ddid_to_spw
            .get(&ddid)
            .copied()
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
            })?;
        let flags = clone_flag_matrix(ms, row)?;
        let data = visibility_amplitudes(table, row, data_column.name(), ms.path())?;
        if data.shape() != flags.shape() {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "{} and FLAG shapes differ on row {row}: {:?} vs {:?}",
                    data_column.name(),
                    data.shape(),
                    flags.shape()
                ),
            });
        }
        let shape = data.shape();
        if shape.len() != 2 {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("{} row {row} is not rank-2", data_column.name()),
            });
        }
        let channels = match channel_selections.and_then(|selectors| selectors.get(&spw)) {
            Some(selection) => {
                selection
                    .indices(shape[1])
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("resolve SPW {spw} channels: {source}"),
                    })?
            }
            None => (0..shape[1]).collect(),
        };
        let field = table_i32(table, row, "FIELD_ID", ms.path())?;
        let scan = table_i32(table, row, "SCAN_NUMBER", ms.path())?;
        let ant1 = table_i32(table, row, "ANTENNA1", ms.path())?;
        let ant2 = table_i32(table, row, "ANTENNA2", ms.path())?;
        for corr in 0..shape[0] {
            for &chan in &channels {
                samples.push(Sample {
                    row,
                    corr,
                    chan,
                    amp: data[IxDyn(&[corr, chan])],
                    flag: flags[IxDyn(&[corr, chan])],
                    field,
                    spw,
                    scan,
                    ant1,
                    ant2,
                });
            }
        }
    }
    Ok(samples)
}

fn visibility_amplitudes(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<ArrayD<f64>, FlaggingError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value().map(|value| value.cloned()))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value.ok_or_else(|| FlaggingError::MutateFlags {
        path: display_path(path),
        reason: format!("{column} row {row} is undefined"),
    })? {
        Value::Array(ArrayValue::Complex32(values)) => Ok(values.mapv(|value| value.norm() as f64)),
        Value::Array(ArrayValue::Complex64(values)) => Ok(values.mapv(|value| value.norm())),
        Value::Array(ArrayValue::Float32(values)) => Ok(values.mapv(f64::from)),
        Value::Array(ArrayValue::Float64(values)) => Ok(values.clone()),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be numeric array, found {other:?}"),
        }),
    }
}

fn is_casa_clip_zero(value: f64) -> bool {
    value.is_nan() || value <= f64::from(f32::EPSILON)
}

fn numeric_array_shape(value: &Value) -> Option<&[usize]> {
    match value {
        Value::Array(ArrayValue::Complex32(values)) => Some(values.shape()),
        Value::Array(ArrayValue::Complex64(values)) => Some(values.shape()),
        Value::Array(ArrayValue::Float32(values)) => Some(values.shape()),
        Value::Array(ArrayValue::Float64(values)) => Some(values.shape()),
        _ => None,
    }
}

fn is_casa_clip_zero_at(value: &Value, index: &IxDyn) -> Result<bool, FlaggingError> {
    Ok(match value {
        Value::Array(ArrayValue::Complex32(values)) => values
            .get(index.clone())
            .is_some_and(|value| is_casa_clip_zero(value.norm() as f64)),
        Value::Array(ArrayValue::Complex64(values)) => values
            .get(index.clone())
            .is_some_and(|value| is_casa_clip_zero(value.norm())),
        Value::Array(ArrayValue::Float32(values)) => values
            .get(index.clone())
            .is_some_and(|value| is_casa_clip_zero(f64::from(*value))),
        Value::Array(ArrayValue::Float64(values)) => values
            .get(index.clone())
            .is_some_and(|value| is_casa_clip_zero(*value)),
        _ => false,
    })
}

fn summarize_flags(
    ms: &MeasurementSet,
    selected_rows: &[usize],
) -> Result<FlagSummary, FlaggingError> {
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    for &row in selected_rows {
        let flags = clone_flag_matrix(ms, row)?;
        flagged_samples += flags.iter().filter(|flag| **flag).count();
        total_samples += flags.len();
    }
    Ok(FlagSummary {
        flagged_samples,
        total_samples,
    })
}

fn rflag_thresholds(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<(f64, f64), FlaggingError> {
    let samples = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    Ok(rflag_thresholds_for_samples(&samples, request))
}

fn rflag_thresholds_for_samples(samples: &[Sample], request: &FlagDataRequest) -> (f64, f64) {
    let time_values = group_samples(samples)
        .values()
        .flat_map(|group| {
            samples_by_corr(group)
                .into_iter()
                .flat_map(|by_corr| {
                    samples_by_chan(&by_corr)
                        .into_values()
                        .map(|samples| robust_sigma(samples.iter().map(|sample| sample.amp)))
                })
                .collect::<Vec<_>>()
        })
        .filter(|value| *value > 0.0)
        .collect::<Vec<_>>();
    let freq_values = samples_by_row(samples)
        .into_values()
        .flat_map(|samples| {
            let med = median(samples.iter().map(|sample| sample.amp).collect());
            samples
                .into_iter()
                .map(move |sample| (sample.amp - med).abs())
        })
        .filter(|value| *value > 0.0)
        .collect::<Vec<_>>();
    let timedev = request
        .timedev
        .unwrap_or_else(|| robust_threshold(time_values) * request.timedevscale);
    let freqdev = request
        .freqdev
        .unwrap_or_else(|| robust_threshold(freq_values) * request.freqdevscale);
    (timedev, freqdev)
}

fn flag_outliers(
    samples: &[Sample],
    cutoff: f64,
    to_flag: &mut std::collections::BTreeSet<(usize, usize, usize)>,
) {
    if samples.len() < 3 {
        return;
    }
    let values = samples.iter().map(|sample| sample.amp).collect::<Vec<_>>();
    let med = median(values.clone());
    let sigma = robust_sigma(values);
    if sigma <= f64::EPSILON {
        return;
    }
    for sample in samples {
        if !sample.flag && (sample.amp - med).abs() > cutoff * sigma {
            to_flag.insert((sample.row, sample.corr, sample.chan));
        }
    }
}

fn robust_threshold(values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }
    let med = median(values.clone());
    let mad = median(values.iter().map(|value| (*value - med).abs()).collect());
    med + 1.4826 * mad
}

fn robust_sigma(values: impl IntoIterator<Item = f64>) -> f64 {
    let values = values.into_iter().collect::<Vec<_>>();
    if values.len() < 2 {
        return 0.0;
    }
    let med = median(values.clone());
    let mad = median(values.iter().map(|value| (*value - med).abs()).collect());
    let robust = 1.4826 * mad;
    if robust > f64::EPSILON {
        return robust;
    }
    standard_deviation(values)
}

fn standard_deviation(values: Vec<f64>) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn percent_flagged(samples: &[Sample]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    100.0 * samples.iter().filter(|sample| sample.flag).count() as f64 / samples.len() as f64
}

fn trace_flagdata(value: serde_json::Value) {
    let Ok(path) = std::env::var("CASA_RS_FLAGDATA_TRACE") else {
        return;
    };
    match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut file) => {
            if serde_json::to_writer(&mut file, &value).is_ok() {
                let _ = file.write_all(b"\n");
            }
        }
        Err(error) => eprintln!("failed to open CASA_RS_FLAGDATA_TRACE {path}: {error}"),
    }
}

fn flag_set_counts_by_spw_corr(
    flags: &std::collections::BTreeSet<(usize, usize, usize)>,
    samples: &[Sample],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for sample in samples {
        if flags.contains(&(sample.row, sample.corr, sample.chan)) {
            *counts
                .entry(format!("spw{}_corr{}", sample.spw, sample.corr))
                .or_default() += 1;
        }
    }
    counts
}

type GroupKey = (i32, i32, i32, i32, i32);

fn group_samples(samples: &[Sample]) -> BTreeMap<GroupKey, Vec<Sample>> {
    let mut groups = BTreeMap::<GroupKey, Vec<Sample>>::new();
    for sample in samples {
        let ant_low = sample.ant1.min(sample.ant2);
        let ant_high = sample.ant1.max(sample.ant2);
        groups
            .entry((sample.field, sample.spw, sample.scan, ant_low, ant_high))
            .or_default()
            .push(sample.clone());
    }
    groups
}

fn samples_by_corr(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let mut groups = BTreeMap::<usize, Vec<Sample>>::new();
    for sample in samples {
        groups.entry(sample.corr).or_default().push(sample.clone());
    }
    groups.into_values().collect()
}

fn samples_by_row(samples: &[Sample]) -> BTreeMap<usize, Vec<Sample>> {
    let mut groups = BTreeMap::<usize, Vec<Sample>>::new();
    for sample in samples {
        groups.entry(sample.row).or_default().push(sample.clone());
    }
    groups
}

fn samples_by_chan(samples: &[Sample]) -> BTreeMap<usize, Vec<Sample>> {
    let mut groups = BTreeMap::<usize, Vec<Sample>>::new();
    for sample in samples {
        groups.entry(sample.chan).or_default().push(sample.clone());
    }
    groups
}

fn samples_by_row_chan(samples: &[Sample]) -> BTreeMap<(usize, usize), Vec<Sample>> {
    let mut groups = BTreeMap::<(usize, usize), Vec<Sample>>::new();
    for sample in samples {
        groups
            .entry((sample.row, sample.chan))
            .or_default()
            .push(sample.clone());
    }
    groups
}

fn parse_channel_selections(value: &str) -> Result<BTreeMap<i32, ChannelSelection>, MsError> {
    let mut selections = BTreeMap::new();
    for selector in parse_spw_selector(value)? {
        if let Some(channels) = selector.channels {
            selections.insert(selector.spw_id, channels);
        }
    }
    Ok(selections)
}

fn data_description_spw_map(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, FlaggingError> {
    let table = ms
        .data_description()
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("open DATA_DESCRIPTION: {source}"),
        })?;
    let mut result = BTreeMap::new();
    for row in 0..table.row_count() {
        result.insert(
            row as i32,
            table
                .spectral_window_id(row)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("read DATA_DESCRIPTION row {row}: {source}"),
                })?,
        );
    }
    Ok(result)
}

/// Return flag-version list entries.
pub fn list_flag_versions(ms: &MeasurementSet) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let path = ms_path(ms)?;
    list_flag_versions_for_path(path)
}

/// Save current MAIN flags to a named flag version.
pub fn save_flag_version(
    ms: &MeasurementSet,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    ensure_flagversions_dir(path)?;
    let version_path = flag_version_table_path(path, versionname);
    if version_path.exists() {
        merge_flags_into_existing_version(ms, &version_path, merge)?;
        update_flag_version_comment(path, versionname, comment)?;
        return Ok(());
    }
    append_flag_version_entry(path, versionname, comment)?;
    let mut table =
        Table::with_schema(
            flag_version_schema().map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("build flag-version schema: {source}"),
            })?,
        );
    for row in 0..ms.row_count() {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    FLAG_COLUMN,
                    Value::Array(ArrayValue::Bool(clone_flag_matrix(ms, row)?)),
                ),
                RecordField::new(
                    FLAG_ROW_COLUMN,
                    Value::Scalar(ScalarValue::Bool(flag_row_value(ms, row)?)),
                ),
            ]))
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("add flag-version row: {source}"),
            })?;
    }
    let mut bindings = HashMap::new();
    bindings.insert(
        FLAG_COLUMN.to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    );
    table
        .save_with_bindings(
            TableOptions::new(&version_path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("save flag version {versionname:?}: {source}"),
        })
}

/// Restore a named flag version to MAIN.
pub fn restore_flag_version(
    ms: &mut MeasurementSet,
    versionname: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?.to_path_buf();
    let version_path = flag_version_table_path(&path, versionname);
    let table = Table::open(TableOptions::new(&version_path)).map_err(|source| {
        FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("open flag version {versionname:?}: {source}"),
        }
    })?;
    let mut updates = BTreeMap::new();
    let mut flag_row_updates = BTreeMap::new();
    for row in 0..ms.row_count() {
        let source = table_flag_matrix(&table, row, FLAG_COLUMN, Some(&path))?;
        let source_flag_row = table_bool(&table, row, FLAG_ROW_COLUMN, Some(&path))?;
        let dest = clone_flag_matrix(ms, row)?;
        let dest_flag_row = flag_row_value(ms, row)?;
        updates.insert(row, merge_bool_arrays(&source, &dest, merge)?);
        flag_row_updates.insert(row, merge_bool(source_flag_row, dest_flag_row, merge));
    }
    let mut changed = ChangeSet::default();
    for (row, flags) in updates {
        let flag_row = flag_row_updates
            .remove(&row)
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.display().to_string(),
                reason: format!("missing FLAG_ROW update for row {row}"),
            })?;
        if clone_flag_matrix(ms, row)? == flags && flag_row_value(ms, row)? == flag_row {
            continue;
        }
        write_flag_row_update(ms, row, flags, flag_row, &mut changed)?;
    }
    ms.main_table_mut()
        .save_selected_rows_in_place_assuming_valid(
            &[FLAG_COLUMN, FLAG_ROW_COLUMN],
            &changed.changed_row_indices,
        )
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.display().to_string(),
            reason: format!("save restored flags: {source}"),
        })
}

/// Delete a named flag version.
pub fn delete_flag_version(ms: &MeasurementSet, versionname: &str) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let version_path = flag_version_table_path(path, versionname);
    if version_path.exists() {
        fs::remove_dir_all(&version_path).map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("delete flag version {versionname:?}: {source}"),
        })?;
    }
    remove_flag_version_entry(path, versionname)
}

/// Rename a flag version.
pub fn rename_flag_version(
    ms: &MeasurementSet,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, oldname)?;
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    fs::rename(
        flag_version_table_path(path, oldname),
        flag_version_table_path(path, versionname),
    )
    .map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("rename flag version {oldname:?}: {source}"),
    })?;
    rename_flag_version_entry(path, oldname, versionname, comment)
}

fn save_next_flagdata_backup(ms: &MeasurementSet) -> Result<String, FlaggingError> {
    let versions = list_flag_versions(ms)?;
    let mut index = 1;
    loop {
        let candidate = format!("flagdata_{index}");
        if !versions.iter().any(|entry| entry.name == candidate) {
            save_flag_version(ms, &candidate, "flagdata auto-backup", FlagMerge::Replace)?;
            return Ok(candidate);
        }
        index += 1;
    }
}

fn flag_version_schema() -> Result<TableSchema, casa_tables::SchemaError> {
    TableSchema::new(vec![
        ColumnSchema::array_variable(FLAG_COLUMN, PrimitiveType::Bool, Some(2)),
        ColumnSchema::scalar(FLAG_ROW_COLUMN, PrimitiveType::Bool),
    ])
}

fn list_flag_versions_for_path(path: &Path) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let mut entries = vec![FlagVersionEntry {
        name: "main".to_string(),
        comment: "working copy in main table".to_string(),
    }];
    let list_path = flag_version_list_path(path);
    if !list_path.exists() {
        return Ok(entries);
    }
    let text = fs::read_to_string(&list_path).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("read FLAG_VERSION_LIST: {source}"),
    })?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (name, comment) = line
            .split_once(" : ")
            .map(|(name, comment)| (name.to_string(), comment.to_string()))
            .unwrap_or_else(|| (line.to_string(), String::new()));
        entries.push(FlagVersionEntry { name, comment });
    }
    Ok(entries)
}

fn append_flag_version_entry(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        entries.push(FlagVersionEntry {
            name: versionname.to_string(),
            comment: comment.to_string(),
        });
        entries
    })
}

fn update_flag_version_comment(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    if comment.is_empty() {
        return Ok(());
    }
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == versionname {
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

fn remove_flag_version_entry(path: &Path, versionname: &str) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |entries| {
        entries
            .into_iter()
            .filter(|entry| entry.name != versionname)
            .collect()
    })
}

fn rename_flag_version_entry(
    path: &Path,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == oldname {
                entry.name = versionname.to_string();
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

fn rewrite_flag_version_text<F>(path: &Path, edit: F) -> Result<(), FlaggingError>
where
    F: FnOnce(Vec<FlagVersionEntry>) -> Vec<FlagVersionEntry>,
{
    ensure_flagversions_dir(path)?;
    let entries = edit(
        list_flag_versions_for_path(path)?
            .into_iter()
            .skip(1)
            .collect(),
    );
    let text = entries
        .into_iter()
        .map(|entry| format!("{} : {}\n", entry.name, entry.comment))
        .collect::<String>();
    fs::write(flag_version_list_path(path), text).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("write FLAG_VERSION_LIST: {source}"),
    })
}

fn merge_flags_into_existing_version(
    ms: &MeasurementSet,
    version_path: &Path,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    let path = ms_path(ms)?.to_path_buf();
    let mut table = Table::open(TableOptions::new(version_path)).map_err(|source| {
        FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("open existing flag version: {source}"),
        }
    })?;
    for row in 0..ms.row_count() {
        let source = clone_flag_matrix(ms, row)?;
        let source_flag_row = flag_row_value(ms, row)?;
        let dest = table_flag_matrix(&table, row, FLAG_COLUMN, Some(&path))?;
        let dest_flag_row = table_bool(&table, row, FLAG_ROW_COLUMN, Some(&path))?;
        let merged = merge_bool_arrays(&source, &dest, merge)?;
        let merged_flag_row = merge_bool(source_flag_row, dest_flag_row, merge);
        table
            .cell_accessor_mut(row, FLAG_COLUMN)
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("open existing FLAG row {row}: {source}"),
            })?
            .set(Value::Array(ArrayValue::Bool(merged)))
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("write existing FLAG row {row}: {source}"),
            })?;
        table
            .cell_accessor_mut(row, FLAG_ROW_COLUMN)
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("open existing FLAG_ROW row {row}: {source}"),
            })?
            .set(Value::Scalar(ScalarValue::Bool(merged_flag_row)))
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("write existing FLAG_ROW row {row}: {source}"),
            })?;
    }
    table
        .save(TableOptions::new(version_path))
        .map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("save existing flag version: {source}"),
        })
}

fn merge_bool(source: bool, dest: bool, merge: FlagMerge) -> bool {
    match merge {
        FlagMerge::Replace => source,
        FlagMerge::Or => source || dest,
        FlagMerge::And => source && dest,
    }
}

fn merge_bool_arrays(
    source: &ArrayD<bool>,
    dest: &ArrayD<bool>,
    merge: FlagMerge,
) -> Result<ArrayD<bool>, FlaggingError> {
    if source.shape() != dest.shape() {
        return Err(FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!(
                "cannot merge flag arrays with shapes {:?} and {:?}",
                source.shape(),
                dest.shape()
            ),
        });
    }
    let values = source
        .iter()
        .zip(dest.iter())
        .map(|(source, dest)| match merge {
            FlagMerge::Replace => *source,
            FlagMerge::Or => *source || *dest,
            FlagMerge::And => *source && *dest,
        })
        .collect::<Vec<_>>();
    ArrayD::from_shape_vec(IxDyn(source.shape()), values).map_err(|source| {
        FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!("build merged flag array: {source}"),
        }
    })
}

fn ensure_flagversions_dir(path: &Path) -> Result<PathBuf, FlaggingError> {
    let dir = flagversions_dir(path);
    fs::create_dir_all(&dir).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("create flagversions directory: {source}"),
    })?;
    let list = dir.join(FLAG_VERSION_LIST);
    if !list.exists() {
        fs::write(&list, "").map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("create FLAG_VERSION_LIST: {source}"),
        })?;
    }
    Ok(dir)
}

fn flagversions_dir(path: &Path) -> PathBuf {
    let mut os = OsString::from(path.as_os_str());
    os.push(".flagversions");
    PathBuf::from(os)
}

fn flag_version_list_path(path: &Path) -> PathBuf {
    flagversions_dir(path).join(FLAG_VERSION_LIST)
}

fn flag_version_table_path(path: &Path, versionname: &str) -> PathBuf {
    flagversions_dir(path).join(format!("flags.{versionname}"))
}

fn validate_version_name(ms: &MeasurementSet, versionname: &str) -> Result<(), FlaggingError> {
    if versionname.is_empty() || versionname == "main" || versionname.contains('/') {
        return Err(FlaggingError::FlagVersion {
            path: display_ms_path(ms),
            reason: format!("illegal flag version name {versionname:?}"),
        });
    }
    Ok(())
}

fn clone_flag_matrix(ms: &MeasurementSet, row: usize) -> Result<ArrayD<bool>, FlaggingError> {
    table_flag_matrix(ms.main_table(), row, FLAG_COLUMN, ms.path())
}

fn flag_row_value(ms: &MeasurementSet, row: usize) -> Result<bool, FlaggingError> {
    table_bool(ms.main_table(), row, FLAG_ROW_COLUMN, ms.path())
}

fn table_flag_matrix(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<ArrayD<bool>, FlaggingError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value().map(|value| value.cloned()))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value.ok_or_else(|| FlaggingError::MutateFlags {
        path: display_path(path),
        reason: format!("{column} row {row} is undefined"),
    })? {
        Value::Array(ArrayValue::Bool(flags)) => Ok(flags),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool array, found {other:?}"),
        }),
    }
}

fn table_bool(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<bool, FlaggingError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value().map(|value| value.cloned()))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value.ok_or_else(|| FlaggingError::MutateFlags {
        path: display_path(path),
        reason: format!("{column} row {row} is undefined"),
    })? {
        Value::Scalar(ScalarValue::Bool(value)) => Ok(value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool scalar, found {other:?}"),
        }),
    }
}

fn table_i32(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<i32, FlaggingError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value().map(|value| value.cloned()))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value.ok_or_else(|| FlaggingError::MutateFlags {
        path: display_path(path),
        reason: format!("{column} row {row} is undefined"),
    })? {
        Value::Scalar(ScalarValue::Int32(value)) => Ok(value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Int32 scalar, found {other:?}"),
        }),
    }
}

fn table_f64(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<f64, FlaggingError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value().map(|value| value.cloned()))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value.ok_or_else(|| FlaggingError::MutateFlags {
        path: display_path(path),
        reason: format!("{column} row {row} is undefined"),
    })? {
        Value::Scalar(ScalarValue::Float64(value)) => Ok(value),
        Value::Scalar(ScalarValue::Float32(value)) => Ok(f64::from(value)),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Float scalar, found {other:?}"),
        }),
    }
}

fn ms_path(ms: &MeasurementSet) -> Result<&Path, FlaggingError> {
    ms.path().ok_or_else(|| FlaggingError::FlagVersion {
        path: "<memory>".to_string(),
        reason: "flag versions require a disk-backed MeasurementSet".to_string(),
    })
}

fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<memory>".to_string())
}

fn display_path(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "<memory>".to_string())
}

#[derive(Debug, Clone)]
struct Sample {
    row: usize,
    corr: usize,
    chan: usize,
    amp: f64,
    flag: bool,
    field: i32,
    spw: i32,
    scan: i32,
    ant1: i32,
    ant2: i32,
}

#[derive(Default)]
struct ChangeSet {
    changed_rows: usize,
    changed_samples: usize,
    changed_row_indices: Vec<usize>,
    summary: Option<FlagSummary>,
}

#[derive(Clone)]
struct FlagSummary {
    flagged_samples: usize,
    total_samples: usize,
}

#[allow(dead_code)]
fn _complex_norm32(value: Complex32) -> f64 {
    value.norm() as f64
}

#[allow(dead_code)]
fn _complex_norm64(value: Complex64) -> f64 {
    value.norm()
}
