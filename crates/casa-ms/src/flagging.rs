// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native CASA-style flagging primitives for MeasurementSets.
//!
//! CASA implements `flagdata` through the agentflagger framework. This module
//! follows the same user-facing task families that matter in the VLA flagging
//! tutorial: manual edits, clip-zero edits, quack, TFCrop, RFlag, extension,
//! summaries, and `flagmanager` flag-version snapshots. The automatic RFI
//! modes are native robust-statistics implementations of the CASA algorithm
//! families rather than a binding to CASA C++.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, Ix2, IxDyn};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::MsError;
use crate::least_squares::solve_weighted_least_squares;
use crate::ms::MeasurementSet;
use crate::schema::main_table::VisibilityDataColumn;
use crate::selection::MsSelection;
use crate::selection_syntax::{ChannelSelection, parse_spw_selector};

const FLAG_COLUMN: &str = "FLAG";
const FLAG_ROW_COLUMN: &str = "FLAG_ROW";
const FLAG_VERSION_LIST: &str = "FLAG_VERSION_LIST";
const CLIP_SCAN_CHUNK_ROWS: usize = 4096;
type FlagSampleKey = (usize, usize, usize);
type FlagSampleSet = HashSet<FlagSampleKey>;

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
    /// RFlag maximum acceptable spectral standard deviation before flagging
    /// the full row spectrum.
    pub spectralmax: f64,
    /// RFlag minimum acceptable spectral standard deviation before flagging
    /// the full row spectrum.
    pub spectralmin: f64,
    /// Optional RFlag time threshold. `None` computes one from the data.
    pub timedev: Option<f64>,
    /// Optional RFlag spectral threshold. `None` computes one from the data.
    pub freqdev: Option<f64>,
    /// Run CASA's automatic post-extension agent after TFCrop/RFlag.
    pub extendflags: bool,
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
            spectralmax: 1.0e6,
            spectralmin: 0.0,
            timedev: None,
            freqdev: None,
            extendflags: true,
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
        if changes.changed_flag_row_rows == 0 {
            ms.main_table_mut()
                .save_selected_rows_in_place_assuming_valid(
                    &[FLAG_COLUMN],
                    &changes.changed_row_indices,
                )
                .map_err(|source| FlaggingError::MutateFlags {
                    path: ms_path,
                    reason: format!("save changed MAIN flag rows: {source}"),
                })?;
        } else {
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
    }
    let thresholds = changes.thresholds;
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
    let summary = match changes.summary {
        Some(summary) => summary,
        None => {
            let summary_rows = selected_rows(ms, request)?;
            summarize_flags(ms, &summary_rows)?
        }
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
    let mut updates = BTreeMap::<usize, ArrayD<bool>>::new();
    let mut touched_rows = std::collections::BTreeSet::<usize>::new();
    let mut changed = ChangeSet::default();
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    let table = ms.main_table();
    let mut row_changes = Vec::<(usize, usize)>::new();
    for rows in selected_rows.chunks(CLIP_SCAN_CHUNK_ROWS) {
        let data_cells = table
            .column_accessor(request.data_column.name())
            .and_then(|column| column.array_cells_owned(rows))
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "read selected {} rows: {source}",
                    request.data_column.name()
                ),
            })?;
        let flag_cells = table
            .column_accessor(FLAG_COLUMN)
            .and_then(|column| column.array_cells_owned(rows))
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read selected FLAG rows: {source}"),
            })?;
        let ddids = if channel_selections.is_some() {
            Some(load_i32_cells(table, rows, "DATA_DESC_ID", &path)?)
        } else {
            None
        };
        for (row_slot, &row) in rows.iter().enumerate() {
            let flags = match flag_cells.get(row_slot).and_then(Option::as_ref) {
                Some(ArrayValue::Bool(values)) => values,
                Some(other) => {
                    return Err(FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("FLAG row {row} must be bool array, found {other:?}"),
                    });
                }
                None => {
                    return Err(FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("FLAG row {row} is undefined"),
                    });
                }
            };
            let data_value = data_cells
                .get(row_slot)
                .and_then(Option::as_ref)
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("{} row {row} is undefined", request.data_column.name()),
                })?;
            let data_shape = numeric_array_value_shape(data_value).ok_or_else(|| {
                FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!(
                        "{} row {row} must be numeric array, found {data_value:?}",
                        request.data_column.name()
                    ),
                }
            })?;
            if data_shape != flags.shape() {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!(
                        "{} and FLAG shapes differ on row {row}: {:?} vs {:?}",
                        request.data_column.name(),
                        data_shape,
                        flags.shape()
                    ),
                });
            }
            if data_shape.len() != 2 {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("{} row {row} is not rank-2", request.data_column.name()),
                });
            }
            total_samples += flags.len();
            flagged_samples += flags.iter().filter(|flag| **flag).count();
            let channels = if let Some(ddids) = ddids.as_ref() {
                let ddid = ddids[row_slot];
                let spw =
                    ddid_to_spw
                        .get(&ddid)
                        .copied()
                        .ok_or_else(|| FlaggingError::MutateFlags {
                            path: path.clone(),
                            reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                        })?;
                Some(
                    match channel_selections.and_then(|selectors| selectors.get(&spw)) {
                        Some(selection) => selection.indices(data_shape[1]).map_err(|source| {
                            FlaggingError::MutateFlags {
                                path: path.clone(),
                                reason: format!("resolve SPW {spw} channels: {source}"),
                            }
                        })?,
                        None => (0..data_shape[1]).collect(),
                    },
                )
            } else {
                None
            };
            row_changes.clear();
            apply_clip_zero_array(
                data_value,
                flags,
                channels.as_deref(),
                &path,
                row,
                &mut row_changes,
            )?;
            let row_changed = row_changes.len();
            if row_changed > 0 {
                let mut updated_flags = flags.clone();
                {
                    let mut updated_flags = updated_flags
                        .view_mut()
                        .into_dimensionality::<Ix2>()
                        .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("FLAG row {row} must be rank-2: {source}"),
                    })?;
                    for &(corr, chan) in &row_changes {
                        updated_flags[(corr, chan)] = true;
                    }
                }
                changed.changed_samples += row_changed;
                flagged_samples += row_changed;
                updates.insert(row, updated_flags);
                touched_rows.insert(row);
            }
        }
    }
    write_touched_flag_updates(ms, updates, touched_rows, &mut changed)?;
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
    let started = Instant::now();
    let loaded = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let samples = loaded.samples;
    let loaded_ms = started.elapsed().as_millis();
    let mut modified = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let groups = group_samples(&samples);
    for group in groups.values() {
        for by_corr in samples_by_corr(group) {
            tfcrop_fit_base_and_flag(
                &by_corr,
                TfcropDirection::Freq,
                TfcropFit::Poly,
                request.freqcutoff,
                &mut modified,
                &mut freq_flags,
            );
            tfcrop_fit_base_and_flag(
                &by_corr,
                TfcropDirection::Time,
                TfcropFit::Line,
                request.timecutoff,
                &mut modified,
                &mut time_flags,
            );
        }
    }
    if request.extendflags {
        extend_autoflag_grouped(&groups, &samples, &mut modified);
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(json!({
        "mode": "tfcrop",
        "implementation": "casa-rs-native-current",
        "selected_rows": selected_rows.len(),
        "samples": samples.len(),
        "groups": groups.len(),
        "freqcutoff": request.freqcutoff,
        "timecutoff": request.timecutoff,
        "freq_candidate_samples": count_mask(&freq_flags),
        "time_candidate_samples": count_mask(&time_flags),
        "union_candidate_samples": union_candidates,
        "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
        "timing_ms": {
            "load": loaded_ms,
            "plan": planned_ms.saturating_sub(loaded_ms),
        },
    }));
    let mut changes = apply_flag_mask_with_preloaded(&samples, &modified, loaded.flags_by_row, ms)?;
    let applied_ms = started.elapsed().as_millis();
    changes.summary = Some(summary_after_flag_only(&samples, changes.changed_samples));
    trace_flagdata(json!({
        "mode": "tfcrop",
        "implementation": "casa-rs-casa-shaped-tfcrop",
        "timing_ms": {
            "apply": applied_ms.saturating_sub(planned_ms),
            "total_before_save": applied_ms,
        },
    }));
    Ok(changes)
}

fn apply_rflag_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let started = Instant::now();
    let loaded = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let samples = loaded.samples;
    let loaded_ms = started.elapsed().as_millis();
    let groups = group_samples(&samples);
    let thresholds = rflag_threshold_maps_for_groups(&groups, request);
    let threshold_ms = started.elapsed().as_millis();
    let mut time_flags = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    for group in groups.values() {
        rflag_group_flags(
            group,
            &thresholds,
            request,
            &mut time_flags,
            &mut freq_flags,
        );
    }
    let mut modified = union_masks(&time_flags, &freq_flags);
    if request.extendflags {
        extend_autoflag_grouped(&groups, &samples, &mut modified);
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(json!({
        "mode": "rflag",
        "implementation": "casa-rs-casa-shaped-rflag",
        "selected_rows": selected_rows.len(),
        "samples": samples.len(),
        "groups": groups.len(),
        "timedev_by_field_spw": format_threshold_map(&thresholds.timedev),
        "freqdev_by_field_spw": format_threshold_map(&thresholds.freqdev),
        "time_candidate_samples": count_mask(&time_flags),
        "freq_candidate_samples": count_mask(&freq_flags),
        "union_candidate_samples": union_candidates,
        "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
        "timing_ms": {
            "load": loaded_ms,
            "threshold": threshold_ms.saturating_sub(loaded_ms),
            "plan": planned_ms.saturating_sub(threshold_ms),
        },
    }));
    let mut changes = apply_flag_mask_with_preloaded(&samples, &modified, loaded.flags_by_row, ms)?;
    let applied_ms = started.elapsed().as_millis();
    changes.summary = Some(summary_after_flag_only(&samples, changes.changed_samples));
    changes.thresholds = Some(thresholds.representative_pair());
    trace_flagdata(json!({
        "mode": "rflag",
        "implementation": "casa-rs-casa-shaped-rflag",
        "timing_ms": {
            "apply": applied_ms.saturating_sub(planned_ms),
            "total_before_save": applied_ms,
        },
    }));
    Ok(changes)
}

fn apply_extend_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let loaded = load_samples(ms, selected_rows, channel_selections, request.data_column)?;
    let samples = loaded.samples;
    let mut to_flag = FlagSampleSet::new();
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
    apply_flag_set_with_preloaded(ms, &to_flag, loaded.flags_by_row)
}

fn extend_autoflag_grouped(
    groups: &BTreeMap<GroupKey, Vec<Sample>>,
    samples: &[Sample],
    modified: &mut [bool],
) {
    let mut grown = modified.to_vec();
    for group in groups.values() {
        for by_corr in samples_by_corr(group) {
            for by_row in sample_row_runs(&by_corr) {
                if percent_modified(by_row, modified) >= 80.0 {
                    for sample in by_row {
                        grown[sample.index] = true;
                    }
                }
            }
            for by_chan in samples_by_chan_dense(&by_corr) {
                if percent_modified(&by_chan, modified) >= 50.0 {
                    for sample in &by_chan {
                        grown[sample.index] = true;
                    }
                }
            }
        }
    }
    modified.copy_from_slice(&grown);
    for group in samples_by_row_chan(samples).values() {
        if group
            .iter()
            .any(|sample| sample.flag || modified[sample.index])
        {
            for sample in group {
                modified[sample.index] = true;
            }
        }
    }
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
    let loaded = load_samples(ms, selected_rows, channel_selections, FlagDataColumn::Data)?;
    let samples = loaded.samples;
    let mut updates = loaded.flags_by_row;
    let mut changed = ChangeSet::default();
    for sample in samples {
        let Some(new_flag) = edit(sample.amp, sample.flag) else {
            continue;
        };
        let Some(flags) = updates.get_mut(&sample.row) else {
            continue;
        };
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

fn apply_flag_set_with_preloaded(
    ms: &mut MeasurementSet,
    to_flag: &FlagSampleSet,
    mut updates: BTreeMap<usize, ArrayD<bool>>,
) -> Result<ChangeSet, FlaggingError> {
    let mut changed = ChangeSet::default();
    let mut touched_rows = std::collections::BTreeSet::<usize>::new();
    for &(row, corr, chan) in to_flag {
        let Some(flags) = updates.get_mut(&row) else {
            continue;
        };
        if let Some(value) = flags.get_mut(IxDyn(&[corr, chan])) {
            if !*value {
                *value = true;
                changed.changed_samples += 1;
                touched_rows.insert(row);
            }
        }
    }
    write_touched_flag_updates(ms, updates, touched_rows, &mut changed)?;
    Ok(changed)
}

fn apply_flag_mask_with_preloaded(
    samples: &[Sample],
    mask: &[bool],
    mut updates: BTreeMap<usize, ArrayD<bool>>,
    ms: &mut MeasurementSet,
) -> Result<ChangeSet, FlaggingError> {
    let mut changed = ChangeSet::default();
    let mut touched_rows = std::collections::BTreeSet::<usize>::new();
    for (sample, flag) in samples.iter().zip(mask.iter()) {
        if !*flag {
            continue;
        }
        let Some(flags) = updates.get_mut(&sample.row) else {
            continue;
        };
        if let Some(value) = flags.get_mut(IxDyn(&[sample.corr, sample.chan])) {
            if !*value {
                *value = true;
                changed.changed_samples += 1;
                touched_rows.insert(sample.row);
            }
        }
    }
    write_touched_flag_updates(ms, updates, touched_rows, &mut changed)?;
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
    changed.changed_flag_row_rows += 1;
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

fn write_touched_flag_updates(
    ms: &mut MeasurementSet,
    mut updates: std::collections::BTreeMap<usize, ArrayD<bool>>,
    touched_rows: std::collections::BTreeSet<usize>,
    changed: &mut ChangeSet,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    let mut prepared = Vec::with_capacity(touched_rows.len());
    for row in touched_rows {
        let Some(flags) = updates.remove(&row) else {
            continue;
        };
        let flag_row = flags.iter().all(|flag| *flag);
        prepared.push((row, flags, flag_row));
    }
    let current_flag_rows = load_bool_cells(
        ms.main_table(),
        &prepared.iter().map(|(row, _, _)| *row).collect::<Vec<_>>(),
        FLAG_ROW_COLUMN,
        &path,
    )?;
    ms.main_table_mut()
        .reserve_array_cell_updates(FLAG_COLUMN, prepared.len());
    {
        let mut flag_column = ms
            .main_table_mut()
            .column_accessor_mut(FLAG_COLUMN)
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("open FLAG column: {source}"),
            })?;
        for (row, flags, _) in &prepared {
            flag_column
                .set_array_assuming_valid(*row, ArrayValue::Bool(flags.clone()))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG row {row}: {source}"),
                })?;
        }
    }
    let flag_row_updates = prepared
        .iter()
        .zip(current_flag_rows.iter())
        .filter_map(|((row, _, flag_row), current)| {
            (*current != *flag_row).then_some((*row, *flag_row))
        })
        .collect::<Vec<_>>();
    if !flag_row_updates.is_empty() {
        let mut flag_row_column = ms
            .main_table_mut()
            .column_accessor_mut(FLAG_ROW_COLUMN)
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("open FLAG_ROW column: {source}"),
            })?;
        for (row, flag_row) in &flag_row_updates {
            flag_row_column
                .set_scalar_assuming_valid(*row, ScalarValue::Bool(*flag_row))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG_ROW row {row}: {source}"),
                })?;
        }
    }
    changed.changed_flag_row_rows += flag_row_updates.len();
    for (row, _, _) in prepared {
        changed.changed_rows += 1;
        changed.changed_row_indices.push(row);
    }
    Ok(())
}

fn load_bool_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<bool>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Bool(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Bool scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

fn load_i32_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<i32>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Int32(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Int32 scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

fn load_samples(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    data_column: FlagDataColumn,
) -> Result<LoadedSamples, FlaggingError> {
    let path = display_ms_path(ms);
    let ddid_to_spw = data_description_spw_map(ms)?;
    let table = ms.main_table();
    let data_cells = table
        .column_accessor(data_column.name())
        .and_then(|column| column.array_cells_owned(selected_rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("read selected {} rows: {source}", data_column.name()),
        })?;
    let flag_cells = table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| column.array_cells_owned(selected_rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("read selected FLAG rows: {source}"),
        })?;
    let ddids = load_i32_cells(table, selected_rows, "DATA_DESC_ID", &path)?;
    let fields = load_i32_cells(table, selected_rows, "FIELD_ID", &path)?;
    let scans = load_i32_cells(table, selected_rows, "SCAN_NUMBER", &path)?;
    let ant1s = load_i32_cells(table, selected_rows, "ANTENNA1", &path)?;
    let ant2s = load_i32_cells(table, selected_rows, "ANTENNA2", &path)?;
    let mut samples = Vec::new();
    let mut flags_by_row = BTreeMap::new();
    for (row_slot, &row) in selected_rows.iter().enumerate() {
        let ddid = ddids[row_slot];
        let spw = ddid_to_spw
            .get(&ddid)
            .copied()
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
            })?;
        let flags = match flag_cells.get(row_slot).and_then(Option::as_ref) {
            Some(ArrayValue::Bool(values)) => values.clone(),
            Some(other) => {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("FLAG row {row} must be bool array, found {other:?}"),
                });
            }
            None => {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("FLAG row {row} is undefined"),
                });
            }
        };
        let data_value = data_cells
            .get(row_slot)
            .and_then(Option::as_ref)
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("{} row {row} is undefined", data_column.name()),
            })?;
        let data_shape =
            numeric_array_value_shape(data_value).ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "{} row {row} must be numeric array, found {data_value:?}",
                    data_column.name()
                ),
            })?;
        if data_shape != flags.shape() {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!(
                    "{} and FLAG shapes differ on row {row}: {:?} vs {:?}",
                    data_column.name(),
                    data_shape,
                    flags.shape()
                ),
            });
        }
        if data_shape.len() != 2 {
            return Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("{} row {row} is not rank-2", data_column.name()),
            });
        }
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
        let field = fields[row_slot];
        let scan = scans[row_slot];
        let ant1 = ant1s[row_slot];
        let ant2 = ant2s[row_slot];
        push_row_samples(
            data_value,
            &flags,
            &channels,
            RowSampleMetadata {
                row,
                field,
                spw,
                scan,
                ant1,
                ant2,
            },
            &path,
            data_column.name(),
            &mut samples,
        )?;
        flags_by_row.insert(row, flags);
    }
    Ok(LoadedSamples {
        samples,
        flags_by_row,
    })
}

fn is_casa_clip_zero(value: f64) -> bool {
    value.is_nan() || value <= f64::from(f32::EPSILON)
}

fn is_casa_clip_zero_complex32(value: Complex32) -> bool {
    value.re.is_nan()
        || value.im.is_nan()
        || f64::from(value.re * value.re + value.im * value.im)
            <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON)
}

fn is_casa_clip_zero_complex64(value: Complex64) -> bool {
    value.re.is_nan()
        || value.im.is_nan()
        || value.re * value.re + value.im * value.im
            <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON)
}

fn numeric_array_value_shape(value: &ArrayValue) -> Option<&[usize]> {
    match value {
        ArrayValue::Complex32(values) => Some(values.shape()),
        ArrayValue::Complex64(values) => Some(values.shape()),
        ArrayValue::Float32(values) => Some(values.shape()),
        ArrayValue::Float64(values) => Some(values.shape()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct RowSampleMetadata {
    row: usize,
    field: i32,
    spw: i32,
    scan: i32,
    ant1: i32,
    ant2: i32,
}

fn push_row_samples(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    channels: &[usize],
    metadata: RowSampleMetadata,
    path: &str,
    data_column: &str,
    samples: &mut Vec<Sample>,
) -> Result<(), FlaggingError> {
    let flag_view = flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("FLAG row {} must be rank-2: {source}", metadata.row),
        })?;
    match data {
        ArrayValue::Complex32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(value.re),
                        f64::from(value.im),
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Complex64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        value.re,
                        value.im,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(values[(corr, chan)]),
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        values[(corr, chan)],
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn push_sample(
    samples: &mut Vec<Sample>,
    metadata: RowSampleMetadata,
    corr: usize,
    chan: usize,
    real: f64,
    imag: f64,
    flag: bool,
) {
    samples.push(Sample {
        index: samples.len(),
        row: metadata.row,
        corr,
        chan,
        amp: real.hypot(imag),
        real,
        imag,
        flag,
        field: metadata.field,
        spw: metadata.spw,
        scan: metadata.scan,
        ant1: metadata.ant1,
        ant2: metadata.ant2,
    });
}

fn apply_clip_zero_array(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    channels: Option<&[usize]>,
    path: &str,
    row: usize,
    changed: &mut Vec<(usize, usize)>,
) -> Result<(), FlaggingError> {
    let flags = flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("FLAG row {row} must be rank-2: {source}"),
        })?;
    match data {
        ArrayValue::Complex32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("DATA row {row} must be rank-2: {source}"),
                })?;
            for corr in 0..values.shape()[0] {
                if let Some(channels) = channels {
                    for &chan in channels {
                        if !flags[(corr, chan)] && is_casa_clip_zero_complex32(values[(corr, chan)])
                        {
                            changed.push((corr, chan));
                        }
                    }
                } else {
                    for chan in 0..values.shape()[1] {
                        if !flags[(corr, chan)] && is_casa_clip_zero_complex32(values[(corr, chan)])
                        {
                            changed.push((corr, chan));
                        }
                    }
                }
            }
        }
        ArrayValue::Complex64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("DATA row {row} must be rank-2: {source}"),
                })?;
            for corr in 0..values.shape()[0] {
                if let Some(channels) = channels {
                    for &chan in channels {
                        if !flags[(corr, chan)] && is_casa_clip_zero_complex64(values[(corr, chan)])
                        {
                            changed.push((corr, chan));
                        }
                    }
                } else {
                    for chan in 0..values.shape()[1] {
                        if !flags[(corr, chan)] && is_casa_clip_zero_complex64(values[(corr, chan)])
                        {
                            changed.push((corr, chan));
                        }
                    }
                }
            }
        }
        ArrayValue::Float32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("DATA row {row} must be rank-2: {source}"),
                })?;
            for corr in 0..values.shape()[0] {
                if let Some(channels) = channels {
                    for &chan in channels {
                        if !flags[(corr, chan)]
                            && is_casa_clip_zero(f64::from(values[(corr, chan)]))
                        {
                            changed.push((corr, chan));
                        }
                    }
                } else {
                    for chan in 0..values.shape()[1] {
                        if !flags[(corr, chan)]
                            && is_casa_clip_zero(f64::from(values[(corr, chan)]))
                        {
                            changed.push((corr, chan));
                        }
                    }
                }
            }
        }
        ArrayValue::Float64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("DATA row {row} must be rank-2: {source}"),
                })?;
            for corr in 0..values.shape()[0] {
                if let Some(channels) = channels {
                    for &chan in channels {
                        if !flags[(corr, chan)] && is_casa_clip_zero(values[(corr, chan)]) {
                            changed.push((corr, chan));
                        }
                    }
                } else {
                    for chan in 0..values.shape()[1] {
                        if !flags[(corr, chan)] && is_casa_clip_zero(values[(corr, chan)]) {
                            changed.push((corr, chan));
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
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

fn rflag_threshold_maps_for_groups(
    groups: &BTreeMap<GroupKey, Vec<Sample>>,
    request: &FlagDataRequest,
) -> RFlagThresholds {
    let mut histograms = RFlagHistograms::default();
    for group in groups.values() {
        rflag_accumulate_group_histograms(group, &mut histograms);
    }
    histograms.into_thresholds(request)
}

#[derive(Debug, Clone, Copy)]
enum TfcropDirection {
    Freq,
    Time,
}

#[derive(Debug, Clone, Copy)]
enum TfcropFit {
    Line,
    Poly,
}

fn tfcrop_fit_base_and_flag(
    samples: &[Sample],
    direction: TfcropDirection,
    fit: TfcropFit,
    cutoff: f64,
    modified: &mut [bool],
    direction_flags: &mut [bool],
) {
    if samples.is_empty() {
        return;
    }
    let rows = unique_sample_rows(samples);
    let chans = unique_sample_chans(samples);
    let (axis0, axis1) = match direction {
        TfcropDirection::Freq => (chans, rows),
        TfcropDirection::Time => (rows, chans),
    };
    if axis0.is_empty() || axis1.is_empty() {
        return;
    }

    let mut by_row_chan = HashMap::<(usize, usize), Sample>::with_capacity(samples.len());
    for sample in samples {
        by_row_chan.insert((sample.row, sample.chan), *sample);
    }

    let mut avg_dat = vec![0.0f32; axis0.len()];
    let mut avg_flag = vec![false; axis0.len()];
    let mut all_zeros = true;
    for (i0, key0) in axis0.iter().enumerate() {
        let mut sum = 0.0f32;
        let mut count = 0usize;
        for key1 in &axis1 {
            let (row, chan) = tfcrop_row_chan(direction, *key0, *key1);
            if let Some(sample) = by_row_chan.get(&(row, chan)) {
                if !sample_is_modified(sample, modified) {
                    sum += tfcrop_value(sample);
                    count += 1;
                }
            }
        }
        if count > 0 {
            avg_dat[i0] = sum / count as f32;
            all_zeros = false;
        } else {
            avg_flag[i0] = true;
        }
    }
    if all_zeros {
        return;
    }

    let avg_fit = match fit {
        TfcropFit::Line => tfcrop_fit_piecewise_poly(&avg_dat, &mut avg_flag, 1, 1),
        TfcropFit::Poly => tfcrop_fit_piecewise_poly(&avg_dat, &mut avg_flag, 7, 4),
    };

    for key1 in &axis1 {
        let mut normalized = vec![0.0f32; axis0.len()];
        let mut flags = vec![false; axis0.len()];
        for (i0, key0) in axis0.iter().enumerate() {
            let (row, chan) = tfcrop_row_chan(direction, *key0, *key1);
            if let Some(sample) = by_row_chan.get(&(row, chan)) {
                flags[i0] = sample_is_modified(sample, modified);
                if !flags[i0] && avg_fit[i0].abs() > f32::EPSILON {
                    normalized[i0] = tfcrop_value(sample) / avg_fit[i0];
                }
            } else {
                flags[i0] = true;
            }
        }

        let mut previous_sd = 0.0f32;
        for _ in 0..5 {
            let sd = tfcrop_std_about_mean(&normalized, &flags, 1.0);
            if !sd.is_finite() || sd <= f32::EPSILON {
                break;
            }
            for i0 in 0..axis0.len() {
                if !flags[i0] && (normalized[i0] - 1.0).abs() > cutoff as f32 * sd {
                    flags[i0] = true;
                }
            }
            if (previous_sd - sd).abs() < 0.1 {
                break;
            }
            previous_sd = sd;
        }

        for (i0, key0) in axis0.iter().enumerate() {
            if !flags[i0] {
                continue;
            }
            let (row, chan) = tfcrop_row_chan(direction, *key0, *key1);
            if let Some(sample) = by_row_chan.get(&(row, chan)) {
                if !modified[sample.index] {
                    modified[sample.index] = true;
                    direction_flags[sample.index] = true;
                }
            }
        }
    }
}

fn tfcrop_row_chan(direction: TfcropDirection, axis0: usize, axis1: usize) -> (usize, usize) {
    match direction {
        TfcropDirection::Freq => (axis1, axis0),
        TfcropDirection::Time => (axis0, axis1),
    }
}

fn tfcrop_value(sample: &Sample) -> f32 {
    sample.amp as f32
}

fn sample_is_modified(sample: &Sample, modified: &[bool]) -> bool {
    sample.flag || modified[sample.index]
}

fn tfcrop_fit_piecewise_poly(
    data: &[f32],
    flag: &mut [bool],
    max_pieces: usize,
    max_degree: usize,
) -> Vec<f32> {
    let mut tdata = data.to_vec();
    for i in 0..tdata.len() {
        if tdata[i] != 0.0 {
            continue;
        }
        if i == 0 {
            if let Some((index, _)) = tdata
                .iter()
                .enumerate()
                .skip(1)
                .find(|(_, value)| **value != 0.0)
            {
                tdata[i] = tdata[index];
            }
        } else {
            let left = (0..=i).rev().find(|index| tdata[*index] != 0.0);
            let right = (i + 1..tdata.len()).find(|index| tdata[*index] != 0.0);
            match (left, right) {
                (Some(left), Some(right)) => tdata[i] = (tdata[left] + tdata[right]) / 2.0,
                (Some(left), None) => tdata[i] = tdata[left],
                (None, Some(right)) => tdata[i] = tdata[right],
                (None, None) => {}
            }
        }
    }
    for (index, value) in tdata.iter().enumerate() {
        if *value == 0.0 {
            flag[index] = true;
        }
    }

    let mut fitted = tdata.clone();
    for iteration in 0..5usize {
        let npieces = (2 * iteration + 1).min(max_pieces);
        let mut degree = 1usize;
        if iteration > 1 {
            degree = 2;
        }
        if iteration > 2 {
            degree = 3;
        }
        degree = degree.min(max_degree);
        let piece_size = tdata.len() / npieces;
        let leftover = tdata.len() % npieces;
        let leftover_front = leftover / 2;
        for piece in 0..npieces {
            let (left, right) = if npieces > 1 {
                let mut left = leftover_front + piece * piece_size;
                let mut right = left + piece_size;
                if piece == 0 {
                    left = 0;
                    right = leftover_front + piece_size;
                }
                if piece == npieces - 1 {
                    right = tdata.len() - 1;
                }
                (left, right)
            } else {
                (0, tdata.len() - 1)
            };
            if degree == 1 {
                tfcrop_line_fit(&tdata, flag, &mut fitted, left, right);
            } else {
                tfcrop_poly_fit(&tdata, flag, &mut fitted, left, right, degree);
            }
        }

        for i in 2..tdata.len().saturating_sub(2) {
            let win_start = i - 2;
            let win_end = (i + 2).min(tdata.len() - 1);
            if win_end <= win_start {
                break;
            }
            let sum = (win_start..=win_end)
                .map(|index| fitted[index])
                .sum::<f32>();
            fitted[i] = sum / (win_end - win_start + 1) as f32;
        }

        let sd = tfcrop_std_about_fit(&tdata, flag, &fitted);
        if !sd.is_finite() {
            continue;
        }
        let tolerance = if iteration >= 2 { 2.0 } else { 3.0 };
        for i in 0..tdata.len() {
            if tdata[i] - fitted[i] > tolerance * sd {
                flag[i] = true;
            }
        }
    }
    fitted
}

fn tfcrop_line_fit(data: &[f32], flag: &[bool], fit: &mut [f32], left: usize, right: usize) {
    let mean = tfcrop_mean(data, flag);
    let sd = tfcrop_std_about_mean(data, flag, mean);
    if !sd.is_finite() || sd <= f32::EPSILON {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let mut sum_w = 0.0f32;
    let mut sum_x = 0.0f32;
    let mut sum_y = 0.0f32;
    let mut sum_xx = 0.0f32;
    let mut sum_xy = 0.0f32;
    let weight = 1.0f32 / (sd * sd);
    for i in left..=right {
        if !flag[i] {
            let x = i as f32;
            sum_w += weight;
            sum_x += x * weight;
            sum_y += data[i] * weight;
            sum_xx += x * x * weight;
            sum_xy += x * data[i] * weight;
        }
    }
    let denom = sum_w * sum_xx - sum_x * sum_x;
    if denom.abs() <= f32::EPSILON {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let intercept = (sum_xx * sum_y - sum_x * sum_xy) / denom;
    let slope = (sum_w * sum_xy - sum_x * sum_y) / denom;
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        *fitted = intercept + slope * i as f32;
    }
}

fn tfcrop_poly_fit(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
    degree: usize,
) {
    let rows = (left..=right)
        .filter(|index| !flag[*index])
        .map(|index| {
            let x = index as f64 + 1.0;
            let basis = (0..=degree)
                .map(|power| x.powi(power as i32))
                .collect::<Vec<_>>();
            (basis, f64::from(data[index]), 1.0)
        })
        .collect::<Vec<_>>();
    if rows.len() <= degree {
        tfcrop_line_fit(data, flag, fit, left, right);
        return;
    }
    let Some(solution) = solve_weighted_least_squares(&rows, degree + 1) else {
        tfcrop_line_fit(data, flag, fit, left, right);
        return;
    };
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        let x = i as f64 + 1.0;
        *fitted = solution
            .iter()
            .enumerate()
            .map(|(power, coefficient)| coefficient * x.powi(power as i32))
            .sum::<f64>() as f32;
    }
}

fn tfcrop_mean(values: &[f32], flags: &[bool]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            sum += value;
            count += 1;
        }
    }
    if count == 0 { 0.0 } else { sum / count as f32 }
}

fn tfcrop_std_about_fit(values: &[f32], flags: &[bool], fit: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for ((value, flag), fitted) in values.iter().zip(flags.iter()).zip(fit.iter()) {
        if !*flag {
            count += 1;
            sum += (value - fitted) * (value - fitted);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

fn tfcrop_std_about_mean(values: &[f32], flags: &[bool], mean: f32) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            count += 1;
            sum += (value - mean) * (value - mean);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    median_in_place(&mut values)
}

fn median_in_place(values: &mut [f64]) -> f64 {
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

#[derive(Debug, Default)]
struct RFlagHistograms {
    time: BTreeMap<FieldSpwKey, ChannelHistogram>,
    freq: BTreeMap<FieldSpwKey, ChannelHistogram>,
}

impl RFlagHistograms {
    fn into_thresholds(self, request: &FlagDataRequest) -> RFlagThresholds {
        let mut timedev = BTreeMap::new();
        let mut freqdev = BTreeMap::new();
        for (key, histogram) in self.time {
            timedev.insert(
                key,
                request
                    .timedev
                    .unwrap_or_else(|| histogram.threshold() * request.timedevscale),
            );
        }
        for (key, histogram) in self.freq {
            freqdev.insert(
                key,
                request
                    .freqdev
                    .unwrap_or_else(|| histogram.threshold() * request.freqdevscale),
            );
        }
        RFlagThresholds { timedev, freqdev }
    }
}

#[derive(Debug, Default)]
struct RFlagThresholds {
    timedev: BTreeMap<FieldSpwKey, f64>,
    freqdev: BTreeMap<FieldSpwKey, f64>,
}

impl RFlagThresholds {
    fn representative_pair(&self) -> (f64, f64) {
        (
            representative_threshold(&self.timedev),
            representative_threshold(&self.freqdev),
        )
    }
}

#[derive(Debug, Clone)]
struct ChannelHistogram {
    sum: Vec<f64>,
    counts: Vec<f64>,
}

impl ChannelHistogram {
    fn new(nchan: usize) -> Self {
        Self {
            sum: vec![0.0; nchan],
            counts: vec![0.0; nchan],
        }
    }

    fn add(&mut self, chan: usize, value: f64) {
        if let (Some(sum), Some(count)) = (self.sum.get_mut(chan), self.counts.get_mut(chan)) {
            *sum += value;
            *count += 1.0;
        }
    }

    fn threshold(&self) -> f64 {
        let samples = self
            .sum
            .iter()
            .zip(self.counts.iter())
            .map(|(sum, count)| if *count > 0.0 { *sum / *count } else { 0.0 })
            .collect::<Vec<_>>();
        let med = median(samples.clone());
        let mad = median(samples.iter().map(|value| (*value - med).abs()).collect());
        med + 1.4826 * mad
    }
}

fn representative_threshold(thresholds: &BTreeMap<FieldSpwKey, f64>) -> f64 {
    if thresholds.is_empty() {
        return f64::INFINITY;
    }
    median(thresholds.values().copied().collect())
}

fn rflag_accumulate_group_histograms(group: &[Sample], histograms: &mut RFlagHistograms) {
    let Some(first) = group.first() else {
        return;
    };
    let key = (first.field, first.spw);
    let nchan = group.iter().map(|sample| sample.chan).max().unwrap_or(0) + 1;
    let time_hist = histograms
        .time
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    let rows_by_corr = samples_by_corr(group);
    for by_corr in &rows_by_corr {
        for by_chan in samples_by_chan_dense(by_corr) {
            for (start, stop) in rflag_time_windows(by_chan.len()) {
                let window = &by_chan[start..=stop];
                let value = rflag_time_std_total(window);
                if let Some(sample) = by_chan.first() {
                    time_hist.add(sample.chan, value);
                }
            }
        }
    }
    let freq_hist = histograms
        .freq
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    for by_corr in rows_by_corr {
        for by_row in sample_row_runs(&by_corr) {
            let stats = rflag_spectral_stats(by_row);
            for sample in by_row {
                if sample.flag {
                    continue;
                }
                if stats.sum_weight_real > 0.0 {
                    freq_hist.add(sample.chan, (sample.real - stats.average_real).abs());
                }
                if stats.sum_weight_imag > 0.0 {
                    freq_hist.add(sample.chan, (sample.imag - stats.average_imag).abs());
                }
            }
        }
    }
}

fn rflag_group_flags(
    group: &[Sample],
    thresholds: &RFlagThresholds,
    request: &FlagDataRequest,
    time_flags: &mut [bool],
    freq_flags: &mut [bool],
) {
    let Some(first) = group.first() else {
        return;
    };
    let key = (first.field, first.spw);
    let timedev = thresholds
        .timedev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    let freqdev = thresholds
        .freqdev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    for by_corr in samples_by_corr(group) {
        for by_chan in samples_by_chan_dense(&by_corr) {
            for (start, stop) in rflag_time_windows(by_chan.len()) {
                if rflag_time_std_total(&by_chan[start..=stop]) > timedev {
                    for sample in &by_chan[start..=stop] {
                        if !sample.flag {
                            time_flags[sample.index] = true;
                        }
                    }
                }
            }
        }
        for by_row in sample_row_runs(&by_corr) {
            let stats = rflag_spectral_stats(by_row);
            let flag_all = stats.std_real > request.spectralmax
                || stats.std_imag > request.spectralmax
                || stats.std_real < request.spectralmin
                || stats.std_imag < request.spectralmin;
            for sample in by_row {
                if sample.flag {
                    continue;
                }
                if flag_all
                    || (sample.real - stats.average_real).abs() > freqdev
                    || (sample.imag - stats.average_imag).abs() > freqdev
                {
                    freq_flags[sample.index] = true;
                }
            }
        }
    }
}

fn rflag_time_windows(len: usize) -> impl Iterator<Item = (usize, usize)> {
    let effective = len.min(3);
    let delta = effective.saturating_sub(1) / 2;
    (0..len).filter_map(move |center| {
        if delta == 0 {
            Some((center, center))
        } else if center >= delta && center < len - delta {
            Some((center - delta, center + delta))
        } else {
            None
        }
    })
}

fn rflag_time_std_total(samples: &[Sample]) -> f64 {
    let mut count = 0usize;
    let mut sum_real = 0.0;
    let mut sum_imag = 0.0;
    let mut sumsq_real = 0.0;
    let mut sumsq_imag = 0.0;
    for sample in samples {
        if sample.flag {
            continue;
        }
        count += 1;
        sum_real += sample.real;
        sum_imag += sample.imag;
        sumsq_real += sample.real * sample.real;
        sumsq_imag += sample.imag * sample.imag;
    }
    if count == 0 {
        return 0.0;
    }
    let sum_weight = count as f64;
    let avg_real = sum_real / sum_weight;
    let avg_imag = sum_imag / sum_weight;
    let var_real = sumsq_real / sum_weight - avg_real * avg_real;
    let var_imag = sumsq_imag / sum_weight - avg_imag * avg_imag;
    (var_real.max(0.0) + var_imag.max(0.0)).sqrt()
}

#[derive(Debug, Clone, Copy, Default)]
struct RFlagSpectralStats {
    average_real: f64,
    average_imag: f64,
    std_real: f64,
    std_imag: f64,
    sum_weight_real: f64,
    sum_weight_imag: f64,
}

fn rflag_spectral_stats(samples: &[Sample]) -> RFlagSpectralStats {
    let mut real_values = Vec::with_capacity(samples.len());
    let mut imag_values = Vec::with_capacity(samples.len());
    for sample in samples {
        if sample.flag {
            continue;
        }
        real_values.push(sample.real);
        imag_values.push(sample.imag);
    }
    if real_values.is_empty() {
        return RFlagSpectralStats::default();
    }
    let average_real = median_in_place(&mut real_values);
    let average_imag = median_in_place(&mut imag_values);
    for value in &mut real_values {
        *value = (*value - average_real).abs();
    }
    for value in &mut imag_values {
        *value = (*value - average_imag).abs();
    }
    let std_real = 1.4826 * median_in_place(&mut real_values);
    let std_imag = 1.4826 * median_in_place(&mut imag_values);
    let weight = real_values.len() as f64;
    RFlagSpectralStats {
        average_real,
        average_imag,
        std_real,
        std_imag,
        sum_weight_real: weight,
        sum_weight_imag: weight,
    }
}

fn format_threshold_map(thresholds: &BTreeMap<FieldSpwKey, f64>) -> BTreeMap<String, f64> {
    thresholds
        .iter()
        .map(|((field, spw), value)| (format!("field{field}_spw{spw}"), *value))
        .collect()
}

fn percent_flagged(samples: &[Sample]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    100.0 * samples.iter().filter(|sample| sample.flag).count() as f64 / samples.len() as f64
}

fn summary_after_flag_only(samples: &[Sample], changed_samples: usize) -> FlagSummary {
    FlagSummary {
        flagged_samples: samples.iter().filter(|sample| sample.flag).count() + changed_samples,
        total_samples: samples.len(),
    }
}

fn percent_modified(samples: &[Sample], modified: &[bool]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    100.0
        * samples
            .iter()
            .filter(|sample| sample.flag || modified[sample.index])
            .count() as f64
        / samples.len() as f64
}

fn count_mask(mask: &[bool]) -> usize {
    mask.iter().filter(|value| **value).count()
}

fn union_masks(left: &[bool], right: &[bool]) -> Vec<bool> {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| *left || *right)
        .collect()
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

fn flag_mask_counts_by_spw_corr(mask: &[bool], samples: &[Sample]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for (sample, flag) in samples.iter().zip(mask.iter()) {
        if *flag {
            *counts
                .entry(format!("spw{}_corr{}", sample.spw, sample.corr))
                .or_default() += 1;
        }
    }
    counts
}

type GroupKey = (i32, i32, i32, i32, i32);
type FieldSpwKey = (i32, i32);

fn group_samples(samples: &[Sample]) -> BTreeMap<GroupKey, Vec<Sample>> {
    let mut groups = BTreeMap::<GroupKey, Vec<Sample>>::new();
    for sample in samples {
        let ant_low = sample.ant1.min(sample.ant2);
        let ant_high = sample.ant1.max(sample.ant2);
        groups
            .entry((sample.field, sample.spw, sample.scan, ant_low, ant_high))
            .or_default()
            .push(*sample);
    }
    groups
}

fn samples_by_corr(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_corr) = samples.iter().map(|sample| sample.corr).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_corr + 1];
    for sample in samples {
        groups[sample.corr].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

fn samples_by_row(samples: &[Sample]) -> BTreeMap<usize, Vec<Sample>> {
    let mut groups = BTreeMap::<usize, Vec<Sample>>::new();
    for sample in samples {
        groups.entry(sample.row).or_default().push(*sample);
    }
    groups
}

fn samples_by_chan(samples: &[Sample]) -> BTreeMap<usize, Vec<Sample>> {
    let mut groups = BTreeMap::<usize, Vec<Sample>>::new();
    for sample in samples {
        groups.entry(sample.chan).or_default().push(*sample);
    }
    groups
}

fn samples_by_chan_dense(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_chan) = samples.iter().map(|sample| sample.chan).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_chan + 1];
    for sample in samples {
        groups[sample.chan].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

fn sample_row_runs(samples: &[Sample]) -> Vec<&[Sample]> {
    let mut runs = Vec::new();
    let mut start = 0usize;
    while start < samples.len() {
        let row = samples[start].row;
        let mut end = start + 1;
        while end < samples.len() && samples[end].row == row {
            end += 1;
        }
        runs.push(&samples[start..end]);
        start = end;
    }
    runs
}

fn unique_sample_rows(samples: &[Sample]) -> Vec<usize> {
    let mut rows = Vec::new();
    let mut previous = None;
    for sample in samples {
        if previous != Some(sample.row) {
            rows.push(sample.row);
            previous = Some(sample.row);
        }
    }
    rows
}

fn unique_sample_chans(samples: &[Sample]) -> Vec<usize> {
    let mut chans = Vec::new();
    for sample in samples {
        if chans.last().copied() == Some(sample.chan) || chans.contains(&sample.chan) {
            continue;
        }
        chans.push(sample.chan);
    }
    chans
}

fn samples_by_row_chan(samples: &[Sample]) -> BTreeMap<(usize, usize), Vec<Sample>> {
    let mut groups = BTreeMap::<(usize, usize), Vec<Sample>>::new();
    for sample in samples {
        groups
            .entry((sample.row, sample.chan))
            .or_default()
            .push(*sample);
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

#[derive(Debug, Clone, Copy)]
struct Sample {
    index: usize,
    row: usize,
    corr: usize,
    chan: usize,
    amp: f64,
    real: f64,
    imag: f64,
    flag: bool,
    field: i32,
    spw: i32,
    scan: i32,
    ant1: i32,
    ant2: i32,
}

struct LoadedSamples {
    samples: Vec<Sample>,
    flags_by_row: BTreeMap<usize, ArrayD<bool>>,
}

#[derive(Default)]
struct ChangeSet {
    changed_rows: usize,
    changed_samples: usize,
    changed_row_indices: Vec<usize>,
    summary: Option<FlagSummary>,
    thresholds: Option<(f64, f64)>,
    changed_flag_row_rows: usize,
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

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    fn sample(index: usize, row: usize, corr: usize, chan: usize, real: f64, flag: bool) -> Sample {
        Sample {
            index,
            row,
            corr,
            chan,
            amp: real.abs(),
            real,
            imag: 0.0,
            flag,
            field: 0,
            spw: 0,
            scan: 1,
            ant1: 0,
            ant2: 1,
        }
    }

    #[test]
    fn clip_zero_and_sample_loading_helpers_cover_numeric_families() {
        assert!(is_casa_clip_zero(f64::NAN));
        assert!(is_casa_clip_zero(f64::from(f32::EPSILON)));
        assert!(!is_casa_clip_zero(f64::from(f32::EPSILON) * 2.0));
        assert!(is_casa_clip_zero_complex32(Complex32::new(f32::NAN, 0.0)));
        assert!(is_casa_clip_zero_complex64(Complex64::new(0.0, f64::NAN)));
        assert!(!is_casa_clip_zero_complex64(Complex64::new(1.0, 0.0)));

        let flags = ArrayD::from_elem(IxDyn(&[2, 3]), false);
        let mut changed = Vec::new();
        let c64 = ArrayValue::Complex64(
            array![
                [
                    Complex64::new(0.0, 0.0),
                    Complex64::new(2.0, 0.0),
                    Complex64::new(f64::NAN, 0.0)
                ],
                [
                    Complex64::new(3.0, 0.0),
                    Complex64::new(0.0, 0.0),
                    Complex64::new(4.0, 0.0)
                ]
            ]
            .into_dyn(),
        );
        apply_clip_zero_array(&c64, &flags, Some(&[0, 2]), "<test>", 4, &mut changed)
            .expect("clip complex64");
        assert_eq!(changed, vec![(0, 0), (0, 2)]);

        changed.clear();
        let flags_2x2 = ArrayD::from_elem(IxDyn(&[2, 2]), false);
        let f32s = ArrayValue::Float32(array![[0.0_f32, 1.0], [f32::EPSILON, 2.0]].into_dyn());
        apply_clip_zero_array(&f32s, &flags_2x2, None, "<test>", 5, &mut changed)
            .expect("clip float32");
        assert_eq!(changed, vec![(0, 0), (1, 0)]);

        changed.clear();
        let f64s = ArrayValue::Float64(array![[0.0_f64, 5.0], [f64::NAN, 6.0]].into_dyn());
        apply_clip_zero_array(&f64s, &flags_2x2, None, "<test>", 6, &mut changed)
            .expect("clip float64");
        assert_eq!(changed, vec![(0, 0), (1, 0)]);

        let mut samples = Vec::new();
        let metadata = RowSampleMetadata {
            row: 9,
            field: 2,
            spw: 3,
            scan: 4,
            ant1: 5,
            ant2: 6,
        };
        push_row_samples(
            &c64,
            &flags,
            &[1, 2],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push complex64");
        push_row_samples(
            &f32s,
            &flags_2x2,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push float32");
        push_row_samples(
            &f64s,
            &flags_2x2,
            &[1],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push float64");
        assert_eq!(samples.len(), 8);
        assert!(
            samples
                .iter()
                .any(|s| s.field == 2 && s.spw == 3 && s.amp == 5.0)
        );
    }

    #[test]
    fn tfcrop_fitting_helpers_cover_zero_interpolation_and_fallbacks() {
        let data = [1.0, 0.0, 3.0, 4.0, 40.0, 6.0, 7.0, 8.0, 9.0];
        let mut flags = vec![false; data.len()];
        let fit = tfcrop_fit_piecewise_poly(&data, &mut flags, 7, 4);
        assert_eq!(fit.len(), data.len());
        assert!(fit.iter().all(|value| value.is_finite()));
        assert!(flags.iter().any(|flag| *flag));

        let mut all_flagged_fit = vec![0.0; 3];
        tfcrop_line_fit(
            &[2.0, 2.0, 2.0],
            &[true, true, true],
            &mut all_flagged_fit,
            0,
            2,
        );
        assert_eq!(all_flagged_fit, vec![0.0, 0.0, 0.0]);

        let mut fallback_fit = vec![0.0; 3];
        tfcrop_poly_fit(
            &[1.0, 2.0, 3.0],
            &[false, true, true],
            &mut fallback_fit,
            0,
            2,
            3,
        );
        assert_eq!(fallback_fit, vec![1.0, 1.0, 1.0]);
        assert_eq!(tfcrop_mean(&[1.0, 3.0], &[false, false]), 2.0);
        assert_eq!(
            tfcrop_std_about_mean(&[1.0, 3.0], &[false, false], 2.0),
            1.0
        );
        assert_eq!(
            tfcrop_std_about_fit(&[1.0, 3.0], &[false, false], &[1.0, 1.0]),
            2.0_f32.sqrt()
        );
        assert_eq!(median(Vec::new()), 0.0);
        assert_eq!(median(vec![1.0, 4.0, 2.0, 3.0]), 2.5);
    }

    #[test]
    fn rflag_thresholds_and_group_flags_cover_time_and_spectral_paths() {
        let samples = vec![
            sample(0, 0, 0, 0, 1.0, false),
            sample(1, 0, 0, 1, 1.0, false),
            sample(2, 1, 0, 0, 1.0, false),
            sample(3, 1, 0, 1, 1.0, false),
            sample(4, 2, 0, 0, 10.0, false),
            sample(5, 2, 0, 1, 1.0, false),
            sample(6, 0, 1, 0, 2.0, true),
            sample(7, 0, 1, 1, 2.0, true),
        ];
        let groups = group_samples(&samples);
        let request = FlagDataRequest {
            timedev: Some(1.0),
            freqdev: Some(2.0),
            spectralmax: 0.1,
            spectralmin: 0.0,
            ..FlagDataRequest::default()
        };
        let thresholds = rflag_threshold_maps_for_groups(&groups, &request);
        assert_eq!(thresholds.representative_pair(), (1.0, 2.0));
        assert_eq!(
            format_threshold_map(&thresholds.timedev).get("field0_spw0"),
            Some(&1.0)
        );

        let mut time_flags = vec![false; samples.len()];
        let mut freq_flags = vec![false; samples.len()];
        for group in groups.values() {
            rflag_group_flags(
                group,
                &thresholds,
                &request,
                &mut time_flags,
                &mut freq_flags,
            );
        }
        assert!(time_flags[0] || time_flags[2] || time_flags[4]);
        assert!(freq_flags[4] || freq_flags[5]);
        assert!(!freq_flags[6]);

        assert_eq!(
            rflag_time_windows(0).collect::<Vec<_>>(),
            Vec::<(usize, usize)>::new()
        );
        assert_eq!(rflag_time_windows(1).collect::<Vec<_>>(), vec![(0, 0)]);
        assert_eq!(
            rflag_time_windows(4).collect::<Vec<_>>(),
            vec![(0, 2), (1, 3)]
        );
        assert_eq!(rflag_time_std_total(&samples[6..8]), 0.0);
        assert_eq!(rflag_spectral_stats(&samples[6..8]).sum_weight_real, 0.0);

        let mut histogram = ChannelHistogram::new(2);
        histogram.add(0, 1.0);
        histogram.add(0, 3.0);
        histogram.add(1, 9.0);
        assert!(histogram.threshold().is_finite());
        assert_eq!(representative_threshold(&BTreeMap::new()), f64::INFINITY);
    }

    #[test]
    fn grouping_extension_and_merge_helpers_cover_boundary_cases() {
        let mut samples = Vec::new();
        for row in 0..2 {
            for corr in 0..2 {
                for chan in 0..3 {
                    let index = samples.len();
                    samples.push(sample(
                        index,
                        row,
                        corr,
                        chan,
                        (row + chan + 1) as f64,
                        false,
                    ));
                }
            }
        }
        samples[0].flag = true;
        let groups = group_samples(&samples);
        let mut modified = vec![false; samples.len()];
        modified[1] = true;
        modified[2] = true;
        modified[3] = true;
        modified[4] = true;
        extend_autoflag_grouped(&groups, &samples, &mut modified);
        assert!(modified[0]);
        assert!(modified[5]);

        assert_eq!(samples_by_corr(&samples).len(), 2);
        assert_eq!(samples_by_row(&samples).len(), 2);
        assert_eq!(samples_by_chan(&samples).len(), 3);
        assert_eq!(samples_by_chan_dense(&samples).len(), 3);
        assert_eq!(sample_row_runs(&samples).len(), 2);
        assert_eq!(unique_sample_rows(&samples), vec![0, 1]);
        assert_eq!(unique_sample_chans(&samples), vec![0, 1, 2]);
        assert_eq!(samples_by_row_chan(&samples).len(), 6);
        assert_eq!(percent_flagged(&samples), 100.0 / 12.0);
        assert_eq!(percent_modified(&samples, &modified), 100.0);
        assert_eq!(count_mask(&modified), 12);
        assert_eq!(
            union_masks(&[true, false], &[false, true]),
            vec![true, true]
        );
        assert_eq!(summary_after_flag_only(&samples, 2).flagged_samples, 3);
        assert_eq!(
            flag_mask_counts_by_spw_corr(&modified, &samples).get("spw0_corr0"),
            Some(&6)
        );

        assert!(merge_bool(true, false, FlagMerge::Or));
        assert!(!merge_bool(true, false, FlagMerge::And));
        let source = ArrayD::from_elem(IxDyn(&[1, 2]), true);
        let dest = ArrayD::from_elem(IxDyn(&[1, 2]), false);
        let merged = merge_bool_arrays(&source, &dest, FlagMerge::Replace).expect("merge arrays");
        assert!(merged.iter().all(|flag| *flag));
        let mismatched = ArrayD::from_elem(IxDyn(&[2, 1]), false);
        assert!(merge_bool_arrays(&source, &mismatched, FlagMerge::Or).is_err());
    }

    #[test]
    fn clip_sample_and_threshold_helpers_cover_errors_and_defaults() {
        let flags_rank1 = ArrayD::from_elem(IxDyn(&[2]), false);
        let data_rank2 = ArrayValue::Float32(array![[0.0_f32, 1.0]].into_dyn());
        let mut changed = Vec::new();
        assert!(
            apply_clip_zero_array(&data_rank2, &flags_rank1, None, "<test>", 12, &mut changed,)
                .is_err()
        );

        let flags_rank2 = ArrayD::from_elem(IxDyn(&[1, 2]), false);
        let data_rank1 = ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0_f64));
        assert!(
            apply_clip_zero_array(&data_rank1, &flags_rank2, None, "<test>", 13, &mut changed,)
                .is_err()
        );

        let metadata = RowSampleMetadata {
            row: 14,
            field: 0,
            spw: 0,
            scan: 0,
            ant1: 2,
            ant2: 1,
        };
        let mut samples = Vec::new();
        assert!(
            push_row_samples(
                &data_rank2,
                &flags_rank1,
                &[0],
                metadata,
                "<test>",
                "DATA",
                &mut samples,
            )
            .is_err()
        );
        assert!(
            push_row_samples(
                &data_rank1,
                &flags_rank2,
                &[0],
                metadata,
                "<test>",
                "DATA",
                &mut samples,
            )
            .is_err()
        );

        let strings = ArrayValue::String(ArrayD::from_elem(IxDyn(&[1, 1]), "x".to_string()));
        push_row_samples(
            &strings,
            &flags_rank2,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("ignore non-numeric arrays");
        assert!(samples.is_empty());

        let groups = BTreeMap::from([(
            (0, 0, 0, 1, 2),
            vec![
                sample(0, 0, 0, 0, 1.0, false),
                sample(1, 1, 0, 0, 3.0, false),
                sample(2, 2, 0, 0, 5.0, false),
            ],
        )]);
        let thresholds = rflag_threshold_maps_for_groups(
            &groups,
            &FlagDataRequest {
                timedev: None,
                freqdev: None,
                timedevscale: 2.0,
                freqdevscale: 3.0,
                ..FlagDataRequest::default()
            },
        );
        assert!(thresholds.representative_pair().0.is_finite());
        assert!(thresholds.representative_pair().1.is_finite());
        assert_eq!(
            format_threshold_map(&thresholds.timedev)
                .keys()
                .collect::<Vec<_>>(),
            vec![&"field0_spw0".to_string()]
        );
    }

    #[test]
    fn tfcrop_flagger_handles_empty_all_flagged_and_missing_axis_cases() {
        let mut modified = Vec::new();
        let mut direction_flags = Vec::new();
        tfcrop_fit_base_and_flag(
            &[],
            TfcropDirection::Freq,
            TfcropFit::Poly,
            1.0,
            &mut modified,
            &mut direction_flags,
        );

        let samples = vec![
            sample(0, 0, 0, 0, 1.0, true),
            sample(1, 0, 0, 2, 1.0, true),
            sample(2, 2, 0, 0, 1.0, true),
        ];
        let mut modified = vec![false; samples.len()];
        let mut direction_flags = vec![false; samples.len()];
        tfcrop_fit_base_and_flag(
            &samples,
            TfcropDirection::Time,
            TfcropFit::Line,
            1.0,
            &mut modified,
            &mut direction_flags,
        );
        assert_eq!(modified, vec![false; samples.len()]);
        assert_eq!(direction_flags, vec![false; samples.len()]);

        let mut flagged = vec![false, true, false, true];
        let fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0, 2.0, 0.0], &mut flagged, 3, 2);
        assert_eq!(fit.len(), 4);
        assert!(fit.iter().all(|value| value.is_finite()));

        let mut all_zero_flags = vec![false, false];
        let all_zero_fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0], &mut all_zero_flags, 1, 1);
        assert_eq!(all_zero_fit, vec![0.0, 0.0]);
        assert_eq!(all_zero_flags, vec![true, true]);
    }
}
