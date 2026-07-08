// SPDX-License-Identifier: LGPL-3.0-or-later
//! Limited `bandpass` support for the first `B Jones` solver wave.
//!
//! This module intentionally implements only a narrow, testable slice:
//!
//! - `bandtype='B'`
//! - `solint='inf'`
//! - explicit reference antenna
//! - prior gain-table preapply
//! - point-source Stokes-I sky model (`smodel=[I,0,0,0]`)
//!
//! The acceptance contract is downstream behavior: the resulting caltable
//! should be CASA-compatible on disk and should yield corrected visibilities
//! close to CASA's own `bandpass` when applied after the same prior gains.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_ms::selection::MsSelection;
use casa_tables::{
    ColumnSchema, DataManagerKind, Table, TableError, TableInfo, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CAL_DESC_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_N_POLY_AMP, COL_N_POLY_PHASE, COL_OBSERVATION_ID, COL_PARAMERR, COL_PHASE_UNITS,
    COL_POLY_COEFF_AMP, COL_POLY_COEFF_PHASE, COL_SCALE_FACTOR, COL_SCAN_NUMBER, COL_SNR,
    COL_SPECTRAL_WINDOW_ID, COL_TIME, COL_VALID_DOMAIN, COL_WEIGHT, KEY_CASA_VERSION, KEY_MS_NAME,
    KEY_PAR_TYPE, KEY_POL_BASIS, KEY_VIS_CAL, LEGACY_CAL_DESC_KEYWORD, STANDARD_SUBTABLE_KEYWORDS,
    TABLE_INFO_TYPE,
};
use crate::execute::{ApplyExecutionError, EvaluatedApplyRow, evaluate_apply_rows};
use crate::plan::{ApplyPlanError, ApplyPlanRequest, plan_apply};
use crate::solve::grouping::{
    SelectedSolveRow, collect_selected_rows, correlation_types_for_ddid, resolve_refant,
    validate_smodel,
};
use crate::solve::kernel::{SolveGraphOptions, solve_graph};
use crate::solve::{GainSolveMode, RefAntSelector, correlation_receptors, stokes_name};
use casa_ms::least_squares::solve_weighted_least_squares;

const COL_ARRAY_ID: &str = "ARRAY_ID";
const COL_TIME_EXTRA_PREC: &str = "TIME_EXTRA_PREC";
const LEGACY_CAL_HISTORY_KEYWORD: &str = "CAL_HISTORY";

/// Supported `bandpass` table families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BandpassType {
    /// Channelized complex `B Jones` solutions stored in `CPARAM`.
    B,
    /// Legacy polynomial `BPOLY` bandpass solutions.
    BPoly,
}

/// Supported `bandpass(..., combine=...)` axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct BandpassSolveCombine {
    /// Extend solves across scan boundaries.
    pub scans: bool,
    /// Extend solves across field boundaries.
    pub fields: bool,
}

/// Request for a limited `bandpass` solve.
#[derive(Debug, Clone)]
pub struct BandpassSolveRequest {
    /// MS selection applied before solving.
    pub selection: MsSelection,
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Reference antenna.
    pub refant: RefAntSelector,
    /// Prior calibration tables to apply on the fly before solving.
    pub prior_calibration_tables: Vec<crate::ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction before solving.
    pub parang: bool,
    /// Axes combined before solving.
    pub combine: BandpassSolveCombine,
    /// Requested output bandpass table family.
    pub band_type: BandpassType,
    /// Whether to normalize average amplitudes to unity. Deferred from this wave.
    pub normalize_average_amplitude: bool,
    /// Requested amplitude polynomial degree for `BPOLY` output.
    pub amplitude_degree: usize,
    /// Requested phase polynomial degree for `BPOLY` output.
    pub phase_degree: usize,
    /// Point-source Stokes model. Only `[I,0,0,0]` is supported in this wave.
    pub smodel: [f32; 4],
}

/// Solve summary for a limited `bandpass` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BandpassSolveReport {
    /// Output caltable path.
    pub output_table: PathBuf,
    /// CASA-visible caltable subtype.
    pub table_subtype: String,
    /// Resolved reference antenna id.
    pub refant_antenna_id: i32,
    /// Distinct fields represented in the solved table.
    pub field_ids: Vec<i32>,
    /// Distinct SPWs represented in the solved table.
    pub spectral_window_ids: Vec<i32>,
    /// Number of solution rows written.
    pub solution_row_count: usize,
    /// Number of channels in each solved row.
    pub channel_count: usize,
}

/// Errors returned by the limited `bandpass` solver.
#[derive(Debug, Error)]
pub enum BandpassSolveError {
    /// Opening the MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying error.
        #[source]
        source: MsError,
    },

    /// The selection produced no rows.
    #[error("bandpass solve selection produced no rows")]
    EmptySelection,

    /// The solve requires a point-source Stokes-I model.
    #[error("unsupported smodel {smodel:?}; only [I,0,0,0] is supported in this wave")]
    UnsupportedSkyModel {
        /// Model vector passed by the caller.
        smodel: [f32; 4],
    },

    /// The selected output configuration is unsupported.
    #[error("unsupported bandpass solve configuration: {reason}")]
    UnsupportedConfiguration {
        /// Error context.
        reason: String,
    },

    /// The reference antenna could not be resolved.
    #[error("failed to resolve reference antenna {selector}: {reason}")]
    ResolveRefAnt {
        /// Caller-visible selector.
        selector: String,
        /// Additional context.
        reason: String,
    },

    /// Planning prior on-the-fly calibration application failed.
    #[error("failed to plan prior calibration application: {source}")]
    PriorCalibrationPlan {
        /// Underlying apply-planning error.
        #[source]
        source: Box<ApplyPlanError>,
    },

    /// Evaluating prior on-the-fly calibration application failed.
    #[error("failed to evaluate prior calibration application: {source}")]
    PriorCalibrationApply {
        /// Underlying apply-execution error.
        #[source]
        source: Box<ApplyExecutionError>,
    },

    /// The MS polarization layout is outside the supported diagonal surface.
    #[error(
        "unsupported correlation layout for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
    )]
    UnsupportedCorrelationLayout {
        /// Data description id.
        data_desc_id: i32,
        /// Correlation names.
        correlation_types: Vec<String>,
    },

    /// A table/column cell had an unexpected shape.
    #[error("unsupported parameter shape in {path}: {shape:?}")]
    UnsupportedParameterShape {
        /// Logical source.
        path: String,
        /// Discovered shape.
        shape: Vec<usize>,
    },

    /// Fitting a legacy BPOLY row failed.
    #[error(
        "failed to fit BPOLY row for antenna {antenna_id} field={field_id} spw={spw_id}: {reason}"
    )]
    FitPolynomial {
        /// Antenna id.
        antenna_id: i32,
        /// Field id.
        field_id: i32,
        /// Spectral window id.
        spw_id: i32,
        /// Error context.
        reason: String,
    },

    /// The solve failed because the selected data do not connect the reference
    /// antenna to at least one solved antenna.
    #[error(
        "bandpass solve could not determine a solution for antenna {antenna_id} in field={field_id} spw={spw_id}"
    )]
    UnsolvableAntenna {
        /// Antenna id.
        antenna_id: i32,
        /// Field id.
        field_id: i32,
        /// Spectral window id.
        spw_id: i32,
    },

    /// Persisting the output caltable failed.
    #[error("failed to save caltable {path}: {source}")]
    SaveCalibrationTable {
        /// Output path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Copying an MS subtable into the caltable failed.
    #[error("failed to copy {subtable} subtable into {path}: {source}")]
    CopySubtable {
        /// Subtable name.
        subtable: String,
        /// Destination root.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Filesystem preparation for the output root failed.
    #[error("failed to prepare output path {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },
}

/// Solve a limited `bandpass` request from an on-disk MeasurementSet path.
pub fn solve_bandpass_from_path(
    path: impl AsRef<Path>,
    request: &BandpassSolveRequest,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    let path = path.as_ref().to_path_buf();
    let ms =
        MeasurementSet::open(&path).map_err(|source| BandpassSolveError::OpenMeasurementSet {
            path: path.display().to_string(),
            source,
        })?;
    solve_bandpass(&ms, request)
}

/// Solve a limited `bandpass` request from an already-open MeasurementSet.
pub fn solve_bandpass(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    validate_bandpass_request(request)?;
    let refant_id = resolve_refant(ms, &request.refant).map_err(map_refant_error)?;
    let available_antennas = all_antenna_ids(ms)?;
    let rows = collect_selected_rows(ms, &request.selection).map_err(map_collect_rows_error)?;
    let preapplied_rows = load_preapplied_rows(ms, request)?;
    let groups = build_bandpass_groups(
        ms,
        &rows,
        preapplied_rows.as_ref(),
        request.smodel[0],
        request.combine,
    )?;

    if groups.is_empty() {
        return Err(BandpassSolveError::EmptySelection);
    }

    let mut solution_rows = Vec::new();
    for group in groups.into_values() {
        solution_rows.extend(solve_bandpass_group(
            group,
            &available_antennas,
            refant_id,
            request.combine,
            request.normalize_average_amplitude,
        )?);
    }
    match request.band_type {
        BandpassType::B => write_bandpass_caltable(ms, request, refant_id, &solution_rows),
        BandpassType::BPoly => write_bpoly_caltable(ms, request, refant_id, &solution_rows),
    }
}

fn validate_bandpass_request(request: &BandpassSolveRequest) -> Result<(), BandpassSolveError> {
    validate_smodel(request.smodel).map_err(map_validate_smodel_error)?;
    if matches!(request.band_type, BandpassType::BPoly)
        && (request.amplitude_degree > 31 || request.phase_degree > 31)
    {
        return Err(BandpassSolveError::UnsupportedConfiguration {
            reason: "BPOLY degree must be <= 31".to_string(),
        });
    }
    Ok(())
}

fn load_preapplied_rows(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
) -> Result<Option<HashMap<usize, EvaluatedApplyRow>>, BandpassSolveError> {
    if request.prior_calibration_tables.is_empty() && !request.parang {
        return Ok(None);
    }
    let plan = plan_apply(
        ms,
        &ApplyPlanRequest {
            selection: request.selection.clone(),
            apply_mode: crate::ApplyMode::CalFlag,
            parang: request.parang,
            calibration_tables: request.prior_calibration_tables.clone(),
        },
    )
    .map_err(|source| BandpassSolveError::PriorCalibrationPlan {
        source: Box::new(source),
    })?;
    evaluate_apply_rows(ms, &plan).map(Some).map_err(|source| {
        BandpassSolveError::PriorCalibrationApply {
            source: Box::new(source),
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BandpassGroupKey {
    field_id: i32,
    spw_id: i32,
    observation_id: i32,
    scan_number: i32,
}

#[derive(Debug, Clone)]
struct BandpassAccumulator {
    field_id: i32,
    spw_id: i32,
    observation_id: i32,
    min_time: f64,
    max_time: f64,
    total_interval: f64,
    sample_rows: usize,
    field_ids: BTreeSet<i32>,
    scan_numbers: BTreeSet<i32>,
    antenna_ids: BTreeSet<i32>,
    channel_count: usize,
    receptor_graphs: Vec<Vec<HashMap<(i32, i32), Complex32>>>,
    receptor_weights: Vec<Vec<HashMap<(i32, i32), f32>>>,
}

impl BandpassAccumulator {
    fn new(field_id: i32, spw_id: i32, observation_id: i32, channel_count: usize) -> Self {
        let mut receptor_graphs = Vec::new();
        let mut receptor_weights = Vec::new();
        for _ in 0..2 {
            let mut per_channel_graphs = Vec::new();
            let mut per_channel_weights = Vec::new();
            for _ in 0..channel_count {
                per_channel_graphs.push(HashMap::new());
                per_channel_weights.push(HashMap::new());
            }
            receptor_graphs.push(per_channel_graphs);
            receptor_weights.push(per_channel_weights);
        }

        Self {
            field_id,
            spw_id,
            observation_id,
            min_time: f64::INFINITY,
            max_time: f64::NEG_INFINITY,
            total_interval: 0.0,
            sample_rows: 0,
            field_ids: BTreeSet::new(),
            scan_numbers: BTreeSet::new(),
            antenna_ids: BTreeSet::new(),
            channel_count,
            receptor_graphs,
            receptor_weights,
        }
    }

    fn observe(
        &mut self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        preapplied_row: Option<&EvaluatedApplyRow>,
        stokes_i: f32,
    ) -> Result<(), BandpassSolveError> {
        let (data, flags) = match preapplied_row {
            Some(row) => (&row.corrected_data, &row.flags),
            None => {
                let data = ms
                    .main_table()
                    .cell_accessor(row.row_index, "DATA")
                    .and_then(|cell| cell.array())
                    .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                        path: ms
                            .path()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<in-memory>".to_string()),
                        source: MsError::from(source),
                    })?;
                let flags = ms
                    .main_table()
                    .cell_accessor(row.row_index, "FLAG")
                    .and_then(|cell| cell.array())
                    .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                        path: ms
                            .path()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<in-memory>".to_string()),
                        source: MsError::from(source),
                    })?;
                (data, flags)
            }
        };
        let weights = ms
            .main_table()
            .cell_accessor(row.row_index, "WEIGHT")
            .and_then(|cell| cell.array())
            .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                source: MsError::from(source),
            })?;
        let correlation_types =
            correlation_types_for_ddid(ms, row.data_desc_id).map_err(map_correlation_error)?;

        let ArrayValue::Complex32(data) = data else {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set DATA>".to_string(),
                shape: data.shape().to_vec(),
            });
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set FLAG>".to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        let ArrayValue::Float32(weights) = weights else {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weights.shape().to_vec(),
            });
        };
        if data.ndim() != 2 || flags.ndim() != 2 || data.shape() != flags.shape() {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set row>".to_string(),
                shape: data.shape().to_vec(),
            });
        }
        if self.channel_count != data.shape()[1] {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set DATA>".to_string(),
                shape: data.shape().to_vec(),
            });
        }
        if weights.ndim() != 1 || weights.shape()[0] != data.shape()[0] {
            return Err(BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weights.shape().to_vec(),
            });
        }

        self.min_time = self.min_time.min(row.time_seconds);
        self.max_time = self.max_time.max(row.time_seconds);
        self.total_interval += row.interval_seconds;
        self.sample_rows += 1;
        self.field_ids.insert(row.field_id);
        self.scan_numbers.insert(row.scan_number);
        self.antenna_ids.insert(row.antenna1);
        self.antenna_ids.insert(row.antenna2);

        for corr_index in 0..data.shape()[0] {
            let Some(receptors) = correlation_receptors(correlation_types[corr_index]) else {
                return Err(BandpassSolveError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                });
            };
            if receptors.0 != receptors.1 {
                continue;
            }
            let receptor = receptors.0;
            let weight = weights[[corr_index]];
            if weight <= 0.0 {
                continue;
            }
            for chan_index in 0..self.channel_count {
                if flags[[corr_index, chan_index]] {
                    continue;
                }
                let sample = data[[corr_index, chan_index]] / Complex32::new(stokes_i, 0.0);
                if sample.norm() <= f32::EPSILON {
                    continue;
                }
                crate::solve::kernel::accumulate_edge(
                    &mut self.receptor_graphs[receptor][chan_index],
                    &mut self.receptor_weights[receptor][chan_index],
                    row.antenna1,
                    row.antenna2,
                    weight,
                    sample * Complex32::new(weight, 0.0),
                );
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct BandpassSolutionRow {
    time_seconds: f64,
    interval_seconds: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    scan_number: i32,
    observation_id: i32,
    channel_count: usize,
    gains: Vec<Complex32>,
    flags: Vec<bool>,
}

#[derive(Debug, Clone)]
struct BPolySolutionRow {
    time_seconds: f64,
    interval_seconds: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    scan_number: i32,
    observation_id: i32,
    valid_domain_hz: [f64; 2],
    scale_factor: Complex32,
    amp_coefficients: Vec<Vec<f64>>,
    phase_coefficients: Vec<Vec<f64>>,
}

fn build_bandpass_groups(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    preapplied_rows: Option<&HashMap<usize, EvaluatedApplyRow>>,
    stokes_i: f32,
    combine: BandpassSolveCombine,
) -> Result<BTreeMap<BandpassGroupKey, BandpassAccumulator>, BandpassSolveError> {
    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by_key(|row| {
        (
            row.field_id,
            row.data_spw_id,
            row.observation_id,
            row.time_seconds.to_bits(),
            row.row_index,
        )
    });

    let mut groups = BTreeMap::<BandpassGroupKey, BandpassAccumulator>::new();
    for row in sorted_rows {
        if row.antenna1 == row.antenna2 {
            continue;
        }
        let data = match preapplied_rows.and_then(|rows| rows.get(&row.row_index)) {
            Some(preapplied) => &preapplied.corrected_data,
            None => ms
                .main_table()
                .cell_accessor(row.row_index, "DATA")
                .and_then(|cell| cell.array())
                .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                    path: ms
                        .path()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "<in-memory>".to_string()),
                    source: MsError::from(source),
                })?,
        };
        let channel_count = data.shape().get(1).copied().ok_or_else(|| {
            BandpassSolveError::UnsupportedParameterShape {
                path: "<measurement-set DATA>".to_string(),
                shape: data.shape().to_vec(),
            }
        })?;
        let key = BandpassGroupKey {
            field_id: if combine.fields { 0 } else { row.field_id },
            spw_id: row.data_spw_id,
            observation_id: row.observation_id,
            scan_number: if combine.scans { 0 } else { row.scan_number },
        };
        let group = groups.entry(key).or_insert_with(|| {
            BandpassAccumulator::new(
                row.field_id,
                row.data_spw_id,
                row.observation_id,
                channel_count,
            )
        });
        let preapplied = preapplied_rows.and_then(|rows| rows.get(&row.row_index));
        group.observe(ms, &row, preapplied, stokes_i)?;
    }
    Ok(groups)
}

fn solve_bandpass_group(
    group: BandpassAccumulator,
    available_antennas: &BTreeSet<i32>,
    refant_id: i32,
    combine: BandpassSolveCombine,
    normalize_average_amplitude: bool,
) -> Result<Vec<BandpassSolutionRow>, BandpassSolveError> {
    let averaged_time = (group.min_time + group.max_time) / 2.0;
    let averaged_interval = group.total_interval / group.sample_rows.max(1) as f64;
    let scan_number = *group.scan_numbers.iter().next().unwrap_or(&0);

    let mut solved_by_receptor = Vec::new();
    for receptor in 0..2 {
        let mut channel_solutions = Vec::new();
        for chan_index in 0..group.channel_count {
            channel_solutions.push(
                solve_graph(
                    &group.receptor_graphs[receptor][chan_index],
                    &group.receptor_weights[receptor][chan_index],
                    &HashMap::new(),
                    GainSolveMode::AmplitudePhase,
                    SolveGraphOptions {
                        refant_id,
                        min_baselines_per_antenna: 0,
                    },
                )
                .map_err(map_solve_graph_error)?,
            );
        }
        solved_by_receptor.push(channel_solutions);
    }

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids);
    antenna_ids.insert(refant_id);

    let output_field_ids = if combine.fields && !combine.scans {
        let mut fields = group.field_ids.iter().copied().collect::<Vec<_>>();
        fields.sort_unstable();
        fields
    } else {
        vec![group.field_id]
    };

    let mut solution_rows = Vec::new();
    for field_id in output_field_ids {
        for antenna_id in antenna_ids.iter().copied() {
            let mut gains = Vec::with_capacity(2 * group.channel_count);
            let mut flags = Vec::with_capacity(2 * group.channel_count);
            for channel_solutions in solved_by_receptor[0]
                .iter()
                .zip(solved_by_receptor[1].iter())
            {
                for solved in [channel_solutions.0, channel_solutions.1] {
                    gains.push(
                        *solved
                            .gains
                            .get(&antenna_id)
                            .unwrap_or(&Complex32::new(1.0, 0.0)),
                    );
                    flags.push(antenna_id != refant_id && !solved.reachable.contains(&antenna_id));
                }
            }
            solution_rows.push(BandpassSolutionRow {
                time_seconds: averaged_time,
                interval_seconds: averaged_interval,
                field_id,
                spw_id: group.spw_id,
                antenna_id,
                scan_number,
                observation_id: group.observation_id,
                channel_count: group.channel_count,
                gains,
                flags,
            });
        }
    }

    if normalize_average_amplitude {
        Ok(normalize_bandpass_rows(solution_rows, group.channel_count))
    } else {
        Ok(solution_rows)
    }
}

fn normalize_bandpass_rows(
    mut rows: Vec<BandpassSolutionRow>,
    channel_count: usize,
) -> Vec<BandpassSolutionRow> {
    for row in &mut rows {
        normalize_bandpass_row(row, channel_count);
    }
    rows
}

fn normalize_bandpass_row(row: &mut BandpassSolutionRow, channel_count: usize) {
    if channel_count == 0 {
        return;
    }
    for receptor in 0..2 {
        let mut coherent_sum = Complex32::new(0.0, 0.0);
        let mut power_sum = 0.0_f32;
        let mut good_count = 0usize;
        for channel in 0..channel_count {
            let index = receptor + 2 * channel;
            if row.flags.get(index).copied().unwrap_or(false) {
                continue;
            }
            let gain = row.gains[index];
            let amplitude = gain.norm();
            if amplitude <= f32::EPSILON {
                continue;
            }
            coherent_sum += gain / Complex32::new(amplitude, 0.0);
            power_sum += amplitude * amplitude;
            good_count += 1;
        }
        if good_count <= 1 {
            continue;
        }
        let amplitude_factor = (power_sum / good_count as f32).sqrt();
        if amplitude_factor <= f32::EPSILON {
            continue;
        }
        let phase_factor = if coherent_sum.norm() > f32::EPSILON {
            coherent_sum / Complex32::new(coherent_sum.norm(), 0.0)
        } else {
            Complex32::new(1.0, 0.0)
        };
        let normalization = phase_factor * Complex32::new(amplitude_factor, 0.0);
        for channel in 0..channel_count {
            let index = receptor + 2 * channel;
            row.gains[index] /= normalization;
        }
    }
}

fn write_bandpass_caltable(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    refant_id: i32,
    rows: &[BandpassSolutionRow],
) -> Result<BandpassSolveReport, BandpassSolveError> {
    prepare_output_root(&request.output_table)?;

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable(COL_CPARAM, casa_types::PrimitiveType::Complex32, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid bandpass schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: TABLE_INFO_TYPE.to_string(),
        sub_type: "B Jones".to_string(),
        readme: Vec::new(),
    });
    table.keywords_mut().upsert(
        KEY_PAR_TYPE,
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_VIS_CAL,
        Value::Scalar(ScalarValue::String("B Jones".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_MS_NAME,
        Value::Scalar(ScalarValue::String(
            ms.path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
        )),
    );
    table.keywords_mut().upsert(
        KEY_POL_BASIS,
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_CASA_VERSION,
        Value::Scalar(ScalarValue::String("casa-rs".to_string())),
    );
    set_fixed_unit_keyword(&mut table, COL_TIME, &["s"]);
    set_measinfo_keyword(&mut table, COL_TIME, "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, COL_INTERVAL, &["s"]);
    set_fixed_unit_keyword(&mut table, COL_TIME_EXTRA_PREC, &["s"]);
    for name in STANDARD_SUBTABLE_KEYWORDS {
        table.keywords_mut().upsert(
            *name,
            Value::table_ref(subtable_keyword_value(
                &request.output_table,
                &request.output_table.join(name),
            )),
        );
    }

    for row in rows {
        let receptor_count = 2;
        let element_count = receptor_count * row.channel_count;
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    COL_TIME,
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new(
                    COL_FIELD_ID,
                    Value::Scalar(ScalarValue::Int32(row.field_id)),
                ),
                RecordField::new(
                    COL_SPECTRAL_WINDOW_ID,
                    Value::Scalar(ScalarValue::Int32(row.spw_id)),
                ),
                RecordField::new(
                    COL_ANTENNA1,
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new(COL_ANTENNA2, Value::Scalar(ScalarValue::Int32(refant_id))),
                RecordField::new(
                    COL_INTERVAL,
                    Value::Scalar(ScalarValue::Float64(row.interval_seconds)),
                ),
                RecordField::new(
                    COL_SCAN_NUMBER,
                    Value::Scalar(ScalarValue::Int32(row.scan_number)),
                ),
                RecordField::new(
                    COL_OBSERVATION_ID,
                    Value::Scalar(ScalarValue::Int32(row.observation_id)),
                ),
                RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    COL_TIME_EXTRA_PREC,
                    Value::Scalar(ScalarValue::Float64(0.0)),
                ),
                RecordField::new(
                    COL_CPARAM,
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[receptor_count, row.channel_count]).f(),
                            row.gains.clone(),
                        )
                        .expect("bandpass gains should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_PARAMERR,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[receptor_count, row.channel_count]).f(),
                            vec![0.0; element_count],
                        )
                        .expect("bandpass paramerr should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_FLAG,
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            IxDyn(&[receptor_count, row.channel_count]).f(),
                            row.flags.clone(),
                        )
                        .expect("bandpass flags should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_SNR,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[receptor_count, row.channel_count]).f(),
                            vec![1.0; element_count],
                        )
                        .expect("bandpass snr should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_WEIGHT,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[receptor_count, row.channel_count]).f(),
                            vec![1.0; element_count],
                        )
                        .expect("bandpass weight should reshape to receptor x channel"),
                    )),
                ),
            ]))
            .expect("insert bandpass row");
    }

    table
        .save(
            TableOptions::new(&request.output_table)
                .with_data_manager(DataManagerKind::StandardStMan),
        )
        .map_err(|source| BandpassSolveError::SaveCalibrationTable {
            path: request.output_table.display().to_string(),
            source: Box::new(source),
        })?;

    for (id, name) in [
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ] {
        ms.subtable(id)
            .expect("required subtable available")
            .save(TableOptions::new(request.output_table.join(name)))
            .map_err(|source| BandpassSolveError::CopySubtable {
                subtable: name.to_string(),
                path: request.output_table.display().to_string(),
                source: Box::new(source),
            })?;
    }

    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    let channel_count = rows.first().map(|row| row.channel_count).unwrap_or(0);
    Ok(BandpassSolveReport {
        output_table: request.output_table.clone(),
        table_subtype: "B Jones".to_string(),
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
        channel_count,
    })
}

fn write_bpoly_caltable(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    refant_id: i32,
    rows: &[BandpassSolutionRow],
) -> Result<BandpassSolveReport, BandpassSolveError> {
    prepare_output_root(&request.output_table)?;
    let fitted_rows = fit_bpoly_rows(ms, rows, request.amplitude_degree, request.phase_degree)?;

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("FEED1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("PROCESSOR_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("STATE_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("PHASE_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("PULSAR_BIN", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("PULSAR_GATE_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("FREQ_GROUP", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("FREQ_GROUP_NAME", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("FIELD_NAME", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("FIELD_CODE", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("SOURCE_NAME", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("SOURCE_CODE", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("CALIBRATION_GROUP", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_CAL_DESC_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("CAL_HISTORY_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("GAIN", casa_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::scalar("SIDEBAND_REF", casa_types::PrimitiveType::Complex32),
        ColumnSchema::array_variable("REF_ANT", casa_types::PrimitiveType::Int32, Some(1)),
        ColumnSchema::array_variable("REF_FEED", casa_types::PrimitiveType::Int32, Some(1)),
        ColumnSchema::array_variable("REF_RECEPTOR", casa_types::PrimitiveType::Int32, Some(1)),
        ColumnSchema::array_variable("REF_FREQUENCY", casa_types::PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("MEAS_FREQ_REF", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("REF_DIRECTION", casa_types::PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("MEAS_DIR_REF", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("TOTAL_SOLUTION_OK", casa_types::PrimitiveType::Bool),
        ColumnSchema::scalar("TOTAL_FIT", casa_types::PrimitiveType::Float32),
        ColumnSchema::scalar("TOTAL_FIT_WEIGHT", casa_types::PrimitiveType::Float32),
        ColumnSchema::array_variable("SOLUTION_OK", casa_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("FIT", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FIT_WEIGHT", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::scalar("POLY_TYPE", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("POLY_MODE", casa_types::PrimitiveType::String),
        ColumnSchema::scalar(COL_SCALE_FACTOR, casa_types::PrimitiveType::Complex32),
        ColumnSchema::scalar(COL_N_POLY_AMP, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_N_POLY_PHASE, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_PHASE_UNITS, casa_types::PrimitiveType::String),
        ColumnSchema::array_variable(
            COL_VALID_DOMAIN,
            casa_types::PrimitiveType::Float64,
            Some(1),
        ),
        ColumnSchema::array_variable(
            COL_POLY_COEFF_AMP,
            casa_types::PrimitiveType::Float64,
            Some(4),
        ),
        ColumnSchema::array_variable(
            COL_POLY_COEFF_PHASE,
            casa_types::PrimitiveType::Float64,
            Some(4),
        ),
    ])
    .expect("valid BPOLY schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: TABLE_INFO_TYPE.to_string(),
        sub_type: "BPOLY".to_string(),
        readme: Vec::new(),
    });
    table
        .keywords_mut()
        .upsert(LEGACY_CAL_DESC_KEYWORD, Value::table_ref("././CAL_DESC"));
    table.keywords_mut().upsert(
        LEGACY_CAL_HISTORY_KEYWORD,
        Value::table_ref("././CAL_HISTORY"),
    );
    table.keywords_mut().upsert(
        KEY_VIS_CAL,
        Value::Scalar(ScalarValue::String("BPOLY".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_MS_NAME,
        Value::Scalar(ScalarValue::String(
            ms.path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
        )),
    );
    table.keywords_mut().upsert(
        KEY_CASA_VERSION,
        Value::Scalar(ScalarValue::String("casa-rs".to_string())),
    );
    for name in STANDARD_SUBTABLE_KEYWORDS {
        table.keywords_mut().upsert(
            *name,
            Value::table_ref(subtable_keyword_value(
                &request.output_table,
                &request.output_table.join(name),
            )),
        );
    }
    set_fixed_unit_keyword(&mut table, COL_TIME, &["s"]);
    set_measinfo_keyword(&mut table, COL_TIME, "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, COL_INTERVAL, &["s"]);
    set_fixed_unit_keyword(&mut table, COL_TIME_EXTRA_PREC, &["s"]);
    set_fixed_unit_keyword(&mut table, COL_VALID_DOMAIN, &["Hz"]);
    set_fixed_unit_keyword(&mut table, "REF_FREQUENCY", &["Hz"]);
    set_measinfo_keyword(&mut table, "REF_FREQUENCY", "frequency", Some("TOPO"));
    set_fixed_unit_keyword(&mut table, "REF_DIRECTION", &["rad", "rad"]);
    set_measinfo_keyword(&mut table, "REF_DIRECTION", "direction", Some("J2000"));

    let cal_desc_ids = fitted_rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(cal_desc_id, spw_id)| {
            (
                spw_id,
                i32::try_from(cal_desc_id).expect("small BPOLY CAL_DESC id set"),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for row in &fitted_rows {
        let reference_frequency_hz = (row.valid_domain_hz[0] + row.valid_domain_hz[1]) / 2.0;
        let flat_amp = row
            .amp_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let flat_phase = row
            .phase_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let amp_shape = IxDyn(&[1, 1, 1, flat_amp.len()]).f();
        let phase_shape = IxDyn(&[1, 1, 1, flat_phase.len()]).f();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    COL_TIME,
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new(
                    COL_TIME_EXTRA_PREC,
                    Value::Scalar(ScalarValue::Float64(0.0)),
                ),
                RecordField::new(
                    COL_FIELD_ID,
                    Value::Scalar(ScalarValue::Int32(row.field_id)),
                ),
                RecordField::new(
                    COL_ANTENNA1,
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("FEED1", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    COL_INTERVAL,
                    Value::Scalar(ScalarValue::Float64(row.interval_seconds)),
                ),
                RecordField::new(
                    COL_SCAN_NUMBER,
                    Value::Scalar(ScalarValue::Int32(row.scan_number)),
                ),
                RecordField::new(
                    COL_OBSERVATION_ID,
                    Value::Scalar(ScalarValue::Int32(row.observation_id)),
                ),
                RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("PROCESSOR_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("PHASE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("PULSAR_BIN", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("PULSAR_GATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new(
                    "FIELD_NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new(
                    "FIELD_CODE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new(
                    "SOURCE_NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new(
                    "SOURCE_CODE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new("CALIBRATION_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    COL_CAL_DESC_ID,
                    Value::Scalar(ScalarValue::Int32(
                        *cal_desc_ids
                            .get(&row.spw_id)
                            .expect("BPOLY CAL_DESC entry for solved SPW"),
                    )),
                ),
                RecordField::new("CAL_HISTORY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    "GAIN",
                    Value::Array(ArrayValue::from_complex32_vec(vec![Complex32::new(
                        1.0, 0.0,
                    )])),
                ),
                RecordField::new(
                    "SIDEBAND_REF",
                    Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, 0.0))),
                ),
                RecordField::new(
                    "REF_ANT",
                    Value::Array(ArrayValue::from_i32_vec(vec![row.antenna_id])),
                ),
                RecordField::new("REF_FEED", Value::Array(ArrayValue::from_i32_vec(vec![0]))),
                RecordField::new(
                    "REF_RECEPTOR",
                    Value::Array(ArrayValue::from_i32_vec(vec![0])),
                ),
                RecordField::new(
                    "REF_FREQUENCY",
                    Value::Array(ArrayValue::from_f64_vec(vec![reference_frequency_hz])),
                ),
                RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    "REF_DIRECTION",
                    Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])),
                ),
                RecordField::new("MEAS_DIR_REF", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TOTAL_SOLUTION_OK", Value::Scalar(ScalarValue::Bool(true))),
                RecordField::new("TOTAL_FIT", Value::Scalar(ScalarValue::Float32(0.0))),
                RecordField::new("TOTAL_FIT_WEIGHT", Value::Scalar(ScalarValue::Float32(1.0))),
                RecordField::new(
                    "SOLUTION_OK",
                    Value::Array(ArrayValue::from_bool_vec(vec![true, true])),
                ),
                RecordField::new(
                    "FIT",
                    Value::Array(ArrayValue::from_f32_vec(vec![0.0, 0.0])),
                ),
                RecordField::new(
                    "FIT_WEIGHT",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
                ),
                RecordField::new(
                    COL_FLAG,
                    Value::Array(ArrayValue::from_bool_vec(vec![false, false])),
                ),
                RecordField::new(
                    COL_PARAMERR,
                    Value::Array(ArrayValue::from_f32_vec(vec![0.0, 0.0])),
                ),
                RecordField::new(
                    COL_SNR,
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
                ),
                RecordField::new(
                    COL_WEIGHT,
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
                ),
                RecordField::new(
                    "POLY_TYPE",
                    Value::Scalar(ScalarValue::String("CHEBYSHEV".to_string())),
                ),
                RecordField::new(
                    "POLY_MODE",
                    Value::Scalar(ScalarValue::String("A&P".to_string())),
                ),
                RecordField::new(
                    COL_SCALE_FACTOR,
                    Value::Scalar(ScalarValue::Complex32(row.scale_factor)),
                ),
                RecordField::new(
                    COL_N_POLY_AMP,
                    Value::Scalar(ScalarValue::Int32(
                        i32::try_from(row.amp_coefficients.first().map_or(0, Vec::len))
                            .expect("small BPOLY amp coefficient count"),
                    )),
                ),
                RecordField::new(
                    COL_N_POLY_PHASE,
                    Value::Scalar(ScalarValue::Int32(
                        i32::try_from(row.phase_coefficients.first().map_or(0, Vec::len))
                            .expect("small BPOLY phase coefficient count"),
                    )),
                ),
                RecordField::new(
                    COL_PHASE_UNITS,
                    Value::Scalar(ScalarValue::String("RAD".to_string())),
                ),
                RecordField::new(
                    COL_VALID_DOMAIN,
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2]).f(),
                            vec![row.valid_domain_hz[0], row.valid_domain_hz[1]],
                        )
                        .expect("BPOLY valid domain should reshape"),
                    )),
                ),
                RecordField::new(
                    COL_POLY_COEFF_AMP,
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(amp_shape, flat_amp)
                            .expect("BPOLY amp coefficients should reshape"),
                    )),
                ),
                RecordField::new(
                    COL_POLY_COEFF_PHASE,
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(phase_shape, flat_phase)
                            .expect("BPOLY phase coefficients should reshape"),
                    )),
                ),
            ]))
            .expect("insert BPOLY row");
    }

    table
        .save(
            TableOptions::new(&request.output_table)
                .with_data_manager(DataManagerKind::StandardStMan),
        )
        .map_err(|source| BandpassSolveError::SaveCalibrationTable {
            path: request.output_table.display().to_string(),
            source: Box::new(source),
        })?;

    write_bpoly_cal_desc_subtable(
        request.output_table.join("CAL_DESC"),
        &cal_desc_ids,
        &fitted_rows,
    )?;
    for (id, name) in [
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ] {
        ms.subtable(id)
            .expect("required subtable available")
            .save(TableOptions::new(request.output_table.join(name)))
            .map_err(|source| BandpassSolveError::CopySubtable {
                subtable: name.to_string(),
                path: request.output_table.display().to_string(),
                source: Box::new(source),
            })?;
    }
    Table::with_schema(TableSchema::new(vec![]).expect("empty schema"))
        .save(TableOptions::new(request.output_table.join("CAL_HISTORY")))
        .map_err(|source| BandpassSolveError::SaveCalibrationTable {
            path: request
                .output_table
                .join("CAL_HISTORY")
                .display()
                .to_string(),
            source: Box::new(source),
        })?;

    let field_ids = fitted_rows
        .iter()
        .map(|row| row.field_id)
        .collect::<BTreeSet<_>>();
    let spw_ids = fitted_rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>();
    let channel_count = rows.first().map(|row| row.channel_count).unwrap_or(0);
    Ok(BandpassSolveReport {
        output_table: request.output_table.clone(),
        table_subtype: "BPOLY".to_string(),
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: fitted_rows.len(),
        channel_count,
    })
}

fn fit_bpoly_rows(
    ms: &MeasurementSet,
    rows: &[BandpassSolutionRow],
    amplitude_degree: usize,
    phase_degree: usize,
) -> Result<Vec<BPolySolutionRow>, BandpassSolveError> {
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                source,
            })?;
    rows.iter()
        .map(|row| {
            let spw_row = usize::try_from(row.spw_id).map_err(|_| {
                BandpassSolveError::UnsupportedParameterShape {
                    path: "SPECTRAL_WINDOW row id".to_string(),
                    shape: vec![row.spw_id as usize],
                }
            })?;
            let channel_frequencies_hz = spectral_window.chan_freq(spw_row).map_err(|source| {
                BandpassSolveError::OpenMeasurementSet {
                    path: ms
                        .path()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "<in-memory>".to_string()),
                    source,
                }
            })?;
            fit_bpoly_row(row, &channel_frequencies_hz, amplitude_degree, phase_degree)
        })
        .collect()
}

fn fit_bpoly_row(
    row: &BandpassSolutionRow,
    channel_frequencies_hz: &[f64],
    amplitude_degree: usize,
    phase_degree: usize,
) -> Result<BPolySolutionRow, BandpassSolveError> {
    if channel_frequencies_hz.len() != row.channel_count || row.channel_count == 0 {
        return Err(BandpassSolveError::UnsupportedParameterShape {
            path: "BPOLY fit channel grid".to_string(),
            shape: vec![channel_frequencies_hz.len(), row.channel_count],
        });
    }
    let valid_domain_hz = [
        channel_frequencies_hz.first().copied().unwrap_or(0.0),
        channel_frequencies_hz.last().copied().unwrap_or(0.0),
    ];
    let receptor_count = 2;
    let mut amp_coefficients = Vec::with_capacity(receptor_count);
    let mut phase_coefficients = Vec::with_capacity(receptor_count);
    for receptor in 0..receptor_count {
        let mut log_amplitudes = Vec::with_capacity(row.channel_count);
        let mut phases = Vec::with_capacity(row.channel_count);
        for channel in 0..row.channel_count {
            let gain = row.gains[receptor + receptor_count * channel];
            log_amplitudes.push(gain.norm().max(f32::EPSILON).ln() as f64);
            phases.push((gain.im as f64).atan2(gain.re as f64));
        }
        let unwrapped_phases = unwrap_phases(&phases);
        amp_coefficients.push(fit_legacy_bpoly_coefficients(
            channel_frequencies_hz,
            &log_amplitudes,
            valid_domain_hz,
            amplitude_degree,
            row,
        )?);
        phase_coefficients.push(fit_legacy_bpoly_coefficients(
            channel_frequencies_hz,
            &unwrapped_phases,
            valid_domain_hz,
            phase_degree,
            row,
        )?);
    }
    Ok(BPolySolutionRow {
        time_seconds: row.time_seconds,
        interval_seconds: row.interval_seconds,
        field_id: row.field_id,
        spw_id: row.spw_id,
        antenna_id: row.antenna_id,
        valid_domain_hz,
        scan_number: row.scan_number,
        observation_id: row.observation_id,
        scale_factor: Complex32::new(1.0, 0.0),
        amp_coefficients,
        phase_coefficients,
    })
}

fn fit_legacy_bpoly_coefficients(
    channel_frequencies_hz: &[f64],
    values: &[f64],
    valid_domain_hz: [f64; 2],
    degree: usize,
    row: &BandpassSolutionRow,
) -> Result<Vec<f64>, BandpassSolveError> {
    let coefficient_count = degree
        .saturating_add(1)
        .min(channel_frequencies_hz.len().max(1));
    if coefficient_count == 0 {
        return Ok(Vec::new());
    }

    let mut basis = vec![0.0_f64; coefficient_count];
    let mut rows = Vec::with_capacity(channel_frequencies_hz.len());
    for (frequency_hz, value) in channel_frequencies_hz
        .iter()
        .copied()
        .zip(values.iter().copied())
    {
        legacy_bpoly_basis(
            valid_domain_hz[0],
            valid_domain_hz[1],
            frequency_hz,
            &mut basis,
        );
        rows.push((basis.clone(), value, 1.0));
    }
    solve_weighted_least_squares(&rows, coefficient_count).ok_or_else(|| {
        BandpassSolveError::FitPolynomial {
            antenna_id: row.antenna_id,
            field_id: row.field_id,
            spw_id: row.spw_id,
            reason: "least-squares system was singular".to_string(),
        }
    })
}

fn legacy_bpoly_basis(x_start: f64, x_end: f64, x: f64, basis: &mut [f64]) {
    if basis.is_empty() {
        return;
    }
    basis.fill(0.0);
    basis[0] = 0.5_f64;
    if basis.len() == 1 || (x_end - x_start).abs() <= f64::EPSILON {
        return;
    }
    let xcap = ((x - x_start) - (x_end - x)) / (x_end - x_start);
    let mut t_prev = 1.0_f64;
    let mut t_curr = xcap;
    basis[1] = t_curr;
    for slot in basis.iter_mut().skip(2) {
        let t_next = 2.0_f64 * xcap * t_curr - t_prev;
        *slot = t_next;
        t_prev = t_curr;
        t_curr = t_next;
    }
}

fn unwrap_phases(phases: &[f64]) -> Vec<f64> {
    let mut unwrapped = Vec::with_capacity(phases.len());
    let mut offset = 0.0_f64;
    let mut previous = None;
    for phase in phases.iter().copied() {
        if let Some(previous_phase) = previous {
            let delta = phase - previous_phase;
            if delta > std::f64::consts::PI {
                offset -= 2.0_f64 * std::f64::consts::PI;
            } else if delta < -std::f64::consts::PI {
                offset += 2.0_f64 * std::f64::consts::PI;
            }
        }
        let adjusted = phase + offset;
        unwrapped.push(adjusted);
        previous = Some(phase);
    }
    unwrapped
}

fn write_bpoly_cal_desc_subtable(
    path: PathBuf,
    cal_desc_ids: &BTreeMap<i32, i32>,
    rows: &[BPolySolutionRow],
) -> Result<(), BandpassSolveError> {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NUM_RECEPTORS", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_types::PrimitiveType::Int32,
            Some(1),
        ),
    ])
    .expect("BPOLY CAL_DESC schema");
    let mut table = Table::with_schema(schema);
    for (spw_id, cal_desc_id) in cal_desc_ids {
        let receptor_count = rows
            .iter()
            .find(|row| row.spw_id == *spw_id)
            .map(|row| row.amp_coefficients.len() as i32)
            .unwrap_or(1);
        while table.row_count() < usize::try_from(*cal_desc_id).expect("small CAL_DESC id") {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(1))),
                    RecordField::new(
                        "SPECTRAL_WINDOW_ID",
                        Value::Array(ArrayValue::from_i32_vec(vec![0])),
                    ),
                ]))
                .expect("pad BPOLY CAL_DESC rows");
        }
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "NUM_RECEPTORS",
                    Value::Scalar(ScalarValue::Int32(receptor_count)),
                ),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Array(ArrayValue::from_i32_vec(vec![*spw_id])),
                ),
            ]))
            .expect("insert BPOLY CAL_DESC row");
    }
    table.save(TableOptions::new(&path)).map_err(|source| {
        BandpassSolveError::SaveCalibrationTable {
            path: path.display().to_string(),
            source: Box::new(source),
        }
    })
}

fn all_antenna_ids(ms: &MeasurementSet) -> Result<BTreeSet<i32>, BandpassSolveError> {
    let antenna = ms
        .antenna()
        .map_err(|source| BandpassSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    Ok((0..antenna.row_count())
        .map(|row| i32::try_from(row).expect("antenna row count should fit in i32"))
        .collect())
}

fn prepare_output_root(path: &Path) -> Result<(), BandpassSolveError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| BandpassSolveError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| BandpassSolveError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

fn set_fixed_unit_keyword(table: &mut Table, column: &str, units: &[&str]) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert(
        "QuantumUnits",
        Value::Array(ArrayValue::from_string_vec(
            units.iter().map(|unit| (*unit).to_string()).collect(),
        )),
    );
    table.set_column_keywords(column, keywords);
}

fn set_measinfo_keyword(
    table: &mut Table,
    column: &str,
    measure_type: &str,
    measure_ref: Option<&str>,
) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let mut fields = vec![RecordField::new(
        "type",
        Value::Scalar(ScalarValue::String(measure_type.to_string())),
    )];
    if let Some(measure_ref) = measure_ref {
        fields.push(RecordField::new(
            "Ref",
            Value::Scalar(ScalarValue::String(measure_ref.to_string())),
        ));
    }
    keywords.upsert("MEASINFO", Value::Record(RecordValue::new(fields)));
    table.set_column_keywords(column, keywords);
}

fn subtable_keyword_value(base_path: &Path, subtable_path: &Path) -> String {
    if let Ok(relative) = subtable_path.strip_prefix(base_path) {
        let rel = relative.to_string_lossy();
        return format!("././{rel}");
    }
    if let Some(parent) = base_path.parent()
        && let Ok(relative) = subtable_path.strip_prefix(parent)
    {
        let rel = relative.to_string_lossy();
        return format!("./{rel}");
    }
    if subtable_path.is_relative() {
        let rel = subtable_path.to_string_lossy();
        return format!("././{}", rel.trim_start_matches("./"));
    }
    subtable_path.to_string_lossy().to_string()
}

fn map_validate_smodel_error(error: crate::solve::GainSolveError) -> BandpassSolveError {
    match error {
        crate::solve::GainSolveError::UnsupportedSkyModel { smodel } => {
            BandpassSolveError::UnsupportedSkyModel { smodel }
        }
        other => panic!("unexpected validate_smodel error variant: {other}"),
    }
}

fn map_refant_error(error: crate::solve::GainSolveError) -> BandpassSolveError {
    match error {
        crate::solve::GainSolveError::ResolveRefAnt { selector, reason } => {
            BandpassSolveError::ResolveRefAnt { selector, reason }
        }
        crate::solve::GainSolveError::OpenMeasurementSet { path, source } => {
            BandpassSolveError::OpenMeasurementSet { path, source }
        }
        other => panic!("unexpected resolve_refant error variant: {other}"),
    }
}

fn map_collect_rows_error(error: crate::solve::GainSolveError) -> BandpassSolveError {
    match error {
        crate::solve::GainSolveError::EmptySelection => BandpassSolveError::EmptySelection,
        crate::solve::GainSolveError::OpenMeasurementSet { path, source } => {
            BandpassSolveError::OpenMeasurementSet { path, source }
        }
        crate::solve::GainSolveError::UnsupportedParameterShape { path, shape } => {
            BandpassSolveError::UnsupportedParameterShape { path, shape }
        }
        other => panic!("unexpected collect_selected_rows error variant: {other}"),
    }
}

fn map_correlation_error(error: crate::solve::GainSolveError) -> BandpassSolveError {
    match error {
        crate::solve::GainSolveError::OpenMeasurementSet { path, source } => {
            BandpassSolveError::OpenMeasurementSet { path, source }
        }
        crate::solve::GainSolveError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types,
        } => BandpassSolveError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types,
        },
        other => panic!("unexpected correlation_types_for_ddid error variant: {other}"),
    }
}

fn map_solve_graph_error(error: crate::solve::GainSolveError) -> BandpassSolveError {
    match error {
        crate::solve::GainSolveError::UnsolvableAntenna {
            antenna_id,
            field_id,
            spw_id,
        } => BandpassSolveError::UnsolvableAntenna {
            antenna_id,
            field_id,
            spw_id,
        },
        other => panic!("unexpected solve_graph error variant: {other}"),
    }
}
