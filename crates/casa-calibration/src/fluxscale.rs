// SPDX-License-Identifier: LGPL-3.0-or-later
//! Limited `fluxscale` support for the first bootstrap wave.
//!
//! This module intentionally implements a narrow but useful slice:
//!
//! - complex antenna-based gain tables (`CPARAM`)
//! - reference and transfer field resolution by id, exact name, or simple `*` glob
//! - optional `refspwmap`
//! - optional `gainthreshold`
//! - full-table output or incremental correction-factor output
//!
//! The acceptance contract is twofold:
//!
//! - the output caltable must remain CASA-compatible on disk
//! - the derived transfer fluxes and scaled gains must agree closely with CASA
//!   on slow parity fixtures

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::ArrayD;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_SPECTRAL_WINDOW_ID};
use crate::summary::{CalibrationTableError, summarize_table};

/// Request for a first-wave `fluxscale` solve.
#[derive(Debug, Clone)]
pub struct FluxScaleRequest {
    /// Input gain table produced by `gaincal`.
    pub input_table: PathBuf,
    /// Output flux-scaled gain table.
    pub output_table: PathBuf,
    /// Reference field selectors, by id, exact name, or simple `*` glob.
    pub reference_fields: Vec<String>,
    /// Transfer field selectors. Empty means all solved non-reference fields.
    pub transfer_fields: Vec<String>,
    /// Optional spectral-window remapping from transfer SPW id to reference SPW id.
    pub refspwmap: Vec<i32>,
    /// Optional fractional threshold around the median gain amplitude.
    pub gainthreshold: Option<f64>,
    /// Emit only multiplicative correction factors instead of a fully scaled table.
    pub incremental: bool,
}

/// Per-SPW flux result for one transfer field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluxScaleSpwResult {
    /// Flux density vector in CASA-style Stokes order.
    pub fluxd: [f64; 4],
    /// Flux-density errors in CASA-style Stokes order.
    pub fluxd_err: [f64; 4],
    /// Number of scalar gain samples contributing to the estimate.
    pub num_sol: [f64; 4],
}

/// Per-field `fluxscale` result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluxScaleFieldResult {
    /// Transfer-field name from the FIELD subtable.
    pub field_name: String,
    /// Per-SPW transfer flux results keyed by SPW id.
    pub spw_results: BTreeMap<i32, FluxScaleSpwResult>,
    /// Fitted reference frequency in Hz, or `0.0` when no single-SPW fit is available.
    pub fit_ref_frequency_hz: f64,
    /// Fitted Stokes-I flux density, or `0.0` when spectral fitting is deferred.
    pub fit_fluxd: f64,
    /// Fitted Stokes-I flux-density uncertainty.
    pub fit_fluxd_err: f64,
    /// Deferred spectral-index coefficients.
    pub spidx: Vec<f64>,
    /// Deferred spectral-index uncertainties.
    pub spidx_err: Vec<f64>,
    /// Deferred covariance matrix.
    pub covar_mat: Vec<Vec<f64>>,
}

/// Machine-readable `fluxscale` report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluxScaleReport {
    /// Output table path.
    pub output_table: PathBuf,
    /// Sorted transfer SPW ids represented in the report.
    pub spw_ids: Vec<i32>,
    /// SPW names aligned with [`Self::spw_ids`].
    pub spw_names: Vec<String>,
    /// Reference frequencies aligned with [`Self::spw_ids`].
    pub frequencies_hz: Vec<f64>,
    /// Transfer-field results keyed by FIELD_ID.
    pub fields: BTreeMap<i32, FluxScaleFieldResult>,
}

/// Errors returned by the limited `fluxscale` implementation.
#[derive(Debug, Error)]
pub enum FluxScaleError {
    /// Opening or validating the input caltable failed.
    #[error("failed to summarize input calibration table: {source}")]
    SummarizeInput {
        /// Underlying summary error.
        #[source]
        source: CalibrationTableError,
    },

    /// The input table is outside the first supported `fluxscale` surface.
    #[error("unsupported input calibration table {path}: {reason}")]
    UnsupportedInput {
        /// Input path.
        path: String,
        /// Error context.
        reason: String,
    },

    /// The request did not specify any reference fields.
    #[error("fluxscale requires at least one reference field selector")]
    MissingReferenceFields,

    /// A field selector did not match any field rows.
    #[error("field selector {selector:?} did not match any FIELD rows")]
    UnresolvedFieldSelector {
        /// Original selector.
        selector: String,
    },

    /// The transfer field selection produced no solved non-reference fields.
    #[error("fluxscale transfer field selection produced no solved non-reference fields")]
    EmptyTransferSelection,

    /// No usable gain samples were found for a required `(field, spw)` pair.
    #[error("no usable gain samples for field={field_id} spw={spw_id}")]
    MissingGainSamples {
        /// Field id.
        field_id: i32,
        /// SPW id.
        spw_id: i32,
    },

    /// The input gain amplitudes did not permit a stable scale factor.
    #[error("unstable fluxscale factor for field={field_id} spw={spw_id}")]
    UnstableScaleFactor {
        /// Field id.
        field_id: i32,
        /// SPW id.
        spw_id: i32,
    },

    /// An output path operation failed.
    #[error("failed to prepare output path {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },

    /// Copying the caltable tree failed.
    #[error("failed to copy calibration table {source_path} -> {destination_path}: {reason}")]
    CopyTableTree {
        /// Source path.
        source_path: String,
        /// Destination path.
        destination_path: String,
        /// Error context.
        reason: String,
    },

    /// Opening or mutating the copied output table failed.
    #[error("failed to mutate output calibration table {path}: {source}")]
    MutateOutput {
        /// Output path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },
}

#[derive(Debug, Clone)]
struct GainSampleRow {
    row_index: usize,
    field_id: i32,
    spw_id: i32,
    gains: ArrayD<Complex32>,
    flags: ArrayD<bool>,
}

#[derive(Debug, Clone)]
struct FieldInfo {
    name: String,
}

#[derive(Debug, Clone)]
struct SpwInfo {
    name: String,
    ref_frequency_hz: f64,
}

#[derive(Debug, Clone, Copy)]
struct FluxScaleEstimate {
    flux_density_jy: f64,
    flux_density_error_jy: f64,
    correction_factor: f32,
    num_sol: usize,
}

/// Execute a first-wave `fluxscale` request.
pub fn fluxscale(request: &FluxScaleRequest) -> Result<FluxScaleReport, FluxScaleError> {
    validate_request(request)?;
    let summary = summarize_table(&request.input_table)
        .map_err(|source| FluxScaleError::SummarizeInput { source })?;
    validate_input_table(&summary)?;

    let field_info = load_field_info(&request.input_table)?;
    let spw_info = load_spw_info(&request.input_table)?;
    let rows = load_gain_rows(&request.input_table)?;

    let reference_field_ids = resolve_field_selectors(&request.reference_fields, &field_info)?;
    let transfer_field_ids = resolve_transfer_fields(
        &request.transfer_fields,
        &reference_field_ids,
        &field_info,
        &rows,
    )?;

    let estimates = derive_estimates(
        &rows,
        &reference_field_ids,
        &transfer_field_ids,
        request.gainthreshold,
        &request.refspwmap,
    )?;

    write_scaled_output(
        &request.input_table,
        &request.output_table,
        &rows,
        &estimates,
        request.incremental,
    )?;
    Ok(build_report(
        &request.output_table,
        &transfer_field_ids,
        &field_info,
        &spw_info,
        &estimates,
    ))
}

fn validate_request(request: &FluxScaleRequest) -> Result<(), FluxScaleError> {
    if request.reference_fields.is_empty() {
        return Err(FluxScaleError::MissingReferenceFields);
    }
    Ok(())
}

fn validate_input_table(summary: &crate::CalibrationTableSummary) -> Result<(), FluxScaleError> {
    if summary.table_type != crate::constants::TABLE_INFO_TYPE {
        return Err(FluxScaleError::UnsupportedInput {
            path: summary.path.display().to_string(),
            reason: format!("table.info type is {:?}", summary.table_type),
        });
    }
    if summary.parameter_family != crate::CalibrationParameterFamily::Complex {
        return Err(FluxScaleError::UnsupportedInput {
            path: summary.path.display().to_string(),
            reason: "fluxscale currently supports only complex CPARAM tables".to_string(),
        });
    }
    match summary.keywords.vis_cal.as_deref() {
        Some("G Jones") | Some("T Jones") => Ok(()),
        other => Err(FluxScaleError::UnsupportedInput {
            path: summary.path.display().to_string(),
            reason: format!("expected VisCal to be G Jones or T Jones, found {other:?}"),
        }),
    }
}

fn load_field_info(root: &Path) -> Result<HashMap<i32, FieldInfo>, FluxScaleError> {
    let table = Table::open(TableOptions::new(root.join("FIELD"))).map_err(|source| {
        FluxScaleError::MutateOutput {
            path: root.join("FIELD").display().to_string(),
            source: Box::new(source),
        }
    })?;
    let mut out = HashMap::new();
    for row in 0..table.row_count() {
        let name = get_string(&table, row, "NAME")?;
        out.insert(row as i32, FieldInfo { name });
    }
    Ok(out)
}

fn load_spw_info(root: &Path) -> Result<HashMap<i32, SpwInfo>, FluxScaleError> {
    let table = Table::open(TableOptions::new(root.join("SPECTRAL_WINDOW"))).map_err(|source| {
        FluxScaleError::MutateOutput {
            path: root.join("SPECTRAL_WINDOW").display().to_string(),
            source: Box::new(source),
        }
    })?;
    let mut out = HashMap::new();
    for row in 0..table.row_count() {
        let ref_frequency_hz = get_ref_frequency(&table, row)?;
        let name = get_string(&table, row, "NAME").unwrap_or_default();
        out.insert(
            row as i32,
            SpwInfo {
                name,
                ref_frequency_hz,
            },
        );
    }
    Ok(out)
}

fn load_gain_rows(root: &Path) -> Result<Vec<GainSampleRow>, FluxScaleError> {
    let table =
        Table::open(TableOptions::new(root)).map_err(|source| FluxScaleError::MutateOutput {
            path: root.display().to_string(),
            source: Box::new(source),
        })?;
    let mut rows = Vec::new();
    for row_index in 0..table.row_count() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let spw_id = get_i32(&table, row_index, COL_SPECTRAL_WINDOW_ID)?;
        let gains = get_complex_array(&table, row_index, COL_CPARAM)?;
        let flags = get_bool_array(&table, row_index, COL_FLAG)?;
        rows.push(GainSampleRow {
            row_index,
            field_id,
            spw_id,
            gains,
            flags,
        });
    }
    Ok(rows)
}

fn resolve_field_selectors(
    selectors: &[String],
    field_info: &HashMap<i32, FieldInfo>,
) -> Result<BTreeSet<i32>, FluxScaleError> {
    let mut resolved = BTreeSet::new();
    for selector in selectors {
        let selector = selector.trim();
        if selector.is_empty() {
            continue;
        }
        for part in selector
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            if let Ok(field_id) = part.parse::<i32>() {
                if field_info.contains_key(&field_id) {
                    resolved.insert(field_id);
                    continue;
                }
                return Err(FluxScaleError::UnresolvedFieldSelector {
                    selector: part.to_string(),
                });
            }
            let mut matched = false;
            for (&field_id, info) in field_info {
                if matches_simple_glob(part, &info.name) {
                    matched = true;
                    resolved.insert(field_id);
                }
            }
            if !matched {
                return Err(FluxScaleError::UnresolvedFieldSelector {
                    selector: part.to_string(),
                });
            }
        }
    }
    Ok(resolved)
}

fn resolve_transfer_fields(
    selectors: &[String],
    reference_field_ids: &BTreeSet<i32>,
    field_info: &HashMap<i32, FieldInfo>,
    rows: &[GainSampleRow],
) -> Result<BTreeSet<i32>, FluxScaleError> {
    let solved_fields = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let mut transfer = if selectors.is_empty() {
        solved_fields
            .difference(reference_field_ids)
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        resolve_field_selectors(selectors, field_info)?
    };
    transfer.retain(|field_id| {
        !reference_field_ids.contains(field_id) && solved_fields.contains(field_id)
    });
    if transfer.is_empty() {
        return Err(FluxScaleError::EmptyTransferSelection);
    }
    Ok(transfer)
}

fn derive_estimates(
    rows: &[GainSampleRow],
    reference_field_ids: &BTreeSet<i32>,
    transfer_field_ids: &BTreeSet<i32>,
    gainthreshold: Option<f64>,
    refspwmap: &[i32],
) -> Result<HashMap<(i32, i32), FluxScaleEstimate>, FluxScaleError> {
    let mut amplitudes = HashMap::<(i32, i32), Vec<f64>>::new();

    for row in rows {
        let key = (row.field_id, row.spw_id);
        let mut values = row_amplitudes(row);
        if values.is_empty() {
            continue;
        }
        amplitudes.entry(key).or_default().append(&mut values);
    }

    let mut filtered = HashMap::<(i32, i32), Vec<f64>>::new();
    for (key, values) in amplitudes {
        filtered.insert(key, apply_gainthreshold(values, gainthreshold));
    }

    let mut reference_means = HashMap::<i32, f64>::new();
    for field_id in reference_field_ids {
        for (&(candidate_field_id, spw_id), values) in &filtered {
            if candidate_field_id != *field_id || values.is_empty() {
                continue;
            }
            reference_means.entry(spw_id).or_default();
        }
    }
    for (spw_id, mean) in reference_means.iter_mut() {
        let pooled = reference_field_ids
            .iter()
            .filter_map(|field_id| filtered.get(&(*field_id, *spw_id)))
            .flat_map(|values| values.iter().copied())
            .collect::<Vec<_>>();
        if pooled.is_empty() {
            return Err(FluxScaleError::MissingGainSamples {
                field_id: *reference_field_ids.iter().next().unwrap_or(&-1),
                spw_id: *spw_id,
            });
        }
        *mean = pooled.iter().sum::<f64>() / pooled.len() as f64;
    }

    let mut estimates = HashMap::new();
    for field_id in transfer_field_ids {
        let field_spws = filtered
            .keys()
            .filter_map(|(candidate_field_id, spw_id)| {
                (*candidate_field_id == *field_id).then_some(*spw_id)
            })
            .collect::<BTreeSet<_>>();
        for spw_id in field_spws {
            let values =
                filtered
                    .get(&(*field_id, spw_id))
                    .ok_or(FluxScaleError::MissingGainSamples {
                        field_id: *field_id,
                        spw_id,
                    })?;
            if values.is_empty() {
                return Err(FluxScaleError::MissingGainSamples {
                    field_id: *field_id,
                    spw_id,
                });
            }
            let reference_spw_id = mapped_reference_spw(spw_id, refspwmap);
            let reference_mean = *reference_means.get(&reference_spw_id).ok_or(
                FluxScaleError::MissingGainSamples {
                    field_id: *reference_field_ids.iter().next().unwrap_or(&-1),
                    spw_id: reference_spw_id,
                },
            )?;
            let transfer_mean = values.iter().sum::<f64>() / values.len() as f64;
            if !(reference_mean.is_finite()
                && transfer_mean.is_finite()
                && reference_mean > 0.0
                && transfer_mean > 0.0)
            {
                return Err(FluxScaleError::UnstableScaleFactor {
                    field_id: *field_id,
                    spw_id,
                });
            }

            let ratio = transfer_mean / reference_mean;
            let flux_density_jy = ratio * ratio;
            let correction_factor = (1.0 / ratio) as f32;
            let flux_density_error_jy = estimate_flux_error(values, reference_mean);
            estimates.insert(
                (*field_id, spw_id),
                FluxScaleEstimate {
                    flux_density_jy,
                    flux_density_error_jy,
                    correction_factor,
                    num_sol: values.len(),
                },
            );
        }
    }

    Ok(estimates)
}

fn row_amplitudes(row: &GainSampleRow) -> Vec<f64> {
    if row.gains.ndim() != row.flags.ndim() {
        return Vec::new();
    }
    row.gains
        .iter()
        .zip(row.flags.iter())
        .filter_map(|(gain, flagged)| {
            (!*flagged && gain.norm() > f32::EPSILON).then_some(gain.norm() as f64)
        })
        .collect()
}

fn apply_gainthreshold(mut values: Vec<f64>, gainthreshold: Option<f64>) -> Vec<f64> {
    let Some(gainthreshold) = gainthreshold else {
        return values;
    };
    if gainthreshold < 0.0 || values.is_empty() {
        return values;
    }
    values.sort_by(f64::total_cmp);
    let median = median(&values);
    values
        .into_iter()
        .filter(|value| (value - median).abs() <= gainthreshold * median)
        .collect()
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn mapped_reference_spw(spw_id: i32, refspwmap: &[i32]) -> i32 {
    let Ok(index) = usize::try_from(spw_id) else {
        return spw_id;
    };
    match refspwmap.get(index).copied() {
        Some(mapped) if mapped >= 0 => mapped,
        _ => spw_id,
    }
}

fn estimate_flux_error(transfer_values: &[f64], reference_mean: f64) -> f64 {
    if transfer_values.len() <= 1 || reference_mean <= 0.0 {
        return 0.0;
    }
    let factors = transfer_values
        .iter()
        .map(|value| (value / reference_mean).powi(2))
        .collect::<Vec<_>>();
    let mean = factors.iter().sum::<f64>() / factors.len() as f64;
    let variance = factors
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / (factors.len() as f64);
    variance.sqrt() / (factors.len() as f64).sqrt()
}

fn write_scaled_output(
    input_root: &Path,
    output_root: &Path,
    rows: &[GainSampleRow],
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
    incremental: bool,
) -> Result<(), FluxScaleError> {
    prepare_output_root(input_root, output_root)?;
    copy_table_tree(input_root, output_root)?;
    let mut table = Table::open(TableOptions::new(output_root)).map_err(|source| {
        FluxScaleError::MutateOutput {
            path: output_root.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for row in rows {
        let Some(estimate) = estimates.get(&(row.field_id, row.spw_id)) else {
            continue;
        };
        let scaled = scale_gains(&row.gains, estimate.correction_factor, incremental);
        table
            .set_cell(
                row.row_index,
                COL_CPARAM,
                Value::Array(ArrayValue::Complex32(scaled)),
            )
            .map_err(|source| FluxScaleError::MutateOutput {
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
    }
    table
        .save(TableOptions::new(output_root))
        .map_err(|source| FluxScaleError::MutateOutput {
            path: output_root.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

fn scale_gains(
    values: &ArrayD<Complex32>,
    correction_factor: f32,
    incremental: bool,
) -> ArrayD<Complex32> {
    if incremental {
        ArrayD::from_elem(values.raw_dim(), Complex32::new(correction_factor, 0.0))
    } else {
        values.mapv(|value| value * correction_factor)
    }
}

fn build_report(
    output_table: &Path,
    transfer_field_ids: &BTreeSet<i32>,
    field_info: &HashMap<i32, FieldInfo>,
    spw_info: &HashMap<i32, SpwInfo>,
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
) -> FluxScaleReport {
    let mut fields = BTreeMap::new();
    let mut spw_ids = BTreeSet::new();

    for field_id in transfer_field_ids {
        let mut spw_results = BTreeMap::new();
        let mut single_spw = None;
        for (&(candidate_field_id, spw_id), estimate) in estimates {
            if candidate_field_id != *field_id {
                continue;
            }
            spw_ids.insert(spw_id);
            spw_results.insert(
                spw_id,
                FluxScaleSpwResult {
                    fluxd: [estimate.flux_density_jy, 0.0, 0.0, 0.0],
                    fluxd_err: [estimate.flux_density_error_jy, 0.0, 0.0, 0.0],
                    num_sol: [estimate.num_sol as f64, 0.0, 0.0, 0.0],
                },
            );
            single_spw = Some((spw_id, estimate));
        }

        let (fit_ref_frequency_hz, fit_fluxd, fit_fluxd_err) = if spw_results.len() == 1 {
            let (spw_id, estimate) = single_spw.expect("single spw estimate");
            (
                spw_info
                    .get(&spw_id)
                    .map(|info| info.ref_frequency_hz)
                    .unwrap_or(0.0),
                estimate.flux_density_jy,
                0.0,
            )
        } else {
            (0.0, 0.0, 0.0)
        };

        fields.insert(
            *field_id,
            FluxScaleFieldResult {
                field_name: field_info
                    .get(field_id)
                    .map(|info| info.name.clone())
                    .unwrap_or_default(),
                spw_results,
                fit_ref_frequency_hz,
                fit_fluxd,
                fit_fluxd_err,
                spidx: vec![0.0, 0.0],
                spidx_err: vec![0.0, 0.0],
                covar_mat: Vec::new(),
            },
        );
    }

    let sorted_spw_ids = spw_ids.into_iter().collect::<Vec<_>>();
    let spw_names = sorted_spw_ids
        .iter()
        .map(|spw_id| {
            spw_info
                .get(spw_id)
                .map(|info| info.name.clone())
                .unwrap_or_default()
        })
        .collect();
    let frequencies_hz = sorted_spw_ids
        .iter()
        .map(|spw_id| {
            spw_info
                .get(spw_id)
                .map(|info| info.ref_frequency_hz)
                .unwrap_or(0.0)
        })
        .collect();

    FluxScaleReport {
        output_table: output_table.to_path_buf(),
        spw_ids: sorted_spw_ids,
        spw_names,
        frequencies_hz,
        fields,
    }
}

fn prepare_output_root(input_root: &Path, output_root: &Path) -> Result<(), FluxScaleError> {
    if input_root == output_root {
        return Err(FluxScaleError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: "input_table and output_table must differ".to_string(),
        });
    }
    if output_root.exists() {
        fs::remove_dir_all(output_root)
            .or_else(|_| fs::remove_file(output_root))
            .map_err(|error| FluxScaleError::PrepareOutput {
                path: output_root.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = output_root.parent() {
        fs::create_dir_all(parent).map_err(|error| FluxScaleError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

fn copy_table_tree(source: &Path, destination: &Path) -> Result<(), FluxScaleError> {
    if source.is_dir() {
        fs::create_dir_all(destination).map_err(|error| FluxScaleError::CopyTableTree {
            source_path: source.display().to_string(),
            destination_path: destination.display().to_string(),
            reason: error.to_string(),
        })?;
        for entry in fs::read_dir(source).map_err(|error| FluxScaleError::CopyTableTree {
            source_path: source.display().to_string(),
            destination_path: destination.display().to_string(),
            reason: error.to_string(),
        })? {
            let entry = entry.map_err(|error| FluxScaleError::CopyTableTree {
                source_path: source.display().to_string(),
                destination_path: destination.display().to_string(),
                reason: error.to_string(),
            })?;
            copy_table_tree(&entry.path(), &destination.join(entry.file_name()))?;
        }
        return Ok(());
    }
    fs::copy(source, destination).map_err(|error| FluxScaleError::CopyTableTree {
        source_path: source.display().to_string(),
        destination_path: destination.display().to_string(),
        reason: error.to_string(),
    })?;
    Ok(())
}

fn get_i32(table: &Table, row: usize, column: &str) -> Result<i32, FluxScaleError> {
    match table
        .cell(row, column)
        .map_err(|source| FluxScaleError::MutateOutput {
            path: column.to_string(),
            source: Box::new(source),
        })? {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(other) => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!("expected Int32 in {column}, found {:?}", other.kind()),
        }),
        None => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!("missing required column {column}"),
        }),
    }
}

fn get_string(table: &Table, row: usize, column: &str) -> Result<String, FluxScaleError> {
    match table
        .cell(row, column)
        .map_err(|source| FluxScaleError::MutateOutput {
            path: column.to_string(),
            source: Box::new(source),
        })? {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.clone()),
        Some(other) => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!("expected String in {column}, found {:?}", other.kind()),
        }),
        None => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!("missing required column {column}"),
        }),
    }
}

fn get_ref_frequency(table: &Table, row: usize) -> Result<f64, FluxScaleError> {
    let chan_freq =
        table
            .get_array_cell(row, "CHAN_FREQ")
            .map_err(|source| FluxScaleError::MutateOutput {
                path: "CHAN_FREQ".to_string(),
                source: Box::new(source),
            })?;
    match chan_freq {
        ArrayValue::Float64(values) if !values.is_empty() => {
            Ok(values.iter().copied().sum::<f64>() / values.len() as f64)
        }
        _ => match table.cell(row, "REF_FREQUENCY").map_err(|source| {
            FluxScaleError::MutateOutput {
                path: "REF_FREQUENCY".to_string(),
                source: Box::new(source),
            }
        })? {
            Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
            Some(Value::Array(ArrayValue::Float64(values))) if values.len() == 1 => {
                Ok(values.iter().copied().next().unwrap_or(0.0))
            }
            Some(_) | None => Ok(0.0),
        },
    }
}

fn get_complex_array(
    table: &Table,
    row: usize,
    column: &str,
) -> Result<ArrayD<Complex32>, FluxScaleError> {
    match table
        .get_array_cell(row, column)
        .map_err(|source| FluxScaleError::MutateOutput {
            path: column.to_string(),
            source: Box::new(source),
        })? {
        ArrayValue::Complex32(values) => Ok(values.clone()),
        other => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!(
                "expected Complex32 array in {column}, found {:?}",
                other.primitive_type()
            ),
        }),
    }
}

fn get_bool_array(table: &Table, row: usize, column: &str) -> Result<ArrayD<bool>, FluxScaleError> {
    match table
        .get_array_cell(row, column)
        .map_err(|source| FluxScaleError::MutateOutput {
            path: column.to_string(),
            source: Box::new(source),
        })? {
        ArrayValue::Bool(values) => Ok(values.clone()),
        other => Err(FluxScaleError::UnsupportedInput {
            path: column.to_string(),
            reason: format!(
                "expected Bool array in {column}, found {:?}",
                other.primitive_type()
            ),
        }),
    }
}

fn matches_simple_glob(pattern: &str, candidate: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == candidate;
    }
    let mut rest = candidate;
    let parts = pattern.split('*').collect::<Vec<_>>();
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');

    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 && anchored_start {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            continue;
        }
        if index == parts.len() - 1 && anchored_end {
            return rest.ends_with(part);
        }
        let Some(position) = rest.find(part) else {
            return false;
        };
        rest = &rest[position + part.len()..];
    }

    true
}
