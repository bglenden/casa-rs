// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `fluxscale`: reference/transfer gain ratios, calibration-row selections,
//! flux-density estimates and polynomial spectral fits. Full and incremental
//! calibration products preserve CASA/casacore interchange.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::ArrayD;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::COL_CPARAM;
use crate::summary::CalibrationTableError;

mod fit;
mod full_output;
pub(crate) mod history;
mod incremental;
mod input;
mod listing;
mod selection;
mod stats;

/// Request for a CASA flux-scale transfer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FluxScaleRequest {
    /// Selection and MeasurementSet metadata used by the CASA task surface.
    #[serde(flatten)]
    pub options: FluxScaleOptions,
    /// Input gain table produced by `gaincal`.
    pub input_table: PathBuf,
    /// Output flux-scaled gain table.
    pub output_table: PathBuf,
    /// Reference fields in CASA FIELD selection grammar.
    pub reference_fields: Vec<String>,
    /// Transfer field selectors. Empty means all solved non-reference fields.
    pub transfer_fields: Vec<String>,
    /// Optional spectral-window remapping from transfer SPW id to reference SPW id.
    pub refspwmap: Vec<i32>,
    /// Optional fractional threshold around the median gain amplitude.
    pub gainthreshold: Option<f64>,
    /// Emit only multiplicative correction factors instead of a fully scaled table.
    pub incremental: bool,
    /// Append compatible solutions to an existing flux table.
    #[serde(default)]
    pub append: bool,
}

/// Optional metadata and solution selections for flux-scale estimation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct FluxScaleOptions {
    /// CASA resolves reference/transfer field names against this MeasurementSet.
    /// Native table-only callers may use the gain table's FIELD metadata.
    pub measurement_set: Option<PathBuf>,
    /// CASA antenna/baseline expression on the calibration table.
    pub antenna: String,
    /// CASA time expression on calibration solution times.
    pub timerange: String,
    /// CASA scan expression on calibration solution scan numbers.
    pub scan: String,
    /// Polynomial degree in log-frequency/log-flux space (CASA default 1).
    pub fitorder: i32,
    /// Optional text report, overwritten for each successful invocation.
    pub listfile: Option<PathBuf>,
    /// CASA 6.7.6's display hooks are empty; this requests the same no-plot behavior.
    pub display: bool,
}

impl Default for FluxScaleOptions {
    fn default() -> Self {
        Self {
            measurement_set: None,
            antenna: String::new(),
            timerange: String::new(),
            scan: String::new(),
            fitorder: 1,
            listfile: None,
            display: false,
        }
    }
}

/// Per-SPW flux result for one transfer field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FluxScaleSpwResult {
    /// Flux density vector in CASA-style Stokes order.
    pub fluxd: [f64; 4],
    /// Flux-density errors in CASA-style Stokes order.
    pub fluxd_err: [f64; 4],
    /// Number of matched antenna/polarization mean gain ratios.
    pub num_sol: [f64; 4],
}

/// Per-field `fluxscale` result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FluxScaleFieldResult {
    /// Transfer-field name from the FIELD subtable.
    pub field_name: String,
    /// Per-SPW transfer flux results keyed by SPW id.
    pub spw_results: BTreeMap<i32, FluxScaleSpwResult>,
    /// The fit's reference frequency in Hz, the windows' geometric mean
    /// frequency, or the one window's frequency.
    pub fit_ref_frequency_hz: f64,
    /// Fitted Stokes-I flux density at the reference frequency, or the one
    /// window's flux density.
    pub fit_fluxd: f64,
    /// Fitted Stokes-I flux-density uncertainty.
    pub fit_fluxd_err: f64,
    /// The fit's coefficients, `log10 S = spidx[0] + spidx[1] log10(f / f0)`,
    /// as CASA's `fitorder=1` fits them; zero for one window.
    pub spidx: Vec<f64>,
    /// The coefficients' uncertainties.
    pub spidx_err: Vec<f64>,
    /// The fit's covariance matrix, empty for one window.
    pub covar_mat: Vec<Vec<f64>>,
}

/// Machine-readable `fluxscale` report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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

/// Errors returned by flux-scale estimation and persistence.
#[derive(Debug, Error)]
pub enum FluxScaleError {
    /// Staging or publishing the complete scientific product failed.
    #[error(transparent)]
    CalibrationTable(#[from] crate::CalibrationTableWriteError),
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
    antenna_id: i32,
    antenna2_id: i32,
    time_seconds: f64,
    interval_seconds: f64,
    scan_number: i32,
    gains: ArrayD<Complex32>,
    flags: ArrayD<bool>,
}

#[derive(Debug, Clone)]
struct FieldInfo {
    name: String,
    code: Option<String>,
    flagged: bool,
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

/// Estimate transfer flux densities and write the corresponding gain corrections.
pub fn fluxscale(request: &FluxScaleRequest) -> Result<FluxScaleReport, FluxScaleError> {
    fluxscale_with_output_mode(
        request,
        if request.append {
            crate::CalibrationOutputMode::Append
        } else {
            crate::CalibrationOutputMode::Create
        },
    )
}

pub(crate) fn fluxscale_with_output_mode(
    request: &FluxScaleRequest,
    mode: crate::CalibrationOutputMode,
) -> Result<FluxScaleReport, FluxScaleError> {
    let input_phase = crate::timing::Phase::start("fluxscale_read");
    validate_request(request)?;
    let input = Table::open(TableOptions::new(&request.input_table)).map_err(|error| {
        FluxScaleError::UnsupportedInput {
            path: request.input_table.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    validate_input_table(&input, &request.input_table)?;

    let field_root = request
        .options
        .measurement_set
        .as_deref()
        .unwrap_or(&request.input_table);
    if let Some(vis) = &request.options.measurement_set {
        casa_ms::ms::MeasurementSet::open(vis).map_err(|error| {
            FluxScaleError::UnsupportedInput {
                path: vis.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    }
    let field_info = load_field_info(field_root)?;
    let spw_info = load_spw_info(&request.input_table)?;
    let rows = input::read(&input, &request.input_table)?;

    let reference_field_ids = resolve_field_selectors(&request.reference_fields, &field_info)?;
    let transfer_field_ids = resolve_transfer_fields(
        &request.transfer_fields,
        &reference_field_ids,
        &field_info,
        &rows,
    )?;
    let selected_fields = reference_field_ids
        .union(&transfer_field_ids)
        .copied()
        .collect();
    let masks = selection::sample_masks(
        &request.input_table,
        &rows,
        &request.options,
        &selected_fields,
    )?;

    drop(input_phase);
    let compute_phase = crate::timing::Phase::start("fluxscale_estimate");
    let estimates = stats::derive_selected_estimates(
        &rows,
        &reference_field_ids,
        &transfer_field_ids,
        request.gainthreshold,
        &request.refspwmap,
        &masks,
    )?;

    drop(compute_phase);
    let write_phase = crate::timing::Phase::start("fluxscale_write");
    write_scaled_output(request, &input, &rows, &estimates, mode, &masks)?;
    drop(write_phase);
    let report = build_report(
        request,
        &transfer_field_ids,
        &field_info,
        &spw_info,
        &estimates,
        &rows,
        &masks,
    );
    if let Some(path) = &request.options.listfile {
        listing::write(path, request, &report)?;
    }
    if request.options.display {
        tracing::debug!("CASA fluxscale display hooks do not produce a plot");
    }
    Ok(report)
}

fn validate_request(request: &FluxScaleRequest) -> Result<(), FluxScaleError> {
    if request.reference_fields.is_empty() {
        return Err(FluxScaleError::MissingReferenceFields);
    }
    listing::validate(request)?;
    Ok(())
}

fn validate_input_table(table: &Table, path: &Path) -> Result<(), FluxScaleError> {
    let keyword = |name| match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.as_str()),
        _ => None,
    };
    if table.info().table_type != crate::constants::TABLE_INFO_TYPE {
        return Err(FluxScaleError::UnsupportedInput {
            path: path.display().to_string(),
            reason: format!("table.info type is {:?}", table.info().table_type),
        });
    }
    if keyword("ParType") == Some("Float")
        || !table
            .schema()
            .is_some_and(|schema| schema.contains_column(COL_CPARAM))
    {
        return Err(FluxScaleError::UnsupportedInput {
            path: path.display().to_string(),
            reason: "fluxscale currently supports only complex CPARAM tables".to_string(),
        });
    }
    match keyword("VisCal") {
        Some("G Jones") | Some("T Jones") => Ok(()),
        other => Err(FluxScaleError::UnsupportedInput {
            path: path.display().to_string(),
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
        let code = get_string(&table, row, "CODE").ok();
        let flagged = matches!(
            table
                .cell_accessor(row, "FLAG_ROW")
                .and_then(|cell| cell.value()),
            Ok(Some(Value::Scalar(ScalarValue::Bool(true))))
        );
        out.insert(
            row as i32,
            FieldInfo {
                name,
                code,
                flagged,
            },
        );
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

fn resolve_field_selectors(
    selectors: &[String],
    field_info: &HashMap<i32, FieldInfo>,
) -> Result<BTreeSet<i32>, FluxScaleError> {
    let fields = (0..field_info.len())
        .map(|id| casa_ms::selection::FieldSelectorRow {
            name: &field_info[&(id as i32)].name,
            code: field_info[&(id as i32)].code.as_deref(),
            flagged: field_info[&(id as i32)].flagged,
        })
        .collect::<Vec<_>>();
    casa_ms::selection::resolve_field_selector_rows(&fields, &selectors.join(","))
        .map(|ids| ids.into_iter().collect())
        .map_err(|error| FluxScaleError::UnsupportedInput {
            path: "reference/transfer".into(),
            reason: error.to_string(),
        })
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

fn write_scaled_output(
    request: &FluxScaleRequest,
    input: &Table,
    rows: &[GainSampleRow],
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
    mode: crate::CalibrationOutputMode,
    masks: &selection::SampleMasks,
) -> Result<(), FluxScaleError> {
    let input_root = &request.input_table;
    let output_root = &request.output_table;
    let incremental = request.incremental;
    if crate::paths::overlap(input_root, output_root).map_err(|e| {
        FluxScaleError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: e.to_string(),
        }
    })? {
        return Err(FluxScaleError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: "input_table and output_table must differ".to_string(),
        });
    }
    if mode == crate::CalibrationOutputMode::Create && output_root.exists() {
        return Err(FluxScaleError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: "fluxtable exists; please specify a different name or append=True".to_string(),
        });
    }
    let target = crate::output::CalibrationOutput::prepare(output_root, mode)?;
    crate::output_merge::preserve_subtables(input_root, &target.staging)?;
    let sorted =
        !incremental && mode != crate::CalibrationOutputMode::Append && full_output::ordered(rows);
    if sorted && full_output::save(input, input_root, &target.staging, rows, estimates)? {
        mark_output_windows(&target.staging, rows, false)?;
        target.commit()?;
        return Ok(());
    }
    let mut table = if incremental {
        empty_output(input)
    } else {
        input
            .to_memory()
            .map_err(|source| FluxScaleError::MutateOutput {
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?
    };
    for name in crate::constants::STANDARD_SUBTABLE_KEYWORDS {
        table.keywords_mut().upsert(
            *name,
            Value::table_ref(casa_tables::strip_directory(
                &target.staging.join(name),
                &target.staging,
            )),
        );
    }
    if incremental {
        let metadata = request
            .options
            .measurement_set
            .as_deref()
            .unwrap_or(input_root);
        if let Some(vis) = &request.options.measurement_set {
            table.keywords_mut().upsert(
                "MSName",
                Value::Scalar(ScalarValue::String(
                    vis.file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned(),
                )),
            );
        }
        incremental::populate(&mut table, metadata, rows, estimates, masks)?;
        incremental::copy_metadata(metadata, &target.staging)?;
    } else {
        for row in rows {
            let Some(estimate) = estimates.get(&(row.field_id, row.spw_id)) else {
                continue;
            };
            let scaled = row.gains.mapv(|value| value * estimate.correction_factor);
            table
                .cell_accessor_mut(row.row_index, COL_CPARAM)
                .and_then(|mut cell| cell.set(Value::Array(ArrayValue::Complex32(scaled))))
                .map_err(|source| FluxScaleError::MutateOutput {
                    path: output_root.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    if !incremental && rows.len() != table.row_count() {
        let selected = rows
            .iter()
            .map(|row| row.row_index)
            .collect::<BTreeSet<_>>();
        let removed = (0..table.row_count())
            .filter(|index| !selected.contains(index))
            .collect::<Vec<_>>();
        table
            .remove_rows(&removed)
            .map_err(|source| FluxScaleError::MutateOutput {
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
    }
    mark_output_windows(&target.staging, rows, incremental)?;
    if mode == crate::CalibrationOutputMode::Append && output_root.exists() {
        let spw = crate::output_merge::merge_spw(output_root, &target.staging)?;
        let mut old = crate::output_merge::open(output_root)?;
        crate::output_merge::append(&mut old, &table, output_root)?;
        crate::output_merge::preserve_subtables(output_root, &target.staging)?;
        spw.save(TableOptions::new(target.staging.join("SPECTRAL_WINDOW")))
            .map_err(|e| crate::output_merge::invalid(output_root, e.to_string()))?;
        table = old;
    }
    if sorted {
        table
            .save(
                TableOptions::new(&target.staging)
                    .with_data_manager(casa_tables::DataManagerKind::StandardStMan),
            )
            .map_err(|error| crate::output_merge::invalid(output_root, error.to_string()))?;
    } else {
        crate::output_merge::save_sorted(&mut table, &target.staging)?;
    }
    target.commit()?;
    Ok(())
}

fn empty_output(input: &Table) -> Table {
    let mut table =
        Table::with_schema_memory(input.schema().expect("validated gain schema").clone());
    table.set_info(input.info().clone());
    *table.keywords_mut() = input.keywords().clone();
    for column in input.schema().expect("validated gain schema").columns() {
        if let Some(keywords) = input.column_keywords(column.name()) {
            table.set_column_keywords(column.name(), keywords.clone());
        }
    }
    table
}

fn mark_output_windows(
    stage: &Path,
    rows: &[GainSampleRow],
    incremental: bool,
) -> Result<(), FluxScaleError> {
    let path = stage.join("SPECTRAL_WINDOW");
    let failed = |error: TableError| crate::output_merge::invalid(&path, error.to_string());
    let mut table = Table::open(TableOptions::new(&path)).map_err(failed)?;
    let used = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    for index in 0..table.row_count() {
        table
            .cell_accessor_mut(index, "FLAG_ROW")
            .and_then(|mut cell| {
                cell.set(Value::Scalar(ScalarValue::Bool(
                    !incremental && !used.contains(&(index as i32)),
                )))
            })
            .map_err(failed)?;
    }
    table.save(TableOptions::new(&path)).map_err(failed)?;
    Ok(())
}

fn build_report(
    request: &FluxScaleRequest,
    transfer_field_ids: &BTreeSet<i32>,
    field_info: &HashMap<i32, FieldInfo>,
    spw_info: &HashMap<i32, SpwInfo>,
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
    rows: &[GainSampleRow],
    masks: &selection::SampleMasks,
) -> FluxScaleReport {
    let mut fields = BTreeMap::new();
    let mut spw_ids = BTreeSet::new();

    for field_id in transfer_field_ids {
        let mut spw_results = BTreeMap::new();
        let mut windows = Vec::new();
        for &spw_id in spw_info.keys() {
            spw_ids.insert(spw_id);
            let Some(estimate) = estimates.get(&(*field_id, spw_id)) else {
                spw_results.insert(
                    spw_id,
                    FluxScaleSpwResult {
                        fluxd: [-1.; 4],
                        fluxd_err: [-1.; 4],
                        num_sol: [-1.; 4],
                    },
                );
                continue;
            };
            spw_results.insert(
                spw_id,
                FluxScaleSpwResult {
                    fluxd: [estimate.flux_density_jy, 0.0, 0.0, 0.0],
                    fluxd_err: [estimate.flux_density_error_jy, 0.0, 0.0, 0.0],
                    num_sol: [estimate.num_sol as f64, 0.0, 0.0, 0.0],
                },
            );
            windows.push(fit::WindowFlux {
                frequency_hz: spw_info
                    .get(&spw_id)
                    .map(|info| info.ref_frequency_hz)
                    .unwrap_or(0.0),
                flux_density_jy: estimate.flux_density_jy,
                flux_density_error_jy: estimate.flux_density_error_jy,
            });
        }
        windows.sort_by(|a, b| a.frequency_hz.total_cmp(&b.frequency_hz));
        let fit = fit::spectral_fit_order(&windows, request.options.fitorder);

        fields.insert(
            *field_id,
            FluxScaleFieldResult {
                field_name: field_info
                    .get(field_id)
                    .map(|info| info.name.clone())
                    .unwrap_or_default(),
                spw_results,
                fit_ref_frequency_hz: fit.ref_frequency_hz,
                fit_fluxd: fit.fluxd,
                fit_fluxd_err: fit.fluxd_err,
                spidx: fit.spidx,
                spidx_err: fit.spidx_err,
                covar_mat: fit.covariance,
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
            if !rows
                .iter()
                .zip(&masks.data)
                .any(|(row, selected)| *selected && row.spw_id == *spw_id)
            {
                return -1.;
            }
            spw_info
                .get(spw_id)
                .map(|info| info.ref_frequency_hz)
                .unwrap_or(0.0)
        })
        .collect();

    FluxScaleReport {
        output_table: request.output_table.clone(),
        spw_ids: sorted_spw_ids,
        spw_names,
        frequencies_hz,
        fields,
    }
}

fn get_string(table: &Table, row: usize, column: &str) -> Result<String, FluxScaleError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value())
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
    let chan_freq = table
        .cell_accessor(row, "CHAN_FREQ")
        .and_then(|cell| cell.array())
        .map_err(|source| FluxScaleError::MutateOutput {
            path: "CHAN_FREQ".to_string(),
            source: Box::new(source),
        })?;
    // CASA's solution frequency, `solFreq`, is the window's middle channel.
    match chan_freq {
        ArrayValue::Float64(values) if !values.is_empty() => {
            Ok(values.iter().copied().nth(values.len() / 2).unwrap_or(0.0))
        }
        _ => match table
            .cell_accessor(row, "REF_FREQUENCY")
            .and_then(|cell| cell.value())
            .map_err(|source| FluxScaleError::MutateOutput {
                path: "REF_FREQUENCY".to_string(),
                source: Box::new(source),
            })? {
            Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
            Some(Value::Array(ArrayValue::Float64(values))) if values.len() == 1 => {
                Ok(values.iter().copied().next().unwrap_or(0.0))
            }
            Some(_) | None => Ok(0.0),
        },
    }
}
