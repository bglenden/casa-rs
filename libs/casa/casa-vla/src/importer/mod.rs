// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native `importvla`-style MeasurementSet writer for disk archives.

use std::collections::{BTreeMap, HashMap};
use std::f64::consts::PI;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::{MPosition, MeasFrame, MeasuresProvider};
use casa_core::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_ms::MeasurementSet;
use casa_ms::MeasurementSetBuilder;
use casa_ms::observation_io::creation::{
    Msv2CreationRow, Msv2MainWriter, msv2_array_columns, msv2_main_plan,
};
use casa_ms::schema::SubtableId;
use casa_ms::{
    MeasurementSetArrayShapePlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    OptionalMainColumn, SubTable,
};
use casa_tables::{ColumnSchema, Table};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::disk::LogicalRecord;
use crate::output::ImportOutput;
use crate::record::{
    CdaId, DirectionEpoch, DopplerDefinition, FrequencyFrame, IfId, IfUsage, StokesProduct,
};
use crate::selection::ArchiveSelection;
use crate::{AntennaNameScheme, ImportVlaFormat, ImportVlaOptions, VlaDiskReader, VlaError};

const DEFAULT_FLAG_CATEGORIES: usize = 6;
const DEFAULT_TELESCOPE_NAME: &str = "VLA";
const VLA_SUBARRAY_COUNT: i32 = 5;
const SECONDS_PER_JULIAN_YEAR: f64 = 365.25 * 86_400.0;
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const PERF_ENV: &str = "CASA_RS_IMPORTVLA_PERF";
const PERF_DIR_ENV: &str = "CASA_RS_IMPORTVLA_PERF_DIR";
const FLAG_CATEGORY_NAMES: [&str; DEFAULT_FLAG_CATEGORIES] = [
    "ONLINE_1", "ONLINE_2", "ONLINE_4", "ONLINE_8", "SHADOW", "FLAG_CMD",
];
mod archive;
mod msv4;
mod normalize;
mod output_rows;
mod products;
mod spectral;
mod state;

use archive::*;
use normalize::*;
use output_rows::*;
use products::*;
use spectral::*;
use state::*;

/// Summary of one native import run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImportReport {
    /// Output MeasurementSet path.
    pub vis: PathBuf,
    /// Number of logical records examined.
    pub logical_records_seen: usize,
    /// Number of logical records copied into the MS.
    pub logical_records_imported: usize,
    /// Number of logical records skipped by task-style filters.
    pub logical_records_skipped: usize,
    /// Number of rows written to the main table.
    pub main_rows_written: usize,
    /// Features that the explicitly requested MSv4 output cannot carry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub losses: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ImportPerfEventKind {
    ImportCompleted,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ImportPerfEvent {
    kind: ImportPerfEventKind,
    monotonic_ns: u64,
    vis: String,
    logical_records_seen: usize,
    logical_records_imported: usize,
    logical_records_skipped: usize,
    main_rows_written: usize,
    create_measurement_set_ns: u64,
    normalize_record_ns: u64,
    push_record_ns: u64,
    reorder_baseline_ns: u64,
    flag_row_ns: u64,
    append_main_row_ns: u64,
    finish_metadata_ns: u64,
    save_ns: u64,
    write_producer_ns: u64,
    write_queue_wait_ns: u64,
    write_assemble_ns: u64,
    write_physical_ns: u64,
    write_finalize_ns: u64,
    write_rows: usize,
    write_bytes: usize,
    write_peak_planned_resident_bytes: usize,
    total_ns: u64,
    unattributed_ns: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ImportPerfSummary {
    create_measurement_set_ns: u64,
    normalize_record_ns: u64,
    push_record_ns: u64,
    reorder_baseline_ns: u64,
    flag_row_ns: u64,
    append_main_row_ns: u64,
    finish_metadata_ns: u64,
    save_ns: u64,
    write_producer_ns: u64,
    write_queue_wait_ns: u64,
    write_assemble_ns: u64,
    write_physical_ns: u64,
    write_finalize_ns: u64,
    write_rows: usize,
    write_bytes: usize,
    write_peak_planned_resident_bytes: usize,
}

struct ImportPerfTracer {
    started_at: Option<Instant>,
    json_file: Option<File>,
    log_file: Option<File>,
}

impl ImportPerfTracer {
    fn from_env() -> Self {
        if std::env::var_os(PERF_ENV).is_none() {
            return Self::disabled();
        }
        let output_dir = std::env::var_os(PERF_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        if create_dir_all(&output_dir).is_err() {
            return Self::disabled();
        }
        let pid = std::process::id();
        let json_path = output_dir.join(format!("casa-vla-import-perf-{pid}.jsonl"));
        let log_path = output_dir.join(format!("casa-vla-import-perf-{pid}.log"));
        let json_file = open_append_file(&json_path);
        let log_file = open_append_file(&log_path);
        if json_file.is_none() && log_file.is_none() {
            return Self::disabled();
        }
        Self {
            started_at: Some(Instant::now()),
            json_file,
            log_file,
        }
    }

    const fn disabled() -> Self {
        Self {
            started_at: None,
            json_file: None,
            log_file: None,
        }
    }

    fn monotonic_ns(&self) -> u64 {
        self.started_at
            .map(|started| started.elapsed().as_nanos() as u64)
            .unwrap_or_default()
    }

    fn emit_import_completed(
        &mut self,
        vis: &std::path::Path,
        report: &ImportReport,
        summary: ImportPerfSummary,
        total_ns: u64,
    ) {
        if self.started_at.is_none() {
            return;
        }
        let attributed_ns = summary.create_measurement_set_ns
            + summary.normalize_record_ns
            + summary.push_record_ns
            + summary.finish_metadata_ns
            + summary.save_ns;
        let event = ImportPerfEvent {
            kind: ImportPerfEventKind::ImportCompleted,
            monotonic_ns: self.monotonic_ns(),
            vis: vis.display().to_string(),
            logical_records_seen: report.logical_records_seen,
            logical_records_imported: report.logical_records_imported,
            logical_records_skipped: report.logical_records_skipped,
            main_rows_written: report.main_rows_written,
            create_measurement_set_ns: summary.create_measurement_set_ns,
            normalize_record_ns: summary.normalize_record_ns,
            push_record_ns: summary.push_record_ns,
            reorder_baseline_ns: summary.reorder_baseline_ns,
            flag_row_ns: summary.flag_row_ns,
            append_main_row_ns: summary.append_main_row_ns,
            finish_metadata_ns: summary.finish_metadata_ns,
            save_ns: summary.save_ns,
            write_producer_ns: summary.write_producer_ns,
            write_queue_wait_ns: summary.write_queue_wait_ns,
            write_assemble_ns: summary.write_assemble_ns,
            write_physical_ns: summary.write_physical_ns,
            write_finalize_ns: summary.write_finalize_ns,
            write_rows: summary.write_rows,
            write_bytes: summary.write_bytes,
            write_peak_planned_resident_bytes: summary.write_peak_planned_resident_bytes,
            total_ns,
            unattributed_ns: total_ns.saturating_sub(attributed_ns),
        };
        if let Some(file) = self.json_file.as_mut() {
            let _ = serde_json::to_writer(&mut *file, &event);
            let _ = writeln!(file);
            let _ = file.flush();
        }
        if let Some(file) = self.log_file.as_mut() {
            let _ = writeln!(
                file,
                "[+{:>7} ms] kind={:?} rows={} total_ms={:.2} create_ms={:.2} normalize_ms={:.2} push_ms={:.2} reorder_ms={:.2} flag_row_ms={:.2} append_row_ms={:.2} finish_metadata_ms={:.2} save_ms={:.2} write_producer_ms={:.2} write_queue_wait_ms={:.2} write_assemble_ms={:.2} write_physical_ms={:.2} write_finalize_ms={:.2} write_bytes={} write_peak_planned_resident_bytes={} unattributed_ms={:.2}",
                event.monotonic_ns / 1_000_000,
                event.kind,
                event.main_rows_written,
                event.total_ns as f64 / 1_000_000.0,
                event.create_measurement_set_ns as f64 / 1_000_000.0,
                event.normalize_record_ns as f64 / 1_000_000.0,
                event.push_record_ns as f64 / 1_000_000.0,
                event.reorder_baseline_ns as f64 / 1_000_000.0,
                event.flag_row_ns as f64 / 1_000_000.0,
                event.append_main_row_ns as f64 / 1_000_000.0,
                event.finish_metadata_ns as f64 / 1_000_000.0,
                event.save_ns as f64 / 1_000_000.0,
                event.write_producer_ns as f64 / 1_000_000.0,
                event.write_queue_wait_ns as f64 / 1_000_000.0,
                event.write_assemble_ns as f64 / 1_000_000.0,
                event.write_physical_ns as f64 / 1_000_000.0,
                event.write_finalize_ns as f64 / 1_000_000.0,
                event.write_bytes,
                event.write_peak_planned_resident_bytes,
                event.unattributed_ns as f64 / 1_000_000.0
            );
            let _ = file.flush();
        }
    }
}

fn open_append_file(path: &std::path::Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

#[derive(Debug, Clone)]
struct NormalizedRecord {
    project: String,
    source_name: String,
    calibration_code: String,
    source_num_lines: i32,
    source_direction: [f64; 2],
    array_id: i32,
    time_seconds: f64,
    integration_seconds: f64,
    direction_epoch: DirectionEpoch,
    antennas: Vec<NormalizedAntenna>,
    groups: Vec<NormalizedGroup>,
}

#[derive(Debug, Clone)]
struct NormalizedAntenna {
    antenna_id: u8,
    name: String,
    station: String,
    position_itrf_m: [f64; 3],
}

#[derive(Debug, Clone)]
struct NormalizedGroup {
    descriptor: SpectralDescriptor,
    cda_ids: Vec<CdaId>,
    baselines: Vec<NormalizedBaselineRow>,
}

#[derive(Debug, Clone)]
struct NormalizedBaselineRow {
    antenna1_archive: usize,
    antenna2_archive: usize,
    corr_types: Vec<i32>,
    uvw_m: [f64; 3],
    data: ArrayValue,
    corrected_data: ArrayValue,
    model_data: ArrayValue,
    flag: ArrayValue,
    flag_category: ArrayValue,
    weight: ArrayValue,
    sigma: ArrayValue,
}

#[derive(Debug, Clone)]
struct SpectralDescriptor {
    edge_frequency_hz: f64,
    observed_frequency_hz: f64,
    channel_width_hz: f64,
    num_chan: usize,
    if_conv_chain: i32,
    rest_frequency_hz: f64,
    rest_frame: FrequencyFrame,
    doppler_definition: DopplerDefinition,
    doppler_velocity_mps: f64,
    doppler_tracking: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FieldKey {
    source_name: String,
    calibration_code: String,
    ra_bits: u64,
    dec_bits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PolarizationKey {
    corr_types: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DataDescriptionKey {
    spectral_window_id: i32,
    polarization_id: i32,
}

#[derive(Debug, Clone)]
struct SpectralWindowEntry {
    descriptor: SpectralDescriptor,
    stored_bandwidth_hz: f64,
    row: i32,
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
}

/// Import archive files from disk into a new MeasurementSet.
///
/// Apply the archive selection to disk-file input and publish the complete
/// MeasurementSet at `vis`, or the archive-derived name if one was omitted.
pub fn import_archive_files_to_measurement_set_from_options(
    options: &ImportVlaOptions,
) -> Result<ImportReport, VlaError> {
    import_archive_with_history(options, None)
}

/// Import archive files from disk into a MeasurementSet, optionally recording task history.
pub fn import_archive_with_history(
    options: &ImportVlaOptions,
    history: Option<&casa_mstransform::TransformTaskHistory>,
) -> Result<ImportReport, VlaError> {
    let total_started = Instant::now();
    let mut perf_tracer = ImportPerfTracer::from_env();
    let selection = ArchiveSelection::new(options)?;
    let vis = options.effective_vis_for_import()?;
    if options.outformat == ImportVlaFormat::Msv4 {
        return msv4::import(options, &selection, vis, history.is_some());
    }
    let output = ImportOutput::prepare(&vis, history.is_some())?;
    let (write_plan, flag_shapes) = plan_import_main_arrays(options, &selection, &vis)?;

    let create_started = Instant::now();
    let (ms, main_write_session) =
        Msv2MainWriter::create(output.path(), import_builder(), write_plan).map_err(|error| {
            VlaError::import(format!("create MeasurementSet {}: {error}", vis.display()))
        })?;

    let mut writer =
        VlaImportState::new_with_write_session(options.clone(), ms, main_write_session)?;
    writer.perf_summary.create_measurement_set_ns = create_started.elapsed().as_nanos() as u64;
    set_flag_category_names(writer.ms.main_table_mut())?;
    let mut info = writer.ms.main_table().info().clone();
    info.sub_type = "VLA".into();
    info.readme = vec![
        "This is a MeasurementSet Table holding measurements from the VLA".into(),
        "radio synthesis array (operated by NRAO)".into(),
    ];
    writer.ms.main_table_mut().set_info(info);
    process_archive_records(
        &mut writer,
        options,
        &selection,
        VlaImportState::push_record,
    )?;
    writer.finish()?;
    if let Some(history) = history {
        casa_mstransform::append_task_history(output.path(), history)
            .map_err(|error| VlaError::import(error.to_string()))?;
        let staged = MeasurementSet::open(output.path())
            .map_err(|error| VlaError::import(error.to_string()))?;
        casa_flagging::save_initial_flag_version(&staged, flag_shapes)
            .map_err(|error| VlaError::import(error.to_string()))?;
    }
    output.publish()?;
    let report = writer.report(vis, Vec::new());
    perf_tracer.emit_import_completed(
        &report.vis,
        &report,
        writer.perf_summary,
        total_started.elapsed().as_nanos() as u64,
    );
    Ok(report)
}

fn import_builder() -> MeasurementSetBuilder {
    MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler)
}

impl VlaImportState {
    fn report(&self, vis: PathBuf, losses: Vec<String>) -> ImportReport {
        ImportReport {
            vis,
            logical_records_seen: self.logical_records_seen,
            logical_records_imported: self.logical_records_imported,
            logical_records_skipped: self.logical_records_skipped,
            main_rows_written: self.main_rows_written,
            losses,
        }
    }
}

fn process_archive_records(
    writer: &mut VlaImportState,
    options: &ImportVlaOptions,
    selection: &ArchiveSelection,
    mut push: impl FnMut(&mut VlaImportState, NormalizedRecord) -> Result<(), VlaError>,
) -> Result<(), VlaError> {
    for path in options.require_archivefiles()? {
        let mut reader = open_import_archive(path)?;
        let mut frequency_tolerance = options.frequencytol_hz;
        while let Some(record) = reader.next_record()? {
            writer.logical_records_seen += 1;
            let normalize_started = Instant::now();
            let normalized = normalize_record(
                &record,
                options,
                selection,
                writer.measures.as_ref(),
                &mut frequency_tolerance,
            )?;
            writer.perf_summary.normalize_record_ns +=
                normalize_started.elapsed().as_nanos() as u64;
            if let Some(normalized) = normalized {
                writer.logical_records_imported += 1;
                writer.options.frequencytol_hz = frequency_tolerance;
                let push_started = Instant::now();
                push(writer, normalized)?;
                writer.perf_summary.push_record_ns += push_started.elapsed().as_nanos() as u64;
            } else {
                writer.logical_records_skipped += 1;
                writer.note_skipped_record();
            }
        }
        writer.finish_archive()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
