// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native `importvla`-style MeasurementSet writer for disk archives.

use std::collections::HashMap;
use std::f64::consts::PI;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use casa_ms::builder::MeasurementSetBuilder;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_ms::{OptionalMainColumn, SubTable};
use casa_tables::{ColumnSchema, Table};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{MPosition, MeasFrame};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::disk::LogicalRecord;
use crate::record::{
    CdaId, DirectionEpoch, DopplerDefinition, FrequencyFrame, IfId, IfUsage, StokesProduct,
};
use crate::{AntennaNameScheme, ImportVlaOptions, VlaDiskReader, VlaError};

const DEFAULT_FLAG_CATEGORIES: usize = 6;
const DEFAULT_TELESCOPE_NAME: &str = "VLA";
const SECONDS_PER_JULIAN_YEAR: f64 = 365.25 * 86_400.0;
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const PERF_ENV: &str = "CASA_RS_IMPORTVLA_PERF";
const PERF_DIR_ENV: &str = "CASA_RS_IMPORTVLA_PERF_DIR";
const FLAG_CATEGORY_NAMES: [&str; DEFAULT_FLAG_CATEGORIES] = [
    "ONLINE_1", "ONLINE_2", "ONLINE_4", "ONLINE_8", "SHADOW", "FLAG_CMD",
];

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
    make_main_row_ns: u64,
    append_main_row_ns: u64,
    finish_metadata_ns: u64,
    save_ns: u64,
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
    make_main_row_ns: u64,
    append_main_row_ns: u64,
    finish_metadata_ns: u64,
    save_ns: u64,
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
            make_main_row_ns: summary.make_main_row_ns,
            append_main_row_ns: summary.append_main_row_ns,
            finish_metadata_ns: summary.finish_metadata_ns,
            save_ns: summary.save_ns,
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
                "[+{:>7} ms] kind={:?} rows={} total_ms={:.2} create_ms={:.2} normalize_ms={:.2} push_ms={:.2} reorder_ms={:.2} flag_row_ms={:.2} make_row_ms={:.2} append_row_ms={:.2} finish_metadata_ms={:.2} save_ms={:.2} unattributed_ms={:.2}",
                event.monotonic_ns / 1_000_000,
                event.kind,
                event.main_rows_written,
                event.total_ns as f64 / 1_000_000.0,
                event.create_measurement_set_ns as f64 / 1_000_000.0,
                event.normalize_record_ns as f64 / 1_000_000.0,
                event.push_record_ns as f64 / 1_000_000.0,
                event.reorder_baseline_ns as f64 / 1_000_000.0,
                event.flag_row_ns as f64 / 1_000_000.0,
                event.make_main_row_ns as f64 / 1_000_000.0,
                event.append_main_row_ns as f64 / 1_000_000.0,
                event.finish_metadata_ns as f64 / 1_000_000.0,
                event.save_ns as f64 / 1_000_000.0,
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
    total_bandwidth_hz: f64,
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
    row: i32,
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
}

/// Import archive files from disk into a new MeasurementSet.
///
/// The current implementation accepts the task-style `importvla` option shape,
/// but this wave only supports disk-file input plus the subset of selectors
/// needed by the native writer: `archivefiles`, optional `vis`, `project`,
/// `antnamescheme`, `autocorr`, `applytsys`, and `keepblanks`.
pub fn import_archive_files_to_measurement_set_from_options(
    options: &ImportVlaOptions,
) -> Result<ImportReport, VlaError> {
    let total_started = Instant::now();
    let mut perf_tracer = ImportPerfTracer::from_env();
    validate_import_options(options)?;
    let vis = options.effective_vis_for_import()?;
    if vis.exists() {
        return Err(VlaError::InvalidArgument {
            argument: "vis",
            message: format!("output MeasurementSet already exists: {}", vis.display()),
        });
    }

    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let create_started = Instant::now();
    let ms = MeasurementSet::create(&vis, builder).map_err(|error| {
        VlaError::import(format!("create MeasurementSet {}: {error}", vis.display()))
    })?;

    let mut writer = MsImportWriter::new(options.clone(), ms)?;
    writer.perf_summary.create_measurement_set_ns = create_started.elapsed().as_nanos() as u64;
    set_flag_category_names(writer.ms.main_table_mut())?;
    for path in options.require_archivefiles()? {
        let mut reader = VlaDiskReader::open(path)?;
        while let Some(record) = reader.next_record()? {
            writer.logical_records_seen += 1;
            let normalize_started = Instant::now();
            if let Some(normalized) = normalize_record(&record, options)? {
                writer.perf_summary.normalize_record_ns +=
                    normalize_started.elapsed().as_nanos() as u64;
                writer.logical_records_imported += 1;
                let push_started = Instant::now();
                writer.push_record(normalized)?;
                writer.perf_summary.push_record_ns += push_started.elapsed().as_nanos() as u64;
            } else {
                writer.perf_summary.normalize_record_ns +=
                    normalize_started.elapsed().as_nanos() as u64;
                writer.logical_records_skipped += 1;
                writer.note_skipped_record();
            }
        }
    }
    writer.finish()?;
    let report = ImportReport {
        vis,
        logical_records_seen: writer.logical_records_seen,
        logical_records_imported: writer.logical_records_imported,
        logical_records_skipped: writer.logical_records_skipped,
        main_rows_written: writer.main_rows_written,
    };
    perf_tracer.emit_import_completed(
        &report.vis,
        &report,
        writer.perf_summary,
        total_started.elapsed().as_nanos() as u64,
    );
    Ok(report)
}

fn validate_import_options(options: &ImportVlaOptions) -> Result<(), VlaError> {
    options.require_archivefiles()?;
    if options.bandname.is_some() {
        return Err(VlaError::InvalidArgument {
            argument: "bandname",
            message: "band selection is not implemented in this writer wave".to_string(),
        });
    }
    if options.starttime.is_some() {
        return Err(VlaError::InvalidArgument {
            argument: "starttime",
            message: "time selection is not implemented in this writer wave".to_string(),
        });
    }
    if options.stoptime.is_some() {
        return Err(VlaError::InvalidArgument {
            argument: "stoptime",
            message: "time selection is not implemented in this writer wave".to_string(),
        });
    }
    if options.evlabands {
        return Err(VlaError::InvalidArgument {
            argument: "evlabands",
            message: "EVLA band remapping is not implemented in this writer wave".to_string(),
        });
    }
    Ok(())
}

fn normalize_record(
    record: &LogicalRecord,
    options: &ImportVlaOptions,
) -> Result<Option<NormalizedRecord>, VlaError> {
    let rca = record
        .rca()
        .length_bytes()
        .map_err(|error| VlaError::import(format!("decode RCA length: {error}")))?;
    if rca == 0 {
        return Ok(None);
    }

    let rca = record.rca();
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let project = sda
        .observation_id()
        .map_err(|error| VlaError::import(format!("decode observation id: {error}")))?;
    if let Some(expected) = &options.project {
        if project.trim() != expected.trim() {
            return Ok(None);
        }
    }
    let observation_mode = sda
        .observation_mode_code()
        .map_err(|error| VlaError::import(format!("decode observation mode: {error}")))?;
    if !is_supported_observation_mode(&observation_mode) {
        return Ok(None);
    }
    let source_name = sda
        .source_name()
        .map_err(|error| VlaError::import(format!("decode source name: {error}")))?;
    if source_name.is_empty() && !options.keepblanks {
        return Ok(None);
    }

    let direction_epoch = sda
        .direction_epoch()
        .map_err(|error| VlaError::import(format!("decode direction epoch: {error}")))?;
    let source_direction = sda
        .source_direction_radians()
        .map_err(|error| VlaError::import(format!("decode source direction: {error}")))?;
    let source_direction = [
        normalize_longitude_radians(source_direction[0]),
        source_direction[1],
    ];
    let time_seconds = time_from_archive(
        rca.obs_day()
            .map_err(|error| VlaError::import(format!("decode obs day: {error}")))?,
        sda.observation_time_seconds()
            .map_err(|error| VlaError::import(format!("decode observation time: {error}")))?,
    );
    let integration_seconds = sda
        .integration_time_seconds()
        .map_err(|error| VlaError::import(format!("decode integration time: {error}")))?;

    let antennas = normalize_antennas(record, options.antnamescheme)?;
    let groups = normalize_groups(record, options, antennas.len())?;
    if groups.is_empty() {
        return Ok(None);
    }

    Ok(Some(NormalizedRecord {
        project,
        source_name,
        calibration_code: sda
            .calibration_code()
            .map_err(|error| VlaError::import(format!("decode calibration code: {error}")))?,
        source_num_lines: count_valid_source_lines(record)?,
        source_direction,
        array_id: i32::from(
            sda.subarray_id()
                .map_err(|error| VlaError::import(format!("decode subarray id: {error}")))?,
        ) - 1,
        time_seconds,
        integration_seconds,
        direction_epoch,
        antennas,
        groups,
    }))
}

fn is_supported_observation_mode(code: &str) -> bool {
    matches!(
        code,
        "  " | "H " | "S " | "SP" | "VA" | "VB" | "VL" | "VR" | "VX"
    )
}

fn normalize_antennas(
    record: &LogicalRecord,
    naming: AntennaNameScheme,
) -> Result<Vec<NormalizedAntenna>, VlaError> {
    let n_antennas = usize::from(
        record
            .rca()
            .n_antennas()
            .map_err(|error| VlaError::import(format!("decode antenna count: {error}")))?,
    );
    let observatory =
        MPosition::from_observatory_name(DEFAULT_TELESCOPE_NAME).ok_or_else(|| {
            VlaError::import("resolve observatory position for VLA from measures catalog")
        })?;
    let [obs_x, obs_y, obs_z] = observatory.as_itrf();
    let longitude = observatory.longitude_rad();
    let (sin_lon, cos_lon) = longitude.sin_cos();

    let mut antennas = Vec::with_capacity(n_antennas);
    for archive_index in 0..n_antennas {
        let ada = record
            .ada(archive_index)
            .map_err(|error| VlaError::import(format!("decode ADA {archive_index}: {error}")))?;
        let bx = ada
            .bx_meters()
            .map_err(|error| VlaError::import(format!("decode ADA bx: {error}")))?;
        let by = ada
            .by_meters()
            .map_err(|error| VlaError::import(format!("decode ADA by: {error}")))?;
        let bz = ada
            .bz_meters()
            .map_err(|error| VlaError::import(format!("decode ADA bz: {error}")))?;
        let dx = cos_lon * bx - sin_lon * by;
        let dy = sin_lon * bx + cos_lon * by;
        let dz = bz;
        let name = ada
            .antenna_name(matches!(naming, AntennaNameScheme::New))
            .map_err(|error| VlaError::import(format!("decode antenna name: {error}")))?;
        antennas.push(NormalizedAntenna {
            antenna_id: ada
                .antenna_id()
                .map_err(|error| VlaError::import(format!("decode antenna id: {error}")))?,
            station: ada
                .pad_name()
                .map_err(|error| VlaError::import(format!("decode antenna pad name: {error}")))?,
            name,
            position_itrf_m: [obs_x + dx, obs_y + dy, obs_z + dz],
        });
    }
    Ok(antennas)
}

fn count_valid_source_lines(record: &LogicalRecord) -> Result<i32, VlaError> {
    let mut count = 0_i32;
    let rca = record.rca();
    for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
        if rca
            .cda_offset_bytes(cda_id.index())
            .map_err(|error| VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id)))?
            == 0
        {
            continue;
        }
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        if cda.is_valid() {
            count += 1;
        }
    }
    Ok(count)
}

fn normalize_groups(
    record: &LogicalRecord,
    options: &ImportVlaOptions,
    n_antennas: usize,
) -> Result<Vec<NormalizedGroup>, VlaError> {
    let rca = record.rca();
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let revision = record
        .rca()
        .revision()
        .map_err(|error| VlaError::import(format!("decode revision: {error}")))?;
    let mut cda_groups: Vec<(SpectralDescriptor, Vec<CdaId>)> = Vec::new();

    for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
        if rca
            .cda_offset_bytes(cda_id.index())
            .map_err(|error| VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id)))?
            == 0
        {
            continue;
        }
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        if !cda.is_valid() {
            continue;
        }
        if sda
            .n_polarizations(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA polarization count: {error}")))?
            == 0
        {
            continue;
        }
        let descriptor = SpectralDescriptor {
            edge_frequency_hz: sda.edge_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode edge frequency for {:?}: {error}", cda_id))
            })?,
            observed_frequency_hz: sda.observed_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode observed frequency for {:?}: {error}",
                    cda_id
                ))
            })?,
            channel_width_hz: sda.channel_width_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel width for {:?}: {error}", cda_id))
            })?,
            total_bandwidth_hz: sda.correlated_bandwidth_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode bandwidth for {:?}: {error}", cda_id))
            })?,
            num_chan: usize::try_from(sda.n_channels(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel count for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("channel count does not fit in usize"))?,
            if_conv_chain: i32::try_from(sda.electronic_path(cda_id).map_err(|error| {
                VlaError::import(format!("decode IF chain for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("IF conversion chain does not fit in i32"))?,
            rest_frequency_hz: sda.rest_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frequency for {:?}: {error}", cda_id))
            })?,
            rest_frame: sda.rest_frame(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frame for {:?}: {error}", cda_id))
            })?,
            doppler_definition: sda.doppler_definition(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode doppler definition for {:?}: {error}",
                    cda_id
                ))
            })?,
            doppler_velocity_mps: sda.radial_velocity_mps(cda_id).map_err(|error| {
                VlaError::import(format!("decode radial velocity for {:?}: {error}", cda_id))
            })?,
            doppler_tracking: sda.doppler_tracking(cda_id).map_err(|error| {
                VlaError::import(format!("decode doppler tracking for {:?}: {error}", cda_id))
            })?,
        };

        if let Some((_, cdas)) = cda_groups.iter_mut().find(|(existing, _)| {
            same_spectral_descriptor(existing, &descriptor, options.frequencytol_hz)
        }) {
            cdas.push(cda_id);
        } else {
            cda_groups.push((descriptor, vec![cda_id]));
        }
    }

    let mut groups = Vec::with_capacity(cda_groups.len());
    for (descriptor, cda_ids) in cda_groups {
        let corr_types = correlation_types_for_group(record, &cda_ids, n_antennas)?;
        let shadowed = compute_shadowed_antennas(record, n_antennas)?;
        let mut baselines = Vec::new();
        let mut cross_index = 0_usize;
        for ant1 in 0..n_antennas {
            for ant2 in ant1..n_antennas {
                let is_cross = ant1 != ant2;
                if !is_cross && !options.autocorr {
                    continue;
                }
                let uvw_m = if is_cross {
                    let ada1 = record.ada(ant1).map_err(|error| {
                        VlaError::import(format!("decode ADA {ant1} for UVW: {error}"))
                    })?;
                    let ada2 = record.ada(ant2).map_err(|error| {
                        VlaError::import(format!("decode ADA {ant2} for UVW: {error}"))
                    })?;
                    [
                        ada1.u_meters()
                            .map_err(|error| VlaError::import(format!("decode u: {error}")))?
                            - ada2
                                .u_meters()
                                .map_err(|error| VlaError::import(format!("decode u: {error}")))?,
                        ada1.v_meters()
                            .map_err(|error| VlaError::import(format!("decode v: {error}")))?
                            - ada2
                                .v_meters()
                                .map_err(|error| VlaError::import(format!("decode v: {error}")))?,
                        ada1.w_meters()
                            .map_err(|error| VlaError::import(format!("decode w: {error}")))?
                            - ada2
                                .w_meters()
                                .map_err(|error| VlaError::import(format!("decode w: {error}")))?,
                    ]
                } else {
                    [0.0, 0.0, 0.0]
                };
                baselines.push(normalize_baseline_row(
                    record,
                    &descriptor,
                    &cda_ids,
                    &corr_types,
                    ant1,
                    ant2,
                    is_cross,
                    cross_index,
                    revision,
                    options.applytsys,
                    uvw_m,
                    &shadowed,
                )?);
                if is_cross {
                    cross_index += 1;
                }
            }
        }
        groups.push(NormalizedGroup {
            descriptor,
            baselines,
        });
    }

    Ok(groups)
}

#[allow(clippy::too_many_arguments)]
fn normalize_baseline_row(
    record: &LogicalRecord,
    descriptor: &SpectralDescriptor,
    cda_ids: &[CdaId],
    corr_types: &[i32],
    ant1: usize,
    ant2: usize,
    is_cross: bool,
    cross_index: usize,
    revision: u16,
    apply_tsys: bool,
    uvw_m: [f64; 3],
    shadowed: &[bool],
) -> Result<NormalizedBaselineRow, VlaError> {
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let n_corr = corr_types.len();
    let n_chan = descriptor.num_chan;
    let mut values = vec![Complex32::new(0.0, 0.0); n_corr * n_chan];
    let model_values = build_model_data(corr_types, n_chan);
    let mut flags = vec![false; n_corr * n_chan];
    let mut flag_levels = vec![false; n_corr * n_chan * DEFAULT_FLAG_CATEGORIES];
    let mut weights = vec![0.0_f32; n_corr];
    let mut sigmas = vec![0.0_f32; n_corr];

    let mut is_scaled_by_nominal_sensitivity = false;
    for &cda_id in cda_ids {
        for usage in sda.if_usage(cda_id).map_err(|error| {
            VlaError::import(format!("decode IF usage for {:?}: {error}", cda_id))
        })? {
            let a1 = record.ada(ant1).map_err(|error| {
                VlaError::import(format!("decode ADA {ant1} nominal sensitivity: {error}"))
            })?;
            let a2 = record.ada(ant2).map_err(|error| {
                VlaError::import(format!("decode ADA {ant2} nominal sensitivity: {error}"))
            })?;
            is_scaled_by_nominal_sensitivity |= a1
                .nominal_sensitivity_applied(usage.ant1, revision)
                .map_err(|error| {
                    VlaError::import(format!(
                        "decode nominal sensitivity state for antenna {ant1}: {error}"
                    ))
                })?;
            is_scaled_by_nominal_sensitivity |= a2
                .nominal_sensitivity_applied(usage.ant2, revision)
                .map_err(|error| {
                    VlaError::import(format!(
                        "decode nominal sensitivity state for antenna {ant2}: {error}"
                    ))
                })?;
        }
    }

    for &cda_id in cda_ids {
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        let baseline = if is_cross {
            cda.cross_corr(cross_index)
        } else {
            cda.auto_corr(ant1)
        }
        .map_err(|error| VlaError::import(format!("decode baseline: {error}")))?;
        let mut baseline_data = baseline
            .data()
            .map_err(|error| VlaError::import(format!("decode baseline data: {error}")))?;
        let stokes = record.stokes_products(cda_id, 0, 0).map_err(|error| {
            VlaError::import(format!("derive Stokes products for {:?}: {error}", cda_id))
        })?;
        let if_usage = sda
            .if_usage(cda_id)
            .map_err(|error| VlaError::import(format!("decode IF usage: {error}")))?;

        if baseline_data.len() == stokes.len() && n_chan == 1 {
            for (product_index, (stokes_product, usage)) in
                stokes.iter().zip(if_usage.iter()).enumerate()
            {
                let corr_index = corr_types
                    .iter()
                    .position(|code| *code == stokes_code(*stokes_product))
                    .ok_or_else(|| {
                        VlaError::import("correlation type not found in continuum row")
                    })?;
                let bl_sensitivity =
                    baseline_sensitivity(record, ant1, ant2, *usage, revision).unwrap_or(1.0);
                let mut value = baseline_data[product_index];
                let (weight, sigma) = visibility_weight_sigma(
                    descriptor.channel_width_hz,
                    sda.integration_time_seconds().map_err(|error| {
                        VlaError::import(format!("decode integration time: {error}"))
                    })?,
                    bl_sensitivity,
                    apply_tsys,
                    is_scaled_by_nominal_sensitivity,
                );
                if apply_tsys {
                    if !is_scaled_by_nominal_sensitivity {
                        value *= bl_sensitivity.sqrt();
                    }
                } else if is_scaled_by_nominal_sensitivity && bl_sensitivity > 1.0e-10 {
                    value /= bl_sensitivity.sqrt();
                }
                let (product_flag, product_categories) =
                    product_flagging(record, ant1, ant2, *usage, shadowed)?;
                values[corr_index] = value;
                weights[corr_index] = weight;
                sigmas[corr_index] = sigma;
                flags[corr_index] = product_flag;
                for (category_index, flagged) in product_categories.iter().copied().enumerate() {
                    flag_levels
                        [flag_category_index(corr_index, 0, category_index, n_corr, n_chan)] =
                        flagged;
                }
            }
        } else if stokes.len() == 1 && baseline_data.len() == n_chan {
            let corr_index = corr_types
                .iter()
                .position(|code| *code == stokes_code(stokes[0]))
                .ok_or_else(|| VlaError::import("correlation type not found in spectral row"))?;
            let bl_sensitivity =
                baseline_sensitivity(record, ant1, ant2, if_usage[0], revision).unwrap_or(1.0);
            let (weight, sigma) = visibility_weight_sigma(
                descriptor.channel_width_hz,
                sda.integration_time_seconds().map_err(|error| {
                    VlaError::import(format!("decode integration time: {error}"))
                })?,
                bl_sensitivity,
                apply_tsys,
                is_scaled_by_nominal_sensitivity,
            );
            let (product_flag, product_categories) =
                product_flagging(record, ant1, ant2, if_usage[0], shadowed)?;
            for (chan_index, value) in baseline_data.iter_mut().enumerate() {
                if apply_tsys {
                    if !is_scaled_by_nominal_sensitivity {
                        *value *= bl_sensitivity.sqrt();
                    }
                } else if is_scaled_by_nominal_sensitivity && bl_sensitivity > 1.0e-10 {
                    *value /= bl_sensitivity.sqrt();
                }
                values[corr_index + chan_index * n_corr] = *value;
                flags[corr_index + chan_index * n_corr] = product_flag;
                for (category_index, flagged) in product_categories.iter().copied().enumerate() {
                    flag_levels[flag_category_index(
                        corr_index,
                        chan_index,
                        category_index,
                        n_corr,
                        n_chan,
                    )] = flagged;
                }
            }
            weights[corr_index] = weight;
            sigmas[corr_index] = sigma;
        } else {
            return Err(VlaError::import(format!(
                "unsupported CDA/data layout for {:?}: data={} stokes={} nchan={n_chan}",
                cda_id,
                baseline_data.len(),
                stokes.len()
            )));
        }
    }

    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), values)
            .map_err(|error| VlaError::import(format!("shape data array: {error}")))?,
    );
    let corrected_data = data.clone();
    let model_data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), model_values)
            .map_err(|error| VlaError::import(format!("shape model-data array: {error}")))?,
    );
    let flag = ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), flags)
            .map_err(|error| VlaError::import(format!("shape flag array: {error}")))?,
    );
    let flag_category = ArrayValue::Bool(
        ArrayD::from_shape_vec(
            IxDyn(&[n_corr, n_chan, DEFAULT_FLAG_CATEGORIES]).f(),
            flag_levels,
        )
        .map_err(|error| VlaError::import(format!("shape flag-category array: {error}")))?,
    );
    let weight = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr]).f(), weights)
            .map_err(|error| VlaError::import(format!("shape weight array: {error}")))?,
    );
    let sigma = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr]).f(), sigmas)
            .map_err(|error| VlaError::import(format!("shape sigma array: {error}")))?,
    );
    Ok(NormalizedBaselineRow {
        antenna1_archive: ant1,
        antenna2_archive: ant2,
        corr_types: corr_types.to_vec(),
        uvw_m,
        data,
        corrected_data,
        model_data,
        flag,
        flag_category,
        weight,
        sigma,
    })
}

fn same_spectral_descriptor(
    left: &SpectralDescriptor,
    right: &SpectralDescriptor,
    tolerance_hz: f64,
) -> bool {
    let center_frequency_matches = if left.doppler_tracking {
        let left_ref = spectral_reference_frequency(left);
        let right_ref = spectral_reference_frequency(right);
        (left_ref.hz() - right_ref.hz()).abs() <= tolerance_hz
            && left_ref.refer() == right_ref.refer()
    } else {
        (left.observed_frequency_hz - right.observed_frequency_hz).abs() <= tolerance_hz
    };

    left.num_chan == right.num_chan
        && left.if_conv_chain == right.if_conv_chain
        && left.doppler_tracking == right.doppler_tracking
        && center_frequency_matches
        && (left.channel_width_hz - right.channel_width_hz).abs() <= tolerance_hz
        && (left.total_bandwidth_hz - right.total_bandwidth_hz).abs() <= tolerance_hz
        && (!left.doppler_tracking
            || (left.rest_frame == right.rest_frame
                && left.doppler_definition == right.doppler_definition
                && (left.rest_frequency_hz - right.rest_frequency_hz).abs() <= tolerance_hz
                && (left.doppler_velocity_mps - right.doppler_velocity_mps).abs() <= 1.0e-6))
}

fn same_spectral_window_shape(
    left: &SpectralDescriptor,
    right: &SpectralDescriptor,
    tolerance_hz: f64,
) -> bool {
    let bandwidth_tolerance_hz = left
        .total_bandwidth_hz
        .abs()
        .max(right.total_bandwidth_hz.abs())
        / 4.0;
    left.num_chan == right.num_chan
        && left.if_conv_chain == right.if_conv_chain
        && (left.total_bandwidth_hz - right.total_bandwidth_hz).abs()
            <= bandwidth_tolerance_hz.max(tolerance_hz)
        && (left.channel_width_hz - right.channel_width_hz).abs() <= tolerance_hz
}

fn spectral_window_matches(
    existing: &SpectralWindowEntry,
    descriptor: &SpectralDescriptor,
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
    tolerance_hz: f64,
) -> Result<bool, VlaError> {
    if !same_spectral_window_shape(&existing.descriptor, descriptor, tolerance_hz) {
        return Ok(false);
    }

    let existing_frame = spectral_conversion_frame(
        existing.time_seconds,
        existing.source_direction,
        existing.direction_epoch,
    )?;
    let current_frame = spectral_conversion_frame(time_seconds, source_direction, direction_epoch)?;
    let existing_topo = spectral_reference_frequency(&existing.descriptor)
        .convert_to(FrequencyRef::TOPO, &existing_frame)
        .map_err(|error| {
            VlaError::import(format!("convert existing spectral window to TOPO: {error}"))
        })?
        .hz();
    let current_topo = spectral_reference_frequency(descriptor)
        .convert_to(FrequencyRef::TOPO, &current_frame)
        .map_err(|error| {
            VlaError::import(format!("convert current spectral window to TOPO: {error}"))
        })?
        .hz();
    Ok((existing_topo - current_topo).abs() <= tolerance_hz)
}

fn correlation_types_for_group(
    record: &LogicalRecord,
    cda_ids: &[CdaId],
    n_antennas: usize,
) -> Result<Vec<i32>, VlaError> {
    let _ = n_antennas;
    let mut corr_types = Vec::new();
    for &cda_id in cda_ids {
        for stokes in record
            .stokes_products(cda_id, 0, 0)
            .map_err(|error| VlaError::import(format!("derive correlation types: {error}")))?
        {
            corr_types.push(stokes_code(stokes));
        }
    }
    if corr_types.iter().all(|code| matches!(code, 5..=8)) && corr_types.len() > 1 {
        let mut standardized = Vec::with_capacity(corr_types.len());
        for code in [5, 6, 7, 8] {
            if corr_types.contains(&code) {
                standardized.push(code);
            }
        }
        return Ok(standardized);
    }
    Ok(corr_types)
}

fn compute_shadowed_antennas(
    record: &LogicalRecord,
    n_antennas: usize,
) -> Result<Vec<bool>, VlaError> {
    let mut shadowed = vec![false; n_antennas];
    for ant1 in 0..n_antennas {
        let ada1 = record.ada(ant1).map_err(|error| {
            VlaError::import(format!("decode ADA {ant1} for shadowing: {error}"))
        })?;
        for ant2 in (ant1 + 1)..n_antennas {
            let ada2 = record.ada(ant2).map_err(|error| {
                VlaError::import(format!("decode ADA {ant2} for shadowing: {error}"))
            })?;
            let du = ada1
                .u_meters()
                .map_err(|error| VlaError::import(format!("decode u: {error}")))?
                - ada2
                    .u_meters()
                    .map_err(|error| VlaError::import(format!("decode u: {error}")))?;
            let dv = ada1
                .v_meters()
                .map_err(|error| VlaError::import(format!("decode v: {error}")))?
                - ada2
                    .v_meters()
                    .map_err(|error| VlaError::import(format!("decode v: {error}")))?;
            if du * du + dv * dv < 625.0 {
                let dw = ada1
                    .w_meters()
                    .map_err(|error| VlaError::import(format!("decode w: {error}")))?
                    - ada2
                        .w_meters()
                        .map_err(|error| VlaError::import(format!("decode w: {error}")))?;
                if dw > 0.0 {
                    shadowed[ant2] = true;
                } else {
                    shadowed[ant1] = true;
                }
            }
        }
    }
    Ok(shadowed)
}

fn product_flagging(
    record: &LogicalRecord,
    ant1: usize,
    ant2: usize,
    usage: IfUsage,
    shadowed: &[bool],
) -> Result<(bool, [bool; DEFAULT_FLAG_CATEGORIES]), VlaError> {
    let ada1 = record
        .ada(ant1)
        .map_err(|error| VlaError::import(format!("decode ADA {ant1} for flagging: {error}")))?;
    let ada2 = record
        .ada(ant2)
        .map_err(|error| VlaError::import(format!("decode ADA {ant2} for flagging: {error}")))?;
    let status1 = ada1.if_status(usage.ant1).map_err(|error| {
        VlaError::import(format!("decode IF status for antenna {ant1}: {error}"))
    })?;
    let status2 = ada2.if_status(usage.ant2).map_err(|error| {
        VlaError::import(format!("decode IF status for antenna {ant2}: {error}"))
    })?;
    let shadow = shadowed.get(ant1).copied().unwrap_or(false)
        || shadowed.get(ant2).copied().unwrap_or(false);
    let categories = [
        (status1 & 0x01) != 0 || (status2 & 0x01) != 0,
        (status1 & 0x02) != 0 || (status2 & 0x02) != 0,
        (status1 & 0x04) != 0 || (status2 & 0x04) != 0,
        (status1 & 0x08) != 0 || (status2 & 0x08) != 0,
        shadow,
        false,
    ];
    Ok((categories[2] || categories[3] || shadow, categories))
}

fn flag_category_index(
    corr_index: usize,
    chan_index: usize,
    category_index: usize,
    n_corr: usize,
    n_chan: usize,
) -> usize {
    corr_index + chan_index * n_corr + category_index * n_corr * n_chan
}

fn stokes_code(stokes: StokesProduct) -> i32 {
    match stokes {
        StokesProduct::Rr => 5,
        StokesProduct::Rl => 6,
        StokesProduct::Lr => 7,
        StokesProduct::Ll => 8,
    }
}

fn build_model_data(corr_types: &[i32], n_chan: usize) -> Vec<Complex32> {
    let mut values = Vec::with_capacity(corr_types.len() * n_chan);
    for _ in 0..n_chan {
        for &corr_type in corr_types {
            let value = match corr_type {
                5 | 8 => Complex32::new(1.0, 0.0),
                _ => Complex32::new(0.0, 0.0),
            };
            values.push(value);
        }
    }
    values
}

fn visibility_weight_sigma(
    channel_width_hz: f64,
    integration_seconds: f64,
    baseline_sensitivity: f32,
    apply_tsys: bool,
    _is_scaled_by_nominal_sensitivity: bool,
) -> (f32, f32) {
    let nominal_weight = (integration_seconds * 0.12 / 10_000.0 * channel_width_hz) as f32;
    if nominal_weight <= 0.0 {
        return (0.0, 0.0);
    }
    let mut weight = nominal_weight;
    let mut sigma = 1.0 / nominal_weight.sqrt();
    if apply_tsys && baseline_sensitivity > 1.0e-10 {
        weight /= baseline_sensitivity;
        sigma *= baseline_sensitivity.sqrt();
    }
    (weight, sigma)
}

fn baseline_sensitivity(
    record: &LogicalRecord,
    ant1: usize,
    ant2: usize,
    usage: IfUsage,
    revision: u16,
) -> Option<f32> {
    let a1 = record.ada(ant1).ok()?;
    let a2 = record.ada(ant2).ok()?;
    let s1 = nominal_sensitivity_or_default(&a1, usage.ant1);
    let s2 = nominal_sensitivity_or_default(&a2, usage.ant2);
    let _scaled = a1.nominal_sensitivity_applied(usage.ant1, revision).ok()?
        || a2.nominal_sensitivity_applied(usage.ant2, revision).ok()?;
    Some(s1 * s2)
}

fn nominal_sensitivity_or_default(ada: &crate::record::AntennaDataArea<'_>, if_id: IfId) -> f32 {
    ada.nominal_sensitivity(if_id)
        .ok()
        .filter(|value| *value > 1.0e-10)
        .unwrap_or(0.333)
}

fn time_from_archive(obs_day: u32, obs_seconds: f64) -> f64 {
    f64::from(obs_day) * 86_400.0 + obs_seconds
}

struct MsImportWriter {
    options: ImportVlaOptions,
    ms: MeasurementSet,
    logical_records_seen: usize,
    logical_records_imported: usize,
    logical_records_skipped: usize,
    main_rows_written: usize,
    perf_summary: ImportPerfSummary,
    antenna_rows: HashMap<String, i32>,
    field_rows: HashMap<FieldKey, i32>,
    source_rows: HashMap<FieldKey, i32>,
    observation_rows: HashMap<String, i32>,
    observation_ranges: HashMap<i32, (f64, f64)>,
    ms_direction_epoch: Option<DirectionEpoch>,
    spectral_windows: Vec<SpectralWindowEntry>,
    current_freq_group: i32,
    pending_record_freq_group: Option<i32>,
    polarization_rows: HashMap<PolarizationKey, i32>,
    data_description_rows: HashMap<DataDescriptionKey, i32>,
    doppler_rows: Vec<((i32, SpectralDescriptor), i32)>,
    last_source_row: Option<i32>,
    next_scan_number: i32,
    new_scan_pending: bool,
    current_scan_by_array: HashMap<i32, i32>,
    last_field_by_array: HashMap<i32, i32>,
    last_data_desc_ids_by_array: HashMap<i32, Vec<i32>>,
}

impl MsImportWriter {
    fn new(options: ImportVlaOptions, ms: MeasurementSet) -> Result<Self, VlaError> {
        let mut writer = Self {
            options,
            ms,
            logical_records_seen: 0,
            logical_records_imported: 0,
            logical_records_skipped: 0,
            main_rows_written: 0,
            perf_summary: ImportPerfSummary::default(),
            antenna_rows: HashMap::new(),
            field_rows: HashMap::new(),
            source_rows: HashMap::new(),
            observation_rows: HashMap::new(),
            observation_ranges: HashMap::new(),
            ms_direction_epoch: None,
            spectral_windows: Vec::new(),
            current_freq_group: 0,
            pending_record_freq_group: None,
            polarization_rows: HashMap::new(),
            data_description_rows: HashMap::new(),
            doppler_rows: Vec::new(),
            last_source_row: None,
            next_scan_number: 1,
            new_scan_pending: true,
            current_scan_by_array: HashMap::new(),
            last_field_by_array: HashMap::new(),
            last_data_desc_ids_by_array: HashMap::new(),
        };
        writer.initialize_epoch_measure_references()?;
        writer.initialize_vla_spectral_window_columns()?;
        Ok(writer)
    }

    fn note_skipped_record(&mut self) {
        self.new_scan_pending = true;
    }

    fn initialize_epoch_measure_references(&mut self) -> Result<(), VlaError> {
        for column in ["TIME", "TIME_CENTROID"] {
            set_epoch_measure_reference(self.ms.main_table_mut(), column, EpochRef::TAI)?;
        }
        {
            let mut field = self.ms.field_mut().map_err(|error| {
                VlaError::import(format!("open FIELD for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(field.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut feed = self.ms.feed_mut().map_err(|error| {
                VlaError::import(format!("open FEED for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(feed.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut source = self.ms.source_mut().map_err(|error| {
                VlaError::import(format!("open SOURCE for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(source.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut observation = self.ms.observation_mut().map_err(|error| {
                VlaError::import(format!(
                    "open OBSERVATION for epoch metadata update: {error}"
                ))
            })?;
            for column in ["TIME_RANGE", "RELEASE_DATE"] {
                set_epoch_measure_reference(observation.table_mut(), column, EpochRef::TAI)?;
            }
        }
        Ok(())
    }

    fn initialize_vla_spectral_window_columns(&mut self) -> Result<(), VlaError> {
        let mut spw = self.ms.spectral_window_mut().map_err(|error| {
            VlaError::import(format!(
                "open SPECTRAL_WINDOW for VLA schema initialization: {error}"
            ))
        })?;
        if spw
            .as_ref()
            .table()
            .schema()
            .is_none_or(|schema| !schema.contains_column("DOPPLER_ID"))
        {
            spw.table_mut()
                .add_column(
                    ColumnSchema::scalar("DOPPLER_ID", PrimitiveType::Int32),
                    Some(Value::Scalar(ScalarValue::Int32(-1))),
                )
                .map_err(|error| {
                    VlaError::import(format!("add SPECTRAL_WINDOW.DOPPLER_ID column: {error}"))
                })?;
        }
        Ok(())
    }

    fn push_record(&mut self, record: NormalizedRecord) -> Result<(), VlaError> {
        let mut new_scan = self.new_scan_pending;
        self.new_scan_pending = false;
        self.pending_record_freq_group = None;
        let ms_direction_epoch = self.ensure_direction_epoch(record.direction_epoch)?;
        let source_direction = align_direction_for_epoch(
            record.source_direction,
            record.direction_epoch,
            ms_direction_epoch,
        )?;
        let observation_id = self.ensure_observation(&record)?;
        let field_key = FieldKey {
            source_name: record.source_name.clone(),
            calibration_code: record.calibration_code.clone(),
            ra_bits: source_direction[0].to_bits(),
            dec_bits: source_direction[1].to_bits(),
        };
        let source_id = self.ensure_source(&record, &field_key, source_direction)?;
        let field_id = self.ensure_field(&record, &field_key, source_direction, source_id)?;
        if self.last_field_by_array.get(&record.array_id).copied() != Some(field_id) {
            new_scan = true;
            self.last_field_by_array.insert(record.array_id, field_id);
        }

        let array_id = record.array_id;
        let time_seconds = record.time_seconds;
        let integration_seconds = record.integration_seconds;
        let antennas = record.antennas;
        let mut sorted_antennas = antennas.iter().collect::<Vec<_>>();
        sorted_antennas.sort_by_key(|antenna| antenna.antenna_id);
        let mut antenna_added = false;
        for antenna in sorted_antennas {
            let (_, inserted) = self.ensure_antenna(antenna)?;
            antenna_added |= inserted;
        }
        new_scan |= antenna_added;

        let mut group_data_desc_ids = Vec::with_capacity(record.groups.len());
        let mut group_rows = Vec::with_capacity(record.groups.len());

        for group in record.groups {
            let polarization_id = self.ensure_polarization(&group.baselines[0].corr_types)?;
            let spectral_window_id = self.ensure_spectral_window(
                &group.descriptor,
                source_id,
                time_seconds,
                source_direction,
                ms_direction_epoch,
            )?;
            let data_desc_id = self.ensure_data_description(spectral_window_id, polarization_id)?;
            group_data_desc_ids.push(data_desc_id);
            group_rows.push((group.baselines, data_desc_id));
        }

        if self
            .last_data_desc_ids_by_array
            .get(&array_id)
            .is_none_or(|existing| existing != &group_data_desc_ids)
        {
            new_scan = true;
            self.last_data_desc_ids_by_array
                .insert(array_id, group_data_desc_ids);
        }

        let scan_number = if new_scan {
            let scan_number = self.next_scan_number;
            self.next_scan_number += 1;
            self.current_scan_by_array.insert(array_id, scan_number);
            scan_number
        } else {
            *self.current_scan_by_array.get(&array_id).ok_or_else(|| {
                VlaError::import(format!(
                    "missing current scan number for subarray {}",
                    array_id
                ))
            })?
        };

        for (baselines, data_desc_id) in group_rows {
            for baseline in baselines {
                let antenna1_id = *self
                    .antenna_rows
                    .get(antennas[baseline.antenna1_archive].name.as_str())
                    .ok_or_else(|| VlaError::import("missing ANTENNA row for baseline ANTENNA1"))?;
                let antenna2_id = *self
                    .antenna_rows
                    .get(antennas[baseline.antenna2_archive].name.as_str())
                    .ok_or_else(|| VlaError::import("missing ANTENNA row for baseline ANTENNA2"))?;
                self.add_main_row(
                    antenna1_id,
                    antenna2_id,
                    field_id,
                    observation_id,
                    data_desc_id,
                    scan_number,
                    array_id,
                    time_seconds,
                    integration_seconds,
                    baseline,
                )?;
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), VlaError> {
        let finish_started = Instant::now();
        for (&row, &(start, end)) in &self.observation_ranges {
            let mut obs = self.ms.observation_mut().map_err(|error| {
                VlaError::import(format!("open OBSERVATION for update: {error}"))
            })?;
            let row_index =
                usize::try_from(row).map_err(|_| VlaError::import("negative OBSERVATION row"))?;
            obs.set_array(
                row_index,
                "TIME_RANGE",
                ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![start, end]).map_err(|error| {
                        VlaError::import(format!("shape observation time range: {error}"))
                    })?,
                ),
            )
            .map_err(|error| VlaError::import(format!("update OBSERVATION.TIME_RANGE: {error}")))?;
            obs.set_f64(
                row_index,
                "RELEASE_DATE",
                end + 1.5 * SECONDS_PER_JULIAN_YEAR,
            )
            .map_err(|error| {
                VlaError::import(format!("update OBSERVATION.RELEASE_DATE: {error}"))
            })?;
        }
        self.perf_summary.finish_metadata_ns = finish_started.elapsed().as_nanos() as u64;
        let save_started = Instant::now();
        let result = self
            .ms
            .save_assuming_valid()
            .map_err(|error| VlaError::import(format!("save MeasurementSet: {error}")));
        self.perf_summary.save_ns = save_started.elapsed().as_nanos() as u64;
        result
    }

    fn ensure_direction_epoch(
        &mut self,
        epoch: DirectionEpoch,
    ) -> Result<DirectionEpoch, VlaError> {
        if let Some(existing) = self.ms_direction_epoch {
            return Ok(existing);
        }
        set_direction_measure_reference(self.ms.main_table_mut(), "UVW", epoch)?;
        {
            let mut field = self.ms.field_mut().map_err(|error| {
                VlaError::import(format!("open FIELD for metadata update: {error}"))
            })?;
            for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
                set_direction_measure_reference(field.table_mut(), column, epoch)?;
            }
        }
        {
            let mut source = self.ms.source_mut().map_err(|error| {
                VlaError::import(format!("open SOURCE for metadata update: {error}"))
            })?;
            set_direction_measure_reference(source.table_mut(), "DIRECTION", epoch)?;
        }
        self.ms_direction_epoch = Some(epoch);
        Ok(epoch)
    }

    fn ensure_antenna(&mut self, antenna: &NormalizedAntenna) -> Result<(i32, bool), VlaError> {
        if let Some(&row) = self.antenna_rows.get(&antenna.name) {
            return Ok((row, false));
        }
        let row = {
            let mut ant = self
                .ms
                .antenna_mut()
                .map_err(|error| VlaError::import(format!("open ANTENNA for write: {error}")))?;
            i32::try_from(
                ant.add_antenna(
                    &antenna.name,
                    &antenna.station,
                    "GROUND-BASED",
                    "ALT-AZ",
                    antenna.position_itrf_m,
                    [0.0, 0.0, 0.0],
                    25.0,
                )
                .map_err(|error| VlaError::import(format!("add ANTENNA row: {error}")))?,
            )
            .map_err(|_| VlaError::import("ANTENNA row index does not fit in i32"))?
        };
        self.add_feed_row(row)?;
        self.antenna_rows.insert(antenna.name.clone(), row);
        Ok((row, true))
    }

    fn add_feed_row(&mut self, antenna_id: i32) -> Result<(), VlaError> {
        let feed_table = self
            .ms
            .subtable_mut(SubtableId::Feed)
            .ok_or_else(|| VlaError::import("missing FEED subtable"))?;
        let pol_response = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .map_err(|error| VlaError::import(format!("shape FEED.POL_RESPONSE: {error}")))?,
        );
        let polarization_type = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec!["R".to_string(), "L".to_string()])
                .map_err(|error| {
                    VlaError::import(format!("shape FEED.POLARIZATION_TYPE: {error}"))
                })?,
        );
        let receptor_angle = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0, 0.0])
                .map_err(|error| VlaError::import(format!("shape FEED.RECEPTOR_ANGLE: {error}")))?,
        );
        let beam_offset = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![0.0; 4])
                .map_err(|error| VlaError::import(format!("shape FEED.BEAM_OFFSET: {error}")))?,
        );
        let position = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[3]).f(), vec![0.0, 0.0, 0.0])
                .map_err(|error| VlaError::import(format!("shape FEED.POSITION: {error}")))?,
        );
        let row = make_row_from_columns(
            casa_ms::schema::feed::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna_id))),
                ("BEAM_ID", Value::Scalar(ScalarValue::Int32(-1))),
                ("BEAM_OFFSET", Value::Array(beam_offset)),
                ("FEED_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("INTERVAL", Value::Scalar(ScalarValue::Float64(0.0))),
                ("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(2))),
                ("POL_RESPONSE", Value::Array(pol_response)),
                ("POLARIZATION_TYPE", Value::Array(polarization_type)),
                ("POSITION", Value::Array(position)),
                ("RECEPTOR_ANGLE", Value::Array(receptor_angle)),
                ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(-1))),
                ("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
            ],
        )?;
        feed_table
            .add_row_assuming_valid(row)
            .map_err(|error| VlaError::import(format!("add FEED row: {error}")))
    }

    fn ensure_observation(&mut self, record: &NormalizedRecord) -> Result<i32, VlaError> {
        if let Some(&row) = self.observation_rows.get(&record.project) {
            self.update_observation_range(row, record.time_seconds, record.integration_seconds);
            return Ok(row);
        }
        let log = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec!["unavailable".to_string()])
                .map_err(|error| VlaError::import(format!("shape OBSERVATION.LOG: {error}")))?,
        );
        let schedule = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec!["unavailable".to_string()]).map_err(
                |error| VlaError::import(format!("shape OBSERVATION.SCHEDULE: {error}")),
            )?,
        );
        let time_range = ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2]).f(),
                vec![record.time_seconds, record.time_seconds],
            )
            .map_err(|error| VlaError::import(format!("shape OBSERVATION.TIME_RANGE: {error}")))?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Observation)
                .ok_or_else(|| VlaError::import("missing OBSERVATION subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("OBSERVATION row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::observation::REQUIRED_COLUMNS,
                &[
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                    ("LOG", Value::Array(log)),
                    (
                        "OBSERVER",
                        Value::Scalar(ScalarValue::String("unavailable".to_string())),
                    ),
                    (
                        "PROJECT",
                        Value::Scalar(ScalarValue::String(record.project.clone())),
                    ),
                    ("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(0.0))),
                    ("SCHEDULE", Value::Array(schedule)),
                    (
                        "SCHEDULE_TYPE",
                        Value::Scalar(ScalarValue::String("unknown".to_string())),
                    ),
                    (
                        "TELESCOPE_NAME",
                        Value::Scalar(ScalarValue::String(DEFAULT_TELESCOPE_NAME.to_string())),
                    ),
                    ("TIME_RANGE", Value::Array(time_range)),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add OBSERVATION row: {error}")))?;
            row_index
        };
        self.observation_rows.insert(record.project.clone(), row);
        self.update_observation_range(row, record.time_seconds, record.integration_seconds);
        Ok(row)
    }

    fn update_observation_range(&mut self, row: i32, time_seconds: f64, _integration_seconds: f64) {
        self.observation_ranges
            .entry(row)
            .and_modify(|range| {
                range.0 = range.0.min(time_seconds);
                range.1 = range.1.max(time_seconds);
            })
            .or_insert((time_seconds, time_seconds));
    }

    fn ensure_source(
        &mut self,
        record: &NormalizedRecord,
        field_key: &FieldKey,
        source_direction: [f64; 2],
    ) -> Result<i32, VlaError> {
        if let Some(&row) = self.source_rows.get(field_key) {
            return Ok(row);
        }
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2]).f(),
                vec![source_direction[0], source_direction[1]],
            )
            .map_err(|error| VlaError::import(format!("shape SOURCE.DIRECTION: {error}")))?,
        );
        let proper_motion = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0, 0.0]).map_err(|error| {
                VlaError::import(format!("shape SOURCE.PROPER_MOTION: {error}"))
            })?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Source)
                .ok_or_else(|| VlaError::import("missing SOURCE subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("SOURCE row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::source::REQUIRED_COLUMNS,
                &[
                    ("CALIBRATION_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                    (
                        "CODE",
                        Value::Scalar(ScalarValue::String(record.calibration_code.clone())),
                    ),
                    ("DIRECTION", Value::Array(direction)),
                    ("INTERVAL", Value::Scalar(ScalarValue::Float64(0.0))),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(record.source_name.clone())),
                    ),
                    (
                        "NUM_LINES",
                        Value::Scalar(ScalarValue::Int32(record.source_num_lines)),
                    ),
                    ("PROPER_MOTION", Value::Array(proper_motion)),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(row_index))),
                    ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(-1))),
                    (
                        "TIME",
                        Value::Scalar(ScalarValue::Float64(record.time_seconds)),
                    ),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add SOURCE row: {error}")))?;
            row_index
        };
        self.source_rows.insert(field_key.clone(), row);
        self.last_source_row = Some(row);
        Ok(row)
    }

    fn ensure_field(
        &mut self,
        record: &NormalizedRecord,
        field_key: &FieldKey,
        source_direction: [f64; 2],
        source_id: i32,
    ) -> Result<i32, VlaError> {
        if let Some(&row) = self.field_rows.get(field_key) {
            return Ok(row);
        }
        let dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 1]).f(),
                vec![source_direction[0], source_direction[1]],
            )
            .map_err(|error| VlaError::import(format!("shape FIELD direction array: {error}")))?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Field)
                .ok_or_else(|| VlaError::import("missing FIELD subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("FIELD row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::field::REQUIRED_COLUMNS,
                &[
                    (
                        "CODE",
                        Value::Scalar(ScalarValue::String(record.calibration_code.clone())),
                    ),
                    ("DELAY_DIR", Value::Array(dir.clone())),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(record.source_name.clone())),
                    ),
                    ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                    ("PHASE_DIR", Value::Array(dir.clone())),
                    ("REFERENCE_DIR", Value::Array(dir)),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
                    (
                        "TIME",
                        Value::Scalar(ScalarValue::Float64(record.time_seconds)),
                    ),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add FIELD row: {error}")))?;
            row_index
        };
        self.field_rows.insert(field_key.clone(), row);
        Ok(row)
    }

    fn ensure_polarization(&mut self, corr_types: &[i32]) -> Result<i32, VlaError> {
        let key = PolarizationKey {
            corr_types: corr_types.to_vec(),
        };
        if let Some(&row) = self.polarization_rows.get(&key) {
            return Ok(row);
        }
        let corr_product = corr_product_array(corr_types)?;
        let corr_type = ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[corr_types.len()]).f(), corr_types.to_vec()).map_err(
                |error| VlaError::import(format!("shape POLARIZATION.CORR_TYPE: {error}")),
            )?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Polarization)
                .ok_or_else(|| VlaError::import("missing POLARIZATION subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("POLARIZATION row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::polarization::REQUIRED_COLUMNS,
                &[
                    ("CORR_PRODUCT", Value::Array(corr_product)),
                    ("CORR_TYPE", Value::Array(corr_type)),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                    (
                        "NUM_CORR",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(corr_types.len())
                                .map_err(|_| VlaError::import("NUM_CORR does not fit in i32"))?,
                        )),
                    ),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add POLARIZATION row: {error}")))?;
            row_index
        };
        self.polarization_rows.insert(key, row);
        Ok(row)
    }

    fn ensure_doppler(
        &mut self,
        source_id: i32,
        descriptor: &SpectralDescriptor,
    ) -> Result<i32, VlaError> {
        let source_id = self.last_source_row.unwrap_or(source_id);
        if let Some((_, row)) = self
            .doppler_rows
            .iter()
            .find(|((existing_source, existing), _)| {
                *existing_source == source_id
                    && same_spectral_descriptor(existing, descriptor, self.options.frequencytol_hz)
            })
        {
            return Ok(*row);
        }
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Doppler)
                .ok_or_else(|| VlaError::import("missing DOPPLER subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("DOPPLER row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::doppler::REQUIRED_COLUMNS,
                &[
                    ("DOPPLER_ID", Value::Scalar(ScalarValue::Int32(row_index))),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
                    ("TRANSITION_ID", Value::Scalar(ScalarValue::Int32(0))),
                    (
                        "VELDEF",
                        Value::Scalar(ScalarValue::Float64(if descriptor.doppler_tracking {
                            descriptor.doppler_velocity_mps
                        } else {
                            0.0
                        })),
                    ),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add DOPPLER row: {error}")))?;
            row_index
        };
        self.doppler_rows
            .push(((source_id, descriptor.clone()), row));
        Ok(row)
    }

    fn ensure_spectral_window(
        &mut self,
        descriptor: &SpectralDescriptor,
        source_id: i32,
        time_seconds: f64,
        source_direction: [f64; 2],
        direction_epoch: DirectionEpoch,
    ) -> Result<i32, VlaError> {
        for entry in &self.spectral_windows {
            if spectral_window_matches(
                entry,
                descriptor,
                time_seconds,
                source_direction,
                direction_epoch,
                self.options.frequencytol_hz,
            )? {
                return Ok(entry.row);
            }
        }
        let freq_group = if let Some(group) = self.pending_record_freq_group {
            group
        } else {
            self.current_freq_group += 1;
            self.pending_record_freq_group = Some(self.current_freq_group);
            self.current_freq_group
        };

        let frame = spectral_conversion_frame(time_seconds, source_direction, direction_epoch)?;
        let ref_freq = spectral_reference_frequency(descriptor);
        let meas_freq_ref = frequency_reference_for_descriptor(descriptor);
        let chan_freq: Vec<f64> = (0..descriptor.num_chan)
            .map(|index| {
                descriptor.edge_frequency_hz + (index as f64 + 0.5) * descriptor.channel_width_hz
            })
            .map(|hz| convert_topocentric_frequency(hz, meas_freq_ref, &frame))
            .collect::<Result<Vec<_>, _>>()?;
        let converted_width = if meas_freq_ref == FrequencyRef::TOPO {
            descriptor.channel_width_hz
        } else {
            convert_topocentric_frequency(descriptor.channel_width_hz, meas_freq_ref, &frame)?
        };
        let widths = vec![converted_width; descriptor.num_chan];
        let resolution = widths.clone();
        let chan_freq_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[descriptor.num_chan]).f(), chan_freq).map_err(
                |error| VlaError::import(format!("shape SPECTRAL_WINDOW.CHAN_FREQ: {error}")),
            )?,
        );
        let width_array = |values: Vec<f64>, column: &str| -> Result<ArrayValue, VlaError> {
            Ok(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[descriptor.num_chan]).f(), values).map_err(
                    |error| VlaError::import(format!("shape SPECTRAL_WINDOW.{column}: {error}")),
                )?,
            ))
        };
        let doppler_id = self.ensure_doppler(source_id, descriptor)?;
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::SpectralWindow)
                .ok_or_else(|| VlaError::import("missing SPECTRAL_WINDOW subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("SPECTRAL_WINDOW row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    (
                        "NUM_CHAN",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(descriptor.num_chan)
                                .map_err(|_| VlaError::import("NUM_CHAN does not fit in i32"))?,
                        )),
                    ),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(spectral_window_name(
                            descriptor,
                            ref_freq.hz(),
                            meas_freq_ref,
                        ))),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(ref_freq.hz())),
                    ),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(widths.iter().sum())),
                    ),
                    ("CHAN_FREQ", Value::Array(chan_freq_array)),
                    (
                        "CHAN_WIDTH",
                        Value::Array(width_array(widths.clone(), "CHAN_WIDTH")?),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(width_array(widths, "EFFECTIVE_BW")?),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(width_array(resolution, "RESOLUTION")?),
                    ),
                    (
                        "MEAS_FREQ_REF",
                        Value::Scalar(ScalarValue::Int32(meas_freq_ref.casacore_code())),
                    ),
                    ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                    ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(freq_group))),
                    (
                        "FREQ_GROUP_NAME",
                        Value::Scalar(ScalarValue::String(format!("Group {freq_group}"))),
                    ),
                    (
                        "IF_CONV_CHAIN",
                        Value::Scalar(ScalarValue::Int32(descriptor.if_conv_chain)),
                    ),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add SPECTRAL_WINDOW row: {error}")))?;
            row_index
        };
        {
            let mut spw = self.ms.spectral_window_mut().map_err(|error| {
                VlaError::import(format!("open SPECTRAL_WINDOW mutator: {error}"))
            })?;
            let has_doppler_id = spw
                .as_ref()
                .table()
                .schema()
                .map(|schema| schema.contains_column("DOPPLER_ID"))
                .unwrap_or(false);
            if has_doppler_id {
                spw.set_i32(
                    usize::try_from(row).map_err(|_| VlaError::import("negative SPW row"))?,
                    "DOPPLER_ID",
                    doppler_id,
                )
                .map_err(|error| VlaError::import(format!("set SPW.DOPPLER_ID: {error}")))?;
            }
        }
        self.spectral_windows.push(SpectralWindowEntry {
            descriptor: descriptor.clone(),
            row,
            time_seconds,
            source_direction,
            direction_epoch,
        });
        Ok(row)
    }

    fn ensure_data_description(
        &mut self,
        spectral_window_id: i32,
        polarization_id: i32,
    ) -> Result<i32, VlaError> {
        let key = DataDescriptionKey {
            spectral_window_id,
            polarization_id,
        };
        if let Some(&row) = self.data_description_rows.get(&key) {
            return Ok(row);
        }
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::DataDescription)
                .ok_or_else(|| VlaError::import("missing DATA_DESCRIPTION subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("DATA_DESCRIPTION row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::data_description::REQUIRED_COLUMNS,
                &[
                    (
                        "SPECTRAL_WINDOW_ID",
                        Value::Scalar(ScalarValue::Int32(spectral_window_id)),
                    ),
                    (
                        "POLARIZATION_ID",
                        Value::Scalar(ScalarValue::Int32(polarization_id)),
                    ),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            )?;
            table
                .add_row_assuming_valid(row)
                .map_err(|error| VlaError::import(format!("add DATA_DESCRIPTION row: {error}")))?;
            row_index
        };
        self.data_description_rows.insert(key, row);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_main_row(
        &mut self,
        antenna1_id: i32,
        antenna2_id: i32,
        field_id: i32,
        observation_id: i32,
        data_desc_id: i32,
        scan_number: i32,
        array_id: i32,
        time_seconds: f64,
        integration_seconds: f64,
        baseline: NormalizedBaselineRow,
    ) -> Result<(), VlaError> {
        let NormalizedBaselineRow {
            corr_types,
            uvw_m,
            data,
            corrected_data,
            model_data,
            flag,
            flag_category,
            weight,
            sigma,
            ..
        } = baseline;
        let (
            row_ant1,
            row_ant2,
            row_data,
            row_corrected,
            row_model,
            row_flag,
            row_flag_category,
            row_weight,
            row_sigma,
            _row_corr_types,
        ) = {
            let reorder_started = Instant::now();
            let result = reorder_baseline_for_ms(
                antenna1_id,
                antenna2_id,
                data,
                corrected_data,
                model_data,
                flag,
                flag_category,
                weight,
                sigma,
                &corr_types,
            )?;
            self.perf_summary.reorder_baseline_ns += reorder_started.elapsed().as_nanos() as u64;
            result
        };
        let row_uvw = if row_ant1 == antenna1_id && row_ant2 == antenna2_id {
            uvw_m
        } else {
            [-uvw_m[0], -uvw_m[1], -uvw_m[2]]
        };
        let row_flag_row = {
            let flag_started = Instant::now();
            let value = flag_row_value(&row_flag)?;
            self.perf_summary.flag_row_ns += flag_started.elapsed().as_nanos() as u64;
            value
        };
        let row = {
            let make_started = Instant::now();
            let row = make_main_row(
                &self.ms,
                &[
                    ("ANTENNA1", Value::Scalar(ScalarValue::Int32(row_ant1))),
                    ("ANTENNA2", Value::Scalar(ScalarValue::Int32(row_ant2))),
                    ("ARRAY_ID", Value::Scalar(ScalarValue::Int32(array_id))),
                    (
                        "DATA_DESC_ID",
                        Value::Scalar(ScalarValue::Int32(data_desc_id)),
                    ),
                    (
                        "EXPOSURE",
                        Value::Scalar(ScalarValue::Float64(integration_seconds)),
                    ),
                    ("FEED1", Value::Scalar(ScalarValue::Int32(0))),
                    ("FEED2", Value::Scalar(ScalarValue::Int32(0))),
                    ("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
                    ("FLAG", Value::Array(row_flag)),
                    ("FLAG_CATEGORY", Value::Array(row_flag_category)),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(row_flag_row))),
                    (
                        "INTERVAL",
                        Value::Scalar(ScalarValue::Float64(integration_seconds)),
                    ),
                    (
                        "OBSERVATION_ID",
                        Value::Scalar(ScalarValue::Int32(observation_id)),
                    ),
                    ("PROCESSOR_ID", Value::Scalar(ScalarValue::Int32(-1))),
                    (
                        "SCAN_NUMBER",
                        Value::Scalar(ScalarValue::Int32(scan_number)),
                    ),
                    ("SIGMA", Value::Array(row_sigma)),
                    ("STATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                    ("TIME", Value::Scalar(ScalarValue::Float64(time_seconds))),
                    (
                        "TIME_CENTROID",
                        Value::Scalar(ScalarValue::Float64(time_seconds)),
                    ),
                    (
                        "UVW",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(IxDyn(&[3]).f(), row_uvw.to_vec()).map_err(
                                |error| VlaError::import(format!("shape UVW array: {error}")),
                            )?,
                        )),
                    ),
                    ("WEIGHT", Value::Array(row_weight)),
                    ("DATA", Value::Array(row_data)),
                    ("CORRECTED_DATA", Value::Array(row_corrected)),
                    ("MODEL_DATA", Value::Array(row_model)),
                ],
            )?;
            self.perf_summary.make_main_row_ns += make_started.elapsed().as_nanos() as u64;
            row
        };
        let append_started = Instant::now();
        self.ms
            .main_table_mut()
            .add_row_assuming_valid(row)
            .map_err(|error| VlaError::import(format!("add MAIN row: {error}")))?;
        self.perf_summary.append_main_row_ns += append_started.elapsed().as_nanos() as u64;
        self.main_rows_written += 1;
        Ok(())
    }
}

fn align_direction_for_epoch(
    source_direction: [f64; 2],
    source_epoch: DirectionEpoch,
    target_epoch: DirectionEpoch,
) -> Result<[f64; 2], VlaError> {
    if source_epoch == target_epoch {
        return Ok(source_direction);
    }
    Err(VlaError::import(format!(
        "mixed direction epochs are not implemented yet: source={source_epoch:?} target={target_epoch:?}"
    )))
}

fn normalize_longitude_radians(value: f64) -> f64 {
    let two_pi = 2.0 * PI;
    let wrapped = (value + PI).rem_euclid(two_pi) - PI;
    if wrapped == -PI { PI } else { wrapped }
}

fn direction_measure_ref(epoch: DirectionEpoch) -> &'static str {
    match epoch {
        DirectionEpoch::J2000 => "J2000",
        DirectionEpoch::B1950Vla => "B1950_VLA",
        DirectionEpoch::Apparent => "APP",
        DirectionEpoch::Unknown(_) => "J2000",
    }
}

fn set_direction_measure_reference(
    table: &mut Table,
    column: &str,
    epoch: DirectionEpoch,
) -> Result<(), VlaError> {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let Some(Value::Record(mut measinfo)) = keywords.get("MEASINFO").cloned() else {
        return Err(VlaError::import(format!(
            "{column} column is missing MEASINFO keywords"
        )));
    };
    measinfo.upsert(
        "Ref",
        Value::Scalar(ScalarValue::String(
            direction_measure_ref(epoch).to_string(),
        )),
    );
    keywords.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords(column, keywords);
    Ok(())
}

fn set_epoch_measure_reference(
    table: &mut Table,
    column: &str,
    epoch_ref: EpochRef,
) -> Result<(), VlaError> {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let Some(Value::Record(mut measinfo)) = keywords.get("MEASINFO").cloned() else {
        return Err(VlaError::import(format!(
            "{column} column is missing MEASINFO keywords"
        )));
    };
    measinfo.upsert(
        "Ref",
        Value::Scalar(ScalarValue::String(epoch_ref.to_string())),
    );
    keywords.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords(column, keywords);
    Ok(())
}

fn set_flag_category_names(table: &mut Table) -> Result<(), VlaError> {
    let mut keywords = table
        .column_keywords("FLAG_CATEGORY")
        .cloned()
        .unwrap_or_default();
    keywords.upsert(
        "CATEGORY",
        Value::Array(ArrayValue::from_string_vec(
            FLAG_CATEGORY_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )),
    );
    table.set_column_keywords("FLAG_CATEGORY", keywords);
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn reorder_baseline_for_ms(
    antenna1_id: i32,
    antenna2_id: i32,
    data: ArrayValue,
    corrected: ArrayValue,
    model: ArrayValue,
    flag: ArrayValue,
    flag_category: ArrayValue,
    weight: ArrayValue,
    sigma: ArrayValue,
    corr_types: &[i32],
) -> Result<
    (
        i32,
        i32,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        Vec<i32>,
    ),
    VlaError,
> {
    if antenna1_id <= antenna2_id {
        return Ok((
            antenna1_id,
            antenna2_id,
            data,
            corrected,
            model,
            flag,
            flag_category,
            weight,
            sigma,
            corr_types.to_vec(),
        ));
    }

    let swapped_corr_types = swap_rl_lr(corr_types);
    Ok((
        antenna2_id,
        antenna1_id,
        reorder_complex_array(&data, corr_types, &swapped_corr_types)?,
        reorder_complex_array(&corrected, corr_types, &swapped_corr_types)?,
        reorder_complex_array(&model, corr_types, &swapped_corr_types)?,
        reorder_flag_array_for_swapped_baseline(&flag, corr_types, &swapped_corr_types)?,
        flag_category,
        reorder_vector_array(&weight, corr_types, &swapped_corr_types)?,
        reorder_vector_array(&sigma, corr_types, &swapped_corr_types)?,
        swapped_corr_types,
    ))
}

fn swap_rl_lr(corr_types: &[i32]) -> Vec<i32> {
    corr_types
        .iter()
        .map(|code| match code {
            6 => 7,
            7 => 6,
            _ => *code,
        })
        .collect()
}

fn reorder_complex_array(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(VlaError::import("expected Complex32 data array"));
    };
    let shape = array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import("complex array missing correlation axis"))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import("complex array missing channel axis"))?;
    let source = array
        .as_slice_memory_order()
        .ok_or_else(|| VlaError::import("complex array is not contiguous in memory order"))?;
    let reorder_map = reorder_index_map(current, target, "source array")?;
    let mut reordered = vec![Complex32::new(0.0, 0.0); n_corr * n_chan];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr].conj();
        }
    }
    Ok(ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered complex array: {error}")))?,
    ))
}

fn reorder_flag_array_for_swapped_baseline(
    flag: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    if !(current.contains(&6) && current.contains(&7)) {
        return reorder_bool_array_generic(flag, current, target, "flag");
    }
    let ArrayValue::Bool(flag_array) = flag else {
        return Err(VlaError::import("expected Bool flag array"));
    };
    let shape = flag_array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import("flag array missing correlation axis"))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import("flag array missing channel axis"))?;
    let source = flag_array
        .as_slice_memory_order()
        .ok_or_else(|| VlaError::import("flag array is not contiguous in memory order"))?;
    let old_lr_index = current
        .iter()
        .position(|code| *code == 7)
        .ok_or_else(|| VlaError::import("swapped flag array missing LR correlation"))?;
    let reorder_map = reorder_index_map(current, target, "flag array")?;
    let mut reordered = vec![false; n_corr * n_chan];
    for (new_index, &mapped_index) in reorder_map.iter().enumerate() {
        let old_index = if target[new_index] == 6 {
            old_lr_index
        } else {
            mapped_index
        };
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr];
        }
    }
    Ok(ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered flag array: {error}")))?,
    ))
}

fn reorder_bool_array_generic(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
    label: &str,
) -> Result<ArrayValue, VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(VlaError::import(format!("expected Bool {label} array")));
    };
    let shape = array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import(format!("{label} array missing correlation axis")))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import(format!("{label} array missing channel axis")))?;
    let source = array.as_slice_memory_order().ok_or_else(|| {
        VlaError::import(format!("{label} array is not contiguous in memory order"))
    })?;
    let reorder_map = reorder_index_map(current, target, label)?;
    let mut reordered = vec![false; n_corr * n_chan];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr];
        }
    }
    Ok(ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered {label} array: {error}")))?,
    ))
}

fn flag_row_value(flag: &ArrayValue) -> Result<bool, VlaError> {
    let ArrayValue::Bool(array) = flag else {
        return Err(VlaError::import("expected Bool flag array"));
    };
    Ok(array.iter().all(|flagged| *flagged))
}

fn spectral_window_name(
    descriptor: &SpectralDescriptor,
    ref_frequency_hz: f64,
    frequency_ref: FrequencyRef,
) -> String {
    let (chan_width_value, chan_width_unit) = if descriptor.channel_width_hz < 1.0e6 {
        (descriptor.channel_width_hz / 1.0e3, "kHz")
    } else {
        (descriptor.channel_width_hz / 1.0e6, "MHz")
    };
    let (ref_freq_value, ref_freq_unit) = if ref_frequency_hz / 1.0e9 < 1.0 {
        (ref_frequency_hz / 1.0e6, "MHz")
    } else {
        (ref_frequency_hz / 1.0e9, "GHz")
    };
    format!(
        "{}*{} {} channels @ {} {} ({})",
        descriptor.num_chan,
        format_sigfigs(chan_width_value, 3),
        chan_width_unit,
        format_sigfigs(ref_freq_value, 3),
        ref_freq_unit,
        frequency_ref.as_str()
    )
}

fn spectral_reference_frequency(descriptor: &SpectralDescriptor) -> MFrequency {
    let refer = frequency_reference_for_descriptor(descriptor);
    if descriptor.doppler_tracking {
        let doppler = doppler_measure_from_descriptor(descriptor);
        MFrequency::new(
            doppler.shift_frequency_hz(descriptor.rest_frequency_hz),
            refer,
        )
    } else {
        MFrequency::new(descriptor.observed_frequency_hz, refer)
    }
}

fn doppler_measure_from_descriptor(descriptor: &SpectralDescriptor) -> MDoppler {
    let value = descriptor.doppler_velocity_mps / SPEED_OF_LIGHT_M_PER_S;
    MDoppler::new(
        value,
        doppler_reference_for_definition(descriptor.doppler_definition),
    )
}

fn frequency_reference_for_descriptor(descriptor: &SpectralDescriptor) -> FrequencyRef {
    if descriptor.doppler_tracking {
        frequency_reference_for_frame(descriptor.rest_frame)
    } else {
        FrequencyRef::TOPO
    }
}

fn frequency_reference_for_frame(frame: FrequencyFrame) -> FrequencyRef {
    match frame {
        FrequencyFrame::Topocentric => FrequencyRef::TOPO,
        FrequencyFrame::Geocentric => FrequencyRef::GEO,
        FrequencyFrame::Barycentric => FrequencyRef::BARY,
        FrequencyFrame::Lsrk => FrequencyRef::LSRK,
    }
}

fn doppler_reference_for_definition(definition: DopplerDefinition) -> DopplerRef {
    match definition {
        DopplerDefinition::Optical => DopplerRef::Z,
        DopplerDefinition::Radio | DopplerDefinition::Unknown => DopplerRef::RADIO,
    }
}

fn spectral_conversion_frame(
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
) -> Result<MeasFrame, VlaError> {
    let observatory =
        MPosition::from_observatory_name(DEFAULT_TELESCOPE_NAME).ok_or_else(|| {
            VlaError::import("resolve observatory position for VLA from measures catalog")
        })?;
    let direction = MDirection::from_angles(
        source_direction[0],
        source_direction[1],
        direction_reference_for_epoch(direction_epoch),
    );
    Ok(MeasFrame::new()
        .with_bundled_eop()
        .with_epoch(MEpoch::from_mjd(time_seconds / 86_400.0, EpochRef::TAI))
        .with_position(observatory)
        .with_direction(direction))
}

fn direction_reference_for_epoch(epoch: DirectionEpoch) -> DirectionRef {
    match epoch {
        DirectionEpoch::J2000 => DirectionRef::J2000,
        DirectionEpoch::B1950Vla => DirectionRef::B1950,
        DirectionEpoch::Apparent => DirectionRef::APP,
        DirectionEpoch::Unknown(_) => DirectionRef::J2000,
    }
}

fn convert_topocentric_frequency(
    hz: f64,
    target: FrequencyRef,
    frame: &MeasFrame,
) -> Result<f64, VlaError> {
    Ok(MFrequency::new(hz, FrequencyRef::TOPO)
        .convert_to(target, frame)
        .map_err(|error| VlaError::import(format!("convert TOPO frequency to {target}: {error}")))?
        .hz())
}

fn format_sigfigs(value: f64, sigfigs: usize) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let digits_before = value.abs().log10().floor() as i32 + 1;
    let decimals = (sigfigs as i32 - digits_before).max(0) as usize;
    let mut formatted = format!("{value:.decimals$}");
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    formatted
}

fn reorder_vector_array(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    let source = match value {
        ArrayValue::Float32(array) => array
            .as_slice_memory_order()
            .ok_or_else(|| VlaError::import("vector array is not contiguous in memory order"))?,
        _ => return Err(VlaError::import("expected Float32 vector array")),
    };
    let reorder_map = reorder_index_map(current, target, "vector")?;
    let mut reordered = vec![0.0_f32; source.len()];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        reordered[new_index] = source[old_index];
    }
    Ok(ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[reordered.len()]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered vector array: {error}")))?,
    ))
}

fn reorder_index_map(current: &[i32], target: &[i32], label: &str) -> Result<Vec<usize>, VlaError> {
    target
        .iter()
        .map(|code| {
            current
                .iter()
                .position(|existing| existing == code)
                .ok_or_else(|| {
                    VlaError::import(format!(
                        "target correlation code missing from {label} array"
                    ))
                })
        })
        .collect()
}

fn corr_product_array(corr_types: &[i32]) -> Result<ArrayValue, VlaError> {
    let mut values = Vec::with_capacity(corr_types.len() * 2);
    for &code in corr_types {
        let (r0, r1) = match code {
            5 => (0, 0),
            6 => (0, 1),
            7 => (1, 0),
            8 => (1, 1),
            _ => return Err(VlaError::import(format!("unsupported Stokes code {code}"))),
        };
        values.push(r0);
        values.push(r1);
    }
    Ok(ArrayValue::Int32(
        ArrayD::from_shape_vec(IxDyn(&[2, corr_types.len()]).f(), values)
            .map_err(|error| VlaError::import(format!("shape CORR_PRODUCT array: {error}")))?,
    ))
}

fn make_main_row(
    ms: &MeasurementSet,
    overrides: &[(&str, Value)],
) -> Result<RecordValue, VlaError> {
    let schema = ms
        .main_table()
        .schema()
        .ok_or_else(|| VlaError::import("main table has no schema"))?;
    let schema_column_names: Vec<&str> = schema
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    let all_cols: Vec<_> = casa_ms::schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(casa_ms::schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect();
    make_row_from_names(&schema_column_names, &all_cols, overrides)
}

fn make_row_from_columns(
    columns: &[casa_ms::column_def::ColumnDef],
    overrides: &[(&str, Value)],
) -> Result<RecordValue, VlaError> {
    let column_names: Vec<&str> = columns.iter().map(|column| column.name).collect();
    let all_columns: Vec<_> = columns.iter().collect();
    make_row_from_names(&column_names, &all_columns, overrides)
}

fn make_row_from_names(
    schema_column_names: &[&str],
    all_columns: &[&casa_ms::column_def::ColumnDef],
    overrides: &[(&str, Value)],
) -> Result<RecordValue, VlaError> {
    let mut fields = Vec::with_capacity(schema_column_names.len());
    for &column_name in schema_column_names {
        if let Some((_, value)) = overrides.iter().find(|(name, _)| *name == column_name) {
            fields.push(RecordField::new(column_name, value.clone()));
            continue;
        }
        let definition = all_columns
            .iter()
            .copied()
            .find(|candidate| candidate.name == column_name)
            .ok_or_else(|| {
                VlaError::import(format!("missing column definition for {column_name}"))
            })?;
        fields.push(RecordField::new(
            column_name,
            default_value_for_column(definition)?,
        ));
    }
    Ok(RecordValue::new(fields))
}

fn default_value_for_column(column: &casa_ms::column_def::ColumnDef) -> Result<Value, VlaError> {
    use casa_ms::column_def::ColumnKind;
    use casa_types::PrimitiveType;

    Ok(match column.column_kind {
        ColumnKind::Scalar => Value::Scalar(match column.data_type {
            PrimitiveType::Int32 => ScalarValue::Int32(0),
            PrimitiveType::Float32 => ScalarValue::Float32(0.0),
            PrimitiveType::Float64 => ScalarValue::Float64(0.0),
            PrimitiveType::Bool => ScalarValue::Bool(false),
            PrimitiveType::String => ScalarValue::String(String::new()),
            _ => {
                return Err(VlaError::import(format!(
                    "no default scalar value for {}",
                    column.name
                )));
            }
        }),
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(match column.data_type {
                PrimitiveType::Float64 => ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Complex32 => ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(shape).f(), vec![Complex32::new(0.0, 0.0); total])
                        .map_err(|error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        })?,
                ),
                _ => {
                    return Err(VlaError::import(format!(
                        "no default fixed-array value for {}",
                        column.name
                    )));
                }
            })
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            let total: usize = shape.iter().product();
            Value::Array(match column.data_type {
                PrimitiveType::Bool => ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![false; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Float32 => ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Float64 => ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Int32 => ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0; total]).map_err(|error| {
                        VlaError::import(format!("shape default array {}: {error}", column.name))
                    })?,
                ),
                PrimitiveType::String => ArrayValue::String(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![String::new(); total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Complex32 => ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![Complex32::new(0.0, 0.0); total],
                    )
                    .map_err(|error| {
                        VlaError::import(format!("shape default array {}: {error}", column.name))
                    })?,
                ),
                _ => {
                    return Err(VlaError::import(format!(
                        "no default variable-array value for {}",
                        column.name
                    )));
                }
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::VlaDiskReader;
    use tempfile::tempdir;

    #[test]
    fn import_perf_tracer_from_env_writes_jsonl_and_summary_log() {
        let tempdir = tempdir().expect("create perf tempdir");
        // SAFETY: test configures process-local env before constructing the tracer and
        // clears it before returning; tests in this crate do not read these keys concurrently.
        unsafe {
            std::env::set_var(PERF_ENV, "1");
            std::env::set_var(PERF_DIR_ENV, tempdir.path());
        }

        let mut tracer = ImportPerfTracer::from_env();
        let report = ImportReport {
            vis: PathBuf::from("/tmp/example.ms"),
            logical_records_seen: 10,
            logical_records_imported: 9,
            logical_records_skipped: 1,
            main_rows_written: 42,
        };
        tracer.emit_import_completed(
            &report.vis,
            &report,
            ImportPerfSummary {
                create_measurement_set_ns: 1,
                normalize_record_ns: 2,
                push_record_ns: 3,
                reorder_baseline_ns: 4,
                flag_row_ns: 5,
                make_main_row_ns: 6,
                append_main_row_ns: 7,
                finish_metadata_ns: 8,
                save_ns: 9,
            },
            40,
        );

        // SAFETY: paired cleanup for the test-local env configuration above.
        unsafe {
            std::env::remove_var(PERF_ENV);
            std::env::remove_var(PERF_DIR_ENV);
        }

        let mut json_paths = Vec::new();
        let mut log_paths = Vec::new();
        for entry in std::fs::read_dir(tempdir.path()).expect("read perf dir") {
            let path = entry.expect("read dir entry").path();
            if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                json_paths.push(path);
            } else if path.extension().is_some_and(|extension| extension == "log") {
                log_paths.push(path);
            }
        }
        assert_eq!(json_paths.len(), 1, "expected one jsonl perf trace");
        assert_eq!(log_paths.len(), 1, "expected one log perf trace");

        let json = std::fs::read_to_string(&json_paths[0]).expect("read perf jsonl");
        assert!(json.contains("\"kind\":\"import_completed\""));
        assert!(json.contains("\"main_rows_written\":42"));

        let log = std::fs::read_to_string(&log_paths[0]).expect("read perf log");
        assert!(log.contains("rows=42"));
        assert!(log.contains("save_ms="));
    }

    #[test]
    fn reorders_cross_hand_correlations_when_antennas_swap() {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1]).f(),
                vec![
                    Complex32::new(1.0, 1.0),
                    Complex32::new(2.0, 2.0),
                    Complex32::new(3.0, 3.0),
                    Complex32::new(4.0, 4.0),
                ],
            )
            .unwrap(),
        );
        let flag =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
        let weight = ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        );
        let sigma = ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![5.0, 6.0, 7.0, 8.0]).unwrap(),
        );

        let (
            a1,
            a2,
            reordered,
            _,
            _,
            reordered_flag,
            reordered_flag_category,
            reordered_weight,
            reordered_sigma,
            corr_types,
        ) = reorder_baseline_for_ms(
            3,
            1,
            data.clone(),
            data.clone(),
            data,
            flag,
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap(),
            ),
            weight,
            sigma,
            &[5, 6, 7, 8],
        )
        .unwrap();

        assert_eq!((a1, a2), (1, 3));
        assert_eq!(corr_types, vec![5, 7, 6, 8]);
        let ArrayValue::Complex32(array) = reordered else {
            panic!("expected Complex32");
        };
        assert_eq!(array[[0, 0]], Complex32::new(1.0, -1.0));
        assert_eq!(array[[1, 0]], Complex32::new(3.0, -3.0));
        assert_eq!(array[[2, 0]], Complex32::new(2.0, -2.0));
        assert_eq!(array[[3, 0]], Complex32::new(4.0, -4.0));
        let ArrayValue::Float32(weight_array) = reordered_weight else {
            panic!("expected Float32");
        };
        let ArrayValue::Float32(sigma_array) = reordered_sigma else {
            panic!("expected Float32");
        };
        let ArrayValue::Bool(flag_array) = reordered_flag else {
            panic!("expected Bool");
        };
        let ArrayValue::Bool(flag_category_array) = reordered_flag_category else {
            panic!("expected Bool");
        };
        assert_eq!(
            weight_array.iter().copied().collect::<Vec<_>>(),
            vec![1.0, 3.0, 2.0, 4.0]
        );
        assert_eq!(
            sigma_array.iter().copied().collect::<Vec<_>>(),
            vec![5.0, 7.0, 6.0, 8.0]
        );
        assert_eq!(
            flag_array.iter().copied().collect::<Vec<_>>(),
            vec![false, false, false, false]
        );
        assert_eq!(
            flag_category_array.iter().copied().collect::<Vec<_>>(),
            vec![false, false, false, false]
        );
    }

    #[test]
    fn swapped_baselines_keep_flag_categories_and_merge_online_shadow_flags() {
        let flag = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false, false, true, true]).unwrap(),
        );
        let flag_category = ArrayValue::Bool(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1, DEFAULT_FLAG_CATEGORIES]).f(),
                vec![
                    false, false, false, false, false, false, //
                    false, false, false, false, false, false, //
                    false, false, true, false, false, false, //
                    false, false, true, false, false, false, //
                ],
            )
            .unwrap(),
        );
        let expected_flag_category = flag_category.clone();

        let (_, _, _, _, _, reordered_flag, reordered_flag_category, _, _, _) =
            reorder_baseline_for_ms(
                3,
                1,
                ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                        .unwrap(),
                ),
                ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                        .unwrap(),
                ),
                ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                        .unwrap(),
                ),
                flag,
                flag_category.clone(),
                ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap()),
                ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap()),
                &[5, 6, 7, 8],
            )
            .unwrap();

        let ArrayValue::Bool(flag_array) = reordered_flag else {
            panic!("expected Bool");
        };
        assert_eq!(
            flag_array.iter().copied().collect::<Vec<_>>(),
            vec![false, true, true, true]
        );
        assert_eq!(reordered_flag_category, expected_flag_category);
    }

    #[test]
    fn writes_normalized_record_into_measurement_set() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("synthetic.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(
            ImportVlaOptions {
                archivefiles: vec![PathBuf::from("synthetic.xp1")],
                vis: Some(ms_path.clone()),
                ..ImportVlaOptions::default()
            },
            ms,
        )
        .unwrap();

        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(2.0, 0.0),
                    Complex32::new(3.0, 0.0),
                    Complex32::new(4.0, 0.0),
                ],
            )
            .unwrap(),
        );
        let model = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .unwrap(),
        );
        let flag =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
        let flag_category = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap(),
        );
        let weight =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
        let sigma =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());

        writer
            .push_record(NormalizedRecord {
                project: "AB123".to_string(),
                source_name: "3C286".to_string(),
                calibration_code: "C".to_string(),
                source_num_lines: 1,
                source_direction: [1.0, 0.5],
                array_id: 0,
                time_seconds: 5_097_600_000.0,
                integration_seconds: 10.0,
                direction_epoch: DirectionEpoch::J2000,
                antennas: vec![
                    NormalizedAntenna {
                        antenna_id: 1,
                        name: "VA01".to_string(),
                        station: "VA01".to_string(),
                        position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
                    },
                    NormalizedAntenna {
                        antenna_id: 2,
                        name: "VA02".to_string(),
                        station: "VA02".to_string(),
                        position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
                    },
                ],
                groups: vec![NormalizedGroup {
                    descriptor: SpectralDescriptor {
                        edge_frequency_hz: 1.4e9 - 0.5 * 5.0e7,
                        observed_frequency_hz: 1.4e9,
                        channel_width_hz: 5.0e7,
                        total_bandwidth_hz: 5.0e7,
                        num_chan: 1,
                        if_conv_chain: 0,
                        rest_frequency_hz: 0.0,
                        rest_frame: FrequencyFrame::Topocentric,
                        doppler_definition: DopplerDefinition::Radio,
                        doppler_velocity_mps: 0.0,
                        doppler_tracking: false,
                    },
                    baselines: vec![NormalizedBaselineRow {
                        antenna1_archive: 0,
                        antenna2_archive: 1,
                        corr_types: vec![5, 6, 7, 8],
                        uvw_m: [10.0, 20.0, 30.0],
                        data: data.clone(),
                        corrected_data: data,
                        model_data: model,
                        flag,
                        flag_category,
                        weight,
                        sigma,
                    }],
                }],
            })
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(reopened.row_count(), 1);
        assert_eq!(reopened.antenna().unwrap().row_count(), 2);
        assert_eq!(reopened.field().unwrap().row_count(), 1);
        assert_eq!(reopened.observation().unwrap().row_count(), 1);
        assert_eq!(reopened.source().unwrap().row_count(), 1);
        assert_eq!(reopened.spectral_window().unwrap().row_count(), 1);
        assert_eq!(reopened.polarization().unwrap().row_count(), 1);
        assert_eq!(reopened.data_description().unwrap().row_count(), 1);

        let data_accessor = reopened
            .data_column(casa_ms::VisibilityDataColumn::Data)
            .unwrap();
        let data_column = data_accessor.get(0).unwrap();
        let ArrayValue::Complex32(array) = data_column else {
            panic!("expected Complex32 DATA column");
        };
        assert_eq!(array.shape(), &[4, 1]);
        assert_eq!(array[[0, 0]], Complex32::new(1.0, 0.0));
        assert_eq!(array[[3, 0]], Complex32::new(4.0, 0.0));
    }

    fn test_descriptor(observed_frequency_hz: f64) -> SpectralDescriptor {
        SpectralDescriptor {
            edge_frequency_hz: observed_frequency_hz - 0.5 * 5.0e7,
            observed_frequency_hz,
            channel_width_hz: 5.0e7,
            total_bandwidth_hz: 5.0e7,
            num_chan: 1,
            if_conv_chain: 0,
            rest_frequency_hz: 0.0,
            rest_frame: FrequencyFrame::Topocentric,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 0.0,
            doppler_tracking: false,
        }
    }

    fn test_antennas() -> Vec<NormalizedAntenna> {
        vec![
            NormalizedAntenna {
                antenna_id: 1,
                name: "VA01".to_string(),
                station: "VA01".to_string(),
                position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
            },
            NormalizedAntenna {
                antenna_id: 2,
                name: "VA02".to_string(),
                station: "VA02".to_string(),
                position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
            },
        ]
    }

    fn test_baseline() -> NormalizedBaselineRow {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(1.0, 0.0); 4]).unwrap(),
        );
        let model = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .unwrap(),
        );
        let flag =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
        let flag_category = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap(),
        );
        let weight =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
        let sigma =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());

        NormalizedBaselineRow {
            antenna1_archive: 0,
            antenna2_archive: 1,
            corr_types: vec![5, 6, 7, 8],
            uvw_m: [10.0, 20.0, 30.0],
            data: data.clone(),
            corrected_data: data,
            model_data: model,
            flag,
            flag_category,
            weight,
            sigma,
        }
    }

    fn test_record(
        source_name: &str,
        time_seconds: f64,
        descriptor: SpectralDescriptor,
    ) -> NormalizedRecord {
        NormalizedRecord {
            project: "AG189".to_string(),
            source_name: source_name.to_string(),
            calibration_code: "".to_string(),
            source_num_lines: 1,
            source_direction: [1.0, 0.5],
            array_id: 0,
            time_seconds,
            integration_seconds: 10.0,
            direction_epoch: DirectionEpoch::J2000,
            antennas: test_antennas(),
            groups: vec![NormalizedGroup {
                descriptor,
                baselines: vec![test_baseline()],
            }],
        }
    }

    fn main_scan_number(ms: &MeasurementSet, row: usize) -> i32 {
        let table_row = ms.main_table().row_accessor().row(row).unwrap();
        let Value::Scalar(ScalarValue::Int32(scan_number)) = table_row.get("SCAN_NUMBER").unwrap()
        else {
            panic!("expected SCAN_NUMBER scalar");
        };
        *scan_number
    }

    fn measure_ref_keyword(table: &Table, column: &str) -> String {
        let Some(keywords) = table.column_keywords(column) else {
            panic!("expected keywords for {column}");
        };
        let Some(Value::Record(measinfo)) = keywords.get("MEASINFO") else {
            panic!("expected MEASINFO for {column}");
        };
        let Some(Value::Scalar(ScalarValue::String(reference))) = measinfo.get("Ref") else {
            panic!("expected Ref string for {column}");
        };
        reference.clone()
    }

    fn measure_type_keyword(table: &Table, column: &str) -> String {
        let Some(keywords) = table.column_keywords(column) else {
            panic!("expected keywords for {column}");
        };
        let Some(Value::Record(measinfo)) = keywords.get("MEASINFO") else {
            panic!("expected MEASINFO for {column}");
        };
        let Some(Value::Scalar(ScalarValue::String(measure_type))) = measinfo.get("type") else {
            panic!("expected type string for {column}");
        };
        measure_type.clone()
    }

    fn quantum_units_keyword(table: &Table, column: &str) -> Vec<String> {
        let Some(keywords) = table.column_keywords(column) else {
            panic!("expected keywords for {column}");
        };
        let Some(Value::Array(ArrayValue::String(units))) = keywords.get("QuantumUnits") else {
            panic!("expected QuantumUnits for {column}");
        };
        units.iter().cloned().collect()
    }

    fn optional_i32_scalar(row: &RecordValue, column: &str) -> Option<i32> {
        match row.get(column) {
            Some(Value::Scalar(ScalarValue::Int32(value))) => Some(*value),
            None => None,
            Some(other) => panic!("{column} had unexpected value {other:?}"),
        }
    }

    #[test]
    fn skipped_records_force_new_scan_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("skip-scan.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();

        let descriptor = test_descriptor(1.4e9);
        writer
            .push_record(test_record("0836+710", 5_097_600_000.0, descriptor.clone()))
            .unwrap();
        writer
            .push_record(test_record("0836+710", 5_097_600_010.0, descriptor.clone()))
            .unwrap();
        writer.note_skipped_record();
        writer
            .push_record(test_record("0836+710", 5_097_600_020.0, descriptor))
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(main_scan_number(&reopened, 0), 1);
        assert_eq!(main_scan_number(&reopened, 1), 1);
        assert_eq!(main_scan_number(&reopened, 2), 2);
    }

    #[test]
    fn data_description_changes_force_new_scan_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("dd-scan.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();

        writer
            .push_record(test_record(
                "0836+710",
                5_097_600_000.0,
                test_descriptor(1.4e9),
            ))
            .unwrap();
        writer
            .push_record(test_record(
                "0836+710",
                5_097_600_010.0,
                test_descriptor(1.4e9),
            ))
            .unwrap();
        writer
            .push_record(test_record(
                "0836+710",
                5_097_600_020.0,
                test_descriptor(1.5e9),
            ))
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(main_scan_number(&reopened, 0), 1);
        assert_eq!(main_scan_number(&reopened, 1), 1);
        assert_eq!(main_scan_number(&reopened, 2), 2);
    }

    #[test]
    fn spectral_windows_match_across_frequency_reference_frames() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("frame-match.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();

        let topo_descriptor = SpectralDescriptor {
            edge_frequency_hz: 23_691_048_000.0 - 127.0 / 2.0 * 24_414.062_5,
            observed_frequency_hz: 23_691_048_000.0,
            channel_width_hz: 24_414.062_5,
            total_bandwidth_hz: 3_125_000.0,
            num_chan: 127,
            if_conv_chain: 0,
            rest_frequency_hz: 0.0,
            rest_frame: FrequencyFrame::Topocentric,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 0.0,
            doppler_tracking: false,
        };
        let source_direction = [1.0, 0.5];
        let time_seconds = 4_558_040_657.5;
        let frame =
            spectral_conversion_frame(time_seconds, source_direction, DirectionEpoch::J2000)
                .unwrap();
        let lsrk_frequency_hz = convert_topocentric_frequency(
            topo_descriptor.observed_frequency_hz,
            FrequencyRef::LSRK,
            &frame,
        )
        .unwrap();
        let lsrk_descriptor = SpectralDescriptor {
            edge_frequency_hz: topo_descriptor.edge_frequency_hz,
            observed_frequency_hz: topo_descriptor.observed_frequency_hz,
            channel_width_hz: topo_descriptor.channel_width_hz,
            total_bandwidth_hz: topo_descriptor.total_bandwidth_hz,
            num_chan: topo_descriptor.num_chan,
            if_conv_chain: topo_descriptor.if_conv_chain,
            rest_frequency_hz: lsrk_frequency_hz,
            rest_frame: FrequencyFrame::Lsrk,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 0.0,
            doppler_tracking: true,
        };

        let mut topo_record = test_record("0530+135", time_seconds, topo_descriptor);
        topo_record.direction_epoch = DirectionEpoch::J2000;
        topo_record.source_direction = source_direction;
        let mut lsrk_record = test_record("05309+13319", time_seconds + 475.0, lsrk_descriptor);
        lsrk_record.direction_epoch = DirectionEpoch::J2000;
        lsrk_record.source_direction = source_direction;

        writer.push_record(topo_record).unwrap();
        writer.push_record(lsrk_record).unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(reopened.spectral_window().unwrap().row_count(), 1);
        assert_eq!(reopened.data_description().unwrap().row_count(), 1);
        let dd0 = reopened
            .main_table()
            .row_accessor()
            .row(0)
            .unwrap()
            .get("DATA_DESC_ID")
            .unwrap()
            .clone();
        let dd1 = reopened
            .main_table()
            .row_accessor()
            .row(1)
            .unwrap()
            .get("DATA_DESC_ID")
            .unwrap()
            .clone();
        assert_eq!(dd0, dd1);
    }

    #[test]
    fn spectral_conversion_frame_uses_tai_like_casa_vlafiller() {
        let time_seconds = 4_558_025_017.500007;
        let source_direction = [1.4439993710957795, 0.23617770712489528];
        let frame =
            spectral_conversion_frame(time_seconds, source_direction, DirectionEpoch::J2000)
                .unwrap();
        let converted =
            convert_topocentric_frequency(23_689_651_393.610_19, FrequencyRef::LSRK, &frame)
                .unwrap();
        assert!(
            (converted - 23_692_506_802.643_28).abs() < 1.0e-2,
            "converted={converted}"
        );
    }

    #[test]
    fn importer_marks_vla_epoch_columns_as_tai() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("tai-metadata.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();
        writer
            .push_record(test_record(
                "0836+710",
                5_097_600_000.0,
                test_descriptor(1.4e9),
            ))
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(measure_ref_keyword(reopened.main_table(), "TIME"), "TAI");
        assert_eq!(
            measure_ref_keyword(reopened.main_table(), "TIME_CENTROID"),
            "TAI"
        );
        assert_eq!(measure_ref_keyword(reopened.main_table(), "UVW"), "J2000");
        assert_eq!(
            measure_ref_keyword(reopened.field().unwrap().table(), "TIME"),
            "TAI"
        );
        assert_eq!(
            measure_ref_keyword(reopened.feed().unwrap().table(), "TIME"),
            "TAI"
        );
        assert_eq!(
            measure_ref_keyword(reopened.source().unwrap().table(), "TIME"),
            "TAI"
        );
        assert_eq!(
            measure_ref_keyword(reopened.observation().unwrap().table(), "TIME_RANGE"),
            "TAI"
        );
        assert_eq!(
            measure_ref_keyword(reopened.observation().unwrap().table(), "RELEASE_DATE"),
            "TAI"
        );
    }

    #[test]
    fn importer_persists_measure_types_refs_and_units_for_vla_columns() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("measure-keywords.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();
        writer
            .push_record(test_record(
                "0836+710",
                5_097_600_000.0,
                test_descriptor(1.4e9),
            ))
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(measure_type_keyword(reopened.main_table(), "TIME"), "epoch");
        assert_eq!(measure_type_keyword(reopened.main_table(), "UVW"), "uvw");
        assert_eq!(measure_ref_keyword(reopened.main_table(), "UVW"), "J2000");
        assert_eq!(
            quantum_units_keyword(reopened.main_table(), "UVW"),
            vec!["m".to_string(), "m".to_string(), "m".to_string()]
        );

        let field = reopened.field().unwrap();
        for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
            assert_eq!(measure_type_keyword(field.table(), column), "direction");
            assert_eq!(measure_ref_keyword(field.table(), column), "J2000");
            assert_eq!(
                quantum_units_keyword(field.table(), column),
                vec!["rad".to_string(), "rad".to_string()]
            );
        }

        let feed = reopened.feed().unwrap();
        assert_eq!(
            measure_type_keyword(feed.table(), "BEAM_OFFSET"),
            "direction"
        );
        assert_eq!(measure_ref_keyword(feed.table(), "BEAM_OFFSET"), "J2000");
        assert_eq!(
            quantum_units_keyword(feed.table(), "BEAM_OFFSET"),
            vec!["rad".to_string(), "rad".to_string()]
        );

        let antenna = reopened.antenna().unwrap();
        assert_eq!(
            measure_type_keyword(antenna.table(), "POSITION"),
            "position"
        );
        assert_eq!(measure_ref_keyword(antenna.table(), "POSITION"), "ITRF");
        assert_eq!(
            quantum_units_keyword(antenna.table(), "POSITION"),
            vec!["m".to_string(), "m".to_string(), "m".to_string()]
        );

        let observation = reopened.observation().unwrap();
        assert_eq!(
            measure_type_keyword(observation.table(), "TIME_RANGE"),
            "epoch"
        );
        assert_eq!(
            quantum_units_keyword(observation.table(), "TIME_RANGE"),
            vec!["s".to_string()]
        );

        let source = reopened.source().unwrap();
        assert_eq!(
            measure_type_keyword(source.table(), "DIRECTION"),
            "direction"
        );
        assert_eq!(measure_ref_keyword(source.table(), "DIRECTION"), "J2000");
        assert_eq!(
            quantum_units_keyword(source.table(), "DIRECTION"),
            vec!["rad".to_string(), "rad".to_string()]
        );

        let spw = reopened.spectral_window().unwrap();
        for column in ["REF_FREQUENCY", "CHAN_FREQ"] {
            assert_eq!(measure_type_keyword(spw.table(), column), "frequency");
        }
        for column in [
            "CHAN_WIDTH",
            "EFFECTIVE_BW",
            "RESOLUTION",
            "TOTAL_BANDWIDTH",
            "REF_FREQUENCY",
            "CHAN_FREQ",
        ] {
            assert_eq!(
                quantum_units_keyword(spw.table(), column),
                vec!["Hz".to_string()]
            );
        }
    }

    #[test]
    fn importer_persists_spw_doppler_id_for_doppler_tracked_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("spw-doppler-id.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();
        writer
            .push_record(test_record(
                "0530+135",
                4_558_025_017.500007,
                SpectralDescriptor {
                    edge_frequency_hz: 23_689_639_186.578_94,
                    observed_frequency_hz: 23_691_189_479.547_69,
                    channel_width_hz: 24_414.062_5,
                    total_bandwidth_hz: 3_125_000.0,
                    num_chan: 127,
                    if_conv_chain: 0,
                    rest_frequency_hz: 23_694_044_992.921_69,
                    rest_frame: FrequencyFrame::Lsrk,
                    doppler_definition: DopplerDefinition::Radio,
                    doppler_velocity_mps: 5_700.0,
                    doppler_tracking: true,
                },
            ))
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        let spw = reopened.spectral_window().unwrap();
        let spw_table = spw.table();
        assert!(
            spw_table
                .schema()
                .is_some_and(|schema| schema.contains_column("DOPPLER_ID"))
        );
        let spw_row = spw_table.row_accessor().row(0).unwrap();
        assert_eq!(optional_i32_scalar(spw_row, "DOPPLER_ID"), Some(0));
    }

    #[test]
    fn rejects_mixed_direction_epochs_until_epoch_conversion_is_implemented() {
        let input = [1.0, 0.5];
        let error =
            align_direction_for_epoch(input, DirectionEpoch::B1950Vla, DirectionEpoch::J2000)
                .unwrap_err();
        assert!(
            error.to_string().contains("mixed direction epochs"),
            "{error}"
        );
    }

    #[test]
    fn wraps_source_longitudes_into_signed_range() {
        let wrapped = normalize_longitude_radians(2.0 * PI - 0.25);
        assert!((wrapped + 0.25).abs() < 1.0e-12, "{wrapped}");
    }

    #[test]
    fn formats_spectral_window_names_like_casa() {
        let descriptor = SpectralDescriptor {
            edge_frequency_hz: 7.8149e9 - 0.5 * 50.0e6,
            observed_frequency_hz: 7.8149e9,
            channel_width_hz: 50.0e6,
            total_bandwidth_hz: 50.0e6,
            num_chan: 1,
            if_conv_chain: 0,
            rest_frequency_hz: 0.0,
            rest_frame: FrequencyFrame::Topocentric,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 0.0,
            doppler_tracking: false,
        };
        assert_eq!(
            spectral_window_name(
                &descriptor,
                descriptor.observed_frequency_hz,
                FrequencyRef::TOPO
            ),
            "1*50 MHz channels @ 7.81 GHz (TOPO)"
        );
    }

    #[test]
    fn helper_paths_cover_import_options_arrays_and_frequency_metadata() {
        let mut options = ImportVlaOptions {
            archivefiles: vec![PathBuf::from("input.xp1")],
            ..ImportVlaOptions::default()
        };
        assert!(validate_import_options(&options).is_ok());
        options.bandname = Some(crate::BandName::L);
        assert!(matches!(
            validate_import_options(&options),
            Err(VlaError::InvalidArgument {
                argument: "bandname",
                ..
            })
        ));
        options.bandname = None;
        options.starttime = Some("01-Jan-1985/00:00:00".to_string());
        assert!(matches!(
            validate_import_options(&options),
            Err(VlaError::InvalidArgument {
                argument: "starttime",
                ..
            })
        ));
        options.starttime = None;
        options.stoptime = Some("01-Jan-1985/01:00:00".to_string());
        assert!(matches!(
            validate_import_options(&options),
            Err(VlaError::InvalidArgument {
                argument: "stoptime",
                ..
            })
        ));
        options.stoptime = None;
        options.evlabands = true;
        assert!(matches!(
            validate_import_options(&options),
            Err(VlaError::InvalidArgument {
                argument: "evlabands",
                ..
            })
        ));

        assert_eq!(
            [
                frequency_reference_for_frame(FrequencyFrame::Topocentric),
                frequency_reference_for_frame(FrequencyFrame::Geocentric),
                frequency_reference_for_frame(FrequencyFrame::Barycentric),
                frequency_reference_for_frame(FrequencyFrame::Lsrk),
            ],
            [
                FrequencyRef::TOPO,
                FrequencyRef::GEO,
                FrequencyRef::BARY,
                FrequencyRef::LSRK,
            ]
        );
        assert_eq!(
            doppler_reference_for_definition(DopplerDefinition::Optical),
            DopplerRef::Z
        );
        assert_eq!(
            doppler_reference_for_definition(DopplerDefinition::Unknown),
            DopplerRef::RADIO
        );
        assert_eq!(
            direction_reference_for_epoch(DirectionEpoch::Unknown(17)),
            DirectionRef::J2000
        );
        assert_eq!(format_sigfigs(0.0, 3), "0");
        assert_eq!(format_sigfigs(12_345.0, 3), "12345");
        assert_eq!(format_sigfigs(12.340, 4), "12.34");
        assert_eq!(
            spectral_window_name(&test_descriptor(327.5e6), 327.5e6, FrequencyRef::TOPO),
            "1*50 MHz channels @ 328 MHz (TOPO)"
        );

        assert_eq!(
            [
                stokes_code(StokesProduct::Rr),
                stokes_code(StokesProduct::Rl)
            ],
            [5, 6]
        );
        assert_eq!(
            [
                stokes_code(StokesProduct::Lr),
                stokes_code(StokesProduct::Ll)
            ],
            [7, 8]
        );
        assert_eq!(
            build_model_data(&[5, 6, 7, 8], 2),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
            ]
        );
        assert_eq!(flag_category_index(1, 2, 3, 4, 5), 69);

        let (weight, sigma) = visibility_weight_sigma(1.0e6, 10.0, 4.0, true, false);
        assert!((weight - 30.0).abs() < 1.0e-6);
        assert!((sigma - (1.0_f32 / 120.0_f32.sqrt() * 2.0)).abs() < 1.0e-6);
        assert_eq!(
            visibility_weight_sigma(0.0, 10.0, 1.0, true, false),
            (0.0, 0.0)
        );

        let all_flagged =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![true, true]).unwrap());
        let partly_flagged = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![true, false]).unwrap(),
        );
        assert!(flag_row_value(&all_flagged).unwrap());
        assert!(!flag_row_value(&partly_flagged).unwrap());
        assert!(
            flag_row_value(&ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![1.0_f32]).unwrap()
            ))
            .is_err()
        );

        let weights = ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        );
        let reordered = reorder_vector_array(&weights, &[5, 6, 7, 8], &[5, 7, 6, 8]).unwrap();
        let ArrayValue::Float32(reordered) = reordered else {
            panic!("expected float vector");
        };
        assert_eq!(
            reordered.iter().copied().collect::<Vec<_>>(),
            vec![1.0, 3.0, 2.0, 4.0]
        );
        assert!(reorder_vector_array(&all_flagged, &[5], &[5]).is_err());
        assert!(reorder_index_map(&[5, 6], &[5, 7], "test").is_err());

        let flags = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, true, true, false]).unwrap(),
        );
        let reordered_flags = reorder_bool_array_generic(&flags, &[5, 8], &[8, 5], "flag").unwrap();
        let ArrayValue::Bool(reordered_flags) = reordered_flags else {
            panic!("expected bool flags");
        };
        assert_eq!(
            reordered_flags.iter().copied().collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
        assert!(reorder_bool_array_generic(&weights, &[5], &[5], "flag").is_err());

        let corr_product = corr_product_array(&[5, 6, 7, 8]).unwrap();
        let ArrayValue::Int32(corr_product) = corr_product else {
            panic!("expected int corr product");
        };
        assert_eq!(
            corr_product.iter().copied().collect::<Vec<_>>(),
            vec![0, 0, 1, 1, 0, 1, 0, 1]
        );
        assert!(corr_product_array(&[42]).is_err());
    }

    #[test]
    fn new_spectral_windows_use_the_most_recently_added_source_row() {
        let tempdir = tempfile::tempdir().unwrap();
        let ms_path = tempdir.path().join("doppler-update.ms");
        let builder = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler);
        let ms = MeasurementSet::create(&ms_path, builder).unwrap();
        let mut writer = MsImportWriter::new(ImportVlaOptions::default(), ms).unwrap();
        set_flag_category_names(writer.ms.main_table_mut()).unwrap();

        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(1.0, 0.0); 4]).unwrap(),
        );
        let model = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[4, 1]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .unwrap(),
        );
        let flag =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
        let flag_category = ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap(),
        );
        let weight =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
        let sigma =
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
        let descriptor = SpectralDescriptor {
            edge_frequency_hz: 1.4e9 - 0.5 * 5.0e7,
            observed_frequency_hz: 1.4e9,
            channel_width_hz: 5.0e7,
            total_bandwidth_hz: 5.0e7,
            num_chan: 1,
            if_conv_chain: 0,
            rest_frequency_hz: 0.0,
            rest_frame: FrequencyFrame::Topocentric,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 0.0,
            doppler_tracking: false,
        };
        let antennas = vec![
            NormalizedAntenna {
                antenna_id: 1,
                name: "VA01".to_string(),
                station: "VA01".to_string(),
                position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
            },
            NormalizedAntenna {
                antenna_id: 2,
                name: "VA02".to_string(),
                station: "VA02".to_string(),
                position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
            },
        ];
        let baseline = || NormalizedBaselineRow {
            antenna1_archive: 0,
            antenna2_archive: 1,
            corr_types: vec![5, 6, 7, 8],
            uvw_m: [10.0, 20.0, 30.0],
            data: data.clone(),
            corrected_data: data.clone(),
            model_data: model.clone(),
            flag: flag.clone(),
            flag_category: flag_category.clone(),
            weight: weight.clone(),
            sigma: sigma.clone(),
        };

        writer
            .push_record(NormalizedRecord {
                project: "AG189".to_string(),
                source_name: "0836+710".to_string(),
                calibration_code: "".to_string(),
                source_num_lines: 1,
                source_direction: [1.0, 0.5],
                array_id: 0,
                time_seconds: 5_097_600_000.0,
                integration_seconds: 10.0,
                direction_epoch: DirectionEpoch::J2000,
                antennas: antennas.clone(),
                groups: vec![NormalizedGroup {
                    descriptor: descriptor.clone(),
                    baselines: vec![baseline()],
                }],
            })
            .unwrap();
        writer
            .push_record(NormalizedRecord {
                project: "AG189".to_string(),
                source_name: "N2146".to_string(),
                calibration_code: "".to_string(),
                source_num_lines: 1,
                source_direction: [1.5, 0.25],
                array_id: 0,
                time_seconds: 5_097_600_100.0,
                integration_seconds: 10.0,
                direction_epoch: DirectionEpoch::J2000,
                antennas: antennas.clone(),
                groups: vec![NormalizedGroup {
                    descriptor: descriptor.clone(),
                    baselines: vec![baseline()],
                }],
            })
            .unwrap();
        writer
            .push_record(NormalizedRecord {
                project: "AG189".to_string(),
                source_name: "0836+710".to_string(),
                calibration_code: "".to_string(),
                source_num_lines: 1,
                source_direction: [1.0, 0.5],
                array_id: 0,
                time_seconds: 5_097_600_200.0,
                integration_seconds: 10.0,
                direction_epoch: DirectionEpoch::J2000,
                antennas,
                groups: vec![NormalizedGroup {
                    descriptor: SpectralDescriptor {
                        edge_frequency_hz: 1.5e9 - 0.5 * 5.0e7,
                        observed_frequency_hz: 1.5e9,
                        ..descriptor
                    },
                    baselines: vec![baseline()],
                }],
            })
            .unwrap();
        writer.finish().unwrap();

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        let doppler = reopened.doppler().unwrap();
        assert_eq!(doppler.table().row_count(), 2);
        let row = doppler.table().row_accessor().row(1).unwrap();
        let Value::Scalar(ScalarValue::Int32(source_id)) = row.get("SOURCE_ID").unwrap() else {
            panic!("expected SOURCE_ID scalar");
        };
        assert_eq!(*source_id, 1);
    }

    #[test]
    fn doppler_tracked_spectral_windows_use_rest_frame_metadata() {
        let descriptor = SpectralDescriptor {
            edge_frequency_hz: 23_691_189_479.547_69 - 127.0 / 2.0 * 24_414.062_5,
            observed_frequency_hz: 23_691_189_479.547_69,
            channel_width_hz: 24_414.062_5,
            total_bandwidth_hz: 3_125_000.0,
            num_chan: 127,
            if_conv_chain: 0,
            rest_frequency_hz: 23_694_044_992.921_69,
            rest_frame: FrequencyFrame::Lsrk,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 5_700.0,
            doppler_tracking: true,
        };

        let ref_freq = spectral_reference_frequency(&descriptor);
        assert_eq!(ref_freq.refer(), FrequencyRef::LSRK);
        assert_eq!(
            spectral_window_name(&descriptor, ref_freq.hz(), ref_freq.refer()),
            "127*24.4 kHz channels @ 23.7 GHz (LSRK)"
        );
    }

    #[test]
    fn doppler_tracked_descriptor_matching_uses_rest_frame_frequency() {
        let left = SpectralDescriptor {
            edge_frequency_hz: 23_691_189_479.547_69 - 127.0 / 2.0 * 24_414.062_5,
            observed_frequency_hz: 23_691_189_479.547_69,
            channel_width_hz: 24_414.062_5,
            total_bandwidth_hz: 3_125_000.0,
            num_chan: 127,
            if_conv_chain: 0,
            rest_frequency_hz: 23_694_044_992.921_69,
            rest_frame: FrequencyFrame::Lsrk,
            doppler_definition: DopplerDefinition::Radio,
            doppler_velocity_mps: 5_700.0,
            doppler_tracking: true,
        };
        let mut right = left.clone();
        right.observed_frequency_hz += 250_000.0;

        assert!(same_spectral_descriptor(&left, &right, 150_000.0));
    }

    #[test]
    fn inspect_real_archive_target_row_when_configured() {
        let Some(path) = std::env::var_os("CASA_RS_IMPORTVLA_ARCHIVE").map(PathBuf::from) else {
            eprintln!("skipping: CASA_RS_IMPORTVLA_ARCHIVE not set");
            return;
        };
        if !path.exists() {
            eprintln!("skipping: {} does not exist", path.display());
            return;
        }

        let target_row = std::env::var("CASA_RS_IMPORTVLA_TARGET_ROW")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(25_726);

        let options = ImportVlaOptions {
            archivefiles: vec![path.clone()],
            ..ImportVlaOptions::default()
        };
        let mut reader = VlaDiskReader::open(&path).expect("open archive");
        let mut row_base = 0usize;
        let mut logical_record_index = 0usize;

        while let Some(record) = reader.next_record().expect("read logical record") {
            let Some(normalized) = normalize_record(&record, &options).expect("normalize record")
            else {
                logical_record_index += 1;
                continue;
            };

            let grouped_cdas = grouped_cdas_for_test(&record, options.frequencytol_hz)
                .expect("group CDAs for diagnostic");
            assert_eq!(grouped_cdas.len(), normalized.groups.len());

            for (group_index, (group, (_, cda_ids))) in normalized
                .groups
                .iter()
                .zip(grouped_cdas.iter())
                .enumerate()
            {
                let row_end = row_base + group.baselines.len();
                if !(row_base..row_end).contains(&target_row) {
                    row_base = row_end;
                    continue;
                }

                let baseline_index = target_row - row_base;
                let baseline = &group.baselines[baseline_index];
                let ant1 = baseline.antenna1_archive;
                let ant2 = baseline.antenna2_archive;

                eprintln!(
                    "target_row={target_row} logical_record_index={logical_record_index} group_index={group_index} baseline_index={baseline_index}"
                );
                eprintln!(
                    "archive_baseline=({ant1},{ant2}) corr_types={:?} descriptor={:?}",
                    baseline.corr_types, group.descriptor
                );
                eprintln!(
                    "archive_antenna_names=({}, {})",
                    normalized.antennas[ant1].name, normalized.antennas[ant2].name
                );
                eprintln!(
                    "global_flag={:?}",
                    bool_matrix(&baseline.flag).expect("flag matrix")
                );
                eprintln!(
                    "global_flag_category={:?}",
                    bool_cube(&baseline.flag_category).expect("flag cube")
                );

                for &cda_id in cda_ids {
                    let global_stokes = record
                        .stokes_products(cda_id, 0, 0)
                        .expect("global stokes products");
                    let pair_stokes = record
                        .stokes_products(cda_id, ant1, ant2)
                        .expect("pair stokes products");
                    let if_usage = record
                        .sda()
                        .expect("SDA")
                        .if_usage(cda_id)
                        .expect("IF usage");
                    eprintln!(
                        "cda={cda_id:?} if_usage={if_usage:?} global_stokes={global_stokes:?} pair_stokes={pair_stokes:?}"
                    );
                }

                let ada1 = record.ada(ant1).expect("decode ant1 ADA");
                let ada2 = record.ada(ant2).expect("decode ant2 ADA");
                eprintln!(
                    "ant1_pols A={:?} B={:?} C={:?} D={:?}",
                    ada1.if_polarization(IfId::A).unwrap(),
                    ada1.if_polarization(IfId::B).unwrap(),
                    ada1.if_polarization(IfId::C).unwrap(),
                    ada1.if_polarization(IfId::D).unwrap()
                );
                eprintln!(
                    "ant2_pols A={:?} B={:?} C={:?} D={:?}",
                    ada2.if_polarization(IfId::A).unwrap(),
                    ada2.if_polarization(IfId::B).unwrap(),
                    ada2.if_polarization(IfId::C).unwrap(),
                    ada2.if_polarization(IfId::D).unwrap()
                );
                eprintln!(
                    "ant1_status A={} B={} C={} D={}",
                    ada1.if_status(IfId::A).unwrap(),
                    ada1.if_status(IfId::B).unwrap(),
                    ada1.if_status(IfId::C).unwrap(),
                    ada1.if_status(IfId::D).unwrap()
                );
                eprintln!(
                    "ant2_status A={} B={} C={} D={}",
                    ada2.if_status(IfId::A).unwrap(),
                    ada2.if_status(IfId::B).unwrap(),
                    ada2.if_status(IfId::C).unwrap(),
                    ada2.if_status(IfId::D).unwrap()
                );
                return;
            }

            logical_record_index += 1;
        }

        panic!("target row {target_row} not found in archive");
    }

    fn grouped_cdas_for_test(
        record: &LogicalRecord,
        tolerance_hz: f64,
    ) -> Result<Vec<(SpectralDescriptor, Vec<CdaId>)>, VlaError> {
        let rca = record.rca();
        let sda = record
            .sda()
            .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
        let mut cda_groups: Vec<(SpectralDescriptor, Vec<CdaId>)> = Vec::new();

        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca.cda_offset_bytes(cda_id.index()).map_err(|error| {
                VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id))
            })? == 0
            {
                continue;
            }
            let cda = record
                .cda(cda_id)
                .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
            if !cda.is_valid() {
                continue;
            }
            if sda.n_polarizations(cda_id).map_err(|error| {
                VlaError::import(format!("decode CDA polarization count: {error}"))
            })? == 0
            {
                continue;
            }

            let descriptor = SpectralDescriptor {
                edge_frequency_hz: sda.edge_frequency_hz(cda_id).map_err(|error| {
                    VlaError::import(format!("decode edge frequency for {:?}: {error}", cda_id))
                })?,
                observed_frequency_hz: sda.observed_frequency_hz(cda_id).map_err(|error| {
                    VlaError::import(format!(
                        "decode observed frequency for {:?}: {error}",
                        cda_id
                    ))
                })?,
                channel_width_hz: sda.channel_width_hz(cda_id).map_err(|error| {
                    VlaError::import(format!("decode channel width for {:?}: {error}", cda_id))
                })?,
                total_bandwidth_hz: sda.correlated_bandwidth_hz(cda_id).map_err(|error| {
                    VlaError::import(format!("decode bandwidth for {:?}: {error}", cda_id))
                })?,
                num_chan: usize::try_from(sda.n_channels(cda_id).map_err(|error| {
                    VlaError::import(format!("decode channel count for {:?}: {error}", cda_id))
                })?)
                .map_err(|_| VlaError::import("channel count does not fit in usize"))?,
                if_conv_chain: i32::try_from(sda.electronic_path(cda_id).map_err(|error| {
                    VlaError::import(format!("decode IF chain for {:?}: {error}", cda_id))
                })?)
                .map_err(|_| VlaError::import("IF conversion chain does not fit in i32"))?,
                rest_frequency_hz: sda.rest_frequency_hz(cda_id).map_err(|error| {
                    VlaError::import(format!("decode rest frequency for {:?}: {error}", cda_id))
                })?,
                rest_frame: sda.rest_frame(cda_id).map_err(|error| {
                    VlaError::import(format!("decode rest frame for {:?}: {error}", cda_id))
                })?,
                doppler_definition: sda.doppler_definition(cda_id).map_err(|error| {
                    VlaError::import(format!(
                        "decode doppler definition for {:?}: {error}",
                        cda_id
                    ))
                })?,
                doppler_velocity_mps: sda.radial_velocity_mps(cda_id).map_err(|error| {
                    VlaError::import(format!("decode radial velocity for {:?}: {error}", cda_id))
                })?,
                doppler_tracking: sda.doppler_tracking(cda_id).map_err(|error| {
                    VlaError::import(format!("decode doppler tracking for {:?}: {error}", cda_id))
                })?,
            };

            if let Some((_, cdas)) = cda_groups
                .iter_mut()
                .find(|(existing, _)| same_spectral_descriptor(existing, &descriptor, tolerance_hz))
            {
                cdas.push(cda_id);
            } else {
                cda_groups.push((descriptor, vec![cda_id]));
            }
        }

        Ok(cda_groups)
    }

    fn bool_matrix(value: &ArrayValue) -> Result<Vec<Vec<bool>>, VlaError> {
        let ArrayValue::Bool(array) = value else {
            return Err(VlaError::import("expected Bool matrix"));
        };
        let shape = array.shape();
        let n_corr = shape[0];
        let n_chan = shape[1];
        Ok((0..n_corr)
            .map(|corr| (0..n_chan).map(|chan| array[[corr, chan]]).collect())
            .collect())
    }

    fn bool_cube(value: &ArrayValue) -> Result<Vec<Vec<Vec<bool>>>, VlaError> {
        let ArrayValue::Bool(array) = value else {
            return Err(VlaError::import("expected Bool cube"));
        };
        let shape = array.shape();
        let n_corr = shape[0];
        let n_chan = shape[1];
        let n_cat = shape[2];
        Ok((0..n_corr)
            .map(|corr| {
                (0..n_chan)
                    .map(|chan| (0..n_cat).map(|cat| array[[corr, chan, cat]]).collect())
                    .collect()
            })
            .collect())
    }
}
