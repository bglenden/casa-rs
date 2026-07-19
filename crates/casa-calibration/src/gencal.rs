// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native generation of externally specified CASA calibration tables.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::{Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_FIELD_ID, COL_FLAG, COL_FPARAM, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_WEIGHT,
};
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter};

const COL_BFREQ: &str = "BFREQ";
const COL_EFREQ: &str = "EFREQ";
const COL_BTIME: &str = "BTIME";
const COL_ETIME: &str = "ETIME";
const COL_ANTENNA: &str = "ANTENNA";
const COL_GAIN: &str = "GAIN";
const COL_BANDNAME: &str = "BANDNAME";
const COL_NAME: &str = "NAME";
const COL_TIME_RANGE: &str = "TIME_RANGE";

/// Supported native `gencal` families for the VLA IRC+10216 prior-cal slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GencalType {
    /// Antenna-position delay correction table (`KAntPos Jones`).
    Antpos,
    /// VLA gain-curve plus antenna-efficiency table (`EGainCurve`).
    Gceff,
    /// Zenith-opacity table (`TOpac`).
    Opac,
}

impl GencalType {
    fn table_subtype(self) -> &'static str {
        match self {
            Self::Antpos => "KAntPos Jones",
            Self::Gceff => "EGainCurve",
            Self::Opac => "TOpac",
        }
    }
}

impl std::str::FromStr for GencalType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "antpos" => Ok(Self::Antpos),
            "gceff" | "gc" => Ok(Self::Gceff),
            "opac" | "opacity" => Ok(Self::Opac),
            other => Err(format!(
                "unsupported gencal caltype {other:?}; expected antpos, gceff, or opac"
            )),
        }
    }
}

/// Request for writing one native prior-cal table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GencalRequest {
    /// Input MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Output calibration-table path.
    pub output_table: PathBuf,
    /// Prior calibration family to generate.
    pub caltype: GencalType,
    /// CASA-style antenna selector for `antpos`.
    #[serde(default)]
    pub antenna: String,
    /// CASA-style SPW selector for `opac`.
    #[serde(default)]
    pub spw: String,
    /// Numeric parameters. `antpos` expects triples per selected antenna; `opac`
    /// expects one opacity per selected SPW.
    #[serde(default)]
    pub parameter: Vec<f64>,
    /// Optional explicit VLA GainCurves table path for `gceff`.
    #[serde(default)]
    pub gaincurve_table: Option<PathBuf>,
}

/// Report returned after writing one prior-cal table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GencalReport {
    /// Output calibration-table path.
    pub output_table: PathBuf,
    /// Generated calibration family.
    pub caltype: GencalType,
    /// CASA-visible table subtype.
    pub table_subtype: String,
    /// Number of MAIN rows written.
    pub row_count: usize,
    /// SPWs represented in the table.
    pub spectral_window_ids: Vec<i32>,
    /// Antennas represented in the table.
    pub antenna_ids: Vec<i32>,
}

/// Errors from native prior-cal table generation.
#[derive(Debug, Error)]
pub enum GencalError {
    /// Shared calibration-table persistence failed.
    #[error(transparent)]
    CalibrationTable(#[from] crate::CalibrationTableWriteError),
    /// Opening the input MS failed.
    #[error("failed to open measurement set {path}: {source}")]
    OpenMeasurementSet {
        /// Path that failed.
        path: String,
        /// Source error.
        source: Box<casa_ms::MsError>,
    },
    /// Opening the VLA GainCurves table failed.
    #[error("failed to open VLA GainCurves table {path}: {source}")]
    OpenGainCurves {
        /// Path that failed.
        path: String,
        /// Source error.
        source: Box<casa_tables::TableError>,
    },
    /// Output table preparation failed.
    #[error("failed to prepare output caltable {path}: {reason}")]
    PrepareOutput {
        /// Path that failed.
        path: String,
        /// Reason.
        reason: String,
    },
    /// Saving a table failed.
    #[error("failed to save caltable {path}: {source}")]
    Save {
        /// Path that failed.
        path: String,
        /// Source error.
        source: Box<casa_tables::TableError>,
    },
    /// Copying an MS subtable failed.
    #[error("failed to copy {subtable} into caltable {path}: {source}")]
    CopySubtable {
        /// Subtable name.
        subtable: String,
        /// Caltable root path.
        path: String,
        /// Source error.
        source: Box<casa_tables::TableError>,
    },
    /// The request does not match the supported native surface.
    #[error("{0}")]
    InvalidRequest(String),
    /// A needed value in a CASA table had an unsupported type or shape.
    #[error("{0}")]
    UnsupportedTableValue(String),
}

#[derive(Debug, Clone)]
struct PriorCalRow {
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    observation_id: i32,
    fparam: Vec<f32>,
    snr_value: f32,
}

/// Generate one native prior calibration table.
pub fn gencal(request: &GencalRequest) -> Result<GencalReport, GencalError> {
    let ms = MeasurementSet::open(&request.measurement_set).map_err(|source| {
        GencalError::OpenMeasurementSet {
            path: request.measurement_set.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let rows = match request.caltype {
        GencalType::Antpos => antpos_rows(&ms, request)?,
        GencalType::Gceff => gaincurve_rows(&ms, request)?,
        GencalType::Opac => opacity_rows(&ms, request)?,
    };
    write_float_caltable(&ms, request, &rows)
}

fn antpos_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let antenna = ms
        .antenna()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "ANTENNA".to_string(),
            source: Box::new(source),
        })?;
    let selected = parse_antenna_offsets(ms, &request.antenna, &request.parameter)?;
    let mut rows = Vec::with_capacity(antenna.row_count());
    for antenna_id in 0..antenna.row_count() {
        let offsets = selected
            .get(&(antenna_id as i32))
            .copied()
            .unwrap_or([0.0, 0.0, 0.0]);
        rows.push(PriorCalRow {
            field_id: -1,
            spw_id: 0,
            antenna_id: antenna_id as i32,
            observation_id: 0,
            fparam: offsets.iter().map(|value| *value as f32).collect(),
            snr_value: 1.0,
        });
    }
    Ok(rows)
}

fn gaincurve_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let source_path = request
        .gaincurve_table
        .clone()
        .or_else(default_vla_gaincurve_table)
        .ok_or_else(|| {
            GencalError::InvalidRequest(
                "gceff requires --gaincurve-table or a CASA data tree at ~/.casa/data/nrao/VLA/GainCurves".to_string(),
            )
        })?;
    let gaincurves = Table::open(TableOptions::new(&source_path)).map_err(|source| {
        GencalError::OpenGainCurves {
            path: source_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let antenna_count = ms
        .antenna()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "ANTENNA".to_string(),
            source: Box::new(source),
        })?
        .row_count();
    let antenna_keys = vla_gaincurve_antenna_keys(ms)?;
    let spw = ms
        .spectral_window()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "SPECTRAL_WINDOW".to_string(),
            source: Box::new(source),
        })?;
    let obs_time = first_observation_time(ms).or_else(|_| first_main_time(ms))?;
    let mut rows = Vec::with_capacity(antenna_count * spw.row_count());
    for spw_id in 0..spw.row_count() {
        let center_hz = spw_center_frequency_hz(&spw, spw_id)?;
        let band = vla_spw_band(&spw, spw_id)?;
        let efficiency = vla_efficiency(center_hz / 1.0e9) as f32;
        for (antenna_id, antenna_key) in antenna_keys.iter().enumerate().take(antenna_count) {
            let mut fparam = matching_gaincurve(
                &gaincurves,
                obs_time,
                center_hz,
                band.as_deref(),
                antenna_key,
            )?;
            for value in &mut fparam {
                *value *= efficiency;
            }
            rows.push(PriorCalRow {
                field_id: -1,
                spw_id: spw_id as i32,
                antenna_id: antenna_id as i32,
                observation_id: -1,
                fparam,
                snr_value: 0.0,
            });
        }
    }
    Ok(rows)
}

fn opacity_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let antenna_count = ms
        .antenna()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "ANTENNA".to_string(),
            source: Box::new(source),
        })?
        .row_count();
    let selected_spws = parse_spw_selector(ms, &request.spw)?;
    if request.parameter.len() != selected_spws.len() {
        return Err(GencalError::InvalidRequest(format!(
            "opac requires one parameter per selected SPW (got {} parameters for {} SPWs)",
            request.parameter.len(),
            selected_spws.len()
        )));
    }
    let mut rows = Vec::with_capacity(antenna_count * selected_spws.len());
    for (index, spw_id) in selected_spws.iter().copied().enumerate() {
        for antenna_id in 0..antenna_count {
            rows.push(PriorCalRow {
                field_id: -1,
                spw_id,
                antenna_id: antenna_id as i32,
                observation_id: 0,
                fparam: vec![request.parameter[index] as f32],
                snr_value: 1.0,
            });
        }
    }
    Ok(rows)
}

fn write_float_caltable(
    ms: &MeasurementSet,
    request: &GencalRequest,
    rows: &[PriorCalRow],
) -> Result<GencalReport, GencalError> {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable(COL_FPARAM, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid gencal caltable schema");
    let subtype = request.caltype.table_subtype();
    let mut writer = CalibrationTableWriter::create(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype,
            parameter_type: Some("Float"),
            measurement_set_name: ms
                .path()
                .and_then(Path::file_name)
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
    )?;

    for row in rows {
        let len = row.fparam.len();
        let zeros = vec![0.0_f32; len];
        let flags = vec![false; len];
        let snr = vec![row.snr_value; len];
        writer.append(RecordValue::new(vec![
            RecordField::new(COL_TIME, Value::Scalar(ScalarValue::Float64(0.0))),
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
            RecordField::new(COL_ANTENNA2, Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new(COL_INTERVAL, Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(COL_SCAN_NUMBER, Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new(
                COL_OBSERVATION_ID,
                Value::Scalar(ScalarValue::Int32(row.observation_id)),
            ),
            RecordField::new(COL_FPARAM, Value::Array(f32_column(&row.fparam))),
            RecordField::new(COL_PARAMERR, Value::Array(f32_column(&zeros))),
            RecordField::new(COL_FLAG, Value::Array(bool_column(&flags))),
            RecordField::new(COL_SNR, Value::Array(f32_column(&snr))),
            RecordField::new(COL_WEIGHT, Value::Array(f32_column(&zeros))),
        ]))?;
    }

    writer.finish(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
        (SubtableId::History, "HISTORY"),
    ])?;

    let spectral_window_ids = rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let antenna_ids = rows
        .iter()
        .map(|row| row.antenna_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(GencalReport {
        output_table: request.output_table.clone(),
        caltype: request.caltype,
        table_subtype: subtype.to_string(),
        row_count: rows.len(),
        spectral_window_ids,
        antenna_ids,
    })
}

fn parse_antenna_offsets(
    ms: &MeasurementSet,
    antenna_selector: &str,
    parameters: &[f64],
) -> Result<BTreeMap<i32, [f64; 3]>, GencalError> {
    if antenna_selector.trim().is_empty() {
        return Err(GencalError::InvalidRequest(
            "antpos currently requires explicit --antenna and --parameter offsets; automatic VLA baseline lookup is not implemented".to_string(),
        ));
    }
    let antenna = ms
        .antenna()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "ANTENNA".to_string(),
            source: Box::new(source),
        })?;
    let mut name_to_id = BTreeMap::new();
    for row in 0..antenna.row_count() {
        let name = antenna.name(row).map_err(|source| {
            GencalError::InvalidRequest(format!("failed to read ANTENNA.NAME row {row}: {source}"))
        })?;
        name_to_id.insert(name.to_ascii_lowercase(), row as i32);
    }
    let selected = antenna_selector
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            if let Ok(index) = part.parse::<i32>() {
                return Ok(index);
            }
            name_to_id
                .get(&part.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| GencalError::InvalidRequest(format!("unknown antenna {part:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if parameters.len() != selected.len() * 3 {
        return Err(GencalError::InvalidRequest(format!(
            "antpos requires three parameters per selected antenna (got {} parameters for {} antennas)",
            parameters.len(),
            selected.len()
        )));
    }
    let mut offsets = BTreeMap::new();
    for (index, antenna_id) in selected.iter().copied().enumerate() {
        offsets.insert(
            antenna_id,
            [
                parameters[index * 3],
                parameters[index * 3 + 1],
                parameters[index * 3 + 2],
            ],
        );
    }
    Ok(offsets)
}

fn parse_spw_selector(ms: &MeasurementSet, selector: &str) -> Result<Vec<i32>, GencalError> {
    let count = ms
        .spectral_window()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "SPECTRAL_WINDOW".to_string(),
            source: Box::new(source),
        })?
        .row_count() as i32;
    if selector.trim().is_empty() {
        return Ok((0..count).collect());
    }
    let mut ids = BTreeSet::new();
    for part in selector.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((start, end)) = part.split_once('~') {
            let start = start.parse::<i32>().map_err(|_| {
                GencalError::InvalidRequest(format!("invalid SPW selector {part:?}"))
            })?;
            let end = end.parse::<i32>().map_err(|_| {
                GencalError::InvalidRequest(format!("invalid SPW selector {part:?}"))
            })?;
            for id in start.min(end)..=start.max(end) {
                ids.insert(id);
            }
        } else {
            ids.insert(part.parse::<i32>().map_err(|_| {
                GencalError::InvalidRequest(format!("invalid SPW selector {part:?}"))
            })?);
        }
    }
    if ids.iter().any(|id| *id < 0 || *id >= count) {
        return Err(GencalError::InvalidRequest(format!(
            "SPW selector {selector:?} is outside 0..{}",
            count.saturating_sub(1)
        )));
    }
    Ok(ids.into_iter().collect())
}

fn matching_gaincurve(
    table: &Table,
    time_seconds: f64,
    frequency_hz: f64,
    band: Option<&str>,
    antenna_key: &str,
) -> Result<Vec<f32>, GencalError> {
    if let Some(band) = band {
        if let Some(gain) = matching_gaincurve_from_rows(
            table,
            time_seconds,
            |row| {
                get_string(table, row, COL_BANDNAME)
                    .map(|row_band| row_band == band)
                    .unwrap_or(false)
            },
            antenna_key,
        )? {
            return Ok(gain);
        }
    }
    matching_gaincurve_from_rows(
        table,
        time_seconds,
        |row| {
            let Ok(bfreq) = get_f64(table, row, COL_BFREQ) else {
                return false;
            };
            let Ok(efreq) = get_f64(table, row, COL_EFREQ) else {
                return false;
            };
            bfreq <= frequency_hz && efreq > frequency_hz
        },
        antenna_key,
    )?
    .ok_or_else(|| {
        GencalError::InvalidRequest(format!(
            "no VLA GainCurves row covers antenna {antenna_key}, time {time_seconds}, frequency {frequency_hz}"
        ))
    })
}

fn matching_gaincurve_from_rows(
    table: &Table,
    time_seconds: f64,
    mut row_matches: impl FnMut(usize) -> bool,
    antenna_key: &str,
) -> Result<Option<Vec<f32>>, GencalError> {
    let nominal_key = "0";
    let mut nominal = None;
    for row in 0..table.row_count() {
        if get_f64(table, row, COL_BTIME)? > time_seconds
            || get_f64(table, row, COL_ETIME)? <= time_seconds
            || !row_matches(row)
        {
            continue;
        }
        let antenna = get_string(table, row, COL_ANTENNA)?;
        let gain = get_f32_array_fortran(table, row, COL_GAIN)?;
        if antenna == antenna_key {
            return Ok(Some(gain));
        }
        if antenna == nominal_key {
            nominal = Some(gain);
        }
    }
    Ok(nominal)
}

fn spw_center_frequency_hz(
    spw: &casa_ms::subtables::MsSpectralWindow<'_>,
    row: usize,
) -> Result<f64, GencalError> {
    let freqs = spw.chan_freq(row).map_err(|source| {
        GencalError::InvalidRequest(format!(
            "failed to read SPECTRAL_WINDOW row {row}: {source}"
        ))
    })?;
    casa_middle_channel_frequency_hz(&freqs, row)
}

fn casa_middle_channel_frequency_hz(freqs: &[f64], row: usize) -> Result<f64, GencalError> {
    if freqs.is_empty() {
        return Err(GencalError::InvalidRequest(format!(
            "SPECTRAL_WINDOW row {row} has no channel frequencies"
        )));
    }
    // CASA EGainCurve uses chanfreqs(chanfreqs.nelements()/2), not the
    // arithmetic center, for the VLA efficiency lookup.
    Ok(freqs[freqs.len() / 2])
}

fn vla_spw_band(
    spw: &casa_ms::subtables::MsSpectralWindow<'_>,
    row: usize,
) -> Result<Option<String>, GencalError> {
    let name = spw.name(row).map_err(|source| {
        GencalError::InvalidRequest(format!(
            "failed to read SPECTRAL_WINDOW.NAME row {row}: {source}"
        ))
    })?;
    Ok(name
        .strip_prefix("EVLA_")
        .and_then(|rest| rest.split_once('#').map(|(band, _)| band.to_string())))
}

fn vla_gaincurve_antenna_keys(ms: &MeasurementSet) -> Result<Vec<String>, GencalError> {
    let table = ms
        .subtable(SubtableId::Antenna)
        .ok_or_else(|| GencalError::InvalidRequest("missing ANTENNA subtable".to_string()))?;
    let mut keys = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        let name = get_string(table, row, COL_NAME)?;
        let digits = name
            .chars()
            .skip_while(|ch| !ch.is_ascii_digit())
            .collect::<String>();
        let stripped = digits.trim_start_matches('0');
        keys.push(if stripped.is_empty() {
            row.to_string()
        } else {
            stripped.to_string()
        });
    }
    Ok(keys)
}

fn vla_efficiency(frequency_ghz: f64) -> f64 {
    const FREQ: [f64; 42] = [
        1.0,
        1.1,
        1.2,
        1.3,
        1.4,
        1.5,
        1.6,
        1.7,
        1.8,
        1.9,
        2.0,
        2.0 + 1e-9,
        2.3,
        2.7,
        3.0,
        3.5,
        3.7,
        4.0,
        4.0 + 1e-9,
        5.0,
        6.0,
        7.0,
        8.0,
        8.0 + 1e-9,
        12.0,
        12.0 + 1e-9,
        13.0,
        14.0,
        15.0,
        16.0,
        17.0,
        18.0,
        19.0,
        24.0,
        26.0,
        26.5,
        28.0,
        33.0,
        38.0,
        40.0,
        43.0,
        48.0,
    ];
    const EFF: [f64; 42] = [
        0.45, 0.48, 0.48, 0.45, 0.46, 0.45, 0.43, 0.44, 0.44, 0.49, 0.48, 0.52, 0.52, 0.51, 0.53,
        0.55, 0.53, 0.54, 0.55, 0.54, 0.56, 0.62, 0.64, 0.60, 0.60, 0.65, 0.65, 0.62, 0.58, 0.59,
        0.60, 0.60, 0.57, 0.52, 0.48, 0.50, 0.49, 0.42, 0.35, 0.29, 0.28, 0.26,
    ];
    let scaled = EFF.map(|value| (value / 5.622).sqrt());
    if frequency_ghz <= FREQ[0] {
        return scaled[0];
    }
    for index in 1..FREQ.len() {
        if frequency_ghz <= FREQ[index] {
            let fraction = (frequency_ghz - FREQ[index - 1]) / (FREQ[index] - FREQ[index - 1]);
            return scaled[index - 1] + (scaled[index] - scaled[index - 1]) * fraction;
        }
    }
    scaled[scaled.len() - 1]
}

fn first_observation_time(ms: &MeasurementSet) -> Result<f64, GencalError> {
    let table = ms
        .subtable(SubtableId::Observation)
        .ok_or_else(|| GencalError::InvalidRequest("missing OBSERVATION subtable".to_string()))?;
    match table
        .cell_accessor(0, COL_TIME_RANGE)
        .and_then(|cell| cell.array())
        .map_err(|error| GencalError::UnsupportedTableValue(error.to_string()))?
    {
        ArrayValue::Float64(values) => values.iter().next().copied().ok_or_else(|| {
            GencalError::UnsupportedTableValue("OBSERVATION.TIME_RANGE is empty".to_string())
        }),
        other => Err(GencalError::UnsupportedTableValue(format!(
            "OBSERVATION.TIME_RANGE has shape {:?}, expected Float64 array",
            other.shape()
        ))),
    }
}

fn first_main_time(ms: &MeasurementSet) -> Result<f64, GencalError> {
    if ms.main_table().row_count() == 0 {
        return Ok(0.0);
    }
    get_f64(ms.main_table(), 0, COL_TIME)
}

fn get_string(table: &Table, row: usize, column: &str) -> Result<String, GencalError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(|error| GencalError::UnsupportedTableValue(error.to_string()))?
    {
        ScalarValue::String(value) => Ok(value.clone()),
        other => Err(GencalError::UnsupportedTableValue(format!(
            "{column} row {row} is {other:?}, expected string"
        ))),
    }
}

fn get_f64(table: &Table, row: usize, column: &str) -> Result<f64, GencalError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(|error| GencalError::UnsupportedTableValue(error.to_string()))?
    {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        other => Err(GencalError::UnsupportedTableValue(format!(
            "{column} row {row} is {other:?}, expected float"
        ))),
    }
}

fn get_f32_array_fortran(table: &Table, row: usize, column: &str) -> Result<Vec<f32>, GencalError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(|error| GencalError::UnsupportedTableValue(error.to_string()))?
    {
        ArrayValue::Float32(values) => {
            let shape = values.shape();
            if shape.len() != 2 {
                return Ok(values.iter().copied().collect());
            }
            let mut out = Vec::with_capacity(shape[0] * shape[1]);
            for j in 0..shape[1] {
                for i in 0..shape[0] {
                    out.push(values[[i, j]]);
                }
            }
            Ok(out)
        }
        other => Err(GencalError::UnsupportedTableValue(format!(
            "{column} row {row} has shape {:?}, expected Float32 array",
            other.shape()
        ))),
    }
}

fn f32_column(values: &[f32]) -> ArrayValue {
    ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[values.len(), 1]).f(), values.to_vec())
            .expect("float gencal vector should reshape to receptor x channel"),
    )
}

fn bool_column(values: &[bool]) -> ArrayValue {
    ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[values.len(), 1]).f(), values.to_vec())
            .expect("flag gencal vector should reshape to receptor x channel"),
    )
}

fn default_vla_gaincurve_table() -> Option<PathBuf> {
    if let Ok(path) = env::var("CASA_RS_VLA_GAINCURVES") {
        return Some(PathBuf::from(path));
    }
    if let Ok(path) = env::var("CASA_RS_MEASURESPATH") {
        return Some(PathBuf::from(path).join("nrao/VLA/GainCurves"));
    }
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".casa/data/nrao/VLA/GainCurves"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn casa_middle_channel_frequency_uses_upper_middle_for_even_spw() {
        let freqs = [100.0, 125.0, 150.0, 175.0];
        assert_eq!(casa_middle_channel_frequency_hz(&freqs, 0).unwrap(), 150.0);
    }

    #[test]
    fn casa_middle_channel_frequency_rejects_empty_spw() {
        let err = casa_middle_channel_frequency_hz(&[], 7).unwrap_err();
        assert!(err.to_string().contains("SPECTRAL_WINDOW row 7"));
    }
}
