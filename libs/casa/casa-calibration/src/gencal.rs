// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native generation of externally specified CASA calibration tables.

mod antpos_evla;
mod antpos_file;
mod eop;
pub mod jyperk;
mod power_curve;
mod specify_tool;
mod spectral_axis;
mod switched_power;
pub(crate) mod tecim;
mod tsys;

pub use antpos_file::AntposInputError;
pub use specify_tool::CalibrationSpecification;
use spectral_axis::write_prior_spectral_axis;
pub(crate) use spectral_axis::write_spectral_axis;
pub(crate) use switched_power::CORRELATOR_EFFICIENCY as EVLA_CORRELATOR_EFFICIENCY;
pub(crate) use switched_power::input_paths as switched_power_input_paths;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::{Path, PathBuf};

use casa_core::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM, COL_INTERVAL,
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
const COL_TELESCOPE_NAME: &str = "TELESCOPE_NAME";
pub(crate) const POWER_CURVE_TERMS: usize = 8;

/// Supported native `gencal` families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GencalType {
    /// Multiplicative per-receptor amplitude (`G Jones`).
    Amp,
    /// Per-receptor phase in degrees (`G Jones`).
    Ph,
    /// Additive single-band delay in nanoseconds (`K Jones`).
    Sbd,
    /// Additive multi-band delay in nanoseconds (`K Jones`).
    Mbd,
    /// Antenna-position delay correction table (`KAntPos Jones`).
    Antpos,
    /// Legacy VLA antenna offsets, rotated into ITRF (`KAntPos Jones`).
    #[serde(rename = "antposvla")]
    AntposVla,
    /// VLA gain curve alone (`EGainCurve`).
    Gc,
    /// VLA gain-curve plus antenna-efficiency table (`EGainCurve`).
    Gceff,
    /// VLA efficiency alone (`EGainCurve`).
    Eff,
    /// System temperature from the MeasurementSet's SYSCAL subtable (`B TSYS`).
    Tsys,
    /// Zenith-opacity table (`TOpac`).
    Opac,
    /// VLA switched-power gain and system temperature (`G EVLASWPOW`).
    Swpow,
    /// CASA's legacy spelling of `swpow`.
    Evlagain,
    /// VLA switched-power gain divided by the requantizer factor.
    #[serde(rename = "swp/rq")]
    SwpOverRq,
    /// VLA requantizer gains and switched-power weights.
    Swpwts,
    /// VLA requantizer gain and corresponding weight scale.
    Rq,
    /// Convert an ALMA power scale from kelvins to Jy (`G Jones`).
    Jyperk,
    /// Line-of-sight ionospheric total electron content (`F Jones`).
    Tecim,
    /// Delay and phase corrections for updated Earth-orientation parameters.
    Eop,
}

impl GencalType {
    fn table_subtype(self) -> &'static str {
        match self {
            Self::Amp | Self::Ph | Self::Jyperk => "G Jones",
            Self::Sbd | Self::Mbd => "K Jones",
            Self::Antpos | Self::AntposVla => "KAntPos Jones",
            Self::Gc | Self::Gceff | Self::Eff => "EGainCurve",
            Self::Tsys => "B TSYS",
            Self::Tecim => "F Jones",
            Self::Eop => "Fringe Jones",
            Self::Opac => "TOpac",
            Self::Swpow | Self::Evlagain | Self::SwpOverRq | Self::Swpwts | Self::Rq => {
                "G EVLASWPOW"
            }
        }
    }

    fn parameter_column(self) -> (&'static str, PrimitiveType, &'static str) {
        match self {
            Self::Amp | Self::Ph | Self::Jyperk => {
                (COL_CPARAM, PrimitiveType::Complex32, "Complex")
            }
            _ => (COL_FPARAM, PrimitiveType::Float32, "Float"),
        }
    }

    pub fn is_gain_curve(self) -> bool {
        matches!(self, Self::Gc | Self::Gceff | Self::Eff)
    }

    fn is_switched_power(self) -> bool {
        self.table_subtype() == "G EVLASWPOW"
    }
}

impl std::str::FromStr for GencalType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "amp" => Ok(Self::Amp),
            "ph" => Ok(Self::Ph),
            "sbd" => Ok(Self::Sbd),
            "mbd" => Ok(Self::Mbd),
            "antpos" => Ok(Self::Antpos),
            "antposvla" => Ok(Self::AntposVla),
            "gceff" | "gc" => Ok(Self::Gceff),
            "eff" => Ok(Self::Eff),
            "tsys" => Ok(Self::Tsys),
            "opac" | "opacity" => Ok(Self::Opac),
            "swpow" => Ok(Self::Swpow),
            "evlagain" => Ok(Self::Evlagain),
            "swp/rq" => Ok(Self::SwpOverRq),
            "swpwts" => Ok(Self::Swpwts),
            "rq" => Ok(Self::Rq),
            "jyperk" => Ok(Self::Jyperk),
            "tecim" => Ok(Self::Tecim),
            "eop" => Ok(Self::Eop),
            other => Err(format!(
                "unsupported gencal caltype {other:?}; expected amp, ph, sbd, mbd, antpos, antposvla, gc, gceff, eff, opac, tsys, swpow, evlagain, swp/rq, swpwts, rq, jyperk, tecim, or eop"
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
    /// CASA-style antenna selection for antenna positions and specified priors.
    #[serde(default)]
    pub antenna: String,
    /// CASA-style spectral-window selection for specified priors.
    #[serde(default)]
    pub spw: String,
    /// Numeric parameters. `antpos` expects triples of antenna offsets.
    /// `opac` accepts one broadcast value or values ordered by spectral
    /// window and then antenna. An omitted axis broadcasts across that axis.
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
    CalibrationTable(#[from] crate::table::CalibrationTableWriteError),
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
    /// A supplied ALMA position file or its arguments are invalid.
    #[error(transparent)]
    PositionInput(#[from] AntposInputError),
    /// The MeasurementSet does not identify one observation for automatic lookup.
    #[error("{0}")]
    AutomaticPositionInput(String),
    /// An explicitly supplied Earth-orientation file could not be read.
    #[error("read Earth-orientation file {path}: {source}")]
    EopFile {
        /// File the caller selected.
        path: PathBuf,
        /// File-system error.
        source: std::io::Error,
    },
    /// A VLA archive request failed after the initial connection probe.
    #[error("{source} at {url}")]
    VlaArchive {
        /// Archive URL, including the year being retrieved.
        url: String,
        /// HTTP or transport error.
        source: Box<ureq::Error>,
    },
    /// A needed value in a CASA table had an unsupported type or shape.
    #[error("{0}")]
    UnsupportedTableValue(String),
}

#[derive(Debug, Clone)]
struct PriorCalRow {
    time_seconds: f64,
    interval_seconds: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    observation_id: i32,
    scan_number: i32,
    parameters: ArrayValue,
    snr_value: f32,
    preserved: Option<RecordValue>,
}

/// Generate one native prior calibration table.
pub fn gencal(request: &GencalRequest) -> Result<GencalReport, GencalError> {
    gencal_with_output_mode(
        request,
        "",
        None,
        None,
        true,
        crate::output::CalibrationOutputMode::Update,
    )
}

/// Generate a prior calibration table using an explicit output conflict policy.
pub fn gencal_with_output_mode(
    request: &GencalRequest,
    polarization: &str,
    infile: Option<&Path>,
    ant_pos_time_limit: Option<i64>,
    uniform: bool,
    mode: crate::output::CalibrationOutputMode,
) -> Result<GencalReport, GencalError> {
    generate_prior(
        request,
        polarization,
        infile,
        ant_pos_time_limit,
        uniform,
        mode,
        None,
    )
}

fn generate_prior(
    request: &GencalRequest,
    polarization: &str,
    infile: Option<&Path>,
    ant_pos_time_limit: Option<i64>,
    uniform: bool,
    mode: crate::output::CalibrationOutputMode,
    tool: Option<specify_tool::PriorCalibrationTool<'_>>,
) -> Result<GencalReport, GencalError> {
    if request.caltype == GencalType::Eop {
        return eop::generate(request, infile, mode);
    }
    if request.caltype == GencalType::Tecim {
        return tecim::generate(request, infile, mode);
    }
    if request.caltype == GencalType::Jyperk {
        return jyperk::calibrate(
            request,
            infile,
            &jyperk::ServiceOptions::default(),
            None,
            mode,
        )
        .map_err(|error| GencalError::InvalidRequest(error.to_string()));
    }
    let ms = open_measurement_set(&request.measurement_set)?;
    let switched_power = request
        .caltype
        .is_switched_power()
        .then(|| switched_power::input_paths(&ms, &request.measurement_set))
        .transpose()?;
    if let Some((power, noise_cal)) = &switched_power {
        for input in [power, noise_cal] {
            refuse_prior_input_overlap(request, input)?;
        }
    }
    let embedded = match request.caltype {
        GencalType::Gc | GencalType::Gceff
            if tool.as_ref().is_none_or(|tool| !tool.legacy_iterator) =>
        {
            power_curve::source_path(&request.measurement_set, ms.main_table())?
        }
        _ => None,
    };
    let linked_input = match request.caltype {
        GencalType::Tsys => ms.subtable(SubtableId::SysCal).and_then(Table::path),
        GencalType::Antpos => infile,
        _ => embedded.as_deref(),
    };
    if let Some(path) = linked_input {
        refuse_prior_input_overlap(request, path)?;
    }
    let positions = match (request.caltype, infile, ant_pos_time_limit) {
        (GencalType::Antpos, Some(path), _) => {
            Some(antpos_file::resolve(&ms, request, polarization, path)?)
        }
        (GencalType::Antpos, None, Some(limit)) if request.antenna.is_empty() => {
            let positions = antpos_evla::resolve(&ms, request, limit)?;
            if positions.is_none() {
                tracing::warn!("No offsets found. No caltable created.");
                return Ok(GencalReport {
                    output_table: request.output_table.clone(),
                    caltype: request.caltype,
                    table_subtype: request.caltype.table_subtype().to_string(),
                    row_count: 0,
                    spectral_window_ids: Vec::new(),
                    antenna_ids: Vec::new(),
                });
            }
            positions
        }
        _ => None,
    };
    let request = positions.as_ref().unwrap_or(request);
    // Reserve before reading prior parameters, so concurrent updates cannot
    // both accumulate from the same old product and silently lose a correction.
    let output = crate::output::CalibrationOutput::prepare(&request.output_table, mode)?;
    let updating = mode == crate::output::CalibrationOutputMode::Update
        && request.output_table.exists()
        && !request.caltype.is_gain_curve()
        && request.caltype != GencalType::Tsys;
    let mut collapse_spw = true;
    let rows = match request.caltype {
        GencalType::Amp | GencalType::Ph | GencalType::Sbd | GencalType::Mbd | GencalType::Opac => {
            specified_rows(&ms, request, polarization, updating)?
        }
        GencalType::Antpos | GencalType::AntposVla => antpos_rows(&ms, request, updating)?,
        GencalType::Gc | GencalType::Gceff | GencalType::Eff => {
            validate_prior_selection(&ms, request)?;
            if let Some(path) = embedded.as_deref() {
                power_curve::rows(&ms, path)?
            } else {
                gaincurve_rows(&ms, request, infile)?
            }
        }
        GencalType::Tsys => {
            validate_prior_selection(&ms, request)?;
            let (rows, spectral) = tsys::rows(&ms, uniform)?;
            collapse_spw = !spectral;
            rows
        }
        GencalType::Swpow
        | GencalType::Evlagain
        | GencalType::SwpOverRq
        | GencalType::Swpwts
        | GencalType::Rq => {
            validate_prior_selection(&ms, request)?;
            let (power, noise_cal) = switched_power.expect("switched-power inputs resolved");
            let selection = tool
                .as_ref()
                .filter(|tool| !tool.legacy_iterator)
                .map(|tool| tool.selection);
            switched_power::rows(&ms, &power, &noise_cal, request.caltype, selection)?
        }
        GencalType::Jyperk => unreachable!("Jy/K has its own factor source"),
        GencalType::Tecim => unreachable!("TEC has its own image source"),
        GencalType::Eop => unreachable!("EOP has its own measures source"),
    };
    let subtype = if embedded.is_some() {
        "EPowerCurve"
    } else {
        request.caltype.table_subtype()
    };
    write_prior_caltable(&ms, request, subtype, &rows, output, mode, collapse_spw)
}

/// Load the existing `setSpecify` parameters and metadata. The ordered gain
/// and delay selectors apply their corrections directly to the loaded cells.
fn apply_existing_parameters(
    request: &GencalRequest,
    rows: &mut [PriorCalRow],
) -> Result<(), GencalError> {
    crate::output::validate_destination(&request.output_table, "Calibration")?;
    let old = crate::output_merge::open(&request.output_table)?;
    if old.keywords().get("VisCal")
        != Some(&Value::Scalar(ScalarValue::String(
            request.caltype.table_subtype().to_string(),
        )))
    {
        return Err(GencalError::InvalidRequest(
            "existing table has incompatible calibration type; prior product is unchanged"
                .to_string(),
        ));
    }
    let (column, parameter_type, _) = request.caltype.parameter_column();
    let mut prior = BTreeMap::new();
    for row in old
        .rows()
        .map_err(|e| GencalError::InvalidRequest(e.to_string()))?
    {
        let id = |column| match row.get(column) {
            Some(Value::Scalar(ScalarValue::Int32(v))) => Ok(*v),
            _ => Err(GencalError::InvalidRequest(format!(
                "missing {column} in existing gencal table"
            ))),
        };
        let key = (id(COL_SPECTRAL_WINDOW_ID)?, id(COL_ANTENNA1)?);
        let valid = match row.get(column) {
            Some(Value::Array(ArrayValue::Float32(_))) => parameter_type == PrimitiveType::Float32,
            Some(Value::Array(ArrayValue::Complex32(_))) => {
                parameter_type == PrimitiveType::Complex32
            }
            _ => false,
        };
        if !valid {
            return Err(GencalError::InvalidRequest(format!(
                "existing table has no {parameter_type:?} {column}"
            )));
        }
        if prior.insert(key, row.clone()).is_some() {
            return Err(GencalError::InvalidRequest(
                "existing gencal table has duplicate antenna/SPW entries".to_string(),
            ));
        }
    }
    for row in rows {
        // setSpecify's existing-table branch leaves currObs at its default
        // -1, then keepNCT writes that for the regenerated rows.
        row.observation_id = -1;
        if let Some(metadata) = prior.remove(&(row.spw_id, row.antenna_id)) {
            match (&mut row.parameters, metadata.get(column)) {
                (ArrayValue::Float32(delta), Some(Value::Array(ArrayValue::Float32(previous))))
                    if delta.shape() == previous.shape() =>
                {
                    delta.assign(previous);
                }
                (
                    ArrayValue::Complex32(factor),
                    Some(Value::Array(ArrayValue::Complex32(previous))),
                ) if factor.shape() == previous.shape() => {
                    factor.assign(previous);
                }
                _ => {
                    return Err(GencalError::InvalidRequest(format!(
                        "existing gencal {column} type or shape is incompatible",
                    )));
                }
            }
            row.preserved = Some(metadata);
        }
    }
    if let Some((&(window, antenna), _)) = prior.first_key_value() {
        return Err(GencalError::InvalidRequest(format!(
            "Error in Calibrater::specifycal. Existing table has an entry for window {window}, antenna {antenna}, outside the input grid."
        )));
    }
    Ok(())
}

fn specified_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
    polarization: &str,
    updating: bool,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let mut rows = None;
    apply_specified_rows(ms, request, polarization, updating, &mut rows)?;
    Ok(rows.expect("specified rows have been initialized"))
}

fn apply_specified_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
    polarization: &str,
    updating: bool,
    rows: &mut Option<Vec<PriorCalRow>>,
) -> Result<(), GencalError> {
    let invalid = |message: String| {
        GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {message}"))
    };
    let antenna_count = ms
        .antenna()
        .map_err(|error| invalid(error.to_string()))?
        .row_count();
    let window_count = ms
        .spectral_window()
        .map_err(|error| invalid(error.to_string()))?
        .row_count();

    let repeat_windows = request.spw.is_empty() || request.spw == "*";
    let windows = if repeat_windows {
        (0..window_count).collect::<Vec<_>>()
    } else {
        ms.resolve_spw_selector(&request.spw)
            .map_err(|error| invalid(error.to_string()))?
            .channel_list()
            .iter()
            .map(|range| range.spw_id)
            .collect()
    };
    let antennas = if request.antenna == "*" {
        Vec::new()
    } else {
        ms.resolve_specified_antennas(&request.antenna)
            .map_err(|error| invalid(error.to_string()))?
    };
    let antenna_groups = if antennas.is_empty() {
        vec![(0..antenna_count).collect::<Vec<_>>()]
    } else {
        antennas.into_iter().map(|id| vec![id]).collect()
    };
    let scalar = request.caltype == GencalType::Opac;
    let receptor_groups: &[&[usize]] = match (scalar, polarization) {
        (_, "R" | "X") | (true, "") => &[&[0]],
        (false, "") => &[&[0, 1]],
        (false, "L" | "Y") => &[&[1]],
        (false, "R,L" | "X,Y") => &[&[0], &[1]],
        (false, "L,R" | "Y,X") => &[&[1], &[0]],
        _ => return Err(invalid("Invalid pol specification".into())),
    };

    // An omitted axis broadcasts the parameter. Explicit windows, antennas
    // and receptors use caller order, with the receptor varying fastest.
    let per_window = antenna_groups.len() * receptor_groups.len();
    let required = per_window * if repeat_windows { 1 } else { windows.len() };
    let given = request.parameter.len();
    if antenna_groups.is_empty()
        || windows.is_empty()
        || given == 0
        || (given != 1 && given != required)
    {
        return Err(invalid(format!(
            "Inconsistent number of parameters specified (expected 1 or {required}, got {given}).",
        )));
    }
    if rows.is_none() {
        let identity = if request.caltype.parameter_column().1 == PrimitiveType::Complex32 {
            complex_column([Complex32::new(1.0, 0.0); 2])
        } else {
            f32_column(&[0.0; 2][..if scalar { 1 } else { 2 }])
        };
        let mut nominal = (0..antenna_count * window_count)
            .map(|row| PriorCalRow {
                time_seconds: 0.0,
                interval_seconds: 0.0,
                field_id: -1,
                spw_id: (row / antenna_count) as i32,
                antenna_id: (row % antenna_count) as i32,
                observation_id: 0,
                scan_number: -1,
                parameters: identity.clone(),
                snr_value: 1.0,
                preserved: None,
            })
            .collect::<Vec<_>>();
        if updating {
            apply_existing_parameters(request, &mut nominal)?;
        }
        *rows = Some(nominal);
    }
    let rows = rows.as_mut().expect("specified rows have been initialized");
    let factors = request
        .parameter
        .iter()
        .map(|value| match request.caltype {
            GencalType::Ph => {
                let (sin, cos) = (value * std::f64::consts::PI / 180.0).sin_cos();
                Complex32::new(cos as f32, sin as f32)
            }
            _ => Complex32::new(*value as f32, 0.0),
        })
        .collect::<Vec<_>>();
    for (window_order, window) in windows.into_iter().enumerate() {
        let window_offset = if repeat_windows {
            0
        } else {
            window_order * per_window
        };
        for (antenna_order, antennas) in antenna_groups.iter().enumerate() {
            for (receptor_order, receptors) in receptor_groups.iter().enumerate() {
                let index = window_offset + antenna_order * receptor_groups.len() + receptor_order;
                let value = factors[index % given];
                for antenna in antennas {
                    let parameters = &mut rows[window * antenna_count + antenna].parameters;
                    for receptor in *receptors {
                        match parameters {
                            ArrayValue::Complex32(gains) => {
                                let gain = &mut gains[[*receptor, 0]];
                                *gain *= value;
                                if request.caltype == GencalType::Jyperk
                                    && (!gain.re.is_finite()
                                        || !gain.im.is_finite()
                                        || (gain.re == 0.0 && gain.im == 0.0))
                                {
                                    return Err(invalid(format!(
                                        "Accumulated Jy/K gain must be finite and nonzero for antenna {antenna}, SPW {window}, receptor {receptor}."
                                    )));
                                }
                            }
                            ArrayValue::Float32(values) => values[[*receptor, 0]] += value.re,
                            _ => unreachable!(
                                "specified prior columns contain complex or real parameters"
                            ),
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn complex_column(values: [Complex32; 2]) -> ArrayValue {
    ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), values.to_vec()).expect("two receptors"),
    )
}

fn antpos_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
    updating: bool,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let invalid = |detail: String| {
        GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {detail}"))
    };
    if !request.spw.is_empty() && request.spw != "*" {
        ms.resolve_spw_selector(&request.spw)
            .map_err(|error| invalid(error.to_string()))?;
    }
    if request.caltype == GencalType::Antpos && request.antenna.trim().is_empty() {
        return Err(invalid(
            "antpos requires resolved antenna selections and position offsets".into(),
        ));
    }
    let antenna_count = ms
        .antenna()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "ANTENNA".to_string(),
            source: Box::new(source),
        })?
        .row_count();
    if request.parameter.is_empty() || !request.parameter.len().is_multiple_of(3) {
        return Err(invalid(
            "antpos requires three parameters per antenna.".into(),
        ));
    }
    let selected = match request.antenna.as_str() {
        "" | "*" => Vec::new(),
        selector => ms
            .resolve_specified_antennas(selector)
            .map_err(|error| invalid(error.to_string()))?,
    };
    let broadcast = selected.is_empty();
    let selected = if broadcast {
        (0..antenna_count).collect()
    } else {
        selected
    };
    let rotate = if request.caltype == GencalType::AntposVla {
        let provider =
            casa_ms::open_measures_runtime().map_err(|error| invalid(error.to_string()))?;
        let vla = casa_core::measures::position::MPosition::from_observatory_name(
            "VLA",
            provider.as_ref(),
        )
        .map_err(|error| invalid(error.to_string()))?
        .ok_or_else(|| invalid("missing VLA observatory position".into()))?;
        Some(vla.longitude_rad().sin_cos())
    } else {
        None
    };
    let mut rows = (0..antenna_count)
        .map(|antenna_id| PriorCalRow {
            time_seconds: 0.0,
            interval_seconds: 0.0,
            field_id: -1,
            spw_id: 0,
            antenna_id: antenna_id as i32,
            observation_id: 0,
            scan_number: -1,
            parameters: f32_column(&[0.0; 3]),
            snr_value: 1.0,
            preserved: None,
        })
        .collect::<Vec<_>>();
    if updating {
        apply_existing_parameters(request, &mut rows)?;
    }
    let mut offsets = request.parameter.as_chunks::<3>().0.to_vec();
    let triple_count = offsets.len();
    for (order, antenna) in selected.into_iter().enumerate() {
        let offset = &mut offsets[if broadcast { 0 } else { order % triple_count }];
        // CASA rotates a view of each triple. An explicit selection that
        // cycles through a short vector rotates the shared triple again.
        if let Some((sin, cos)) = rotate
            && (!broadcast || order == 0)
        {
            let [x, y, z] = *offset;
            *offset = [x * cos - y * sin, x * sin + y * cos, z];
        }
        let ArrayValue::Float32(values) = &mut rows[antenna].parameters else {
            unreachable!("antenna positions use real parameters");
        };
        for (coordinate, &delta) in offset.iter().enumerate() {
            values[[coordinate, 0]] += delta as f32;
        }
    }
    Ok(rows)
}

fn validate_prior_selection(
    ms: &MeasurementSet,
    request: &GencalRequest,
) -> Result<(), GencalError> {
    let invalid = |detail: String| {
        GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {detail}"))
    };
    // Calibrater resolves these expressions before the selected family
    // generates its own antenna and spectral-window grid.
    if !request.spw.is_empty() && request.spw != "*" {
        ms.resolve_spw_selector(&request.spw)
            .map_err(|error| invalid(error.to_string()))?;
    }
    if !request.antenna.is_empty() && request.antenna != "*" {
        ms.resolve_reference_antennas(&request.antenna)
            .map_err(|error| invalid(error.to_string()))?;
    }
    Ok(())
}

fn gaincurve_rows(
    ms: &MeasurementSet,
    request: &GencalRequest,
    gaincurve_infile: Option<&Path>,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let invalid = |detail: String| {
        GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {detail}"))
    };
    let use_curve = request.caltype != GencalType::Eff;
    let use_efficiency = request.caltype != GencalType::Gc;

    let explicit = gaincurve_infile.or(request.gaincurve_table.as_deref());
    let source_path = if let Some(explicit) = explicit {
        explicit.to_path_buf()
    } else {
        let observation = ms
            .subtable(SubtableId::Observation)
            .ok_or_else(|| invalid("missing OBSERVATION subtable".into()))?;
        let telescope = get_string(observation, 0, COL_TELESCOPE_NAME)?;
        if !telescope.contains("VLA") {
            return Err(invalid(format!(
                "Automatic gain curve not supported for {telescope}"
            )));
        }
        default_vla_gaincurve_table().ok_or_else(|| {
            invalid("the standard VLA GainCurves table is missing from the CASA data tree".into())
        })?
    };
    refuse_prior_input_overlap(request, &source_path)?;
    let gaincurves = Table::open(TableOptions::new(&source_path)).map_err(|source| {
        GencalError::OpenGainCurves {
            path: source_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let antenna_keys = vla_gaincurve_antenna_keys(ms, gaincurve_infile.is_none())?;
    // The native gaincurve_table extension also accepts shorter polynomials.
    let require_full_curve = gaincurve_infile.is_some() || request.gaincurve_table.is_none();
    let spw = ms
        .spectral_window()
        .map_err(|source| GencalError::OpenMeasurementSet {
            path: "SPECTRAL_WINDOW".to_string(),
            source: Box::new(source),
        })?;
    let obs_time = first_observation_time(ms).or_else(|_| first_main_time(ms))?;
    let mut valid_rows = Vec::new();
    for row in 0..gaincurves.row_count() {
        if get_f64(&gaincurves, row, COL_BTIME)? <= obs_time
            && get_f64(&gaincurves, row, COL_ETIME)? > obs_time
        {
            valid_rows.push(row);
        }
    }
    let flat = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0];
    let mut found_curve = false;
    let mut rows = Vec::with_capacity(antenna_keys.len() * spw.row_count());
    for window in 0..spw.row_count() {
        let center_hz = spw_center_frequency_hz(&spw, window)?;
        let curves = if use_curve {
            gaincurves_for_window(
                &gaincurves,
                &valid_rows,
                center_hz,
                vla_spw_band(&spw, window)?.as_deref(),
                require_full_curve,
            )?
        } else {
            BTreeMap::new()
        };
        found_curve |= !curves.is_empty();
        let nominal = curves.get("0").map_or(&flat[..], Vec::as_slice);
        let efficiency = if use_efficiency {
            vla_efficiency(center_hz / 1.0e9) as f32
        } else {
            1.0
        };
        for (antenna, key) in antenna_keys.iter().enumerate() {
            let mut values = curves.get(key).map_or(nominal, Vec::as_slice).to_vec();
            for value in &mut values {
                *value *= efficiency;
            }
            rows.push(PriorCalRow {
                time_seconds: 0.0,
                interval_seconds: 0.0,
                field_id: -1,
                spw_id: window as i32,
                antenna_id: antenna as i32,
                observation_id: -1,
                scan_number: -1,
                parameters: f32_column(&values),
                snr_value: 0.0,
                preserved: None,
            });
        }
    }
    if use_curve && !found_curve {
        return Err(invalid("Found no gaincurve data for any spw.".into()));
    }
    Ok(rows)
}

fn refuse_prior_input_overlap(request: &GencalRequest, input: &Path) -> Result<(), GencalError> {
    let invalid = |reason| GencalError::PrepareOutput {
        path: request.output_table.display().to_string(),
        reason,
    };
    if crate::paths::overlap(&request.output_table, input)
        .map_err(|error| invalid(error.to_string()))?
    {
        return Err(invalid("caltable overlaps the prior input table".into()));
    }
    Ok(())
}

fn open_measurement_set(path: &Path) -> Result<MeasurementSet, GencalError> {
    MeasurementSet::open(path).map_err(|source| GencalError::OpenMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    })
}

pub fn prior_inputs_for_report(
    request: &GencalRequest,
    infile: Option<&Path>,
) -> Result<Vec<PathBuf>, GencalError> {
    if request.caltype == GencalType::Eop {
        return Ok(eop::input_paths(&request.measurement_set, infile));
    }
    if matches!(request.caltype, GencalType::Antpos | GencalType::Tecim) {
        return Ok(infile.into_iter().map(Path::to_path_buf).collect());
    }
    if request.caltype == GencalType::Tsys {
        let ms = open_measurement_set(&request.measurement_set)?;
        return Ok(ms
            .subtable(SubtableId::SysCal)
            .and_then(Table::path)
            .into_iter()
            .map(Path::to_path_buf)
            .collect());
    }
    if request.caltype.is_switched_power() {
        let ms = open_measurement_set(&request.measurement_set)?;
        let (power, noise_cal) = switched_power::input_paths(&ms, &request.measurement_set)?;
        return Ok(vec![power, noise_cal]);
    }
    if !request.caltype.is_gain_curve() {
        return Ok(Vec::new());
    }
    if matches!(request.caltype, GencalType::Gc | GencalType::Gceff) {
        let ms = &request.measurement_set;
        let main = Table::open(TableOptions::new(ms)).map_err(|source| {
            GencalError::OpenMeasurementSet {
                path: ms.display().to_string(),
                source: Box::new(casa_ms::MsError::from(source)),
            }
        })?;
        if let Some(path) = power_curve::source_path(ms, &main)? {
            return Ok(vec![path]);
        }
    }
    Ok(infile
        .map(Path::to_path_buf)
        .or_else(|| request.gaincurve_table.clone())
        .or_else(default_vla_gaincurve_table)
        .into_iter()
        .collect())
}

fn write_prior_caltable(
    ms: &MeasurementSet,
    request: &GencalRequest,
    subtype: &str,
    rows: &[PriorCalRow],
    output: crate::output::CalibrationOutput,
    mode: crate::output::CalibrationOutputMode,
    collapse_spw: bool,
) -> Result<GencalReport, GencalError> {
    let (parameter_column, parameter_type, parameter_name) = request.caltype.parameter_column();
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(parameter_column, parameter_type, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_core::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_core::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_core::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_core::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid gencal caltable schema");
    let mut writer = CalibrationTableWriter::staged(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype,
            parameter_type: Some(parameter_name),
            measurement_set_name: ms
                .path()
                .and_then(Path::file_name)
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        output,
        mode,
    );

    if matches!(request.caltype, GencalType::Antpos | GencalType::AntposVla)
        && let Some(value) = antpos_evla::tropo_keyword(ms, request, mode)?
    {
        writer
            .table_mut()
            .keywords_mut()
            .upsert(crate::antpos_vla::TROPO_KEYWORD, value);
    }
    if request.caltype == GencalType::Tecim {
        writer.table_mut().keywords_mut().upsert(
            tecim::HEIGHT_KEYWORD,
            Value::Scalar(ScalarValue::Float64(tecim::height_km(&request.parameter))),
        );
    }
    let specified_sky = crate::sd_solver::is_sky_subtype(subtype);
    if request.caltype == GencalType::Eop || specified_sky {
        // EOP appends in observation order. Sky prescriptions retain the
        // order of identical antenna pairs produced by the generic setter.
        writer.preserve_row_order();
    }

    for row in rows {
        let shape = IxDyn(row.parameters.shape());
        let tsys = request.caltype == GencalType::Tsys;
        let parameter_error = if tsys { 0.1 } else { 0.0 };
        let flags = if let ArrayValue::Float32(values) = &row.parameters
            && tsys
        {
            values.mapv(|value| value < f32::MIN_POSITIVE || value.is_nan())
        } else {
            ArrayD::from_elem(shape.clone(), false)
        };
        let errors = ArrayD::from_elem(shape.clone(), parameter_error);
        let snr = ArrayD::from_elem(shape, row.snr_value);
        let mut record = RecordValue::new(vec![
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
                Value::Scalar(ScalarValue::Int32(if specified_sky {
                    0
                } else {
                    row.antenna_id
                })),
            ),
            RecordField::new(
                COL_ANTENNA2,
                Value::Scalar(ScalarValue::Int32(if specified_sky { 0 } else { -1 })),
            ),
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
            RecordField::new(parameter_column, Value::Array(row.parameters.clone())),
            RecordField::new(COL_PARAMERR, Value::Array(ArrayValue::Float32(errors))),
            RecordField::new(COL_FLAG, Value::Array(ArrayValue::Bool(flags))),
            RecordField::new(COL_SNR, Value::Array(ArrayValue::Float32(snr))),
        ]);
        if let Some(prior) = &row.preserved {
            for column in [COL_PARAMERR, COL_FLAG, COL_SNR] {
                if let Some(value) = prior.get(column) {
                    record.upsert(column, value.clone());
                }
            }
            if request.caltype == GencalType::Eop
                && let Some(weight) = prior.get(COL_WEIGHT)
            {
                record.upsert(COL_WEIGHT, weight.clone());
            }
        }
        writer.append(record)?;
    }

    let present_windows = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    let eop = request.caltype == GencalType::Eop;
    crate::table_maintenance::write_standard_calibration_subtables(
        ms,
        writer.output_path(),
        eop.then_some(crate::writer::SingleChannelFrequency::MiddleChannel),
    )
    .map_err(GencalError::InvalidRequest)?;
    if !eop {
        write_prior_spectral_axis(
            &writer.output_path().join("SPECTRAL_WINDOW"),
            &present_windows,
            collapse_spw,
            specified_sky,
        )?;
    }
    writer.save_main()?;

    let spectral_window_ids = present_windows.into_iter().collect();
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

fn gaincurves_for_window(
    table: &Table,
    valid: &[usize],
    center_hz: f64,
    band: Option<&str>,
    require_full_curve: bool,
) -> Result<BTreeMap<String, Vec<f32>>, GencalError> {
    let mut selected = Vec::new();
    if table
        .schema()
        .is_some_and(|schema| schema.contains_column(COL_BANDNAME))
    {
        let band = band.unwrap_or("?");
        for &row in valid {
            if get_string(table, row, COL_BANDNAME)? == band {
                selected.push(row);
            }
        }
    } else if require_full_curve {
        return Err(GencalError::UnsupportedTableValue(
            "gain-curve input is missing BANDNAME".into(),
        ));
    }
    if selected.is_empty() {
        for &row in valid {
            if get_f64(table, row, COL_BFREQ)? <= center_hz
                && get_f64(table, row, COL_EFREQ)? > center_hz
            {
                selected.push(row);
            }
        }
    }
    let mut curves = BTreeMap::new();
    for row in selected {
        let antenna = get_string(table, row, COL_ANTENNA)?;
        if let std::collections::btree_map::Entry::Vacant(entry) = curves.entry(antenna) {
            let gain = get_f32_array_fortran(table, row, COL_GAIN)?;
            if require_full_curve && gain.len() != 8 {
                return Err(GencalError::UnsupportedTableValue(format!(
                    "gain-curve table row {row} has {} coefficients, expected eight",
                    gain.len(),
                )));
            }
            entry.insert(gain);
        }
    }
    Ok(curves)
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

fn vla_gaincurve_antenna_keys(
    ms: &MeasurementSet,
    normalize: bool,
) -> Result<Vec<String>, GencalError> {
    let table = ms
        .subtable(SubtableId::Antenna)
        .ok_or_else(|| GencalError::InvalidRequest("missing ANTENNA subtable".to_string()))?;
    let mut keys = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        let name = get_string(table, row, COL_NAME)?;
        if normalize {
            let start = name.find(|ch: char| ch.is_ascii_digit());
            let digits = start.map_or("", |index| &name[index..]);
            keys.push(digits.strip_prefix('0').unwrap_or(digits).to_string());
        } else {
            keys.push(name);
        }
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
    let upper = FREQ
        .partition_point(|sample| *sample < frequency_ghz)
        .clamp(1, FREQ.len() - 1);
    let lower = upper - 1;
    let fraction = (frequency_ghz - FREQ[lower]) / (FREQ[upper] - FREQ[lower]);
    scaled[lower] + (scaled[upper] - scaled[lower]) * fraction
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

pub fn default_vla_gaincurve_table() -> Option<PathBuf> {
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
