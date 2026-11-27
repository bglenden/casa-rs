// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert ALMA Jy/K factors to cumulative antenna gain corrections.

mod metadata;
mod service;

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use csv_core::ReadRecordResult;
use thiserror::Error;

fn python_string_repr(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

use super::{
    GencalError, GencalReport, GencalRequest, apply_specified_rows, open_measurement_set,
    refuse_prior_input_overlap, write_prior_caltable,
};
use crate::CalibrationOutputMode;
use crate::output::CalibrationOutput;

pub use service::{Endpoint, ServiceOptions};

#[derive(Debug, Error)]
pub enum JyperkError {
    #[error("There is no Jy/K db-derived factor file: {0}")]
    MissingFile(PathBuf),
    #[error("read Jy/K factor file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("read Jy/K factor file {path}: {source}")]
    ParseFile {
        path: PathBuf,
        source: std::str::Utf8Error,
    },
    #[error("generator raised StopIteration")]
    EmptyFile,
    #[error("No valid Jy/K factors found for this MS")]
    NoFactors,
    #[error("could not convert string to float: {0}")]
    InvalidNumber(String),
    #[error(
        "Jy/K factor {factor:?} must yield a finite positive gain for antenna {antenna:?} and SPW {window:?}"
    )]
    InvalidFactor {
        factor: String,
        antenna: String,
        window: String,
    },
    #[error("{0}")]
    Service(String),
    #[error("No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed")]
    AllSourcesFailed,
    #[error(transparent)]
    Generate(#[from] GencalError),
}

#[derive(Debug)]
pub(super) struct Factor {
    antenna: String,
    window: String,
    polarization: &'static str,
    gain: f64,
}

impl Factor {
    fn parse(
        antenna: &str,
        window: &str,
        polarization: &str,
        value: &str,
    ) -> Result<Self, JyperkError> {
        let factor = value
            .trim()
            .parse::<f64>()
            .map_err(|_| JyperkError::InvalidNumber(python_string_repr(value)))?;
        let gain = 1.0 / factor.sqrt();
        if !(gain as f32).is_finite() || gain as f32 <= 0.0 {
            return Err(JyperkError::InvalidFactor {
                factor: value.to_string(),
                antenna: antenna.to_string(),
                window: window.to_string(),
            });
        }
        let polarization = match polarization {
            "XX" => "X",
            "YY" => "Y",
            "RR" => "R",
            "LL" => "L",
            _ => "",
        };
        Ok(Self {
            antenna: antenna.to_string(),
            window: window.to_string(),
            polarization,
            gain,
        })
    }
}

pub fn calibrate(
    request: &GencalRequest,
    path: Option<&Path>,
    options: &ServiceOptions,
    backup_hosts: Option<&[String]>,
    mode: CalibrationOutputMode,
) -> Result<GencalReport, JyperkError> {
    let ms = open_measurement_set(&request.measurement_set)?;
    let file_factors = |path| {
        refuse_prior_input_overlap(request, path)?;
        let name = request.measurement_set.file_name().unwrap_or_default();
        read_factors(path, &name.to_string_lossy())
    };
    let env_url = std::env::var("JYPERKDB_URL").ok();
    let factors = if let Some(hosts) = backup_hosts {
        let mut result = Vec::new();
        for host in env_url
            .iter()
            .filter(|url| !url.trim().is_empty())
            .chain(hosts)
        {
            match service::lookup(&ms, request, options, host) {
                Ok(factors) if !factors.is_empty() => {
                    result = factors;
                    break;
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "Jy/K service lookup failed"),
            }
        }
        if !result.is_empty() {
            service_factors(result)?
        } else {
            file_factors(path.ok_or(JyperkError::AllSourcesFailed)?)?
        }
    } else if let Some(path) = path {
        file_factors(path)?
    } else {
        service_factors(service::lookup(
            &ms,
            request,
            options,
            env_url.as_deref().unwrap_or(service::DEFAULT_HOST),
        )?)?
    };
    Ok(generate(&ms, request, &factors, mode)?)
}

fn service_factors(rows: Vec<service::DatabaseFactor>) -> Result<Vec<Factor>, JyperkError> {
    if rows.is_empty() {
        return Err(JyperkError::NoFactors);
    }
    rows.iter()
        .map(|row| Factor::parse(&row.antenna, &row.window, "I", &row.value))
        .collect()
}

fn read_factors(path: &Path, measurement_set: &str) -> Result<Vec<Factor>, JyperkError> {
    if !path.is_file() {
        return Err(JyperkError::MissingFile(path.to_path_buf()));
    }
    let io_error = |source| JyperkError::ReadFile {
        path: path.to_path_buf(),
        source,
    };
    let content = fs::read(path).map_err(io_error)?;
    if content.is_empty() {
        return Err(JyperkError::EmptyFile);
    }
    // Validate before CSV removes separators. Otherwise malformed adjacent
    // fields could form a UTF-8 character spanning one of the field offsets.
    let encoding_error = |source| JyperkError::ParseFile {
        path: path.to_path_buf(),
        source,
    };
    std::str::from_utf8(&content).map_err(encoding_error)?;
    let mut input = content.as_slice();
    let mut reader = csv_core::Reader::new();
    let mut record = vec![0; 256];
    let mut ends = vec![0; 8];
    let mut factors = Vec::new();
    let mut first = !input.starts_with(b"\r") && !input.starts_with(b"\n");
    let mut len = 0;
    let mut fields = 0;
    loop {
        let (status, consumed, bytes, cells) =
            reader.read_record(input, &mut record[len..], &mut ends[fields..]);
        input = &input[consumed..];
        len += bytes;
        fields += cells;
        match status {
            ReadRecordResult::End => break,
            ReadRecordResult::InputEmpty => continue,
            ReadRecordResult::OutputFull => {
                record.resize(record.len() * 2, 0);
                continue;
            }
            ReadRecordResult::OutputEndsFull => {
                ends.resize(ends.len() * 2, 0);
                continue;
            }
            ReadRecordResult::Record => {}
        }
        let text = std::str::from_utf8(&record[..len]).map_err(encoding_error)?;
        let name = &text[..ends[0]];
        if !(name.is_empty()
            || name.starts_with('#')
            || (first
                && (name.trim().eq_ignore_ascii_case("MS") || name.trim_start().starts_with('#'))))
        {
            if fields != 5 {
                tracing::warn!("Jy/K factor file is invalid format");
            } else if name == measurement_set {
                let cell = |index: usize| &text[ends[index - 1]..ends[index]];
                factors.push(Factor::parse(cell(1), cell(2), cell(3), cell(4))?);
            }
        }
        first = false;
        len = 0;
        fields = 0;
    }
    if factors.is_empty() {
        return Err(JyperkError::NoFactors);
    }
    Ok(factors)
}

fn generate(
    ms: &MeasurementSet,
    request: &GencalRequest,
    factors: &[Factor],
    mode: CalibrationOutputMode,
) -> Result<GencalReport, GencalError> {
    let output = CalibrationOutput::prepare(&request.output_table, mode)?;
    let updating = mode == CalibrationOutputMode::Update && request.output_table.exists();
    let mut rows = None;
    let mut update = request.clone();
    update.parameter = vec![1.0];
    for factor in factors {
        update.antenna.clone_from(&factor.antenna);
        update.spw.clone_from(&factor.window);
        update.parameter[0] = factor.gain;
        apply_specified_rows(ms, &update, factor.polarization, updating, &mut rows)?;
    }
    let mut rows = rows.expect("validated Jy/K factors");
    if factors.len() > 1 {
        // The first specifycal creates the full grid. Each subsequent
        // specifycal update leaves the observation identifier at -1.
        for row in &mut rows {
            row.observation_id = -1;
        }
    }
    write_prior_caltable(ms, request, "G Jones", &rows, output, mode, true)
}
