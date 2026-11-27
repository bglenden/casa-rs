// SPDX-License-Identifier: LGPL-3.0-or-later
//! Append external ANTAB calibration to a staged copy of a MeasurementSet.

mod compute;
mod copy;
mod parse;
mod table;

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use casa_tables::publication::rename_without_replacing;
use serde::Serialize;

use self::compute::{Metadata, message};
use self::parse::Antab;

/// Request to append external ANTAB gain and system-temperature calibration to a copy of an MS.
pub struct AppendantabRequest {
    /// Input MeasurementSet.
    pub input: PathBuf,
    /// Destination MeasurementSet.
    pub output: PathBuf,
    /// Input ANTAB text file.
    pub antab: PathBuf,
    /// Overwrite existing GAIN_CURVE / SYSCAL rows.
    pub overwrite: bool,
    /// Append SYSCAL temperatures.
    pub append_tsys: bool,
    /// Append GAIN_CURVE entries.
    pub append_gain: bool,
}

/// Summary of an `appendantab` execution.
#[derive(Serialize)]
pub struct AppendantabSummary {
    /// Output MeasurementSet created, if published.
    pub output: Option<PathBuf>,
    /// SYSCAL rows written.
    pub tsys_rows: usize,
    /// GAIN_CURVE records written.
    pub gain_records: usize,
}

/// Error from `run_appendantab`, preserving parameter attribution where applicable.
pub enum AppendantabError {
    /// Execution failure without parameter attribution.
    Message(String),
    /// Parameter-attributed failure.
    At(&'static str, String),
}

/// Execute `appendantab`, writing a new MeasurementSet when `output` is non-empty and absent.
pub fn run_appendantab(
    request: &AppendantabRequest,
) -> Result<AppendantabSummary, AppendantabError> {
    let input = &request.input;
    let output = &request.output;
    let antab = &request.antab;
    let overwrite = request.overwrite;
    let append_tsys = request.append_tsys;
    let append_gain = request.append_gain;
    let mut summary = AppendantabSummary {
        output: None,
        gain_records: 0,
        tsys_rows: 0,
    };
    if output.as_os_str().is_empty() || output == input {
        return Ok(summary);
    }
    if output.symlink_metadata().is_ok() {
        // CASA reads the source metadata before declining an existing output.
        let source = MeasurementSet::open(input)
            .map_err(|error| AppendantabError::Message(message(error)))?;
        drop(source);
        return Ok(summary);
    }
    let input = crate::paths::normalized(input)
        .map_err(|error| AppendantabError::Message(message(error)))?;
    let output = crate::paths::normalized(output)
        .map_err(|error| AppendantabError::Message(message(error)))?;
    if output.starts_with(&input) {
        return Err(AppendantabError::Message(
            "The output MeasurementSet lies inside its input".into(),
        ));
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| AppendantabError::Message(message(error)))?;
    let staging = tempfile::Builder::new()
        .prefix(".appendantab-")
        .tempdir_in(parent)
        .map_err(|error| AppendantabError::Message(message(error)))?;
    let copied = staging.path().join("product.ms");

    let preparation = crate::timing::Phase::start("appendantab_read");
    #[cfg(unix)]
    let ms = MeasurementSet::open_retained_read(&input);
    #[cfg(not(unix))]
    let ms = MeasurementSet::open(&input);
    let ms = ms.map_err(|error| AppendantabError::Message(message(error)))?;
    let metadata = Metadata::new(&ms).map_err(AppendantabError::Message)?;
    let text =
        fs::read_to_string(antab).map_err(|error| AppendantabError::Message(message(error)))?;
    let text = Antab::read(&text).map_err(AppendantabError::Message)?;
    let temps = compute::temperatures(&text, &metadata).map_err(AppendantabError::Message)?;
    let gains = compute::gains(&text, &metadata).map_err(|error| match error {
        compute::GainError::Missing(key) => AppendantabError::At("antab_header", key.to_string()),
        compute::GainError::Invalid(message) => AppendantabError::Message(message),
    })?;
    drop(preparation);

    let copy = crate::timing::Phase::start("appendantab_copy_ms");
    let subtables = copy::stage(&ms, &input, &copied, append_gain || append_tsys)
        .map_err(AppendantabError::Message)?;
    drop(ms);
    drop(copy);

    let publication = crate::timing::Phase::start("appendantab_write");
    table::write(
        &copied,
        &subtables,
        &metadata,
        append_gain.then_some(gains.as_slice()),
        append_tsys.then_some(temps.as_slice()),
        overwrite,
    )
    .map_err(AppendantabError::Message)?;
    match rename_without_replacing(&copied, &output) {
        Ok(()) => {
            summary.output = Some(output);
            summary.gain_records = if append_gain { gains.len() } else { 0 };
            summary.tsys_rows = if append_tsys { temps.len() } else { 0 };
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(AppendantabError::Message(message(error))),
    }
    drop(publication);
    crate::timing::report_process_memory();
    Ok(summary)
}
