// SPDX-License-Identifier: LGPL-3.0-or-later
//! Append external ANTAB calibration to a staged copy of a MeasurementSet.

mod compute;
mod copy;
mod parse;
mod table;

use std::fs;
use std::path::{Path, PathBuf};

use casa_contracts::CasaValue;
use casa_ms::MeasurementSet;
use casa_tables::publication::rename_without_replacing;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

use self::compute::{Metadata, message};
use self::parse::Antab;

#[derive(Serialize)]
struct Summary {
    output: Option<PathBuf>,
    tsys_rows: usize,
    gain_records: usize,
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let input = task.input_path("vis")?;
    let output = task.path("outvis")?;
    let antab = task.input_path("antab")?;
    let overwrite = task.flag("overwrite")?;
    let append_tsys = task.flag("append_tsys")?;
    let append_gain = task.flag("append_gc")?;
    let mut summary = Summary {
        output: None,
        gain_records: 0,
        tsys_rows: 0,
    };
    if output.as_os_str().is_empty() || output == input {
        return TaskSuccess::new(CasaValue::None, &summary);
    }
    if output.symlink_metadata().is_ok() {
        // CASA reads the source metadata before declining an existing output.
        let source =
            MeasurementSet::open(&input).map_err(|error| TaskFailure::execution(message(error)))?;
        drop(source);
        return TaskSuccess::new(CasaValue::None, &summary);
    }
    let input =
        crate::paths::normalized(&input).map_err(|error| TaskFailure::execution(message(error)))?;
    let output = crate::paths::normalized(&output)
        .map_err(|error| TaskFailure::execution(message(error)))?;
    if output.starts_with(&input) {
        return Err(TaskFailure::execution(
            "The output MeasurementSet lies inside its input",
        ));
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| TaskFailure::execution(message(error)))?;
    let staging = tempfile::Builder::new()
        .prefix(".appendantab-")
        .tempdir_in(parent)
        .map_err(|error| TaskFailure::execution(message(error)))?;
    let copied = staging.path().join("product.ms");

    let preparation = crate::timing::Phase::start("appendantab_read");
    #[cfg(unix)]
    let ms = MeasurementSet::open_retained_read(&input);
    #[cfg(not(unix))]
    let ms = MeasurementSet::open(&input);
    let ms = ms.map_err(|error| TaskFailure::execution(message(error)))?;
    let metadata = Metadata::new(&ms).map_err(TaskFailure::execution)?;
    let text =
        fs::read_to_string(&antab).map_err(|error| TaskFailure::execution(message(error)))?;
    let text = Antab::read(&text).map_err(TaskFailure::execution)?;
    let temps = compute::temperatures(&text, &metadata).map_err(TaskFailure::execution)?;
    let gains = compute::gains(&text, &metadata).map_err(|error| match error {
        compute::GainError::Missing(key) => TaskFailure::execution(key).at("antab_header"),
        compute::GainError::Invalid(message) => TaskFailure::execution(message),
    })?;
    drop(preparation);

    let copy = crate::timing::Phase::start("appendantab_copy_ms");
    let subtables = copy::stage(&ms, &input, &copied, append_gain || append_tsys)
        .map_err(TaskFailure::execution)?;
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
    .map_err(TaskFailure::execution)?;
    match rename_without_replacing(&copied, &output) {
        Ok(()) => {
            summary.output = Some(output);
            summary.gain_records = if append_gain { gains.len() } else { 0 };
            summary.tsys_rows = if append_tsys { temps.len() } else { 0 };
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(TaskFailure::execution(message(error))),
    }
    drop(publication);
    crate::timing::report_process_memory();
    TaskSuccess::new(CasaValue::None, &summary)
}
