// SPDX-License-Identifier: LGPL-3.0-or-later
//! `Calibrater::fluxscale`, `write_history` and `writeResultsHistory` provenance.

use std::path::Path;

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};

use super::{FluxScaleError, FluxScaleRequest};

pub(super) fn record_failure(request: &FluxScaleRequest, caltype: &str) {
    let _phase = crate::timing::Phase::start("fluxscale_history");
    if let Err(error) = append_messages(request, vec![type_message(request, caltype)], 1) {
        tracing::warn!("updating fluxscale HISTORY: {error}");
    }
}

pub(super) fn record_tool_type(
    path: &Path,
    request: &FluxScaleRequest,
    caltype: &str,
) -> Result<(), FluxScaleError> {
    record_tool_message(path, type_message(request, caltype))
}

pub(super) fn record_tool_output(
    path: &Path,
    request: &FluxScaleRequest,
) -> Result<(), FluxScaleError> {
    record_tool_message(path, output_message(request))
}

fn record_tool_message(path: &Path, message: String) -> Result<(), FluxScaleError> {
    let _phase = crate::timing::Phase::start("fluxscale_history");
    append_to_path(path, vec![message], 1).map_err(|e| FluxScaleError::History(e.to_string()))
}

/// Append preformatted `fluxscale` HISTORY messages to the request's MeasurementSet.
pub fn append_messages(
    request: &FluxScaleRequest,
    messages: Vec<String>,
    calibrater_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(vis) = &request.options.measurement_set else {
        return Ok(());
    };
    append_to_path(&vis.join("HISTORY"), messages, calibrater_count)
}

fn append_to_path(
    path: &Path,
    messages: Vec<String>,
    calibrater_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    let mut table = Table::open_with_lock(
        TableOptions::new(path),
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait),
    )?;
    #[cfg(not(unix))]
    let mut table = Table::open(TableOptions::new(path))?;
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64()
        + 40_587. * 86_400.;
    for (index, message) in messages.into_iter().enumerate() {
        let calibrater = index < calibrater_count;
        let mut row = RecordValue::new(Vec::new());
        for (column, text) in [
            ("APPLICATION", if calibrater { "calibrater" } else { "ms" }),
            (
                "ORIGIN",
                if calibrater {
                    "calibrater::fluxscale()"
                } else {
                    "fluxscale"
                },
            ),
            ("PRIORITY", "INFO"),
            ("MESSAGE", message.as_str()),
        ] {
            row.upsert(column, Value::Scalar(ScalarValue::String(text.into())));
        }
        row.upsert("TIME", Value::Scalar(ScalarValue::Float64(time)));
        row.upsert("OBJECT_ID", Value::Scalar(ScalarValue::Int32(0)));
        row.upsert("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(-1)));
        for column in ["APP_PARAMS", "CLI_COMMAND"] {
            row.upsert(
                column,
                Value::Array(ArrayValue::from_string_vec(vec![String::new()])),
            );
        }
        table.add_row(row)?;
    }
    table.save(TableOptions::new(path))?;
    Ok(())
}

/// Return the tool header lines written to `HISTORY` for `fluxscale`.
pub fn tool_messages(request: &FluxScaleRequest) -> Vec<String> {
    let caltype = Table::open(TableOptions::new(&request.input_table))
        .ok()
        .map(|table| table.info().sub_type.clone())
        .unwrap_or_default();
    vec![type_message(request, &caltype), output_message(request)]
}

fn type_message(request: &FluxScaleRequest, caltype: &str) -> String {
    format!(
        "Table {} is of type: {caltype}",
        request.input_table.display()
    )
}

fn output_message(request: &FluxScaleRequest) -> String {
    let action = if request.append {
        "Appending result to"
    } else {
        "Storing result in"
    };
    format!("{action} {}", request.output_table.display())
}
