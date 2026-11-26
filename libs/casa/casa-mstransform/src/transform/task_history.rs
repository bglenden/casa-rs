// SPDX-License-Identifier: LGPL-3.0-or-later
//! Provenance rows for public MeasurementSet tasks.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableError, TableOptions};

use casa_ms::{SubtableId, subtable_path_from_main};

use super::{MsTransformError, open_staged_subtable_for_write};

/// Resolved task arguments recorded in a MeasurementSet's HISTORY.
#[derive(Debug, Clone)]
pub struct TransformTaskHistory {
    /// CASA task name recorded as the HISTORY origin.
    pub task: String,
    /// CASA parameter names and their printable values in signature order.
    pub parameters: Vec<(String, String)>,
}

/// Append the task heading, CASA-style argument messages and native version
/// to the target's HISTORY subtable under its normal write lock.
pub fn append_task_history(
    output: &Path,
    history: &TransformTaskHistory,
) -> Result<(), MsTransformError> {
    let heading = [
        format!("taskname={}", history.task),
        format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
    ];
    let messages = heading.into_iter().chain(
        history
            .parameters
            .iter()
            .map(|(name, value)| format!("{name:<11} = {value}")),
    );
    append_history_messages(output, &history.task, messages)
}

pub(crate) fn append_history_messages(
    output: &Path,
    origin: &str,
    messages: impl IntoIterator<Item = String>,
) -> Result<(), MsTransformError> {
    let main = Table::open(TableOptions::new(output)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: output.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let path = subtable_path_from_main(&main, output, SubtableId::History)
        .unwrap_or_else(|| output.join(SubtableId::History.name()));
    drop(main);
    let failure = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = open_staged_subtable_for_write(&path).map_err(failure)?;
    let mut rows = Vec::new();
    for message in messages {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| failure(TableError::Schema(error.to_string())))?
            .as_secs_f64()
            + 40_587.0 * 86_400.0;
        let text = |text: &str| Value::Scalar(ScalarValue::String(text.to_string()));
        let empty = || Value::Array(ArrayValue::from_string_vec(vec![String::new()]));
        let mut row = RecordValue::default();
        for (column, value) in [
            ("APP_PARAMS", empty()),
            ("CLI_COMMAND", empty()),
            ("APPLICATION", text("ms")),
            ("MESSAGE", text(&message)),
            ("OBJECT_ID", Value::Scalar(ScalarValue::Int32(0))),
            ("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(-1))),
            ("ORIGIN", text(origin)),
            ("PRIORITY", text("INFO")),
            ("TIME", Value::Scalar(ScalarValue::Float64(time))),
        ] {
            row.upsert(column, value);
        }
        rows.push(row);
    }
    table.append_records_in_place(&rows).map_err(failure)
}
