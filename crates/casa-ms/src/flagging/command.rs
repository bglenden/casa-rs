// SPDX-License-Identifier: LGPL-3.0-or-later
//! Append a recorded flagging command to the MeasurementSet.

use std::path::Path;

use casa_core::{RecordValue, ScalarValue, Value};
use casa_tables::TableError;

use super::FlaggingError;
use crate::{SubtableId, transform::open_staged_subtable_for_write};

/// Append one CASA-style command to FLAG_CMD under the subtable's write lock.
pub fn append_flag_command(
    ms: &Path,
    command: &str,
    reason: &str,
    applied: bool,
) -> Result<(), FlaggingError> {
    append_flag_commands(ms, &[(command.to_owned(), reason.to_owned())], applied)
}

/// Append a batch of CASA-style `(command, reason)` entries to FLAG_CMD under
/// one subtable write lock.
pub fn append_flag_commands(
    ms: &Path,
    entries: &[(String, String)],
    applied: bool,
) -> Result<(), FlaggingError> {
    let path = ms.join(SubtableId::FlagCmd.name());
    let fail = |source: TableError| FlaggingError::MutateFlags {
        path: path.display().to_string(),
        reason: format!("append FLAG_CMD: {source}"),
    };
    let mut table = open_staged_subtable_for_write(&path).map_err(fail)?;
    if entries.is_empty() {
        return Ok(());
    }
    let rows = entries
        .iter()
        .map(|(command, reason)| {
            let mut row = RecordValue::default();
            for (name, value) in [
                ("APPLIED", ScalarValue::Bool(applied)),
                ("COMMAND", ScalarValue::String(command.clone())),
                ("REASON", ScalarValue::String(reason.clone())),
                ("INTERVAL", ScalarValue::Float64(0.0)),
                ("TIME", ScalarValue::Float64(0.0)),
                ("LEVEL", ScalarValue::Int32(0)),
                ("SEVERITY", ScalarValue::Int32(0)),
                ("TYPE", ScalarValue::String(String::new())),
            ] {
                row.upsert(name, Value::Scalar(value));
            }
            row
        })
        .collect::<Vec<_>>();
    table.append_records_in_place(&rows).map_err(fail)
}
