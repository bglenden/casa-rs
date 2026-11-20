// SPDX-License-Identifier: LGPL-3.0-or-later
//! Append a recorded flagging command to the MeasurementSet.

use std::path::Path;

use casa_tables::{TableError, TableOptions};
use casa_types::{ScalarValue, Value};

use super::FlaggingError;
use crate::{SubtableId, transform::open_staged_subtable_for_write};

/// Append one CASA-style command to FLAG_CMD under the subtable's write lock.
pub fn append_flag_command(
    ms: &Path,
    command: &str,
    reason: &str,
    applied: bool,
) -> Result<(), FlaggingError> {
    let path = ms.join(SubtableId::FlagCmd.name());
    let fail = |source: TableError| FlaggingError::MutateFlags {
        path: path.display().to_string(),
        reason: format!("append FLAG_CMD: {source}"),
    };
    let mut table = open_staged_subtable_for_write(&path).map_err(fail)?;
    let mut row = table
        .schema()
        .ok_or_else(|| fail(TableError::PreparedRowRequiresSchema))?
        .default_row();
    for (name, value) in [
        ("APPLIED", ScalarValue::Bool(applied)),
        ("COMMAND", ScalarValue::String(command.to_string())),
        ("REASON", ScalarValue::String(reason.to_string())),
        ("INTERVAL", ScalarValue::Float64(0.0)),
        ("TIME", ScalarValue::Float64(0.0)),
        ("LEVEL", ScalarValue::Int32(0)),
        ("SEVERITY", ScalarValue::Int32(0)),
        ("TYPE", ScalarValue::String(String::new())),
    ] {
        row.upsert(name, Value::Scalar(value));
    }
    table.add_row(row).map_err(fail)?;
    table
        .prepare_write()
        .save(TableOptions::new(&path))
        .map_err(fail)
}
