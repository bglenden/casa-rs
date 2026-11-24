// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the standard HISTORY records used by the MS tool.

use std::path::PathBuf;

use casa_tables::Table;
use casa_types::ScalarValue;

use crate::ms::linked_subtables::strict_subtable_path;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

/// One entry displayed by `ms.listhistory()`.
pub struct ToolHistoryEntry {
    /// Stored HISTORY.TIME in MJD seconds.
    pub time: f64,
    /// Stored log message priority, such as `INFO` or `WARN`.
    pub priority: String,
    /// Origin stored in HISTORY.ORIGIN.
    pub origin: String,
    /// The message text.
    pub message: String,
}

/// Read HISTORY in row order through the MAIN table's subtable links.
/// For a plain MAIN descriptor already known to be current, callers can
/// pass `main_is_plain` to avoid decoding the descriptor again.
///
/// # Errors
/// The MAIN descriptor, HISTORY table or any of its required cells is unreadable.
pub fn read_tool_history(main: &Table, main_is_plain: bool) -> MsResult<Vec<ToolHistoryEntry>> {
    let mut entries = Vec::new();
    for path in history_paths(main, main_is_plain)? {
        let mut columns = Table::read_scalar_columns_from_disk(
            &path,
            &["TIME", "PRIORITY", "ORIGIN", "MESSAGE"],
        )?;
        let mut column = |name: &str| {
            columns
                .remove(name)
                .ok_or_else(|| MsError::InvalidInput(format!("missing HISTORY.{name}")))
        };
        for (((time, priority), origin), message) in column("TIME")?
            .into_iter()
            .zip(column("PRIORITY")?)
            .zip(column("ORIGIN")?)
            .zip(column("MESSAGE")?)
        {
            let (
                Some(ScalarValue::Float64(time)),
                Some(ScalarValue::String(priority)),
                Some(ScalarValue::String(origin)),
                Some(ScalarValue::String(message)),
            ) = (time, priority, origin, message)
            else {
                return Err(MsError::InvalidInput("invalid HISTORY entry".into()));
            };
            entries.push(ToolHistoryEntry {
                time,
                priority,
                origin,
                message,
            });
        }
    }
    Ok(entries)
}

fn history_paths(main: &Table, main_is_plain: bool) -> MsResult<Vec<PathBuf>> {
    if main_is_plain && let Some(path) = main.path() {
        let id = SubtableId::History;
        return strict_subtable_path(main, path, id)?
            .map(|path| vec![path])
            .ok_or_else(|| MsError::MissingSubtable(id.name().into()));
    }
    MeasurementSet::linked_subtable_paths_strict(main, SubtableId::History)
}
