// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and append the standard HISTORY records used by the MS tool.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_tables::{Table, TableOptions, TablePartNames, TableWriteLock};
use casa_types::measures::MeasFrame;
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::column_def::build_table_schema;
use crate::ms::{apply_column_metadata, measurement_set_table_options, subtable_path_from_main};
use crate::{MeasurementSet, MsError, MsResult, SubtableId, open_measures_runtime, schema};

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

/// Append MS tool messages in one write to the linked HISTORY table.
/// An empty message produces a record only when a CLI command was supplied.
///
/// If HISTORY is absent at a plain MS, create and link the standard table
/// together with the first messages.
/// Supply any write locks already held by this attachment, by canonical path.
///
/// # Errors
/// MAIN or HISTORY is missing, unreadable, read-only, has invalid metadata,
/// or cannot be locked for the requested update.
pub fn append_tool_history(
    main: &Table,
    main_is_plain: bool,
    messages: &[String],
    cli_command: &str,
    origin: &str,
    application: &str,
    held_locks: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    let path = main.path().ok_or_else(|| {
        MsError::InvalidInput("HISTORY requires a persisted MeasurementSet".into())
    })?;
    OpenOptions::new()
        .write(true)
        .open(path.join("table.dat"))
        .map_err(|error| {
            MsError::InvalidInput(format!("cannot update {}: {error}", path.display()))
        })?;
    let history = match history_path(main, main_is_plain)? {
        Some(history) => history,
        None => with_history_write_lock(path, held_locks, create_history)?,
    };
    with_history_write_lock(&history, held_locks, |table, lock| {
        let rows = history_rows(&table, messages, cli_command, origin, application)?;
        if !rows.is_empty() {
            lock.record_write();
        }
        table.append_records_and_close(&rows)?;
        Ok(())
    })
}

fn with_history_write_lock<T>(
    path: &Path,
    held_locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    write: impl FnOnce(Table, &mut TableWriteLock) -> MsResult<T>,
) -> MsResult<T> {
    if !held_locks.is_empty() {
        let canonical = path
            .canonicalize()
            .map_err(|error| MsError::InvalidInput(format!("{}: {error}", path.display())))?;
        if let Some(lock) = held_locks.get_mut(&canonical) {
            let table = Table::open(TableOptions::new(&canonical))?;
            return write(table, lock);
        }
    }
    let mut lock = TableWriteLock::acquire_to_reread(path, 0)?;
    let table = Table::open(TableOptions::new(path))?;
    let result = write(table, &mut lock);
    let released = lock.release();
    let value = result?;
    released?;
    Ok(value)
}

fn history_paths(main: &Table, main_is_plain: bool) -> MsResult<Vec<PathBuf>> {
    if main_is_plain && let Some(path) = main.path() {
        let id = SubtableId::History;
        return subtable_path_from_main(main, path, id)
            .map(|path| vec![path])
            .ok_or_else(|| MsError::MissingSubtable(id.name().into()));
    }
    MeasurementSet::linked_subtable_paths(main, SubtableId::History)
}

fn history_path(main: &Table, main_is_plain: bool) -> MsResult<Option<PathBuf>> {
    let Some(path) = main.path() else {
        return Ok(None);
    };
    if main.keywords().get(SubtableId::History.name()).is_none()
        && (main_is_plain
            || matches!(
                Table::immediate_part_names(path, path)?,
                TablePartNames::Plain(_)
            ))
    {
        return Ok(None);
    }
    match history_paths(main, main_is_plain) {
        Ok(mut parts) if parts.len() == 1 => Ok(parts.pop()),
        Ok(_) => Err(MsError::InvalidInput(
            "rows cannot be appended to a concatenated HISTORY table".into(),
        )),
        Err(MsError::MissingSubtable(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn create_history(mut main: Table, lock: &mut TableWriteLock) -> MsResult<PathBuf> {
    let path = main
        .path()
        .ok_or_else(|| MsError::InvalidInput("HISTORY requires a persisted MeasurementSet".into()))?
        .to_path_buf();
    if !matches!(
        Table::immediate_part_names(&path, &path)?,
        TablePartNames::Plain(_)
    ) {
        return Err(MsError::InvalidInput(
            "HISTORY is missing from the source of this MeasurementSet view".into(),
        ));
    }
    if let Some(history) = history_path(&main, true)? {
        // Another writer may have created HISTORY while MAIN was being locked.
        return Ok(history);
    }
    let name = SubtableId::History.name();
    let history_path = path.join(name);
    let columns = schema::required_columns_in_casacore_order(SubtableId::History);
    let schema = build_table_schema(&columns)?;
    match fs::create_dir(&history_path) {
        Ok(()) => {
            let mut history = Table::with_schema(schema);
            apply_column_metadata(&mut history, &columns);
            history.save(measurement_set_table_options(&history_path))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = Table::open(TableOptions::new(&history_path))?;
            let matches_schema = existing.schema().is_some_and(|actual| {
                schema.columns().iter().all(|required| {
                    actual.column(required.name()).is_some_and(|column| {
                        column.column_type() == required.column_type()
                            && column.data_type() == required.data_type()
                    })
                })
            });
            if !matches_schema || TableMeasDesc::reconstruct(&existing, "TIME").is_none() {
                return Err(MsError::InvalidInput(format!(
                    "cannot link incompatible table {} as HISTORY",
                    history_path.display()
                )));
            }
        }
        Err(error) => {
            return Err(MsError::InvalidInput(format!(
                "cannot create {}: {error}",
                history_path.display()
            )));
        }
    }
    // Publish the link before any messages are appended, so a failed link
    // update can be retried without duplicating messages.
    lock.record_write();
    main.keywords_mut()
        .upsert(name, Value::table_ref(format!("././{name}")));
    main.save_keywords_atomically(&path)?;
    Ok(history_path)
}

fn history_rows(
    table: &Table,
    messages: &[String],
    cli_command: &str,
    origin: &str,
    application: &str,
) -> MsResult<Vec<RecordValue>> {
    let mut messages = messages
        .iter()
        .filter(|message| !message.is_empty() || !cli_command.is_empty())
        .peekable();
    if messages.peek().is_none() {
        return Ok(Vec::new());
    }
    let reference = TableMeasDesc::reconstruct(table, "TIME")
        .and_then(|measure| match measure.ref_desc() {
            MeasRefDesc::Fixed { refer } => Some(refer.clone()),
            _ => None,
        })
        .ok_or_else(|| MsError::InvalidInput("HISTORY.TIME has no fixed epoch reference".into()))?
        .parse::<EpochRef>()?;
    let mut frame = MeasFrame::new();
    if reference != EpochRef::UTC {
        frame = frame.with_measures(open_measures_runtime()?);
    }
    let origin = if origin.is_empty() {
        "MSHistoryHandler::addMessage()"
    } else {
        origin
    };
    messages
        .map(|message| {
            let unix_seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| MsError::InvalidInput(error.to_string()))?
                .as_secs_f64();
            let epoch = MEpoch::from_mjd_seconds(unix_seconds + 40_587.0 * 86_400.0, EpochRef::UTC)
                .convert_to(reference, &frame)?;
            let text = |value: &str| Value::Scalar(ScalarValue::String(value.to_owned()));
            let vector =
                |value: &str| Value::Array(ArrayValue::from_string_vec(vec![value.into()]));
            Ok(RecordValue::new(vec![
                RecordField::new("APPLICATION", text(application)),
                RecordField::new("APP_PARAMS", vector("")),
                RecordField::new("CLI_COMMAND", vector(cli_command)),
                RecordField::new("MESSAGE", text(message)),
                RecordField::new("OBJECT_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("ORIGIN", text(origin)),
                RecordField::new("PRIORITY", text("INFO")),
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(epoch.value().as_mjd() * 86_400.0)),
                ),
            ]))
        })
        .collect()
}
