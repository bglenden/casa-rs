// SPDX-License-Identifier: LGPL-3.0-or-later
//! Attach a casacore ephemeris to a MeasurementSet's FIELD table.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use casa_measures::CometTable;
use casa_tables::{ColumnAddition, ColumnSchema, Table, TableOptions, TableWriteLock, publication};

use crate::tool_history::append_tool_history;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

const ID_COLUMN: &str = "EPHEMERIS_ID";
const SECONDS_PER_DAY: f64 = 86_400.0;

/// An `ms.addephemeris()` request with its CASA field expression.
pub struct ToolEphemeris<'a> {
    /// The requested ID, or a negative value for the next ID.
    pub id: i64,
    /// An existing casacore comet table.
    pub source: &'a Path,
    /// User spelling of the input path recorded in HISTORY.
    pub source_name: &'a str,
    /// Suffix of the ephemeris table name stored inside FIELD.
    pub comment: &'a str,
    /// The FIELD expression. Empty text installs an unassigned ephemeris.
    pub field: &'a str,
    /// Whether the attachment permits FIELD column updates.
    pub writable: bool,
}

/// Install the ephemeris and update its selected FIELD rows and HISTORY.
///
/// Stage the physical FIELD directory before replacing it, so invalid
/// inputs and copy failures leave its original tables readable.
///
/// # Errors
/// The selection or source is invalid, a required table cannot be written,
/// or the filesystem cannot publish the staged FIELD.
pub fn add_tool_ephemeris(
    main: &Table,
    request: &ToolEphemeris<'_>,
    held: &mut BTreeMap<PathBuf, TableWriteLock>,
    read_locks: &mut BTreeMap<PathBuf, Table>,
) -> MsResult<bool> {
    let path = main
        .path()
        .ok_or_else(|| invalid("an ephemeris requires a persisted MS"))?;
    let field = subtable_path(main, SubtableId::Field)?;
    let field = field.canonicalize().map_err(io)?;
    if request.source.as_os_str().is_empty() {
        return Err(invalid("addephemeris requires an ephemeris table name"));
    }
    let source = match request.source.canonicalize() {
        Ok(path) if path.join("table.dat").is_file() => path,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io(error)),
    };
    #[cfg(unix)]
    let _source_lock = retain_reader(&source)?;
    let span = CometTable::open(&source)
        .map_err(|error| MsError::MeasuresRuntime(error.to_string()))?
        .span_mjd()
        .map(|day| day * SECONDS_PER_DAY);
    let guards = publication::lock_table_tree_for_replacement_with_held(&field, held)?;
    let ms = MeasurementSet::open(path)?;
    if subtable_path(ms.main_table(), SubtableId::Field)?
        .canonicalize()
        .map_err(io)?
        != field
    {
        return Err(invalid("the MeasurementSet FIELD link changed"));
    }
    let selected: BTreeSet<usize> = ms
        .resolve_field_selector(request.field)?
        .into_iter()
        .map(|id| id as usize)
        .collect();
    let table = Table::open(TableOptions::new(&field))?;
    let has_id = table
        .schema()
        .is_some_and(|schema| schema.contains_column(ID_COLUMN));
    if !request.writable {
        if !has_id {
            return Ok(false);
        }
        if !selected.is_empty() {
            return Err(invalid("the FIELD table is not writable"));
        }
    }
    let id = ephemeris_id(&table, &selected, has_id, request.id)?;
    if request.comment.contains(['/', '\0']) {
        return Err(invalid(
            "the ephemeris comment must be part of a table name",
        ));
    }
    let observed = observed_span(main)?;
    let temporary = tempfile::Builder::new()
        .prefix(".ephemeris-")
        .tempdir_in(
            field
                .parent()
                .ok_or_else(|| invalid("FIELD has no containing directory"))?,
        )
        .map_err(io)?;
    let staged = temporary.path().join("FIELD");
    publication::copy_locked_table_tree(&field, &staged).map_err(io)?;
    install_table(&staged, &source, id, request.comment)?;
    assign_fields(&staged, &selected, id, has_id, span[0], observed)?;
    let mut staged_locks = publication::lock_table_tree_for_replacement(&staged)?;
    let field_lock = staged_locks
        .iter()
        .position(|lock| lock.path() == staged)
        .ok_or_else(|| invalid("the staged FIELD has no write lock"))?;
    let mut replacement = staged_locks.swap_remove(field_lock);
    if !original_field_matches(held, &guards, &field, &field) {
        return Err(invalid("FIELD changed before attaching the ephemeris"));
    }
    publication::exchange_directories(&staged, &field).map_err(io)?;
    let result = (|| -> MsResult<Option<Table>> {
        replacement.follow_directory_rename(&field)?;
        // A second read handle on the same inode can be acquired while this
        // process holds its write lock. Releasing the writer then downgrades
        // the kernel lock to that read lease without a gap.
        let reader = if read_locks.contains_key(&field) {
            retain_reader(&field)?
        } else {
            None
        };
        let message = String::from("Added ephemeris to FIELD table.");
        let command = format!(
            "field={} id={} name='{}' coment='{}'",
            request.field, request.id, request.source_name, request.comment
        );
        append_tool_history(
            main,
            false,
            &[message],
            &command,
            "ms::addephemeris()",
            "ms",
            held,
        )?;
        Ok(reader)
    })();
    match result {
        Ok(reader) => {
            if let std::collections::btree_map::Entry::Occupied(mut entry) =
                held.entry(field.clone())
            {
                entry.insert(replacement);
            }
            if let Some(reader) = reader {
                read_locks.insert(field, reader);
            }
            Ok(true)
        }
        Err(error) => {
            let can_restore = replacement.matches_table_path(&field)
                && original_field_matches(held, &guards, &field, &staged);
            if can_restore && publication::exchange_directories(&staged, &field).is_ok() {
                return Err(error);
            }
            let preserved = temporary.keep();
            Err(MsError::InvalidInput(format!(
                "{error}; could not restore FIELD; old FIELD preserved at {}",
                preserved.display(),
            )))
        }
    }
}

fn original_field_matches(
    held: &BTreeMap<PathBuf, TableWriteLock>,
    guards: &[TableWriteLock],
    field: &Path,
    location: &Path,
) -> bool {
    held.get(field)
        .or_else(|| guards.iter().find(|lock| lock.path() == field))
        .is_some_and(|lock| lock.matches_table_path(location))
}

fn retain_reader(path: &Path) -> MsResult<Option<Table>> {
    #[cfg(unix)]
    {
        Ok(Some(Table::open_with_lock_attempts(
            TableOptions::new(path),
            casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(true),
            1,
        )?))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

fn subtable_path(main: &Table, id: SubtableId) -> MsResult<PathBuf> {
    MeasurementSet::linked_subtable_paths(main, id)?
        .into_iter()
        .next()
        .ok_or_else(|| MsError::MissingSubtable(id.name().into()))
}

fn ephemeris_id(
    table: &Table,
    selected: &BTreeSet<usize>,
    present: bool,
    request: i64,
) -> MsResult<i32> {
    if request >= 0 {
        return i32::try_from(request)
            .map_err(|_| invalid("ephemeris ID is outside the FIELD Int32 range"));
    }
    let mut highest = -1;
    if present {
        for row in (0..table.row_count()).filter(|row| !selected.contains(row)) {
            let cell = table.cell_accessor(row, ID_COLUMN)?;
            if let ScalarValue::Int32(id) = cell.scalar()? {
                highest = highest.max(*id);
            } else {
                return Err(invalid("FIELD EPHEMERIS_ID is not Int32"));
            }
        }
    }
    highest
        .checked_add(1)
        .ok_or_else(|| invalid("no free Int32 ephemeris ID"))
}

fn install_table(field: &Path, source: &Path, id: i32, comment: &str) -> MsResult<()> {
    let prefix = format!("EPHEM{id}_");
    for entry in fs::read_dir(field).map_err(io)? {
        let entry = entry.map_err(io)?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".tab"))
        {
            if entry.file_type().map_err(io)?.is_symlink() {
                fs::remove_file(entry.path()).map_err(io)?;
            } else {
                fs::remove_dir_all(entry.path()).map_err(io)?;
            }
        }
    }
    let destination = field.join(format!("{prefix}{comment}.tab"));
    if matches!(
        Table::immediate_part_names(source, source)?,
        casa_tables::TablePartNames::Plain(_)
    ) {
        publication::copy_locked_table_tree(source, &destination).map_err(io)?;
        for entry in fs::read_dir(&destination).map_err(io)? {
            let entry = entry.map_err(io)?;
            if entry.file_type().map_err(io)?.is_symlink() {
                let referent = source.join(entry.file_name()).canonicalize().map_err(io)?;
                if !referent.is_file() {
                    return Err(invalid("ephemeris table links must address regular files"));
                }
                fs::remove_file(entry.path()).map_err(io)?;
                fs::copy(referent, entry.path()).map_err(io)?;
            }
        }
    } else {
        Table::open(TableOptions::new(source))?.deep_copy(TableOptions::new(&destination))?;
    }
    CometTable::open(&destination).map_err(|error| MsError::MeasuresRuntime(error.to_string()))?;
    Ok(())
}

fn observed_span(main: &Table) -> MsResult<[f64; 2]> {
    let observation = subtable_path(main, SubtableId::Observation)?;
    let table = Table::open(TableOptions::new(observation))?;
    let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
    for row in 0..table.row_count() {
        let cell = table.cell_accessor(row, "TIME_RANGE")?;
        let ArrayValue::Float64(values) = cell.array()? else {
            return Err(invalid("OBSERVATION TIME_RANGE is not Float64"));
        };
        for &value in values {
            bounds[0] = bounds[0].min(value);
            bounds[1] = bounds[1].max(value);
        }
    }
    Ok(bounds)
}

fn assign_fields(
    field: &Path,
    selected: &BTreeSet<usize>,
    id: i32,
    present: bool,
    first_ephemeris_time: f64,
    observed: [f64; 2],
) -> MsResult<()> {
    if present && selected.is_empty() {
        return Ok(());
    }
    let mut lock = TableWriteLock::acquire_to_reread(field, 1)?;
    let mut table = Table::open(TableOptions::new(field))?;
    lock.record_write();
    if !present {
        table.add_columns_in_place(
            vec![ColumnAddition {
                schema: ColumnSchema::scalar(ID_COLUMN, PrimitiveType::Int32)
                    .with_comment("Ephemeris id, pointer to EPHEMERIS table"),
                default: Some(Value::Scalar(ScalarValue::Int32(-1))),
                keywords: RecordValue::default(),
                manager_group: None,
                manager_type: None,
            }],
            None,
        )?;
    }
    let observing = observed[0]..=observed[1];
    for &row in selected {
        let when = table.cell_accessor(row, "TIME")?;
        let ScalarValue::Float64(when) = when.scalar()? else {
            return Err(invalid("FIELD TIME is not Float64"));
        };
        if !observing.contains(when) && observing.contains(&first_ephemeris_time) {
            table
                .cell_accessor_mut(row, "TIME")?
                .set(Value::Scalar(ScalarValue::Float64(first_ephemeris_time)))?;
        }
        table
            .cell_accessor_mut(row, ID_COLUMN)?
            .set(Value::Scalar(ScalarValue::Int32(id)))?;
    }
    table.flush()?;
    lock.release()?;
    Ok(())
}

fn invalid(message: &str) -> MsError {
    MsError::InvalidInput(message.to_owned())
}

fn io(error: std::io::Error) -> MsError {
    MsError::InvalidInput(format!("FIELD ephemeris: {error}"))
}
