// SPDX-License-Identifier: LGPL-3.0-or-later
//! Attach a casacore ephemeris to a MeasurementSet's FIELD table.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_measures_data::CometTable;
use casa_tables::{ColumnAddition, ColumnSchema, Table, TableOptions, TableWriteLock, publication};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};

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
) -> MsResult<bool> {
    let path = main
        .path()
        .ok_or_else(|| invalid("an ephemeris requires a persisted MS"))?;
    let ms = MeasurementSet::open(path)?;
    let selected: BTreeSet<usize> = ms
        .resolve_field_selector(request.field)?
        .into_iter()
        .map(|id| id as usize)
        .collect();
    let field = subtable_path(main, SubtableId::Field)?;
    let field = field.canonicalize().map_err(io)?;
    let guards = publication::lock_table_tree_for_replacement_with_held(&field, held)?;
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
    let source = request.source.canonicalize().map_err(io)?;
    #[cfg(unix)]
    let _source_lock = if held.contains_key(&source)
        || guards.iter().any(|lock| lock.path() == source)
    {
        None
    } else {
        Some(Table::open_with_lock_attempts(
            TableOptions::new(&source),
            casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(true),
            1,
        )?)
    };
    let span = CometTable::open(&source)
        .map_err(|error| MsError::MeasuresRuntime(error.to_string()))?
        .span_mjd()
        .map(|day| day * SECONDS_PER_DAY);
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

    let matches = held
        .get(&field)
        .or_else(|| guards.iter().find(|lock| lock.path() == field))
        .is_some_and(|lock| lock.matches_table_path(&field));
    if !matches {
        return Err(invalid("FIELD changed before attaching the ephemeris"));
    }
    publication::exchange_directories(&staged, &field).map_err(io)?;
    let message = String::from("Added ephemeris to FIELD table.");
    let command = format!(
        "field={} id={} name='{}' coment='{}'",
        request.field, request.id, request.source_name, request.comment
    );
    if let Err(error) = append_tool_history(
        main,
        false,
        &[message],
        &command,
        "ms::addephemeris()",
        "ms",
        held,
    ) {
        if let Err(rollback) = publication::exchange_directories(&staged, &field) {
            let preserved = temporary.keep();
            return Err(MsError::InvalidInput(format!(
                "{error}; FIELD rollback failed: {rollback}; old FIELD preserved at {}",
                preserved.display()
            )));
        }
        return Err(error);
    }
    if let Some(old) = held.remove(&field) {
        old.release()?;
        held.insert(field.clone(), TableWriteLock::acquire_to_reread(&field, 1)?);
    }
    Ok(true)
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
    publication::copy_locked_table_tree(source, &field.join(format!("{prefix}{comment}.tab")))
        .map_err(io)
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
