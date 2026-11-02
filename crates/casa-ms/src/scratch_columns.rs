// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA clears from a MeasurementSet before it writes a scratch
//! column, so that the column casa-rs writes is the one CASA reads (CASA
//! 6.7.5).
//!
//! - **Cached sort keywords.** CASA's iterators may cache a pre-sorted
//!   reference table of MAIN and record it in MAIN's keywords as
//!   `SORTED_TABLE`, with the columns it is sorted by as `SORT_COLUMNS`
//!   (`MSIter2`). A sorted table made before a column was added does not
//!   see it, so adding `MODEL_DATA` or `CORRECTED_DATA` removes both
//!   keywords (`VisSetUtil::addScrCols`; the table itself is left, as CASA
//!   leaves it). The MS definition's own `SORTED_TABLES` is untouched.
//! - **The virtual model.** CASA can hold a field's model as a recipe
//!   instead of a column: `setjy` (`usescratch=False`) and `tclean`
//!   (`savemodel='virtual'`) write one, and CASA's iterator prefers it to
//!   `MODEL_DATA` (`VisibilityIteratorImpl2::visibilityModel`). A task that
//!   writes `MODEL_DATA` makes the column "the exclusive origin for model
//!   visibilities", whether or not it adds it: it clears every field's
//!   virtual model first. `addScrCols` does so when it adds the column
//!   (`remOTFModel`, `VisModelData::clearModel` of
//!   `synthesis/TransformMachines/VisModelData.cc`), and the imagers do so
//!   whenever they write it (`SynthesisImagerVi2::selectData`,
//!   `Imager::setjy` and `ft`, `refim::VisModelData::clearModel` of
//!   `TransformMachines2`). The two `clearModel` bodies are the same.
//!
//! A field's virtual model is a keyword `definedmodel_field_<id>` naming a
//! model key, in SOURCE's keywords when SOURCE has rows and MAIN's
//! otherwise. Under SOURCE the model key is a keyword holding the SOURCE
//! row whose `SOURCE_MODEL` cell holds the model record; under MAIN it is a
//! keyword holding the record itself. Clearing a field removes both
//! keywords wherever they are and empties the `SOURCE_MODEL` cell, which
//! is what CASA leaves (measured with `setjy` then `delmod` on CASA 6.7.5).
//! A pointer under SOURCE naming a key held in MAIN is removed with the
//! key, where CASA keeps the pointer; either way the field has no model.
//!
//! `clearModel` also deletes the model images a gridder's record names
//! (`ft_<k>.container.diskimage`), wherever they lie, and every `FT_MODEL*`
//! table in the MeasurementSet and its SOURCE, where `tclean` writes them.
//! casa-rs deletes only the `FT_MODEL*` tables, so that a path in a record
//! never deletes anything else. A multi-MS is cleared as one table, not
//! part by part as CASA clears it.

use std::collections::BTreeSet;
use std::path::Path;

use casa_tables::{Table, TableError, TableOptions, TableWriteLock};
use casa_types::{RecordValue, ScalarValue, Value};

use crate::ms::{MeasurementSet, measurement_set_table_options};
use crate::schema::SubtableId;
use crate::schema::main_table::VisibilityDataColumn;
use crate::{MsError, MsResult};

/// MAIN's keywords for a cached pre-sorted reference table (`MSIter2`).
const SORT_KEYWORDS: [&str; 2] = ["SORT_COLUMNS", "SORTED_TABLE"];

/// Clear what CASA clears before it writes scratch column `column`
/// (`MODEL_DATA` or `CORRECTED_DATA`) to `ms`, adding it when `adds`:
/// for an added column the cached sort keywords, and for `MODEL_DATA`,
/// added or not, every field's virtual model, as the module states. Every
/// path that adds either column to an existing MeasurementSet, or writes
/// `MODEL_DATA`, calls it before it changes the column.
///
/// What is cleared is found first, and a table with nothing to clear is
/// not changed. A MeasurementSet on disk then has SOURCE and MAIN changed
/// and written in turn, each under casacore's write lock on it: SOURCE in
/// full as [`MeasurementSet::save`] writes it, MAIN's metadata alone. A
/// table opened with locking is written under its own lock, held already
/// or taken and released here; one opened without, under a
/// [`TableWriteLock`] taken and released here, so a caller holds MAIN's
/// write lock, if at all, through its handle, never through such a guard.
/// A lock is waited for as casacore waits, except SOURCE's while the
/// caller holds MAIN's write lock: one attempt is then made, as a save
/// never waits for one table while it holds another, where CASA waits
/// (casa-rs holders do not yet release a lock on request, #694). The
/// `FT_MODEL*` model images are deleted last and, as CASA deletes them, at
/// best: one that is held open or cannot be deleted is logged and left.
///
/// # Errors
///
/// [`MsError::InvalidInput`] for `DATA`, which is no scratch column. A
/// [`TableError::LockFailed`] when SOURCE's lock is held elsewhere while
/// the caller holds MAIN's write lock, and any error reading the fields
/// or locking and writing SOURCE or MAIN. A table whose write fails is
/// reloaded as it is on disk, holding whatever of the write reached it,
/// as after an interrupted casacore write; when SOURCE's fails, MAIN is
/// not changed.
pub fn prepare_write(
    ms: &mut MeasurementSet,
    column: VisibilityDataColumn,
    adds: bool,
) -> MsResult<()> {
    prepare(ms, column, adds, None)
}

/// [`prepare_write`] for a caller that holds MAIN's write lock through
/// `main_lock`, a [`TableWriteLock`] on the MAIN of `ms` (opened without
/// locking), as a writer of several mutation sessions in turn holds it:
/// MAIN's metadata is written under it, the write recorded so that the
/// caller's release publishes it, and SOURCE's lock is tried once, as while
/// a caller holds MAIN's through its handle.
///
/// # Errors
///
/// As [`prepare_write`]; a MAIN whose write fails is reloaded as it is on
/// disk, and the caller's lock publishes what reached it.
pub fn prepare_write_holding(
    ms: &mut MeasurementSet,
    column: VisibilityDataColumn,
    adds: bool,
    main_lock: &mut TableWriteLock,
) -> MsResult<()> {
    prepare(ms, column, adds, Some(main_lock))
}

/// Storage managers whose selected-row save patches the written rows' tiles
/// in place: the managers CASA and casa-rs store visibilities in.
pub(crate) const BOUNDED_WRITE_MANAGERS: [&str; 2] = ["TiledShapeStMan", "TiledColumnStMan"];

/// Why MAIN's `column` cannot be written in place in bounded memory, or
/// `None` when it can: a column stored outside [`BOUNDED_WRITE_MANAGERS`]
/// is saved by rewriting every row of its storage group, once a save. A
/// column the writer creates (`created`) clones `DATA`'s manager, which must
/// be one of them and hold `DATA` alone.
pub(crate) fn unbounded_destination(main: &Table, column: &str, created: bool) -> Option<String> {
    let stored = if created { "DATA" } else { column };
    let Some(manager) = main
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns.iter().any(|name| name == stored))
    else {
        return Some(format!("{stored} has no storage manager on disk"));
    };
    let storage = if created {
        format!(
            "it would be created in a clone of DATA's {}",
            manager.dm_type
        )
    } else {
        format!("it is stored in {}", manager.dm_type)
    };
    if !BOUNDED_WRITE_MANAGERS.contains(&manager.dm_type.as_str()) {
        return Some(format!(
            "{storage}; only TiledShapeStMan and TiledColumnStMan columns are written in place"
        ));
    }
    (created && manager.columns.len() != 1)
        .then(|| format!("{storage}, which DATA shares with other columns"))
}

fn prepare(
    ms: &mut MeasurementSet,
    column: VisibilityDataColumn,
    adds: bool,
    main_lock: Option<&mut TableWriteLock>,
) -> MsResult<()> {
    if column == VisibilityDataColumn::Data {
        return Err(MsError::InvalidInput(
            "DATA is not a scratch column".to_string(),
        ));
    }
    let mut clearing = Clearing::default();
    if column == VisibilityDataColumn::ModelData {
        clearing.plan_virtual_model(ms)?;
    }
    if adds {
        let keywords = ms.main_table().keywords();
        clearing.main.extend(
            SORT_KEYWORDS
                .iter()
                .filter(|name| keywords.get(name).is_some())
                .map(|name| name.to_string()),
        );
    }
    let holds_main = main_lock.is_some() || holds_write_lock(ms.main_table());
    if let Some(source) = ms.subtable_mut(SubtableId::Source)
        && !(clearing.source.is_empty() && clearing.source_cells.is_empty())
    {
        let attempts = if holds_main { 1 } else { 0 };
        change_in_place(source, attempts, Write::Full, None, |source| {
            for row in &clearing.source_cells {
                source
                    .cell_accessor_mut(*row, "SOURCE_MODEL")
                    .and_then(|mut cell| cell.set(Value::Record(RecordValue::default())))?;
            }
            remove(source, &clearing.source);
            Ok(())
        })?;
    }
    if !clearing.main.is_empty() {
        change_in_place(ms.main_table_mut(), 0, Write::Metadata, main_lock, |main| {
            remove(main, &clearing.main);
            Ok(())
        })?;
    }
    if column == VisibilityDataColumn::ModelData {
        delete_model_images(ms);
    }
    Ok(())
}

/// The keywords to remove from MAIN and SOURCE and the SOURCE rows whose
/// `SOURCE_MODEL` is emptied: each present, so that a table with nothing
/// to clear is not changed.
#[derive(Default)]
struct Clearing {
    main: Vec<String>,
    source: Vec<String>,
    source_cells: Vec<usize>,
}

impl Clearing {
    /// `VisModelData::clearModel`: for every field MAIN names, its pointer
    /// and model keys wherever they are, and the SOURCE row of a model
    /// under SOURCE (`removeRecordByKey`).
    fn plan_virtual_model(&mut self, ms: &MeasurementSet) -> MsResult<()> {
        let fields = fields_with_models(ms)?;
        let main = ms.main_table().keywords();
        let source = ms.subtable(SubtableId::Source);
        let source_rows = source.map_or(0, Table::row_count);
        let source_keywords = source.map(Table::keywords);
        let source_model = source
            .and_then(Table::schema)
            .is_some_and(|schema| schema.contains_column("SOURCE_MODEL"));
        for field in fields {
            let pointer = format!("definedmodel_field_{field}");
            let in_source = source_rows > 0
                && source_keywords.is_some_and(|keywords| keywords.get(&pointer).is_some());
            let model = if in_source {
                source_keywords.and_then(|keywords| string(keywords, &pointer))
            } else {
                string(main, &pointer)
            };
            if let Some(model) = model {
                if let Some(Value::Scalar(ScalarValue::Int32(row))) =
                    source_keywords.and_then(|keywords| keywords.get(&model))
                    && let Ok(row) = usize::try_from(*row)
                    && row < source_rows
                    && source_model
                {
                    self.source_cells.push(row);
                }
                present(&mut self.source, source_keywords, &model);
                present(&mut self.main, Some(main), &model);
            }
            present(&mut self.source, source_keywords, &pointer);
            present(&mut self.main, Some(main), &pointer);
        }
        self.source_cells.sort_unstable();
        self.source_cells.dedup();
        Ok(())
    }
}

/// Add `name` to `names` when `keywords` holds it and `names` does not.
fn present(names: &mut Vec<String>, keywords: Option<&RecordValue>, name: &str) {
    if keywords.is_some_and(|keywords| keywords.get(name).is_some())
        && !names.iter().any(|held| held == name)
    {
        names.push(name.to_string());
    }
}

fn remove(table: &mut Table, names: &[String]) {
    let keywords = table.keywords_mut();
    for name in names {
        keywords.remove(name);
    }
}

/// MAIN rows read at a time while looking for the fields that have models.
const FIELD_SCAN_ROWS: usize = 65_536;

/// The fields with a virtual model pointer, `definedmodel_field_<id>`, in
/// MAIN's or SOURCE's keywords, as the module states: those CASA's iterator
/// reads a virtual model for in place of `MODEL_DATA`. Only keywords are
/// read.
pub fn virtual_model_fields(ms: &MeasurementSet) -> BTreeSet<i32> {
    let pointed = |keywords: &RecordValue| -> Vec<i32> {
        keywords
            .fields()
            .iter()
            .filter_map(|field| field.name.strip_prefix("definedmodel_field_")?.parse().ok())
            .collect()
    };
    let mut fields: BTreeSet<i32> = pointed(ms.main_table().keywords()).into_iter().collect();
    if let Some(source) = ms.subtable(SubtableId::Source) {
        fields.extend(pointed(source.keywords()));
    }
    fields
}

/// The fields that have a model pointer, in MAIN's or SOURCE's keywords,
/// and rows in MAIN: those `clearModel` reaches, since it visits the fields
/// MAIN names and a field without a pointer has nothing to clear. MAIN's
/// `FIELD_ID` is read only when a pointer exists, a bounded chunk at a
/// time, and no further than to find every pointed field.
fn fields_with_models(ms: &MeasurementSet) -> MsResult<BTreeSet<i32>> {
    let wanted = virtual_model_fields(ms);
    let mut found = BTreeSet::new();
    if wanted.is_empty() {
        return Ok(found);
    }
    let main = ms.main_table();
    let column = main.column_accessor("FIELD_ID")?;
    let mut start = 0;
    while start < main.row_count() && found.len() < wanted.len() {
        let rows: Vec<usize> = (start..main.row_count().min(start + FIELD_SCAN_ROWS)).collect();
        for cell in column.scalar_cells_owned_for_rows(&rows)? {
            if let Some(ScalarValue::Int32(field)) = cell
                && wanted.contains(&field)
            {
                found.insert(field);
            }
        }
        start += FIELD_SCAN_ROWS;
    }
    Ok(found)
}

fn string(keywords: &RecordValue, name: &str) -> Option<String> {
    match keywords.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

/// How a changed table is written.
#[derive(Clone, Copy)]
enum Write {
    /// In full, with the storage managers [`MeasurementSet::save`] uses.
    Full,
    /// Its keywords and description alone, its storage left as it is.
    Metadata,
}

/// Whether `table` was opened with locking and holds its write lock.
fn holds_write_lock(table: &Table) -> bool {
    #[cfg(unix)]
    {
        table.has_lock(casa_tables::LockType::Write)
    }
    #[cfg(not(unix))]
    {
        let _ = table;
        false
    }
}

/// Apply `change` to `table` and write it to its directory under
/// casacore's write lock on it, taken in `attempts` (0 waits) unless the
/// handle holds it or the caller does through `held`, whose write is then
/// recorded for the caller's release to publish. A table not on disk is
/// changed in memory alone. When the change or the write fails, a lock
/// taken here has the table reloaded as it is on disk and is released, and
/// under `held` the table is reloaded and the lock left to the caller; a
/// lock the handle holds is left to it.
fn change_in_place(
    table: &mut Table,
    attempts: u32,
    write: Write,
    held: Option<&mut TableWriteLock>,
    change: impl FnOnce(&mut Table) -> MsResult<()>,
) -> MsResult<()> {
    let Some(path) = table.path().map(Path::to_path_buf) else {
        return change(table);
    };
    let written = |table: &mut Table| -> MsResult<()> {
        change(table)?;
        match write {
            Write::Full => table.save(measurement_set_table_options(&path))?,
            Write::Metadata => table.save_metadata_only(TableOptions::new(&path))?,
        }
        Ok(())
    };
    if let Some(lock) = held {
        lock.record_write();
        let result = written(table);
        if result.is_err() {
            let _ = table.resync();
        }
        return result;
    }
    #[cfg(unix)]
    if table.lock_options().is_some() {
        if holds_write_lock(table) {
            // Published when the caller releases its lock.
            return written(table);
        }
        if !table.lock(casa_tables::LockType::Write, attempts)? {
            return Err(lock_refused(&path));
        }
        let result = written(table);
        if result.is_err() {
            let _ = table.resync();
        }
        let released = table.unlock_metadata_only_or_discard();
        return result.and(released.map_err(MsError::from));
    }
    let mut lock = TableWriteLock::acquire(&path, attempts)?;
    lock.record_write();
    let result = written(table);
    if result.is_err() {
        let _ = table.resync();
    }
    let released = lock.release();
    result.and(released.map_err(MsError::from))
}

fn lock_refused(path: &Path) -> MsError {
    TableError::LockFailed {
        path: path.display().to_string(),
        message: "another process holds the table's lock".to_string(),
    }
    .into()
}

/// Where the table at `path` is held open, as `TableUtil::canDeleteTable`
/// asks, read from its lock file without opening the table or waiting for
/// a lock.
fn held_open(path: &Path) -> std::io::Result<Option<casa_tables::TableHolder>> {
    #[cfg(unix)]
    {
        casa_tables::table_holder(path)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

/// Delete every `FT_MODEL*` table in the MeasurementSet's directory and in
/// its SOURCE's, as `clearModel` does, at best: a table held open, in this
/// process or another, is left, as `TableUtil::canDeleteTable` leaves it,
/// and so is one whose lock file cannot be read or that cannot be deleted,
/// each logged.
fn delete_model_images(ms: &MeasurementSet) {
    let directories = [
        ms.path(),
        ms.subtable(SubtableId::Source).and_then(Table::path),
    ];
    for directory in directories.into_iter().flatten() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let image = entry.path();
            if !entry.file_name().to_string_lossy().starts_with("FT_MODEL")
                || !image.join("table.dat").is_file()
            {
                continue;
            }
            let deleted = match held_open(&image) {
                Ok(None) => std::fs::remove_dir_all(&image).map_err(|error| error.to_string()),
                Ok(Some(holder)) => Err(format!("it is held open {holder}")),
                Err(error) => Err(error.to_string()),
            };
            if let Err(error) = deleted {
                tracing::warn!(
                    image = %image.display(),
                    %error,
                    "a cleared virtual model's image is left"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

    use super::prepare_write;
    use crate::MeasurementSet;
    use crate::observation_io::{OpenOptions, UpdateSession, open_update};
    use crate::schema::SubtableId;
    use crate::schema::main_table::VisibilityDataColumn;
    use casa_observation::{
        AccessRequest, CorrelationRequest, FieldSelection, Grouping, ObservationError, PartitionId,
        Roles, TimeSelection, UpdateTarget, Updater, WeightRequest, WritableRole,
    };

    /// Imaging's writer of `MODEL_DATA` at `path`: an update session with a
    /// model target.
    fn imaging_session(path: &Path) -> Result<UpdateSession, ObservationError> {
        let model = UpdateTarget {
            create: true,
            ..UpdateTarget::new("model", WritableRole::Visibilities)
        };
        open_update(path, &OpenOptions::default(), &[model])
    }

    /// The session's first updater, which clears what CASA clears and
    /// creates a missing `MODEL_DATA` seeded with CASA's initial model.
    fn begin(session: &UpdateSession) -> Result<Box<dyn Updater + '_>, ObservationError> {
        let channels = session
            .observation()
            .catalogue()
            .partition(PartitionId(0))
            .frequencies_hz
            .len();
        let request = AccessRequest {
            partitions: vec![PartitionId(0)],
            data_group: "base".to_string(),
            roles: Roles {
                values: false,
                flags: false,
                weights: false,
                uvw: false,
                time_centroid: false,
                interval: false,
                exposure: false,
                scan: false,
            },
            channels: 0..channels,
            times: TimeSelection::All,
            fields: FieldSelection::All,
            weights: WeightRequest::Stored,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        session.updater(&request, &[0])
    }

    /// Imaging taking `MODEL_DATA` at `path` before its first batch: the
    /// session's first updater bound and finished.
    fn imaging_takes_model_data(path: &Path) -> Result<(), ObservationError> {
        let session = imaging_session(path)?;
        begin(&session)?.finish()?;
        Ok(())
    }

    fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
        let archive = Path::new(env!("CARGO_MANIFEST_DIR")).join(archive);
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(archive).expect("fixture"),
        ))
        .unpack(root)
        .expect("unpack");
        root.join(name)
    }

    /// The trial fixture, whose MAIN names field 0 and which has no SOURCE.
    fn trial(root: &Path) -> PathBuf {
        unpack(
            root,
            "tests/fixtures/ms_trial/ms_trial.ms.tgz",
            "ms_trial.ms",
        )
    }

    fn text(value: &str) -> Value {
        Value::Scalar(ScalarValue::String(value.to_string()))
    }

    /// A component-list model record as `setjy` writes it.
    fn model() -> RecordValue {
        RecordValue::new(vec![
            RecordField::new("numcl", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                "cl_0",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "fields",
                    Value::Array(ArrayValue::Int32(
                        ndarray::ArrayD::from_shape_vec(vec![1], vec![0]).expect("shape"),
                    )),
                )])),
            ),
            RecordField::new("version", Value::Scalar(ScalarValue::Float64(2.0))),
        ])
    }

    /// A small table at `path`.
    fn table_at(path: &Path) {
        let mut table = Table::with_schema(
            TableSchema::new(vec![ColumnSchema::scalar("X", PrimitiveType::Int32)])
                .expect("schema"),
        );
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "X",
                Value::Scalar(ScalarValue::Int32(1)),
            )]))
            .expect("row");
        table.save(TableOptions::new(path)).expect("save");
    }

    /// MAIN's keywords as CASA leaves them after a sorted iteration and a
    /// `setjy` into MAIN (an MS without SOURCE rows): the sort keywords and
    /// field 0's model, plus a model for a field MAIN does not name and an
    /// unrelated keyword, which stay.
    fn as_casa_left_it(path: &Path) {
        table_at(&path.join("SORTED_TABLE"));
        let mut ms = MeasurementSet::open(path).expect("open");
        let keywords = ms.main_table_mut().keywords_mut();
        keywords.upsert(
            "SORT_COLUMNS",
            Value::Array(ArrayValue::String(
                ndarray::ArrayD::from_shape_vec(vec![1], vec!["TIME".to_string()]).expect("shape"),
            )),
        );
        keywords.upsert(
            "SORTED_TABLE",
            Value::TableRef("././SORTED_TABLE".to_string()),
        );
        keywords.upsert("definedmodel_field_0", text("model_0"));
        keywords.upsert("model_0", Value::Record(model()));
        keywords.upsert("definedmodel_field_7", text("model_7"));
        keywords.upsert("model_7", Value::Record(model()));
        keywords.upsert("KEEP", text("kept"));
        ms.save().expect("save");
    }

    fn main_keywords(path: &Path) -> Vec<String> {
        let ms = MeasurementSet::open(path).expect("reopen");
        let mut names: Vec<String> = ms
            .main_table()
            .keywords()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .filter(|name| {
                !SubtableId::ALL_REQUIRED
                    .iter()
                    .chain(SubtableId::ALL_OPTIONAL)
                    .any(|id| id.name() == name)
            })
            .collect();
        names.sort();
        names
    }

    /// Adding `MODEL_DATA` clears the sort keywords and the model of every
    /// field MAIN names (not the sorted table itself, as CASA leaves it);
    /// adding `CORRECTED_DATA` clears only the sort keywords.
    #[test]
    fn scratch_columns_clear_what_casa_clears() {
        for (column, expected) in [
            (
                VisibilityDataColumn::ModelData,
                vec!["KEEP", "MS_VERSION", "definedmodel_field_7", "model_7"],
            ),
            (
                VisibilityDataColumn::CorrectedData,
                vec![
                    "KEEP",
                    "MS_VERSION",
                    "definedmodel_field_0",
                    "definedmodel_field_7",
                    "model_0",
                    "model_7",
                ],
            ),
        ] {
            let root = tempfile::tempdir().expect("root");
            let path = trial(root.path());
            as_casa_left_it(&path);
            let mut ms = MeasurementSet::open(&path).expect("open");
            prepare_write(&mut ms, column, true).expect("clear");
            drop(ms);
            assert_eq!(main_keywords(&path), expected, "{column:?}");
            assert!(path.join("SORTED_TABLE/table.dat").is_file(), "{column:?}");
        }
    }

    /// A field's pointer naming no model is a dangling key, removed as CASA
    /// removes it; a field without a model changes nothing.
    #[test]
    fn a_dangling_model_pointer_is_removed() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        let mut ms = MeasurementSet::open(&path).expect("open");
        ms.main_table_mut()
            .keywords_mut()
            .upsert("definedmodel_field_0", text("model_9"));
        prepare_write(&mut ms, VisibilityDataColumn::ModelData, true).expect("clear");
        assert!(
            ms.main_table()
                .keywords()
                .get("definedmodel_field_0")
                .is_none()
        );
        let before = ms.main_table().keywords().clone();
        prepare_write(&mut ms, VisibilityDataColumn::ModelData, true).expect("again");
        assert_eq!(*ms.main_table().keywords(), before);
    }

    /// Clearing a model deletes the `FT_MODEL*` tables `tclean` leaves in
    /// the MeasurementSet, and no image a model's record names: one named
    /// inside the MeasurementSet, one outside it and one reached through
    /// `..` all stay.
    #[test]
    fn clearing_deletes_the_ft_model_tables_and_nothing_a_record_names() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        let inside = path.join("model_image");
        let outside = root.path().join("outside_image");
        let escaping = path.join("..").join("escaping_image");
        for table in [&inside, &outside, &escaping, &path.join("FT_MODEL_x")] {
            table_at(table);
        }
        let ft = |image: &Path| {
            Value::Record(RecordValue::new(vec![RecordField::new(
                "container",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "diskimage",
                    text(&image.display().to_string()),
                )])),
            )]))
        };
        let mut ms = MeasurementSet::open(&path).expect("open");
        let keywords = ms.main_table_mut().keywords_mut();
        keywords.upsert("definedmodel_field_0", text("model_0"));
        keywords.upsert(
            "model_0",
            Value::Record(RecordValue::new(vec![
                RecordField::new("numft", Value::Scalar(ScalarValue::Int32(3))),
                RecordField::new("ft_0", ft(&inside)),
                RecordField::new("ft_1", ft(&outside)),
                RecordField::new("ft_2", ft(&escaping)),
            ])),
        );
        prepare_write(&mut ms, VisibilityDataColumn::ModelData, false).expect("clear");
        assert!(!path.join("FT_MODEL_x").exists(), "the FT_MODEL table");
        for image in [&inside, &outside, &escaping] {
            assert!(image.join("table.dat").is_file(), "{}", image.display());
        }
    }

    /// A fixture with SOURCE rows and `setjy`'s model under SOURCE: its
    /// keywords name the model key and the row whose `SOURCE_MODEL` holds
    /// the record (CASA 6.7.5 on this fixture), beside MAIN's cached sort
    /// keywords.
    fn with_model_under_source(root: &Path) -> PathBuf {
        let path = unpack(
            root,
            "tests/fixtures/mssel_test_small.ms.tgz",
            "mssel_test_small.ms",
        );
        table_at(&path.join("SORTED_TABLE"));
        let mut ms = MeasurementSet::open(&path).expect("open");
        let source = ms.subtable_mut(SubtableId::Source).expect("SOURCE");
        source
            .add_column(ColumnSchema::record("SOURCE_MODEL"), None)
            .expect("SOURCE_MODEL");
        source
            .cell_accessor_mut(0, "SOURCE_MODEL")
            .and_then(|mut cell| cell.set(Value::Record(model())))
            .expect("model");
        let keywords = source.keywords_mut();
        keywords.upsert("definedmodel_field_0", text("model_0"));
        keywords.upsert("model_0", Value::Scalar(ScalarValue::Int32(0)));
        ms.main_table_mut().keywords_mut().upsert(
            "SORTED_TABLE",
            Value::TableRef("././SORTED_TABLE".to_string()),
        );
        ms.save().expect("save");
        path
    }

    /// The storage managers of MAIN and SOURCE, by type and columns.
    fn managers(path: &Path) -> [Vec<(String, Vec<String>)>; 2] {
        let ms = MeasurementSet::open(path).expect("open");
        let of = |table: &Table| {
            table
                .data_manager_info()
                .iter()
                .map(|manager| (manager.dm_type.clone(), manager.columns.clone()))
                .collect()
        };
        [
            of(ms.main_table()),
            of(ms.subtable(SubtableId::Source).expect("SOURCE")),
        ]
    }

    /// SOURCE as clearing leaves it: no model keywords and an emptied
    /// `SOURCE_MODEL` cell, the column kept, as `delmod` leaves it.
    fn assert_source_cleared(path: &Path) {
        let ms = MeasurementSet::open(path).expect("reopen");
        let source = ms.subtable(SubtableId::Source).expect("SOURCE");
        for key in ["definedmodel_field_0", "model_0"] {
            assert!(source.keywords().get(key).is_none(), "{key}");
        }
        assert_eq!(
            source.record_cell(0, "SOURCE_MODEL").expect("cell"),
            RecordValue::default()
        );
    }

    /// Clearing a model under SOURCE removes both keywords and empties the
    /// cell, and writes SOURCE as a MeasurementSet save writes it
    /// (StandardStMan) and MAIN's metadata alone, its storage kept; the
    /// `FT_MODEL*` tables `tclean` leaves in SOURCE go too.
    #[test]
    fn a_model_under_source_is_cleared_and_its_cell_emptied() {
        let root = tempfile::tempdir().expect("root");
        let path = with_model_under_source(root.path());
        table_at(&path.join("SOURCE/FT_MODEL_y"));
        let before = managers(&path);
        let mut ms = MeasurementSet::open(&path).expect("open");
        prepare_write(&mut ms, VisibilityDataColumn::ModelData, true).expect("clear");
        drop(ms);
        assert_source_cleared(&path);
        assert!(
            !path.join("SOURCE/FT_MODEL_y").exists(),
            "SOURCE's FT_MODEL table"
        );
        assert_eq!(managers(&path), before);
        assert!(
            main_keywords(&path)
                .iter()
                .all(|name| name != "SORTED_TABLE")
        );
    }

    /// Imaging's writer clears a model under SOURCE under SOURCE's own lock,
    /// writing SOURCE as a save writes it, and keeps MAIN's storage
    /// managers, adding `MODEL_DATA`'s.
    #[test]
    fn imaging_clears_a_model_under_source() {
        let root = tempfile::tempdir().expect("root");
        let path = with_model_under_source(root.path());
        let [main_before, source_before] = managers(&path);
        imaging_takes_model_data(&path).expect("take MODEL_DATA");
        assert_source_cleared(&path);
        let [main, source] = managers(&path);
        assert_eq!(source, source_before);
        assert_eq!(main[..main_before.len()], main_before[..]);
        assert!(
            main_keywords(&path)
                .iter()
                .all(|name| name != "SORTED_TABLE")
        );
    }

    /// A write refused once MAIN's lock is taken (here, SOURCE's lock held
    /// by another handle) changes nothing: MAIN keeps its storage managers
    /// and keywords, with no `MODEL_DATA`, and SOURCE its model.
    #[test]
    fn a_write_refused_under_the_lock_changes_nothing() {
        let root = tempfile::tempdir().expect("root");
        let path = with_model_under_source(root.path());
        let managers_before = managers(&path);
        let keywords_before = main_keywords(&path);
        let held = casa_tables::TableWriteLock::acquire(path.join("SOURCE"), 1).expect("lock");
        let refused = imaging_takes_model_data(&path);
        assert!(refused.is_err(), "refused while SOURCE is held");
        held.release().expect("release");
        assert_eq!(managers(&path), managers_before);
        assert_eq!(main_keywords(&path), keywords_before);
        let ms = MeasurementSet::open(&path).expect("reopen");
        let source = ms.subtable(SubtableId::Source).expect("SOURCE");
        assert!(source.keywords().get("definedmodel_field_0").is_some());
        assert_eq!(
            source.record_cell(0, "SOURCE_MODEL").expect("cell"),
            model()
        );
    }

    /// Imaging's in-place writer, creating `MODEL_DATA`, clears what CASA
    /// clears, on disk.
    #[test]
    fn imaging_s_model_data_write_clears_what_casa_clears() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        as_casa_left_it(&path);
        imaging_takes_model_data(&path).expect("take MODEL_DATA");
        assert_eq!(
            main_keywords(&path),
            ["KEEP", "MS_VERSION", "definedmodel_field_7", "model_7"]
        );
    }

    /// A MeasurementSet opened without locking writes under casacore's
    /// write lock: while another handle holds SOURCE's, clearing a model
    /// there is refused and changes nothing.
    #[test]
    fn a_plain_handle_writes_under_casacore_s_lock() {
        let root = tempfile::tempdir().expect("root");
        let path = with_model_under_source(root.path());
        let keywords_before = main_keywords(&path);
        let held = casa_tables::TableWriteLock::acquire(path.join("SOURCE"), 1).expect("lock");
        let mut ms = MeasurementSet::open(&path).expect("open");
        assert!(
            matches!(
                prepare_write(&mut ms, VisibilityDataColumn::ModelData, true),
                Err(crate::MsError::Table(
                    casa_tables::TableError::LockFailed { .. }
                ))
            ),
            "refused while SOURCE is held"
        );
        drop(ms);
        held.release().expect("release");
        assert_eq!(main_keywords(&path), keywords_before);
        let ms = MeasurementSet::open(&path).expect("reopen");
        let source = ms.subtable(SubtableId::Source).expect("SOURCE");
        assert!(source.keywords().get("definedmodel_field_0").is_some());
    }

    /// MAIN's `table.*` files.
    fn table_files(path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path)
            .expect("list")
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("table."))
            .collect();
        names.sort();
        names
    }

    /// Restores `table.dat`'s mode when dropped.
    struct Writable(PathBuf);

    impl Drop for Writable {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o644));
        }
    }

    /// Make MAIN's `table.dat` read-only, or `None` where it stays
    /// writable (a process running as root).
    fn read_only_table_dat(path: &Path) -> Option<Writable> {
        use std::os::unix::fs::PermissionsExt;
        let table_dat = path.join("table.dat");
        std::fs::set_permissions(&table_dat, std::fs::Permissions::from_mode(0o444))
            .expect("read only");
        let restore = Writable(table_dat);
        if std::fs::OpenOptions::new()
            .write(true)
            .open(&restore.0)
            .is_ok()
        {
            eprintln!("skipped: table.dat stays writable for this process");
            return None;
        }
        Some(restore)
    }

    /// A write whose MAIN metadata cannot be written as it begins fails and
    /// leaves MAIN as it was: released, not rewritten in full.
    #[test]
    fn a_failed_metadata_write_leaves_main_as_it_was() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        as_casa_left_it(&path);
        let files_before = table_files(&path);
        let keywords_before = main_keywords(&path);
        let Some(read_only) = read_only_table_dat(&path) else {
            return;
        };
        let written = imaging_takes_model_data(&path);
        drop(read_only);
        assert!(written.is_err(), "the metadata write fails");
        assert_eq!(table_files(&path), files_before);
        assert_eq!(main_keywords(&path), keywords_before);
    }

    /// A write's completion writes none of MAIN's metadata: the update
    /// session creates and seeds `MODEL_DATA` when its first updater begins
    /// (where the deleted selected-visibility write did it on completion),
    /// so a completion with `table.dat` read-only succeeds and leaves MAIN
    /// as the write left it on disk, `MODEL_DATA` created and seeded.
    #[test]
    fn a_completion_leaves_main_as_its_beginning_left_it() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        let session = imaging_session(&path).expect("session");
        let write = begin(&session).expect("begin");
        let files_begun = table_files(&path);
        let Some(read_only) = read_only_table_dat(&path) else {
            return;
        };
        let completed = write.finish();
        drop(read_only);
        completed.expect("the completion writes no metadata");
        assert_eq!(table_files(&path), files_begun);
        let ms = MeasurementSet::open(&path).expect("reopen");
        assert!(
            ms.main_table()
                .data_manager_info()
                .iter()
                .any(|manager| manager.columns.iter().any(|name| name == "MODEL_DATA")),
            "MODEL_DATA's storage stays"
        );
    }

    /// A write to an existing `MODEL_DATA` clears the virtual model, as
    /// CASA's imager does whenever it writes the column, and keeps the sort
    /// keywords, which only adding a column clears.
    #[test]
    fn imaging_s_write_of_an_existing_model_data_clears_the_virtual_model() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        imaging_takes_model_data(&path).expect("created");
        as_casa_left_it(&path);
        imaging_takes_model_data(&path).expect("taken again");
        assert_eq!(
            main_keywords(&path),
            [
                "KEEP",
                "MS_VERSION",
                "SORTED_TABLE",
                "SORT_COLUMNS",
                "definedmodel_field_7",
                "model_7"
            ]
        );
    }
}
