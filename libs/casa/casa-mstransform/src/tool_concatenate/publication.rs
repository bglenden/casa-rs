// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish concatenation's small table updates together.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_core::{PrimitiveType, RecordValue, ScalarValue, Value};
use casa_tables::{
    ColumnAddition, ColumnSchema, PreparedTableEdit, Table, TableOptions, TablePartNames,
    TableWriteLock,
    publication::{
        check_write_access, copy_locked_table_tree, exchange_directories,
        lock_table_tree_for_replacement_with_held,
    },
};
use rayon::prelude::*;
use tempfile::TempDir;

use super::{
    MetadataPlan, PhysicalConcatenation, TableRowsChange, TestConcatenation, VirtualConcatenation,
    auxiliary::ScanFile,
    locking::{InputTables, WriteGuards},
    main_rows::{self, MainAppend},
    named_output::NamedOutput,
    required_table,
    virtual_rows::{self, MainReplacement},
};
use casa_ms::history_rows;
use casa_ms::{MsError, MsResult, SubtableId};

pub(super) fn metadata_only(
    target: &Path,
    donor: &Path,
    request: &TestConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    concatenate(target, donor, request, Mode::Metadata, retained)
}

pub(super) fn physical(
    target: &Path,
    donor: &Path,
    request: &PhysicalConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    concatenate(
        target,
        donor,
        &request.metadata,
        Mode::Physical(request),
        retained,
    )
}

pub(super) fn virtual_concat(
    target: &Path,
    donor: &Path,
    request: &VirtualConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    concatenate(
        target,
        donor,
        &request.metadata,
        Mode::Virtual(request),
        retained,
    )
}

#[derive(Clone, Copy)]
enum Mode<'a> {
    Metadata,
    Physical(&'a PhysicalConcatenation<'a>),
    Virtual(&'a VirtualConcatenation<'a>),
}

fn concatenate(
    target: &Path,
    donor: &Path,
    request: &TestConcatenation<'_>,
    mode: Mode<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    let physical = if let Mode::Physical(mode) = mode {
        Some(mode)
    } else {
        None
    };
    let virtual_mode = if let Mode::Virtual(mode) = mode {
        Some(mode)
    } else {
        None
    };
    let mut scan_file = virtual_mode
        .and_then(|mode| mode.scan_file)
        .map(ScanFile::inspect)
        .transpose()?;
    let target = target.canonicalize().map_err(io)?;
    let donor = donor.canonicalize().map_err(io)?;
    if target.starts_with(&donor) || donor.starts_with(&target) {
        return Err(io("concatenation requires separate input MeasurementSets"));
    }
    for root in [&target, &donor] {
        check_write_access(&root.join("table.dat")).map_err(io)?;
    }
    let mut held = WriteGuards::new(retained);
    held.acquire(BTreeSet::from([target.clone(), donor.clone()]))?;
    let (attached, incoming) = super::join_with_tracing(
        || InputTables::capture(&target),
        || InputTables::capture(&donor),
    );
    let (attached, target_main) = attached?;
    let (incoming, donor_main) = incoming?;
    let mut leaf_writers = attached.write_paths(true);
    leaf_writers.extend(incoming.write_paths(false));
    let pointing = physical.is_some_and(|mode| mode.handling < 2);
    if pointing {
        leaf_writers.extend(
            attached
                .subtable_path(SubtableId::Pointing)
                .map(Path::to_path_buf),
        );
    }
    held.acquire(leaf_writers)?;
    validate_inputs(&attached, &incoming)?;

    let (target_ms, donor_ms) = super::join_with_tracing(
        || attached.open(target_main, false),
        || incoming.open(donor_main, true),
    );
    let target_ms = target_ms?;
    let donor_ms = donor_ms?;
    let _donor_ephemerides = incoming.hold_ephemerides()?;
    let target_calibration = open_calibration(&attached.calibration, false)?;
    let donor_calibration = open_calibration(&incoming.calibration, true)?;
    super::layout::check(&target_ms, &donor_ms)?;
    let named = physical
        .and_then(|mode| mode.output)
        .map(|output| NamedOutput::stage(output, &target_ms, &donor_ms))
        .transpose()?;
    let plan = MetadataPlan::reconcile(
        &target_ms,
        &donor_ms,
        [&target_calibration, &donor_calibration],
        request.frequency_hz,
        request.direction_rad,
        request.respect_name,
        virtual_mode.is_some(),
    )?;

    let virtual_pointing = virtual_mode.is_some()
        && plan
            .row_map
            .antennas
            .iter()
            .copied()
            .ne(0..plan.row_map.antennas.len());
    if virtual_pointing {
        held.acquire(
            incoming
                .subtable_path(SubtableId::Pointing)
                .map(Path::to_path_buf)
                .into_iter()
                .collect(),
        )?;
    }
    let donor_pointing = incoming
        .subtable_path(SubtableId::Pointing)
        .filter(|_| pointing || virtual_pointing)
        .map(|path| open_pointing(path, virtual_pointing))
        .transpose()?;
    let target_pointing = attached
        .subtable_path(SubtableId::Pointing)
        .filter(|_| pointing || virtual_pointing)
        .map(|path| open_pointing(path, pointing))
        .transpose()?;
    let attached_history = attached
        .subtable_path(SubtableId::History)
        .filter(|_| !plan.row_map.attached_observations.is_empty())
        .map(|path| -> MsResult<_> {
            held.acquire(BTreeSet::from([path.to_path_buf()]))?;
            Ok(Table::open(TableOptions::new(path))?)
        })
        .transpose()?;
    let mut requests = metadata_edits(&plan, &target_ms, &incoming, &target, &held.guards)?;
    if let Some(history) = &attached_history {
        prepare_attached_history(
            history,
            &plan.row_map.attached_observations,
            &incoming,
            &target,
            &mut requests,
        )?;
    }
    if let Some((left, right)) = target_pointing.as_ref().zip(donor_pointing.as_ref()) {
        let (table, opposite, root) = if virtual_pointing {
            (right, &attached, &donor)
        } else {
            (left, &incoming, &target)
        };
        let path = output_path(table, opposite, root)?;
        let edit = if virtual_pointing {
            prepare_virtual_pointing(left, right, &plan, path.clone(), &held.guards)?
        } else {
            prepare_pointing(left, right, &plan, path.clone(), &held.guards)?
        };
        if let Some(edit) = edit {
            insert_edit(&mut requests, path, edit)?;
        }
    }
    let history = required_table(&donor_ms, SubtableId::History)?;
    let history_path = output_path(history, &attached, &donor)?;
    let logs = prepare_history(history, request, mode)?;
    insert_edit(
        &mut requests,
        history_path,
        EditRequest::Append(history, Cow::Owned(logs)),
    )?;
    if let Some(mode) = physical.filter(|mode| mode.handling % 2 == 0 && named.is_none())
        && let Some(rows) = main_rows::prepare(
            &target_ms,
            &donor_ms,
            target_ms.main_table(),
            &plan,
            mode.weight_scale,
        )?
    {
        insert_edit(
            &mut requests,
            target.clone(),
            EditRequest::Main(target_ms.main_table(), Box::new(rows)),
        )?;
    }
    let scan_text = if let Some(mode) = virtual_mode {
        prepare_virtual_main(
            &target_ms,
            &donor_ms,
            &plan,
            mode,
            scan_file.as_mut(),
            &mut requests,
        )?
    } else {
        None
    };
    let named_held = named
        .as_ref()
        .map(|_| held.guards.keys().cloned().collect::<BTreeSet<_>>());
    // Canonical paths are distinct. Iterate the map to borrow each guard
    // once and hold it across all fallible edits and synchronization.
    let work = held
        .guards
        .iter_mut()
        .filter_map(|(path, guard)| requests.remove(path).map(|request| (guard, request)))
        .collect::<Vec<_>>();
    if !requests.is_empty() {
        return Err(io("concatenation edit is missing its captured write lock"));
    }
    let prepare_tables = || {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        work.into_par_iter()
            .map(|entry| tracing::dispatcher::with_default(&dispatch, || prepare_table_edit(entry)))
            .collect::<MsResult<Vec<_>>>()
    };
    let edits = if let Some((file, text)) = scan_file.as_mut().zip(scan_text.as_deref()) {
        super::join_with_tracing(prepare_tables, || file.stage(text)).0?
    } else {
        prepare_tables()?
    };
    validate_inputs(&attached, &incoming)?;
    publish_batch_then(edits, || {
        if let Some(named) = named {
            let mode = physical.expect("named physical request");
            named.finish(
                &target_ms,
                &donor_ms,
                &plan,
                mode,
                &named_held.expect("named output lock paths"),
            )?;
        }
        if let Some(scan_file) = scan_file {
            scan_file.publish()?;
        }
        Ok(())
    })
}

fn validate_inputs(attached: &InputTables, incoming: &InputTables) -> MsResult<()> {
    let (attached, incoming) = rayon::join(|| attached.validate(), || incoming.validate());
    attached.and(incoming)
}

fn prepare_attached_history<'a>(
    history: &'a Table,
    observations: &[usize],
    incoming: &InputTables,
    target: &Path,
    requests: &mut BTreeMap<PathBuf, EditRequest<'a>>,
) -> MsResult<()> {
    let mut columns = BTreeMap::new();
    virtual_rows::remap_ids(history, "OBSERVATION_ID", observations, &mut columns)?;
    if columns.is_empty() {
        return Ok(());
    }
    let path = output_path(history, incoming, target)?;
    let values = MainReplacement {
        columns,
        channel_selection: None,
    };
    insert_edit(
        requests,
        path,
        EditRequest::Columns(history, Box::new(values)),
    )
}

fn prepare_table_edit<'a>(
    (guard, edit): (&'a mut TableWriteLock, EditRequest<'_>),
) -> MsResult<TableEdit<'a>> {
    match edit {
        EditRequest::Append(table, rows) => Ok(TableEdit::Append(
            table.prepare_record_append(&rows, guard)?,
        )),
        EditRequest::Replace(staged) => Ok(TableEdit::Replace(*staged, guard)),
        EditRequest::Columns(table, values) => {
            let keyword = values
                .channel_selection
                .as_ref()
                .map(|value| ("MODEL_DATA", "CHANNEL_SELECTION", value));
            Ok(TableEdit::Append(table.prepare_column_replacement(
                &values.columns,
                keyword,
                guard,
            )?))
        }
        EditRequest::Main(table, rows) => Ok(TableEdit::Append(
            table.prepare_record_append_with_options(&rows.records, rows.options(), guard)?,
        )),
    }
}

fn metadata_edits<'a>(
    plan: &'a MetadataPlan<'a>,
    target_ms: &'a casa_ms::MeasurementSet,
    incoming: &InputTables,
    target: &Path,
    held: &BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<BTreeMap<PathBuf, EditRequest<'a>>> {
    let mut requests = BTreeMap::new();
    for change in &plan.changes {
        let table = required_table(target_ms, change.table)?;
        let path = output_path(table, incoming, target)?;
        let field = (change.table == SubtableId::Field).then_some(plan);
        let request = metadata_edit(table, &path, &change.rows, field, held)?;
        insert_edit(&mut requests, path, request)?;
    }
    for (table, rows) in &plan.calibration {
        let path = output_path(table, incoming, target)?;
        let request = metadata_edit(table, &path, rows, None, held)?;
        insert_edit(&mut requests, path, request)?;
    }
    Ok(requests)
}

fn metadata_edit<'a>(
    table: &'a Table,
    path: &Path,
    rows: &'a TableRowsChange,
    field: Option<&MetadataPlan<'_>>,
    held: &BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<EditRequest<'a>> {
    let replacement = !rows.updated.is_empty()
        || !rows.removed.is_empty()
        || field.is_some_and(|plan| plan.add_ephemeris_column || !plan.ephemerides.is_empty());
    if replacement {
        Ok(EditRequest::Replace(Box::new(StagedTable::prepare_change(
            path.to_owned(),
            rows,
            field,
            held,
        )?)))
    } else {
        Ok(EditRequest::Append(
            table,
            matching_fields(table, &rows.appended),
        ))
    }
}

fn open_pointing(path: &Path, write: bool) -> MsResult<Table> {
    if write {
        Ok(Table::open(TableOptions::new(path))?)
    } else {
        casa_ms::held_subtable_reader(path)
    }
}

fn prepare_virtual_main<'a>(
    target: &'a casa_ms::MeasurementSet,
    donor: &'a casa_ms::MeasurementSet,
    plan: &MetadataPlan<'_>,
    mode: &VirtualConcatenation<'_>,
    auxiliary: Option<&mut ScanFile>,
    edits: &mut BTreeMap<PathBuf, EditRequest<'a>>,
) -> MsResult<Option<String>> {
    let left = target.main_table();
    let right = donor.main_table();
    let row_map = &plan.row_map;
    let read_bounds = auxiliary.map(|file| {
        move || file.starting_bounds(left, mode.attached_rows, &row_map.attached_observations)
    });
    let (columns, scan) =
        virtual_rows::prepare(target, donor, &plan.row_map, mode.weight_scale, read_bounds)?;
    if !columns.columns.is_empty() || columns.channel_selection.is_some() {
        let path = right.path().ok_or_else(|| io("donor MAIN has no path"))?;
        insert_edit(
            edits,
            path.to_owned(),
            EditRequest::Columns(right, Box::new(columns)),
        )?;
    }
    let columns = virtual_rows::attached_replacements(left, row_map, mode.attached_rows)?;
    if !columns.columns.is_empty() {
        let path = left.path().ok_or_else(|| io("attached MAIN has no path"))?;
        insert_edit(
            edits,
            path.to_owned(),
            EditRequest::Columns(left, Box::new(columns)),
        )?;
    }
    Ok(scan)
}

fn prepare_virtual_pointing<'a>(
    target: &Table,
    donor: &'a Table,
    plan: &MetadataPlan<'_>,
    path: PathBuf,
    held: &BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<Option<EditRequest<'a>>> {
    if donor.row_count() == 0 {
        return Ok(None);
    }
    if target.row_count() == 0 {
        let change = TableRowsChange {
            removed: (0..donor.row_count()).collect(),
            ..TableRowsChange::default()
        };
        return Ok(Some(EditRequest::Replace(Box::new(
            StagedTable::prepare_change(path, &change, None, held)?,
        ))));
    }
    let values = donor.scalar_columns_owned(&["ANTENNA_ID"])?;
    let mapped = values["ANTENNA_ID"]
        .iter()
        .map(|value| {
            let Some(ScalarValue::Int32(id)) = value else {
                return Err(io("POINTING entry has no antenna ID"));
            };
            let antenna =
                super::main_rows::lookup(&plan.row_map.antennas, *id, "POINTING.ANTENNA_ID")?;
            Ok(Some(Value::Scalar(ScalarValue::Int32(
                super::spectral::row_id(*antenna)?,
            ))))
        })
        .collect::<MsResult<Vec<_>>>()?;
    Ok(Some(EditRequest::Columns(
        donor,
        Box::new(MainReplacement {
            columns: BTreeMap::from([("ANTENNA_ID".into(), mapped)]),
            channel_selection: None,
        }),
    )))
}

fn prepare_pointing<'a>(
    target: &'a Table,
    donor: &Table,
    plan: &MetadataPlan<'_>,
    path: PathBuf,
    held: &BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<Option<EditRequest<'a>>> {
    if target.row_count() == 0 {
        return Ok(None);
    }
    if donor.row_count() == 0 {
        let change = TableRowsChange {
            removed: (0..target.row_count()).collect(),
            ..TableRowsChange::default()
        };
        return Ok(Some(EditRequest::Replace(Box::new(
            StagedTable::prepare_change(path, &change, None, held)?,
        ))));
    }
    let rows = donor
        .rows()?
        .iter()
        .map(|record| {
            let Some(Value::Scalar(ScalarValue::Int32(antenna))) = record.get("ANTENNA_ID") else {
                return Err(io("POINTING entry has no antenna ID"));
            };
            let id = usize::try_from(*antenna)
                .ok()
                .and_then(|id| plan.row_map.antennas.get(id))
                .ok_or_else(|| io(format!("POINTING antenna ID {antenna} is invalid")))?;
            let mut row = record.clone();
            row.upsert(
                "ANTENNA_ID",
                Value::Scalar(ScalarValue::Int32(super::spectral::row_id(*id)?)),
            );
            Ok(row)
        })
        .collect::<MsResult<Vec<_>>>()?;
    Ok(Some(EditRequest::Append(
        target,
        Cow::Owned(matching_fields(target, &rows).into_owned()),
    )))
}

fn open_calibration(
    links: &BTreeMap<&'static str, PathBuf>,
    donor: bool,
) -> MsResult<BTreeMap<&'static str, Table>> {
    links
        .iter()
        .map(|(&name, path)| {
            let table = if donor {
                casa_ms::held_subtable_reader(path)?
            } else {
                Table::open(TableOptions::new(path))?
            };
            Ok((name, table))
        })
        .collect()
}

fn prepare_history(
    table: &Table,
    request: &TestConcatenation<'_>,
    operation: Mode<'_>,
) -> MsResult<Vec<RecordValue>> {
    if matches!(operation, Mode::Virtual(_)) {
        let message = format!(
            "{} virtually concatenated with {}",
            request.donor_name,
            request.attached_name.display()
        );
        let command = format!(
            "msfile='{}' freqTol='{}' dirTol='{}' respectname='{}'",
            request.donor_name,
            request.frequency_text,
            request.direction_text,
            u8::from(request.respect_name),
        );
        return history_rows(table, &[message], &command, "ms::virtconcatenate()", "ms");
    }
    if let Mode::Physical(mode) = operation {
        let message = format!(
            "{} appended to {}",
            request.donor_name,
            request.attached_name.display()
        );
        let destination = mode
            .output
            .map(|output| output.to_string_lossy())
            .unwrap_or_default();
        let command = format!(
            "msfile='{}' freqTol='{}' dirTol='{}' respectname='{}' handling= {} destmsfile='{}'",
            request.donor_name,
            request.frequency_text,
            request.direction_text,
            u8::from(request.respect_name),
            mode.handling,
            destination,
        );
        return history_rows(table, &[message], &command, "ms::concatenate()", "ms");
    }
    let message = format!(
        "Subtables from {} appended to those from {}",
        request.donor_name,
        request.attached_name.display()
    );
    let command = format!(
        "msfile= {} freqTol='{}' dirTol='{}'' respectname='{}",
        request.donor_name,
        request.frequency_text,
        request.direction_text,
        u8::from(request.respect_name),
    );
    history_rows(table, &[message], &command, "ms::testconcatenate()", "ms")
}

fn output_path(table: &Table, opposite: &InputTables, own_root: &Path) -> MsResult<PathBuf> {
    let path = table.path().ok_or_else(|| io("a subtable has no path"))?;
    if path == own_root || opposite.contains(path) {
        return Err(io(format!(
            "concatenation would change a shared table at {}",
            path.display()
        )));
    }
    check_write_access(&path.join("table.dat")).map_err(io)?;
    Ok(path.to_path_buf())
}

enum EditRequest<'a> {
    Append(&'a Table, Cow<'a, [RecordValue]>),
    Main(&'a Table, Box<MainAppend>),
    Columns(&'a Table, Box<MainReplacement>),
    Replace(Box<StagedTable>),
}

fn insert_edit<'a>(
    edits: &mut BTreeMap<PathBuf, EditRequest<'a>>,
    path: PathBuf,
    edit: EditRequest<'a>,
) -> MsResult<()> {
    use std::collections::btree_map::Entry;
    match edits.entry(path) {
        Entry::Occupied(entry) => Err(io(format!("several edits share {}", entry.key().display()))),
        Entry::Vacant(entry) => {
            entry.insert(edit);
            Ok(())
        }
    }
}

fn publish_batch_then(
    mut edits: Vec<TableEdit<'_>>,
    finish: impl FnOnce() -> MsResult<()>,
) -> MsResult<()> {
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let publish = edits
        .par_iter_mut()
        .map(|edit| tracing::dispatcher::with_default(&dispatch, || edit.publish()))
        .collect::<Vec<_>>();
    let result: MsResult<()> = publish
        .into_iter()
        .collect::<MsResult<()>>()
        .and_then(|()| {
            edits
                .par_iter_mut()
                .map(|edit| tracing::dispatcher::with_default(&dispatch, || edit.synchronize()))
                .collect::<Vec<_>>()
                .into_iter()
                .collect()
        })
        .and_then(|()| finish());
    if let Err(error) = result {
        let failures: Vec<_> = edits
            .into_iter()
            .rev()
            .filter_map(|edit| edit.restore().err())
            .map(|error| error.to_string())
            .collect();
        return if failures.is_empty() {
            Err(error)
        } else {
            Err(io(format!(
                "{error}; recovery errors: {}",
                failures.join(", ")
            )))
        };
    }
    edits
        .into_par_iter()
        .for_each(|edit| tracing::dispatcher::with_default(&dispatch, || edit.finish()));
    Ok(())
}

enum TableEdit<'a> {
    Append(PreparedTableEdit<'a>),
    Replace(StagedTable, &'a mut TableWriteLock),
}

impl TableEdit<'_> {
    fn publish(&mut self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.publish()?),
            Self::Replace(edit, original) => edit.publish(original),
        }
    }

    fn synchronize(&mut self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.synchronize()?),
            Self::Replace(..) => Ok(()),
        }
    }

    fn restore(self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.restore()?),
            Self::Replace(edit, original) if edit.published => {
                if let Err(error) = edit.restore(original) {
                    return Err(io(format!(
                        "{error}; original subtable retained at {}",
                        edit.directory.keep().display()
                    )));
                }
                Ok(())
            }
            Self::Replace(..) => Ok(()),
        }
    }

    fn finish(self) {
        if let Self::Replace(mut edit, original) = self {
            *original = edit.staged.take().expect("replacement table lock");
        }
    }
}

struct StagedTable {
    live: PathBuf,
    private: PathBuf,
    directory: TempDir,
    _nested_locks: Vec<TableWriteLock>,
    staged: Option<TableWriteLock>,
    published: bool,
}

impl StagedTable {
    fn prepare_change(
        live: PathBuf,
        change: &TableRowsChange,
        field: Option<&MetadataPlan<'_>>,
        held: &BTreeMap<PathBuf, TableWriteLock>,
    ) -> MsResult<Self> {
        if !matches!(
            Table::immediate_part_names(&live, &live)?,
            TablePartNames::Plain(_)
        ) {
            return Err(io(format!(
                "{}. Concatenation requires writable physical subtables",
                live.display()
            )));
        }
        let nested_locks = lock_table_tree_for_replacement_with_held(&live, held)?;
        let directory = tempfile::Builder::new()
            .prefix(".casa-rs-ms-concat-")
            .tempdir_in(live.parent().expect("physical table parent"))
            .map_err(io)?;
        let private = directory.path().join("table");
        copy_locked_table_tree(&live, &private).map_err(io)?;
        require_owned_payload(&private)?;
        let mut staged = Self {
            live,
            private,
            directory,
            _nested_locks: nested_locks,
            staged: None,
            published: false,
        };
        let mut lock = TableWriteLock::acquire_to_reread(&staged.private, 1)?;
        lock.record_write();
        staged.write(change, field)?;
        lock.release()?;
        staged.lock_staged()?;
        Ok(staged)
    }

    fn write(&self, change: &TableRowsChange, field: Option<&MetadataPlan<'_>>) -> MsResult<()> {
        let mut table = Table::open(TableOptions::new(&self.private))?;
        if let Some(plan) = field {
            if plan.add_ephemeris_column {
                table.add_columns_in_place(
                    vec![ColumnAddition {
                        schema: ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32)
                            .with_comment("Ephemeris id, pointer to EPHEMERIS table"),
                        default: Some(Value::Scalar(ScalarValue::Int32(-1))),
                        keywords: RecordValue::default(),
                        manager_group: None,
                        manager_type: None,
                    }],
                    None,
                )?;
            }
            for (source, name) in &plan.ephemerides {
                let source = source.canonicalize().map_err(io)?;
                let _reader = casa_ms::held_subtable_reader(&source)?;
                copy_locked_table_tree(&source, &self.private.join(name)).map_err(io)?;
            }
        }
        for (row, record) in &change.updated {
            for field in record.fields() {
                let value = table.cell_accessor(*row, &field.name)?.value()?;
                if value != Some(&field.value) {
                    table
                        .cell_accessor_mut(*row, &field.name)?
                        .set(field.value.clone())?;
                }
            }
        }
        if !change.updated.is_empty() {
            table.flush()?;
            table = Table::open(TableOptions::new(&self.private))?;
        }
        let rows = matching_fields(&table, &change.appended);
        table.append_records_and_close(&rows)?;
        if !change.removed.is_empty() {
            let mut table = Table::open(TableOptions::new(&self.private))?;
            let kept: Vec<_> = (0..table.row_count())
                .filter(|index| change.removed.binary_search(index).is_err())
                .collect();
            table.retain_rows_in_place(&kept)?;
        }
        Ok(())
    }

    fn lock_staged(&mut self) -> MsResult<()> {
        self.staged = Some(TableWriteLock::acquire_to_reread(&self.private, 1)?);
        Ok(())
    }

    fn publish(&mut self, original: &TableWriteLock) -> MsResult<()> {
        if !original.matches_table_path(&self.live) {
            return Err(io(format!(
                "{} changed during concatenation",
                self.live.display()
            )));
        }
        exchange_directories(&self.private, &self.live).map_err(io)?;
        self.published = true;
        if !original.matches_table_path(&self.private)
            || !self
                .staged
                .as_ref()
                .expect("staged table lock")
                .matches_table_path(&self.live)
        {
            return Err(io(format!(
                "{} changed at publication",
                self.live.display()
            )));
        }
        self.staged
            .as_mut()
            .expect("staged table lock")
            .follow_directory_rename(&self.live)?;
        Ok(())
    }

    fn restore(&self, original: &TableWriteLock) -> MsResult<()> {
        if !original.matches_table_path(&self.private)
            || !self
                .staged
                .as_ref()
                .expect("staged table lock")
                .matches_table_path(&self.live)
        {
            return Err(io(format!(
                "{} changed after publication",
                self.live.display()
            )));
        }
        exchange_directories(&self.private, &self.live).map_err(io)
    }
}

fn matching_fields<'a>(table: &Table, records: &'a [RecordValue]) -> Cow<'a, [RecordValue]> {
    let schema = table.schema().expect("MeasurementSet subtable schema");
    if records
        .iter()
        .flat_map(RecordValue::fields)
        .all(|field| schema.contains_column(&field.name))
    {
        return Cow::Borrowed(records);
    }
    Cow::Owned(
        records
            .iter()
            .map(|row| {
                RecordValue::new(
                    row.fields()
                        .iter()
                        .filter(|field| schema.contains_column(&field.name))
                        .cloned()
                        .collect(),
                )
            })
            .collect(),
    )
}

fn require_owned_payload(path: &Path) -> MsResult<()> {
    for entry in fs::read_dir(path).map_err(io)? {
        let entry = entry.map_err(io)?;
        let kind = entry.file_type().map_err(io)?;
        if kind.is_symlink() {
            return Err(io(format!(
                "subtable payload {} is symbolic",
                entry.path().display()
            )));
        }
        if kind.is_dir() {
            require_owned_payload(&entry.path())?;
        }
    }
    Ok(())
}

pub(super) fn io(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
