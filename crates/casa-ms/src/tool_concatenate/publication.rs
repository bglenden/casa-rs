// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish concatenation's small table updates together.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{
    ColumnAddition, ColumnSchema, PreparedRecordAppend, Table, TableOptions, TablePartNames,
    TableWriteLock,
    publication::{
        check_write_access, copy_locked_table_tree, exchange_directories,
        lock_table_tree_for_replacement,
    },
};
use casa_types::{PrimitiveType, RecordValue, ScalarValue, Value};
use rayon::prelude::*;
use tempfile::TempDir;

use super::{MetadataPlan, SubtableChange, TestConcatenation, required_table};
use crate::tool_history::history_rows;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

pub(super) fn metadata_only(
    target: &Path,
    donor: &Path,
    request: &TestConcatenation<'_>,
) -> MsResult<()> {
    let target = target.canonicalize().map_err(io)?;
    let donor = donor.canonicalize().map_err(io)?;
    if target.starts_with(&donor) || donor.starts_with(&target) {
        return Err(MsError::InvalidInput(
            "concatenation requires separate input MeasurementSets".into(),
        ));
    }
    let mut roots = [&target, &donor];
    roots.sort_unstable();
    let mut held = Vec::with_capacity(roots.len());
    for path in roots {
        check_write_access(&path.join("table.dat")).map_err(io)?;
        held.push(TableWriteLock::acquire_to_reread(path, 1)?);
    }
    let (attached, incoming) = rayon::join(
        || MeasurementSet::open_concatenation_metadata(&target, false),
        || MeasurementSet::open_concatenation_metadata(&donor, true),
    );
    let attached = attached?;
    let incoming = incoming?;
    super::layout::check(&attached, &incoming)?;
    let plan = MetadataPlan::reconcile(
        &attached,
        &incoming,
        request.frequency_hz,
        request.direction_rad,
        request.respect_name,
    )?;
    let mut paths = BTreeSet::new();
    let targets = plan
        .changes
        .iter()
        .map(|change| {
            let table = required_table(&attached, change.table)?;
            let live = check_target(table, &mut paths)?;
            Ok((change, table, live))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let history = required_table(&incoming, SubtableId::History)?;
    let history_path = check_target(history, &mut paths)?;
    let (edits, history) = rayon::join(
        || {
            targets
                .into_par_iter()
                .map(|(change, table, live)| prepare_change(change, table, live, &plan))
                .collect::<MsResult<Vec<_>>>()
        },
        || prepare_history(history, &history_path, request),
    );
    let mut edits = edits?;
    edits.push(history?);
    for (path, records) in &plan.calibration {
        let table = Table::open(TableOptions::new(path))?;
        let live = check_target(&table, &mut paths)?;
        let lock = TableWriteLock::acquire_to_reread(&live, 1)?;
        let records = matching_fields(&table, records);
        edits.push(TableEdit::Append(
            table.prepare_record_append(&records, lock)?,
        ));
    }
    let publication = edits
        .par_iter_mut()
        .map(TableEdit::publish)
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<MsResult<()>>();
    if let Err(error) = publication {
        let failures = edits
            .into_iter()
            .rev()
            .filter_map(|edit| edit.restore().err());
        let failures: Vec<_> = failures.map(|error| error.to_string()).collect();
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
        .map(TableEdit::finish)
        .collect::<Vec<_>>()
        .into_iter()
        .collect()
}

fn prepare_history(
    table: &Table,
    path: &Path,
    request: &TestConcatenation<'_>,
) -> MsResult<TableEdit> {
    let lock = TableWriteLock::acquire_to_reread(path, 1)?;
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
    let logs = history_rows(table, &[message], &command, "ms::testconcatenate()", "ms")?;
    Ok(TableEdit::Append(table.prepare_record_append(&logs, lock)?))
}

fn prepare_change(
    change: &SubtableChange,
    table: &Table,
    live: PathBuf,
    plan: &MetadataPlan,
) -> MsResult<TableEdit> {
    let requires_copy = !change.updated.is_empty()
        || !change.removed.is_empty()
        || (change.table == SubtableId::Field
            && (plan.add_ephemeris_column || !plan.ephemerides.is_empty()));
    if requires_copy {
        let mut staged = StagedTable::prepare(live)?;
        let mut lock = TableWriteLock::acquire_to_reread(&staged.private, 1)?;
        lock.record_write();
        staged.write(change, plan)?;
        lock.release()?;
        staged.lock_staged()?;
        Ok(TableEdit::Replace(staged))
    } else {
        let lock = TableWriteLock::acquire_to_reread(&live, 1)?;
        let records = matching_fields(table, &change.appended);
        Ok(TableEdit::Append(
            table.prepare_record_append(&records, lock)?,
        ))
    }
}

fn check_target(table: &Table, paths: &mut BTreeSet<PathBuf>) -> MsResult<PathBuf> {
    let live = table
        .path()
        .ok_or_else(|| io("a subtable has no path"))?
        .canonicalize()
        .map_err(io)?;
    if !paths.insert(live.clone()) {
        return Err(io(format!("several changes share {}", live.display())));
    }
    check_write_access(&live.join("table.dat")).map_err(io)?;
    Ok(live)
}

enum TableEdit {
    Append(PreparedRecordAppend),
    Replace(StagedTable),
}

impl TableEdit {
    fn publish(&mut self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.publish()?),
            Self::Replace(edit) => edit.publish(),
        }
    }

    fn restore(self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.restore()?),
            Self::Replace(edit) if edit.published => {
                if let Err(error) = edit.restore() {
                    return Err(io(format!(
                        "{error}; original subtable retained at {}",
                        edit.directory.keep().display()
                    )));
                }
                Ok(())
            }
            Self::Replace(_) => Ok(()),
        }
    }

    fn finish(self) -> MsResult<()> {
        match self {
            Self::Append(append) => Ok(append.finish()?),
            Self::Replace(_) => Ok(()),
        }
    }
}

struct StagedTable {
    live: PathBuf,
    private: PathBuf,
    directory: TempDir,
    original: Vec<TableWriteLock>,
    staged: Option<TableWriteLock>,
    published: bool,
}

impl StagedTable {
    fn prepare(live: PathBuf) -> MsResult<Self> {
        if !matches!(
            Table::immediate_part_names(&live, &live)?,
            TablePartNames::Plain(_)
        ) {
            return Err(io(format!(
                "{}. Concatenation requires writable physical subtables",
                live.display()
            )));
        }
        let original = lock_table_tree_for_replacement(&live)?;
        let directory = tempfile::Builder::new()
            .prefix(".casa-rs-ms-concat-")
            .tempdir_in(live.parent().expect("physical table parent"))
            .map_err(io)?;
        let private = directory.path().join("table");
        copy_locked_table_tree(&live, &private).map_err(io)?;
        require_owned_payload(&private)?;
        Ok(Self {
            live,
            private,
            directory,
            original,
            staged: None,
            published: false,
        })
    }

    fn write(&self, change: &SubtableChange, plan: &MetadataPlan) -> MsResult<()> {
        let mut table = Table::open(TableOptions::new(&self.private))?;
        if change.table == SubtableId::Field {
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
                let _locks = lock_table_tree_for_replacement(&source)?;
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

    fn publish(&mut self) -> MsResult<()> {
        if !self.original[0].matches_table_path(&self.live) {
            return Err(io(format!(
                "{} changed during concatenation",
                self.live.display()
            )));
        }
        exchange_directories(&self.private, &self.live).map_err(io)?;
        self.published = true;
        if !self.original[0].matches_table_path(&self.private)
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
        Ok(())
    }

    fn restore(&self) -> MsResult<()> {
        if !self
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

fn io(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
