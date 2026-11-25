// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish concatenation's small table updates together.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{
    ColumnAddition, ColumnSchema, Table, TableOptions, TablePartNames, TableWriteLock,
    publication::{
        check_write_access, copy_locked_table_tree, exchange_directories,
        lock_table_tree_for_replacement,
    },
};
use casa_types::{PrimitiveType, RecordValue, ScalarValue, Value};
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
    let attached = MeasurementSet::open(&target)?;
    let incoming = MeasurementSet::open(&donor)?;
    let plan = MetadataPlan::reconcile(
        &attached,
        &incoming,
        request.frequency_hz,
        request.direction_rad,
        request.respect_name,
    )?;
    let mut paths = BTreeSet::new();
    let mut edits = Vec::with_capacity(plan.changes.len() + 1);
    for change in &plan.changes {
        let table = required_table(&attached, change.table)?;
        let edit = StagedTable::prepare(table, &mut paths)?;
        let mut lock = TableWriteLock::acquire_to_reread(&edit.private, 1)?;
        lock.record_write();
        edit.write(change, &plan)?;
        lock.release()?;
        edits.push(edit);
    }
    let history = required_table(&incoming, SubtableId::History)?;
    let message = format!(
        "Subtables from {} appended to those from {}",
        request.donor_name,
        target.display()
    );
    let command = format!(
        "msfile= {} freqTol='{}' dirTol='{}'' respectname='{}",
        request.donor_name,
        request.frequency_text,
        request.direction_text,
        u8::from(request.respect_name),
    );
    let history = StagedTable::prepare(history, &mut paths)?;
    let mut history_lock = TableWriteLock::acquire_to_reread(&history.private, 1)?;
    let table = Table::open(TableOptions::new(&history.private))?;
    let logs = history_rows(&table, &[message], &command, "ms::testconcatenate()", "ms")?;
    history_lock.record_write();
    table.append_records_and_close(&logs)?;
    history_lock.release()?;
    edits.push(history);
    for edit in &mut edits {
        edit.lock_staged()?;
    }
    let result = edits.iter_mut().try_for_each(StagedTable::publish);
    if let Err(error) = result {
        let mut recovery = Vec::new();
        for edit in edits.into_iter().rev() {
            if edit.published && edit.restore().is_err() {
                recovery.push(edit.directory.keep());
            }
        }
        if !recovery.is_empty() {
            return Err(MsError::InvalidInput(format!(
                "{error}; original subtables retained at {recovery:?}"
            )));
        }
        return Err(error);
    }
    drop(held);
    Ok(())
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
    fn prepare(table: &Table, paths: &mut BTreeSet<PathBuf>) -> MsResult<Self> {
        let live = table
            .path()
            .ok_or_else(|| MsError::InvalidInput("a subtable has no path".into()))?
            .canonicalize()
            .map_err(io)?;
        if !paths.insert(live.clone()) {
            return Err(MsError::InvalidInput(format!(
                "several changes share {}",
                live.display()
            )));
        }
        if !matches!(
            Table::immediate_part_names(&live, &live)?,
            TablePartNames::Plain(_)
        ) {
            return Err(MsError::InvalidInput(format!(
                "{}. Concatenation requires writable physical subtables",
                live.display()
            )));
        }
        check_write_access(&live.join("table.dat")).map_err(io)?;
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
        let schema = table.schema().expect("MeasurementSet subtable schema");
        let rows = change
            .appended
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
            .collect::<Vec<_>>();
        table.append_records_and_close(&rows)?;
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
