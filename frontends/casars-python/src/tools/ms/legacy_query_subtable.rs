// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve and publish a legacy TaQL result named through a table keyword.

use std::borrow::Borrow;
use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, Value};
use casa_ms::MeasurementSet;
use casa_tables::publication::check_write_access;
use casa_tables::{Table, TableOptions, TablePartNames, TableWriteLock, add_directory};
use pyo3::prelude::*;

use super::metadata_link::same_directory;
use super::runtime_error;

pub(super) struct QuerySubtable {
    pub(super) output: PathBuf,
    pub(super) owner: PathBuf,
    pub(super) keyword: String,
    parent_directory: Metadata,
    owner_directory: Metadata,
}

impl QuerySubtable {
    pub(super) fn resolve(
        name: &str,
        active: Option<&Path>,
        source: &Path,
    ) -> PyResult<Option<Self>> {
        let Some((parent, keyword)) = split_destination(name)? else {
            return Ok(None);
        };
        let mut chain = parent.split("::").peekable();
        let first = chain.next().expect("split returns a first component");
        if active.is_none() && first == "." && chain.peek().is_none() {
            return Err(runtime_error(
                "the active TaQL view has no persisted directory",
            ));
        }
        let (start, column) = first_parent(first, active.unwrap_or(source))?;
        let (parent, _) = follow_parent_keywords(&start, column, chain, open_parent)?;
        let owner = keyword_owner(&parent)?;
        let output = parent.join(keyword);
        let parent_directory = fs::metadata(&parent).map_err(runtime_error)?;
        let owner_directory = fs::metadata(&owner).map_err(runtime_error)?;
        validate_keyword(&open(&owner)?, keyword)?;
        check_write_access(&owner.join("table.dat")).map_err(runtime_error)?;
        Ok(Some(Self {
            output,
            owner,
            keyword: keyword.into(),
            parent_directory,
            owner_directory,
        }))
    }

    pub(super) fn has_implicit_parent(name: &str) -> PyResult<bool> {
        Ok(split_destination(name)?.is_some_and(|(parent, _)| {
            let first = parent.split("::").next().expect("parent component");
            implicit_parent_column(first).is_some()
        }))
    }

    pub(super) fn validate_memory_output(
        name: &str,
        active: Option<&Path>,
        source: &Path,
        memory: Option<&MeasurementSet>,
    ) -> PyResult<()> {
        let Some((parent, keyword)) = split_destination(name)? else {
            return Ok(());
        };
        let mut chain = parent.split("::").peekable();
        let first = chain.next().expect("parent component");
        if let (None, Some(memory), Some(column)) = (active, memory, implicit_parent_column(first))
        {
            let (_, table) = follow_parent_keywords(Path::new(""), column, chain, |relative| {
                let table = memory
                    .detached_keyword_table(relative)
                    .ok_or_else(|| runtime_error("unknown TaQL parent subtable keyword"))?;
                Ok((relative.to_owned(), table))
            })?;
            return validate_keyword(table, keyword);
        }
        let (start, column) = first_parent(first, active.unwrap_or(source))?;
        let (_, table) = follow_parent_keywords(&start, column, chain, open_parent)?;
        validate_keyword(&table, keyword)
    }

    pub(super) fn verify_parent(&self) -> PyResult<()> {
        let parent = self
            .output
            .parent()
            .expect("subtable has a parent directory");
        for (path, expected) in [
            (parent, &self.parent_directory),
            (&self.owner, &self.owner_directory),
        ] {
            let current = fs::metadata(path).map_err(runtime_error)?;
            if !same_directory(expected, &current) {
                return Err(runtime_error("TaQL result keyword parent was replaced"));
            }
        }
        Ok(())
    }

    pub(super) fn verify_lock(&self, lock: &TableWriteLock) -> PyResult<()> {
        self.verify_parent()?;
        if !lock.matches_table_path(&self.owner) {
            return Err(runtime_error("TaQL result keyword parent was replaced"));
        }
        Ok(())
    }

    pub(super) fn write_link(&self, lock: &mut TableWriteLock) -> PyResult<()> {
        self.verify_lock(lock)?;
        let mut table = open(&self.owner)?;
        validate_keyword(&table, &self.keyword)?;
        table.keywords_mut().upsert(
            &self.keyword,
            Value::table_ref(self.output.to_string_lossy()),
        );
        lock.record_write();
        table
            .save_keywords_atomically(&self.owner)
            .map_err(runtime_error)?;
        crate::tools::table::notify_keyword_change(&self.owner);
        Ok(())
    }
}

fn split_destination(name: &str) -> PyResult<Option<(&str, &str)>> {
    let Some((parent, keyword)) = name.rsplit_once("::") else {
        return Ok(None);
    };
    if !matches!(Path::new(keyword).file_name(), Some(component) if component == keyword)
        || keyword.contains('/')
        || parent.is_empty()
    {
        return Err(runtime_error("invalid TaQL output subtable name"));
    }
    Ok(Some((parent, keyword)))
}

fn implicit_parent_column(first: &str) -> Option<Option<&str>> {
    if matches!(first, "" | ".") {
        return Some(None);
    }
    let column = first.strip_prefix('.').filter(|name| !name.contains('/'))?;
    if open(Path::new(first)).is_ok() {
        return None;
    }
    Some(Some(column))
}

fn open_parent(path: &Path) -> PyResult<(PathBuf, Table)> {
    let path = path.canonicalize().map_err(runtime_error)?;
    let table = open(&path)?;
    Ok((path, table))
}

fn follow_parent_keywords<'a, T: Borrow<Table>>(
    start: &Path,
    mut column: Option<&'a str>,
    mut chain: std::iter::Peekable<impl Iterator<Item = &'a str>>,
    mut open_table: impl FnMut(&Path) -> PyResult<(PathBuf, T)>,
) -> PyResult<(PathBuf, T)> {
    let (mut parent, mut table) = open_table(start)?;
    while let Some(part) = chain.next() {
        if part == "." {
            continue;
        }
        let (record, fields) = match column.take() {
            Some(column) => (
                table
                    .borrow()
                    .column_keywords(column)
                    .ok_or_else(|| runtime_error(format!("unknown TaQL parent column {column}")))?,
                part,
            ),
            None => match (table.borrow().column_keywords(part), chain.peek()) {
                (Some(column), Some(_)) => (column, chain.next().expect("checked next keyword")),
                _ => (table.borrow().keywords(), part),
            },
        };
        let linked = subtable_keyword(record, fields)?;
        (parent, table) = open_table(&add_directory(linked, &parent))?;
    }
    if column.is_some() {
        return Err(runtime_error("TaQL parent column requires a table keyword"));
    }
    Ok((parent, table))
}

fn open(path: &Path) -> PyResult<Table> {
    Table::open(TableOptions::new(path)).map_err(runtime_error)
}

fn first_parent<'a>(first: &'a str, active: &Path) -> PyResult<(PathBuf, Option<&'a str>)> {
    if matches!(first, "" | ".") {
        return Ok((active.to_owned(), None));
    }
    let path = Path::new(first).canonicalize();
    if let Ok(path) = &path
        && open(path).is_ok()
    {
        return Ok((path.clone(), None));
    }
    if let Some(column) = first.strip_prefix('.').filter(|name| !name.contains('/')) {
        return Ok((active.to_owned(), Some(column)));
    }
    Ok((path.map_err(runtime_error)?, None))
}

fn keyword_owner(parent: &Path) -> PyResult<PathBuf> {
    let path = Table::keyword_descriptor_path(parent).map_err(runtime_error)?;
    if matches!(
        Table::immediate_part_names(&path, &path).map_err(runtime_error)?,
        TablePartNames::Plain(_)
    ) {
        Ok(path)
    } else {
        Err(runtime_error(
            "a concatenation cannot persist TaQL result keywords",
        ))
    }
}

fn subtable_keyword<'a>(mut record: &'a RecordValue, fields: &str) -> PyResult<&'a str> {
    let mut fields = fields.split('.').peekable();
    while let Some(field) = fields.next() {
        match record.get(field) {
            Some(Value::TableRef(link)) if fields.peek().is_none() => return Ok(link),
            Some(Value::Record(nested)) => record = nested,
            _ => break,
        }
    }
    Err(runtime_error("unknown TaQL parent subtable keyword"))
}

fn validate_keyword(table: &Table, keyword: &str) -> PyResult<()> {
    if table
        .keywords()
        .get(keyword)
        .is_some_and(|value| !matches!(value, Value::TableRef(_)))
    {
        return Err(runtime_error(format!(
            "incorrect data type used for TaQL result keyword {keyword}"
        )));
    }
    Ok(())
}
