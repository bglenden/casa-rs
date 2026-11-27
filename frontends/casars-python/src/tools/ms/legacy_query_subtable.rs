// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve and publish a legacy TaQL result named through a table keyword.

use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, Value};
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
    pub(super) fn resolve(name: &str, active: &Path) -> PyResult<Option<Self>> {
        let Some((parent, keyword)) = name.rsplit_once("::") else {
            return Ok(None);
        };
        if !matches!(Path::new(keyword).file_name(), Some(component) if component == keyword)
            || keyword.contains('/')
            || parent.is_empty()
        {
            return Err(runtime_error("invalid TaQL output subtable name"));
        }
        let mut chain = parent.split("::").peekable();
        let first = chain.next().expect("split returns a first component");
        let (mut parent, mut column) = first_parent(first, active)?;
        let mut table = open(&parent)?;
        while let Some(part) = chain.next() {
            if part == "." {
                continue;
            }
            let (record, fields) = match column.take() {
                Some(column) => (
                    table.column_keywords(column).ok_or_else(|| {
                        runtime_error(format!("unknown TaQL parent column {column}"))
                    })?,
                    part,
                ),
                None => match (table.column_keywords(part), chain.peek()) {
                    (Some(column), Some(_)) => {
                        (column, chain.next().expect("checked next keyword"))
                    }
                    _ => (table.keywords(), part),
                },
            };
            let linked = subtable_keyword(record, fields)?;
            parent = add_directory(linked, &parent)
                .canonicalize()
                .map_err(runtime_error)?;
            table = open(&parent)?;
        }
        if column.is_some() {
            return Err(runtime_error("TaQL parent column requires a table keyword"));
        }
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

    pub(super) fn publish_link(&self, retained: Option<&mut TableWriteLock>) -> PyResult<()> {
        if let Some(lock) = retained {
            return self.write_link(lock);
        }
        let mut lock = TableWriteLock::acquire_to_reread(&self.owner, 1).map_err(runtime_error)?;
        self.write_link(&mut lock)
    }

    fn write_link(&self, lock: &mut TableWriteLock) -> PyResult<()> {
        self.verify_parent()?;
        if !lock.matches_table_path(&self.owner) {
            return Err(runtime_error("TaQL result keyword parent was replaced"));
        }
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

fn open(path: &Path) -> PyResult<Table> {
    Table::open(TableOptions::new(path)).map_err(runtime_error)
}

fn first_parent<'a>(first: &'a str, active: &Path) -> PyResult<(PathBuf, Option<&'a str>)> {
    if first == "." {
        return Ok((active.to_owned(), None));
    }
    match Path::new(first).canonicalize() {
        Ok(path) => Ok((path, None)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => first
            .strip_prefix('.')
            .filter(|name| !name.is_empty() && !name.contains('/'))
            .map(|column| (active.to_owned(), Some(column)))
            .ok_or_else(|| runtime_error(error)),
        Err(error) => Err(runtime_error(error)),
    }
}

fn keyword_owner(parent: &Path) -> PyResult<PathBuf> {
    let mut path = parent.to_owned();
    for _ in 0..64 {
        match Table::immediate_part_names(&path, &path).map_err(runtime_error)? {
            TablePartNames::Plain(_) => return Ok(path),
            TablePartNames::Reference(source) => {
                path = source.canonicalize().map_err(runtime_error)?
            }
            TablePartNames::Concatenation { .. } => {
                return Err(runtime_error(
                    "a concatenation cannot persist TaQL result keywords",
                ));
            }
        }
    }
    Err(runtime_error(
        "TaQL parent table reference chain is too deep",
    ))
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
