// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent in-memory MeasurementSets and persistence of their selections.

use std::collections::{BTreeMap, BTreeSet};

use casa_tables::{EndianFormat, add_directory};

use super::*;

impl MeasurementSet {
    /// Create an in-memory MeasurementSet from a builder.
    /// The tables have the declared schemas and no rows, and MAIN has its
    /// `MS_VERSION` keyword.
    pub fn create_memory(builder: MeasurementSetBuilder) -> MsResult<Self> {
        let schemas = builder.build_schemas()?;
        Self::from_schemas(schemas, None)
    }

    /// Detach the selected MAIN rows and this MeasurementSet's subtables
    /// from their storage. Main rows retain the given order and duplicates.
    /// # Errors
    /// A selected row or subtable cannot be read from the source.
    pub fn to_memory_rows(&self, rows: &[usize]) -> MsResult<Self> {
        self.to_memory_rows_with_read_locks(rows, &BTreeSet::new())
    }

    /// Detach a selection while reusing read or write locks the caller holds.
    #[doc(hidden)]
    pub fn to_memory_rows_with_read_locks(
        &self,
        rows: &[usize],
        held: &BTreeSet<PathBuf>,
    ) -> MsResult<Self> {
        let main_source = open_snapshot_source(&self.main, held)?;
        let mut main = main_source
            .as_ref()
            .unwrap_or(&self.main)
            .to_memory_rows(rows)?;
        let mut subtables = self
            .subtables
            .iter()
            .map(|(&id, table)| {
                let locked = open_snapshot_source(table, held)?;
                let copy = locked.as_ref().unwrap_or(table).to_memory()?;
                Ok((id, copy))
            })
            .collect::<MsResult<HashMap<_, _>>>()?;
        let detached_keyword_tables = if let Some(path) = &self.path {
            self.detach_keyword_tables(path, &mut main, &mut subtables, held)?
        } else {
            self.detached_keyword_tables
                .iter()
                .map(|(path, table)| table.to_memory().map(|copy| (path.clone(), copy)))
                .collect::<Result<BTreeMap<_, _>, _>>()?
        };
        Ok(Self {
            main,
            subtables,
            subtable_paths: HashMap::new(),
            detached_keyword_tables,
            path: None,
        })
    }

    /// Materialize a detached MeasurementSet in the caller's private stage.
    /// Keep the table and column keywords from the captured snapshot.
    /// # Errors
    /// The destination or a table in the result cannot be written.
    #[doc(hidden)]
    pub fn save_detached_in_stage(&mut self, path: &Path, endian: EndianFormat) -> MsResult<()> {
        let options = measurement_set_table_options(path).with_endian_format(endian);
        let bindings = measurement_set_main_table_bindings(&self.main);
        self.main
            .prepare_write()
            .save_with_bindings(options, &bindings)?;
        for (&id, table) in &self.subtables {
            table.save(measurement_set_table_options(&path.join(id.name())))?;
        }
        for (relative, table) in &self.detached_keyword_tables {
            table.save(measurement_set_table_options(&path.join(relative)))?;
        }
        Ok(())
    }

    /// Access a table in the detached MeasurementSet by its relative name.
    #[doc(hidden)]
    pub fn detached_keyword_table(&self, relative: &Path) -> Option<&Table> {
        if relative.as_os_str().is_empty() {
            return Some(&self.main);
        }
        self.detached_keyword_tables.get(relative).or_else(|| {
            self.subtables
                .iter()
                .find(|(id, _)| Path::new(id.name()) == relative)
                .map(|(_, table)| table)
        })
    }

    fn detach_keyword_tables(
        &self,
        path: &Path,
        main: &mut Table,
        subtables: &mut HashMap<SubtableId, Table>,
        held: &BTreeSet<PathBuf>,
    ) -> MsResult<BTreeMap<PathBuf, Table>> {
        let mut links = KeywordTableCopies {
            sources: BTreeMap::new(),
            tables: BTreeMap::new(),
            active: Vec::new(),
            held,
        };
        for (&id, source) in &self.subtables {
            if let Some(source_path) = source.path() {
                let relative = PathBuf::from(id.name());
                links.sources.insert(relative, canonical(source_path)?);
            }
        }
        links.rewrite_keywords(main, path, Path::new(""), &|name| {
            subtables.keys().any(|id| id.name() == name)
        })?;
        for (&id, copy) in subtables.iter_mut() {
            main.keywords_mut()
                .upsert(id.name(), Value::table_ref(format!("././{}", id.name())));
            if let Some(source) = self.subtables.get(&id).and_then(Table::path) {
                links.rewrite_keywords(copy, source, Path::new(id.name()), &|_| false)?;
            }
        }
        Ok(links.tables)
    }
}

struct KeywordTableCopies<'a> {
    sources: BTreeMap<PathBuf, PathBuf>,
    tables: BTreeMap<PathBuf, Table>,
    active: Vec<PathBuf>,
    held: &'a BTreeSet<PathBuf>,
}

impl KeywordTableCopies<'_> {
    fn rewrite_keywords(
        &mut self,
        copy: &mut Table,
        source: &Path,
        relative: &Path,
        skip: &dyn Fn(&str) -> bool,
    ) -> MsResult<()> {
        let columns = copy
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
            .filter_map(|column| {
                let name = column.name();
                let keywords = copy.column_keywords(name)?;
                keywords
                    .fields()
                    .iter()
                    .any(|field| contains_table_link(&field.value))
                    .then(|| name.to_owned())
            })
            .collect::<Vec<_>>();
        if columns.is_empty()
            && !copy
                .keywords()
                .fields()
                .iter()
                .any(|field| !skip(&field.name) && contains_table_link(&field.value))
        {
            return Ok(());
        }
        let owner = Table::keyword_descriptor_path(source)?;
        self.active.push(owner.clone());
        self.rewrite(copy.keywords_mut(), source, &owner, relative, skip)?;
        for name in columns {
            if let Some(mut keywords) = copy.column_keywords(&name).cloned() {
                self.rewrite(&mut keywords, source, &owner, relative, &|_| false)?;
                copy.set_column_keywords(&name, keywords);
            }
        }
        self.active.pop();
        Ok(())
    }

    fn rewrite(
        &mut self,
        keys: &mut RecordValue,
        source: &Path,
        owner: &Path,
        relative: &Path,
        skip: &dyn Fn(&str) -> bool,
    ) -> MsResult<()> {
        let mut omit = Vec::new();
        for field in keys.fields_mut() {
            if skip(&field.name) {
                continue;
            }
            let link = match &mut field.value {
                Value::TableRef(link) => link,
                Value::Record(nested) => {
                    self.rewrite(nested, source, owner, relative, &|_| false)?;
                    continue;
                }
                _ => continue,
            };
            let declared = add_directory(link, source);
            let linked = canonical(&declared)?;
            let linked_root = Table::keyword_descriptor_path(&linked)?;
            if linked_root == owner {
                omit.push(field.name.clone());
                continue;
            }
            let name = declared
                .file_name()
                .ok_or_else(|| MsError::InvalidInput("linked table has no name".into()))?;
            let output = relative.join(name);
            if let Some(previous) = self.sources.get(&output) {
                if previous != &linked {
                    return Err(MsError::InvalidInput(format!(
                        "different linked tables use {}",
                        output.display()
                    )));
                }
            } else {
                self.copy_link(&linked, &linked_root, &output)?;
            }
            *link = format!("././{}", name.to_string_lossy());
        }
        for name in omit {
            keys.remove(&name);
        }
        Ok(())
    }

    fn copy_link(&mut self, linked: &Path, root: &Path, output: &Path) -> MsResult<()> {
        if self.active.iter().any(|path| path == root) {
            return Err(MsError::InvalidInput(format!(
                "cyclic table keyword at {}",
                linked.display()
            )));
        }
        let table = open_snapshot_path(linked, self.held)?;
        let mut copy = table.to_memory()?;
        self.rewrite_keywords(&mut copy, linked, output, &|_| false)?;
        self.sources.insert(output.to_owned(), linked.to_owned());
        self.tables.insert(output.to_owned(), copy);
        Ok(())
    }
}

fn contains_table_link(value: &Value) -> bool {
    match value {
        Value::TableRef(_) => true,
        Value::Record(record) => record
            .fields()
            .iter()
            .any(|field| contains_table_link(&field.value)),
        _ => false,
    }
}

fn open_snapshot_source(table: &Table, held: &BTreeSet<PathBuf>) -> MsResult<Option<Table>> {
    let Some(path) = table.path() else {
        return Ok(None);
    };
    let canonical = canonical(path)?;
    if held.contains(&canonical) {
        return Ok(None);
    }
    open_snapshot_path(path, held).map(Some)
}

fn open_snapshot_path(path: &Path, held: &BTreeSet<PathBuf>) -> MsResult<Table> {
    #[cfg(unix)]
    if !held.contains(&canonical(path)?) {
        return Ok(Table::open_with_lock(
            TableOptions::new(path),
            casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait)
                .read_only(true),
        )?);
    }
    #[cfg(not(unix))]
    let _ = held;
    Ok(Table::open(TableOptions::new(path))?)
}

fn canonical(path: &Path) -> MsResult<PathBuf> {
    path.canonicalize()
        .map_err(|error| MsError::InvalidInput(format!("linked table {}: {error}", path.display())))
}
