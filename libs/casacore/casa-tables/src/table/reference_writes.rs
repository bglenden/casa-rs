// SPDX-License-Identifier: LGPL-3.0-or-later
//! A saved RefTable's lock and cell storage belong to its base table.

use std::fs::{File, Metadata};

use casa_aipsio::AipsIo;

use super::*;
use crate::TableWriteLock;
use crate::storage::table_control::{read_ref_table_file, write_ref_table_stream};
use crate::storage::{
    RefTableDatContents, TABLE_CONTROL_FILE, TABLE_INFO_FILE, TableDatResult,
    read_table_dat_dispatch,
};
use crate::table_path::rebase_table_keywords;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReferenceRoot {
    LockPath,
    Plain,
    Any,
}

/// Ordered RefTable descriptors from the view to its root. No cells are read
/// and the returned path locates the backing table.
fn reference_chain(
    mut path: PathBuf,
    root: ReferenceRoot,
) -> Result<(PathBuf, Vec<RefTableDatContents>), TableError> {
    let mut seen = std::collections::HashSet::new();
    let mut chain = Vec::new();
    loop {
        let control = path.join(TABLE_CONTROL_FILE);
        match crate::storage::table_control::is_reference_descriptor(&control) {
            // The descriptor of a plain base may be absent or incomplete
            // while its writer holds the lock. Wait at that directory and
            // defer validation to the locked open.
            Ok(false) if root != ReferenceRoot::Plain || chain.is_empty() => {
                return Ok((path, chain));
            }
            Err(_) if root == ReferenceRoot::LockPath => return Ok((path, chain)),
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let real = std::fs::canonicalize(&path).map_err(crate::storage::StorageError::from)?;
        if !seen.insert(real) {
            return Err(TableError::Storage("reference table cycle".into()));
        }
        match read_table_dat_dispatch(&control)? {
            TableDatResult::Ref(reference) => {
                path = crate::table_path::add_directory_for_link(
                    &reference.parent_relative_path,
                    &path,
                );
                chain.push(reference);
            }
            TableDatResult::Plain(_) => return Ok((path, chain)),
            TableDatResult::Concat(_) if chain.is_empty() => return Ok((path, chain)),
            TableDatResult::Concat(_) => {
                return Err(TableError::Storage(
                    "reference base is a concatenated table".into(),
                ));
            }
        }
    }
}

pub(super) fn reference_lock_path(path: &Path) -> Result<PathBuf, TableError> {
    reference_chain(path.to_path_buf(), ReferenceRoot::LockPath).map(|(root, _)| root)
}

fn base_name<'a>(chain: &'a [RefTableDatContents], mut name: &'a str) -> &'a str {
    for view in chain {
        name = view.parent_column(name);
    }
    name
}

fn base_row(chain: &[RefTableDatContents], mut row: usize) -> Result<usize, TableError> {
    for view in chain {
        row = view
            .parent_row(row)
            .ok_or_else(|| TableError::Storage(format!("reference row {row} is out of bounds")))?;
    }
    Ok(row)
}

/// The writable physical cells of selected rows in a saved RefTable.
#[doc(hidden)]
pub struct ReferenceWriteProjection {
    /// The root MAIN directory. For a concatenation, the member locks
    /// protect its payloads.
    pub path: PathBuf,
    /// Root row numbers in the requested logical row order.
    pub rows: Vec<usize>,
    /// Physical column names in the requested logical column order.
    pub columns: Vec<String>,
}

/// Prepared additions to a saved reference's column projection.
#[doc(hidden)]
pub struct ReferenceColumnPublication {
    root: PathBuf,
    layers: Vec<ReferenceColumnLayer>,
    /// Physical destination for each requested logical column, in order.
    pub columns: Vec<String>,
}

struct ReferenceColumnLayer {
    path: PathBuf,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    directory: File,
    _descriptor: File,
    before: Metadata,
    contents: RefTableDatContents,
    changed: bool,
}

impl ReferenceColumnLayer {
    fn open(path: PathBuf) -> Result<Self, TableError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let directory = crate::publication::open_directory_identity(&path)
            .map_err(crate::storage::StorageError::from)?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let descriptor = crate::publication::open_regular_directory_entry(
            &directory,
            std::ffi::OsStr::new(TABLE_CONTROL_FILE),
        )
        .map_err(crate::storage::StorageError::from)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let descriptor = File::open(path.join(TABLE_CONTROL_FILE))
            .map_err(crate::storage::StorageError::from)?;
        let before = descriptor
            .metadata()
            .map_err(crate::storage::StorageError::from)?;
        let contents = read_ref_table_file(
            descriptor
                .try_clone()
                .map_err(crate::storage::StorageError::from)?,
        )?;
        Ok(Self {
            path,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            directory,
            _descriptor: descriptor,
            before,
            contents,
            changed: false,
        })
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn publish(&self) -> Result<(), TableError> {
        use std::os::unix::fs::MetadataExt;
        let io = crate::storage::StorageError::from;
        let captured = self.directory.metadata().map_err(io)?;
        let current = std::fs::metadata(&self.path).map_err(io)?;
        if (captured.dev(), captured.ino()) != (current.dev(), current.ino()) {
            return Err(TableError::Storage(
                "saved reference directory was replaced".into(),
            ));
        }
        let mut staged = crate::publication::StagedDirectoryFile::new(&self.directory, &self.path)
            .map_err(io)?;
        let file = staged.file_mut();
        file.set_permissions(self.before.permissions())
            .map_err(io)?;
        write_ref_table_stream(
            AipsIo::new_read_write(file.try_clone().map_err(io)?),
            &self.contents,
        )?;
        staged
            .replace(std::ffi::OsStr::new(TABLE_CONTROL_FILE), &self.before)
            .map_err(io)?;
        Ok(())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn publish(&self) -> Result<(), TableError> {
        let io = crate::storage::StorageError::from;
        let path = self.path.join(TABLE_CONTROL_FILE);
        let staged = tempfile::NamedTempFile::new_in(&self.path).map_err(io)?;
        staged
            .as_file()
            .set_permissions(self.before.permissions())
            .map_err(io)?;
        write_ref_table_stream(
            AipsIo::new_read_write(staged.as_file().try_clone().map_err(io)?),
            &self.contents,
        )?;
        let current = std::fs::metadata(&path).map_err(io)?;
        if self.before.len() != current.len()
            || self.before.modified().ok() != current.modified().ok()
        {
            return Err(TableError::Storage(
                "saved reference descriptor changed".into(),
            ));
        }
        staged.persist(path).map_err(|error| io(error.error))?;
        Ok(())
    }
}

impl ReferenceColumnPublication {
    /// Publish the extended column projections after their physical columns
    /// have been installed, under the same verified root write lock.
    pub fn publish(self, root: &Table, lock: &TableWriteLock) -> Result<Vec<PathBuf>, TableError> {
        if !lock.matches_table_path(&self.root)
            || !root
                .path()
                .is_some_and(|path| lock.matches_table_path(path))
        {
            return Err(TableError::NotLocked {
                operation: "publish saved reference columns".into(),
            });
        }
        for column in &self.columns {
            root.require_column(column)?;
        }
        let mut published = Vec::new();
        for layer in self.layers.into_iter().rev().filter(|layer| layer.changed) {
            layer.publish()?;
            published.push(layer.path);
        }
        Ok(published)
    }
}

impl Table {
    /// Plan adding same-name bindings to a saved reference where a logical
    /// column is missing. An existing ancestor binding still supplies its
    /// original physical name. The physical MAIN lock covers every view.
    #[doc(hidden)]
    pub fn prepare_reference_columns_holding(
        &self,
        columns: &[&str],
        lock: &TableWriteLock,
    ) -> Result<Option<ReferenceColumnPublication>, TableError> {
        if !self.is_reference_table()? {
            return Ok(None);
        }
        let mut path = self.source_path.clone().expect("saved reference");
        let mut layers = Vec::new();
        let mut seen = HashSet::new();
        while crate::storage::table_control::is_reference_descriptor(
            &path.join(TABLE_CONTROL_FILE),
        )? {
            path = path
                .canonicalize()
                .map_err(crate::storage::StorageError::from)?;
            if !seen.insert(path.clone()) {
                return Err(TableError::Storage("reference table cycle".into()));
            }
            let layer = ReferenceColumnLayer::open(path)?;
            path = crate::table_path::add_directory_for_link(
                &layer.contents.parent_relative_path,
                &layer.path,
            );
            layers.push(layer);
        }
        if !lock.matches_table_path(&path) {
            return Err(TableError::NotLocked {
                operation: "prepare saved reference columns".into(),
            });
        }
        let mut physical = Vec::with_capacity(columns.len());
        for &column in columns {
            let mut name = column.to_owned();
            for layer in &mut layers {
                if layer.contents.column_names.contains(&name) {
                    name = layer.contents.parent_column(&name).to_owned();
                } else {
                    layer.contents.column_names.push(name.clone());
                    layer
                        .contents
                        .column_name_map
                        .push((name.clone(), name.clone()));
                    layer.changed = true;
                }
            }
            physical.push(name);
        }
        Ok(Some(ReferenceColumnPublication {
            root: path,
            layers,
            columns: physical,
        }))
    }

    /// Resolve selected logical rows and columns through their saved references.
    /// Repeated rows remain repeated. The caller must verify the descriptors
    /// after acquiring the returned root's write lock.
    ///
    /// # Errors
    /// A requested cell is outside the view or a descriptor is unreadable.
    #[doc(hidden)]
    pub fn reference_write_projection(
        &self,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<Option<ReferenceWriteProjection>, TableError> {
        self.reference_projection(rows, columns, ReferenceRoot::Plain)
    }

    /// Resolve saved row and column maps through to a plain or concatenated
    /// root. Rows are in logical input order, including any repetitions.
    /// A caller that writes cells must resolve a concatenated root to its
    /// member tables and verify the layout under those members' write locks.
    /// # Errors
    /// A requested cell is outside the view or a descriptor is unreadable.
    #[doc(hidden)]
    pub fn reference_root_projection(
        &self,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<Option<ReferenceWriteProjection>, TableError> {
        self.reference_projection(rows, columns, ReferenceRoot::Any)
    }

    fn reference_projection(
        &self,
        rows: &[usize],
        columns: &[&str],
        root: ReferenceRoot,
    ) -> Result<Option<ReferenceWriteProjection>, TableError> {
        let Some(source) = &self.source_path else {
            return Ok(None);
        };
        if !self.is_reference_table()? {
            return Ok(None);
        }
        let (root, chain) = reference_chain(source.clone(), root)?;
        let rows = rows
            .iter()
            .map(|&row| base_row(&chain, row))
            .collect::<Result<_, _>>()?;
        let columns = columns
            .iter()
            .map(|&column| {
                self.require_column(column)?;
                Ok(base_name(&chain, column).to_owned())
            })
            .collect::<Result<_, TableError>>()?;
        Ok(Some(ReferenceWriteProjection {
            path: root,
            rows,
            columns,
        }))
    }

    pub(crate) fn reference_column_manager_type(
        &self,
        column: &str,
    ) -> Result<Option<String>, TableError> {
        if !self.is_reference_table()? {
            return Ok(None);
        }
        let source = self.source_path.as_ref().expect("saved reference");
        let (root, chain) = reference_chain(source.clone(), ReferenceRoot::Plain)?;
        let root_table = Table::open_metadata_only(TableOptions::new(root))?;
        let name = base_name(&chain, column);
        Ok(root_table
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|column| column == name))
            .map(|manager| manager.dm_type.clone()))
    }

    /// Apply cell changes through the saved row/column projection under
    /// the view's already-held write lock. Return false for plain tables.
    pub(super) fn try_save_reference_cells(
        &mut self,
        columns: &[&str],
        changed_rows: &[usize],
        allow_rewrite: bool,
    ) -> Result<bool, TableError> {
        if !self.is_reference_table()? {
            return Ok(false);
        }
        for column in columns {
            self.require_writable_column(column)?;
        }
        let source = self.source_path.as_ref().expect("saved reference");
        let (root, chain) = reference_chain(source.clone(), ReferenceRoot::Plain)?;
        self.check_reference_base(&root)?;
        let mut base = Table::open(TableOptions::new(&root))?;
        let mut selected_columns: Vec<_> =
            columns.iter().map(|col| base_name(&chain, col)).collect();
        selected_columns.sort_unstable();
        selected_columns.dedup();
        let changed_rows = if changed_rows.is_empty() {
            (0..self.row_count()).collect::<Vec<_>>()
        } else {
            changed_rows.to_vec()
        };
        let mut selected_rows = Vec::new();
        for &row in &changed_rows {
            let index = base_row(&chain, row)?;
            for &column in columns {
                let name = base_name(&chain, column);
                let descriptor = self
                    .schema()
                    .and_then(|schema| schema.column(column))
                    .ok_or_else(|| TableError::SchemaColumnUnknown {
                        column: column.to_string(),
                    })?;
                // Mutated scalar/array cells are in the overlay. Read
                // those directly instead of loading the parent rows.
                match descriptor.column_type() {
                    crate::ColumnType::Scalar => match self.get_scalar_cell(row, column) {
                        Ok(value) => base.write_scalar_cell_bound(index, name, value.clone())?,
                        Err(TableError::ColumnNotFound { .. }) => {}
                        Err(error) => return Err(error),
                    },
                    crate::ColumnType::Array(_) => match self.get_array_cell(row, column) {
                        Ok(value) => base.write_array_cell_bound(index, name, value.clone())?,
                        Err(TableError::ColumnNotFound { .. }) => {}
                        Err(error) => return Err(error),
                    },
                    crate::ColumnType::Record => {
                        base.set_cell(index, name, Value::Record(self.record_cell(row, column)?))?;
                    }
                }
            }
            selected_rows.push(index);
        }
        selected_rows.sort_unstable();
        selected_rows.dedup();
        self.note_persisted_write(&root);
        let result = base.persist_selected_rows_in_place(&selected_columns, &selected_rows);
        if allow_rewrite
            && matches!(&result, Err(TableError::Storage(message)) if message.starts_with("partial in-place save does not support"))
            && let Some(options) = base.single_rewrite_options()?
        {
            base.save(options)?;
        } else {
            result?;
        }
        let mut changed_rows = changed_rows;
        changed_rows.sort_unstable();
        self.inner.note_selected_cells_saved(columns, &changed_rows);
        Ok(true)
    }

    fn check_reference_base(&self, base: &Path) -> Result<(), TableError> {
        #[cfg(unix)]
        if let Some(lock) = &self.lock_state
            && !locking::same_directory(&lock.path, base)
        {
            return Err(TableError::Storage(
                "the saved reference now points to a different base; reopen it".into(),
            ));
        }
        #[cfg(not(unix))]
        let _ = base;
        Ok(())
    }

    /// RefTable keywords (table and column) belong to the base table.
    /// TableInfo is stored beside the view's descriptor.
    pub(super) fn try_save_reference_metadata(
        &self,
        destination: &Path,
    ) -> Result<bool, TableError> {
        if !self.is_reference_table()?
            || !self
                .source_path
                .as_deref()
                .is_some_and(|path| locking::same_directory(path, destination))
        {
            return Ok(false);
        }
        let (root, chain) = reference_chain(destination.to_path_buf(), ReferenceRoot::Plain)?;
        self.check_reference_base(&root)?;
        let mut table = Table::open(TableOptions::new(&root))?;
        let view = self.source_path.as_deref().expect("saved reference");
        let mut keywords = self.keywords().clone();
        rebase_table_keywords(&mut keywords, view, &root);
        *table.keywords_mut() = keywords;
        for column in &chain[0].column_names {
            let mut keywords = self.column_keywords(column).cloned().unwrap_or_default();
            rebase_table_keywords(&mut keywords, view, &root);
            table.set_column_keywords(base_name(&chain, column), keywords);
        }
        self.note_persisted_write(&root);
        table.save_metadata_only(TableOptions::new(&root))?;
        std::fs::write(destination.join(TABLE_INFO_FILE), self.info().to_string())
            .map_err(crate::storage::StorageError::from)?;
        self.inner.note_metadata_saved();
        Ok(true)
    }
}
