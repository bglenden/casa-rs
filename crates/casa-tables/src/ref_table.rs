// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reference table — a view over a parent table's rows and columns.
//!
//! [`RefTable`] is a read-only view over a parent [`Table`]. [`RefTableMut`]
//! adds write-through cell mutation while retaining the same row/column
//! mapping semantics.
//!
//! # C++ reference
//!
//! `RefTable`, `RefColumn`, `RefRows`.

use std::path::Path;

use casa_types::{RecordValue, Value};

use crate::schema::{ColumnSchema, TableSchema};
use crate::storage::{CompositeStorage, RefTableDatContents};
use crate::table::{Table, TableError};
use crate::table_path::strip_directory;

#[derive(Clone)]
struct RefLayout {
    row_map: Vec<usize>,
    column_names: Vec<String>,
    /// (view_name, parent_name) pairs for column name translation.
    column_name_map: Vec<(String, String)>,
    row_order: bool,
}

impl RefLayout {
    fn from_rows(parent: &Table, row_map: Vec<usize>) -> Result<Self, TableError> {
        validate_row_map(parent, &row_map)?;
        let column_names = default_column_names(parent);
        Ok(Self {
            row_order: row_map.windows(2).all(|w| w[0] < w[1]),
            row_map,
            column_name_map: identity_column_name_map(&column_names),
            column_names,
        })
    }

    fn from_columns(parent: &Table, columns: &[&str]) -> Result<Self, TableError> {
        validate_columns(parent, columns.iter().copied())?;
        let row_map: Vec<usize> = (0..parent.row_count()).collect();
        let column_names: Vec<String> = columns.iter().map(|&s| s.to_string()).collect();
        Ok(Self {
            row_map,
            row_order: true,
            column_name_map: identity_column_name_map(&column_names),
            column_names,
        })
    }

    fn from_rows_and_columns(
        parent: &Table,
        row_map: Vec<usize>,
        columns: &[String],
    ) -> Result<Self, TableError> {
        validate_row_map(parent, &row_map)?;
        validate_columns(parent, columns.iter().map(String::as_str))?;
        Ok(Self {
            row_order: row_map.windows(2).all(|w| w[0] < w[1]),
            row_map,
            column_name_map: identity_column_name_map(columns),
            column_names: columns.to_vec(),
        })
    }

    fn from_predicate<F>(parent: &Table, predicate: F) -> Result<Self, TableError>
    where
        F: Fn(&RecordValue) -> bool,
    {
        let mut row_map = Vec::new();
        for i in 0..parent.row_count() {
            if predicate(parent.row_accessor().row(i)?) {
                row_map.push(i);
            }
        }

        let column_names = default_column_names(parent);
        Ok(Self {
            row_map,
            row_order: true,
            column_name_map: identity_column_name_map(&column_names),
            column_names,
        })
    }

    fn row_count(&self) -> usize {
        self.row_map.len()
    }

    fn row_numbers(&self) -> &[usize] {
        &self.row_map
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn translate_row(&self, index: usize) -> Result<usize, TableError> {
        self.row_map
            .get(index)
            .copied()
            .ok_or(TableError::RowIndexOutOfRange {
                index,
                count: self.row_map.len(),
            })
    }

    fn translate_column<'a>(&'a self, name: &str) -> Result<&'a str, TableError> {
        self.column_name_map
            .iter()
            .find(|(view_name, _)| view_name == name)
            .map(|(_, parent_name)| parent_name.as_str())
            .ok_or_else(|| TableError::UnknownColumn {
                name: name.to_string(),
            })
    }

    fn schema(&self, parent: &Table) -> Option<TableSchema> {
        let parent_schema = parent.schema()?;
        let cols: Vec<ColumnSchema> = self
            .column_names
            .iter()
            .filter_map(|name| {
                let parent_name = self
                    .column_name_map
                    .iter()
                    .find(|(view_name, _)| view_name == name)
                    .map(|(_, parent_name)| parent_name.as_str())
                    .unwrap_or(name.as_str());
                parent_schema
                    .columns()
                    .iter()
                    .find(|c| c.name() == parent_name)
                    .cloned()
            })
            .collect();
        TableSchema::new(cols).ok()
    }

    fn save(
        &self,
        parent: &Table,
        opts: &crate::table::TableOptions,
        link_owner: &Path,
    ) -> Result<(), TableError> {
        let parent_path = parent.path().ok_or(TableError::ParentNotSaved)?;
        let ref_path = opts.path();
        let parent_relative = strip_directory(parent_path, link_owner);

        let ref_dat = RefTableDatContents {
            nrrow: self.row_map.len() as u64,
            big_endian: opts.endian_format().is_big_endian(),
            parent_relative_path: parent_relative,
            column_name_map: self.column_name_map.clone(),
            column_names: self.column_names.clone(),
            parent_nrrow: parent.row_count() as u64,
            row_order: self.row_order,
            row_map: self.row_map.iter().map(|&r| r as u64).collect(),
        };

        let storage = CompositeStorage;
        storage
            .save_ref_table(ref_path, &ref_dat, &crate::storage::TableInfo::default())
            .map_err(|e| TableError::Storage(e.to_string()))?;
        Ok(())
    }
}

fn default_column_names(parent: &Table) -> Vec<String> {
    parent
        .schema()
        .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default()
}

fn identity_column_name_map(column_names: &[String]) -> Vec<(String, String)> {
    column_names
        .iter()
        .map(|name| (name.clone(), name.clone()))
        .collect()
}

fn validate_row_map(parent: &Table, row_map: &[usize]) -> Result<(), TableError> {
    let count = parent.row_count();
    for &idx in row_map {
        if idx >= count {
            return Err(TableError::RowIndexOutOfRange { index: idx, count });
        }
    }
    Ok(())
}

fn validate_columns<'a>(
    parent: &Table,
    columns: impl IntoIterator<Item = &'a str>,
) -> Result<(), TableError> {
    if let Some(schema) = parent.schema() {
        for name in columns {
            if !schema.columns().iter().any(|c| c.name() == name) {
                return Err(TableError::UnknownColumn {
                    name: name.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// A read-only view over a parent [`Table`]'s rows and/or columns.
pub struct RefTable<'a> {
    parent: &'a Table,
    layout: RefLayout,
}

impl<'a> RefTable<'a> {
    pub(crate) fn from_rows(parent: &'a Table, row_map: Vec<usize>) -> Result<Self, TableError> {
        Ok(Self {
            parent,
            layout: RefLayout::from_rows(parent, row_map)?,
        })
    }

    pub(crate) fn from_columns(parent: &'a Table, columns: &[&str]) -> Result<Self, TableError> {
        Ok(Self {
            parent,
            layout: RefLayout::from_columns(parent, columns)?,
        })
    }

    pub(crate) fn from_rows_and_columns(
        parent: &'a Table,
        row_map: Vec<usize>,
        columns: &[String],
    ) -> Result<Self, TableError> {
        Ok(Self {
            parent,
            layout: RefLayout::from_rows_and_columns(parent, row_map, columns)?,
        })
    }

    pub(crate) fn from_predicate<F>(parent: &'a Table, predicate: F) -> Result<Self, TableError>
    where
        F: Fn(&RecordValue) -> bool,
    {
        Ok(Self {
            parent,
            layout: RefLayout::from_predicate(parent, predicate)?,
        })
    }

    pub fn row_count(&self) -> usize {
        self.layout.row_count()
    }

    pub fn row(&self, index: usize) -> Result<&RecordValue, TableError> {
        self.parent
            .row_accessor()
            .row(self.layout.translate_row(index)?)
    }

    pub fn cell(&self, row: usize, column: &str) -> Result<Option<&Value>, TableError> {
        let parent_idx = self.layout.translate_row(row)?;
        let parent_col = self.layout.translate_column(column)?;
        self.parent.cell_accessor(parent_idx, parent_col)?.value()
    }

    pub fn row_numbers(&self) -> &[usize] {
        self.layout.row_numbers()
    }

    pub fn column_names(&self) -> &[String] {
        self.layout.column_names()
    }

    pub fn schema(&self) -> Option<TableSchema> {
        self.layout.schema(self.parent)
    }

    pub fn parent_path(&self) -> Option<&Path> {
        self.parent.path()
    }

    pub fn save(&self, opts: crate::table::TableOptions) -> Result<(), TableError> {
        self.layout.save(self.parent, &opts, opts.path())
    }

    /// Stage this reference table with its parent link relative to
    /// the intended publication path. The staged directory must be moved
    /// to that destination before opening it. A saved reference parent is
    /// flattened to its physical root, with its visible column names preserved.
    /// Hold the root's write lock and verify the source descriptors while staging.
    ///
    /// # Errors
    /// The parent has no persisted path, or the staged descriptor cannot be written.
    pub fn save_for_destination(
        &self,
        staging: crate::table::TableOptions,
        destination: &Path,
    ) -> Result<(), TableError> {
        if self.parent.is_reference_table()? {
            let names = self
                .layout
                .column_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let projection = self
                .parent
                .reference_write_projection(&self.layout.row_map, &names)?
                .ok_or_else(|| {
                    TableError::Storage("reference parent changed while staging".into())
                })?;
            let root = Table::open(crate::table::TableOptions::new(projection.path))?;
            let mut layout =
                RefLayout::from_rows_and_columns(&root, projection.rows, &projection.columns)?;
            layout.column_names = self.layout.column_names.clone();
            layout.column_name_map = layout
                .column_names
                .iter()
                .cloned()
                .zip(projection.columns)
                .collect();
            return layout.save(&root, &staging, destination);
        }
        self.layout.save(self.parent, &staging, destination)
    }
}

/// A mutable write-through view over a parent [`Table`]'s rows and/or columns.
pub struct RefTableMut<'a> {
    parent: &'a mut Table,
    layout: RefLayout,
}

impl<'a> RefTableMut<'a> {
    pub(crate) fn from_rows(
        parent: &'a mut Table,
        row_map: Vec<usize>,
    ) -> Result<Self, TableError> {
        let layout = RefLayout::from_rows(&*parent, row_map)?;
        Ok(Self { parent, layout })
    }

    pub(crate) fn from_columns(
        parent: &'a mut Table,
        columns: &[&str],
    ) -> Result<Self, TableError> {
        let layout = RefLayout::from_columns(&*parent, columns)?;
        Ok(Self { parent, layout })
    }

    pub(crate) fn from_rows_and_columns(
        parent: &'a mut Table,
        row_map: Vec<usize>,
        columns: &[String],
    ) -> Result<Self, TableError> {
        let layout = RefLayout::from_rows_and_columns(&*parent, row_map, columns)?;
        Ok(Self { parent, layout })
    }

    pub(crate) fn from_predicate<F>(parent: &'a mut Table, predicate: F) -> Result<Self, TableError>
    where
        F: Fn(&RecordValue) -> bool,
    {
        let layout = RefLayout::from_predicate(&*parent, predicate)?;
        Ok(Self { parent, layout })
    }

    pub fn as_ref(&self) -> RefTable<'_> {
        RefTable {
            parent: &*self.parent,
            layout: self.layout.clone(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.layout.row_count()
    }

    pub fn row(&self, index: usize) -> Result<&RecordValue, TableError> {
        self.parent
            .row_accessor()
            .row(self.layout.translate_row(index)?)
    }

    pub fn cell(&self, row: usize, column: &str) -> Result<Option<&Value>, TableError> {
        let parent_idx = self.layout.translate_row(row)?;
        let parent_col = self.layout.translate_column(column)?;
        self.parent.cell_accessor(parent_idx, parent_col)?.value()
    }

    pub fn set_cell(&mut self, row: usize, column: &str, value: Value) -> Result<(), TableError> {
        let parent_idx = self.layout.translate_row(row)?;
        let parent_col = self.layout.translate_column(column)?.to_string();
        self.parent
            .cell_accessor_mut(parent_idx, &parent_col)?
            .set(value)
    }

    pub fn row_numbers(&self) -> &[usize] {
        self.layout.row_numbers()
    }

    pub fn column_names(&self) -> &[String] {
        self.layout.column_names()
    }

    pub fn schema(&self) -> Option<TableSchema> {
        self.layout.schema(&*self.parent)
    }

    pub fn parent_path(&self) -> Option<&Path> {
        self.parent.path()
    }

    pub fn save(&self, opts: crate::table::TableOptions) -> Result<(), TableError> {
        self.layout.save(&*self.parent, &opts, opts.path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::{PrimitiveType, RecordField, ScalarValue};

    fn test_table() -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        for i in 0..5 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                    RecordField::new(
                        "name",
                        Value::Scalar(ScalarValue::String(format!("row_{i}"))),
                    ),
                ]))
                .unwrap();
        }
        table
    }

    #[test]
    fn select_rows_basic() {
        let table = test_table();
        let view = table.select_rows(&[1, 3]).unwrap();
        assert_eq!(view.row_count(), 2);

        let id = view.cell(0, "id").unwrap().unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(1)));

        let id = view.cell(1, "id").unwrap().unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(3)));
    }

    #[test]
    fn select_rows_out_of_range() {
        let table = test_table();
        let result = table.select_rows(&[0, 99]);
        assert!(matches!(
            result,
            Err(TableError::RowIndexOutOfRange {
                index: 99,
                count: 5
            })
        ));
    }

    #[test]
    fn select_columns_basic() {
        let table = test_table();
        let view = table.select_columns(&["name"]).unwrap();
        assert_eq!(view.row_count(), 5);
        assert_eq!(view.column_names(), &["name"]);

        let name = view.cell(0, "name").unwrap().unwrap();
        assert_eq!(
            name,
            &Value::Scalar(ScalarValue::String("row_0".to_string()))
        );

        let result = view.cell(0, "id");
        assert!(matches!(result, Err(TableError::UnknownColumn { .. })));
    }

    #[test]
    fn select_columns_unknown() {
        let table = test_table();
        let result = table.select_columns(&["nonexistent"]);
        assert!(matches!(result, Err(TableError::UnknownColumn { .. })));
    }

    #[test]
    fn select_predicate() {
        let table = test_table();
        let view = table
            .select(|row| {
                row.get("id")
                    .map(|v| matches!(v, Value::Scalar(ScalarValue::Int32(i)) if *i >= 3))
                    .unwrap_or(false)
            })
            .unwrap();
        assert_eq!(view.row_count(), 2);
        assert_eq!(view.row_numbers(), &[3, 4]);
    }

    #[test]
    fn write_through_view() {
        let mut table = test_table();
        {
            let mut view = table.select_rows_mut(&[2]).unwrap();
            view.set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("modified".into())),
            )
            .unwrap();
        }
        let name = table.row_accessor().row(2).unwrap().get("name").unwrap();
        assert_eq!(
            name,
            &Value::Scalar(ScalarValue::String("modified".to_string()))
        );
    }

    #[test]
    fn ref_table_schema_projection() {
        let table = test_table();
        let view = table.select_columns(&["name"]).unwrap();
        let schema = view.schema().unwrap();
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(schema.columns()[0].name(), "name");
    }

    #[test]
    fn save_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let parent_path = dir.path().join("parent.tbl");
        let ref_path = dir.path().join("ref.tbl");

        let table = test_table();
        table
            .save(crate::table::TableOptions::new(&parent_path))
            .unwrap();

        let table = Table::open(crate::table::TableOptions::new(&parent_path)).unwrap();
        let view = table.select_rows(&[0, 2, 4]).unwrap();
        view.save(crate::table::TableOptions::new(&ref_path))
            .unwrap();

        let reopened = Table::open(crate::table::TableOptions::new(&ref_path)).unwrap();
        assert_eq!(reopened.row_count(), 3);

        let id = reopened.row_accessor().row(0).unwrap().get("id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(0)));
        let id = reopened.row_accessor().row(1).unwrap().get("id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(2)));
        let id = reopened.row_accessor().row(2).unwrap().get("id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(4)));
    }

    #[test]
    fn full_row_projection_preserves_schema_field_order() {
        use crate::table::{ColumnBinding, DataManagerKind, TableOptions};

        let dir = tempfile::tempdir().unwrap();
        let parent_path = dir.path().join("parent.tbl");
        let bindings = std::collections::HashMap::from([(
            "id".to_owned(),
            ColumnBinding {
                data_manager: DataManagerKind::IncrementalStMan,
                tile_shape: None,
            },
        )]);
        test_table()
            .save_with_bindings(
                TableOptions::new(&parent_path).with_data_manager(DataManagerKind::StandardStMan),
                &bindings,
            )
            .unwrap();
        let table = Table::open(TableOptions::new(&parent_path)).unwrap();
        for (filename, indices) in [("unique", &[4, 0, 2][..]), ("repeated", &[4, 1, 4][..])] {
            let path = dir.path().join(filename);
            table
                .select_rows(indices)
                .unwrap()
                .save(TableOptions::new(&path))
                .unwrap();
            let projected = Table::open(TableOptions::new(&path)).unwrap();
            for (row, &id) in projected.rows().unwrap().iter().zip(indices) {
                assert_eq!(
                    row.fields()
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>(),
                    ["id", "name"],
                );
                assert_eq!(
                    row.get("id"),
                    Some(&Value::Scalar(ScalarValue::Int32(id as i32)))
                );
            }
        }
    }

    #[test]
    fn selected_scalar_reads_follow_reference_column_and_row_maps() {
        use crate::storage::table_control::{
            TableDatResult, read_table_dat_dispatch, write_ref_table_dat,
        };
        use crate::table::TableOptions;

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent.tbl");
        let view = dir.path().join("view.tbl");
        test_table().save(TableOptions::new(&parent)).unwrap();
        let base = Table::open(TableOptions::new(&parent)).unwrap();
        base.select_rows(&[4, 1, 3, 1])
            .unwrap()
            .save(TableOptions::new(&view))
            .unwrap();
        let control = view.join("table.dat");
        let TableDatResult::Ref(mut reference) = read_table_dat_dispatch(&control).unwrap() else {
            panic!("expected a reference descriptor");
        };
        reference.column_names[0] = "selected_id".to_owned();
        reference
            .column_name_map
            .retain(|(column, _)| column != "id");
        reference
            .column_name_map
            .push(("selected_id".into(), "id".into()));
        write_ref_table_dat(&control, &reference).unwrap();

        let table = Table::open(TableOptions::new(&view)).unwrap();
        let values = table
            .column_accessor("selected_id")
            .unwrap()
            .scalar_cells_owned_for_rows(&[3, 0, 1, 2])
            .unwrap();
        assert_eq!(values, [1, 4, 1, 3].map(|id| Some(ScalarValue::Int32(id))));
    }

    #[test]
    fn staged_reference_keeps_renamed_columns_and_does_not_depend_on_the_input_view() {
        use crate::storage::table_control::{
            TableDatResult, read_table_dat_dispatch, write_ref_table_dat,
        };
        use crate::table::TableOptions;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root.tbl");
        let input = dir.path().join("input.tbl");
        let stage = root.join("stage");
        let output = root.join("SORTED_TABLE");
        test_table().save(TableOptions::new(&root)).unwrap();
        let parent = Table::open(TableOptions::new(&root)).unwrap();
        parent
            .select_rows_and_columns(&[4, 1, 3], &["id".into()])
            .unwrap()
            .save(TableOptions::new(&input))
            .unwrap();
        let control = input.join("table.dat");
        let TableDatResult::Ref(mut reference) = read_table_dat_dispatch(&control).unwrap() else {
            panic!("expected a reference descriptor");
        };
        reference.column_names = vec!["row_id".into()];
        reference.column_name_map = vec![("row_id".into(), "id".into())];
        write_ref_table_dat(&control, &reference).unwrap();

        let view = Table::open(TableOptions::new(&input)).unwrap();
        let lock = crate::TableWriteLock::acquire(&root, 1).unwrap();
        view.select_rows(&[2, 0, 2])
            .unwrap()
            .save_for_destination(TableOptions::new(&stage), &output)
            .unwrap();
        std::fs::rename(stage, &output).unwrap();
        lock.release().unwrap();
        std::fs::rename(&input, dir.path().join("moved.tbl")).unwrap();
        let saved = Table::open(TableOptions::new(&output)).unwrap();
        assert_eq!(saved.root_row_numbers().unwrap(), [3, 4, 3]);
        assert!(saved.column_accessor("id").is_err());
        let values = saved
            .column_accessor("row_id")
            .unwrap()
            .scalar_cells_owned_for_rows(&[0, 1, 2])
            .unwrap();
        assert_eq!(values, [3, 4, 3].map(|id| Some(ScalarValue::Int32(id))));
    }

    #[test]
    fn projected_reference_reads_keep_physical_incremental_scalar_values() {
        use crate::schema::ColumnOptions;
        use crate::table::{DataManagerKind, TableOptions};

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent.tbl");
        let view = dir.path().join("view.tbl");
        let column = ColumnSchema::scalar("probe", PrimitiveType::Int32)
            .with_options(ColumnOptions {
                undefined: true,
                ..ColumnOptions::default()
            })
            .unwrap();
        let mut base = Table::with_schema(TableSchema::new(vec![column]).unwrap());
        for value in [0, 4] {
            base.add_row(RecordValue::new(vec![RecordField::new(
                "probe",
                Value::Scalar(ScalarValue::Int32(value)),
            )]))
            .unwrap();
        }
        base.save(TableOptions::new(&parent).with_data_manager(DataManagerKind::IncrementalStMan))
            .unwrap();
        let base = Table::open(TableOptions::new(&parent)).unwrap();
        base.select_rows(&[1, 0])
            .unwrap()
            .save(TableOptions::new(&view))
            .unwrap();

        let storage = CompositeStorage;
        let parent_values = storage
            .load_scalar_column_rows_with_row_hint(&parent, "probe", &[1, 0], None)
            .unwrap();
        assert_eq!(parent_values, [Some(ScalarValue::Int32(4)), None]);
        let reference_values = storage
            .load_scalar_column_rows_with_row_hint(&view, "probe", &[0, 1], None)
            .unwrap();
        assert_eq!(
            reference_values,
            [4, 0].map(|value| Some(ScalarValue::Int32(value)))
        );
    }

    #[test]
    fn reference_metadata_writes_keep_parent_table_and_column_keyword_links() {
        use crate::table::TableOptions;
        use crate::table_path::add_directory;

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("run").join("parent.tbl");
        let view = dir.path().join("view.tbl");
        let target = parent.join("child.tbl");
        std::fs::create_dir_all(parent.parent().unwrap()).unwrap();
        let mut base = test_table();
        let link = Value::table_ref(target.to_string_lossy().into_owned());
        base.keywords_mut().upsert("LINK", link.clone());
        base.set_column_keywords("id", RecordValue::new(vec![RecordField::new("LINK", link)]));
        base.save(TableOptions::new(&parent)).unwrap();

        let base = Table::open(TableOptions::new(&parent)).unwrap();
        base.select_rows(&[1])
            .unwrap()
            .save(TableOptions::new(&view))
            .unwrap();
        let mut selected = Table::open(TableOptions::new(&view)).unwrap();
        let check = |table: &Table, owner: &std::path::Path| {
            for record in [table.keywords(), table.column_keywords("id").unwrap()] {
                let Some(Value::TableRef(path)) = record.get("LINK") else {
                    panic!("expected a table keyword");
                };
                assert_eq!(add_directory(path, owner), target);
            }
        };
        check(&selected, &view);
        selected
            .keywords_mut()
            .upsert("CHANGED", Value::Scalar(ScalarValue::Bool(true)));
        selected
            .save_metadata_only(TableOptions::new(&view))
            .unwrap();
        let reopened = Table::open(TableOptions::new(&parent)).unwrap();
        assert_eq!(
            reopened.keywords().get("CHANGED"),
            Some(&Value::Scalar(ScalarValue::Bool(true)))
        );
        check(&reopened, &parent);
    }

    #[test]
    fn row_numbers_accessor() {
        let table = test_table();
        let view = table.select_rows(&[4, 1, 3]).unwrap();
        assert_eq!(view.row_numbers(), &[4, 1, 3]);
        assert!(!view.layout.row_order);
    }

    #[test]
    fn mutable_projection_exposes_read_only_view_and_saves() {
        let dir = tempfile::tempdir().unwrap();
        let parent_path = dir.path().join("parent.tbl");
        let ref_path = dir.path().join("ref.tbl");

        let table = test_table();
        table
            .save(crate::table::TableOptions::new(&parent_path))
            .unwrap();

        let mut reopened = Table::open(crate::table::TableOptions::new(&parent_path)).unwrap();
        let mut view = reopened.select_columns_mut(&["name"]).unwrap();

        assert_eq!(view.parent_path(), Some(parent_path.as_path()));
        assert_eq!(view.column_names(), &["name"]);
        assert_eq!(view.as_ref().column_names(), &["name"]);
        assert_eq!(view.schema().unwrap().columns().len(), 1);
        assert_eq!(
            view.row(0).unwrap().get("id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            view.cell(0, "name").unwrap(),
            Some(&Value::Scalar(ScalarValue::String("row_0".to_string())))
        );

        view.set_cell(
            1,
            "name",
            Value::Scalar(ScalarValue::String("projected".into())),
        )
        .unwrap();
        drop(view);

        assert_eq!(
            reopened.row_accessor().row(1).unwrap().get("name"),
            Some(&Value::Scalar(ScalarValue::String("projected".to_string())))
        );

        reopened.flush().unwrap();
        let view = reopened.select_columns_mut(&["name"]).unwrap();
        view.save(crate::table::TableOptions::new(&ref_path))
            .unwrap();
        drop(view);

        let saved = Table::open(crate::table::TableOptions::new(&ref_path)).unwrap();
        assert_eq!(saved.row_count(), 5);
        assert_eq!(saved.schema().unwrap().columns().len(), 1);
        assert_eq!(
            saved.row_accessor().row(1).unwrap().get("name"),
            Some(&Value::Scalar(ScalarValue::String("projected".to_string())))
        );
    }

    #[test]
    fn direct_read_only_constructors_wrap_layout_builders() {
        let table = test_table();

        let from_rows = RefTable::from_rows(&table, vec![0, 2]).unwrap();
        assert_eq!(from_rows.row_count(), 2);
        assert_eq!(from_rows.row_numbers(), &[0, 2]);
        assert!(from_rows.parent_path().is_none());

        let from_columns = RefTable::from_columns(&table, &["name"]).unwrap();
        assert_eq!(from_columns.column_names(), &["name"]);

        let projected =
            RefTable::from_rows_and_columns(&table, vec![1, 4], &["name".to_string()]).unwrap();
        assert_eq!(projected.row_count(), 2);
        assert_eq!(projected.column_names(), &["name"]);

        let filtered = RefTable::from_predicate(&table, |row| {
            matches!(
                row.get("id"),
                Some(Value::Scalar(ScalarValue::Int32(id))) if *id >= 3
            )
        })
        .unwrap();
        assert_eq!(filtered.row_numbers(), &[3, 4]);
    }

    #[test]
    fn direct_mutable_constructors_wrap_layout_builders() {
        let mut table = test_table();

        let from_rows = RefTableMut::from_rows(&mut table, vec![0, 4]).unwrap();
        assert_eq!(from_rows.row_count(), 2);
        drop(from_rows);

        let from_columns = RefTableMut::from_columns(&mut table, &["name"]).unwrap();
        assert_eq!(from_columns.row_count(), 5);
        assert_eq!(from_columns.column_names(), &["name"]);
        drop(from_columns);

        let projected =
            RefTableMut::from_rows_and_columns(&mut table, vec![1, 3], &["name".to_string()])
                .unwrap();
        assert_eq!(projected.row_count(), 2);
        assert_eq!(projected.column_names(), &["name"]);
        drop(projected);

        let filtered = RefTableMut::from_predicate(&mut table, |row| {
            matches!(
                row.get("id"),
                Some(Value::Scalar(ScalarValue::Int32(id))) if *id <= 1
            )
        })
        .unwrap();
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn strip_directory_siblings() {
        let rel = strip_directory(Path::new("/a/b/parent.tbl"), Path::new("/a/b/ref.tbl"));
        assert_eq!(rel, "./parent.tbl");
    }

    #[test]
    fn strip_directory_nested() {
        let rel = strip_directory(Path::new("/a/b/ref.tbl/sub"), Path::new("/a/b/ref.tbl"));
        assert_eq!(rel, "././sub");
    }

    fn stored_parent(ref_table: &Path) -> String {
        let control = ref_table.join("table.dat");
        match crate::storage::table_control::read_table_dat_dispatch(&control).unwrap() {
            crate::storage::table_control::TableDatResult::Ref(dat) => dat.parent_relative_path,
            _ => panic!("{} is not a RefTable", ref_table.display()),
        }
    }

    #[test]
    fn deeper_and_enclosing_parents_are_stored_relative_and_survive_a_move() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("run");
        let parent_path = root.join("sub").join("parent.tbl");
        let beside = root.join("beside.tbl");
        let inside = parent_path.join("sel.tbl");
        std::fs::create_dir_all(parent_path.parent().unwrap()).unwrap();
        test_table()
            .save(crate::table::TableOptions::new(&parent_path))
            .unwrap();
        let table = Table::open(crate::table::TableOptions::new(&parent_path)).unwrap();
        let rows = table.select_rows(&[0, 2, 4]).unwrap();
        rows.save(crate::table::TableOptions::new(&beside)).unwrap();
        let rows = table.select_rows(&[1, 3]).unwrap();
        rows.save(crate::table::TableOptions::new(&inside)).unwrap();
        assert_eq!(stored_parent(&beside), "./sub/parent.tbl");
        assert_eq!(stored_parent(&inside), "sel.tbl/.");

        let moved = dir.path().join("elsewhere").join("run");
        std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
        std::fs::rename(&root, &moved).unwrap();
        let reopened = Table::open(crate::table::TableOptions::new(moved.join("beside.tbl")));
        assert_eq!(reopened.unwrap().row_count(), 3);
        let inside = moved.join("sub").join("parent.tbl").join("sel.tbl");
        let reopened = Table::open(crate::table::TableOptions::new(inside)).unwrap();
        assert_eq!(reopened.row_count(), 2);
    }
}
