// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-manager specifications and single-group rewrite options (#1028).

use std::collections::HashSet;
use std::path::PathBuf;

use casa_core::RecordValue;

use crate::storage::{
    TABLE_CONTROL_FILE,
    incremental_stman::ism_spec,
    standard_stman::{dm_name_from_blob, ssm_spec},
    table_control::{TableDatContents, TableDatResult, read_table_dat_dispatch},
    tiled_stman::tiled_spec,
};
use crate::table_path::add_directory_for_link;

use super::{DataManagerKind, EndianFormat, Table, TableError, TableOptions};

fn plain_table_dat(
    start: &std::path::Path,
) -> Result<Option<(PathBuf, TableDatContents)>, TableError> {
    let mut path = start.to_path_buf();
    let mut seen = HashSet::new();
    for _ in 0..32 {
        let real =
            std::fs::canonicalize(&path).map_err(|error| TableError::Storage(error.to_string()))?;
        if !seen.insert(real) {
            return Err(TableError::Storage("reference table cycle".into()));
        }
        match read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))? {
            TableDatResult::Plain(table_dat) => return Ok(Some((path, table_dat))),
            TableDatResult::Ref(reference) => {
                path = add_directory_for_link(&reference.parent_relative_path, &path);
            }
            TableDatResult::Concat(_) => return Ok(None),
        }
    }
    Err(TableError::Storage(
        "reference table chain is too deep".into(),
    ))
}

impl Table {
    /// Return the `SPEC` record of data manager `seq_nr` on disk, or `None` for
    /// memory, concatenated, or unimplemented storage manager types.
    ///
    /// # C++ equivalent
    ///
    /// `DataManager::dataManagerSpec()` exposed through `Table::dataManagerInfo()`.
    pub fn data_manager_spec(&self, seq_nr: u32) -> Result<Option<RecordValue>, TableError> {
        if self.is_memory() {
            return Ok(self
                .data_manager_info()
                .iter()
                .find(|manager| manager.seq_nr == seq_nr && manager.dm_type == "MemoryStMan")
                .map(|_| RecordValue::default()));
        }
        let Some(source) = self.source_path.as_ref() else {
            return Ok(None);
        };
        let Some((root_path, table_dat)) = plain_table_dat(source)? else {
            return Ok(None);
        };
        let Some(dm) = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|entry| entry.seq_nr == seq_nr)
        else {
            return Ok(None);
        };
        match dm.type_name.as_str() {
            "StManAipsIO" | "MemoryStMan" => Ok(Some(RecordValue::default())),
            "StandardStMan" => ssm_spec(&root_path, seq_nr)
                .map(Some)
                .map_err(TableError::from),
            "IncrementalStMan" => ism_spec(&root_path, seq_nr)
                .map(Some)
                .map_err(TableError::from),
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_spec(&root_path, seq_nr, &table_dat)
                    .map(Some)
                    .map_err(TableError::from)
            }
            _ => Ok(None),
        }
    }

    /// Return full-save options preserving the layout when the table is a
    /// single `StManAipsIO` or single `StandardStMan` group whose manager name
    /// the writer reproduces.
    pub fn single_rewrite_options(&self) -> Result<Option<TableOptions>, TableError> {
        if let Some(options) = self.single_stman_aipsio_options()? {
            return Ok(Some(options));
        }
        let Some(path) = self.source_path.as_ref() else {
            return Ok(None);
        };
        let TableDatResult::Plain(table_dat) =
            read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))?
        else {
            return Ok(None);
        };
        let [manager] = table_dat.column_set.data_managers.as_slice() else {
            return Ok(None);
        };
        let groups: HashSet<&str> = table_dat
            .table_desc
            .columns
            .iter()
            .map(|column| column.data_manager_group.as_str())
            .collect();
        let (Some(group), 1) = (groups.iter().next(), groups.len()) else {
            return Ok(None);
        };
        if manager.type_name != "StandardStMan"
            || manager.seq_nr != 0
            || table_dat
                .table_desc
                .columns
                .iter()
                .any(|column| column.data_manager_type != "StandardStMan")
        {
            return Ok(None);
        }
        let stored_name = dm_name_from_blob(&manager.data);
        let preserve_ssm_name = stored_name.as_deref() == Some(*group);
        if !preserve_ssm_name && stored_name.as_deref() != Some("SSM") {
            return Ok(None);
        }
        let data_path = path.join(format!(
            "{}{}",
            crate::storage::TABLE_DATA_FILE_PREFIX,
            manager.seq_nr
        ));
        let casacore_default = crate::storage::standard_stman::ssm_has_small_buckets(&data_path)?;
        let format = if table_dat.big_endian {
            EndianFormat::BigEndian
        } else {
            EndianFormat::LittleEndian
        };
        Ok(Some(
            TableOptions::new(path)
                .with_data_manager(DataManagerKind::StandardStMan)
                .with_data_manager_group(*group)
                .with_casacore_default_ssm(casacore_default)
                .with_preserved_ssm_group_name(preserve_ssm_name)
                .with_endian_format(format),
        ))
    }
}

#[cfg(test)]
mod tests {
    use casa_core::{PrimitiveType, RecordField, ScalarValue, Value};
    use tempfile::tempdir;

    use crate::{ColumnSchema, TableSchema};

    use super::*;

    #[test]
    fn single_rewrite_preserves_32k_bucket_size_and_group_name() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("big_ssm.table");
        let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
            .expect("schema");
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(7)),
            )]))
            .expect("add row");
        table
            .save(
                TableOptions::new(&path)
                    .with_data_manager(DataManagerKind::StandardStMan)
                    .with_data_manager_group("StandardStMan")
                    .with_preserved_ssm_group_name(true),
            )
            .expect("save");

        let mut reopened = Table::open(TableOptions::new(&path)).expect("open");
        let spec_before = reopened
            .data_manager_spec(0)
            .expect("spec")
            .expect("some spec");
        assert_eq!(
            spec_before.get("BUCKETSIZE"),
            Some(&Value::Scalar(ScalarValue::Int32(32_768)))
        );

        let opts = reopened
            .single_rewrite_options()
            .expect("rewrite opts")
            .expect("single group");
        assert!(!opts.casacore_default_ssm());
        reopened
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(9)),
            )]))
            .expect("add row 2");
        reopened.save(opts).expect("resave");

        let again = Table::open(TableOptions::new(&path)).expect("reopen");
        let dm = again.data_manager_info();
        assert_eq!(dm.len(), 1);
        assert_eq!(dm[0].name, "StandardStMan");
        let spec_after = again
            .data_manager_spec(0)
            .expect("spec after")
            .expect("some spec");
        assert_eq!(
            spec_after.get("BUCKETSIZE"),
            Some(&Value::Scalar(ScalarValue::Int32(32_768)))
        );
    }
}
