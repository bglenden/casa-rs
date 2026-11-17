// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admit new columns without changing storage managers outside the binding.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use casa_aipsio::ByteOrder;
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

use crate::DataManagerInfo;
use crate::table::{ColumnAddition, ManagerAddition};

use super::row_changes::StagedEdit;
use super::table_control::{
    ColumnDescContents, DataManagerEntry, PlainColumnEntry, TableDatContents,
};
use super::{
    CompositeStorage, StorageError, incremental_stman, standard_stman, stman_aipsio, tiled_stman,
};

impl<'a> StagedEdit<'a> {
    pub(crate) fn stage_columns(
        table: &'a Path,
        mut dat: TableDatContents,
        added: Vec<ColumnAddition>,
        dminfo: Option<ManagerAddition>,
        existing: &[DataManagerInfo],
    ) -> Result<Self, StorageError> {
        let edit = Self::new(table)?;
        let mut groups = BTreeSet::new();
        let mut defaults = RecordValue::default();
        let mut names: HashSet<_> = dat
            .table_desc
            .columns
            .iter()
            .map(|c| c.col_name.clone())
            .collect();
        let mut manager_names: HashSet<_> = existing.iter().map(|dm| dm.name.clone()).collect();
        let spec = dminfo.as_ref().map(|dm| &dm.spec);
        let mut assigned = if let Some(dm) = &dminfo {
            if !dm.name.is_empty() && !manager_names.insert(dm.name.clone()) {
                return Err(StorageError::Schema(format!(
                    "storage manager {} already exists",
                    dm.name
                )));
            }
            Some((
                allocate(&mut dat, &dm.kind),
                dm.kind.clone(),
                dm.name.clone(),
            ))
        } else {
            existing
                .iter()
                .find(|dm| admits_add(&dm.dm_type))
                .map(|dm| (dm.seq_nr, dm.dm_type.clone(), dm.name.clone()))
        };
        for column in added {
            let mut desc = ColumnDescContents::from_column_schema(&column.schema);
            if !names.insert(desc.col_name.clone()) {
                return Err(StorageError::Schema(format!(
                    "column {} already exists",
                    desc.col_name
                )));
            }
            let (seq, kind, group) = match &assigned {
                Some(binding) => binding.clone(),
                None => {
                    let kind = column
                        .manager_type
                        .unwrap_or_else(|| "StandardStMan".to_string());
                    let requested = column.manager_group.unwrap_or_else(|| kind.clone());
                    let mut name = requested.clone();
                    let mut suffix = 1u64;
                    while !manager_names.insert(name.clone()) {
                        name = format!("{requested}_{suffix}");
                        suffix += 1;
                    }
                    let chosen = (allocate(&mut dat, &kind), kind, name);
                    if admits_add(&chosen.1) {
                        assigned = Some(chosen.clone());
                    }
                    chosen
                }
            };
            // A manager's stored name can be empty even when its column
            // descriptions name a group (for example StManAipsIO).
            // Joining a manager keeps that descriptor's declared binding.
            if let Some((declared, _)) = dat
                .table_desc
                .columns
                .iter()
                .zip(&dat.column_set.columns)
                .find(|(_, entry)| entry.dm_seq_nr == seq)
            {
                desc.data_manager_type = declared.data_manager_type.clone();
                desc.data_manager_group = declared.data_manager_group.clone();
            } else {
                desc.data_manager_type = kind;
                desc.data_manager_group = group;
            }
            desc.keywords = column.keywords;
            if let Some(value) = column.default {
                defaults.push(RecordField::new(&desc.col_name, value));
            }
            dat.column_set.columns.push(PlainColumnEntry {
                original_name: desc.col_name.clone(),
                dm_seq_nr: seq,
                is_array: desc.is_array,
            });
            dat.table_desc.columns.push(desc);
            groups.insert(seq);
        }
        for seq in groups {
            let columns: Vec<_> = dat
                .table_desc
                .columns
                .iter()
                .zip(&dat.column_set.columns)
                .filter(|(_, entry)| entry.dm_seq_nr == seq)
                .map(|(column, _)| column.clone())
                .collect();
            let original = existing.iter().find(|dm| dm.seq_nr == seq);
            let output =
                edit.write_added_columns(&dat, seq, &columns, original, &defaults, spec)?;
            if let Some(blob) = output {
                dat.column_set
                    .data_managers
                    .iter_mut()
                    .find(|dm| dm.seq_nr == seq)
                    .expect("allocated manager")
                    .data = blob;
            }
        }
        edit.stage_control(&dat)?;
        Ok(edit)
    }

    fn write_added_columns(
        &self,
        dat: &TableDatContents,
        seq: u32,
        columns: &[ColumnDescContents],
        existing: Option<&DataManagerInfo>,
        defaults: &RecordValue,
        spec: Option<&RecordValue>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let name = existing.map_or(&columns[0].data_manager_group, |dm| &dm.name);
        let kind = existing.map_or(&columns[0].data_manager_type, |dm| &dm.dm_type);
        let file = format!("table.f{seq}");
        let dst = self.staged.path().join(&file);
        let nrows = usize::try_from(dat.nrrow).map_err(|_| {
            StorageError::FormatMismatch("column addition row count overflow".into())
        })?;
        let defaults = RecordValue::new(
            defaults
                .fields()
                .iter()
                .filter(|field| columns.iter().any(|column| column.col_name == field.name))
                .cloned()
                .collect(),
        );
        let mut rows = if let Some(existing) = existing {
            let names: Vec<&str> = existing.columns.iter().map(String::as_str).collect();
            CompositeStorage
                .load_selected_columns_with_row_hint(self.table, &names, Some(dat.nrrow))?
                .rows
        } else {
            vec![RecordValue::default(); nrows]
        };
        for row in &mut rows {
            for field in defaults.fields() {
                row.push(field.clone());
            }
        }
        match kind.as_str() {
            "StandardStMan" if existing.is_some() => Ok(Some(standard_stman::rewrite_ssm_rows(
                &self.table.join(file),
                &dst,
                columns,
                &rows,
                name,
            )?)),
            "StandardStMan" => {
                let layout = Some((
                    uint_spec(spec, "BUCKETSIZE").filter(|&n| n >= 128),
                    uint_spec(spec, "PERSCACHESIZE").unwrap_or(2),
                ));
                Ok(Some(standard_stman::write_ssm_with_bucket(
                    &dst,
                    columns,
                    &rows,
                    dat.big_endian,
                    name,
                    layout,
                )?))
            }
            "StManAipsIO" => {
                stman_aipsio::write_stman_file(&dst, seq, columns, &rows, ByteOrder::BigEndian)?;
                Ok(None)
            }
            "IncrementalStMan" => {
                incremental_stman::write_ism_with_layout(
                    &dst,
                    columns,
                    &rows,
                    dat.big_endian,
                    uint_spec(spec, "BUCKETSIZE").filter(|&n| n > 0),
                    uint_spec(spec, "PERSCACHESIZE").unwrap_or(1),
                )?;
                Ok(Some(incremental_stman::serialize_ism_dm_blob(name)?))
            }
            "TiledShapeStMan" | "TiledColumnStMan" | "TiledCellStMan" => {
                let shape = tile_shape(spec)?;
                if columns.iter().any(|col| !col.is_array || col.is_record()) {
                    return Err(StorageError::Schema(format!(
                        "{kind} requires array columns"
                    )));
                }
                tiled_stman::save_tiled_columns(
                    self.staged.path(),
                    seq,
                    kind,
                    columns,
                    &rows,
                    dat.big_endian,
                    shape.as_deref(),
                    name,
                    &dat.table_desc.private_keywords,
                )?;
                if let Some(cache) = uint_spec(spec, "MAXIMUMCACHESIZE") {
                    tiled_stman::append::set_cache(&dst, u64::from(cache))?;
                }
                Ok(None)
            }
            other => Err(StorageError::UnsupportedDataManager(other.to_string())),
        }
    }
}

fn allocate(dat: &mut TableDatContents, kind: &str) -> u32 {
    let seq = dat.column_set.seq_count;
    dat.column_set.seq_count += 1;
    dat.column_set.data_managers.push(DataManagerEntry {
        type_name: kind.to_string(),
        seq_nr: seq,
        data: Vec::new(),
    });
    seq
}

fn admits_add(kind: &str) -> bool {
    matches!(kind, "StandardStMan" | "StManAipsIO")
}

fn uint_spec(spec: Option<&RecordValue>, key: &str) -> Option<u32> {
    match spec?.get(key)? {
        Value::Scalar(ScalarValue::Int32(x)) => u32::try_from(*x).ok(),
        Value::Scalar(ScalarValue::Int64(x)) => u32::try_from(*x).ok(),
        Value::Scalar(ScalarValue::UInt32(x)) => Some(*x),
        _ => None,
    }
}

fn tile_shape(spec: Option<&RecordValue>) -> Result<Option<Vec<usize>>, StorageError> {
    let Some(value) = spec.and_then(|spec| spec.get("DEFAULTTILESHAPE")) else {
        return Ok(None);
    };
    let invalid = || StorageError::Schema("DEFAULTTILESHAPE must contain positive integers".into());
    let dims: Vec<i64> = match value {
        Value::Array(casa_types::ArrayValue::Int32(a)) => {
            a.iter().copied().map(i64::from).collect()
        }
        Value::Array(casa_types::ArrayValue::Int64(a)) => a.iter().copied().collect(),
        _ => return Err(invalid()),
    };
    dims.iter()
        .map(|&n| {
            usize::try_from(n)
                .ok()
                .filter(|&size| size > 0)
                .ok_or_else(invalid)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}
