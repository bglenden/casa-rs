// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed whole-cell I/O for uniform array columns.

use std::ops::Range;

use super::data_type::CasacoreDataType;
use super::*;
use crate::table::SelectedArray2DCells;
use tiled_stman::packed_update::PackedArrayUpdate;

impl CompositeStorage {
    pub(crate) fn load_uniform_2d_from_plain(
        &self,
        path: &Path,
        metadata: &RetainedTableReadMetadata,
        column: &str,
        rows: &[usize],
        widen_complex: bool,
    ) -> Result<Option<ArrayValue>, StorageError> {
        let table = &metadata.table_dat;
        let Some(entry) = table
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
        else {
            return Ok(None);
        };
        if rows.is_empty()
            || !table
                .column_set
                .data_managers
                .iter()
                .any(|dm| dm.seq_nr == entry.dm_seq_nr && dm.type_name == "TiledShapeStMan")
            || table
                .column_set
                .data_managers
                .iter()
                .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Ok(None);
        }
        let bound: Vec<_> = table
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.dm_seq_nr == entry.dm_seq_nr)
            .map(|(index, _)| &table.table_desc.columns[index])
            .collect();
        let Some(bound_index) = bound.iter().position(|desc| desc.col_name == column) else {
            return Ok(None);
        };
        metadata.tiled.load_uniform_2d(
            path,
            entry.dm_seq_nr,
            &bound,
            &table.table_desc,
            bound_index,
            tiled_stman::Packed2DSelection {
                rows,
                widen_complex,
            },
        )
    }

    pub(crate) fn prepare_packed_array_update(
        &self,
        path: &Path,
        table: &TableDatContents,
        column: &str,
        rows: Range<usize>,
        cell_shape: [usize; 2],
        data_type: CasacoreDataType,
    ) -> Result<Option<PackedArrayUpdate>, StorageError> {
        let set = &table.column_set;
        let Some(entry) = set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
        else {
            return Ok(None);
        };
        if !entry.is_array
            || set
                .data_managers
                .iter()
                .any(|dm| is_virtual_engine(&dm.type_name))
            || !set
                .data_managers
                .iter()
                .any(|dm| dm.seq_nr == entry.dm_seq_nr && dm.type_name == "TiledShapeStMan")
            || set
                .columns
                .iter()
                .filter(|other| other.dm_seq_nr == entry.dm_seq_nr)
                .count()
                != 1
        {
            return Ok(None);
        }
        tiled_stman::packed_update::prepare_packed_array_update(
            path,
            entry.dm_seq_nr,
            rows,
            cell_shape,
            data_type,
        )
    }

    pub(crate) fn load_fixed_2d_from_plain(
        &self,
        path: &Path,
        table: &TableDatContents,
        column: &str,
        rows: &[usize],
        shape: [usize; 2],
    ) -> Result<Option<SelectedArray2DCells>, StorageError> {
        if rows.is_empty() || shape.contains(&0) {
            return Ok(None);
        }
        let entry = table
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("column {column} has no storage binding"))
            })?;
        let dm = table
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == entry.dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("column {column} has no data manager"))
            })?;
        if table
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Ok(None);
        }
        if dm.type_name == "StandardStMan" {
            let descs = table
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.dm_seq_nr == dm.seq_nr)
                .map(|(index, _)| &table.table_desc.columns[index])
                .collect::<Vec<_>>();
            let index = descs
                .iter()
                .position(|desc| desc.col_name == column)
                .ok_or_else(|| {
                    StorageError::FormatMismatch(format!("column {column} has no descriptor"))
                })?;
            if let Some(values) = standard_stman::read_fixed_2d_cells(
                &path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", dm.seq_nr)),
                &dm.data,
                &descs,
                index,
                rows,
                shape,
            )? {
                return Ok(Some(values));
            }
        }
        self.whole_cell_read(path, table, column, rows)
            .load_fixed_2d(shape)
    }
}
