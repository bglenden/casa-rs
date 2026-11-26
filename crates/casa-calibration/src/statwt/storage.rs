// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and write physical MAIN tables in aggregate row order.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::{
    MeasurementSet, MsError, MsResult, MsSelection, MsSelectionError, ResolvedMsSelectionRow,
};
use casa_tables::{RequiredScalarColumnValues, Table, TableWriteLock};
use casa_types::ArrayValue;

use crate::solve::grouping::VisibilityModels;
use crate::visibility_model_source_paths;
use crate::weight_spectra::has_column;

use super::output::Output;
use super::{RowUpdate, StatWtChannelBin, StatWtOptions, persist_updates};

/// One on-disk MeasurementSet or the physical MAIN tables of a Multi-MS.
pub struct StatWtMs {
    parts: Vec<Part>,
}

struct Part {
    offset: usize,
    path: PathBuf,
    ms: MeasurementSet,
}

#[derive(Clone, Copy, Default)]
pub(super) struct RowMetadata {
    pub(super) array: i32,
    pub(super) scan: i32,
    pub(super) state: i32,
    pub(super) exposure: f64,
}

impl StatWtMs {
    /// Open every physical MAIN table. Task previews can retain read locks.
    pub fn open(path: &Path, retain_read: bool) -> MsResult<Self> {
        let mut parts = Vec::new();
        let mut offset = 0;
        for path in visibility_model_source_paths(path)? {
            #[cfg(unix)]
            let ms = if retain_read {
                MeasurementSet::open_retained_read(&path)?
            } else {
                MeasurementSet::open(&path)?
            };
            #[cfg(not(unix))]
            let ms = {
                let _ = retain_read;
                MeasurementSet::open(&path)?
            };
            let count = ms.main_table().row_count();
            parts.push(Part { offset, path, ms });
            offset += count;
        }
        if parts.is_empty() {
            return Err(MsError::InvalidInput(
                "statwt requires a physical MAIN table".into(),
            ));
        }
        Ok(Self { parts })
    }

    /// Acquire the write capability for every physical MAIN table in path order.
    pub fn write_locks(paths: &[PathBuf]) -> MsResult<BTreeMap<PathBuf, TableWriteLock>> {
        let mut paths = paths.iter().collect::<Vec<_>>();
        paths.sort_unstable();
        let attempts = if paths.len() == 1 { 0 } else { 1 };
        paths
            .into_iter()
            .map(|path| {
                Ok((
                    path.clone(),
                    TableWriteLock::acquire_to_reread(path, attempts)?,
                ))
            })
            .collect()
    }

    /// Resolve CASA selectors with row IDs in aggregate MAIN order.
    pub fn select(
        &self,
        selection: &MsSelection,
    ) -> Result<Vec<ResolvedMsSelectionRow>, MsSelectionError> {
        let mut rows = Vec::new();
        for part in &self.parts {
            let result = crate::session::resolve_casa_calibration_selection(&part.ms, selection)?;
            rows.extend(result.selected_rows.into_iter().map(|mut row| {
                row.row_index += part.offset;
                row
            }));
        }
        Ok(rows)
    }

    /// Whether the expression names a FIELD in any constituent MS, independent
    /// of whether that field has any MAIN rows matching the other selectors.
    #[doc(hidden)]
    pub fn has_matching_field(&self, expression: &str) -> bool {
        self.parts
            .iter()
            .any(|part| part.ms.resolve_field_selector(expression).is_ok())
    }

    pub(super) fn metadata(&self) -> &MeasurementSet {
        &self.parts[0].ms
    }

    pub(super) fn by_row(&self, row: usize) -> (usize, &MeasurementSet, usize) {
        let index = self.parts.partition_point(|part| part.offset <= row) - 1;
        let part = &self.parts[index];
        (index, &part.ms, row - part.offset)
    }

    fn fragments(&self, rows: &[usize]) -> Vec<Vec<(usize, usize)>> {
        let mut fragments = vec![Vec::new(); self.parts.len()];
        for (position, &row) in rows.iter().enumerate() {
            let (part, _, physical_row) = self.by_row(row);
            fragments[part].push((position, physical_row));
        }
        fragments
    }

    pub(super) fn array_cells(
        &self,
        column: &str,
        rows: &[usize],
    ) -> MsResult<Vec<Option<ArrayValue>>> {
        let read = |ms: &MeasurementSet, rows: &[usize]| {
            if column == "MODEL_DATA" && !has_column(ms.main_table(), column) {
                return Ok(vec![None; rows.len()]);
            }
            Ok(ms
                .main_table()
                .column_accessor(column)?
                .array_cells_owned_uncached(rows)?)
        };
        if self.parts.len() == 1 {
            return read(&self.parts[0].ms, rows);
        }
        let mut values = vec![None; rows.len()];
        for (part, fragment) in self.parts.iter().zip(self.fragments(rows)) {
            if fragment.is_empty() {
                continue;
            }
            let rows = fragment.iter().map(|&(_, row)| row).collect::<Vec<_>>();
            let cells = read(&part.ms, &rows)?;
            for ((position, _), cell) in fragment.into_iter().zip(cells) {
                values[position] = cell;
            }
        }
        Ok(values)
    }

    pub(super) fn row_metadata(
        &self,
        rows: &[ResolvedMsSelectionRow],
    ) -> MsResult<Vec<RowMetadata>> {
        let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
        if self.parts.len() == 1 {
            return row_metadata(self.parts[0].ms.main_table(), &ids);
        }
        let mut metadata = vec![RowMetadata::default(); rows.len()];
        for (part, fragment) in self.parts.iter().zip(self.fragments(&ids)) {
            if fragment.is_empty() {
                continue;
            }
            let local_ids = fragment.iter().map(|&(_, row)| row).collect::<Vec<_>>();
            for ((position, _), row) in fragment
                .into_iter()
                .zip(row_metadata(part.ms.main_table(), &local_ids)?)
            {
                metadata[position] = row;
            }
        }
        Ok(metadata)
    }

    pub(super) fn legacy_boundaries(
        &self,
        rows: &[ResolvedMsSelectionRow],
    ) -> MsResult<Vec<[i32; 4]>> {
        let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let mut boundaries = vec![[0; 4]; rows.len()];
        for (part, fragment) in self.parts.iter().zip(self.fragments(&ids)) {
            if fragment.is_empty() {
                continue;
            }
            let columns = ["ARRAY_ID", "SCAN_NUMBER", "STATE_ID", "OBSERVATION_ID"];
            let ids = fragment.iter().map(|&(_, row)| row).collect::<Vec<_>>();
            let mut values = part
                .ms
                .main_table()
                .required_scalar_columns_owned_for_rows(&columns, &ids)?;
            for (axis, column) in columns.into_iter().enumerate() {
                let Some(RequiredScalarColumnValues::Int32(values)) = values.remove(column) else {
                    return Err(invalid(format!("{column} must be int32")));
                };
                for (&(slot, _), value) in fragment.iter().zip(values) {
                    boundaries[slot][axis] = value;
                }
            }
        }
        Ok(boundaries)
    }

    pub(super) fn persist_legacy(
        &mut self,
        updates: Vec<super::legacy::LegacyOutputRow>,
        locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    ) -> MsResult<()> {
        let mut remaining = updates.into_iter().peekable();
        let columns = ["WEIGHT", "SIGMA"];
        for part in &mut self.parts {
            let table = part.ms.main_table_mut();
            let limit = part.offset + table.row_count();
            if !remaining.peek().is_some_and(|(row, _)| *row < limit) {
                continue;
            }
            let lock = locks
                .get_mut(&part.path)
                .ok_or_else(|| invalid("missing oldstatwt write lock"))?;
            let mut writer = table.row_accessor_mut().prepare(&columns)?;
            let mut rows = Vec::new();
            while remaining.peek().is_some_and(|(row, _)| *row < limit) {
                let (row, values) = remaining.next().expect("update inside partition");
                let row = row - part.offset;
                rows.push(row);
                writer.seek(row)?;
                for (index, value) in values.into_iter().enumerate() {
                    writer.set_value_at(index, casa_types::Value::Array(value))?;
                }
            }
            writer.flush()?;
            drop(writer);
            lock.record_write();
            table.prepare_write().save_selected_rows(&columns, &rows)?;
            table.discard_persisted_cell_updates(&columns, &rows);
        }
        Ok(())
    }

    pub(super) fn models(
        &self,
        rows: &[ResolvedMsSelectionRow],
    ) -> MsResult<Vec<VisibilityModels>> {
        if self.parts.len() == 1 {
            return VisibilityModels::new(&self.parts[0].ms, rows)
                .map(|models| vec![models])
                .map_err(invalid);
        }
        self.parts
            .iter()
            .map(|part| {
                let start = rows.partition_point(|row| row.row_index < part.offset);
                let end = rows.partition_point(|row| {
                    row.row_index < part.offset + part.ms.main_table().row_count()
                });
                let local = rows[start..end]
                    .iter()
                    .map(|row| {
                        let mut row = row.clone();
                        row.row_index -= part.offset;
                        row
                    })
                    .collect::<Vec<_>>();
                VisibilityModels::new(&part.ms, &local).map_err(invalid)
            })
            .collect()
    }

    pub(super) fn output(
        &mut self,
        rows: &[ResolvedMsSelectionRow],
        options: &StatWtOptions,
        mut locks: Option<&mut BTreeMap<PathBuf, TableWriteLock>>,
    ) -> MsResult<Output> {
        let split = !matches!(options.channel_bin, StatWtChannelBin::Window);
        let mut output = None;
        for part in &mut self.parts {
            let start = rows.partition_point(|row| row.row_index < part.offset);
            let end = rows.partition_point(|row| {
                row.row_index < part.offset + part.ms.main_table().row_count()
            });
            let selected = rows[start..end]
                .iter()
                .map(|row| row.row_index - part.offset)
                .collect::<Vec<_>>();
            let lock = locks
                .as_deref_mut()
                .map(|locks| {
                    locks
                        .get_mut(&part.path)
                        .ok_or_else(|| invalid("missing statwt write lock"))
                })
                .transpose()?;
            let planned = Output::prepare(
                part.ms.main_table_mut(),
                options.data_column,
                split,
                &selected,
                lock,
            )?;
            if let Some(previous) = &output
                && previous != &planned
            {
                return Err(invalid(
                    "inconsistent spectrum initialization across Multi-MS partitions",
                ));
            }
            output = Some(planned);
        }
        output.ok_or_else(|| invalid("statwt requires a physical MAIN table"))
    }

    pub(super) fn persist(
        &mut self,
        columns: &[&str],
        updates: &mut Vec<RowUpdate>,
        locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    ) -> MsResult<()> {
        updates.sort_unstable_by_key(|update| update.row);
        if self.parts.len() == 1 {
            let part = &mut self.parts[0];
            let lock = locks
                .get_mut(&part.path)
                .ok_or_else(|| invalid("missing statwt write lock"))?;
            return persist_updates(part.ms.main_table_mut(), columns, updates, lock);
        }
        let mut remaining = updates.drain(..).peekable();
        for part in &mut self.parts {
            let limit = part.offset + part.ms.main_table().row_count();
            let mut local = Vec::new();
            while remaining.peek().is_some_and(|update| update.row < limit) {
                let mut update = remaining.next().expect("row inside partition");
                update.row -= part.offset;
                local.push(update);
            }
            if !local.is_empty() {
                let lock = locks
                    .get_mut(&part.path)
                    .ok_or_else(|| invalid("missing statwt write lock"))?;
                persist_updates(part.ms.main_table_mut(), columns, &mut local, lock)?;
            }
        }
        Ok(())
    }
}

fn row_metadata(table: &Table, rows: &[usize]) -> MsResult<Vec<RowMetadata>> {
    let mut metadata = table.required_scalar_columns_owned_for_rows(
        &["ARRAY_ID", "SCAN_NUMBER", "STATE_ID", "EXPOSURE"],
        rows,
    )?;
    let Some(RequiredScalarColumnValues::Float64(exposures)) = metadata.remove("EXPOSURE") else {
        return Err(invalid("EXPOSURE must be float64"));
    };
    let mut ints = |name: &str| match metadata.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(invalid(format!("{name} must be int32"))),
    };
    let arrays = ints("ARRAY_ID")?;
    let scans = ints("SCAN_NUMBER")?;
    let states = ints("STATE_ID")?;
    Ok(arrays
        .into_iter()
        .zip(scans)
        .zip(states)
        .zip(exposures)
        .map(|(((array, scan), state), exposure)| RowMetadata {
            array,
            scan,
            state,
            exposure,
        })
        .collect())
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
