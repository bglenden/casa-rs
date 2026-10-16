// SPDX-License-Identifier: LGPL-3.0-or-later

//! Selected visibilities written back into MAIN in place, as CASA writes
//! `MODEL_DATA` and `CORRECTED_DATA`: under casacore's table write lock, in
//! bounded row batches, with nothing of casa-rs's own stored beside them.

use std::path::Path;

use casa_imaging_model::MsColumnKind;
use casa_tables::{ColumnSchema, LockType, Table};
use casa_types::{ArrayValue, Complex32, PrimitiveType, ScalarValue, Value};

use super::ObservationOwnerError;
use crate::{MeasurementSet, MsError};

const VISIBILITY_WRITE_BATCH_ROWS: u64 = 10_000;

/// Selected visibility columns written by one bounded owner transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityWriteTargets {
    model_data: bool,
    corrected_data: bool,
}

impl SelectedVisibilityWriteTargets {
    /// Construct the exact destination set. At least one destination is required.
    #[must_use]
    pub const fn new(model_data: bool, corrected_data: bool) -> Self {
        Self {
            model_data,
            corrected_data,
        }
    }

    /// Return whether `MODEL_DATA` is written.
    #[must_use]
    pub const fn model_data(self) -> bool {
        self.model_data
    }

    /// Return whether existing `CORRECTED_DATA` is written.
    #[must_use]
    pub const fn corrected_data(self) -> bool {
        self.corrected_data
    }
}

/// Bounded in-place selected-visibility writer following ordinary casacore semantics.
///
/// The writer holds the MAIN write lock until it completes. Completion flushes
/// every selected destination and releases the lock. A failure may leave
/// partially written derived values, as an interrupted CASA write does; the
/// next run recomputes them. No backup, staging column, rollback, snapshot,
/// marker, or content digest is created.
#[cfg(unix)]
pub struct SelectedVisibilityWrite {
    measurement_set: Option<MeasurementSet>,
    targets: SelectedVisibilityWriteTargets,
    pending_cell: Option<PendingVisibilityCells>,
    pending_rows: Vec<usize>,
    completed: bool,
}

struct PendingVisibilityCells {
    row: usize,
    model_data: Option<ndarray::ArrayD<Complex32>>,
    corrected_data: Option<ndarray::ArrayD<Complex32>>,
}

#[cfg(unix)]
impl SelectedVisibilityWrite {
    /// Take the MAIN write lock and start an in-place write. `MODEL_DATA` is
    /// created, zero-filled, when MAIN lacks it, as CASA does.
    ///
    /// The MeasurementSet is opened with retained read locks, and MAIN's is
    /// then upgraded to the write lock. Both wait, as casacore's do, for a
    /// conflicting lock another process holds. When another process holding
    /// a read lock waits to upgrade it too, the kernel refuses one of the
    /// two waits; if it is this one, the write fails with
    /// [`ObservationOwnerError::WriteLockUnavailable`] and releases its read
    /// locks.
    pub fn begin(
        path: impl AsRef<Path>,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<Self, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let mut measurement_set = MeasurementSet::open_retained_read(path.as_ref())?;
        if !measurement_set.main_table_mut().lock(LockType::Write, 0)? {
            return Err(ObservationOwnerError::WriteLockUnavailable);
        }
        let has_model = {
            let schema = measurement_set.main_table().schema().ok_or_else(|| {
                MsError::InvalidInput("MeasurementSet MAIN has no schema".to_string())
            })?;
            schema.contains_column("MODEL_DATA")
        };
        let has_corrected = measurement_set
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("CORRECTED_DATA"));
        if targets.corrected_data && !has_corrected {
            return Err(ObservationOwnerError::MissingCorrectedDataDestination);
        }
        if targets.model_data && !has_model {
            measurement_set.main_table_mut().add_column(
                ColumnSchema::array_variable("MODEL_DATA", PrimitiveType::Complex32, Some(2)),
                None,
            )?;
            measurement_set
                .main_table_mut()
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL_DATA", "TiledModelData")?;
            let row_count = measurement_set.main_table().row_count();
            let batch = VISIBILITY_WRITE_BATCH_ROWS as usize;
            for start in (0..row_count).step_by(batch) {
                let rows: Vec<usize> = (start..row_count.min(start + batch)).collect();
                let descriptions = main_data_description_ids(&measurement_set, &rows)?;
                for (&row, description) in rows.iter().zip(descriptions) {
                    let shape = model_cell_shape(&measurement_set, description)?;
                    queue_visibility_cell(
                        measurement_set.main_table_mut(),
                        "MODEL_DATA",
                        row,
                        ArrayValue::Complex32(ndarray::ArrayD::from_elem(
                            ndarray::IxDyn(&shape),
                            Complex32::new(0.0, 0.0),
                        )),
                    )?;
                }
                persist_visibility_rows(measurement_set.main_table_mut(), &["MODEL_DATA"], &rows)?;
            }
        }
        Ok(Self {
            measurement_set: Some(measurement_set),
            targets,
            pending_cell: None,
            pending_rows: Vec::with_capacity(VISIBILITY_WRITE_BATCH_ROWS as usize),
            completed: false,
        })
    }

    /// Heap bytes a write of `targets` to the MeasurementSet at `path`
    /// holds at most, which the pass that writes admits before it begins.
    ///
    /// Per written column, that is a batch of queued cells of the
    /// MeasurementSet's largest cell, the queue's list of them, the row
    /// being written, and, while a batch is saved, the batch's cells twice
    /// more (gathered for the save and staged by tile) with the column's tile
    /// read and its tile written; the list holds its old and new buffers at
    /// once while it grows. Beside them are the batch's row numbers and, while a missing
    /// `MODEL_DATA` is created, their data-description ids as read and as
    /// indices. Tiles cached for reuse go to the process-wide table cache,
    /// charged on its own.
    ///
    /// # Errors
    ///
    /// When the MeasurementSet's spectral-window, polarization or table
    /// descriptions cannot be read.
    pub fn retained_bytes(
        path: impl AsRef<Path>,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<u64, ObservationOwnerError> {
        /// Staging per batch row: the save's sorted row list and the cells
        /// it gathers by tile; measured at about 140 bytes.
        const STAGING_BYTES_PER_ROW: u64 = 192;
        /// Staging whatever the batch: the MeasurementSet opened for the
        /// write and the table description each save reads; measured at
        /// about 240 KB.
        const STAGING_BYTES: u64 = 1 << 20;
        let measurement_set = MeasurementSet::open(path.as_ref())?;
        let mut cell_values = 0;
        for description in 0..measurement_set.data_description()?.row_count() {
            let [correlations, channels] = model_cell_shape(&measurement_set, description)?;
            cell_values = cell_values.max(correlations * channels);
        }
        let main = measurement_set.main_table();
        let tile = |column: &str| -> Result<u64, ObservationOwnerError> {
            Ok(main.array_column_tile_bytes(column)?.unwrap_or(0) as u64)
        };
        // A created MODEL_DATA clones DATA's tiled manager.
        let model_tile = if main
            .schema()
            .is_some_and(|schema| schema.contains_column("MODEL_DATA"))
        {
            tile("MODEL_DATA")?
        } else {
            tile("DATA")?
        };
        let tiles = 2
            * (u64::from(targets.model_data) * model_tile
                + u64::from(targets.corrected_data) * tile("CORRECTED_DATA")?);
        let cell = (cell_values * size_of::<Complex32>()) as u64;
        let batch = VISIBILITY_WRITE_BATCH_ROWS;
        let entry = size_of::<(usize, ArrayValue)>() as u64;
        let queue = 3 * batch * cell + batch.next_power_of_two() * entry * 3 / 2;
        let columns = u64::from(targets.model_data) + u64::from(targets.corrected_data);
        let rows = batch * (size_of::<usize>() as u64 + STAGING_BYTES_PER_ROW);
        let creation = if targets.model_data {
            batch * (size_of::<Option<ScalarValue>>() + size_of::<usize>()) as u64
        } else {
            0
        };
        Ok(columns * (queue + cell) + tiles + rows + creation + STAGING_BYTES)
    }

    /// Write one selected prediction at its physical row/channel/correlation.
    pub fn write(
        &mut self,
        column: MsColumnKind,
        row: u64,
        channel: u32,
        correlation: u32,
        value: Complex32,
    ) -> Result<(), ObservationOwnerError> {
        let row = usize::try_from(row).map_err(|_| ObservationOwnerError::PredictionAddress)?;
        if self.pending_cell.as_ref().map(|cell| cell.row) != Some(row) {
            self.flush_pending_cell()?;
            let measurement_set = self
                .measurement_set
                .as_ref()
                .ok_or(ObservationOwnerError::TransactionClosed)?;
            // The row's one cell, read past the table's row cache: a cached
            // read would load every row of MAIN (ADR-0008).
            let load = |name: &str| -> Result<ndarray::ArrayD<Complex32>, ObservationOwnerError> {
                let current = measurement_set
                    .main_table()
                    .column_accessor(name)?
                    .array_cells_owned_uncached(&[row])?
                    .pop()
                    .flatten()
                    .ok_or(ObservationOwnerError::PredictionAddress)?;
                let ArrayValue::Complex32(values) = current else {
                    return Err(ObservationOwnerError::PredictionAddress);
                };
                Ok(values)
            };
            self.pending_cell = Some(PendingVisibilityCells {
                row,
                model_data: self
                    .targets
                    .model_data
                    .then(|| load("MODEL_DATA"))
                    .transpose()?,
                corrected_data: self
                    .targets
                    .corrected_data
                    .then(|| load("CORRECTED_DATA"))
                    .transpose()?,
            });
        }
        let pending = self
            .pending_cell
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        let values = match column {
            MsColumnKind::ModelData if self.targets.model_data => pending.model_data.as_mut(),
            MsColumnKind::CorrectedData if self.targets.corrected_data => {
                pending.corrected_data.as_mut()
            }
            _ => None,
        }
        .ok_or(ObservationOwnerError::UnselectedWriteTarget)?;
        let index = [correlation as usize, channel as usize];
        let Some(cell) = values.get_mut(index) else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        *cell = value;
        Ok(())
    }

    /// Flush the in-place write and release the MAIN write lock.
    ///
    /// The selected cells were persisted in bounded row batches, so only the
    /// table metadata (a newly created `MODEL_DATA`) remains to flush.
    pub fn complete(mut self) -> Result<(), ObservationOwnerError> {
        self.flush_pending_cell()?;
        self.persist_pending_rows()?;
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        measurement_set.main_table_mut().unlock_metadata_only()?;
        self.measurement_set = None;
        self.completed = true;
        Ok(())
    }

    fn flush_pending_cell(&mut self) -> Result<(), ObservationOwnerError> {
        let Some(cell) = self.pending_cell.take() else {
            return Ok(());
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        if let Some(values) = cell.model_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "MODEL_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        if let Some(values) = cell.corrected_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "CORRECTED_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        self.pending_rows.push(cell.row);
        if self.pending_rows.len() == VISIBILITY_WRITE_BATCH_ROWS as usize {
            self.persist_pending_rows()?;
        }
        Ok(())
    }

    fn persist_pending_rows(&mut self) -> Result<(), ObservationOwnerError> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let columns = match (self.targets.model_data, self.targets.corrected_data) {
            (true, true) => &["MODEL_DATA", "CORRECTED_DATA"][..],
            (true, false) => &["MODEL_DATA"][..],
            (false, true) => &["CORRECTED_DATA"][..],
            (false, false) => return Err(ObservationOwnerError::EmptyWriteTargets),
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        persist_visibility_rows(
            measurement_set.main_table_mut(),
            columns,
            &self.pending_rows,
        )?;
        self.pending_rows.clear();
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for SelectedVisibilityWrite {
    /// An abandoned write releases the MAIN write lock without flushing the
    /// cells still queued: the batches persisted so far stay, as after an
    /// interrupted CASA write, and MAIN is not rewritten.
    fn drop(&mut self) {
        if !self.completed {
            if let Some(measurement_set) = self.measurement_set.as_mut() {
                let _ = measurement_set.main_table_mut().unlock_metadata_only();
            }
            self.measurement_set = None;
        }
    }
}

/// The `DATA_DESC_ID` of each of `rows`, read without filling the table's
/// row cache.
fn main_data_description_ids(
    measurement_set: &MeasurementSet,
    rows: &[usize],
) -> Result<Vec<usize>, ObservationOwnerError> {
    measurement_set
        .main_table()
        .column_accessor("DATA_DESC_ID")?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .map(|value| match value {
            Some(ScalarValue::Int32(id)) => {
                usize::try_from(id).map_err(|_| ObservationOwnerError::PredictionAddress)
            }
            _ => Err(ObservationOwnerError::PredictionAddress),
        })
        .collect()
}

fn model_cell_shape(
    measurement_set: &MeasurementSet,
    data_description_id: usize,
) -> Result<[usize; 2], ObservationOwnerError> {
    let data_description = measurement_set.data_description()?;
    let spectral_window_id =
        usize::try_from(data_description.spectral_window_id(data_description_id)?)
            .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let polarization_id = usize::try_from(data_description.polarization_id(data_description_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let channels = usize::try_from(
        measurement_set
            .spectral_window()?
            .num_chan(spectral_window_id)?,
    )
    .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let correlations = usize::try_from(measurement_set.polarization()?.num_corr(polarization_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    Ok([correlations, channels])
}

#[cfg(unix)]
fn queue_visibility_cell(
    table: &mut Table,
    column: &str,
    row: usize,
    value: ArrayValue,
) -> Result<(), ObservationOwnerError> {
    {
        let mut prepared = table.row_accessor_mut().prepare(&[column])?;
        prepared.seek(row)?;
        prepared.set_value_at(0, Value::Array(value))?;
    }
    Ok(())
}

#[cfg(unix)]
fn persist_visibility_rows(
    table: &mut Table,
    columns: &[&str],
    rows: &[usize],
) -> Result<(), ObservationOwnerError> {
    table.prepare_write().save_selected_rows(columns, rows)?;
    table.discard_persisted_cell_updates(columns, rows);
    Ok(())
}
