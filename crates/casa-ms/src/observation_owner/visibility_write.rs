// SPDX-License-Identifier: LGPL-3.0-or-later

//! Selected visibilities written back into MAIN in place, as CASA writes
//! `MODEL_DATA` and `CORRECTED_DATA`: under casacore's table write lock, in
//! bounded row batches, with nothing of casa-rs's own stored beside them.

use std::path::Path;

use casa_imaging_model::MsColumnKind;
use casa_tables::{ColumnSchema, LockType, Table};
use casa_types::{ArrayValue, Complex32, PrimitiveType, ScalarValue, Value};

use super::ObservationOwnerError;
use crate::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
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
///
/// Only destinations stored in `TiledShapeStMan` or `TiledColumnStMan`, the
/// managers CASA and casa-rs store visibilities in, are written: their save
/// patches the written rows' tiles in place. Any other manager's save
/// rewrites its whole group from loaded cells, which holds every row of
/// MAIN (ADR-0008), so [`retained_bytes`](Self::retained_bytes) and
/// [`begin`](Self::begin) refuse such a destination before anything is
/// changed.
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
    /// created when MAIN lacks it and filled with CASA's initial model, as
    /// tclean creates it ([`initial_model_visibility`]: 1, and 0 in the
    /// cross hands), so rows the run does not predict hold that model.
    ///
    /// The MeasurementSet is opened with retained read locks, and MAIN's is
    /// then upgraded to the write lock. Both wait, as casacore's do, for a
    /// conflicting lock another process holds. When another process holding
    /// a read lock waits to upgrade it too, the kernel refuses one of the
    /// two waits; if it is this one, the write fails with
    /// [`ObservationOwnerError::WriteLockUnavailable`] and releases its read
    /// locks.
    ///
    /// A destination that cannot be written in bounded memory fails with
    /// [`ObservationOwnerError::UnboundedWriteDestination`] before the lock
    /// is taken.
    pub fn begin(
        path: impl AsRef<Path>,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<Self, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let mut measurement_set = MeasurementSet::open_retained_read(path.as_ref())?;
        require_bounded_destinations(measurement_set.main_table(), targets)?;
        if !measurement_set.main_table_mut().lock(LockType::Write, 0)? {
            return Err(ObservationOwnerError::WriteLockUnavailable);
        }
        let has_model = {
            let schema = measurement_set.main_table().schema().ok_or_else(|| {
                MsError::InvalidInput("MeasurementSet MAIN has no schema".to_string())
            })?;
            schema.contains_column("MODEL_DATA")
        };
        if targets.model_data && !has_model {
            // Everything that could refuse the seeding is checked before the
            // column exists: a refusal after it would leave MODEL_DATA
            // holding DATA's clone, which a later write would not reseed.
            let models = initial_models(&measurement_set)?;
            let row_count = measurement_set.main_table().row_count();
            let batch = VISIBILITY_WRITE_BATCH_ROWS as usize;
            for start in (0..row_count).step_by(batch) {
                let rows: Vec<usize> = (start..row_count.min(start + batch)).collect();
                let descriptions = main_data_description_ids(&measurement_set, &rows)?;
                if let Some((row, id)) = rows
                    .iter()
                    .zip(descriptions)
                    .find(|(_, id)| *id >= models.len())
                {
                    return Err(MsError::InvalidInput(format!(
                        "MAIN row {row} names DATA_DESC_ID {id}, which DATA_DESCRIPTION \
                         does not hold"
                    ))
                    .into());
                }
            }
            crate::scratch_columns::clear_before_adding(
                &mut measurement_set,
                VisibilityDataColumn::ModelData,
            )?;
            measurement_set.main_table_mut().add_column(
                ColumnSchema::array_variable("MODEL_DATA", PrimitiveType::Complex32, Some(2)),
                None,
            )?;
            measurement_set
                .main_table_mut()
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL_DATA", "TiledModelData")?;
            for start in (0..row_count).step_by(batch) {
                let rows: Vec<usize> = (start..row_count.min(start + batch)).collect();
                let descriptions = main_data_description_ids(&measurement_set, &rows)?;
                for (&row, description) in rows.iter().zip(descriptions) {
                    queue_visibility_cell(
                        measurement_set.main_table_mut(),
                        "MODEL_DATA",
                        row,
                        models[description].cell(),
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
    /// indices, and each data description's initial model (a value per
    /// product). Tiles cached for reuse go to the process-wide table cache,
    /// charged on its own.
    ///
    /// # Errors
    ///
    /// When the MeasurementSet's spectral-window, polarization or table
    /// descriptions cannot be read, and, as [`begin`](Self::begin) would,
    /// when a destination cannot be written in bounded memory or
    /// `CORRECTED_DATA` is missing: the pass that writes then fails before
    /// it runs.
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
        let descriptions = measurement_set.data_description()?.row_count();
        let (mut cell_values, mut products) = (0, 0);
        for description in 0..descriptions {
            let [correlations, channels] = model_cell_shape(&measurement_set, description)?;
            cell_values = cell_values.max(correlations * channels);
            products = products.max(correlations);
        }
        let main = measurement_set.main_table();
        require_bounded_destinations(main, targets)?;
        let tile = |column: &'static str| -> Result<u64, ObservationOwnerError> {
            // A created MODEL_DATA clones DATA's tiled manager.
            let stored = if column == "MODEL_DATA"
                && !main
                    .schema()
                    .is_some_and(|schema| schema.contains_column(column))
            {
                "DATA"
            } else {
                column
            };
            let bytes = main.array_column_tile_bytes(stored)?.ok_or_else(|| {
                ObservationOwnerError::UnboundedWriteDestination {
                    column,
                    reason: format!("{stored} has no tiles on disk"),
                }
            })?;
            Ok(bytes as u64)
        };
        let mut tiles = 0;
        if targets.model_data {
            tiles += 2 * tile("MODEL_DATA")?;
        }
        if targets.corrected_data {
            tiles += 2 * tile("CORRECTED_DATA")?;
        }
        let cell = (cell_values * size_of::<Complex32>()) as u64;
        let batch = VISIBILITY_WRITE_BATCH_ROWS;
        let entry = size_of::<(usize, ArrayValue)>() as u64;
        let queue = 3 * batch * cell + batch.next_power_of_two() * entry * 3 / 2;
        let columns = u64::from(targets.model_data) + u64::from(targets.corrected_data);
        let rows = batch * (size_of::<usize>() as u64 + STAGING_BYTES_PER_ROW);
        let creation = if targets.model_data {
            let models =
                descriptions * (size_of::<InitialModel>() + products * size_of::<Complex32>());
            batch * (size_of::<Option<ScalarValue>>() + size_of::<usize>()) as u64 + models as u64
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

/// Storage managers whose selected-row save patches the written rows' tiles
/// in place: the managers CASA and casa-rs store visibilities in.
#[cfg(unix)]
const BOUNDED_WRITE_MANAGERS: [&str; 2] = ["TiledShapeStMan", "TiledColumnStMan"];

/// Refuse, before anything is changed, a write whose save would not be
/// bounded: an existing destination stored outside
/// [`BOUNDED_WRITE_MANAGERS`], or a missing `MODEL_DATA` whose clone of
/// DATA's manager would be or cannot be made. A missing `CORRECTED_DATA` is
/// refused too, since it holds the output of a continuum transform.
#[cfg(unix)]
fn require_bounded_destinations(
    main: &Table,
    targets: SelectedVisibilityWriteTargets,
) -> Result<(), ObservationOwnerError> {
    let has = |column: &str| {
        main.schema()
            .is_some_and(|schema| schema.contains_column(column))
    };
    if targets.corrected_data && !has("CORRECTED_DATA") {
        return Err(ObservationOwnerError::MissingCorrectedDataDestination);
    }
    let destinations = [
        ("MODEL_DATA", targets.model_data),
        ("CORRECTED_DATA", targets.corrected_data),
    ];
    for (column, written) in destinations {
        if !written {
            continue;
        }
        let created = !has(column);
        let stored = if created { "DATA" } else { column };
        let Some(manager) = main
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|name| name == stored))
        else {
            return Err(ObservationOwnerError::UnboundedWriteDestination {
                column,
                reason: format!("{stored} has no storage manager on disk"),
            });
        };
        let storage = if created {
            format!(
                "it would be created in a clone of DATA's {}",
                manager.dm_type
            )
        } else {
            format!("it is stored in {}", manager.dm_type)
        };
        if !BOUNDED_WRITE_MANAGERS.contains(&manager.dm_type.as_str()) {
            return Err(ObservationOwnerError::UnboundedWriteDestination {
                column,
                reason: format!(
                    "{storage}; only TiledShapeStMan and TiledColumnStMan columns are written in place"
                ),
            });
        }
        if created && manager.columns.len() != 1 {
            return Err(ObservationOwnerError::UnboundedWriteDestination {
                column,
                reason: format!("{storage}, which DATA shares with other columns"),
            });
        }
    }
    Ok(())
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

/// CASA's initial `MODEL_DATA` for one data description: each product's
/// [`initial_model_visibility`], in `CORR_TYPE` order, over its channels.
struct InitialModel {
    products: Vec<Complex32>,
    channels: usize,
}

impl InitialModel {
    /// The `[correlation, channel]` cell.
    fn cell(&self) -> ArrayValue {
        ArrayValue::Complex32(ndarray::ArrayD::from_shape_fn(
            ndarray::IxDyn(&[self.products.len(), self.channels]),
            |at| self.products[at[0]],
        ))
    }
}

/// Every `DATA_DESCRIPTION` row's [`InitialModel`], by row.
///
/// # Errors
///
/// A description whose window or polarization cannot be read, or whose
/// polarization's `CORR_TYPE` does not list `NUM_CORR` products.
fn initial_models(
    measurement_set: &MeasurementSet,
) -> Result<Vec<InitialModel>, ObservationOwnerError> {
    let descriptions = measurement_set.data_description()?;
    let polarizations = measurement_set.polarization()?;
    (0..descriptions.row_count())
        .map(|description| {
            let [correlations, channels] = model_cell_shape(measurement_set, description)?;
            let polarization = descriptions.polarization_id(description)?;
            let types = polarizations.corr_type(
                usize::try_from(polarization)
                    .map_err(|_| ObservationOwnerError::PredictionAddress)?,
            )?;
            if types.len() != correlations {
                return Err(MsError::InvalidInput(format!(
                    "POLARIZATION row {polarization} has NUM_CORR {correlations} but {} \
                     CORR_TYPE entries",
                    types.len()
                ))
                .into());
            }
            Ok(InitialModel {
                products: types.into_iter().map(initial_model_visibility).collect(),
                channels,
            })
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

#[cfg(all(test, unix))]
mod tests {
    use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::{ArrayD, IxDyn};

    use crate::{
        MeasurementSet, SelectedVisibilityWrite, SelectedVisibilityWriteTargets,
        SyntheticObservationRequest, SyntheticSpectralSetup, generate_synthetic_observation_ms,
        tutorial_vla_a_antennas,
    };

    fn i32_array(values: &[i32], shape: &[usize]) -> Value {
        Value::Array(ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(shape), values.to_vec()).expect("shape"),
        ))
    }

    /// A new `MODEL_DATA` is seeded per data description from that
    /// description's own polarization row, in its `CORR_TYPE` order: two
    /// windows whose descriptions name `RR RL LR LL` and `XX YY XY YX` hold
    /// 1 0 0 1 and 1 1 0 0 in every channel of every row, though the write
    /// touches none.
    #[test]
    fn a_new_model_data_is_seeded_from_each_descriptions_polarization() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("two-polarizations.ms");
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            root.path().join("unused.fits"),
            &path,
            tutorial_vla_a_antennas(),
        );
        request.predict_model = false;
        request.polarization_setup.correlation_count = 4;
        request.duration_seconds = 6.0;
        request.integration_seconds = 2.0;
        request.spectral_windows = vec![
            SyntheticSpectralSetup {
                name: "circular".to_string(),
                start_frequency_hz: 43.0e9,
                channel_width_hz: 2.0e6,
                channel_count: 3,
            },
            SyntheticSpectralSetup {
                name: "linear".to_string(),
                start_frequency_hz: 44.0e9,
                channel_width_hz: 2.0e6,
                channel_count: 3,
            },
        ];
        generate_synthetic_observation_ms(&request).expect("simulate");
        let mut ms = MeasurementSet::open(&path).expect("open");
        assert_eq!(
            ms.polarization()
                .expect("POLARIZATION")
                .corr_type(0)
                .expect("row 0"),
            [5, 6, 7, 8],
            "the simulator's circular products"
        );
        let linear = RecordValue::new(vec![
            RecordField::new(
                "CORR_PRODUCT",
                i32_array(&[0, 1, 0, 1, 0, 1, 1, 0], &[2, 4]),
            ),
            RecordField::new("CORR_TYPE", i32_array(&[9, 12, 10, 11], &[4])),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("NUM_CORR", Value::Scalar(ScalarValue::Int32(4))),
        ]);
        ms.subtable_mut(crate::SubtableId::Polarization)
            .expect("POLARIZATION")
            .add_row(linear)
            .expect("add the linear row");
        ms.data_description_mut()
            .expect("DATA_DESCRIPTION")
            .set_i32(1, "POLARIZATION_ID", 1)
            .expect("point the second window at it");
        ms.save().expect("save");
        drop(ms);

        let write =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
                .expect("begin");
        write.complete().expect("complete");

        let ms = MeasurementSet::open(&path).expect("reopen");
        let main = ms.main_table();
        let rows: Vec<usize> = (0..main.row_count()).collect();
        let descriptions = super::main_data_description_ids(&ms, &rows).expect("DATA_DESC_ID");
        let cells = main
            .column_accessor("MODEL_DATA")
            .and_then(|column| column.array_cells_owned_uncached(&rows))
            .expect("MODEL_DATA");
        let expected = |description: usize| match description {
            0 => [1.0, 0.0, 0.0, 1.0],
            _ => [1.0, 1.0, 0.0, 0.0],
        };
        let mut seen = [0, 0];
        for ((row, description), cell) in rows.iter().zip(descriptions).zip(cells) {
            let Some(ArrayValue::Complex32(cell)) = cell else {
                panic!("row {row} has a MODEL_DATA cell");
            };
            assert_eq!(cell.shape(), [4, 3], "row {row}");
            for ((correlation, _), value) in cell.indexed_iter().map(|(at, v)| ((at[0], at[1]), v))
            {
                assert_eq!(
                    *value,
                    Complex32::new(expected(description)[correlation], 0.0),
                    "row {row}, description {description}, product {correlation}"
                );
            }
            seen[description] += 1;
        }
        assert!(
            seen[0] > 0 && seen[1] > 0,
            "rows of both descriptions: {seen:?}"
        );
    }

    /// A description the initial model cannot be built for (here a
    /// polarization whose `NUM_CORR` is not its `CORR_TYPE`'s length) refuses
    /// the write before `MODEL_DATA` exists, so no half-seeded column holding
    /// `DATA` is left for a later write to take as seeded.
    #[test]
    fn an_unseedable_model_is_refused_before_model_data_exists() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("inconsistent.ms");
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            root.path().join("unused.fits"),
            &path,
            tutorial_vla_a_antennas(),
        );
        request.predict_model = false;
        request.duration_seconds = 4.0;
        request.integration_seconds = 2.0;
        generate_synthetic_observation_ms(&request).expect("simulate");
        let mut ms = MeasurementSet::open(&path).expect("open");
        ms.polarization_mut()
            .expect("POLARIZATION")
            .set_i32(0, "NUM_CORR", 3)
            .expect("an inconsistent NUM_CORR");
        ms.save().expect("save");
        drop(ms);
        let refused =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false));
        assert!(
            matches!(&refused, Err(error) if error.to_string().contains("CORR_TYPE")),
            "{:?}",
            refused.err()
        );
        let ms = MeasurementSet::open(&path).expect("reopen");
        assert!(
            !ms.main_table()
                .schema()
                .expect("schema")
                .contains_column("MODEL_DATA"),
            "no MODEL_DATA was added"
        );
    }
}
