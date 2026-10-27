// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing back to a MeasurementSet v2 through production's bounded
//! selected-row mutation (`MeasurementSetWriteSession`).
//!
//! An updater plans its traversal's MAIN rows up front, every present cell
//! of the requested partitions in delivery order, and takes casacore's
//! write lock for its life. Each batch's samples are their grid cells'
//! rows; a padded cell has no row and receives nothing. A cell the patch
//! supplies whole (every channel and product, and for flags every element
//! set or cleared) is written without reading it; any other is read and
//! merged. Writes are in place, as casacore's are: a failure leaves the
//! rows already written.
//!
//! A `CORRECTED_DATA` the session creates starts as `DATA`'s copy and a
//! `MODEL_DATA` as CASA's initial model (1, 0 in the cross hands), as CASA
//! initializes them, so rows and channels a pass does not reach read as
//! uncorrected data and a unit point source.

use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use casa_observation::{
    AccessRequest, ObservationError, PartitionEntry, PatchValues, ResolvedTarget, UpdateReport,
    Updater, VisibilityBatch, WeightChannels, WritableRole, check_values, uniform_over_channels,
};
use casa_types::ArrayValue;
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayD, ArrayView3, IxDyn};
use num_complex::Complex32;

use super::index::NO_ROW;
use super::{Msv2Observation, OpenOptions};
use crate::column_def::build_table_schema;
use crate::observation_io::update::{Busy, request_error, storage};
use crate::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
use crate::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession,
};

/// An MSv2 opened for update: the observation it reads and the
/// MeasurementSet handle its updaters write through.
pub(in crate::observation_io) struct Msv2Update {
    observation: Msv2Observation,
    writer: Mutex<MeasurementSet>,
}

/// A bound target: the role and the MAIN column it writes.
struct Column {
    role: WritableRole,
    name: String,
}

/// Where the traversal is: the request's partition being written and its
/// next grid time.
#[derive(Clone, Copy)]
struct Position {
    partition: usize,
    time: usize,
}

struct Msv2Updater<'s> {
    update: &'s Msv2Update,
    _busy: Busy<'s>,
    session: MeasurementSetWriteSession,
    columns: Vec<Column>,
    request: AccessRequest,
    /// Each requested partition's selected grid times.
    selections: Vec<Range<usize>>,
    position: Position,
    batch_rows: usize,
    report: UpdateReport,
}

/// One target's cells for a batch's present rows, in row order.
type Cells = Vec<ArrayValue>;

impl Msv2Update {
    #[cfg(unix)]
    pub(in crate::observation_io) fn open(
        path: &std::path::Path,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
        let observation = Msv2Observation::open_with(path, options)?;
        let writer = MeasurementSet::open(path).map_err(super::storage)?;
        Ok(Self {
            observation,
            writer: Mutex::new(writer),
        })
    }

    pub(in crate::observation_io) fn observation(&self) -> &Msv2Observation {
        &self.observation
    }

    /// Plan `request`'s rows and start production's mutation of `targets`.
    pub(in crate::observation_io) fn updater<'s>(
        &'s self,
        request: &AccessRequest,
        targets: &[&ResolvedTarget],
        busy: Busy<'s>,
    ) -> Result<Box<dyn Updater + 's>, ObservationError> {
        let estimate = self.observation.estimate(request)?;
        let catalogue = self.observation.catalogue();
        let selections: Vec<Range<usize>> = estimate
            .partitions
            .iter()
            .map(|partition| partition.first_time..partition.first_time + partition.times)
            .collect();
        let mut rows = Vec::new();
        for (id, selection) in request.partitions.iter().zip(&selections) {
            let grid = &self.observation.partitions[id.0 as usize].grid;
            rows.extend(
                grid.cells(selection.clone())
                    .iter()
                    .filter(|row| **row != NO_ROW)
                    .map(|row| *row as usize),
            );
        }
        let entries: Vec<&PartitionEntry> = request
            .partitions
            .iter()
            .map(|id| catalogue.partition(*id))
            .collect();
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut seeded = 0;
        for target in targets.iter().filter(|target| target.created) {
            add_column(&mut writer, &target.variable)?;
            if target.variable == "MODEL_DATA" && !persisted(writer.main_table(), "MODEL_DATA") {
                seeded += seed_model(&mut writer, &self.observation)?;
            }
        }
        let plans: Vec<MeasurementSetWriteColumnPlan> = targets
            .iter()
            .map(|target| column_plan(target, &entries, writer.main_table()))
            .collect();
        let bytes_per_row: usize = plans.iter().map(|plan| plan.bytes_per_row).sum();
        let samples_per_batch = estimate
            .partitions
            .iter()
            .map(|partition| partition.times_per_batch * partition.baselines)
            .max()
            .unwrap_or(1)
            .max(1);
        let plan = MeasurementSetWritePlan::selected_row_mutation(
            rows,
            plans,
            MeasurementSetWriteResources {
                available_bytes: samples_per_batch.saturating_mul(bytes_per_row.max(1)),
                maximum_live_batches: 1,
                tiled_column_buffer_bytes: casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES,
            },
        )
        .map_err(storage)?;
        let batch_rows = plan.batch_rows().max(1);
        let session = MeasurementSetWriteSession::start_selected_row_mutation(&mut writer, plan)
            .map_err(storage)?;
        Ok(Box::new(Msv2Updater {
            update: self,
            _busy: busy,
            session,
            columns: targets
                .iter()
                .map(|target| Column {
                    role: target.target.role,
                    name: target.variable.clone(),
                })
                .collect(),
            request: request.clone(),
            position: Position {
                partition: 0,
                time: selections.first().map_or(0, |selection| selection.start),
            },
            selections,
            batch_rows,
            report: UpdateReport {
                units_written: seeded,
                ..UpdateReport::default()
            },
        }))
    }
}

/// Whether a data manager stores MAIN's `column`.
fn persisted(main: &casa_tables::Table, column: &str) -> bool {
    main.data_manager_info()
        .iter()
        .any(|manager| manager.columns.iter().any(|name| name == column))
}

/// The bytes one seeding batch may hold.
const SEED_BATCH_BYTES: usize = 64 << 20;

/// Install a `MODEL_DATA` just added to the schema and write CASA's
/// initial model ([`initial_model_visibility`]) into every row, as CASA initializes
/// the column when it adds it; returns the cells written. The storage is
/// cloned from `DATA`'s (the layout casacore is known to read, as applycal
/// installs `CORRECTED_DATA`) and every cell is then overwritten, so once
/// this returns no row of the model holds `DATA`; a pass then writes its
/// own values over the seed. Every MAIN row lies in one partition's grid.
fn seed_model(
    ms: &mut MeasurementSet,
    observation: &Msv2Observation,
) -> Result<usize, ObservationError> {
    let catalogue = observation.catalogue();
    let mut rows = Vec::new();
    let mut row_seeds = Vec::new();
    let mut seeds = Vec::with_capacity(catalogue.partitions.len());
    let mut cell_bytes = 0;
    for (index, entry) in catalogue.partitions.iter().enumerate() {
        let shape = [entry.correlations.len(), entry.frequencies_hz.len()];
        cell_bytes = cell_bytes.max(shape[0] * shape[1] * size_of::<Complex32>());
        seeds.push(ArrayValue::Complex32(ArrayD::from_shape_fn(
            IxDyn(&shape),
            |at| initial_model_visibility(entry.correlations[at[0]].casacore_code()),
        )));
        for row in observation.partitions[index].grid.cells.iter() {
            if *row != NO_ROW {
                rows.push(*row as usize);
                row_seeds.push(index);
            }
        }
    }
    let main_rows = ms.main_table().row_count();
    if rows.len() != main_rows {
        return Err(request_error(format!(
            "MAIN has {main_rows} rows and the session's grids place {}: it changed since the \
             session opened, so MODEL_DATA cannot be seeded in every row",
            rows.len()
        )));
    }
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        rows.clone(),
        vec![MeasurementSetWriteColumnPlan {
            name: "MODEL_DATA".to_string(),
            bytes_per_row: cell_bytes.max(1),
            mode: MeasurementSetColumnWriteMode::Create,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
            tile_shape: None,
            create_source_column: Some("DATA".to_string()),
        }],
        MeasurementSetWriteResources {
            available_bytes: SEED_BATCH_BYTES.max(cell_bytes),
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
    .map_err(storage)?;
    let batch_rows = plan.batch_rows().max(1);
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(ms, plan).map_err(storage)?;
    for (batch, indices) in rows.chunks(batch_rows).zip(row_seeds.chunks(batch_rows)) {
        session
            .write_mutation_batch(
                ms,
                MeasurementSetMutationBatch {
                    row_indices: batch.to_vec(),
                    columns: vec![MeasurementSetMutationColumnBatch {
                        name: "MODEL_DATA".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(
                            indices.iter().map(|index| seeds[*index].clone()).collect(),
                        ),
                    }],
                },
            )
            .map_err(storage)?;
    }
    session.finish_mutation().map_err(storage)?;
    Ok(rows.len())
}

/// Add visibility column `name` (`MODEL_DATA` or `CORRECTED_DATA`) to
/// MAIN's schema with `DATA`'s keywords, as applycal adds `CORRECTED_DATA`,
/// unless it is there. `CORRECTED_DATA`'s storage is then installed by the
/// updater's mutation, cloned from `DATA` values included, as CASA seeds it,
/// so rows a pass skips read as uncorrected; `MODEL_DATA` is installed and
/// seeded with CASA's initial model by [`seed_model`].
fn add_column(ms: &mut MeasurementSet, name: &str) -> Result<(), ObservationError> {
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(name))
    {
        return Ok(());
    }
    let column = match name {
        "MODEL_DATA" => VisibilityDataColumn::ModelData,
        "CORRECTED_DATA" => VisibilityDataColumn::CorrectedData,
        _ => {
            return Err(request_error(format!(
                "{name} is not a column the session creates"
            )));
        }
    };
    let schema = build_table_schema(&[*column.optional_column().column_def()]).map_err(storage)?;
    let definition = schema
        .column(name)
        .ok_or_else(|| request_error(format!("no schema for {name}")))?
        .clone();
    ms.main_table_mut()
        .add_column(definition, None)
        .map_err(storage)?;
    if let Some(keywords) = ms.main_table().column_keywords("DATA").cloned() {
        ms.main_table_mut().set_column_keywords(name, keywords);
    }
    Ok(())
}

/// Production's plan for `target`'s column: replace an existing one, or
/// create `CORRECTED_DATA` seeded from `DATA`, as applycal does.
fn column_plan(
    target: &ResolvedTarget,
    entries: &[&PartitionEntry],
    main: &casa_tables::Table,
) -> MeasurementSetWriteColumnPlan {
    let cell = entries
        .iter()
        .map(|entry| entry.correlations.len() * entry.frequencies_hz.len())
        .max()
        .unwrap_or(0);
    let correlations = entries
        .iter()
        .map(|entry| entry.correlations.len())
        .max()
        .unwrap_or(0);
    let bytes_per_row = match (target.target.role, target.variable.as_str()) {
        (WritableRole::Visibilities, _) => cell * 8,
        (WritableRole::Flags, _) => cell,
        (WritableRole::Weights, "WEIGHT") => correlations * 4,
        (WritableRole::Weights, _) => cell * 4,
    };
    let create = target.created && !persisted(main, &target.variable);
    MeasurementSetWriteColumnPlan {
        name: target.variable.clone(),
        bytes_per_row,
        mode: if create {
            MeasurementSetColumnWriteMode::Create
        } else {
            MeasurementSetColumnWriteMode::Replace
        },
        storage_manager: if create {
            MeasurementSetColumnStorage::TiledShape
        } else {
            MeasurementSetColumnStorage::Persisted
        },
        tile_shape: None,
        create_source_column: create.then(|| "DATA".to_string()),
    }
}

impl Updater for Msv2Updater<'_> {
    fn write(
        &mut self,
        batch: &VisibilityBatch<'_>,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError> {
        let roles: Vec<WritableRole> = self.columns.iter().map(|column| column.role).collect();
        check_values(batch, &roles, values)?;
        let observation = &self.update.observation;
        if !Arc::ptr_eq(batch.catalogue, observation.catalogue()) {
            return Err(request_error(
                "the batch was not delivered by this session's observation",
            ));
        }
        let first = self.place(batch)?;
        let grid = &observation.partitions[batch.partition.0 as usize].grid;
        let cells = grid.cells(first..first + batch.times());
        let entry = batch.catalogue.partition(batch.partition);
        let products = product_indices(batch, entry)?;
        let window = Window {
            first_channel: batch.first_channel,
            channels: entry.frequencies_hz.len(),
            products: &products,
            correlations: entry.correlations.len(),
            whole: batch.channels() == (0..entry.frequencies_hz.len())
                && products.len() == entry.correlations.len(),
        };
        for (column, value) in self.columns.iter().zip(values) {
            refuse_unaddressable(column, value, cells, &window)?;
        }
        let present: Vec<(usize, usize)> = cells
            .iter()
            .enumerate()
            .filter(|(_, row)| **row != NO_ROW)
            .map(|(sample, row)| (sample, *row as usize))
            .collect();
        let mut writer = self
            .update
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut columns = Vec::with_capacity(self.columns.len());
        for (column, value) in self.columns.iter().zip(values) {
            let (cells, read) = column_cells(&writer, column, value, &present, &window)?;
            self.report.units_read += read;
            columns.push(cells);
        }
        for start in (0..present.len()).step_by(self.batch_rows) {
            let end = (start + self.batch_rows).min(present.len());
            let row_indices = present[start..end].iter().map(|(_, row)| *row).collect();
            let batch_columns = self
                .columns
                .iter()
                .zip(&mut columns)
                .map(|(column, cells)| MeasurementSetMutationColumnBatch {
                    name: column.name.clone(),
                    values: MeasurementSetMutationColumnValues::Arrays(
                        cells.drain(..end - start).collect(),
                    ),
                })
                .collect();
            self.session
                .write_mutation_batch(
                    &mut writer,
                    MeasurementSetMutationBatch {
                        row_indices,
                        columns: batch_columns,
                    },
                )
                .map_err(storage)?;
        }
        self.report.batches += 1;
        self.report.samples += present.len();
        self.report.unaddressed += cells.len() - present.len();
        self.report.units_written += present.len() * self.columns.len();
        self.position.time = first + batch.times();
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<UpdateReport, ObservationError> {
        let report = self.report;
        self.session.finish_mutation().map_err(|error| {
            request_error(format!(
                "the traversal was not written batch for batch: {error}"
            ))
        })?;
        Ok(report)
    }
}

impl Msv2Updater<'_> {
    /// `batch`'s first grid time, after checking it is the traversal's
    /// next batch: the same partition from the next time, or, once its
    /// selected times are all written, the next partition from its first
    /// selected time, passing partitions whose selection holds no time
    /// (they deliver no batch).
    fn place(&mut self, batch: &VisibilityBatch<'_>) -> Result<usize, ObservationError> {
        let partitions = &self.request.partitions;
        let observation = &self.update.observation;
        let selections = &self.selections;
        let mut position = self.position;
        let done = |position: Position| selections[position.partition].end == position.time;
        let passes = |partition: usize| {
            partitions[partition] != batch.partition && selections[partition].is_empty()
        };
        if partitions.get(position.partition) != Some(&batch.partition)
            || (done(position) && position.partition + 1 < partitions.len())
        {
            if !done(position) {
                return Err(request_error(
                    "the batch is not the traversal's next: an earlier partition's selected \
                     times are not all written",
                ));
            }
            let mut next = position.partition + 1;
            while next + 1 < partitions.len() && passes(next) {
                next += 1;
            }
            position = Position {
                partition: next,
                time: selections.get(next).map_or(0, |selection| selection.start),
            };
        }
        if partitions.get(position.partition) != Some(&batch.partition)
            || batch.channels() != self.request.channels
        {
            return Err(request_error(
                "the batch is not the traversal's next: an updater is written batch for batch \
                 in delivery order",
            ));
        }
        let grid = &observation.partitions[batch.partition.0 as usize].grid;
        let time = batch.time.seconds_since(0, MjdHighPrec::new(0.0, 0.0));
        let first = grid
            .times_s
            .binary_search_by(|known| known.total_cmp(&time))
            .map_err(|_| request_error("the batch's first time is not on the partition's grid"))?;
        if first != position.time {
            return Err(request_error(
                "the batch is not the traversal's next: an updater is written batch for batch \
                 in delivery order",
            ));
        }
        self.position = position;
        Ok(first)
    }
}

/// The delivered products' positions among the partition's.
fn product_indices(
    batch: &VisibilityBatch<'_>,
    entry: &PartitionEntry,
) -> Result<Vec<usize>, ObservationError> {
    batch
        .correlations
        .iter()
        .map(|product| {
            entry
                .correlations
                .iter()
                .position(|known| known == product)
                .ok_or_else(|| request_error("a delivered product is not the partition's"))
        })
        .collect()
}

/// The part of a MAIN cell a batch covers.
struct Window<'a> {
    first_channel: usize,
    channels: usize,
    products: &'a [usize],
    correlations: usize,
    /// Every channel and product of the cell.
    whole: bool,
}

/// Refuse, before anything is written, a change the store cannot make:
/// clearing a flag where no row exists, or a weight shared by channels
/// changed for some channels only (`WEIGHT` without `WEIGHT_SPECTRUM`).
fn refuse_unaddressable(
    column: &Column,
    value: &PatchValues<'_>,
    cells: &[u32],
    window: &Window<'_>,
) -> Result<(), ObservationError> {
    match value {
        PatchValues::Flags {
            clear: Some(clear), ..
        } => {
            let padded_cleared = cells.iter().enumerate().any(|(sample, row)| {
                *row == NO_ROW && clear.slice(ndarray::s![sample, .., ..]).iter().any(|c| *c)
            });
            if padded_cleared {
                return Err(request_error(
                    "a flag cleared where the MeasurementSet holds no row (padding)",
                ));
            }
        }
        PatchValues::Weights {
            values,
            channels: WeightChannels::Delivered,
        } if column.name == "WEIGHT" => {
            let every_channel = window.first_channel == 0 && values.shape()[1] == window.channels;
            if !every_channel || !uniform_over_channels(values) {
                return Err(request_error(
                    "WEIGHT is one value per row and correlation that every channel reads: \
                     changing it for some channels changes the others too; give every \
                     channel one value, or change all channels (WeightChannels::All)",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

/// `column`'s new cells for the `present` (sample, row) pairs, and how
/// many cells were read to merge into.
fn column_cells(
    ms: &MeasurementSet,
    column: &Column,
    value: &PatchValues<'_>,
    present: &[(usize, usize)],
    window: &Window<'_>,
) -> Result<(Cells, usize), ObservationError> {
    let mut read = 0;
    let mut stored = |row: usize| -> Result<ArrayValue, ObservationError> {
        read += 1;
        ms.main_table()
            .cell_accessor(row, &column.name)
            .and_then(|cell| cell.array())
            .cloned()
            .map_err(|error| super::storage(crate::MsError::from(error)))
    };
    let shape = [window.correlations, window.channels];
    let mut cells = Vec::with_capacity(present.len());
    for &(sample, row) in present {
        let cell = match value {
            PatchValues::Visibilities(values) => {
                let mut cell = if window.whole {
                    ArrayD::from_elem(IxDyn(&shape), Complex32::default())
                } else {
                    complex(stored(row)?)?
                };
                overlay(&mut cell, values, sample, window);
                ArrayValue::Complex32(cell)
            }
            PatchValues::Flags { set, clear } => {
                // A change with neither mask assigns nothing: every flag
                // keeps its stored value.
                let assigns_all = window.whole
                    && (set.is_some() || clear.is_some())
                    && (0..values_len(set, clear, sample)).all(|element| {
                        flag_at(set, sample, element) || flag_at(clear, sample, element)
                    });
                let mut cell = if assigns_all {
                    ArrayD::from_elem(IxDyn(&shape), false)
                } else {
                    boolean(stored(row)?)?
                };
                change_flags(&mut cell, set.as_ref(), clear.as_ref(), sample, window);
                ArrayValue::Bool(cell)
            }
            PatchValues::Weights { values, channels } => ArrayValue::Float32(weight_cell(
                column,
                values,
                *channels,
                sample,
                window,
                || stored(row).and_then(float),
            )?),
        };
        cells.push(cell);
    }
    Ok((cells, read))
}

/// Elements of one sample in a flag change: channels × products.
fn values_len(
    set: &Option<ArrayView3<'_, bool>>,
    clear: &Option<ArrayView3<'_, bool>>,
    sample: usize,
) -> usize {
    set.as_ref()
        .or(clear.as_ref())
        .map_or(0, |mask| mask.slice(ndarray::s![sample, .., ..]).len())
}

/// Whether `mask` holds true at element `element` of `sample`'s
/// `[channel, product]` cell.
fn flag_at(mask: &Option<ArrayView3<'_, bool>>, sample: usize, element: usize) -> bool {
    mask.as_ref().is_some_and(|mask| {
        let products = mask.shape()[2];
        mask[[sample, element / products, element % products]]
    })
}

/// Write `values`' sample into the `[correlation, channel]` cell.
fn overlay<T: Copy>(
    cell: &mut ArrayD<T>,
    values: &ArrayView3<'_, T>,
    sample: usize,
    window: &Window<'_>,
) {
    for (channel, row) in values
        .slice(ndarray::s![sample, .., ..])
        .outer_iter()
        .enumerate()
    {
        for (product, value) in row.iter().enumerate() {
            cell[[window.products[product], window.first_channel + channel]] = *value;
        }
    }
}

/// Apply a flag change to the `[correlation, channel]` cell.
fn change_flags(
    cell: &mut ArrayD<bool>,
    set: Option<&ArrayView3<'_, bool>>,
    clear: Option<&ArrayView3<'_, bool>>,
    sample: usize,
    window: &Window<'_>,
) {
    for (mask, to) in [(set, true), (clear, false)] {
        let Some(mask) = mask else { continue };
        for (channel, row) in mask
            .slice(ndarray::s![sample, .., ..])
            .outer_iter()
            .enumerate()
        {
            for (product, on) in row.iter().enumerate() {
                if *on {
                    cell[[window.products[product], window.first_channel + channel]] = to;
                }
            }
        }
    }
}

/// The weight cell of `sample`: `WEIGHT` `[correlation]` takes each
/// product's one value; `WEIGHT_SPECTRUM` `[correlation, channel]` takes the
/// delivered channels, or every channel for [`WeightChannels::All`].
fn weight_cell(
    column: &Column,
    values: &ArrayView3<'_, f32>,
    channels: WeightChannels,
    sample: usize,
    window: &Window<'_>,
    stored: impl FnOnce() -> Result<ArrayD<f32>, ObservationError>,
) -> Result<ArrayD<f32>, ObservationError> {
    let all_products = window.products.len() == window.correlations;
    if column.name == "WEIGHT" {
        let mut cell = if all_products {
            ArrayD::zeros(IxDyn(&[window.correlations]))
        } else {
            stored()?
        };
        for (product, corr) in window.products.iter().enumerate() {
            cell[[*corr]] = values[[sample, 0, product]];
        }
        return Ok(cell);
    }
    let every_channel = channels == WeightChannels::All || window.whole;
    let mut cell = if every_channel && all_products {
        ArrayD::zeros(IxDyn(&[window.correlations, window.channels]))
    } else {
        stored()?
    };
    if channels == WeightChannels::All {
        for (product, corr) in window.products.iter().enumerate() {
            let value = values[[sample, 0, product]];
            cell.slice_mut(ndarray::s![*corr, ..]).fill(value);
        }
    } else {
        overlay(&mut cell, values, sample, window);
    }
    Ok(cell)
}

fn complex(value: ArrayValue) -> Result<ArrayD<Complex32>, ObservationError> {
    match value {
        ArrayValue::Complex32(cell) => Ok(cell),
        _ => Err(super::invalid("a visibility cell is not complex64")),
    }
}

fn boolean(value: ArrayValue) -> Result<ArrayD<bool>, ObservationError> {
    match value {
        ArrayValue::Bool(cell) => Ok(cell),
        _ => Err(super::invalid("a FLAG cell is not boolean")),
    }
}

fn float(value: ArrayValue) -> Result<ArrayD<f32>, ObservationError> {
    match value {
        ArrayValue::Float32(cell) => Ok(cell),
        _ => Err(super::invalid("a weight cell is not float32")),
    }
}
