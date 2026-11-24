// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's precedence for scratch and virtual model visibilities.

mod fourier;
mod mosaic_pointing;
mod single_dish;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use casa_components::{ComponentList, Polarisation};
use casa_ms::columns::direction_columns::DirectionColumn;
use casa_ms::derived::engine::{MsCalEngine, raw_field_phase_direction};
use casa_ms::{
    MeasurementSet, MsError, SubTable, SubtableId, VirtualModelEntry, VirtualModelKind,
    virtual_model,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{MEpoch, MeasFrame, UvwMachine};
use casa_types::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;

use crate::ft::{ComponentVoltageCache, PreparedVoltagePattern};

use super::grouping::{BucketedRow, SelectedSolveRow, VisibilityBuffer};
use super::{GainSolveCombine, GainSolveError, GainSolveModelSource, get_i32};
use fourier::{FourierMerge, FourierModel, FourierRow};
use mosaic_pointing::{FeedOffsets, MosaicPointing};
use single_dish::{PointingQuery, SingleDishModel};

pub(super) struct ModelInputs {
    read_column: bool,
    uncached_main_reads: bool,
    virtual_models: BTreeMap<i32, Arc<[Entry]>>,
    virtual_refusals: BTreeMap<i32, String>,
    frequencies: BTreeMap<i32, (Vec<f64>, FrequencyRef)>,
    geometry: Option<MsCalEngine>,
    dynamic_fields: BTreeSet<i32>,
    pub(super) phase_times: BTreeMap<usize, f64>,
    pub(super) plot_phase_references: BTreeMap<usize, (i32, f64)>,
    finite_fields: BTreeSet<i32>,
    model_buffers: BTreeMap<usize, Arc<ModelBuffer>>,
    single_dish_pointings: HashMap<PointingQuery, BTreeMap<usize, MDirection>>,
    single_dish_feeds: FeedOffsets,
    component_voltage: Option<ComponentVoltageCache>,
}

struct ModelBuffer {
    selection: [i32; 4],
    focus_rows: Vec<FocusSample>,
    legacy_mosaic_pointing: Option<MosaicPointing>,
}

struct FocusSample {
    index: usize,
    antennas: [i32; 2],
}

struct Entry {
    selection: EntrySelection,
    prediction: Prediction,
    scratch_channels: Vec<[i32; 4]>,
}

enum EntrySelection {
    Rows(Vec<[i32; 4]>),
    FieldAndWindow { fields: Vec<i32>, windows: Vec<i32> },
}

impl EntrySelection {
    fn matches(&self, combination: [i32; 4]) -> bool {
        match self {
            Self::Rows(rows) => rows.contains(&combination),
            Self::FieldAndWindow { fields, windows } => {
                fields.contains(&combination[0]) && windows.contains(&combination[1])
            }
        }
    }
}

enum Prediction {
    Components(ComponentList),
    FourierGrid(Box<FourierModel>),
    SingleDish(Box<SingleDishModel>),
}

impl ModelInputs {
    pub(super) fn new(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
        source: GainSolveModelSource,
    ) -> Result<Self, GainSolveError> {
        Self::load(ms, rows, source, false)
    }

    /// Subtraction reports a malformed recipe when its first selected
    /// buffer is reached, after writing any preceding valid predictions.
    pub(super) fn for_uvsub(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
    ) -> Result<Self, GainSolveError> {
        Self::load(ms, rows, GainSolveModelSource::FromMeasurementSet, true)
    }

    fn load(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
        source: GainSolveModelSource,
        defer_refusals: bool,
    ) -> Result<Self, GainSolveError> {
        let read_column = source == GainSolveModelSource::ModelColumn
            || (source == GainSolveModelSource::FromMeasurementSet
                && ms
                    .main_table()
                    .schema()
                    .is_some_and(|schema| schema.contains_column("MODEL_DATA")));
        let mut inputs = Self::empty(read_column);
        if source != GainSolveModelSource::FromMeasurementSet {
            return Ok(inputs);
        }
        let fields: BTreeSet<_> = rows.iter().map(|row| row.field_id).collect();
        let mut records = BTreeMap::<_, Arc<[Entry]>>::new();
        for field in fields {
            if let Some(record) =
                virtual_model(ms, field).map_err(|source| read_error(ms, source))?
            {
                let entries = match records.entry(record.key) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let decoded = match decode_entries(&record.record) {
                            Ok(decoded) => decoded,
                            Err(GainSolveError::VirtualModel(error)) if defer_refusals => {
                                inputs.virtual_refusals.insert(field, error);
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        entry.insert(decoded.into())
                    }
                };
                inputs.virtual_models.insert(field, Arc::clone(entries));
            }
        }
        inputs.load_geometry(ms, rows)?;
        Ok(inputs)
    }

    pub(super) fn from_entries(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
        entries: &[VirtualModelEntry],
    ) -> Result<Self, GainSolveError> {
        let mut inputs = Self::empty(false);
        let entries: Arc<[Entry]> = entries
            .iter()
            .map(|entry| {
                decode_entry(
                    entry.kind,
                    EntrySelection::Rows(entry.index_combination.clone()),
                    &entry.container,
                )
                .map(|mut model| {
                    model.scratch_channels = entry.channel_selection.clone();
                    model
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into();
        for row in rows {
            inputs
                .virtual_models
                .entry(row.field_id)
                .or_insert_with(|| Arc::clone(&entries));
        }
        inputs.load_geometry(ms, rows)?;
        Ok(inputs)
    }

    fn empty(read_column: bool) -> Self {
        Self {
            read_column,
            uncached_main_reads: false,
            virtual_models: BTreeMap::new(),
            virtual_refusals: BTreeMap::new(),
            frequencies: BTreeMap::new(),
            geometry: None,
            dynamic_fields: BTreeSet::new(),
            phase_times: BTreeMap::new(),
            plot_phase_references: BTreeMap::new(),
            finite_fields: BTreeSet::new(),
            model_buffers: BTreeMap::new(),
            single_dish_pointings: HashMap::new(),
            single_dish_feeds: FeedOffsets::default(),
            component_voltage: None,
        }
    }

    pub(super) fn set_component_voltage(&mut self, voltage: PreparedVoltagePattern) {
        self.component_voltage = Some(ComponentVoltageCache::new(voltage));
    }

    pub(super) fn use_calibration_field_directions(
        &mut self,
        ms: &MeasurementSet,
    ) -> Result<(), GainSolveError> {
        if !self.dynamic_fields.is_empty() {
            self.geometry =
                Some(MsCalEngine::for_calibration(ms).map_err(|source| read_error(ms, source))?);
        }
        Ok(())
    }

    fn load_geometry(
        &mut self,
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
    ) -> Result<(), GainSolveError> {
        self.finite_fields = self.virtual_models.iter().filter_map(|(&field, models)| {
            models.iter().any(|entry| matches!(&entry.prediction, Prediction::FourierGrid(grid) if grid.needs_refocus())).then_some(field)
        }).collect();
        if self
            .virtual_models
            .values()
            .any(|entries| !entries.is_empty())
        {
            let spectral_window = ms
                .spectral_window()
                .map_err(|source| read_error(ms, source))?;
            let windows: BTreeSet<_> = rows
                .iter()
                .filter(|row| self.virtual_models.contains_key(&row.field_id))
                .map(|row| row.data_spw_id)
                .collect();
            for window in windows {
                let frequencies = spectral_window
                    .chan_freq(window as usize)
                    .map_err(|source| read_error(ms, source))?;
                let reference = spectral_window
                    .frequency_reference(window as usize)
                    .map_err(|source| read_error(ms, source))?;
                self.frequencies.insert(window, (frequencies, reference));
            }
            let engine = MsCalEngine::new(ms)
                .and_then(|engine| engine.with_field_ephemerides(ms))
                .map_err(|source| read_error(ms, source))?;
            let field = ms.field().map_err(|source| read_error(ms, source))?;
            for &id in self.virtual_models.keys() {
                let index = id as usize;
                if engine.field_moves(index)
                    || field
                        .num_poly(index)
                        .map_err(|source| read_error(ms, source))?
                        > 0
                {
                    self.dynamic_fields.insert(id);
                }
            }
            self.geometry = Some(engine);
        }
        Ok(())
    }

    pub(super) fn reads_column(&self, field: i32) -> bool {
        self.read_column
            && !self.virtual_models.contains_key(&field)
            && !self.virtual_refusals.contains_key(&field)
    }

    pub(super) fn has_virtual_refusals(&self) -> bool {
        !self.virtual_refusals.is_empty()
    }

    pub(super) fn virtual_refusal(&self, field: i32) -> Option<GainSolveError> {
        self.virtual_refusals
            .get(&field)
            .cloned()
            .map(GainSolveError::VirtualModel)
    }

    pub(super) fn dynamic_fields(&self) -> &BTreeSet<i32> {
        &self.dynamic_fields
    }

    pub(super) fn uses_virtual_model(&self, field: i32) -> bool {
        self.virtual_models
            .get(&field)
            .is_some_and(|entries| !entries.is_empty())
    }

    pub(super) fn uses_fourier(&self, field: i32) -> bool {
        self.virtual_models.get(&field).is_some_and(|entries| {
            entries.iter().any(|entry| {
                matches!(
                    entry.prediction,
                    Prediction::FourierGrid(_) | Prediction::SingleDish(_)
                )
            })
        })
    }

    pub(super) fn needs_buffers(&self) -> bool {
        !self.virtual_models.is_empty()
    }

    pub(super) fn bind_buffers(
        &mut self,
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
        combine: GainSolveCombine,
    ) -> Result<(), GainSolveError> {
        self.bind_model_buffers(ms, rows, combine, None)
    }

    pub(super) fn bind_transform_buffers(
        &mut self,
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
        states: &BTreeMap<usize, i32>,
    ) -> Result<(), GainSolveError> {
        self.uncached_main_reads = true;
        self.bind_model_buffers(ms, rows, GainSolveCombine::default(), Some(states))
    }

    fn bind_model_buffers(
        &mut self,
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
        combine: GainSolveCombine,
        states: Option<&BTreeMap<usize, i32>>,
    ) -> Result<(), GainSolveError> {
        if !self.needs_buffers() {
            return Ok(());
        }
        let mut buffers =
            BTreeMap::<(VisibilityBuffer, Option<i32>), Vec<&SelectedSolveRow>>::new();
        for row in rows
            .iter()
            .filter(|(row, _, _)| self.virtual_models.contains_key(&row.field_id))
        {
            buffers
                .entry((
                    VisibilityBuffer::new(row, combine),
                    states.map(|states| states[&row.0.row_index]),
                ))
                .or_default()
                .push(row.0);
        }
        let mosaic_fields: BTreeSet<_> = self
            .virtual_models
            .iter()
            .filter_map(|(&field, entries)| {
                entries
                    .iter()
                    .any(|entry| {
                        matches!(&entry.prediction,
                            Prediction::FourierGrid(grid) if grid.uses_legacy_mosaic_pointing()
                        )
                    })
                    .then_some(field)
            })
            .collect();
        let mut pointings =
            if !mosaic_fields.is_empty() && ms.subtable(SubtableId::Pointing).is_some() {
                let queries: Vec<_> = buffers
                    .values()
                    .map(|rows| rows[0])
                    .filter(|row| mosaic_fields.contains(&row.field_id))
                    .map(|row| (row.antenna1, row.time_seconds, row.interval_seconds))
                    .collect();
                ms.pointing()
                    .and_then(|pointing| pointing.legacy_buffer_directions(&queries))
                    .map_err(|source| read_error(ms, source))?
            } else {
                Vec::new()
            }
            .into_iter();
        let feeds = if mosaic_fields.is_empty() {
            FeedOffsets::default()
        } else {
            FeedOffsets::load(ms).map_err(|source| read_error(ms, source))?
        };
        for ((_, state), rows) in buffers {
            let first = rows[0];
            let state =
                state.map_or_else(|| get_i32(ms.main_table(), first.row_index, "STATE_ID"), Ok)?;
            let selection = [first.field_id, first.data_spw_id, first.scan_number, state];
            let focus_rows = if self.finite_fields.contains(&first.field_id) {
                rows.iter()
                    .map(|row| FocusSample {
                        index: row.row_index,
                        antennas: [row.antenna1, row.antenna2],
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let buffer = Arc::new(ModelBuffer {
                selection,
                focus_rows,
                legacy_mosaic_pointing: if mosaic_fields.contains(&first.field_id) {
                    Some(feeds.direction(
                        ms,
                        self.geometry.as_ref().expect("mosaic has geometry"),
                        first,
                        pointings.next().flatten(),
                    )?)
                } else {
                    None
                },
            });
            for row in rows {
                self.model_buffers
                    .insert(row.row_index, Arc::clone(&buffer));
            }
        }
        self.load_single_dish_pointings(ms, rows)
    }

    fn load_single_dish_pointings(
        &mut self,
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
    ) -> Result<(), GainSolveError> {
        let queries: HashSet<_> = self
            .virtual_models
            .values()
            .flat_map(|entries| entries.iter())
            .filter_map(|entry| match &entry.prediction {
                Prediction::SingleDish(dish) => Some(dish.pointing()),
                _ => None,
            })
            .collect();
        if queries.is_empty() {
            return Ok(());
        }
        let pointing = ms
            .subtable(SubtableId::Pointing)
            .is_some()
            .then(|| ms.pointing())
            .transpose()
            .map_err(|error| read_error(ms, error))?;
        let Some(pointing) = pointing.filter(|pointing| pointing.row_count() > 0) else {
            self.single_dish_feeds =
                FeedOffsets::load(ms).map_err(|error| read_error(ms, error))?;
            return Ok(());
        };
        let engine = self.geometry.as_ref().expect("single-dish geometry");
        for query in queries {
            let selected: Vec<_> = rows.iter().map(|(row, _, _)| *row).filter(|row| {
                if row.antenna1 != row.antenna2 || row.flag_row {
                    return false;
                }
                let Some(buffer) = self.model_buffers.get(&row.row_index) else { return false };
                self.virtual_models[&row.field_id].iter().any(|entry| {
                    entry.selection.matches(buffer.selection)
                        && matches!(&entry.prediction, Prediction::SingleDish(dish) if dish.pointing() == query)
                })
            }).collect();
            let requests: Vec<_> = selected
                .iter()
                .map(|row| (row.antenna1, row.time_seconds, row.interval_seconds))
                .collect();
            let target = query.convert_first.map(|reference| (engine, reference));
            let directions = pointing
                .single_dish_directions(&requests, &query.column, target)
                .map_err(|error| read_error(ms, error))?;
            self.single_dish_pointings.insert(
                query.clone(),
                selected
                    .into_iter()
                    .zip(directions)
                    .filter_map(|(row, direction)| {
                        direction.map(|direction| (row.row_index, direction))
                    })
                    .collect(),
            );
        }
        Ok(())
    }

    /// A defined virtual model overrides MODEL_DATA. CASA uses a unit point
    /// model if none of the field's entries matches this visibility buffer.
    pub(super) fn predict(
        &self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        correlations: &[i32],
        raw_flags: &ArrayD<bool>,
        uvw_cell: Option<&ArrayValue>,
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        self.predict_model(ms, row, correlations, (raw_flags, None), false, uvw_cell)
    }

    pub(super) fn predict_model(
        &self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        correlations: &[i32],
        (raw_flags, previous): (&ArrayD<bool>, Option<ArrayD<Complex32>>),
        scratch_model: bool,
        uvw_cell: Option<&ArrayValue>,
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        if let Some(error) = self.virtual_refusal(row.field_id) {
            return Err(error);
        }
        let Some(entries) = self.virtual_models.get(&row.field_id) else {
            return Ok(None);
        };
        if entries.is_empty() {
            return Ok(None);
        }
        let buffer = &self.model_buffers[&row.row_index];
        let mut matching = entries
            .iter()
            .filter(|entry| entry.selection.matches(buffer.selection))
            .peekable();
        if matching.peek().is_none() {
            return Ok(None);
        }
        // CASA restores the last component recipe registered for a visibility
        // buffer. A MeasurementSet can retain older overlapping recipes, for
        // example after replacing a setjy model with imager.ft.
        let virtual_components = (!scratch_model)
            .then(|| {
                entries.iter().rev().find(|entry| {
                    matches!(&entry.prediction, Prediction::Components(_))
                        && entry.selection.matches(buffer.selection)
                })
            })
            .flatten();
        let engine = self
            .geometry
            .as_ref()
            .expect("virtual models have geometry");
        // A vector PlotMS time iterator can span fields within an input chunk.
        // Keep the row's model recipe and UVW while using the chunk's direction.
        let (phase_field, phase_time) = self
            .plot_phase_references
            .get(&row.row_index)
            .copied()
            .unwrap_or_else(|| {
                let time = self
                    .phase_times
                    .get(&row.row_index)
                    .copied()
                    .unwrap_or(row.time_seconds);
                (row.field_id, time)
            });
        let field = field_direction(ms, engine, phase_field as usize, phase_time)
            .map_err(|source| read_error(ms, source))?;
        let frame = if self.component_voltage.is_some() {
            engine
                .spectral_frame_observatory_direction(row.time_seconds, field.clone())
                .map_err(|source| read_error(ms, source))?
        } else {
            MeasFrame::new()
                .with_epoch(MEpoch::from_mjd(
                    row.time_seconds / 86400.0,
                    engine.time_reference(),
                ))
                .with_position(engine.observatory_position().clone())
                .with_direction(field.clone())
        };
        let uvw = match uvw_cell {
            Some(cell) => uvw_coordinates(cell)?,
            None => uvw(ms, row.row_index, self.uncached_main_reads)?,
        };
        let (frequencies, frequency_ref) = &self.frequencies[&row.data_spw_id];
        let shape = [correlations.len(), frequencies.len()];
        let mut model = match previous {
            Some(model) if model.shape() != shape => {
                return Err(model_error("MODEL_DATA does not match its layout"));
            }
            Some(model) if model.t().is_standard_layout() => model,
            Some(model) => ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| model[index]),
            None => ArrayD::<Complex32>::zeros(IxDyn(&shape).f()),
        };
        // The cube sky equation adds each result from a working VisBuffer.
        // Unassigned cells retain their last value in that buffer.
        let multiple_grids = scratch_model
            && matching
                .clone()
                .filter(|entry| matches!(entry.prediction, Prediction::FourierGrid(_)))
                .nth(1)
                .is_some();
        let mut scratch_buffer = None;
        let mut components_written = false;
        for entry in matching {
            let grid_row = || FourierRow {
                ms,
                row,
                correlations,
                frequencies,
                raw_flags,
                frequency_ref: *frequency_ref,
                engine,
                field: &field,
                phase_time,
                frame: &frame,
                uvw,
                uncached_main_reads: self.uncached_main_reads,
                focus_rows: &buffer.focus_rows,
                legacy_mosaic_pointing: buffer.legacy_mosaic_pointing.as_ref(),
                scratch_channels: entry
                    .scratch_channels
                    .iter()
                    .find(|channels| channels[0] == row.data_spw_id)
                    .copied(),
            };
            let components = match &entry.prediction {
                Prediction::Components(components) => {
                    if virtual_components.is_some_and(|active| !std::ptr::eq(active, entry)) {
                        continue;
                    }
                    components_written = true;
                    components
                }
                Prediction::SingleDish(dish) => {
                    if row.antenna1 != row.antenna2 || row.flag_row {
                        continue;
                    }
                    let corrected;
                    let pointing = match self.single_dish_pointings.get(dish.pointing()) {
                        Some(pointings) => pointings.get(&row.row_index),
                        None if !self.single_dish_feeds.is_empty() => {
                            corrected = self
                                .single_dish_feeds
                                .direction(ms, engine, row, Some(field.clone()))?
                                .direction;
                            Some(&corrected)
                        }
                        None => Some(&field),
                    };
                    if let Some(direction) = pointing {
                        dish.predict(&grid_row(), direction, &mut model)?;
                    }
                    continue;
                }
                Prediction::FourierGrid(grid) => {
                    let merge = if !scratch_model {
                        FourierMerge::VirtualSum
                    } else if multiple_grids {
                        FourierMerge::ScratchAccumulate(
                            scratch_buffer
                                .get_or_insert_with(|| model.clone())
                                .as_slice_memory_order_mut()
                                .expect("contiguous scratch buffer"),
                        )
                    } else if components_written {
                        FourierMerge::ScratchReplaceComponents
                    } else {
                        FourierMerge::ScratchSum
                    };
                    grid.predict(grid_row(), &mut model, merge)?;
                    components_written = false;
                    continue;
                }
            };
            self.add_component_prediction(components, grid_row(), &mut model)?;
        }
        Ok(Some(model))
    }
    fn add_component_prediction(
        &self,
        components: &ComponentList,
        sample: FourierRow<'_>,
        model: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let FourierRow {
            ms,
            row,
            correlations,
            frequencies,
            engine,
            field,
            frame,
            uvw,
            scratch_channels,
            ..
        } = sample;
        let (basis, offset) = component_basis(correlations)?;
        let antenna_frame = if self.component_voltage.is_some() {
            // ComponentFTMachine rotates baselines in the antenna-zero
            // frame. BeamSkyJones uses the observatory frame separately.
            Some(
                engine
                    .antenna_frame(row.time_seconds, 0)
                    .map_err(|source| read_error(ms, source))?,
            )
        } else {
            None
        };
        let projection_frame = antenna_frame.as_ref().unwrap_or(frame);
        let spectra = if let Some(voltage) = &self.component_voltage {
            let start = scratch_channels.map_or(0, |channels| channels[1] as usize);
            Some(
                voltage
                    .spectra(
                        components,
                        frequencies,
                        basis,
                        (row.data_spw_id, row.time_seconds, start),
                        frame,
                    )
                    .map_err(model_error)?,
            )
        } else {
            None
        };
        for (index, component) in components.iter().enumerate() {
            let projection = UvwMachine::with_frame(
                component.shape().reference_direction(),
                field,
                projection_frame,
            )
            .map_err(model_error)?;
            let (rotated, phase) = projection.convert_projected([-uvw[0], -uvw[1], uvw[2]]);
            let baseline = [-rotated[0], -rotated[1], rotated[2]];
            let apparent = spectra
                .as_ref()
                .map(|spectra| {
                    let shape = component.transform()?.at_baseline(baseline);
                    Ok::<_, casa_components::ComponentError>((
                        spectra.component(index, frequencies.len()),
                        shape,
                    ))
                })
                .transpose()
                .map_err(model_error)?;
            for (channel, &frequency) in frequencies.iter().enumerate() {
                let visibility = if let Some((flux, shape)) = &apparent {
                    let scale = shape.at_frequency(frequency).map_err(model_error)?;
                    flux[channel].map(|value| value * scale)
                } else {
                    component
                        .visibility(baseline, frequency, basis)
                        .map_err(model_error)?
                };
                let rotation = Complex64::from_polar(
                    1.0,
                    std::f64::consts::TAU * phase * frequency / 299_792_458.0,
                );
                for (correlation, &code) in correlations.iter().enumerate() {
                    let value = visibility[(code - offset) as usize] * rotation;
                    model[[correlation, channel]] +=
                        Complex32::new(value.re as f32, value.im as f32);
                }
            }
        }
        Ok(())
    }
}

fn component_basis(correlations: &[i32]) -> Result<(Polarisation, i32), GainSolveError> {
    let (basis, offset) = match correlations.first().copied() {
        Some(1..=4) => (Polarisation::Stokes, 1),
        Some(5..=8) => (Polarisation::Circular, 5),
        Some(9..=12) => (Polarisation::Linear, 9),
        _ => {
            return Err(model_error(
                "virtual component prediction requires Stokes, linear, or circular correlations",
            ));
        }
    };
    if correlations
        .iter()
        .any(|&code| !(offset..offset + 4).contains(&code))
    {
        return Err(model_error(
            "virtual component prediction requires one correlation basis",
        ));
    }
    Ok((basis, offset))
}

fn decode_entries(record: &RecordValue) -> Result<Vec<Entry>, GainSolveError> {
    if record.get("version").is_some() {
        return VirtualModelEntry::entries(record)
            .map_err(model_error)?
            .into_iter()
            .map(|entry| {
                let mut model = decode_entry(
                    entry.kind,
                    EntrySelection::Rows(entry.index_combination),
                    &entry.container,
                )?;
                // These native recipes replay the writer's channel span.
                // The SDGrid span also controls its single-channel bypass.
                let saved_span = match entry.container.get("name") {
                    Some(Value::Scalar(ScalarValue::String(name))) => {
                        matches!(
                            name.as_str(),
                            "nPBWProjectFT" | "PBMosaicFT" | crate::ft::legacy_aw::MACHINE
                        ) || (name == "SDGrid"
                            && entry.container.get("casars_live_image_frequency").is_some())
                    }
                    _ => false,
                };
                if saved_span {
                    if entry
                        .channel_selection
                        .iter()
                        .any(|&[spw, start, count, step]| {
                            spw < 0 || start < 0 || count <= 0 || step <= 0
                        })
                    {
                        return Err(model_error("invalid saved Fourier channel selection"));
                    }
                    model.scratch_channels = entry.channel_selection;
                }
                Ok(model)
            })
            .collect();
    }
    let mut entries = Vec::new();
    for (kind, count, prefix) in [
        (VirtualModelKind::FtMachine, "numft", "ft"),
        (VirtualModelKind::ComponentList, "numcl", "cl"),
    ] {
        let Some(Value::Scalar(ScalarValue::Int32(count))) = record.get(count) else {
            continue;
        };
        for index in 0..*count {
            let name = format!("{prefix}_{index}");
            let Some(Value::Record(entry)) = record.get(&name) else {
                return Err(model_error(format!(
                    "legacy virtual model has no {name} record"
                )));
            };
            let Some(Value::Record(container)) = entry.get("container") else {
                return Err(model_error(format!("legacy model {name} has no container")));
            };
            let selection = EntrySelection::FieldAndWindow {
                fields: legacy_ids(entry, "fields")?,
                windows: legacy_ids(entry, "spws")?,
            };
            entries.push(decode_entry(kind, selection, container)?);
        }
    }
    Ok(entries)
}

fn decode_entry(
    kind: VirtualModelKind,
    selection: EntrySelection,
    container: &RecordValue,
) -> Result<Entry, GainSolveError> {
    let prediction = match kind {
        VirtualModelKind::ComponentList => {
            Prediction::Components(ComponentList::from_record(container).map_err(model_error)?)
        }
        VirtualModelKind::FtMachine if matches!(container.get("name"), Some(Value::Scalar(ScalarValue::String(name))) if name == "SDGrid") => {
            Prediction::SingleDish(Box::new(SingleDishModel::new(container.clone())?))
        }
        VirtualModelKind::FtMachine => {
            Prediction::FourierGrid(Box::new(FourierModel::new(container.clone())))
        }
    };
    Ok(Entry {
        selection,
        prediction,
        scratch_channels: Vec::new(),
    })
}

fn legacy_ids(record: &RecordValue, field: &str) -> Result<Vec<i32>, GainSolveError> {
    match record.get(field) {
        Some(Value::Array(ArrayValue::Int32(values))) if values.ndim() == 1 => {
            Ok(values.iter().copied().collect())
        }
        _ => Err(model_error(format!(
            "legacy virtual model has no {field} vector"
        ))),
    }
}

fn field_direction(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    field_id: usize,
    time: f64,
) -> Result<MDirection, MsError> {
    if engine.field_moves(field_id) {
        return engine.observation_direction_j2000(time, field_id);
    }
    let field = ms.field()?;
    let direction = raw_field_phase_direction(ms, field_id)?;
    if field.num_poly(field_id)? == 0 {
        return Ok(direction);
    }
    let dt = time - field.time(field_id)?;
    let polynomial = DirectionColumn::phase_dir(field.table()).get_direction_at(field_id, dt)?;
    let (longitude, latitude) = polynomial.as_angles();
    Ok(MDirection::from_angles(
        longitude,
        latitude,
        direction.refer(),
    ))
}

fn uvw(ms: &MeasurementSet, row: usize, uncached: bool) -> Result<[f64; 3], GainSolveError> {
    let error = |source: casa_tables::TableError| read_error(ms, source.into());
    let table = ms.main_table();
    let values = if uncached {
        let value = table
            .column_accessor("UVW")
            .and_then(|column| column.array_cells_owned_uncached(&[row]))
            .map_err(error)?
            .pop()
            .flatten()
            .ok_or_else(|| model_error("UVW must contain three real coordinates"))?;
        Cow::Owned(value)
    } else {
        Cow::Borrowed(
            table
                .cell_accessor(row, "UVW")
                .and_then(|cell| cell.array())
                .map_err(error)?,
        )
    };
    uvw_coordinates(values.as_ref())
}

fn uvw_coordinates(cell: &ArrayValue) -> Result<[f64; 3], GainSolveError> {
    if let ArrayValue::Float64(values) = cell
        && let Some(values) = values.as_slice()
        && let Ok(values) = values.try_into()
    {
        return Ok(values);
    }
    Err(model_error("UVW must contain three real coordinates"))
}

fn read_error(ms: &MeasurementSet, source: MsError) -> GainSolveError {
    GainSolveError::OpenMeasurementSet {
        path: ms
            .path()
            .unwrap_or(Path::new("<in-memory>"))
            .display()
            .to_string(),
        source,
    }
}

fn model_error(error: impl std::fmt::Display) -> GainSolveError {
    GainSolveError::VirtualModel(error.to_string())
}
