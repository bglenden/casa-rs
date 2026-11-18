// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's precedence for scratch and virtual model visibilities.

mod fourier;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use casa_components::{ComponentList, Polarisation};
use casa_ms::columns::direction_columns::DirectionColumn;
use casa_ms::derived::engine::{MsCalEngine, raw_field_phase_direction};
use casa_ms::{
    MeasurementSet, MsError, SubTable, VirtualModelEntry, VirtualModelKind, virtual_model,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{MEpoch, MeasFrame, UvwMachine};
use casa_types::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;

use super::grouping::{BucketedRow, SelectedSolveRow, VisibilityBuffer};
use super::{GainSolveCombine, GainSolveError, GainSolveModelSource, get_i32};
use fourier::{FourierModel, FourierRow};

pub(super) struct ModelInputs {
    read_column: bool,
    virtual_models: BTreeMap<i32, Arc<[Entry]>>,
    frequencies: BTreeMap<i32, (Vec<f64>, FrequencyRef)>,
    geometry: Option<MsCalEngine>,
    dynamic_fields: BTreeSet<i32>,
    pub(super) phase_times: BTreeMap<usize, f64>,
    finite_fields: BTreeSet<i32>,
    model_buffers: BTreeMap<usize, Arc<ModelBuffer>>,
}

struct ModelBuffer {
    selection: [i32; 4],
    focus_rows: Vec<FocusSample>,
}

struct FocusSample {
    index: usize,
    antennas: [i32; 2],
}

struct Entry {
    selection: EntrySelection,
    prediction: Prediction,
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
}

impl ModelInputs {
    pub(super) fn new(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
        source: GainSolveModelSource,
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
                        entry.insert(decode_entries(&record.record)?.into())
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
            virtual_models: BTreeMap::new(),
            frequencies: BTreeMap::new(),
            geometry: None,
            dynamic_fields: BTreeSet::new(),
            phase_times: BTreeMap::new(),
            finite_fields: BTreeSet::new(),
            model_buffers: BTreeMap::new(),
        }
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
        self.read_column && !self.virtual_models.contains_key(&field)
    }

    pub(super) fn dynamic_fields(&self) -> &BTreeSet<i32> {
        &self.dynamic_fields
    }

    pub(super) fn uses_fourier(&self, field: i32) -> bool {
        self.virtual_models.get(&field).is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| matches!(entry.prediction, Prediction::FourierGrid(_)))
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
        if !self.needs_buffers() {
            return Ok(());
        }
        let mut buffers = BTreeMap::<VisibilityBuffer, Vec<&SelectedSolveRow>>::new();
        for row in rows
            .iter()
            .filter(|(row, _, _)| self.virtual_models.contains_key(&row.field_id))
        {
            buffers
                .entry(VisibilityBuffer::new(row, combine))
                .or_default()
                .push(row.0);
        }
        for rows in buffers.into_values() {
            let first = rows[0];
            let state = get_i32(ms.main_table(), first.row_index, "STATE_ID")?;
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
            });
            for row in rows {
                self.model_buffers
                    .insert(row.row_index, Arc::clone(&buffer));
            }
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
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        self.predict_model(ms, row, correlations, (raw_flags, None), false)
    }

    pub(super) fn predict_model(
        &self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        correlations: &[i32],
        (raw_flags, previous): (&ArrayD<bool>, Option<ArrayD<Complex32>>),
        scratch_model: bool,
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
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
        let engine = self
            .geometry
            .as_ref()
            .expect("virtual models have geometry");
        let phase_time = self
            .phase_times
            .get(&row.row_index)
            .copied()
            .unwrap_or(row.time_seconds);
        let field = field_direction(ms, engine, row.field_id as usize, phase_time)
            .map_err(|source| read_error(ms, source))?;
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(
                row.time_seconds / 86400.0,
                engine.time_reference(),
            ))
            .with_position(engine.observatory_position().clone())
            .with_direction(field.clone());
        let uvw = uvw(ms, row.row_index)?;
        let (frequencies, frequency_ref) = &self.frequencies[&row.data_spw_id];
        let (basis, offset) = match correlations.first().copied() {
            Some(5..=8) => (Polarisation::Circular, 5),
            Some(9..=12) => (Polarisation::Linear, 9),
            _ => {
                return Err(model_error(
                    "virtual component prediction requires linear or circular correlations",
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
        let shape = [correlations.len(), frequencies.len()];
        let mut model = match previous {
            Some(model) if model.shape() == shape => model,
            Some(_) => return Err(model_error("MODEL_DATA does not match its layout")),
            None => ArrayD::<Complex32>::zeros(IxDyn(&shape).f()),
        };
        for entry in matching {
            let components = match &entry.prediction {
                Prediction::Components(components) => components,
                Prediction::FourierGrid(grid) => {
                    grid.predict(
                        FourierRow {
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
                            focus_rows: &buffer.focus_rows,
                        },
                        &mut model,
                        scratch_model,
                    )?;
                    continue;
                }
            };
            for component in components.iter() {
                let projection =
                    UvwMachine::with_frame(component.shape().reference_direction(), &field, &frame)
                        .map_err(model_error)?;
                let (rotated, phase) = projection.convert_projected([-uvw[0], -uvw[1], uvw[2]]);
                let baseline = [-rotated[0], -rotated[1], rotated[2]];
                for (channel, &frequency) in frequencies.iter().enumerate() {
                    let visibility = component
                        .visibility(baseline, frequency, basis)
                        .map_err(model_error)?;
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
        }
        Ok(Some(model))
    }
}

fn decode_entries(record: &RecordValue) -> Result<Vec<Entry>, GainSolveError> {
    if record.get("version").is_some() {
        return VirtualModelEntry::entries(record)
            .map_err(model_error)?
            .into_iter()
            .map(|entry| {
                decode_entry(
                    entry.kind,
                    EntrySelection::Rows(entry.index_combination),
                    &entry.container,
                )
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
        VirtualModelKind::FtMachine => {
            Prediction::FourierGrid(Box::new(FourierModel::new(container.clone())))
        }
    };
    Ok(Entry {
        selection,
        prediction,
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

fn uvw(ms: &MeasurementSet, row: usize) -> Result<[f64; 3], GainSolveError> {
    let values = ms
        .main_table()
        .cell_accessor(row, "UVW")
        .and_then(|cell| cell.array())
        .map_err(|source| read_error(ms, source.into()))?;
    if let ArrayValue::Float64(values) = values
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
