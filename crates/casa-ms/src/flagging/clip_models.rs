// SPDX-License-Identifier: LGPL-3.0-or-later
//! Evaluate virtual component models for the selected clipping rows.

use std::collections::{BTreeMap, BTreeSet};

use casa_components::{ComponentList, Polarisation};
use casa_core::measures::{MEpoch, MeasFrame, UvwMachine};
use casa_core::{ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use casa_tables::Table;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::derived::engine::{MsCalEngine, raw_field_phase_direction};
use crate::scratch_columns::virtual_model_fields;
use crate::selection::{MsSelectionRowBlock, ResolvedMsSelectionRow, resolve_selected_row_facts};
use crate::{MeasurementSet, VirtualModelEntry, VirtualModelKind, virtual_model};

use super::{FlagDataColumn, FlaggingError, load_i32_cells};

/// Construct the existing full visibility predictor for rows of fields with
/// Fourier recipes. The MeasurementSet supplies their original flags and geometry.
pub type FlagDataModelFactory = fn(
    &MeasurementSet,
    &[ResolvedMsSelectionRow],
) -> Result<Box<dyn FlagDataModelPredictor>, String>;

/// Predict the visible Fourier or component terms for an individual selected row.
pub trait FlagDataModelPredictor {
    /// Return a model in MS axis order (correlation, channel). An unmatched
    /// buffer returns `None` and `flagdata` uses the default unit point model.
    fn predict(
        &self,
        ms: &MeasurementSet,
        row: usize,
        correlations: &[i32],
        flags: &ArrayD<bool>,
    ) -> Result<Option<ArrayD<Complex32>>, String>;
}

pub(super) struct ClipModels {
    fields: BTreeMap<i32, Vec<Recipe>>,
    first_buffer_states: BTreeMap<BufferKey, i32>,
    geometry: Option<MsCalEngine>,
    physical: bool,
    fourier: Option<Box<dyn FlagDataModelPredictor>>,
}

pub(super) struct ReadClipModels {
    pub(super) cells: Vec<Option<ArrayValue>>,
    pub(super) fourier: Vec<bool>,
}

struct Recipe {
    selection: Selection,
    components: Option<ComponentList>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BufferKey {
    observation: i32,
    array: i32,
    scan: i32,
    field: i32,
    description: i32,
    time: u64,
}

enum Selection {
    Rows(BTreeSet<[i32; 4]>),
    FieldAndWindow { fields: Vec<i32>, windows: Vec<i32> },
}

impl Selection {
    fn includes(&self, keys: [i32; 4]) -> bool {
        match self {
            Self::Rows(combinations) => combinations.contains(&keys),
            Self::FieldAndWindow { fields, windows } => {
                fields.contains(&keys[0]) && windows.contains(&keys[1])
            }
        }
    }
}

impl ClipModels {
    pub(super) fn for_selection(
        ms: &MeasurementSet,
        column: FlagDataColumn,
        rows: &[usize],
        factory: Option<FlagDataModelFactory>,
        path: &str,
    ) -> Result<Option<Self>, FlaggingError> {
        if !matches!(
            column,
            FlagDataColumn::ModelData | FlagDataColumn::Residual | FlagDataColumn::ResidualData
        ) {
            return Ok(None);
        }
        let physical = ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("MODEL_DATA"));
        if !physical && column == FlagDataColumn::ModelData {
            return Err(FlaggingError::MissingClipColumn);
        }
        let available = virtual_model_fields(ms);
        if available.is_empty() {
            return Ok(None);
        }
        let mut fields = BTreeMap::new();
        let mut first_buffer_states = BTreeMap::new();
        let mut fourier_rows = Vec::new();
        for block in rows.chunks(65_536) {
            for (&row, (key, state)) in block.iter().zip(buffer_rows(ms.main_table(), block, path)?)
            {
                let field = key.field;
                if !available.contains(&field) {
                    continue;
                }
                first_buffer_states.entry(key).or_insert(state);
                if let std::collections::btree_map::Entry::Vacant(entry) = fields.entry(field)
                    && let Some(model) =
                        virtual_model(ms, field).map_err(|error| failure(path, error))?
                {
                    entry.insert(decode(&model.record).map_err(|error| failure(path, error))?);
                }
                if factory.is_some()
                    && fields
                        .get(&field)
                        .is_some_and(|recipes| has_fourier(recipes))
                {
                    fourier_rows.push(row);
                }
            }
        }
        let fourier = factory
            .filter(|_| !fourier_rows.is_empty())
            .map(|factory| {
                let planned = fourier_rows
                    .chunks(65_536)
                    .enumerate()
                    .map(|(index, rows)| MsSelectionRowBlock {
                        selected_row_start: index * 65_536,
                        selected_row_count: rows.len(),
                        planned_bytes: std::mem::size_of_val(rows),
                    })
                    .collect::<Vec<_>>();
                let rows = resolve_selected_row_facts(ms, &fourier_rows, &planned)
                    .map_err(|error| failure(path, error))?;
                factory(ms, &rows).map_err(|error| failure(path, error))
            })
            .transpose()?;
        Ok((!fields.is_empty()).then_some(Self {
            fields,
            first_buffer_states,
            geometry: None,
            physical,
            fourier,
        }))
    }

    pub(super) fn read(
        &mut self,
        ms: &MeasurementSet,
        rows: &[usize],
        flags: &[ArrayD<bool>],
        path: &str,
    ) -> Result<ReadClipModels, FlaggingError> {
        let table = ms.main_table();
        let mut cells = vec![None; rows.len()];
        let mut fourier = vec![false; rows.len()];
        let descriptions = ms
            .data_description()
            .map_err(|error| failure(path, error))?;
        let pol = ms.polarization().map_err(|error| failure(path, error))?;
        let windows = ms.spectral_window().map_err(|error| failure(path, error))?;
        let mut labels = BTreeMap::new();
        let mut frequencies = BTreeMap::new();
        let mut physical = Vec::new();
        let mut plans = Vec::new();
        let mut prediction_rows = Vec::new();
        let mut fourier_rows = Vec::new();
        for (slot, (buffer, state)) in buffer_rows(table, rows, path)?.into_iter().enumerate() {
            let field_id = buffer.field;
            let selected = self.fields.get(&field_id);
            if self.physical && selected.is_none() {
                physical.push(slot);
                continue;
            }
            let ddid = buffer.description as usize;
            let spw = descriptions
                .spectral_window_id(ddid)
                .map_err(|error| failure(path, error))?;
            if self.fourier.is_some() && selected.is_some_and(|recipes| has_fourier(recipes)) {
                fourier_rows.push((slot, buffer, spw));
                continue;
            }
            let state = self
                .first_buffer_states
                .get(&buffer)
                .copied()
                .unwrap_or(state);
            let keys = [field_id, spw, buffer.scan, state];
            let mut components = None;
            for recipe in selected
                .into_iter()
                .flatten()
                .filter(|recipe| recipe.selection.includes(keys))
            {
                components = Some(recipe.components.as_ref().ok_or_else(|| {
                    failure(path, format!("clipping a virtual Fourier model for FIELD_ID {field_id} is unimplemented (#1617)"))
                })?);
            }
            if components.is_some() {
                prediction_rows.push(rows[slot]);
            }
            plans.push((slot, buffer, spw, components));
        }
        if !fourier_rows.is_empty() {
            let predictor = self
                .fourier
                .as_ref()
                .expect("Fourier rows require a predictor");
            for (slot, buffer, spw) in fourier_rows {
                let row = rows[slot];
                let polarization = descriptions
                    .polarization_id(buffer.description as usize)
                    .map_err(|error| failure(path, error))?;
                let labels = cached(&mut labels, polarization, |id| pol.corr_type(id as usize))
                    .map_err(|error| failure(path, error))?;
                let frequencies =
                    cached(&mut frequencies, spw, |id| windows.chan_freq(id as usize))
                        .map_err(|error| failure(path, error))?;
                let model = predictor
                    .predict(ms, row, labels, &flags[slot])
                    .map_err(|error| failure(path, error))?;
                fourier[slot] = model.is_some();
                cells[slot] = Some(ArrayValue::Complex32(
                    model.unwrap_or_else(|| default_model(labels, frequencies.len())),
                ));
            }
        }
        if !physical.is_empty() {
            let physical_rows = physical.iter().map(|&slot| rows[slot]).collect::<Vec<_>>();
            let values = table
                .column_accessor("MODEL_DATA")
                .and_then(|column| column.array_cells_owned_uncached(&physical_rows))
                .map_err(|error| failure(path, error))?;
            for (slot, value) in physical.into_iter().zip(values) {
                cells[slot] = value;
            }
        }
        let mut baselines = if prediction_rows.is_empty() {
            Vec::new().into_iter()
        } else {
            if self.geometry.is_none() {
                self.geometry = Some(
                    MsCalEngine::new(ms)
                        .and_then(|engine| engine.with_field_ephemerides(ms))
                        .map_err(|error| failure(path, error))?,
                );
            }
            table
                .column_accessor("UVW")
                .and_then(|column| column.array_cells_owned_uncached(&prediction_rows))
                .map_err(|error| failure(path, error))?
                .into_iter()
        };
        for (slot, buffer, spw, components) in plans {
            let row = rows[slot];
            let polarization = descriptions
                .polarization_id(buffer.description as usize)
                .map_err(|error| failure(path, error))?;
            let labels = cached(&mut labels, polarization, |id| pol.corr_type(id as usize))
                .map_err(|error| failure(path, error))?;
            let frequencies = cached(&mut frequencies, spw, |id| windows.chan_freq(id as usize))
                .map_err(|error| failure(path, error))?;
            let model = if let Some(components) = components {
                let Some(Some(ArrayValue::Float64(uvw))) = baselines.next() else {
                    return Err(failure(
                        path,
                        format!("UVW is undefined or invalid at row {row}"),
                    ));
                };
                if uvw.shape() != [3] {
                    return Err(failure(
                        path,
                        format!("UVW row {row} must contain three coordinates"),
                    ));
                }
                component_model(
                    ms,
                    self.geometry.as_ref().expect("geometry loaded"),
                    ModelRow {
                        row,
                        field: buffer.field as usize,
                        correlations: labels,
                        frequencies,
                        time: f64::from_bits(buffer.time),
                        uvw: [-uvw[0], -uvw[1], uvw[2]],
                    },
                    components,
                    path,
                )?
            } else {
                default_model(labels, frequencies.len())
            };
            cells[slot] = Some(ArrayValue::Complex32(model));
        }
        Ok(ReadClipModels { cells, fourier })
    }
}

fn has_fourier(recipes: &[Recipe]) -> bool {
    recipes.iter().any(|recipe| recipe.components.is_none())
}

fn buffer_rows(
    table: &Table,
    rows: &[usize],
    path: &str,
) -> Result<Vec<(BufferKey, i32)>, FlaggingError> {
    let groups = [
        "OBSERVATION_ID",
        "ARRAY_ID",
        "SCAN_NUMBER",
        "FIELD_ID",
        "DATA_DESC_ID",
        "STATE_ID",
    ]
    .map(|name| load_i32_cells(table, rows, name, path));
    let [observation, array, scan, field, description, state] = groups;
    let (observation, array, scan, field, description, state) =
        (observation?, array?, scan?, field?, description?, state?);
    let times = table
        .column_accessor("TIME")
        .and_then(|column| column.scalar_cells_owned_for_rows(rows))
        .map_err(|error| failure(path, error))?;
    times
        .into_iter()
        .enumerate()
        .map(|(slot, time)| {
            let Some(ScalarValue::Float64(time)) = time else {
                return Err(failure(
                    path,
                    format!("TIME row {} must contain seconds", rows[slot]),
                ));
            };
            Ok((
                BufferKey {
                    observation: observation[slot],
                    array: array[slot],
                    scan: scan[slot],
                    field: field[slot],
                    description: description[slot],
                    time: time.to_bits(),
                },
                state[slot],
            ))
        })
        .collect()
}

fn cached<T>(
    cache: &mut BTreeMap<i32, T>,
    id: i32,
    load: impl FnOnce(i32) -> crate::MsResult<T>,
) -> crate::MsResult<&T> {
    Ok(match cache.entry(id) {
        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::btree_map::Entry::Vacant(entry) => entry.insert(load(id)?),
    })
}

fn decode(record: &RecordValue) -> Result<Vec<Recipe>, String> {
    if record.get("version").is_some() {
        return VirtualModelEntry::entries(record)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|entry| {
                let selection = Selection::Rows(entry.index_combination.into_iter().collect());
                recipe(entry.kind, selection, &entry.container)
            })
            .collect();
    }
    let mut recipes = Vec::new();
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
                return Err(format!("missing legacy virtual model {name}"));
            };
            let Some(Value::Record(container)) = entry.get("container") else {
                return Err(format!("missing container in legacy virtual model {name}"));
            };
            let ids = |name| match entry.get(name) {
                Some(Value::Array(ArrayValue::Int32(values))) if values.ndim() == 1 => {
                    Ok(values.iter().copied().collect::<Vec<_>>())
                }
                _ => Err(format!("missing {name} in legacy virtual model")),
            };
            recipes.push(recipe(
                kind,
                Selection::FieldAndWindow {
                    fields: ids("fields")?,
                    windows: ids("spws")?,
                },
                container,
            )?);
        }
    }
    Ok(recipes)
}

fn recipe(
    kind: VirtualModelKind,
    selection: Selection,
    container: &RecordValue,
) -> Result<Recipe, String> {
    let components = (kind == VirtualModelKind::ComponentList)
        .then(|| ComponentList::from_record(container).map_err(|error| error.to_string()))
        .transpose()?;
    Ok(Recipe {
        selection,
        components,
    })
}

struct ModelRow<'a> {
    row: usize,
    field: usize,
    correlations: &'a [i32],
    frequencies: &'a [f64],
    time: f64,
    uvw: [f64; 3],
}

fn component_model(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    selected: ModelRow<'_>,
    components: &ComponentList,
    path: &str,
) -> Result<ArrayD<Complex32>, FlaggingError> {
    let ModelRow {
        row,
        field,
        correlations,
        frequencies,
        time,
        uvw,
    } = selected;
    let error = |reason| failure(path, reason);
    let direction = if engine.field_moves(field) {
        engine.observation_direction_j2000(time, field)
    } else {
        raw_field_phase_direction(ms, field)
    }
    .map_err(|cause| error(cause.to_string()))?;
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(time / 86_400.0, engine.time_reference()))
        .with_position(engine.observatory_position().clone())
        .with_direction(direction.clone());
    let (basis, offset) = match correlations.first() {
        Some(1..=4) => (Polarisation::Stokes, 1),
        Some(5..=8) => (Polarisation::Circular, 5),
        Some(9..=12) => (Polarisation::Linear, 9),
        _ => {
            return Err(error(format!(
                "unsupported virtual model polarization at row {row}"
            )));
        }
    };
    if correlations
        .iter()
        .any(|code| !(offset..offset + 4).contains(code))
    {
        return Err(error(format!(
            "mixed virtual model polarization at row {row}"
        )));
    }
    let mut model = ArrayD::<Complex32>::zeros(IxDyn(&[correlations.len(), frequencies.len()]).f());
    for component in components.iter() {
        let projection =
            UvwMachine::with_frame(component.shape().reference_direction(), &direction, &frame)
                .map_err(|cause| error(cause.to_string()))?;
        let (rotated, distance) = projection.convert_projected(uvw);
        let baseline = [-rotated[0], -rotated[1], rotated[2]];
        for (channel, &frequency) in frequencies.iter().enumerate() {
            let visibility = component
                .visibility(baseline, frequency, basis)
                .map_err(|cause| error(cause.to_string()))?;
            let phase = Complex64::from_polar(
                1.0,
                std::f64::consts::TAU * distance * frequency / 299_792_458.0,
            );
            for (plane, &code) in correlations.iter().enumerate() {
                let value = visibility[(code - offset) as usize] * phase;
                model[[plane, channel]] += Complex32::new(value.re as f32, value.im as f32);
            }
        }
    }
    Ok(model)
}

fn default_model(correlations: &[i32], channels: usize) -> ArrayD<Complex32> {
    ArrayD::from_shape_fn(IxDyn(&[correlations.len(), channels]).f(), |index| {
        Complex32::new(
            f32::from(u8::from(matches!(
                correlations[index[0]],
                1 | 5 | 8 | 9 | 12
            ))),
            0.0,
        )
    })
}

fn failure(path: &str, reason: impl std::fmt::Display) -> FlaggingError {
    FlaggingError::MutateFlags {
        path: path.to_string(),
        reason: format!("read clipping model: {reason}"),
    }
}
