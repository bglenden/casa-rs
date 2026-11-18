// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's precedence for scratch and virtual model visibilities.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_components::{ComponentList, Polarisation};
use casa_ms::derived::engine::{MsCalEngine, raw_field_phase_direction};
use casa_ms::{MeasurementSet, MsError, VirtualModelEntry, VirtualModelKind, virtual_model};
use casa_types::measures::{MEpoch, MeasFrame, UvwMachine};
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;

use super::grouping::SelectedSolveRow;
use super::{GainSolveError, GainSolveModelSource, get_i32};

pub(super) struct ModelInputs {
    read_column: bool,
    virtual_models: BTreeMap<i32, Vec<Entry>>,
    frequencies: BTreeMap<i32, Vec<f64>>,
    geometry: Option<MsCalEngine>,
}

struct Entry {
    combinations: Vec<[i32; 4]>,
    prediction: Prediction,
}

enum Prediction {
    Components(ComponentList),
    FourierGrid,
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
        let mut inputs = Self {
            read_column,
            virtual_models: BTreeMap::new(),
            frequencies: BTreeMap::new(),
            geometry: None,
        };
        if source != GainSolveModelSource::FromMeasurementSet {
            return Ok(inputs);
        }
        let fields: BTreeSet<_> = rows.iter().map(|row| row.field_id).collect();
        for field in fields {
            if let Some(record) =
                virtual_model(ms, field).map_err(|source| read_error(ms, source))?
            {
                let entries = VirtualModelEntry::entries(&record.record).map_err(model_error)?;
                let decoded = entries
                    .into_iter()
                    .map(|entry| {
                        let prediction = match entry.kind {
                            VirtualModelKind::ComponentList => Prediction::Components(
                                ComponentList::from_record(&entry.container)
                                    .map_err(model_error)?,
                            ),
                            VirtualModelKind::FtMachine => Prediction::FourierGrid,
                        };
                        Ok(Entry {
                            combinations: entry.index_combination,
                            prediction,
                        })
                    })
                    .collect::<Result<_, GainSolveError>>()?;
                inputs.virtual_models.insert(field, decoded);
            }
        }
        if inputs
            .virtual_models
            .values()
            .any(|entries| !entries.is_empty())
        {
            let spectral_window = ms
                .spectral_window()
                .map_err(|source| read_error(ms, source))?;
            let windows: BTreeSet<_> = rows
                .iter()
                .filter(|row| inputs.virtual_models.contains_key(&row.field_id))
                .map(|row| row.data_spw_id)
                .collect();
            for window in windows {
                let frequencies = spectral_window
                    .chan_freq(window as usize)
                    .map_err(|source| read_error(ms, source))?;
                inputs.frequencies.insert(window, frequencies);
            }
            inputs.geometry = Some(MsCalEngine::new(ms).map_err(|source| read_error(ms, source))?);
        }
        Ok(inputs)
    }

    pub(super) fn reads_column(&self, field: i32) -> bool {
        self.read_column && !self.virtual_models.contains_key(&field)
    }

    /// A defined virtual model overrides MODEL_DATA. CASA uses a unit point
    /// model if none of the field's entries matches this visibility buffer.
    pub(super) fn predict(
        &self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        correlations: &[i32],
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        let Some(entries) = self.virtual_models.get(&row.field_id) else {
            return Ok(None);
        };
        if entries.is_empty() {
            return Ok(None);
        }
        let state = get_i32(ms.main_table(), row.row_index, "STATE_ID")?;
        let combination = [row.field_id, row.data_spw_id, row.scan_number, state];
        let mut matching = entries
            .iter()
            .filter(|entry| entry.combinations.contains(&combination))
            .peekable();
        if matching.peek().is_none() {
            return Ok(None);
        }
        let engine = self
            .geometry
            .as_ref()
            .expect("virtual models have geometry");
        let field = raw_field_phase_direction(ms, row.field_id as usize)
            .map_err(|source| read_error(ms, source))?;
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(
                row.time_seconds / 86400.0,
                engine.time_reference(),
            ))
            .with_position(engine.observatory_position().clone())
            .with_direction(field.clone());
        let uvw = uvw(ms, row.row_index)?;
        let frequencies = &self.frequencies[&row.data_spw_id];
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
        let mut model =
            ArrayD::<Complex32>::zeros(IxDyn(&[correlations.len(), frequencies.len()]).f());
        for entry in matching {
            let Prediction::Components(components) = &entry.prediction else {
                return Err(model_error(
                    "Fourier-grid virtual models are pending in #979",
                ));
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
