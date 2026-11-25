// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-model routing for operations using the legacy iterator.

use std::collections::BTreeMap;

use casa_ms::{MeasurementSet, ResolvedMsSelectionRow, VirtualModelEntry, scratch_columns};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use ndarray::ArrayD;

use crate::ft::PreparedVoltagePattern;
use crate::solve::{GainSolveCombine, GainSolveError, GainSolveModelSource, model::ModelInputs};

use super::{SelectedSolveRow, SolveBaseKey, SolveBucketKey, selected_solve_row, weights};

const LEGACY_COMBINE: GainSolveCombine = GainSolveCombine {
    scans: true,
    observations: true,
    fields: false,
    spectral_windows: false,
};

/// The MODEL_DATA or virtual model visible to the old visibility iterator.
pub(crate) struct VisibilityModels {
    rows: BTreeMap<usize, SelectedSolveRow>,
    models: ModelInputs,
}

impl VisibilityModels {
    pub(crate) fn new(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
    ) -> Result<Self, GainSolveError> {
        Self::from_ms(ms, rows, LEGACY_COMBINE, false)
    }

    pub(crate) fn for_transform(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        recipe_slots: Option<&[usize]>,
    ) -> Result<Self, GainSolveError> {
        let fields = scratch_columns::virtual_model_fields(ms);
        let (rows, states) = transform_model_rows(ms, rows, &fields, recipe_slots)?;
        let models = ModelInputs::new(ms, &rows, GainSolveModelSource::FromMeasurementSet)?;
        Self::with_models(ms, rows, models, GainSolveCombine::default(), Some(&states))
    }

    pub(crate) fn for_uvsub(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
    ) -> Result<Self, GainSolveError> {
        Self::from_ms(ms, rows, LEGACY_COMBINE, true)
    }

    pub(crate) fn for_flagdata(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
    ) -> Result<Self, GainSolveError> {
        Self::from_ms(ms, rows, GainSolveCombine::default(), true)
    }

    fn from_ms(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        combine: GainSolveCombine,
        defer_refusals: bool,
    ) -> Result<Self, GainSolveError> {
        let virtual_fields = scratch_columns::virtual_model_fields(ms);
        let rows = rows
            .iter()
            .filter(|row| virtual_fields.contains(&row.field_id))
            .map(|row| selected_solve_row(ms, row, None))
            .collect::<Result<Vec<_>, _>>()?;
        let models = if defer_refusals {
            ModelInputs::for_uvsub(ms, &rows)?
        } else {
            ModelInputs::new(ms, &rows, GainSolveModelSource::FromMeasurementSet)?
        };
        Self::with_models(ms, rows, models, combine, None)
    }

    pub(crate) fn from_entries(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        entries: &[VirtualModelEntry],
    ) -> Result<Self, GainSolveError> {
        let rows = rows
            .iter()
            .map(|row| selected_solve_row(ms, row, None))
            .collect::<Result<Vec<_>, _>>()?;
        let models = ModelInputs::from_entries(ms, &rows, entries)?;
        Self::with_models(ms, rows, models, LEGACY_COMBINE, None)
    }

    pub(crate) fn with_component_voltage(mut self, voltage: PreparedVoltagePattern) -> Self {
        self.models.set_component_voltage(voltage);
        self
    }

    /// Use the legacy visibility iterator's FIELD-column directions for
    /// attached moving fields in `setjy` component predictions.
    pub(crate) fn with_calibration_field_directions(
        mut self,
        ms: &MeasurementSet,
    ) -> Result<Self, GainSolveError> {
        self.models.use_calibration_field_directions(ms)?;
        Ok(self)
    }

    pub(crate) fn with_phase_centre_time(mut self, time_mjd_seconds: f64) -> Self {
        for time in self.models.phase_times.values_mut() {
            *time = time_mjd_seconds;
        }
        self
    }

    pub(crate) fn set_plot_phase_references(
        &mut self,
        rows: &[ResolvedMsSelectionRow],
        slots: &[usize],
    ) -> Result<(), GainSolveError> {
        if slots.len() != rows.len() {
            return Err(GainSolveError::VirtualModel(
                "PlotMS phase reference slots do not match the selected rows".into(),
            ));
        }
        for (row, &slot) in rows.iter().zip(slots) {
            if !self.rows.contains_key(&row.row_index) {
                continue;
            }
            let reference = rows.get(slot).ok_or_else(|| {
                GainSolveError::VirtualModel(format!("invalid PlotMS phase reference slot {slot}"))
            })?;
            self.models.plot_phase_references.insert(
                row.row_index,
                (reference.field_id, reference.time_mjd_seconds),
            );
        }
        Ok(())
    }

    fn with_models(
        ms: &MeasurementSet,
        rows: Vec<SelectedSolveRow>,
        mut models: ModelInputs,
        combine: GainSolveCombine,
        states: Option<&BTreeMap<usize, i32>>,
    ) -> Result<Self, GainSolveError> {
        // VisBuffer chooses the first physical row at a timestamp. Solves
        // order tied timestamps by interval, which can choose a different state.
        let mut ordered: Vec<_> = rows
            .iter()
            .map(|row| {
                (
                    row,
                    SolveBaseKey::for_row(row, combine),
                    SolveBucketKey::Infinite,
                )
            })
            .collect();
        ordered.sort_unstable_by_key(|(row, _, _)| row.row_index);
        if let Some(states) = states {
            models.bind_transform_buffers(ms, &ordered, states)?;
        } else {
            models.bind_buffers(ms, &ordered, combine)?;
        }
        models.phase_times = weights::chunk_phase_times(&ordered, combine, models.dynamic_fields());
        Ok(Self {
            rows: rows.into_iter().map(|row| (row.row_index, row)).collect(),
            models,
        })
    }

    pub(crate) fn reads_column(&self, field: i32) -> bool {
        self.models.reads_column(field)
    }

    pub(crate) fn reads_transform_column(&self, row: usize) -> bool {
        self.rows
            .get(&row)
            .is_none_or(|row| self.models.reads_column(row.field_id))
    }

    pub(crate) fn has_virtual_refusals(&self) -> bool {
        self.models.has_virtual_refusals()
    }

    pub(crate) fn virtual_refusal(&self, field: i32) -> Option<GainSolveError> {
        self.models.virtual_refusal(field)
    }

    pub(crate) fn uses_fourier(&self, field: i32) -> bool {
        self.models.uses_fourier(field)
    }

    pub(crate) fn predict(
        &self,
        ms: &MeasurementSet,
        row: usize,
        correlations: &[i32],
        flags: &ArrayD<bool>,
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        match self.rows.get(&row) {
            Some(row) => self.models.predict(ms, row, correlations, flags, None),
            None => Ok(None),
        }
    }

    pub(crate) fn predict_scratch(
        &self,
        ms: &MeasurementSet,
        row: usize,
        correlations: &[i32],
        flags: &ArrayD<bool>,
        previous: Option<ArrayD<Complex32>>,
        uvw_cell: Option<&ArrayValue>,
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        match self.rows.get(&row) {
            Some(row) => {
                self.models
                    .predict_model(ms, row, correlations, (flags, previous), true, uvw_cell)
            }
            None => Ok(None),
        }
    }
}

fn transform_model_rows(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    fields: &std::collections::BTreeSet<i32>,
    recipe_slots: Option<&[usize]>,
) -> Result<(Vec<SelectedSolveRow>, BTreeMap<usize, i32>), GainSolveError> {
    if recipe_slots.is_some_and(|slots| {
        slots.len() != rows.len() || slots.iter().any(|&slot| slot >= rows.len())
    }) {
        return Err(GainSolveError::VirtualModel(
            "PlotMS recipe slots do not match the selected rows".into(),
        ));
    }
    let rows = rows
        .iter()
        .enumerate()
        .filter_map(|(slot, row)| {
            let recipe = recipe_slots.map_or(row, |slots| &rows[slots[slot]]);
            fields.contains(&recipe.field_id).then_some((row, recipe))
        })
        .collect::<Vec<_>>();
    let indices = rows
        .iter()
        .map(|(row, _)| row.row_index)
        .collect::<Vec<_>>();
    let recipe_indices = (recipe_slots.is_some()
        && rows
            .iter()
            .any(|(row, recipe)| row.row_index != recipe.row_index))
    .then(|| {
        rows.iter()
            .map(|(_, recipe)| recipe.row_index)
            .collect::<Vec<_>>()
    });
    let recipe_rows = recipe_indices.as_deref().unwrap_or(&indices);
    let read = |name, indices| {
        ms.main_table()
            .column_accessor(name)
            .and_then(|column| column.scalar_cells_owned_for_rows(indices))
            .map_err(|source| GainSolveError::VirtualModel(source.to_string()))
    };
    let arrays = read("ARRAY_ID", &indices)?;
    let observations = read("OBSERVATION_ID", &indices)?;
    let scans = read("SCAN_NUMBER", recipe_rows)?;
    let states = read("STATE_ID", recipe_rows)?;
    let centroids = read("TIME_CENTROID", &indices)?;
    let intervals = read("INTERVAL", &indices)?;
    let mut selected = Vec::with_capacity(rows.len());
    let mut state_ids = BTreeMap::new();
    for (slot, (row, recipe)) in rows.into_iter().enumerate() {
        let error = |name| {
            GainSolveError::VirtualModel(format!("invalid {name} in MAIN row {}", row.row_index))
        };
        let integer = |column: &[Option<ScalarValue>], name| match &column[slot] {
            Some(ScalarValue::Int32(value)) => Ok(*value),
            _ => Err(error(name)),
        };
        let double = |column: &[Option<ScalarValue>], name| match &column[slot] {
            Some(ScalarValue::Float64(value)) => Ok(*value),
            _ => Err(error(name)),
        };
        state_ids.insert(row.row_index, integer(&states, "STATE_ID")?);
        selected.push(SelectedSolveRow {
            row_index: row.row_index,
            array_id: integer(&arrays, "ARRAY_ID")?,
            field_id: recipe.field_id,
            observation_id: integer(&observations, "OBSERVATION_ID")?,
            data_desc_id: row.data_desc_id,
            data_spw_id: recipe.spectral_window_id,
            channel_selection: None,
            antenna1: row.antenna1_id,
            antenna2: row.antenna2_id,
            time_seconds: row.time_mjd_seconds,
            time_centroid_seconds: double(&centroids, "TIME_CENTROID")?,
            interval_seconds: double(&intervals, "INTERVAL")?,
            scan_number: integer(&scans, "SCAN_NUMBER")?,
            flag_row: row.flag_row,
        });
    }
    Ok((selected, state_ids))
}
