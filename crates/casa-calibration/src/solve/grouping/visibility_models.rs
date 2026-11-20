// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-model routing for operations using the legacy iterator.

use std::collections::BTreeMap;

use casa_ms::{MeasurementSet, ResolvedMsSelectionRow, VirtualModelEntry, scratch_columns};
use casa_types::Complex32;
use ndarray::ArrayD;

use crate::solve::{GainSolveCombine, GainSolveError, GainSolveModelSource, model::ModelInputs};

use super::{SelectedSolveRow, SolveBaseKey, SolveBucketKey, selected_solve_row, weights};

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
        Self::from_ms(ms, rows, false)
    }

    pub(crate) fn for_uvsub(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
    ) -> Result<Self, GainSolveError> {
        Self::from_ms(ms, rows, true)
    }

    fn from_ms(
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
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
        Self::with_models(ms, rows, models)
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
        Self::with_models(ms, rows, models)
    }

    fn with_models(
        ms: &MeasurementSet,
        rows: Vec<SelectedSolveRow>,
        mut models: ModelInputs,
    ) -> Result<Self, GainSolveError> {
        let combine = GainSolveCombine {
            scans: true,
            observations: true,
            ..GainSolveCombine::default()
        };
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
        models.bind_buffers(ms, &ordered, combine)?;
        models.phase_times = weights::chunk_phase_times(&ordered, combine, models.dynamic_fields());
        Ok(Self {
            rows: rows.into_iter().map(|row| (row.row_index, row)).collect(),
            models,
        })
    }

    pub(crate) fn reads_column(&self, field: i32) -> bool {
        self.models.reads_column(field)
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
            Some(row) => self.models.predict(ms, row, correlations, flags),
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
    ) -> Result<Option<ArrayD<Complex32>>, GainSolveError> {
        match self.rows.get(&row) {
            Some(row) => self
                .models
                .predict_model(ms, row, correlations, (flags, previous), true),
            None => Ok(None),
        }
    }
}
