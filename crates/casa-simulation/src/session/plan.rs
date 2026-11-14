// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered DATA phases. Each phase keeps one selection and the existing
//! fused prediction/corruption plan. A partial replacement or an addition
//! after Jones starts a new phase; an all-row replacement discards previous
//! phases. Thus ordinary all-row workloads retain their single fused pass.

mod phase;
pub(crate) use phase::{DataPhase, NoiseTerm};

use std::sync::Arc;

use super::prediction::Prediction;
use super::selection::DataSelection;
use crate::corrupt::NoiseApplication;
use crate::corrupt::jones::HeldTerm;
use crate::predict::{ComponentPrediction, SingleDishPrediction};

#[derive(Debug, Clone, Default)]
pub(crate) struct DataPlan {
    phases: Vec<DataPhase>,
    selection: DataSelection,
    replaced: bool,
}

impl DataPlan {
    pub(in crate::session) fn cloned_bytes(&self) -> usize {
        self.phases.len() * size_of::<DataPhase>()
            + self
                .phases
                .iter()
                .map(DataPhase::cloned_bytes)
                .sum::<usize>()
    }
    pub(crate) fn selection(&self) -> &DataSelection {
        &self.selection
    }
    pub(crate) fn phases(&self) -> &[DataPhase] {
        &self.phases
    }

    pub(crate) fn select(&mut self, selection: DataSelection) {
        self.selection = selection;
    }

    pub(crate) fn reset_selection(&mut self) {
        self.selection = DataSelection::All;
    }

    pub(crate) fn begin_prediction(&mut self, incremental: bool) {
        if !incremental && self.selection.is_all() {
            self.phases.clear();
            self.replaced = true;
        }
        let can_fuse = incremental
            && self.phases.last().is_some_and(|phase| {
                phase.selection == self.selection && !phase.corrupts_by_jones()
            });
        if !can_fuse {
            self.phases
                .push(DataPhase::new(self.selection.clone(), !incremental));
        }
    }

    fn prediction_phase(&mut self) -> &mut DataPhase {
        self.phases
            .last_mut()
            .expect("begin_prediction precedes prepared models")
    }

    pub(crate) fn predict(&mut self, prediction: Arc<Prediction>, scans: usize) {
        self.prediction_phase().predict(prediction, scans);
    }

    pub(crate) fn predict_components(
        &mut self,
        components: Arc<ComponentPrediction>,
        scans: usize,
    ) {
        self.prediction_phase()
            .predict_components(components, scans);
    }

    pub(crate) fn predict_single_dish(
        &mut self,
        prediction: Arc<SingleDishPrediction>,
        scans: usize,
    ) {
        self.prediction_phase()
            .predict_single_dish(prediction, scans);
    }

    pub(crate) fn corrupt(
        &mut self,
        applications: impl IntoIterator<Item = NoiseApplication>,
        jones: impl IntoIterator<Item = Arc<HeldTerm>>,
        scans: usize,
    ) {
        if self
            .phases
            .last()
            .is_none_or(|phase| phase.selection != self.selection)
        {
            self.phases
                .push(DataPhase::new(self.selection.clone(), false));
        }
        self.phases
            .last_mut()
            .expect("phase was installed")
            .corrupt(applications, jones, scans);
    }

    pub(crate) fn replaced(&self) -> bool {
        self.replaced
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.phases.iter().all(DataPhase::is_empty)
    }

    pub(crate) fn predicts_images(&self) -> bool {
        self.phases
            .iter()
            .any(|phase| phase.predictions().next().is_some())
    }

    pub(crate) fn predicts(&self) -> bool {
        self.phases.iter().any(DataPhase::predicts)
    }

    pub(crate) fn predicts_single_dish(&self) -> bool {
        self.phases.iter().any(DataPhase::predicts_single_dish)
    }

    pub(crate) fn corrupts_by_jones(&self) -> bool {
        self.phases.iter().any(DataPhase::corrupts_by_jones)
    }

    pub(crate) fn has_selection(&self) -> bool {
        self.phases.iter().any(|phase| !phase.selection.is_all())
    }

    /// Partial operations need per-row scratch-column preservation even
    /// when the caller selected all rows.
    pub(crate) fn has_partial_rows(&self, scans: &[super::observe::Scan]) -> bool {
        self.has_selection()
            || scans.iter().enumerate().any(|(index, scan)| {
                if scan.row_count() == 0 {
                    return false;
                }
                let uncovered_prediction =
                    self.predicts() && !self.phases.iter().any(|phase| phase.predicts_scan(index));
                uncovered_prediction
                    || self
                        .phases
                        .iter()
                        .any(|phase| phase.has_uncovered_corruption(index, scan.spectral_window_id))
            })
    }

    pub(crate) fn has_query(&self) -> bool {
        self.phases.iter().any(|phase| phase.selection.has_query())
    }
}
