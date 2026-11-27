// SPDX-License-Identifier: LGPL-3.0-or-later

//! Use the already open stores to grid the separately centred mosaic PSF.

use std::sync::Arc;

use casa_core::measures::MeasuresProvider;
use casa_imaging_model::{CompiledProblem, GeometryPlan};
use casa_imaging_runtime::{Admitted, RunSummary, run_phase};

use super::NativeError;
use crate::ApplicationRuntime;
use crate::compile::resolve_partitions;
use crate::imaging::{self, BatchRoute, Batching, PartitionSelection, SeparatePsf, StoreBatches};

/// Temporary geometry and admission for a second view of the same rows.
/// The stores, resolved selection and optional update sessions are shared.
pub(super) struct PsfRoute {
    problem: CompiledProblem,
    geometry: Admitted<(Vec<StoreView>, Batching)>,
}

struct StoreView {
    geometry: GeometryPlan,
    selection: Option<PartitionSelection>,
}

impl PsfRoute {
    pub(super) fn new(
        problem: CompiledProblem,
        measures: Arc<dyn MeasuresProvider>,
        (main, route): (&CompiledProblem, &BatchRoute),
        runtime: &ApplicationRuntime,
    ) -> Result<Self, NativeError> {
        let shared_selection = std::ptr::eq(
            problem.observation().sources(),
            main.observation().sources(),
        );
        let plans = route
            .stores
            .iter()
            .enumerate()
            .map(|(source, store)| {
                let observation = store.observation();
                let geometry = GeometryPlan::compile(
                    &problem,
                    observation.catalogue(),
                    Arc::clone(&measures),
                    None,
                )
                .map_err(|error| NativeError::Other(Box::new(error)))?;
                let selection = (!shared_selection)
                    .then(|| resolve_partitions(&problem, source, observation))
                    .transpose()
                    .map_err(|error| NativeError::Other(Box::new(error)))?;
                Ok(StoreView {
                    geometry,
                    selection,
                })
            })
            .collect::<Result<Vec<_>, NativeError>>()?;
        let stores = route
            .stores
            .iter()
            .zip(&plans)
            .map(|(store, plan)| StoreBatches {
                observation: store.observation(),
                selection: plan.selection.as_ref().unwrap_or(&store.selection),
                plan: &plan.geometry,
                pointing_budget: store.pointing_budget,
            })
            .collect::<Vec<_>>();
        let batching = imaging::admit_batches(
            &problem,
            &stores,
            imaging::row_samples(&problem)?,
            (&runtime.host, &runtime.resource_policy),
        )?;
        Ok(Self {
            problem,
            geometry: Admitted {
                value: (plans, batching.value),
                reservation: batching.reservation,
            },
        })
    }

    pub(super) fn form(
        self,
        route: &mut BatchRoute,
        runtime: &ApplicationRuntime,
        summary: &mut RunSummary,
    ) -> Result<SeparatePsf, NativeError> {
        let (mut plans, mut batching) = self.geometry.value;
        swap_view(route, &mut plans, &mut batching);
        let result = run_phase(
            "separate mosaic PSF",
            &runtime.cancel,
            runtime.diagnostics,
            summary,
            || imaging::form_separate_psf(&self.problem, route, runtime),
        );
        swap_view(route, &mut plans, &mut batching);
        result.map_err(NativeError::from)
    }
}

fn swap_view(route: &mut BatchRoute, plans: &mut [StoreView], batching: &mut Batching) {
    for (store, plan) in route.stores.iter_mut().zip(plans) {
        std::mem::swap(&mut store.plan, &mut plan.geometry);
        if let Some(selection) = &mut plan.selection {
            std::mem::swap(&mut store.selection, selection);
        }
    }
    std::mem::swap(&mut route.batching, batching);
}
