// SPDX-License-Identifier: LGPL-3.0-or-later

//! Use the already open stores to grid the separately centred mosaic PSF.

use std::sync::Arc;

use casa_imaging_model::{CompiledProblem, GeometryPlan};
use casa_imaging_runtime::{Admitted, RunSummary, run_phase};
use casa_types::measures::MeasuresProvider;

use super::NativeError;
use crate::ApplicationRuntime;
use crate::imaging::{self, BatchRoute, Batching, StoreBatches};

/// Temporary geometry and admission for a second view of the same rows.
/// The stores, resolved selection and optional update sessions are shared.
pub(super) struct PsfRoute {
    problem: CompiledProblem,
    geometry: Admitted<(Vec<GeometryPlan>, Batching)>,
}

impl PsfRoute {
    pub(super) fn new(
        problem: CompiledProblem,
        measures: Arc<dyn MeasuresProvider>,
        route: &BatchRoute,
        runtime: &ApplicationRuntime,
    ) -> Result<Self, NativeError> {
        let plans = route
            .stores
            .iter()
            .map(|store| {
                GeometryPlan::compile(
                    &problem,
                    store.observation().catalogue(),
                    Arc::clone(&measures),
                    None,
                )
                .map_err(|error| NativeError::Other(Box::new(error)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stores = route
            .stores
            .iter()
            .zip(&plans)
            .map(|(store, plan)| StoreBatches {
                observation: store.observation(),
                selection: &store.selection,
                plan,
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
    ) -> Result<Admitted<Vec<f32>>, NativeError> {
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

fn swap_view(route: &mut BatchRoute, plans: &mut [GeometryPlan], batching: &mut Batching) {
    for (store, plan) in route.stores.iter_mut().zip(plans) {
        std::mem::swap(&mut store.plan, plan);
    }
    std::mem::swap(&mut route.batching, batching);
}
