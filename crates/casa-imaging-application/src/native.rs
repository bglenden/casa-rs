// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native run: the source admitted, the major and minor cycles on one
//! worker team, and the products, each phase recorded in the run summary.

use std::cell::OnceCell;
use std::path::Path;
use std::sync::Arc;

use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, ReconstructionMaskSet,
};
use casa_imaging_model::CompiledProblem;
use casa_imaging_model::GeometryPlan;
use casa_imaging_products::{
    ContinuumProductInputs, FacetSumWeights, MinorCycleImageResponse, PlannedContinuumGeneration,
    ProductStoragePlan, ProductsError, PublishedContinuumGeneration, produce_continuum_members,
};
use casa_imaging_runtime::pass::{PassError, WorkerTeam};
use casa_imaging_runtime::{
    Admission, Admitted, Cancelled, Demand, Reservation, RunSummary, admit, free_memory, run_phase,
};
use casa_ms::observation_io::Msv2Observation;
use casa_observation::FieldEphemeris;
use casa_types::measures::MeasuresProvider;

use crate::compile::resolve_partitions;
use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationDispatchError, ApplicationError, ApplicationNative, ApplicationPublication,
    ApplicationRuntime, AwCatalogDeployment, ImagingOutcome,
};

/// The observation compile opened, and what its geometry evaluates with.
pub(crate) struct OpenedObservation {
    /// The MeasurementSet's observation, with the charge of its index
    /// while it is open ([`crate::compile::open_observation`]).
    pub(crate) store: Admitted<Msv2Observation>,
    /// The run's one Measures provider: compile's geometry and the batches'
    /// evaluate with it.
    pub(crate) measures: Arc<dyn MeasuresProvider>,
    /// The ephemeris table a moving phase centre names, read when the
    /// request compiled.
    pub(crate) phase_table: Option<Arc<FieldEphemeris>>,
}

/// What the native run needs besides the compiled problem and its runtime.
pub(crate) struct NativeInput {
    pub(crate) observation: OpenedObservation,
    pub(crate) write_model_column: bool,
    pub(crate) write_corrected_data: bool,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    pub(crate) start_model: Option<crate::compile::StartModel>,
    pub(crate) minor_cycle_image_response: Option<MinorCycleImageResponse>,
}

/// Why a native run did not complete.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NativeError {
    /// A phase's memory did not fit the resource policy.
    #[error(transparent)]
    Admission(#[from] Admission),
    /// The run was cancelled.
    #[error(transparent)]
    Cancelled(#[from] Cancelled),
    /// Any other failure.
    #[error("{0}")]
    Other(ApplicationError),
}

impl From<NativeError> for ApplicationDispatchError {
    fn from(error: NativeError) -> Self {
        match error {
            NativeError::Admission(admission) => Self::Admission(admission),
            NativeError::Cancelled(_) => Self::Cancelled,
            NativeError::Other(error) => Self::Native(error),
        }
    }
}

impl From<ApplicationError> for NativeError {
    fn from(error: ApplicationError) -> Self {
        Self::Other(error)
    }
}

impl From<ImagingError> for NativeError {
    fn from(error: ImagingError) -> Self {
        match error {
            ImagingError::Admission(admission)
            | ImagingError::Minor(casa_imaging_runtime::MinorCycleRunError::Admission(admission)) => {
                Self::Admission(admission)
            }
            ImagingError::Cancelled(cancelled) => Self::Cancelled(cancelled),
            ImagingError::Pass(error) => error.into(),
            error => Self::Other(Box::new(error)),
        }
    }
}

impl From<PassError> for NativeError {
    fn from(error: PassError) -> Self {
        match error {
            PassError::Cancelled => Self::Cancelled(Cancelled),
            error => Self::Other(Box::new(error)),
        }
    }
}

impl From<ProductsError> for NativeError {
    fn from(error: ProductsError) -> Self {
        Self::Other(Box::new(error))
    }
}

impl From<std::io::Error> for NativeError {
    fn from(error: std::io::Error) -> Self {
        Self::Other(Box::new(error))
    }
}

/// A native run ready to start: its admitted source and what its passes,
/// cycles and products need. Its setup chooses and forms its worker teams.
pub(crate) struct NativeRun {
    runtime: ApplicationRuntime,
    publication: ApplicationPublication,
    aw_catalog: Option<AwCatalogDeployment>,
    /// The observation's batches the passes read, and through whose update
    /// session a model or corrected column is written.
    route: Box<imaging::BatchRoute>,
    /// The source's charge, held while it is open.
    source: Reservation,
    /// What the products hold on one lane, which the run's setup fits
    /// beside it ([`products_floor`]).
    products: u64,
    masks: ImageDomainReconstructionMaskPlans,
    start_model: Option<crate::compile::StartModel>,
    image_response: Option<MinorCycleImageResponse>,
}

/// The batch route of `problem` on `observation`, the observation compile
/// opened: the selection resolved to its partitions, the geometry compiled
/// and its batches admitted ([`imaging::admit_batches`]). A run that writes
/// the model (`model_data`) or the corrected data (`corrected_data`) reads
/// the observation through the update session that writes them
/// ([`imaging::WriteBack`]).
pub(crate) fn batch_route(
    problem: &CompiledProblem,
    observation: OpenedObservation,
    (model_data, corrected_data): (bool, bool),
    runtime: &ApplicationRuntime,
) -> Result<Admitted<imaging::BatchRoute>, NativeError> {
    let OpenedObservation {
        store: Admitted {
            value: opened,
            reservation: mut opening,
        },
        measures,
        phase_table,
    } = observation;
    let selection = resolve_partitions(problem, &opened)
        .map_err(|error| NativeError::Other(Box::new(error)))?;
    let plan = GeometryPlan::compile(problem, opened.catalogue(), measures, phase_table)
        .map_err(|error| NativeError::Other(Box::new(error)))?;
    let pointing_budget = if plan.reads_pointing() {
        opened
            .pointing_bytes()
            .map_err(|error| NativeError::Other(Box::new(error)))?
    } else {
        0
    };
    let Admitted {
        value: batching,
        reservation,
    } = imaging::admit_batches(
        (problem, &opened, &selection, &plan),
        imaging::row_samples(problem)?,
        pointing_budget,
        (&runtime.host, &runtime.resource_policy),
    )?;
    opening.join(reservation);
    let store = if model_data || corrected_data {
        imaging::RouteStore::Write(Box::new(
            imaging::WriteBack::open(opened, model_data, corrected_data)
                .map_err(|error| NativeError::Other(Box::new(error)))?,
        ))
    } else {
        imaging::RouteStore::Read(Box::new(opened))
    };
    Ok(Admitted {
        value: imaging::BatchRoute {
            store,
            selection,
            plan,
            batching,
            pointing_budget,
        },
        reservation: opening,
    })
}

/// The end of a run's preparation: check the products against `problem`
/// and admit the source, its pass storage sized for the policy's streaming
/// workers ([`casa_imaging_runtime::ResourcePolicy::cores`]), the most the run's setup then
/// chooses.
pub(crate) fn prepare(
    problem: &CompiledProblem,
    input: NativeInput,
    native: ApplicationNative,
) -> Result<NativeRun, NativeError> {
    let ApplicationNative {
        runtime,
        publication,
        aw_catalog,
    } = native;
    publication.controls.validate_for_problem(problem)?;
    let products = products_floor(problem, &publication)?;
    let Admitted {
        value: route,
        reservation: source,
    } = batch_route(
        problem,
        input.observation,
        (input.write_model_column, input.write_corrected_data),
        &runtime,
    )?;
    Ok(NativeRun {
        runtime,
        publication,
        aw_catalog,
        route: Box::new(route),
        source,
        products,
        masks: input.masks,
        start_model: input.start_model,
        image_response: input.minor_cycle_image_response,
    })
}

/// Run the major-cycle passes, the minor cycles between them and any
/// visibility write, then publish the products. `summary` collects the
/// phases and totals as they end; a completed run's outcome takes it once
/// the caller has finished it.
pub(crate) fn run(
    problem: &CompiledProblem,
    run: NativeRun,
    summary: &mut RunSummary,
) -> Result<impl FnOnce(RunSummary) -> ImagingOutcome + use<>, NativeError> {
    let NativeRun {
        runtime,
        publication,
        aw_catalog,
        route,
        source,
        products,
        masks,
        start_model,
        image_response,
    } = run;
    let teams = OnceCell::new();
    // The run's source borrows the batch route, which is released with the
    // source's charge once the run has finished with it.
    let visibility_write = route
        .write_back()
        .map(|_| imaging::VisibilityWriteTarget(&route));
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            source: &route,
            masks,
            start_model,
            image_response,
            visibility_write,
            products: (products, source.memory()),
            host: runtime.host,
            policy: runtime.resource_policy,
            teams: &teams,
            cancel: runtime.cancel.clone(),
            spill_directory: &runtime.spill_directory,
            aw_catalog,
            backend: runtime.backend,
            grid_precision: runtime.grid_precision,
            diagnostics: runtime.diagnostics,
        },
        summary,
    )?;
    drop(route);
    drop(source);
    let team = teams
        .get()
        .expect("a run that completed formed its teams")
        .compute();
    let mut published = Vec::new();
    let mut lanes = (0, None);
    let products = run_phase(
        "products",
        &runtime.cancel,
        runtime.diagnostics,
        summary,
        || {
            publish_products(
                problem,
                ProductSource {
                    scientific: outcome.scientific,
                    masks: outcome.masks,
                    facet_sum_weights: outcome.facet_sum_weights,
                },
                &runtime,
                publication,
                team,
                &mut published,
                &mut lanes,
            )
        },
    );
    summary.products = published;
    (summary.product_lanes, summary.product_lanes_reason) = lanes;
    let products = products?;
    let workers = team.workers();
    let problem = problem.clone();
    Ok(move |summary| ImagingOutcome {
        problem,
        minor_cycles: outcome.minor_cycles,
        stop: outcome.stop,
        major_cycle_count: outcome.major_cycle_count,
        total_minor_iterations: outcome.total_minor_iterations,
        total_actual_minor_iterations: outcome.total_actual_minor_iterations,
        workers,
        planes_per_wave: outcome.planes_per_wave,
        visibility_products: outcome.visibility_products,
        summary,
        scientific: products.scientific,
        planned_products: products.planned,
        products: products.published,
        _retained: outcome.retained,
    })
}

/// The products of a run and the state they were generated from.
struct Products {
    planned: PlannedContinuumGeneration,
    scientific: MajorCycleCompletion,
    published: PublishedContinuumGeneration,
}

/// What the products are formed from: the final completion, its
/// reconstruction masks and the faceted domains' facets' own sums of
/// weights.
struct ProductSource {
    scientific: MajorCycleCompletion,
    masks: Option<ReconstructionMaskSet>,
    facet_sum_weights: FacetSumWeights,
}

/// What `problem`'s products hold on one lane, before the run
/// (`PlannedContinuumGeneration::planned_demand`): the least they can run
/// on, which the run's setup fits beside it.
fn products_floor(
    problem: &CompiledProblem,
    publication: &ApplicationPublication,
) -> Result<u64, NativeError> {
    Ok(
        PlannedContinuumGeneration::for_problem(problem, &publication.controls)?
            .planned_demand(problem, ProductStoragePlan::new(1, 1)?, &publication.writer)?
            .peak_residency_bytes(),
    )
}

/// Generate every planned product member into the writer's private staging,
/// one output channel per window on the run's team, and publish them,
/// adding each image published to `published`; a cancelled run publishes
/// nothing. `lanes` receives the lanes the products ran on and, when fewer
/// than the team's workers, why.
fn publish_products(
    problem: &CompiledProblem,
    source: ProductSource,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    team: &WorkerTeam,
    published: &mut Vec<String>,
    lanes: &mut (usize, Option<String>),
) -> Result<Products, NativeError> {
    let ProductSource {
        scientific,
        masks: reconstruction_masks,
        facet_sum_weights,
    } = source;
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific);
    if let Some(masks) = reconstruction_masks.as_ref() {
        inputs = match masks {
            ReconstructionMaskSet::Shared(mask) => inputs.with_reconstruction_mask(mask),
            ReconstructionMaskSet::Domains(masks) => inputs.with_domain_reconstruction_masks(masks),
        };
    }
    if let Some(centres) = publication.primary_beam_centres.as_ref() {
        inputs = inputs.with_primary_beam_centres(centres);
    }
    if !facet_sum_weights.is_empty() {
        inputs = inputs.with_facet_sum_weights(&facet_sum_weights);
    }
    let planned = PlannedContinuumGeneration::new(&inputs, &publication.controls)?;
    // The products' scratch grows with their lanes, and the gridding's
    // workers were chosen without it: the products take the most lanes, up
    // to the team's, whose demand fits what the policy leaves free (#763
    // review), and one when none does, which admission then refuses.
    let free = free_memory(&runtime.host, &runtime.resource_policy);
    let demand_on = |count: usize| -> Result<_, NativeError> {
        let window = ProductStoragePlan::new(1, count)?;
        Ok((
            window,
            planned.demand(&inputs, window, &publication.writer)?,
        ))
    };
    let workers = team.workers();
    let (mut window, mut demand) = demand_on(workers)?;
    debug_assert_eq!(
        planned.planned_demand(problem, window, &publication.writer)?,
        demand,
        "the products' demand before the run is the run's"
    );
    let needed = demand.peak_residency_bytes();
    if needed > free {
        for count in (1..workers).rev() {
            (window, demand) = demand_on(count)?;
            if demand.peak_residency_bytes() <= free {
                break;
            }
        }
        let count = window.maximum_workers();
        let gigabytes = |bytes: u64| format!("{:.2} GB", bytes as f64 / 1e9);
        let reason = if demand.peak_residency_bytes() <= free {
            format!(
                "{workers} lanes need {}, {count} fit {} free",
                gigabytes(needed),
                gigabytes(free)
            )
        } else {
            format!(
                "{workers} lanes need {} and no fewer fit {} free",
                gigabytes(needed),
                gigabytes(free)
            )
        };
        if count < workers {
            tracing::warn!("products on {count} lanes, not the team's {workers}: {reason}");
            lanes.1 = Some(reason);
        }
    }
    lanes.0 = window.maximum_workers();
    let held = admit(
        &runtime.host,
        &runtime.resource_policy,
        &Demand {
            phase: "products",
            memory: demand.peak_residency_bytes(),
        },
    )?;
    let generation =
        produce_continuum_members(&planned, &inputs, window, team, &publication.writer)?;
    if runtime.cancel.is_cancelled() {
        // The staged members are removed when the writer drops.
        return Err(Cancelled.into());
    }
    publication
        .writer
        .publish(|image: &Path| published.push(image.to_string_lossy().into_owned()))?;
    // The charge covers the staged records until they are published.
    drop(held);
    Ok(Products {
        planned,
        scientific,
        published: generation,
    })
}
