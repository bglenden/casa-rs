// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native run: the source admitted, the major and minor cycles on one
//! worker team, and the products, each phase recorded in the run summary.

use std::cell::OnceCell;
use std::path::{Path, PathBuf};

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
    Admission, Admitted, Cancelled, Demand, Reservation, RunSummary, SourceAccessError, admit,
    finalize_source_access, free_memory, run_phase,
};
use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_ms::{ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest};
use casa_observation::ObservationError;

use crate::compile::resolve_partitions;
use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationDispatchError, ApplicationError, ApplicationNative, ApplicationPublication,
    ApplicationRuntime, AwCatalogDeployment, ImagingOutcome, SourceRoute,
    visibility_write_selection,
};

/// What the native run needs besides the compiled problem and its runtime.
pub(crate) struct NativeInput {
    pub(crate) observation: SelectedObservationResolutionRequest,
    pub(crate) initial_access: ResolvedSelectedObservationAccess,
    pub(crate) source_route: SourceRoute,
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

impl From<SourceAccessError> for NativeError {
    fn from(error: SourceAccessError) -> Self {
        match error {
            SourceAccessError::Admission(admission) => Self::Admission(admission),
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
    visibility_write: Option<imaging::VisibilityWriteTarget>,
    access: NativeSource,
    /// The source's charge, held while it is open.
    source: Reservation,
    masks: ImageDomainReconstructionMaskPlans,
    start_model: Option<crate::compile::StartModel>,
    image_response: Option<MinorCycleImageResponse>,
}

/// What a prepared run reads its visibilities from ([`SourceRoute`]).
pub(crate) enum NativeSource {
    /// The selected MAIN rows, through their admitted access.
    SelectedRows(ResolvedSelectedObservationAccess),
    /// The observation's batches.
    ObservationBatches(Box<imaging::BatchRoute>),
}

/// The batch route of `problem` on the MeasurementSet `observation` names:
/// the observation opened, the selection resolved to its partitions, the
/// geometry compiled and its batches admitted
/// ([`imaging::admit_batches`]). A run that writes visibilities back is
/// refused: the batch route writes through update sessions in step 5 of
/// #932.
pub(crate) fn batch_route(
    problem: &CompiledProblem,
    observation: &SelectedObservationResolutionRequest,
    writes: bool,
    runtime: &ApplicationRuntime,
) -> Result<Admitted<imaging::BatchRoute>, NativeError> {
    if writes {
        return Err(NativeError::Other(
            "the observation-batch route writes no model or corrected column yet (#932 step 5)"
                .into(),
        ));
    }
    // Batch geometry follows a moving centre Measures names; a table named
    // like a body (`Mars`) would be followed as the body.
    if observation
        .ephemeris()
        .is_some_and(casa_ms::SelectedObservationEphemeris::is_table)
    {
        return Err(NativeError::Other(
            "the observation-batch route does not follow an ephemeris table yet (#932)".into(),
        ));
    }
    let Admitted {
        value: opened,
        reservation: mut opening,
    } = open_observation(observation, runtime)?;
    let selection = resolve_partitions(problem, &opened)
        .map_err(|error| NativeError::Other(Box::new(error)))?;
    let plan = GeometryPlan::compile(problem, opened.catalogue(), observation.measures_provider())
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
    Ok(Admitted {
        value: imaging::BatchRoute {
            observation: Box::new(opened),
            selection,
            plan,
            batching,
            pointing_budget,
        },
        reservation: opening,
    })
}

/// The MeasurementSet `observation` names, opened, with the charge of its
/// index while it is open ([`Msv2Observation::index_bytes`]). What opening
/// usually holds is admitted before it opens, from MAIN's row count
/// ([`Msv2Observation::opening_bytes`]), and each increase it asks for
/// beyond ([`Msv2Observation::open_granted`]) is admitted before it is
/// allocated: one that does not fit refuses the opening, as an admission.
/// The charge is then trimmed to the index, or raised to it.
fn open_observation(
    observation: &SelectedObservationResolutionRequest,
    runtime: &ApplicationRuntime,
) -> Result<Admitted<Msv2Observation>, NativeError> {
    let main_rows = usize::try_from(observation.selection().rows().source_row_count())
        .map_err(|_| NativeError::Other("MAIN holds more rows than this host addresses".into()))?;
    let (host, policy) = (&runtime.host, &runtime.resource_policy);
    let mut reservation = admit(
        host,
        policy,
        &Demand {
            phase: "observation opening",
            memory: Msv2Observation::opening_bytes(main_rows) as u64,
        },
    )?;
    let mut refusal = None;
    let opened = Msv2Observation::open_granted(
        observation.locator(),
        &OpenOptions::default(),
        &mut |total| {
            let total = total as u64;
            if total > reservation.memory() {
                let demand = Demand {
                    phase: "observation opening",
                    memory: total - reservation.memory(),
                };
                match admit(host, policy, &demand) {
                    Ok(more) => reservation.join(more),
                    Err(admission) => {
                        refusal = Some(admission);
                        return Err(ObservationError::OverBudget {
                            what: "the MSv2 opening index",
                            required: total as usize,
                            budget: reservation.memory() as usize,
                        });
                    }
                }
            }
            Ok(())
        },
    );
    let opened = opened.map_err(|error| match refusal.take() {
        Some(admission) => NativeError::Admission(admission),
        None => NativeError::Other(Box::new(error)),
    })?;
    let index = opened.index_bytes() as u64;
    if index <= reservation.memory() {
        reservation.retain(index);
    } else {
        reservation.join(admit(
            &runtime.host,
            &runtime.resource_policy,
            &Demand {
                phase: "observation index",
                memory: index - reservation.memory(),
            },
        )?);
    }
    Ok(Admitted {
        value: opened,
        reservation,
    })
}

/// The end of a run's preparation: check the products against `problem`,
/// resolve any visibility write and admit the source, its pass storage
/// sized for the policy's streaming workers ([`ResourcePolicy::cores`]), the
/// most the run's setup then chooses.
///
/// [`ResourcePolicy::cores`]: casa_imaging_runtime::ResourcePolicy::cores
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
    let visibility_write = (input.write_model_column || input.write_corrected_data)
        .then(|| {
            Ok::<_, ApplicationError>(imaging::VisibilityWriteTarget {
                path: PathBuf::from(input.observation.locator()),
                selection: visibility_write_selection(problem, input.observation.selection())?,
                model_data: input.write_model_column,
                corrected_data: input.write_corrected_data,
            })
        })
        .transpose()?;
    let (access, source) = match input.source_route {
        SourceRoute::SelectedRows => {
            let Admitted { value, reservation } = finalize_source_access(
                problem,
                input.initial_access,
                imaging::row_samples(problem)?,
                &runtime.host,
                &runtime.resource_policy,
            )?;
            (NativeSource::SelectedRows(value), reservation)
        }
        SourceRoute::ObservationBatches => {
            drop(input.initial_access);
            let Admitted { value, reservation } = batch_route(
                problem,
                &input.observation,
                visibility_write.is_some(),
                &runtime,
            )?;
            (
                NativeSource::ObservationBatches(Box::new(value)),
                reservation,
            )
        }
    };
    Ok(NativeRun {
        runtime,
        publication,
        aw_catalog,
        visibility_write,
        access,
        source,
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
        visibility_write,
        access,
        source,
        masks,
        start_model,
        image_response,
    } = run;
    let teams = OnceCell::new();
    // The run's source borrows the batch route, which is released with the
    // source's charge once the run has finished with it.
    let mut route = None;
    let source_input = match access {
        NativeSource::SelectedRows(access) => imaging::SourceInput::SelectedRows(access),
        NativeSource::ObservationBatches(owned) => {
            imaging::SourceInput::ObservationBatches(route.insert(owned))
        }
    };
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            source: source_input,
            masks,
            start_model,
            image_response,
            visibility_write,
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
