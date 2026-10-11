// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native run: the source admitted, the major and minor cycles on one
//! worker team, and the products, each phase recorded in the run summary.

use std::path::{Path, PathBuf};

use casa_imaging_model::CompiledProblem;
use casa_imaging_products::{
    ContinuumProductInputs, MinorCycleImageResponse, PlannedContinuumGeneration,
    ProductStoragePlan, ProductsError, PublishedContinuumGeneration, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, ReconstructionMaskSet,
};
use casa_imaging_runtime::pass::{PassError, WorkerTeam};
use casa_imaging_runtime::{
    Admission, Cancelled, Demand, Reservation, RunSummary, SourceAccessError, admit,
    finalize_source_access, run_phase,
};
use casa_ms::{ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest};

use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationDispatchError, ApplicationError, ApplicationNative, ApplicationPublication,
    ApplicationRuntime, AwCatalogDeployment, ImagingOutcome, visibility_write_selection,
};

/// What the native run needs besides the compiled problem and its runtime.
pub(crate) struct NativeInput {
    pub(crate) observation: SelectedObservationResolutionRequest,
    pub(crate) initial_access: ResolvedSelectedObservationAccess,
    pub(crate) write_model_column: bool,
    pub(crate) write_corrected_data: bool,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
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

/// A native run ready to start: its admitted source, its worker team and
/// what its passes, cycles and products need.
pub(crate) struct NativeRun {
    runtime: ApplicationRuntime,
    publication: ApplicationPublication,
    aw_catalog: Option<AwCatalogDeployment>,
    visibility_write: Option<imaging::VisibilityWriteTarget>,
    access: ResolvedSelectedObservationAccess,
    /// The source's charge, held while it is open.
    source: Reservation,
    team: WorkerTeam,
    masks: ImageDomainReconstructionMaskPlans,
    image_response: Option<MinorCycleImageResponse>,
}

/// The end of a run's preparation: check the products against `problem`,
/// resolve any visibility write, admit the source and form the worker team.
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
    let (access, source) = finalize_source_access(
        problem,
        input.initial_access,
        imaging::row_samples(problem)?,
        &runtime.host,
        &runtime.resource_policy,
    )?;
    let team = WorkerTeam::new(runtime.resource_policy.workers(&runtime.host))?;
    Ok(NativeRun {
        runtime,
        publication,
        aw_catalog,
        visibility_write,
        access,
        source,
        team,
        masks: input.masks,
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
    summary.workers = run.team.workers();
    let NativeRun {
        runtime,
        publication,
        aw_catalog,
        visibility_write,
        access,
        source,
        team,
        masks,
        image_response,
    } = run;
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            access,
            masks,
            image_response,
            visibility_write,
            host: runtime.host,
            policy: runtime.resource_policy,
            team: &team,
            cancel: runtime.cancel.clone(),
            spill_directory: &runtime.spill_directory,
            aw_catalog,
            backend: runtime.backend,
            grid_precision: runtime.grid_precision,
            diagnostics: runtime.diagnostics,
        },
        summary,
    )?;
    drop(source);
    let mut published = Vec::new();
    let products = run_phase(
        "products",
        &runtime.cancel,
        runtime.diagnostics,
        summary,
        || {
            publish_products(
                problem,
                outcome.scientific,
                outcome.masks,
                &runtime,
                publication,
                &team,
                &mut published,
            )
        },
    );
    summary.products = published;
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

/// Generate every planned product member into the writer's private staging,
/// one output channel per window on the run's team, and publish them,
/// adding each image published to `published`; a cancelled run publishes
/// nothing.
fn publish_products(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    reconstruction_masks: Option<ReconstructionMaskSet>,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    team: &WorkerTeam,
    published: &mut Vec<String>,
) -> Result<Products, NativeError> {
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
    let planned = PlannedContinuumGeneration::new(&inputs, &publication.controls)?;
    let window = ProductStoragePlan::new(1, team.workers())?;
    let demand = planned.demand(&inputs, window, &publication.writer)?;
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
