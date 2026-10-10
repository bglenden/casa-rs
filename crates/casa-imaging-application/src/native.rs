// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native run: the source admitted, the major and minor cycles on one
//! worker team, the products, and the run summary, written whether the run
//! completes or fails.

use std::path::PathBuf;

use casa_imaging_model::CompiledProblem;
use casa_imaging_products::{
    ContinuumProductInputs, PlannedContinuumGeneration, ProductStoragePlan, ProductsError,
    PublishedContinuumGeneration, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, MinorCycleImageResponse,
    ReconstructionMaskSet,
};
use casa_imaging_runtime::pass::{PassError, WorkerTeam};
use casa_imaging_runtime::{
    Admission, Cancelled, Demand, RunSummary, SourceAccessError, admit, finalize_source_access,
    run_phase,
};
use casa_ms::{ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest};

use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationError, ApplicationNative, ApplicationPublication, ApplicationRuntime,
    ImagingOutcome, visibility_write_selection,
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

impl From<ApplicationError> for NativeError {
    fn from(error: ApplicationError) -> Self {
        Self::Other(error)
    }
}

impl From<ImagingError> for NativeError {
    fn from(error: ImagingError) -> Self {
        match error {
            ImagingError::Admission(admission) => Self::Admission(admission),
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

/// Run the major-cycle passes, the minor cycles between them and any
/// visibility write, then publish the products.
///
/// When the runtime names a summary target, the run summary is written
/// there whether the run completes or fails; a cancelled run writes
/// nothing.
pub(crate) fn run_native(
    problem: &CompiledProblem,
    input: NativeInput,
    native: ApplicationNative,
) -> Result<ImagingOutcome, NativeError> {
    let target = native.runtime.summary.clone();
    let mut summary = RunSummary {
        backend: native.runtime.backend,
        ..RunSummary::default()
    };
    if let Some(target) = &target {
        summary.request = target.request.clone();
    }
    let result = run(problem, input, native, &mut summary);
    let Some(target) = target else {
        return result;
    };
    match result {
        Ok(outcome) => {
            outcome.summary.write(&target.path)?;
            Ok(outcome)
        }
        Err(NativeError::Cancelled(cancelled)) => Err(NativeError::Cancelled(cancelled)),
        Err(error) => {
            summary.error = Some(error.to_string());
            if let Err(write) = summary.write(&target.path) {
                tracing::warn!("the failed run's summary could not be written: {write}");
            }
            Err(error)
        }
    }
}

/// The run itself; `summary` collects its phases, and a completed run's
/// outcome takes it.
fn run(
    problem: &CompiledProblem,
    input: NativeInput,
    native: ApplicationNative,
    summary: &mut RunSummary,
) -> Result<ImagingOutcome, NativeError> {
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
        &runtime.host,
        &runtime.resource_policy,
    )?;
    let team = WorkerTeam::new(runtime.resource_policy.workers(&runtime.host))?;
    summary.workers = team.workers();
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            access,
            masks: input.masks,
            image_response: input.minor_cycle_image_response,
            visibility_write,
            host: runtime.host,
            policy: runtime.resource_policy,
            team: &team,
            cancel: runtime.cancel.clone(),
            spill_directory: &runtime.spill_directory,
            aw_catalog,
            backend: runtime.backend,
            grid_precision: runtime.grid_precision,
        },
        summary,
    )?;
    drop(source);
    summary.minor_cycles = outcome.minor_cycles.len();
    summary.minor_iterations = outcome.total_minor_iterations;
    let products = run_phase("products", &runtime.cancel, summary, || {
        publish_products(
            problem,
            outcome.scientific,
            outcome.masks,
            &runtime,
            publication,
            &team,
        )
    })?;
    summary.products = products
        .planned
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect();
    Ok(ImagingOutcome {
        problem: problem.clone(),
        minor_cycles: outcome.minor_cycles,
        stop: outcome.stop,
        major_cycle_count: outcome.major_cycle_count,
        total_minor_iterations: outcome.total_minor_iterations,
        total_actual_minor_iterations: outcome.total_actual_minor_iterations,
        workers: team.workers(),
        planes_per_wave: outcome.planes_per_wave,
        visibility_products: outcome.visibility_products,
        summary: std::mem::take(summary),
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

/// Generate every planned product member into the sink's private staging,
/// one output channel per window on the run's team, and publish them; a
/// cancelled run publishes nothing.
fn publish_products(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    reconstruction_masks: Option<ReconstructionMaskSet>,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    team: &WorkerTeam,
) -> Result<Products, NativeError> {
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific);
    if let Some(masks) = reconstruction_masks.as_ref() {
        inputs = match masks {
            ReconstructionMaskSet::Shared(mask) => inputs.with_reconstruction_mask(mask)?,
            ReconstructionMaskSet::Domains(masks) => {
                inputs.with_domain_reconstruction_masks(masks)?
            }
        };
    }
    let planned = PlannedContinuumGeneration::new(&inputs, &publication.controls)?;
    let window = ProductStoragePlan::new(1, team.workers())?;
    let demand = planned.demand(&inputs, window)?;
    let held = admit(
        &runtime.host,
        &runtime.resource_policy,
        &Demand {
            phase: "products",
            memory: demand.peak_residency_bytes()
                + publication.sink.residency(&planned, &demand)?,
        },
    )?;
    let published = produce_continuum_members(&planned, &inputs, window, team, &publication.sink)?;
    drop(held);
    if runtime.cancel.is_cancelled() {
        // The staged members are removed when the sink drops.
        return Err(Cancelled.into());
    }
    publication.sink.publish()?;
    Ok(Products {
        planned,
        scientific,
        published,
    })
}
