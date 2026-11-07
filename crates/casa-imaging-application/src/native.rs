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
    AnalyticPrimaryBeam, ContinuumProductInputs, FacetSumWeights, MinorCycleImageResponse,
    PlannedContinuumGeneration, ProductStoragePlan, ProductsError, PublishedContinuumGeneration,
    produce_continuum_members,
};
use casa_imaging_runtime::pass::{PassError, WorkerTeam};
use casa_imaging_runtime::{
    Admission, Admitted, Cancelled, Demand, Reservation, RunSummary, admit, free_memory, run_phase,
};
use casa_observation::FieldEphemeris;
use casa_types::measures::MeasuresProvider;

use crate::compile::{OpenedStore, resolve_partitions};
use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationDispatchError, ApplicationError, ApplicationNative, ApplicationPublication,
    ApplicationRuntime, AwCatalogDeployment, ImagingOutcome,
};

/// The observation compile opened, and what its geometry evaluates with.
pub(crate) struct OpenedObservation {
    /// The observation's stores, one per MeasurementSet in the snapshot's
    /// order, each with the charge of what its opening holds while it is
    /// open (`compile::opening::open_observation`).
    pub(crate) stores: Vec<Admitted<OpenedStore>>,
    /// The run's one Measures provider: compile's geometry and the batches'
    /// evaluate with it.
    pub(crate) measures: Arc<dyn MeasuresProvider>,
    /// The ephemeris table a moving phase centre names, read when the
    /// request compiled.
    pub(crate) phase_table: Option<Arc<FieldEphemeris>>,
}

/// What the native run needs besides the compiled problem and its runtime.
pub(crate) struct NativeInput {
    /// Prepared MVC Taylor view for this cube run, when requested.
    pub(crate) mvc_taylor: Option<crate::compile::TaylorView>,
    pub(crate) observation: OpenedObservation,
    pub(crate) write_model_column: bool,
    /// Whether the model column's final write is CASA's `predictModel`, over
    /// every time's channels ([`imaging::WriteBack::new`]).
    pub(crate) predicts_final_model: bool,
    pub(crate) write_corrected_data: bool,
    /// Saving the final model as CASA's virtual model in each
    /// MeasurementSet, once the products are published.
    pub(crate) virtual_model: Vec<crate::virtual_model::VirtualModelPlan>,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    pub(crate) start_model: Option<crate::compile::StartModel>,
    /// What the run reads in place of forming it ([`imaging::Forms`]).
    pub(crate) existing: Option<crate::restart::ExistingProducts>,
    pub(crate) forms: imaging::Forms,
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

impl From<casa_imaging_deconvolution::ModelLifecycleError> for NativeError {
    fn from(error: casa_imaging_deconvolution::ModelLifecycleError) -> Self {
        Self::Other(Box::new(error))
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
    mvc_taylor: Option<crate::compile::CompiledTaylorView>,
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
    existing: Option<crate::restart::ExistingProducts>,
    forms: imaging::Forms,
    image_response: Option<MinorCycleImageResponse>,
    virtual_model: Vec<crate::virtual_model::VirtualModelPlan>,
}

/// The batch route of `problem` on `observation`, the observation compile
/// opened: each store's selection resolved to its partitions and its
/// geometry compiled, and the batches of every store admitted
/// ([`imaging::admit_batches`]). A run that writes the model (`model_data`)
/// or the corrected data (`corrected_data`) reads each store through the
/// update session that writes them ([`imaging::WriteBack`]);
/// `predicts_final_model` says which channels of each time the model's
/// write takes.
pub(crate) fn batch_route(
    problem: &CompiledProblem,
    observation: OpenedObservation,
    (model_data, corrected_data, predicts_final_model): (bool, bool, bool),
    runtime: &ApplicationRuntime,
) -> Result<Admitted<imaging::BatchRoute>, NativeError> {
    let other = |error: Box<dyn std::error::Error + Send + Sync>| NativeError::Other(error);
    let OpenedObservation {
        stores,
        measures,
        phase_table,
    } = observation;
    let mut opening = Reservation::none();
    let mut routes = Vec::with_capacity(stores.len());
    for (source, Admitted { value, reservation }) in stores.into_iter().enumerate() {
        opening.join(reservation);
        let selection = resolve_partitions(problem, source, value.observation())
            .map_err(|error| other(Box::new(error)))?;
        let plan = GeometryPlan::compile(
            problem,
            value.catalogue(),
            Arc::clone(&measures),
            phase_table.clone(),
        )
        .map_err(|error| other(Box::new(error)))?;
        let pointing_budget = if plan.reads_pointing() {
            value
                .observation()
                .pointing_bytes()
                .map_err(|error| other(Box::new(error)))?
        } else {
            0
        };
        routes.push((value, selection, plan, pointing_budget));
    }
    let Admitted {
        value: batching,
        reservation,
    } = imaging::admit_batches(
        problem,
        &routes
            .iter()
            .map(
                |(store, selection, plan, pointing_budget)| imaging::StoreBatches {
                    observation: store.observation(),
                    selection,
                    plan,
                    pointing_budget: *pointing_budget,
                },
            )
            .collect::<Vec<_>>(),
        imaging::row_samples(problem)?,
        (&runtime.host, &runtime.resource_policy),
    )?;
    opening.join(reservation);
    let write_back = (model_data || corrected_data)
        .then(|| imaging::WriteBack::new(model_data, corrected_data, !predicts_final_model));
    let stores = routes
        .into_iter()
        .map(|(opened, selection, plan, pointing_budget)| {
            let store = match &write_back {
                Some(write_back) => imaging::RouteStore::Write(Box::new(
                    write_back
                        .session(opened)
                        .map_err(|error| other(Box::new(error)))?,
                )),
                None => imaging::RouteStore::Read(opened.into_observation()),
            };
            Ok(imaging::StoreRoute {
                store,
                selection,
                plan,
                pointing_budget,
            })
        })
        .collect::<Result<Vec<_>, NativeError>>()?;
    Ok(Admitted {
        value: imaging::BatchRoute {
            stores,
            batching,
            write_back,
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
    let mvc_taylor = input
        .mvc_taylor
        .map(|view| view.compile_from_cube(problem))
        .transpose()
        .map_err(|error| NativeError::Other(Box::new(error)))?;
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
        (
            input.write_model_column,
            input.write_corrected_data,
            input.predicts_final_model,
        ),
        &runtime,
    )?;
    Ok(NativeRun {
        mvc_taylor,
        runtime,
        publication,
        aw_catalog,
        route: Box::new(route),
        source,
        products,
        masks: input.masks,
        start_model: input.start_model,
        existing: input.existing,
        forms: input.forms,
        image_response: input.minor_cycle_image_response,
        virtual_model: input.virtual_model,
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
        mvc_taylor,
        runtime,
        publication,
        aw_catalog,
        route,
        source,
        products,
        masks,
        start_model,
        existing,
        forms,
        image_response,
        virtual_model,
    } = run;
    let saved_psf_beams = existing
        .as_ref()
        .and_then(crate::restart::ExistingProducts::psf_beams);
    let psf_beams = existing
        .as_ref()
        .and_then(crate::restart::ExistingProducts::psf_pixel_beams);
    let mask_beams = existing
        .as_ref()
        .and_then(crate::restart::ExistingProducts::mask_beams);
    // An unlabelled saved PSF is fitted by getBeamSet's default cutoff at
    // minor-cycle initialization. Publication must reuse that fit's cutoff.
    let mut publication = publication;
    if !forms.psf
        && psf_beams.is_none()
        && problem.reconstruction().controls().max_minor_iterations() > 0
    {
        let model = publication.controls.primary_beam_model();
        let mut controls = casa_imaging_products::ContinuumProductControls::new(
            casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF,
        )?;
        if let Some(model) = model {
            controls = controls.with_primary_beam_model(model);
        }
        publication.controls = controls;
    }
    // CASA selects the cube's VLA aperture through its LSRK conversion layer,
    // but evaluates it at each output plane's own frequency.
    let vla_selection_hz = publication.vla_selection_store.map(|reference| {
        let factor = route.stores[reference]
            .plan
            .output_lsrk_factor()
            .expect("a tracked cube has an LSRK conversion layer");
        problem
            .geometry()
            .spectral()
            .channel_centre_hz(0)
            .expect("a cube has a first channel")
            * factor
    });
    let teams = OnceCell::new();
    let primary_beam = match publication.controls.primary_beam_model() {
        Some(model) => {
            AnalyticPrimaryBeam::new(problem, model, publication.primary_beam_centres.as_ref())?
                .map(|beam| {
                    vla_selection_hz.map_or(beam, |frequency| {
                        beam.with_vla_selection_frequency(frequency)
                    })
                })
        }
        None => None,
    };
    // The run's source borrows the batch route, which is released with the
    // source's charge once the run has finished with it.
    let visibility_write = route
        .write_back()
        .map(|_| imaging::VisibilityWriteTarget(&route));
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            mvc: mvc_taylor.as_ref().and_then(|view| {
                (view
                    .problem
                    .reconstruction()
                    .controls()
                    .max_minor_iterations()
                    > 0)
                .then_some(imaging::MvcCycleSpec {
                    taylor_problem: &view.problem,
                    expansion_reference_hz: view.expansion_reference_hz,
                    controls: publication.controls,
                    centres: publication.primary_beam_centres.as_ref(),
                })
            }),
            psf_cutoff: if !forms.psf && psf_beams.is_none() {
                casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF
            } else {
                publication.controls.psf_cutoff()
            },
            psf_beams,
            mask_beams,
            source: &route,
            masks,
            start_model,
            existing,
            forms,
            image_response,
            primary_beam,
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
    let mvc_run = mvc_taylor.is_some();
    // CASA creates the cube model when it invokes its first minor cycle.
    let cube_model_created = outcome.mvc_taylor.is_none() || !outcome.minor_cycles.is_empty();
    let products: Result<_, NativeError> = run_phase(
        "products",
        &runtime.cancel,
        runtime.diagnostics,
        summary,
        || {
            publish_run_products(
                RunProductInputs {
                    problem,
                    source: ProductSource {
                        scientific: outcome.scientific,
                        masks: outcome.masks,
                        facet_sum_weights: outcome.facet_sum_weights,
                        mem_alpha: outcome.mem_alpha,
                        saved_psf_beams,
                        vla_selection_hz,
                        mvc_beam: None,
                    },
                    taylor_view: mvc_taylor,
                    taylor_completion: outcome.mvc_taylor,
                    cube_model_created,
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
    let (products, taylor_names, names_reservation) = products?;
    let mut retained = outcome.retained;
    retained.join(names_reservation);
    for plan in &virtual_model {
        run_phase(
            "virtual model",
            &runtime.cancel,
            runtime.diagnostics,
            summary,
            || {
                crate::virtual_model::save(
                    problem,
                    products.scientific.final_model(),
                    plan,
                    &runtime,
                )
            },
        )?;
    }
    let workers = team.workers();
    let problem = problem.clone();
    Ok(move |summary| ImagingOutcome {
        problem,
        minor_cycles: outcome.minor_cycles,
        plane_rows: outcome.plane_rows,
        final_residual_statistics: outcome.final_residual_statistics,
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
        mvc_taylor_names: taylor_names,
        mvc_run,
        _retained: retained,
    })
}

/// One run's cube or continuum products and, for MVC, its Taylor view.
struct RunProductInputs<'a> {
    problem: &'a CompiledProblem,
    source: ProductSource<'a>,
    taylor_view: Option<crate::compile::CompiledTaylorView>,
    taylor_completion: Option<Admitted<crate::mvc::TaylorCompletion>>,
    cube_model_created: bool,
}

/// Reduce MVC's cube before staging both families, or publish an ordinary run.
fn publish_run_products(
    inputs: RunProductInputs<'_>,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    team: &WorkerTeam,
    published: &mut Vec<String>,
    lanes: &mut (usize, Option<String>),
) -> Result<(Products, Vec<String>, Reservation), NativeError> {
    let RunProductInputs {
        problem,
        source,
        taylor_view,
        taylor_completion,
        cube_model_created,
    } = inputs;
    if let Some(view) = taylor_view {
        let crate::compile::CompiledTaylorView {
            problem: taylor_problem,
            coordinates,
            expansion_reference_hz,
            reservation: _view_reservation,
        } = view;
        let taylor = if let Some(taylor) = taylor_completion {
            taylor
        } else {
            crate::mvc::assemble_taylor_completion(
                crate::mvc::TaylorAssemblyInputs {
                    cube_problem: problem,
                    cube: &source.scientific,
                    taylor_problem: &taylor_problem,
                    expansion_reference_hz,
                    controls: publication.controls,
                    centres: publication.primary_beam_centres.as_ref(),
                },
                (&runtime.host, &runtime.resource_policy, &runtime.cancel),
            )?
        };
        let products = publish_mvc_products(
            MvcProductSource {
                cube_problem: problem,
                cube_model_created,
                cube: source,
                taylor_problem: &taylor_problem,
                taylor,
                taylor_coordinates: coordinates,
            },
            runtime,
            publication,
            team,
            published,
            lanes,
        )?;
        Ok((
            products.cube,
            products.taylor_names,
            products.names_reservation,
        ))
    } else {
        if taylor_completion.is_some() {
            return Err(NativeError::Other(
                std::io::Error::other("MVC Taylor completion has no compiled view").into(),
            ));
        }
        let products = publish_products(
            problem,
            source,
            runtime,
            publication,
            team,
            published,
            lanes,
        )?;
        Ok((products, Vec::new(), Reservation::none()))
    }
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
enum MvcProductBeam<'a> {
    Cube(&'a [f32]),
    Taylor(&'a [f32]),
}

struct ProductSource<'a> {
    scientific: MajorCycleCompletion,
    masks: Option<ReconstructionMaskSet>,
    facet_sum_weights: FacetSumWeights,
    mem_alpha: Option<f32>,
    saved_psf_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    vla_selection_hz: Option<f64>,
    /// A separately admitted common PB, borrowed until the family is staged.
    mvc_beam: Option<MvcProductBeam<'a>>,
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
    source: ProductSource<'_>,
    runtime: &ApplicationRuntime,
    mut publication: ApplicationPublication,
    team: &WorkerTeam,
    published: &mut Vec<String>,
    lanes: &mut (usize, Option<String>),
) -> Result<Products, NativeError> {
    if let Some(alpha) = source.mem_alpha {
        publication.writer.set_mem_alpha(alpha)?;
    }
    let (products, held) =
        stage_products(problem, source, runtime, &publication, team, lanes, false)?;
    if runtime.cancel.is_cancelled() {
        // The staged members are removed when the writer drops.
        return Err(Cancelled.into());
    }
    publication
        .writer
        .publish(|image: &Path| published.push(image.to_string_lossy().into_owned()))?;
    // The charge covers the staged records until they are published.
    drop(held);
    Ok(products)
}

/// The two MVC families share one staging registry and one admitted beam.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC publication is being wired in #1069")
)]
struct MvcProductSource<'a> {
    cube_problem: &'a CompiledProblem,
    cube_model_created: bool,
    cube: ProductSource<'a>,
    taylor_problem: &'a CompiledProblem,
    taylor: Admitted<crate::mvc::TaylorCompletion>,
    taylor_coordinates: Vec<(
        casa_imaging_model::ImageDomainRole,
        casa_coordinates::CoordinateSystem,
    )>,
}

/// The cube remains the run's scientific result; Taylor names are separately
/// charged so the task's artifact inventory can report both families.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC publication is being wired in #1069")
)]
struct MvcProducts {
    cube: Products,
    taylor_names: Vec<String>,
    names_reservation: Reservation,
}

/// Stage both MVC families before publishing either. A failure or cancellation
/// drops the shared writer and discards its staged members.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC publication is being wired in #1069")
)]
fn publish_mvc_products(
    source: MvcProductSource<'_>,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    team: &WorkerTeam,
    published: &mut Vec<String>,
    lanes: &mut (usize, Option<String>),
) -> Result<MvcProducts, NativeError> {
    if runtime.cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let MvcProductSource {
        cube_problem,
        cube_model_created,
        cube,
        taylor_problem,
        taylor,
        taylor_coordinates,
    } = source;
    let Admitted {
        value:
            crate::mvc::TaylorCompletion {
                scientific: taylor_science,
                primary_beam,
            },
        reservation: taylor_reservation,
    } = taylor;
    let ProductSource {
        scientific,
        masks,
        facet_sum_weights,
        mem_alpha: _,
        saved_psf_beams,
        vla_selection_hz,
        mvc_beam: _,
    } = cube;
    let taylor_publication = ApplicationPublication {
        controls: publication.controls,
        primary_beam_centres: publication.primary_beam_centres.clone(),
        vla_selection_store: None,
        writer: publication.writer.with_coordinates(taylor_coordinates),
    };
    let (cube, cube_charge) = stage_products(
        cube_problem,
        ProductSource {
            scientific,
            masks: None,
            facet_sum_weights,
            mem_alpha: None,
            saved_psf_beams,
            vla_selection_hz,
            mvc_beam: Some(MvcProductBeam::Cube(&primary_beam)),
        },
        runtime,
        &publication,
        team,
        lanes,
        !cube_model_created,
    )?;
    let (taylor, taylor_charge) = stage_products(
        taylor_problem,
        ProductSource {
            scientific: taylor_science,
            masks,
            facet_sum_weights: Default::default(),
            mem_alpha: None,
            saved_psf_beams: None,
            vla_selection_hz: None,
            mvc_beam: Some(MvcProductBeam::Taylor(&primary_beam)),
        },
        runtime,
        &taylor_publication,
        team,
        lanes,
        false,
    )?;
    let count = taylor.planned.members().len();
    let overflow = || ProductsError::ResourceDemandOverflow("MVC product names");
    let bytes = taylor.planned.members().iter().try_fold(
        count
            .checked_mul(size_of::<String>())
            .ok_or_else(overflow)?,
        |sum, member| sum.checked_add(member.name().len()).ok_or_else(overflow),
    )?;
    let names_reservation = admit(
        &runtime.host,
        &runtime.resource_policy,
        &Demand {
            phase: "MVC product inventory",
            memory: u64::try_from(bytes).map_err(|_| overflow())?,
        },
    )?;
    let mut taylor_names = Vec::with_capacity(count);
    taylor_names.extend(
        taylor
            .planned
            .members()
            .iter()
            .map(|member| member.name().to_owned()),
    );
    if runtime.cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    publication
        .writer
        .publish(|image: &Path| published.push(image.to_string_lossy().into_owned()))?;
    drop(taylor);
    drop(taylor_charge);
    drop(cube_charge);
    drop(taylor_reservation);
    Ok(MvcProducts {
        cube,
        taylor_names,
        names_reservation,
    })
}

/// Stage one product family without publishing it. The caller holds its
/// admission until every staged family has been published or discarded.
fn stage_products(
    problem: &CompiledProblem,
    source: ProductSource<'_>,
    runtime: &ApplicationRuntime,
    publication: &ApplicationPublication,
    team: &WorkerTeam,
    lanes: &mut (usize, Option<String>),
    omit_uncreated_mvc_cube_model: bool,
) -> Result<(Products, casa_imaging_runtime::Reservation), NativeError> {
    let ProductSource {
        scientific,
        masks: reconstruction_masks,
        facet_sum_weights,
        mem_alpha: _,
        saved_psf_beams,
        vla_selection_hz,
        mvc_beam,
    } = source;
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific);
    if let Some(beam) = mvc_beam {
        inputs = match beam {
            MvcProductBeam::Cube(beam) => inputs.with_cube_taylor_primary_beam(beam)?,
            MvcProductBeam::Taylor(beam) => inputs
                .with_taylor_primary_beam(beam)?
                .with_taylor_residual_sum_weight(1.0)?,
        };
    }
    if let Some(frequency) = vla_selection_hz {
        inputs = inputs.with_vla_selection_frequency(frequency);
    }
    if let Some(beams) = saved_psf_beams.as_deref() {
        inputs = inputs.with_saved_psf_beams(beams)?;
    }
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
    let mut planned = PlannedContinuumGeneration::new(&inputs, &publication.controls)?;
    if omit_uncreated_mvc_cube_model {
        planned = planned.without_uncreated_mvc_cube_model();
    }
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
    Ok((
        Products {
            planned,
            scientific,
            published: generation,
        },
        held,
    ))
}

#[cfg(test)]
#[path = "native/mvc_publication_law.rs"]
mod mvc_publication_law;
