// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native run: the source admitted, the major and minor cycles on one
//! worker team, and the products, each phase recorded in the run summary.

use std::cell::OnceCell;
use std::path::Path;
use std::sync::Arc;

use casa_core::measures::MeasuresProvider;
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, ReconstructionMaskSet,
};
use casa_imaging_model::{CompiledProblem, GeometryPlan, InstrumentResponse, RestoringBeamPolicy};
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

use crate::compile::{OpenedStore, resolve_partitions};

mod psf;
use crate::imaging::{self, ImagingError};
use crate::{
    ApplicationDispatchError, ApplicationError, ApplicationNative, ApplicationPublication,
    ApplicationRuntime, AwCatalogDeployment, Awp2Deployment, ImagingOutcome,
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
    pub(crate) psf_view: Option<CompiledProblem>,
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
    pub(crate) outlier_models: Vec<Option<crate::compile::StartModel>>,
    pub(crate) outlier_psfs: Vec<Option<crate::restart::ExistingProducts>>,
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
    psf_view: Option<psf::PsfRoute>,
    runtime: ApplicationRuntime,
    publication: ApplicationPublication,
    aw_catalog: Option<AwCatalogDeployment>,
    awp2: Option<Awp2Deployment>,
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
    outlier_models: Vec<Option<crate::compile::StartModel>>,
    outlier_psfs: Vec<Option<crate::restart::ExistingProducts>>,
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
    let write_back = (model_data || corrected_data).then(|| {
        imaging::WriteBack::new(problem, model_data, corrected_data, !predicts_final_model)
    });
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
            mosaic_voltage_pattern: None,
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
        mut publication,
        aw_catalog,
        awp2,
        mosaic_voltage_pattern,
    } = native;
    if let Some(existing) = &input.existing
        && let Some(beam) = existing.mosaic_beam()
    {
        publication.controls = publication
            .controls
            .preserving_mosaic_products(beam.reading_bytes(), existing.has_higher_primary_beam());
    }
    if !input.forms.psf && !input.outlier_psfs.is_empty() {
        publication.controls = publication.controls.preserving_saved_psfs(
            input
                .existing
                .iter()
                .chain(input.outlier_psfs.iter().flatten())
                .any(|saved| saved.psf_beams().is_none()),
            input.forms.residual,
        );
    }
    if mvc_taylor.is_none()
        && !input.forms.psf
        && !input.forms.residual
        && input.minor_cycle_image_response.is_none()
        && problem.reconstruction().controls().max_minor_iterations() == 0
        && let Some(saved) = &input.existing
    {
        let beam_policy = problem.products().restoring_beam();
        let operator = problem.science().measurement_equation();
        if saved.reads_taylor_residual() {
            // CASA keeps the saved Taylor moments and residuals. It fits
            // and persists a missing principal PSF beam during restore.
            publication.controls = publication
                .controls
                .preserving_saved_psfs(
                    saved.psf_beams().is_none() && beam_policy != RestoringBeamPolicy::None,
                    false,
                )
                .preserving_saved_residuals();
        } else if saved.reads_cube_residual()
            && operator.instrument_response() == InstrumentResponse::Scalar
            && operator.w_projection().is_none()
            && beam_policy == RestoringBeamPolicy::Common
            && saved.psf_beams().is_some()
        {
            // SIImageStore::restore gets the common beam from the saved
            // grid and attaches it only to the restored image.
            publication.controls = publication
                .controls
                .preserving_saved_psfs(false, false)
                .preserving_saved_residuals();
        }
    }
    publication.controls.validate_for_problem(problem)?;
    let keeps_pb_mask = problem.products().validity().uncorrected_mask()
        == casa_imaging_model::UncorrectedImageMaskPolicy::PrimaryBeam;
    let saved_fields = saved_field_mask_bytes(input.existing.as_ref(), &input.outlier_psfs);
    let held = |major_cycles| {
        let controls = controls_after_cycles(
            publication.controls,
            input.existing.as_ref(),
            saved_fields,
            major_cycles,
            keeps_pb_mask,
            input.forms.psf,
        );
        products_floor(problem, &publication, controls)
    };
    let products = if problem.reconstruction().controls().max_minor_iterations() == 0 {
        held(0)?
    } else {
        held(0)?.max(held(1)?)
    };
    let measures = input
        .psf_view
        .as_ref()
        .map(|_| Arc::clone(&input.observation.measures));
    let Admitted {
        value: mut route,
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
    route.mosaic_voltage_pattern = mosaic_voltage_pattern;
    let psf_view = input
        .psf_view
        .zip(measures)
        .map(|(view, measures)| psf::PsfRoute::new(view, measures, (problem, &route), &runtime))
        .transpose()?;
    Ok(NativeRun {
        mvc_taylor,
        psf_view,
        runtime,
        publication,
        aw_catalog,
        awp2,
        route: Box::new(route),
        source,
        products,
        masks: input.masks,
        start_model: input.start_model,
        outlier_models: input.outlier_models,
        outlier_psfs: input.outlier_psfs,
        existing: input.existing,
        forms: input.forms,
        image_response: input.minor_cycle_image_response,
        virtual_model: input.virtual_model,
    })
}

struct SavedBeams {
    mosaic: Option<casa_imaging_products::SavedMosaicBeam>,
    psf: Option<Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    psf_pixels: Option<Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    outliers: Vec<Option<Arc<[casa_imaging_deconvolution::RestoringBeam]>>>,
    outlier_pixels: Vec<Option<Arc<[casa_imaging_deconvolution::RestoringBeam]>>>,
    mask_pixels: Option<Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
}

impl SavedBeams {
    fn read(
        existing: Option<&crate::restart::ExistingProducts>,
        outlier_psfs: &[Option<crate::restart::ExistingProducts>],
    ) -> Self {
        use crate::restart::ExistingProducts;
        Self {
            mosaic: existing.and_then(ExistingProducts::mosaic_beam).cloned(),
            psf: existing.and_then(ExistingProducts::psf_beams),
            psf_pixels: existing.and_then(ExistingProducts::psf_pixel_beams),
            outliers: outlier_psfs
                .iter()
                .map(|saved| saved.as_ref().and_then(ExistingProducts::psf_beams))
                .collect(),
            outlier_pixels: outlier_psfs
                .iter()
                .map(|saved| saved.as_ref().and_then(ExistingProducts::psf_pixel_beams))
                .collect(),
            mask_pixels: existing.and_then(ExistingProducts::mask_beams),
        }
    }
}

fn saved_outlier_products(
    forms: imaging::Forms,
    outliers: Vec<Option<crate::restart::ExistingProducts>>,
) -> Vec<crate::restart::ExistingProducts> {
    if forms.psf && forms.residual {
        return Vec::new();
    }
    outliers
        .into_iter()
        .map(|products| products.expect("checked saved outlier products"))
        .collect()
}

fn saved_psf_controls(
    mut publication: ApplicationPublication,
    forms: imaging::Forms,
    missing_beam: bool,
    problem: &CompiledProblem,
) -> Result<ApplicationPublication, ProductsError> {
    if !forms.psf && missing_beam && problem.reconstruction().controls().max_minor_iterations() > 0
    {
        publication.controls = publication
            .controls
            .with_psf_cutoff(casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF)?;
    }
    if !forms.psf {
        publication.controls = publication.controls.with_reused_single_psf();
    }
    Ok(publication)
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
        psf_view,
        runtime,
        publication,
        aw_catalog,
        awp2,
        mut route,
        source,
        products,
        masks,
        start_model,
        outlier_models,
        outlier_psfs,
        existing,
        forms,
        image_response,
        virtual_model,
    } = run;
    let SavedBeams {
        mosaic: saved_mosaic_beam,
        psf: saved_psf_beams,
        psf_pixels: psf_beams,
        outliers: outlier_psf_beams,
        outlier_pixels: outlier_psf_pixel_beams,
        mask_pixels: mask_beams,
    } = SavedBeams::read(existing.as_ref(), &outlier_psfs);
    // An unlabelled saved PSF is fitted by getBeamSet's default cutoff at
    // minor-cycle initialization. Publication must reuse that fit's cutoff.
    let missing_beam = psf_beams.is_none() || outlier_psf_beams.iter().any(Option::is_none);
    let publication = saved_psf_controls(publication, forms, missing_beam, problem)?;
    let vla_selection_hz = publication
        .vla_selection_store
        .map(|store| vla_selection_frequency(problem, &route, store));
    let teams = OnceCell::new();
    let centres = publication.primary_beam_centres.as_ref();
    let primary_beam = match (
        publication.controls.primary_beam_model(),
        publication.saved_radial_primary_beam.as_ref(),
    ) {
        (Some(casa_imaging_products::AnalyticPrimaryBeamModel::SavedRadial), Some(saved)) => Some(
            AnalyticPrimaryBeam::from_saved(problem, centres, &saved.value)?,
        ),
        (Some(model), _) => AnalyticPrimaryBeam::new(problem, model, centres)?,
        (None, _) => None,
    }
    .map(|beam| {
        vla_selection_hz.map_or(beam, |frequency| {
            beam.with_vla_selection_frequency(frequency)
        })
    });
    let separate_psf = psf_view
        .map(|view| view.form(&mut route, &runtime, summary))
        .transpose()?;
    // The run's source borrows the batch route, which is released with the
    // source's charge once the run has finished with it.
    let visibility_write = route
        .write_back()
        .map(|_| imaging::VisibilityWriteTarget(&route));
    let saved_fields = saved_field_mask_bytes(existing.as_ref(), &outlier_psfs);
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
                    start: view.start.as_ref(),
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
            outlier_psf_beams: outlier_psf_pixel_beams,
            mask_beams,
            source: &route,
            separate_psf,
            masks,
            start_model,
            outlier_models,
            outlier_products: saved_outlier_products(forms, outlier_psfs),
            existing: existing.clone(),
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
            awp2,
            backend: runtime.backend,
            grid_precision: runtime.grid_precision,
            ft_padding: runtime.ft_padding,
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
    let cube_model_created = outcome.mvc_taylor.is_none()
        || !outcome.minor_cycles.is_empty()
        || mvc_taylor.as_ref().is_some_and(|view| view.start.is_some());
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
                        outlier_psf_beams,
                        vla_selection_hz,
                        primary_beam: saved_mosaic_beam
                            .as_ref()
                            .map(ProductPrimaryBeam::SavedMosaic),
                    },
                    taylor_view: mvc_taylor,
                    taylor_completion: outcome.mvc_taylor,
                    cube_model_created,
                    major_cycles: outcome.major_cycle_count,
                },
                &runtime,
                ApplicationPublication {
                    controls: controls_after_cycles(
                        publication.controls,
                        existing.as_ref(),
                        saved_fields,
                        outcome.major_cycle_count,
                        problem.products().validity().uncorrected_mask()
                            == casa_imaging_model::UncorrectedImageMaskPolicy::PrimaryBeam,
                        forms.psf,
                    ),
                    ..publication
                },
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
    save_virtual_models(problem, &runtime, &products, &virtual_model, summary)?;
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

/// Save the final virtual models after the image products are published.
fn save_virtual_models(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    products: &Products,
    virtual_model: &[crate::virtual_model::VirtualModelPlan],
    summary: &mut RunSummary,
) -> Result<(), NativeError> {
    for plan in virtual_model {
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
                    runtime,
                )
            },
        )?;
    }
    Ok(())
}

/// CASA selects the cube's VLA aperture through its LSRK conversion layer,
/// but evaluates it at each output plane's own frequency.
fn vla_selection_frequency(
    problem: &CompiledProblem,
    route: &imaging::BatchRoute,
    store: usize,
) -> f64 {
    let factor = route.stores[store]
        .plan
        .output_lsrk_factor()
        .expect("a tracked cube has an LSRK conversion layer");
    problem
        .geometry()
        .spectral()
        .channel_centre_hz(0)
        .expect("a cube has a first channel")
        * factor
}

/// One run's cube or continuum products and, for MVC, its Taylor view.
struct RunProductInputs<'a> {
    problem: &'a CompiledProblem,
    source: ProductSource<'a>,
    taylor_view: Option<crate::compile::CompiledTaylorView>,
    taylor_completion: Option<Admitted<crate::mvc::TaylorCompletion>>,
    cube_model_created: bool,
    major_cycles: usize,
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
        major_cycles,
    } = inputs;
    if let Some(view) = taylor_view {
        let crate::compile::CompiledTaylorView {
            problem: taylor_problem,
            coordinates,
            expansion_reference_hz,
            reservation: _view_reservation,
            start,
        } = view;
        let saved = start.as_ref().and_then(|start| start.saved_psf.as_ref());
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
                    read_saved_residual: false,
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
                saved,
                saved_residual_mask_bytes: (major_cycles == 0)
                    .then(|| saved.and_then(|saved| saved.taylor.saved_residual_mask_bytes()))
                    .flatten(),
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
enum ProductPrimaryBeam<'a> {
    MvcCube(
        &'a [f32],
        Option<&'a casa_imaging_products::SavedMosaicBeam>,
        bool,
    ),
    MvcTaylor(&'a [f32], Option<&'a crate::compile::MvcSavedPsf>),
    SavedMosaic(&'a casa_imaging_products::SavedMosaicBeam),
}

struct ProductSource<'a> {
    scientific: MajorCycleCompletion,
    masks: Option<ReconstructionMaskSet>,
    facet_sum_weights: FacetSumWeights,
    mem_alpha: Option<f32>,
    saved_psf_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    outlier_psf_beams: Vec<Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>>,
    vla_selection_hz: Option<f64>,
    /// A separately admitted PB, borrowed until the family is staged.
    primary_beam: Option<ProductPrimaryBeam<'a>>,
}

/// Per-member mask admission before and after a major cycle. Each
/// publication lane reads one field at a time.
fn saved_field_mask_bytes(
    main: Option<&crate::restart::ExistingProducts>,
    outliers: &[Option<crate::restart::ExistingProducts>],
) -> Option<[u64; 3]> {
    let main = main.filter(|saved| !outliers.is_empty() && saved.reads_residual())?;
    let mut before = 0;
    let mut after = 0;
    let mut retained = 0;
    for saved in std::iter::once(main).chain(outliers.iter().flatten()) {
        let bytes = saved.saved_residual_mask_bytes().unwrap_or(0);
        retained = retained.max(saved.restored_mask_bytes());
        if saved.custom_residual_mask() {
            retained = retained.max(bytes);
        }
        before = before.max(bytes);
        after = after.max(bytes).max(saved.primary_beam_mask_bytes());
    }
    Some([before.max(retained), after.max(retained), retained])
}

/// CASA keeps the saved residual in image units until the first major
/// cycle, then retains its mask when residual division leaves it in place.
fn controls_after_cycles(
    controls: casa_imaging_products::ContinuumProductControls,
    existing: Option<&crate::restart::ExistingProducts>,
    saved_fields: Option<[u64; 3]>,
    major_cycles: usize,
    keeps_pb_mask: bool,
    forms_psf: bool,
) -> casa_imaging_products::ContinuumProductControls {
    if let Some([before, after, retained]) = saved_fields {
        let rebuilds_pb = forms_psf && controls.primary_beam_model().is_some();
        let (bytes, refreshed) = if major_cycles == 0 {
            (before, false)
        } else {
            (if keeps_pb_mask { after } else { retained }, true)
        };
        // CASA's makePB keeps any generated mask on the old PB. Its
        // reader is also live for a stopped run that rebuilds the PB.
        let bytes = if rebuilds_pb { bytes.max(after) } else { bytes };
        return controls.with_saved_field_residuals(
            bytes,
            refreshed,
            keeps_pb_mask,
            (forms_psf, rebuilds_pb),
        );
    }
    let Some(existing) = existing else {
        return controls;
    };
    let Some(bytes) = existing.saved_residual_mask_bytes() else {
        return controls;
    };
    let cube = existing.reads_mosaic_cube_residual();
    let controls = if cube {
        controls.with_saved_cube_pbcor_mask(existing.saved_pbcor_mask_bytes())
    } else {
        controls
    };
    if major_cycles == 0 {
        controls.preserving_weighted_residuals(bytes)
    } else if cube {
        let controls = controls.with_saved_cube_model(existing.saved_model_plane_bytes());
        if keeps_pb_mask
            && existing
                .mosaic_beam()
                .is_none_or(|beam| beam.has_generated_mask())
        {
            controls
        } else {
            controls.with_saved_residual_mask(bytes)
        }
    } else if keeps_pb_mask && existing.reads_mosaic_residual() && bytes > 0 {
        controls.with_saved_residual_mask(bytes)
    } else {
        controls
    }
}

/// What `problem`'s products hold on one lane, before the run
/// (`PlannedContinuumGeneration::planned_demand`): the least they can run
/// on, which the run's setup fits beside it.
fn products_floor(
    problem: &CompiledProblem,
    publication: &ApplicationPublication,
    controls: casa_imaging_products::ContinuumProductControls,
) -> Result<u64, NativeError> {
    Ok(PlannedContinuumGeneration::for_problem(problem, &controls)?
        .planned_demand(problem, ProductStoragePlan::new(1, 1)?, &publication.writer)?
        .peak_residency_bytes())
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
    saved: Option<&'a crate::compile::MvcSavedPsf>,
    saved_residual_mask_bytes: Option<u64>,
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
        saved,
        saved_residual_mask_bytes,
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
        outlier_psf_beams,
        vla_selection_hz,
        primary_beam: _,
    } = cube;
    let taylor_publication = ApplicationPublication {
        controls: saved_residual_mask_bytes.map_or(publication.controls, |bytes| {
            publication.controls.preserving_weighted_residuals(bytes)
        }),
        primary_beam_centres: publication.primary_beam_centres.clone(),
        saved_radial_primary_beam: None,
        vla_selection_store: None,
        saved_mem_model: false,
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
            outlier_psf_beams,
            vla_selection_hz,
            primary_beam: Some(ProductPrimaryBeam::MvcCube(
                &primary_beam,
                saved.map(|saved| &saved.cube_pb),
                saved_residual_mask_bytes.is_some(),
            )),
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
            saved_psf_beams: saved.and_then(|saved| saved.taylor.psf_beams()),
            outlier_psf_beams: Vec::new(),
            vla_selection_hz: None,
            primary_beam: Some(ProductPrimaryBeam::MvcTaylor(&primary_beam, saved)),
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
        mem_alpha,
        saved_psf_beams,
        outlier_psf_beams,
        vla_selection_hz,
        primary_beam,
    } = source;
    let mut controls = publication.controls;
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific);
    let mut preserve_cube_residual = false;
    if let Some(beam) = primary_beam {
        let saved = match beam {
            ProductPrimaryBeam::MvcCube(beam, saved, preserve_residual) => {
                preserve_cube_residual = preserve_residual;
                if !preserve_residual {
                    inputs = inputs.with_cube_taylor_primary_beam(beam)?;
                }
                saved.map(|beam| (beam, false))
            }
            ProductPrimaryBeam::MvcTaylor(beam, saved) => {
                inputs = inputs
                    .with_taylor_primary_beam(beam)?
                    .with_taylor_residual_sum_weight(1.0)?;
                saved.map(|saved| (&saved.taylor_pb, saved.taylor.has_higher_primary_beam()))
            }
            ProductPrimaryBeam::SavedMosaic(beam) => {
                inputs = inputs.with_saved_mosaic_beam(beam)?;
                None
            }
        };
        if let Some((beam, higher)) = saved {
            inputs = inputs.with_saved_mosaic_beam(beam)?;
            controls = controls.preserving_mosaic_products(beam.reading_bytes(), higher);
        }
    }
    if let Some(frequency) = vla_selection_hz {
        inputs = inputs.with_vla_selection_frequency(frequency);
    }
    if let Some(beams) = saved_psf_beams.as_deref() {
        inputs = inputs.with_saved_psf_beams(beams)?;
    }
    if !outlier_psf_beams.is_empty() {
        inputs = inputs.with_saved_outlier_psf_beams(&outlier_psf_beams)?;
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
    if let Some(beam) = &publication.saved_radial_primary_beam {
        inputs = inputs.with_saved_radial_primary_beam(&beam.value);
    }
    if !facet_sum_weights.is_empty() {
        inputs = inputs.with_facet_sum_weights(&facet_sum_weights);
    }
    let mut planned = PlannedContinuumGeneration::new(&inputs, &controls)?;
    // CASA writes the MEM model and its multipliers in takeOneStep.
    // A residual refresh with no MEM step leaves the model untouched.
    if omit_uncreated_mvc_cube_model || (publication.saved_mem_model && mem_alpha.is_none()) {
        planned = planned.without_single_term_model();
    }
    if preserve_cube_residual {
        planned = planned.without_mvc_cube_residual();
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
#[path = "mvc_publication_law.rs"]
mod mvc_publication_law;
