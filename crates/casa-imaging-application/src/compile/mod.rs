// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile an [`ImagingRequest`] against its MeasurementSet into the
//! problem specification, geometry and model lifecycle that
//! `casa_imaging_model::compile` turns into a `CompiledProblem`, with the
//! observation request, the masks and the native run's deployment.
//!
//! Compiling only reads: the deployment's file-system effects (output
//! directories, the AW cache) wait for [`Deployment::deploy`], after the
//! availability check.

#[cfg(test)]
mod aw_plan_tests;
mod direction;
mod domains;
mod envelope;
mod error;
mod instrument;
mod intents;
mod native_aw;
mod opening;
mod outliers;
mod partitions;
mod selection;
mod specification;
mod spectral;
mod start_model;
mod survey;

pub(crate) use opening::{OpenedStore, open_observation};
pub(crate) use start_model::StartModel;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_imaging_deconvolution::ImageDomainReconstructionMaskPlans;
use casa_imaging_model::{
    CentreLaws, FlagPolicy, GeometryInput, ModelBounds, ModelLifecycleRequirements,
    NativeAwRequestInput, NumericPrecision, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, PartitionSelection, ProblemSpecification, SelectedColumns,
    SequentialContinuumTransform, UvwCoordinateLaw,
};
use casa_imaging_operator::{AwCatalog, AwIndexing, NativeCacheLock};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, CasaImageWriter, ContinuumProductControls, MinorCycleImageResponse,
    PrimaryBeamCentres,
};
use casa_imaging_runtime::Admitted;
use casa_ms::PhaseCentreEphemeris;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::observation_io::Msv2Observation;
use casa_observation::Catalogue;
use casa_types::measures::frame::MeasFrame;

use crate::native::OpenedObservation;
use crate::{
    ApplicationNative, ApplicationPublication, ApplicationRuntime, AwCatalogDeployment, AwCfSource,
    AwProjection, BackendChoice, Deconvolver, Gridder, ImagingRequest, NativeAwCachePolicy,
    RunContext, SpecMode,
};
use direction::{Centre, ImageSpectralCoordinate};
use domains::PreparedImageDomain;
pub use error::{OutlierProblem, PrepareError};
pub(crate) use partitions::{PartitionSelectionError, resolve_partitions};
use selection::WindowChannels;
use specification::SpecificationInputs;
use spectral::{FrameContext, PreparedSpectralAxis};
use survey::{Survey, SurveyPlan};

/// A request compiled against its MeasurementSet, before the problem
/// compiler binds the observation.
pub(crate) struct Prepared {
    pub(crate) specification: ProblemSpecification,
    pub(crate) geometry: GeometryInput,
    pub(crate) model_lifecycle: ModelLifecycleRequirements,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    /// Static restart-mask support, including the run's clone, kept through publication.
    pub(crate) mask_resident: casa_imaging_runtime::Reservation,
    /// The main domain's start model ([`StartModel`]).
    pub(crate) start_model: Option<StartModel>,
    /// A direction-dependent gridder's residual units, which bind every
    /// solver.
    pub(crate) minor_cycle_image_response: Option<MinorCycleImageResponse>,
    /// The selected observation's manifest, which the problem compiles.
    pub(crate) snapshot: ObservationSnapshotInput,
    /// The observation, opened once for compile and the run.
    pub(crate) observation: OpenedObservation,
    pub(crate) write_model_column: bool,
    pub(crate) write_corrected_data: bool,
    /// What the native run deploys once the availability check passes.
    pub(crate) deployment: Deployment,
    /// The charge of compile's survey of the observation, held until the
    /// problem compiles.
    pub(crate) survey: casa_imaging_runtime::Reservation,
}

/// The observation as [`prepare`] surveyed it: its catalogue, its geometry
/// engine, the selection survey and the spectral axis.
struct Surveyed<'a> {
    catalogue: &'a Catalogue,
    /// The MeasurementSet, when the store is one.
    msv2: Option<&'a Msv2Observation>,
    engine: &'a MsCalEngine,
    survey: &'a Survey,
    spectral: &'a PreparedSpectralAxis,
}

/// Check outlier output prefixes with the same parser compile uses (#944).
pub(crate) fn check_outlier_outputs(path: &std::path::Path) -> Result<(), PrepareError> {
    let outputs = outliers::read_output_prefixes(path)?;
    for output in outputs {
        crate::restart::check_prefix(&output)?;
    }
    Ok(())
}

/// Compile `request` against its MeasurementSet.
///
/// What compile reads is admitted, not compile itself: store metadata is
/// admitted before opening, the observation opens once under its growing
/// grant ([`open_observation`]), and its survey
/// ([`survey::survey`]) is admitted before reading the selected partitions.
pub(crate) fn prepare(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<Prepared, PrepareError> {
    let opened = open_observation(&request.vis, (&context.host, &context.policy))?;
    let store = &opened.value;
    let catalogue = store.catalogue().as_ref();
    // The one Measures provider of the run: compile's geometry and the
    // batches' both evaluate with it.
    let measures = casa_ms::open_measures_runtime()?;
    let engine = MsCalEngine::from_catalogue(catalogue, Arc::clone(&measures))?;
    let (
        Admitted {
            value: mut survey,
            reservation: survey_charge,
        },
        columns,
    ) = survey_observation(request, store, context)?;
    if request.specmode == SpecMode::Mfs {
        let selected = survey
            .spectral_windows
            .iter()
            .map(|window| {
                Ok((
                    window.spw_id,
                    selection::selected_channels(request, window.spw_id, &window.frequencies_hz)?,
                ))
            })
            .collect::<Result<WindowChannels, PrepareError>>()?;
        let envelope = envelope::continuum_envelope((catalogue, &engine), &survey, &selected)?;
        survey.continuum = Some((selected, envelope));
    }
    let centre = direction::resolve_centre(request, catalogue, &survey, &engine)?;
    let frame = FrameContext {
        anchor_time_mjd_seconds: survey.first_time_mjd_seconds,
        time_bounds_mjd_seconds: survey.time_bounds_mjd_seconds,
        field_id: centre.field_id,
        phase: centre.phase.clone(),
        direction: centre.direction,
        engine: &engine,
    };
    let moving_rest_frame = moving_rest_frame(request, &centre, &frame)?;
    let spectral = spectral::prepare_axis(
        request,
        (
            store.msv2().map(Msv2Observation::measurement_set),
            catalogue,
        ),
        &survey,
        &frame,
        moving_rest_frame.as_ref(),
    )?;
    let surveyed_observation = Surveyed {
        catalogue,
        msv2: store.msv2(),
        engine: &engine,
        survey: &survey,
        spectral: &spectral,
    };
    let surveyed = &surveyed_observation;
    let (continuum_transform, channels) = continuum_selection(request, surveyed, &centre)?;
    let observation_info = direction::observation_info(catalogue, &survey, &centre, &engine)?;
    let mut domains = domains::prepare_domains(
        request,
        &centre,
        ImageSpectralCoordinate {
            frequency_reference: spectral.output_frequency_reference,
            reference_frequency_hz: spectral.reference_frequency_hz,
            increment_hz: spectral.increment_hz,
            rest_frequency_hz: spectral.image_rest_frequency_hz,
        },
        &observation_info,
    )?;
    let primary_beam = primary_beam_model(request, catalogue)?;
    let primary_beam_centres = match primary_beam {
        None | Some(AnalyticPrimaryBeamModel::MosaicSensitivity) => None,
        Some(_) => Some(direction::primary_beam_centres(
            &domains, store, &survey, &centre, &engine,
        )?),
    };
    let deployment = deployment(
        request,
        context,
        surveyed,
        &domains,
        primary_beam,
        primary_beam_centres,
    )?;
    let instrument = instrument::scientific_instrument_model(&request.gridder, catalogue)?;
    let specification = specification::specification(
        request,
        &spectral,
        specification_inputs(
            request,
            surveyed,
            instrument,
            primary_beam.is_some(),
            continuum_transform,
        )?,
    )?;
    let reconstruction_planes = match request.deconvolver {
        Deconvolver::Mtmfs => request.nterms,
        _ => spectral.output_channels,
    };
    let model_samples = domains::model_samples(&domains, reconstruction_planes, &request.stokes)?;
    let mut start_model = request
        .startmodel
        .as_deref()
        .map(|path| {
            StartModel::prepare(
                path,
                &domains[0],
                request.stokes.len(),
                reconstruction_planes,
            )
        })
        .transpose()?;
    let mut mask_resident = casa_imaging_runtime::Reservation::none();
    if request.tclean_restart {
        let template = casa_images::RegridTemplate {
            coordinates: domains[0].coordinates.clone(),
            shape: vec![domains[0].image_size[0], domains[0].image_size[1], 1, 1],
        };
        for path in crate::restart::products(&request.imagename)? {
            if path.extension().is_some_and(|suffix| suffix == "model") {
                start_model = Some(StartModel::restart(&path, &domains[0])?);
            } else {
                crate::restart::check_grid(&path, &template)?;
                if path.extension().is_some_and(|suffix| suffix == "mask") {
                    let input = crate::restart::MaskInput::open(&path)?;
                    let mut charge = casa_imaging_runtime::admit(
                        &context.host,
                        &context.policy,
                        &casa_imaging_runtime::Demand {
                            phase: "restart CLEAN mask",
                            memory: input.reading_bytes(),
                        },
                    )?;
                    let retained = input.retained_bytes();
                    domains[0].mask = domains::DomainMask::PixelSupport(input.read()?);
                    charge.retain(retained);
                    mask_resident.join(charge);
                }
            }
        }
    }
    let geometry = geometry(request, &domains, &centre, &spectral);
    let snapshot = snapshot_input(request, (store, columns), survey, &channels)?;
    // A moving phase centre names a body and a table alike; the table, when
    // it is one, was read here.
    let phase_table = centre
        .ephemeris
        .as_ref()
        .and_then(PhaseCentreEphemeris::external_series)
        .cloned();
    drop(engine);
    Ok(Prepared {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(model_samples, model_samples, f64::MAX, f64::MAX)?,
            NumericPrecision::F64,
        ),
        masks: domains::mask_plans(&domains)?,
        mask_resident,
        start_model,
        minor_cycle_image_response: match &request.gridder {
            Gridder::Mosaic { .. } | Gridder::Awproject(_) => Some(MinorCycleImageResponse::new(
                specification::normalization(request),
                specification::primary_beam_validity(request)?,
            )?),
            Gridder::Standard { .. } | Gridder::Wproject { .. } => None,
        },
        snapshot,
        observation: OpenedObservation {
            store: opened,
            measures,
            phase_table,
        },
        write_model_column: request.savemodel,
        write_corrected_data: request.save_continuum_residual,
        deployment,
        survey: survey_charge,
    })
}

/// Survey the partitions of `store` that `request` selects
/// ([`survey::survey`]): the selected windows' (or MSv2 data
/// description's) partitions holding a selected field and, under an intent
/// selection, a selected observing mode; their UVW when a UV-distance interval or a W-projecting
/// gridder needs it, and native AW's first cross-correlation sample.
fn survey_observation(
    request: &ImagingRequest,
    store: &OpenedStore,
    context: &RunContext,
) -> Result<(Admitted<Survey>, Columns), PrepareError> {
    let catalogue = store.catalogue();
    let windows = selection::window_choice(request, store)?;
    let (row_selection, states) = selection::row_selection(request, catalogue)?;
    let selected = partitions::selected(store, &windows, (&row_selection, &states))?;
    let holds = |column| selection::holds(store, &selected, column);
    let columns = Columns {
        visibility: selection::visibility_column(holds, request.datacolumn)?,
        corrected_data_present: holds(casa_imaging_model::VisibilityColumn::CorrectedData),
    };
    let candidates = partitions::candidates(store, &selected, &row_selection, columns.visibility)?;
    let spectral_windows = selection::spectral_windows(
        catalogue,
        candidates
            .partitions
            .iter()
            .map(|candidate| candidate.spectral_window),
    );
    let plan = SurveyPlan {
        observation: store.observation(),
        msv2: store.msv2(),
        candidates,
        row_selection,
        w_range: matches!(
            request.gridder,
            Gridder::Wproject { .. } | Gridder::Awproject(_)
        ),
        first_cross: matches!(
            &request.gridder,
            Gridder::Awproject(aw) if matches!(aw.cf_source, AwCfSource::NativeEvla { .. })
        ),
    };
    Ok((
        survey::survey(plan, spectral_windows, (&context.host, &context.policy))?,
        columns,
    ))
}

/// The columns compile chose for the selected partitions: the visibility
/// column imaged, and whether they hold `CORRECTED_DATA`
/// ([`selection::holds`]).
#[derive(Clone, Copy)]
struct Columns {
    visibility: casa_imaging_model::VisibilityColumn,
    corrected_data_present: bool,
}

/// The rest frame of a moving source (`cubesource`): the observatory frame
/// at the anchor time with the ephemeris's radial velocity.
fn moving_rest_frame(
    request: &ImagingRequest,
    centre: &Centre,
    frame: &FrameContext<'_>,
) -> Result<Option<MeasFrame>, PrepareError> {
    if request.specmode != SpecMode::Cubesource {
        return Ok(None);
    }
    let ephemeris = centre
        .ephemeris
        .as_ref()
        .ok_or(PrepareError::MovingPhaseCentre)?;
    let velocity = frame.engine.ephemeris_radial_velocity(
        frame.anchor_time_mjd_seconds,
        frame.field_id,
        ephemeris,
    )?;
    Ok(Some(
        frame
            .engine
            .spectral_frame_observatory_direction(
                frame.anchor_time_mjd_seconds,
                centre.phase.clone(),
            )?
            .with_radial_velocity(velocity),
    ))
}

/// The continuum transform of a `fitspw` request and the source channels
/// the observation reads: the fit channels with the output channels, or
/// the output channels alone.
fn continuum_selection(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    centre: &Centre,
) -> Result<(Option<SequentialContinuumTransform>, WindowChannels), PrepareError> {
    let Some(fitspw) = &request.fitspw else {
        return Ok((None, surveyed.spectral.selected_source_channels.clone()));
    };
    let window = selection::one_window(&surveyed.survey.spectral_windows, "continuum subtraction")?;
    let (transform, selected) = specification::continuum_transform(
        fitspw,
        request.fitorder,
        i32::try_from(centre.field_id).expect("the centre's field id is a stored i32 FIELD_ID"),
        window,
        surveyed.spectral.window_channels(window.spw_id),
    )?;
    Ok((Some(transform), BTreeMap::from([(window.spw_id, selected)])))
}

/// The specification's inputs beyond the request: the instrument, the
/// W and AW projections over the observed W range, the cube's density
/// padding and the continuum transform.
fn specification_inputs(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    instrument: Option<casa_imaging_model::InstrumentModel>,
    primary_beam: bool,
    continuum_transform: Option<SequentialContinuumTransform>,
) -> Result<SpecificationInputs, PrepareError> {
    let windows = &surveyed.survey.spectral_windows;
    Ok(SpecificationInputs {
        instrument,
        primary_beam,
        w_projection: match &request.gridder {
            Gridder::Wproject { wprojplanes } => Some(specification::w_projection(
                *wprojplanes,
                windows,
                surveyed.survey.w,
            )?),
            _ => None,
        },
        aw_projection: match &request.gridder {
            Gridder::Awproject(aw) => Some(specification::aw_projection(
                aw,
                windows,
                surveyed.survey.w,
            )?),
            _ => None,
        },
        cube_density_padding: cube_density_padding(request, surveyed)?,
        continuum_transform,
    })
}

/// The selected observation's manifest: the survey's samples, the channels
/// read with each selected window's whole frequency axis, the visibility
/// column, and the weights the selected partitions hold (each partition's
/// by CASA's rule, [`partitions::stored_weights`]).
fn snapshot_input(
    request: &ImagingRequest,
    (store, columns): (&OpenedStore, Columns),
    survey: Survey,
    channels: &WindowChannels,
) -> Result<ObservationSnapshotInput, PrepareError> {
    let visibility = columns.visibility;
    let weights = partitions::stored_weights(
        store,
        survey.partitions.iter().map(PartitionSelection::partition),
        visibility,
    );
    let selection = selection::observation_selection(survey, channels)?;
    Ok(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new(request.vis.display().to_string()),
            selection,
            SelectedColumns::new(visibility, FlagPolicy::FlagOrFlagRow, weights),
            columns.corrected_data_present,
        ),
    ]))
}

/// The image geometry: the domains, the centre laws and the spectral axis.
fn geometry(
    request: &ImagingRequest,
    domains: &[PreparedImageDomain],
    centre: &Centre,
    spectral: &PreparedSpectralAxis,
) -> GeometryInput {
    let direction_dependent = matches!(
        request.gridder,
        Gridder::Mosaic { .. } | Gridder::Awproject(_)
    );
    GeometryInput::new(
        domains.iter().map(PreparedImageDomain::spec).collect(),
        CentreLaws::new(
            centre.law.clone(),
            instrument::pointing_centre_law(&request.gridder),
        ),
        if direction_dependent {
            UvwCoordinateLaw::MosaicPhaseTrackingCentre
        } else {
            UvwCoordinateLaw::PhaseTrackingCentre
        },
        spectral.coordinate(),
    )
}

/// The primary beam the products are written with: the mosaic
/// sensitivity of a direction-dependent gridder, else the telescope's
/// analytic beam when `.pb` or `.image.pbcor` is asked for.
fn primary_beam_model(
    request: &ImagingRequest,
    catalogue: &Catalogue,
) -> Result<Option<AnalyticPrimaryBeamModel>, PrepareError> {
    Ok(match request.gridder {
        Gridder::Mosaic { .. } | Gridder::Awproject(_) => {
            Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
        }
        Gridder::Standard { .. } | Gridder::Wproject { .. }
            if request.write_pb || request.pbcor =>
        {
            Some(instrument::standard_primary_beam_model(catalogue)?)
        }
        Gridder::Standard { .. } | Gridder::Wproject { .. } => None,
    })
}

/// CASA's padded per-channel density grid of a linearly interpolated
/// cube with per-channel density.
fn cube_density_padding(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
) -> Result<Option<usize>, PrepareError> {
    let (survey, spectral) = (surveyed.survey, surveyed.spectral);
    if !specification::cube_density_padded(request, spectral) {
        return Ok(None);
    }
    let window = selection::one_window(&survey.spectral_windows, "cube density")?;
    Ok(Some(envelope::cube_density_padding(
        (surveyed.catalogue, surveyed.engine),
        survey,
        window,
        &envelope::OutputAxis {
            frequency_reference: spectral.output_frequency_reference,
            centres_hz: [
                spectral.reference_frequency_hz,
                spectral.reference_frequency_hz
                    + (spectral.output_channels - 1) as f64 * spectral.increment_hz,
            ],
            channels: spectral.output_channels,
        },
    )?))
}

/// The native run's runtime, product publication and AW catalog, still to
/// be deployed: no directory exists and no AW cell is generated until
/// [`Deployment::deploy`].
pub(crate) struct Deployment {
    runtime: PendingRuntime,
    publication: ApplicationPublication,
    /// Every domain's output directory.
    output_directories: Vec<PathBuf>,
    aw: Option<AwPlan>,
}

/// The run's runtime but for its device, which the compiled problem resolves
/// ([`Deployment::deploy`]); its spill directory is made canonical on
/// deployment.
struct PendingRuntime {
    host: crate::HostResources,
    resource_policy: crate::ResourcePolicy,
    grid_precision: Option<crate::GridPrecision>,
    cancel: crate::Cancel,
    spill_directory: PathBuf,
    diagnostics: crate::Diagnostics,
}

/// The AW catalog a run opens and, for a native cache, the cells to bring
/// to the request's policy.
struct AwPlan {
    root: PathBuf,
    native: Option<(NativeAwCachePolicy, NativeAwRequestInput)>,
    indexing: AwIndexing,
    resident_bytes: usize,
    /// Whether the run raises the bound into the memory its workers leave
    /// (`cf_resident_mb = auto`).
    automatic: bool,
}

/// The cell cache's bound for `cf_resident_mb = auto` before the run raises
/// it into the memory its workers leave (#449).
const AUTOMATIC_CELL_CACHE_FLOOR: usize = 256 << 20;

impl Deployment {
    /// Create every domain's output directory, settle the spill directory
    /// beside the main image, bring a native AW cache to the request's
    /// policy and grid on `backend`, the device the request resolved to on
    /// the compiled problem ([`crate::availability::resolve_backend`]).
    pub(crate) fn deploy(self, backend: BackendChoice) -> Result<ApplicationNative, PrepareError> {
        for directory in &self.output_directories {
            std::fs::create_dir_all(directory).map_err(|source| PrepareError::OutputDirectory {
                path: directory.clone(),
                source,
            })?;
        }
        let PendingRuntime {
            host,
            resource_policy,
            grid_precision,
            cancel,
            spill_directory,
            diagnostics,
        } = self.runtime;
        let spill_directory =
            spill_directory
                .canonicalize()
                .map_err(|source| PrepareError::OutputDirectory {
                    path: spill_directory.clone(),
                    source,
                })?;
        Ok(ApplicationNative {
            runtime: ApplicationRuntime {
                host,
                resource_policy,
                backend,
                grid_precision,
                cancel,
                spill_directory,
                diagnostics,
            },
            publication: self.publication,
            aw_catalog: self
                .aw
                .map(|aw| aw.deploy((&host, &resource_policy)))
                .transpose()?,
        })
    }
}

impl AwPlan {
    /// Under every policy the directory is compared with the request: its
    /// request manifest, the cells the request names, and any it does not
    /// (#735: a cell's conjugate beam and its lookup depend on the whole
    /// frequency list). Reuse needs a directory generated for this request,
    /// complete and holding nothing else. Generation fills in the absent
    /// cells of such a directory, and clears and regenerates any other.
    /// Regeneration clears the cache first. The loader then checks each
    /// cell's sky increment against the image (plan section 5.6).
    ///
    /// The run reads a native cache under its shared [`NativeCacheLock`],
    /// held in the deployment for as long as the run lives, since cells load
    /// lazily. It writes one only under the exclusive lock, which it cannot
    /// take while another run reads the cache. After writing, it takes the
    /// shared lock again and checks the directory once more before it keeps
    /// it (#739 review). Generation is admitted on `admission`'s host and
    /// policy before it starts and released when it ends (#738).
    fn deploy(
        self,
        admission: (&crate::HostResources, &crate::ResourcePolicy),
    ) -> Result<AwCatalogDeployment, PrepareError> {
        let (lock, generation_workers) = match &self.native {
            Some((policy, input)) => {
                let (lock, workers) = self.bring_native(*policy, input, admission)?;
                (Some(Arc::new(lock)), workers)
            }
            None => (None, None),
        };
        Ok(AwCatalogDeployment {
            root: self.root,
            indexing: self.indexing,
            resident_bytes: self.resident_bytes,
            automatic: self.automatic,
            _lock: lock,
            generation_workers,
        })
    }

    /// Bring the native cache to `policy` for `input` and return the shared
    /// lock the run keeps while it reads it ([`Self::deploy`]), and the
    /// workers that generated cells, if any did.
    fn bring_native(
        &self,
        policy: NativeAwCachePolicy,
        input: &NativeAwRequestInput,
        (host, resources): (&crate::HostResources, &crate::ResourcePolicy),
    ) -> Result<(NativeCacheLock, Option<usize>), PrepareError> {
        let root = &self.root;
        let shared = NativeCacheLock::shared(root)?;
        let state = AwCatalog::native_cache_state(root, input)?;
        let fill = match policy {
            NativeAwCachePolicy::ReuseOnly if state.reusable() => return Ok((shared, None)),
            NativeAwCachePolicy::ReuseOnly => {
                return Err(PrepareError::AwCacheNotReusable { state });
            }
            NativeAwCachePolicy::GenerateMissing if state.reusable() => return Ok((shared, None)),
            NativeAwCachePolicy::GenerateMissing if state.fillable() => true,
            NativeAwCachePolicy::GenerateMissing => {
                tracing::warn!(
                    "native AW cache {} does not belong to this request ({state}); \
                     regenerating it",
                    root.display()
                );
                false
            }
            NativeAwCachePolicy::Regenerate => false,
        };
        drop(shared);
        let exclusive = NativeCacheLock::exclusive(root)?;
        // The most workers, up to the policy's and one per cell to generate,
        // whose workspaces fit what the policy leaves free (#708); one when
        // none does, which admission then refuses. Generation gives each of
        // them a unit (#747 review).
        let cells = if fill {
            state.expected - state.present
        } else {
            state.expected
        };
        let free = casa_imaging_runtime::free_memory(host, resources);
        let workers = (1..=resources.workers(host).min(cells))
            .rev()
            .find(|&workers| AwCatalog::generation_bytes(input, workers) <= free)
            .unwrap_or(1);
        let generating = casa_imaging_runtime::admit(
            host,
            resources,
            &casa_imaging_runtime::Demand {
                phase: "native AW generation",
                memory: AwCatalog::generation_bytes(input, workers),
            },
        )?;
        tracing::info!(
            "generating {cells} native AW cells into {} on {workers} workers",
            root.display()
        );
        AwCatalog::generate_native(root, input, fill, workers)?;
        drop(generating);
        drop(exclusive);
        let shared = NativeCacheLock::shared(root)?;
        let state = AwCatalog::native_cache_state(root, input)?;
        if !state.reusable() {
            // Another run replaced the cache between our write and our read.
            return Err(PrepareError::AwCacheNotReusable { state });
        }
        Ok((shared, Some(workers)))
    }
}

/// The native run's deployment: the runtime with the paged cube state
/// beside the main image, the product controls and image writer, and the AW
/// plan.
fn deployment(
    request: &ImagingRequest,
    context: &RunContext,
    surveyed: &Surveyed<'_>,
    domains: &[PreparedImageDomain],
    primary_beam: Option<AnalyticPrimaryBeamModel>,
    primary_beam_centres: Option<PrimaryBeamCentres>,
) -> Result<Deployment, PrepareError> {
    let parent = |path: &Path| {
        path.parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    };
    let mut controls = ContinuumProductControls::new(request.psfcutoff as f32).map_err(|_| {
        PrepareError::PsfCutoff {
            psfcutoff: request.psfcutoff,
        }
    })?;
    if let Some(model) = primary_beam {
        controls = controls.with_primary_beam_model(model);
    }
    Ok(Deployment {
        runtime: PendingRuntime {
            host: context.host,
            resource_policy: context.policy,
            grid_precision: request.gridprecision,
            cancel: context.cancel.clone(),
            spill_directory: parent(&request.imagename),
            diagnostics: request.diagnostics,
        },
        publication: ApplicationPublication {
            controls,
            primary_beam_centres,
            writer: CasaImageWriter::for_domains(domains.iter().map(PreparedImageDomain::output)),
        },
        output_directories: domains
            .iter()
            .map(|domain| parent(&domain.output))
            .collect(),
        aw: match &request.gridder {
            Gridder::Awproject(aw) => Some(aw_plan(request, aw, surveyed)?),
            _ => None,
        },
    })
}

/// The AW catalog to open: a CASA cache read as it is, or a native cache
/// in CASA's format with the input its cells are generated from.
fn aw_plan(
    request: &ImagingRequest,
    aw: &AwProjection,
    surveyed: &Surveyed<'_>,
) -> Result<AwPlan, PrepareError> {
    let (root, native) = match &aw.cf_source {
        AwCfSource::CasaImport { cfcache } => (cfcache.clone(), None),
        AwCfSource::NativeEvla {
            native_cf_cache,
            native_cf_policy,
            ..
        } => {
            let first_cross = surveyed
                .survey
                .first_cross
                .ok_or(PrepareError::NoCrossCorrelationRow)?;
            let input = native_aw::resolve(request, aw, surveyed, first_cross)?;
            input.validate()?;
            (native_cf_cache.clone(), Some((*native_cf_policy, input)))
        }
    };
    Ok(AwPlan {
        root,
        native,
        indexing: AwIndexing {
            conjugate_beams: true,
            image_reference_hz: surveyed.spectral.reference_frequency_hz,
        },
        resident_bytes: match aw.cf_resident_mb {
            Some(megabytes) => megabytes
                .checked_mul(1 << 20)
                .ok_or(PrepareError::CfResidentSize { megabytes })?,
            None => AUTOMATIC_CELL_CACHE_FLOOR,
        },
        automatic: aw.cf_resident_mb.is_none(),
    })
}
