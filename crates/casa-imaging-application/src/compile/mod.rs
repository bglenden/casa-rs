// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile an [`ImagingRequest`] against its MeasurementSets into the
//! problem specification, geometry and model lifecycle that
//! `casa_imaging_model::compile` turns into a `CompiledProblem`, with the
//! observation request, the masks and the native run's deployment.
//!
//! Compiling only reads: the deployment's file-system effects (output
//! directories, the AW cache) wait for [`Deployment::deploy`], after the
//! availability check.

#[cfg(test)]
mod aw_plan_tests;
mod continuation;
mod direction;
mod domains;
mod envelope;
mod error;
mod frequency_bands;
mod instrument;
mod intents;
mod mask_image;
pub(crate) use mask_image::{MaskImage, open_onto_itself as open_mask_image_onto_itself};
mod native_aw;
mod opening;
mod outliers;
mod partitions;
mod selection;
mod specification;
mod spectral;
mod start_model;
mod stores;
mod survey;
mod time_selection;
mod virtual_selection;

pub(crate) use opening::OpenedStore;
pub(crate) use start_model::StartModel;

use std::collections::{BTreeMap, BTreeSet};
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
    AwProjection, BackendChoice, Deconvolver, Gridder, ImagingRequest, MeasurementSetSelection,
    NativeAwCachePolicy, RunContext, SaveModel, SpecMode,
};
use direction::Centre;
use domains::PreparedImageDomain;
pub use error::{OutlierProblem, PrepareError};
pub(crate) use partitions::{PartitionSelectionError, resolve_partitions};
use selection::WindowChannels;
use specification::SpecificationInputs;
use spectral::{FrameContext, PreparedSpectralAxis};
use survey::{Survey, SurveyPlan};

/// A request compiled against its MeasurementSets, before the problem
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
    /// The PSF, sums of weights and residual an earlier run left, which the
    /// run reads in place of forming them (`calcpsf`, `calcres`).
    pub(crate) existing: Option<crate::restart::ExistingProducts>,
    /// A direction-dependent gridder's residual units, which bind every
    /// solver.
    pub(crate) minor_cycle_image_response: Option<MinorCycleImageResponse>,
    /// The selected observation's manifest, which the problem compiles.
    pub(crate) snapshot: ObservationSnapshotInput,
    /// The observation, each of its stores opened once for compile and the
    /// run, in the manifest's order.
    pub(crate) observation: OpenedObservation,
    pub(crate) write_model_column: bool,
    pub(crate) write_corrected_data: bool,
    /// What saving the run's model as CASA's virtual model in each
    /// MeasurementSet needs, when `savemodel='virtual'`; empty otherwise.
    pub(crate) virtual_model: Vec<crate::virtual_model::VirtualModelPlan>,
    /// What the native run deploys once the availability check passes.
    pub(crate) deployment: Deployment,
    /// The charge of compile's survey of the observation, held until the
    /// problem compiles.
    pub(crate) survey: casa_imaging_runtime::Reservation,
}

/// The observation as [`prepare`] surveyed it: every store, and the
/// reference store's ([`stores::reference`]) catalogue, geometry engine and
/// selection survey; and the spectral axis.
struct Surveyed<'a> {
    stores: &'a [stores::SurveyedStore],
    /// The reference store's place in `stores`.
    reference: usize,
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

/// Compile `request` against its MeasurementSets.
///
/// What compile reads is admitted, not compile itself: store metadata is
/// admitted before opening, each observation opens once under its growing
/// grant ([`opening::open_observation`]), and its survey
/// ([`survey::survey`]) is admitted before reading the selected partitions.
/// The MeasurementSets are imaged in CASA's order
/// ([`stores::survey_stores`]); the image's coordinates are laid on one
/// of them ([`stores::reference`]), a continuum band spans them all.
pub(crate) fn prepare(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<Prepared, PrepareError> {
    // The one Measures provider of the run: compile's geometry and the
    // batches' both evaluate with it.
    let measures = casa_ms::open_measures_runtime()?;
    let stores = stores::survey_stores(request, context, &measures)?;
    let continuum = (request.specmode == SpecMode::Mfs)
        .then(|| stores::continuum_band(request, &stores))
        .transpose()?;
    let reference = stores::reference(request, &stores)?;
    let store = &stores[reference];
    let (catalogue, engine, survey) = (store.catalogue(), &store.engine, &store.survey);
    let centre = direction::resolve_centre(request, catalogue, survey, engine)?;
    if centre.ephemeris.is_some() {
        stores::one_store(&stores, "a moving phase centre")?;
    }
    let frame = FrameContext {
        anchor_time_mjd_seconds: survey.first_time_mjd_seconds,
        field_id: centre.field_id,
        phase: centre.phase.clone(),
        direction: centre.direction,
        engine,
    };
    let moving_rest_frame = moving_rest_frame(request, &centre, &frame)?;
    let spectral = spectral::prepare_axis(
        request,
        (&stores, reference),
        continuum.as_ref(),
        &frame,
        moving_rest_frame.as_ref(),
    )?;
    let surveyed_observation = Surveyed {
        stores: &stores,
        reference,
        catalogue,
        msv2: store.msv2(),
        engine,
        survey,
        spectral: &spectral,
    };
    let surveyed = &surveyed_observation;
    let (continuum_transform, channels) = continuum_selection(request, surveyed, &centre)?;
    let observation_info = direction::observation_info(catalogue, survey, &centre, engine)?;
    let mut domains = domains::prepare_domains(
        request,
        &centre,
        (&spectral.image, spectral.output_channels),
        &observation_info,
    )?;
    let primary_beam = primary_beam_model(request, catalogue)?;
    let primary_beam_centres = match primary_beam {
        None | Some(AnalyticPrimaryBeamModel::MosaicSensitivity) => None,
        Some(_) => Some(primary_beam_centres(&domains, &stores, &centre)?),
    };
    let deployment = deployment(
        request,
        context,
        surveyed,
        &domains,
        primary_beam,
        primary_beam_centres,
    )?;
    let instrument = instrument::scientific_instrument_model(
        &request.gridder,
        &stores
            .iter()
            .map(stores::SurveyedStore::catalogue)
            .collect::<Vec<_>>(),
    )?;
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
    let continuation =
        continuation::prepare(request, &mut domains[0], spectral.output_channels, context)?;
    let start_model = match request.startmodel.as_deref() {
        Some(path) => Some(StartModel::prepare(
            path,
            &domains[0],
            request.stokes.len(),
            reconstruction_planes,
        )?),
        None => continuation.start_model,
    };
    let geometry = geometry(request, &domains, &centre, &spectral);
    // A moving phase centre names a body and a table alike; the table, when
    // it is one, was read here.
    let phase_table = centre
        .ephemeris
        .as_ref()
        .and_then(PhaseCentreEphemeris::external_series)
        .cloned();
    let virtual_model = match request.savemodel {
        SaveModel::Virtual => {
            virtual_model_plans(request, &stores, &domains[0].coordinates, &measures)?
        }
        SaveModel::None | SaveModel::ModelColumn => Vec::new(),
    };
    let (masks, mut mask_resident) =
        domains::mask_plans(&domains, spectral.output_channels, context)?;
    mask_resident.join(continuation.mask_resident);
    let (snapshot, stores, survey_charge) =
        snapshot_input(stores, &channels, &spectral.frequency_bands)?;
    Ok(Prepared {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(model_samples, model_samples, f64::MAX, f64::MAX)?,
            NumericPrecision::F64,
        ),
        masks,
        mask_resident,
        start_model,
        existing: continuation.existing,
        minor_cycle_image_response: match &request.gridder {
            Gridder::Mosaic { .. } | Gridder::Awproject(_) => Some(MinorCycleImageResponse::new(
                specification::normalization(request),
                specification::primary_beam_validity(request)?,
            )?),
            Gridder::Standard { .. } | Gridder::Wproject { .. } => None,
        },
        snapshot,
        observation: OpenedObservation {
            stores,
            measures,
            phase_table,
        },
        write_model_column: request.savemodel == SaveModel::ModelColumn,
        write_corrected_data: request.save_continuum_residual,
        virtual_model,
        deployment,
        survey: survey_charge,
    })
}

/// The virtual model of `savemodel='virtual'` of each store: CASA saves
/// the model in each MeasurementSet of its list, each a MeasurementSet v2.
fn virtual_model_plans(
    request: &ImagingRequest,
    stores: &[stores::SurveyedStore],
    coordinates: &casa_coordinates::CoordinateSystem,
    measures: &Arc<dyn casa_types::measures::MeasuresProvider>,
) -> Result<Vec<crate::virtual_model::VirtualModelPlan>, PrepareError> {
    stores
        .iter()
        .map(|store| {
            if store.msv2().is_none() {
                return Err(PrepareError::MeasurementSet(
                    casa_ms::MsError::InvalidInput(
                        "savemodel='virtual' saves the model in a MeasurementSet v2, as CASA does"
                            .to_string(),
                    ),
                ));
            }
            let selection = store.survey.virtual_model.clone().ok_or_else(|| {
                PrepareError::MeasurementSet(casa_ms::MsError::InvalidInput(
                    "savemodel='virtual': the survey resolved no selection".to_string(),
                ))
            })?;
            Ok(crate::virtual_model::VirtualModelPlan::new(
                (request, &store.selection),
                selection,
                coordinates.clone(),
                Arc::clone(measures),
            ))
        })
        .collect()
}

/// Survey the partitions of `store` that `selection`, one MeasurementSet of
/// `request`, selects ([`survey::survey`]): the selected windows' (or MSv2
/// data description's) partitions holding a selected field and, under an
/// intent selection, a selected observing mode; their UVW when a
/// UV-distance interval or a W-projecting gridder needs it, and native AW's
/// first cross-correlation sample.
fn survey_observation(
    request: &ImagingRequest,
    selection: &MeasurementSetSelection,
    store: &OpenedStore,
    context: &RunContext,
) -> Result<(Admitted<Survey>, Columns), PrepareError> {
    let catalogue = store.catalogue();
    let selection::SpwChoice {
        windows,
        channels: spw_channels,
        ranges: spw_ranges,
    } = selection::window_choice(request, selection, store)?;
    let (row_selection, states) = selection::row_selection(selection, catalogue)?;
    let times =
        time_selection::resolve_times(selection, store, &windows, (&row_selection, &states))?;
    let row_selection = row_selection.with_times(times);
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
    let virtual_model = match (request.savemodel, store.msv2()) {
        (SaveModel::Virtual, Some(observation)) => Some(virtual_selection::resolve(
            selection,
            (store, observation),
            (&windows, &spw_channels),
            (&plan.row_selection, &states),
        )?),
        _ => None,
    };
    let mut surveyed = survey::survey(plan, spectral_windows, (&context.host, &context.policy))?;
    surveyed.value.spw_channels = spw_channels;
    surveyed.value.spw_ranges = spw_ranges;
    surveyed.value.virtual_model = virtual_model;
    Ok((surveyed, columns))
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

/// Where an analytic primary beam is centred on each image domain: every
/// store's selected fields ([`direction::primary_beam_centres`]), as CASA's
/// `makePrimaryBeam` adds a beam for each field of each MeasurementSet.
fn primary_beam_centres(
    domains: &[PreparedImageDomain],
    stores: &[stores::SurveyedStore],
    centre: &Centre,
) -> Result<PrimaryBeamCentres, PrepareError> {
    let mut centres = PrimaryBeamCentres::new();
    for store in stores {
        let store_centres = direction::primary_beam_centres(
            domains,
            &store.opened.value,
            &store.survey,
            centre,
            &store.engine,
        )?;
        for (role, pixels) in store_centres {
            centres.entry(role).or_default().extend(pixels);
        }
    }
    Ok(centres)
}

/// The continuum transform of a `fitspw` request and the source channels
/// each store reads: the fit channels with the output channels, or the
/// output channels alone.
fn continuum_selection(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    centre: &Centre,
) -> Result<(Option<SequentialContinuumTransform>, Vec<WindowChannels>), PrepareError> {
    let Some(fitspw) = &request.fitspw else {
        return Ok((None, surveyed.spectral.selected_source_channels.clone()));
    };
    stores::one_store(surveyed.stores, "continuum subtraction (fitspw)")?;
    let window = selection::one_window(&surveyed.survey.spectral_windows, "continuum subtraction")?;
    let (transform, selected) = specification::continuum_transform(
        fitspw,
        request.fitorder,
        i32::try_from(centre.field_id).expect("the centre's field id is a stored i32 FIELD_ID"),
        window,
        surveyed
            .spectral
            .window_channels(surveyed.reference, window.spw_id),
    )?;
    Ok((
        Some(transform),
        vec![BTreeMap::from([(window.spw_id, selected)])],
    ))
}

/// The specification's inputs beyond the request: the instrument, the
/// W and AW projections over the observed W range of every store, the
/// cube's density padding and the continuum transform.
fn specification_inputs(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    instrument: Option<casa_imaging_model::InstrumentModel>,
    primary_beam: bool,
    continuum_transform: Option<SequentialContinuumTransform>,
) -> Result<SpecificationInputs, PrepareError> {
    let windows = surveyed
        .stores
        .iter()
        .flat_map(|store| &store.survey.spectral_windows)
        .cloned()
        .collect::<Vec<_>>();
    let w = surveyed
        .stores
        .iter()
        .map(|store| store.survey.w)
        .reduce(survey::WRange::union)
        .expect("a request images at least one MeasurementSet");
    let density_fields = match request.gridder {
        Gridder::Mosaic {
            mosweight: true, ..
        } => specification::density_fields(&density_field_keys(surveyed.stores)),
        _ => None,
    };
    Ok(SpecificationInputs {
        instrument,
        primary_beam,
        w_projection: match &request.gridder {
            Gridder::Wproject { wprojplanes } => {
                Some(specification::w_projection(*wprojplanes, &windows, w)?)
            }
            _ => None,
        },
        aw_projection: match &request.gridder {
            Gridder::Awproject(aw) => Some(specification::aw_projection(aw, &windows, w)?),
            _ => None,
        },
        mosaic_conjugate_beams: matches!(
            request.gridder,
            Gridder::Mosaic {
                conjbeams: true,
                ..
            }
        ) && surveyed.spectral.output_channels == 1,
        density_fields,
        cube_density_padding: cube_density_padding(request, surveyed, density_fields.is_some())?,
        continuum_transform,
    })
}

/// Every store's selected fields with data under their keys
/// ([`StoreKeys`](crate::imaging::StoreKeys)): `mosweight` grids a density
/// for each MeasurementSet's field apart (CASA's `msId_field`).
fn density_field_keys(stores: &[stores::SurveyedStore]) -> BTreeSet<i32> {
    let keys = crate::imaging::StoreKeys::new(
        stores.iter().map(stores::SurveyedStore::catalogue),
        Vec::new(),
    );
    stores
        .iter()
        .enumerate()
        .flat_map(|(index, store)| {
            let keys = &keys;
            store.survey.fields.iter().map(move |field| {
                let field = u32::try_from(*field).expect("a selected FIELD_ID is not negative");
                i32::try_from(keys.field(index, field)).expect("field keys fit a FIELD_ID")
            })
        })
        .collect()
}

/// The selected observation's manifest, one source per store, each
/// reading its `channels` (in store order): its survey's samples, the
/// channels read with each selected window's whole frequency axis, the
/// visibility column, and the weights the selected partitions hold (each
/// partition's by CASA's rule, [`partitions::stored_weights`]) and the
/// per-time frequency selection `bands` holds for its windows
/// ([`frequency_bands`]); with the stores the run reads, in the manifest's
/// order, and the charge of their surveys.
fn snapshot_input(
    stores: Vec<stores::SurveyedStore>,
    channels: &[WindowChannels],
    bands: &[spectral::WindowBands],
) -> Result<
    (
        ObservationSnapshotInput,
        Vec<Admitted<OpenedStore>>,
        casa_imaging_runtime::Reservation,
    ),
    PrepareError,
> {
    let mut sources = Vec::with_capacity(stores.len());
    let mut opened = Vec::with_capacity(stores.len());
    let mut charge = casa_imaging_runtime::Reservation::none();
    for ((store, channels), bands) in stores.into_iter().zip(channels).zip(bands) {
        let visibility = store.columns.visibility;
        let weights = partitions::stored_weights(
            &store.opened.value,
            store
                .survey
                .partitions
                .iter()
                .map(PartitionSelection::partition),
            visibility,
        );
        // Each MeasurementSet's first buffers are its own.
        let lsrk_factors =
            envelope::first_buffer_lsrk_factors((store.catalogue(), &store.engine), &store.survey)?;
        let selection =
            selection::observation_selection(store.survey, channels, &lsrk_factors, bands)?;
        sources.push(ObservationSourceInput::new(
            ObservationSourceProvenance::new(store.selection.vis.display().to_string()),
            selection,
            SelectedColumns::new(visibility, FlagPolicy::FlagOrFlagRow, weights),
            store.columns.corrected_data_present,
        ));
        opened.push(store.opened);
        charge.join(store.survey_charge);
    }
    Ok((ObservationSnapshotInput::new(sources), opened, charge))
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

/// The density planes on each side of a cube's axis under `mosweight`:
/// `BriggsCubeWeightor::initImgWeightCol` keeps its `allSwingPad` of 4
/// with `multiField`, since each field's density is its own, and
/// `fillImgWeightCol` adds half of it at each end (`refpix[3] += swingpad
/// / 2`). It estimates no swing then (`estimateSwingChanPad`) unless
/// several MeasurementSets are weighed apart, which one store excludes.
const PER_FIELD_CUBE_DENSITY_PADDING: usize = 2;

/// CASA's padded per-channel density grid of a cube with per-channel
/// density, in planes on each side: under `mosweight` (`per_field`) a
/// fixed [`PER_FIELD_CUBE_DENSITY_PADDING`], else the swing the data's
/// frames give ([`envelope::cube_density_padding`]).
fn cube_density_padding(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    per_field: bool,
) -> Result<Option<usize>, PrepareError> {
    let (survey, spectral) = (surveyed.survey, surveyed.spectral);
    if !specification::cube_density_padded(request) {
        return Ok(None);
    }
    stores::one_store(surveyed.stores, "a cube's per-channel density weighting")?;
    if per_field {
        return Ok(Some(PER_FIELD_CUBE_DENSITY_PADDING));
    }
    Ok(Some(envelope::cube_density_padding(
        (surveyed.catalogue, surveyed.engine),
        survey,
        &envelope::OutputAxis {
            frequency_reference: spectral.output_frequency_reference,
            centres_hz: [
                spectral.reference_frequency_hz,
                spectral.reference_frequency_hz
                    + (spectral.output_channels - 1) as f64 * spectral.increment_hz,
            ],
            channels: spectral.output_channels,
            increment_hz: spectral.increment_hz,
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
    /// cell's sky increment against the image.
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
            conjugate_beams: aw.conjbeams,
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
