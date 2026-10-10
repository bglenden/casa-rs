// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile an [`ImagingRequest`] against its MeasurementSet into the
//! problem specification, geometry and model lifecycle that
//! `casa_imaging_model::compile` turns into a `CompiledProblem`, with the
//! observation request, the masks and the native run's deployment.
//!
//! Compiling only reads: the deployment's file-system effects (output
//! directories, the AW cache) wait for [`Deployment::deploy`], after the
//! availability check.

mod direction;
mod domains;
mod error;
mod instrument;
mod native_aw;
mod outliers;
mod selection;
mod specification;
mod spectral;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_imaging_model::{
    CentreLaws, GeometryInput, ModelBounds, ModelLifecycleRequirements, NativeAwRequestInput,
    NumericPrecision, ProblemSpecification, SequentialContinuumTransform,
    UncorrectedImageMaskPolicy, UvwCoordinateLaw, WeightColumn as OwnerWeightColumn,
};
use casa_imaging_operator::{AwCatalog, AwIndexing};
use casa_imaging_products::{AnalyticPrimaryBeamModel, ContinuumProductControls};
use casa_imaging_reconstruction::{ImageDomainReconstructionMaskPlans, MinorCycleImageResponse};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{
    MeasurementSet, SelectedObservationContentBudget, SelectedObservationEphemeris,
    SelectedObservationResolutionRequest, SelectedObservationSpectralWindow,
};
use casa_types::measures::{MeasuresProvider, frame::MeasFrame};

use crate::{
    ApplicationNative, ApplicationPublication, ApplicationRuntime, AwCatalogDeployment, AwCfSource,
    AwProjection, CasaImageProductSink, Deconvolver, Gridder, ImagingRequest, NativeAwCachePolicy,
    RunContext, SpecMode,
};
use direction::{Centre, ImageSpectralCoordinate};
use domains::PreparedImageDomain;
pub use error::{OutlierProblem, PrepareError};
use selection::{Survey, WindowChannels};
use specification::SpecificationInputs;
use spectral::{FrameContext, PreparedSpectralAxis};

/// A request compiled against its MeasurementSet, before the problem
/// compiler binds the observation.
pub(crate) struct Prepared {
    pub(crate) specification: ProblemSpecification,
    pub(crate) geometry: GeometryInput,
    pub(crate) model_lifecycle: ModelLifecycleRequirements,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    /// A direction-dependent gridder's residual units, which bind every
    /// solver.
    pub(crate) minor_cycle_image_response: Option<MinorCycleImageResponse>,
    pub(crate) observation: SelectedObservationResolutionRequest,
    pub(crate) write_model_column: bool,
    pub(crate) write_corrected_data: bool,
    /// What the native run deploys once the availability check passes.
    pub(crate) deployment: Deployment,
}

/// The MeasurementSet as [`prepare`] surveyed it: the table, its geometry
/// engine, the source budget, the selection survey and the spectral axis.
struct Surveyed<'a> {
    ms: &'a MeasurementSet,
    engine: &'a MsCalEngine,
    budget: SelectedObservationContentBudget,
    survey: &'a Survey,
    spectral: &'a PreparedSpectralAxis,
}

/// Compile `request` against its MeasurementSet.
pub(crate) fn prepare(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<Prepared, PrepareError> {
    let ms = MeasurementSet::open(&request.vis)?;
    let budget = casa_imaging_runtime::bootstrap_source_budget();
    // The one Measures provider of the run: compile's geometry and the bound
    // observation's both evaluate with it.
    let measures = casa_ms::open_measures_runtime()?;
    let engine = MsCalEngine::with_measures(&ms, Arc::clone(&measures))?;
    let survey = selection::survey(request, &ms, budget, &engine)?;
    let centre = direction::resolve_centre(request, &ms, &survey, &engine, budget)?;
    let frame = FrameContext {
        anchor_time_mjd_seconds: survey.first_time_mjd_seconds,
        time_bounds_mjd_seconds: survey.time_bounds_mjd_seconds,
        field_id: centre.field_id,
        phase: centre.phase.clone(),
        direction: centre.direction,
        engine: &engine,
    };
    let moving_rest_frame = moving_rest_frame(request, &centre, &frame)?;
    let spectral =
        spectral::prepare_axis(request, &ms, &survey, &frame, moving_rest_frame.as_ref())?;
    let surveyed = Surveyed {
        ms: &ms,
        engine: &engine,
        budget,
        survey: &survey,
        spectral: &spectral,
    };
    let (continuum_transform, channels) = continuum_selection(request, &surveyed, &centre)?;
    let observation_info = direction::observation_info(&ms, &survey, &centre, &engine)?;
    let domains = domains::prepare_domains(
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
    let primary_beam = primary_beam_model(request, &ms)?;
    let deployment = deployment(request, context, &surveyed, &domains, primary_beam)?;
    let instrument = instrument::scientific_instrument_model(&request.gridder, &ms)?;
    let specification = specification::specification(
        request,
        &spectral,
        specification_inputs(
            request,
            &surveyed,
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
    let geometry = geometry(request, &domains, &centre, &spectral);
    let observation = observation_request(
        request,
        &ms,
        survey,
        &channels,
        measures,
        centre.ephemeris,
        budget,
    )?;
    Ok(Prepared {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(model_samples, model_samples, f64::MAX, f64::MAX)?,
            NumericPrecision::F64,
        ),
        masks: domains::mask_plans(&domains)?,
        minor_cycle_image_response: match &request.gridder {
            Gridder::Mosaic { .. } | Gridder::Awproject(_) => Some(MinorCycleImageResponse::new(
                specification::normalization(request),
                specification::primary_beam_validity(request)?,
            )?),
            Gridder::Standard | Gridder::Wproject { .. } => None,
        },
        observation,
        write_model_column: request.savemodel,
        write_corrected_data: request.save_continuum_residual,
        deployment,
    })
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
        uncorrected_mask: if primary_beam && request.pblimit >= 0.0 {
            UncorrectedImageMaskPolicy::PrimaryBeam
        } else {
            UncorrectedImageMaskPolicy::None
        },
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

/// The selected-observation request: the survey's rows and the channels
/// read, the visibility and weight columns, and the Measures provider and
/// centre ephemeris compile evaluated with.
fn observation_request(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    survey: Survey,
    channels: &WindowChannels,
    measures: Arc<dyn MeasuresProvider>,
    ephemeris: Option<SelectedObservationEphemeris>,
    budget: SelectedObservationContentBudget,
) -> Result<SelectedObservationResolutionRequest, PrepareError> {
    let weight_column = if survey.weight_spectrum_complete {
        OwnerWeightColumn::WeightSpectrum
    } else {
        OwnerWeightColumn::Weight
    };
    Ok(SelectedObservationResolutionRequest::new(
        request.vis.display().to_string(),
        selection::observation_selection(ms, survey, channels)?,
        selection::visibility_column(ms, request.datacolumn)?,
        weight_column,
        budget,
        measures,
    )
    .with_ephemeris(ephemeris))
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
    ms: &MeasurementSet,
) -> Result<Option<AnalyticPrimaryBeamModel>, PrepareError> {
    Ok(match request.gridder {
        Gridder::Mosaic { .. } | Gridder::Awproject(_) => {
            Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
        }
        Gridder::Standard | Gridder::Wproject { .. } if request.write_pb || request.pbcor => {
            Some(instrument::standard_primary_beam_model(ms)?)
        }
        Gridder::Standard | Gridder::Wproject { .. } => None,
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
    Ok(Some(
        surveyed.ms.selected_observation_cube_density_padding(
            &survey.row_selection,
            SelectedObservationSpectralWindow::borrow_selected(
                u32::try_from(window.spw_id).expect("SPW ids are nonnegative stored i32 values"),
                window.frequency_reference,
                &window.frequencies_hz,
                &window.channel_widths_hz,
                spectral.window_channels(window.spw_id),
            ),
            survey.fields.iter().copied(),
            spectral.output_frequency_reference,
            [
                spectral.reference_frequency_hz,
                spectral.reference_frequency_hz
                    + (spectral.output_channels - 1) as f64 * spectral.increment_hz,
            ],
            spectral.output_channels,
            surveyed.engine,
            surveyed.budget.row_io_budget(),
        )?,
    ))
}

/// The native run's runtime, product publication and AW catalog, still to
/// be deployed: no directory exists and no AW cell is generated until
/// [`Deployment::deploy`].
pub(crate) struct Deployment {
    /// The runtime; its spill directory is made canonical on deployment.
    runtime: ApplicationRuntime,
    publication: ApplicationPublication,
    /// Every domain's output directory.
    output_directories: Vec<PathBuf>,
    aw: Option<AwPlan>,
}

/// The AW catalog a run opens and, for a native cache, the cells to bring
/// to the request's policy.
struct AwPlan {
    root: PathBuf,
    native: Option<(NativeAwCachePolicy, NativeAwRequestInput)>,
    indexing: AwIndexing,
    resident_bytes: usize,
}

impl Deployment {
    /// Create every domain's output directory, settle the spill directory
    /// beside the main image and bring a native AW cache to the request's
    /// policy.
    pub(crate) fn deploy(self) -> Result<ApplicationNative, PrepareError> {
        for directory in &self.output_directories {
            std::fs::create_dir_all(directory).map_err(|source| PrepareError::OutputDirectory {
                path: directory.clone(),
                source,
            })?;
        }
        let mut runtime = self.runtime;
        runtime.spill_directory = runtime.spill_directory.canonicalize().map_err(|source| {
            PrepareError::OutputDirectory {
                path: runtime.spill_directory.clone(),
                source,
            }
        })?;
        Ok(ApplicationNative {
            runtime,
            publication: self.publication,
            aw_catalog: self.aw.map(AwPlan::deploy).transpose()?,
        })
    }
}

impl AwPlan {
    /// Under every policy the request's cells are compared with the
    /// directory: reuse needs every one, generation fills in the absent
    /// ones, regeneration clears the cache first; the loader then checks
    /// each cell's sky increment against the image (plan section 5.6).
    fn deploy(self) -> Result<AwCatalogDeployment, PrepareError> {
        if let Some((policy, input)) = &self.native {
            let (present, expected) = AwCatalog::native_cells_present(&self.root, input);
            match policy {
                NativeAwCachePolicy::ReuseOnly if present != expected => {
                    return Err(PrepareError::AwCacheIncomplete { present, expected });
                }
                NativeAwCachePolicy::ReuseOnly => {}
                NativeAwCachePolicy::GenerateMissing => {
                    if present != expected {
                        AwCatalog::generate_native(&self.root, input, true)?;
                    }
                }
                NativeAwCachePolicy::Regenerate => {
                    AwCatalog::clear_native(&self.root)?;
                    AwCatalog::generate_native(&self.root, input, false)?;
                }
            }
        }
        Ok(AwCatalogDeployment {
            root: self.root,
            indexing: self.indexing,
            resident_bytes: self.resident_bytes,
        })
    }
}

/// The native run's deployment: the runtime with the paged cube state
/// beside the main image, the product controls and sink, and the AW plan.
fn deployment(
    request: &ImagingRequest,
    context: &RunContext,
    surveyed: &Surveyed<'_>,
    domains: &[PreparedImageDomain],
    primary_beam: Option<AnalyticPrimaryBeamModel>,
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
        runtime: ApplicationRuntime {
            host: context.host,
            resource_policy: context.policy,
            backend: request.backend,
            grid_precision: request.gridprecision,
            cancel: context.cancel.clone(),
            spill_directory: parent(&request.imagename),
            summary: context.summary.clone(),
        },
        publication: ApplicationPublication {
            controls,
            sink: CasaImageProductSink::for_domains(
                domains.iter().map(PreparedImageDomain::output),
            ),
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
            let first_row = surveyed
                .survey
                .first_cross_row
                .ok_or(PrepareError::NoCrossCorrelationRow)?;
            let input = native_aw::resolve(request, aw, surveyed, first_row)?;
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
        resident_bytes: aw.cf_resident_mb.checked_mul(1 << 20).ok_or(
            PrepareError::CfResidentSize {
                megabytes: aw.cf_resident_mb,
            },
        )?,
    })
}
