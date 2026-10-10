// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile an [`ImagingRequest`] against its MeasurementSet into the
//! problem specification, geometry and model lifecycle that
//! `casa_imaging_model::compile` turns into a `CompiledProblem`, with the
//! observation request, the masks and the native run's deployment.

mod direction;
mod domains;
mod instrument;
mod native_aw;
mod outliers;
mod selection;
mod specification;
mod spectral;

use std::collections::BTreeMap;
use std::path::Path;

use casa_imaging_model::{
    CentreLaws, DelayCentreLaw, GeometryInput, ModelBounds, ModelInputCommitment,
    ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision, ProblemSpecification,
    ReferenceDataKind, UncorrectedImageMaskPolicy, UvwCoordinateLaw,
    WeightColumn as OwnerWeightColumn,
};
use casa_imaging_products::{AnalyticPrimaryBeamModel, ContinuumProductControls};
use casa_imaging_reconstruction::{ImageDomainReconstructionMaskPlans, MinorCycleImageResponse};
use casa_ms::{
    MeasurementSet, SelectedObservationContentBudget, SelectedObservationResolutionRequest,
    SelectedObservationSpectralWindow,
};

use crate::{
    ApplicationError, ApplicationNative, ApplicationPublication, ApplicationRuntime,
    AwCatalogDeployment, AwCfSource, AwProjection, CasaImageProductSink, Deconvolver, Gridder,
    ImagingRequest, NativeAwCachePolicy, RunContext, SpecMode,
};
use direction::{Centre, ImageSpectralCoordinate};
use domains::PreparedImageDomain;
use selection::Survey;
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
    /// The native run's deployment; its failure is reported after the
    /// availability check, since there is no other implementation.
    pub(crate) native: Result<ApplicationNative, ApplicationError>,
}

/// Compile `request` against its MeasurementSet.
pub(crate) fn prepare(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<Prepared, ApplicationError> {
    let ms = MeasurementSet::open(&request.vis)?;
    let budget = casa_imaging_runtime::bootstrap_source_budget();
    let engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
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
    let moving_rest_frame = if request.specmode == SpecMode::Cubesource {
        let ephemeris = centre
            .ephemeris
            .as_ref()
            .ok_or_else(|| boxed("source-frame cube imaging requires a moving phase centre"))?;
        let velocity = engine.ephemeris_radial_velocity(
            frame.anchor_time_mjd_seconds,
            frame.field_id,
            ephemeris,
        )?;
        Some(
            engine
                .spectral_frame_observatory_direction(
                    frame.anchor_time_mjd_seconds,
                    centre.phase.clone(),
                )?
                .with_radial_velocity(velocity),
        )
    } else {
        None
    };
    let spectral =
        spectral::prepare_axis(request, &ms, &survey, &frame, moving_rest_frame.as_ref())?;
    let (continuum_transform, channels) = match &request.fitspw {
        Some(fitspw) => {
            let window = selection::one_window(&survey.spectral_windows, "continuum subtraction")?;
            let (transform, selected) = specification::continuum_transform(
                fitspw,
                request.fitorder,
                i32::try_from(centre.field_id).map_err(|_| boxed("FIELD_ID exceeds i32"))?,
                window,
                &spectral.selected_source_channels[&window.spw_id],
            )?;
            (Some(transform), BTreeMap::from([(window.spw_id, selected)]))
        }
        None => (None, spectral.selected_source_channels.clone()),
    };
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
    let native = native_deployment(
        request,
        context,
        &ms,
        &survey,
        &spectral,
        &domains,
        primary_beam,
        &engine,
    );
    let instrument = instrument::scientific_instrument_model(&request.gridder, &ms)?;
    let specification = specification::specification(
        request,
        &spectral,
        SpecificationInputs {
            instrument: instrument.map(|(model, _)| model),
            uncorrected_mask: if primary_beam.is_some() && request.pblimit >= 0.0 {
                UncorrectedImageMaskPolicy::PrimaryBeam
            } else {
                UncorrectedImageMaskPolicy::None
            },
            w_projection: match &request.gridder {
                Gridder::Wproject { wprojplanes } => Some(specification::w_projection(
                    *wprojplanes,
                    &survey.spectral_windows,
                    survey.w,
                )?),
                _ => None,
            },
            aw_projection: match &request.gridder {
                Gridder::Awproject(aw) => Some(specification::aw_projection(
                    aw,
                    &survey.spectral_windows,
                    survey.w,
                )?),
                _ => None,
            },
            cube_density_padding: cube_density_padding(
                request, &ms, &survey, &spectral, &engine, budget,
            )?,
            continuum_transform,
        },
    )?;
    let reconstruction_planes = match request.deconvolver {
        Deconvolver::Mtmfs => request.nterms,
        _ => spectral.output_channels,
    };
    let model_samples = domains::model_samples(&domains, reconstruction_planes, &request.stokes)?;
    let masks = domains::mask_plans(&domains)?;
    let weight_column = if survey.weight_spectrum_complete {
        OwnerWeightColumn::WeightSpectrum
    } else {
        OwnerWeightColumn::Weight
    };
    let visibility_column = selection::visibility_column(&ms, request.datacolumn)?;
    let geometry = geometry(request, &domains, &centre, &spectral);
    let observation = SelectedObservationResolutionRequest::new(
        request.vis.display().to_string(),
        selection_request_identity(),
        selection::observation_selection(&ms, survey, &channels)?,
        visibility_column,
        weight_column,
        instrument
            .map(|(_, reference)| vec![(ReferenceDataKind::Instrument, reference)])
            .unwrap_or_default(),
        ModelStateIdentity::Empty,
        budget,
        centre.measures,
    )
    .with_ephemeris(centre.ephemeris);
    Ok(Prepared {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(
                model_samples,
                spectral.output_channels,
                spectral.output_channels,
                model_samples,
                f64::MAX,
                f64::MAX,
            )?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
        masks,
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
        native,
    })
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
            DelayCentreLaw::PhaseTrackingCentre,
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
) -> Result<Option<AnalyticPrimaryBeamModel>, ApplicationError> {
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
    ms: &MeasurementSet,
    survey: &Survey,
    spectral: &PreparedSpectralAxis,
    engine: &casa_ms::derived::engine::MsCalEngine,
    budget: SelectedObservationContentBudget,
) -> Result<Option<usize>, ApplicationError> {
    if !specification::cube_density_padded(request, spectral) {
        return Ok(None);
    }
    let window = selection::one_window(&survey.spectral_windows, "cube density")?;
    Ok(Some(ms.selected_observation_cube_density_padding(
        &survey.row_selection,
        SelectedObservationSpectralWindow::borrow_selected(
            u32::try_from(window.spw_id).map_err(|_| boxed("SPW id exceeds u32"))?,
            window.frequency_reference,
            &window.frequencies_hz,
            &window.channel_widths_hz,
            &spectral.selected_source_channels[&window.spw_id],
        ),
        survey.fields.iter().copied(),
        spectral.output_frequency_reference,
        [
            spectral.reference_frequency_hz,
            spectral.reference_frequency_hz
                + (spectral.output_channels - 1) as f64 * spectral.increment_hz,
        ],
        spectral.output_channels,
        engine,
        budget.row_io_budget(),
    )?))
}

/// The native run's runtime, product publication and AW catalog. Every
/// domain's output directory is created; the paged cube state lives
/// beside the main image.
#[allow(clippy::too_many_arguments)]
fn native_deployment(
    request: &ImagingRequest,
    context: &RunContext,
    ms: &MeasurementSet,
    survey: &Survey,
    spectral: &PreparedSpectralAxis,
    domains: &[PreparedImageDomain],
    primary_beam: Option<AnalyticPrimaryBeamModel>,
    engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<ApplicationNative, ApplicationError> {
    for domain in domains {
        std::fs::create_dir_all(domain.output.parent().unwrap_or_else(|| Path::new(".")))?;
    }
    let runtime = ApplicationRuntime {
        host: context.host,
        resource_policy: context.policy,
        backend: request.backend,
        grid_precision: request.gridprecision,
        cancel: context.cancel.clone(),
        spill_directory: request
            .imagename
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()?,
        summary: context.summary.clone(),
    };
    let mut controls = ContinuumProductControls::new(request.psfcutoff as f32)?;
    if let Some(model) = primary_beam {
        controls = controls.with_primary_beam_model(model);
    }
    let aw_catalog = match &request.gridder {
        Gridder::Awproject(aw) => Some(aw_catalog(request, aw, ms, survey, spectral, engine)?),
        _ => None,
    };
    Ok(ApplicationNative {
        runtime,
        publication: ApplicationPublication {
            controls,
            sink: CasaImageProductSink::for_domains(
                domains.iter().map(PreparedImageDomain::output),
            )?,
        },
        aw_catalog,
    })
}

/// The AW catalog: a CASA cache read as it is, or a native cache in
/// CASA's format brought to the request's policy. Under every policy the
/// request's cells are compared with the directory: reuse needs every one,
/// generation fills in the absent ones, regeneration clears the cache
/// first; the loader then checks each cell's sky increment against the
/// image (plan section 5.6).
fn aw_catalog(
    request: &ImagingRequest,
    aw: &AwProjection,
    ms: &MeasurementSet,
    survey: &Survey,
    spectral: &PreparedSpectralAxis,
    engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<AwCatalogDeployment, ApplicationError> {
    let root = match &aw.cf_source {
        AwCfSource::CasaImport { cfcache } => cfcache.clone(),
        AwCfSource::NativeEvla {
            native_cf_cache,
            native_cf_policy,
            ..
        } => {
            let first_row = survey
                .first_cross_row
                .ok_or_else(|| boxed("native AW has no unflagged cross-correlation row"))?;
            let input = native_aw::resolve(request, aw, ms, survey, spectral, first_row, engine)?;
            input.validate()?;
            let (present, expected) =
                casa_imaging_operator::AwCatalog::native_cells_present(native_cf_cache, &input);
            match native_cf_policy {
                NativeAwCachePolicy::ReuseOnly if present != expected => {
                    return Err(boxed(format!(
                        "native AW cache reuse found {present} of the {expected} cells the \
                         request names"
                    )));
                }
                NativeAwCachePolicy::ReuseOnly => {}
                NativeAwCachePolicy::GenerateMissing => {
                    if present != expected {
                        casa_imaging_operator::AwCatalog::generate_native(
                            native_cf_cache,
                            &input,
                            true,
                        )?;
                    }
                }
                NativeAwCachePolicy::Regenerate => {
                    casa_imaging_operator::AwCatalog::clear_native(native_cf_cache)?;
                    casa_imaging_operator::AwCatalog::generate_native(
                        native_cf_cache,
                        &input,
                        false,
                    )?;
                }
            }
            native_cf_cache.clone()
        }
    };
    Ok(AwCatalogDeployment {
        root,
        indexing: casa_imaging_operator::AwIndexing {
            conjugate_beams: true,
            image_reference_hz: spectral.reference_frequency_hz,
        },
        resident_bytes: aw
            .cf_resident_mb
            .checked_mul(1 << 20)
            .ok_or_else(|| boxed("cf_resident_mb exceeds addressable memory"))?,
    })
}

/// The identity of one prepared selection request: unique in the process,
/// since a request is identified by who owns it, not by its content.
fn selection_request_identity() -> casa_imaging_model::LogicalIdentity {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let mut identity = [0_u8; 32];
    identity[0] = 2;
    identity[24..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    casa_imaging_model::LogicalIdentity::from_bytes(identity)
}

fn boxed(message: impl Into<String>) -> ApplicationError {
    Box::new(std::io::Error::other(message.into()))
}
