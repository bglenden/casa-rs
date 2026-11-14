// SPDX-License-Identifier: LGPL-3.0-or-later

//! The cube major-cycle request of CASA's MT-MFS via cube (#1069).

use casa_ms::CubeInterpolation;

use crate::{Deconvolver, ImagingRequest, SpecMode};

/// Construct the cube over the complete selected band CASA's
/// `PyMtmfsViaCubeSynthesisImager::determineFreqRange` surveyed. Its output
/// depth is independent of the input selection, and its nearest-neighbor
/// cube forms the dirty normals that the Taylor imager subsequently uses.
pub(crate) fn cube_request(
    request: &ImagingRequest,
    [lower, upper]: [f64; 2],
    context: &crate::RunContext,
) -> Result<casa_imaging_runtime::Admitted<ImagingRequest>, super::PrepareError> {
    let channels = resolve_cube_depth(request.mvc_output_channels, [lower, upper], request.nterms)?;
    let reservation = casa_imaging_runtime::admit(
        &context.host,
        &context.policy,
        &casa_imaging_runtime::Demand {
            phase: "MVC cube request",
            memory: super::mvc_request::clone_demand(request)?,
        },
    )?;
    let mut cube = request.clone();
    cube.mvc_output_channels = Some(channels);
    cube.mvc_auto_channels = false;
    let channels = channels.get();
    cube.specmode = SpecMode::Cube;
    cube.deconvolver = Deconvolver::Hogbom;
    cube.nterms = 1;
    cube.niter = 0;
    cube.start = Some(format!("{lower}Hz"));
    cube.width = Some(format!("{}Hz", (upper - lower) / channels as f64));
    cube.interpolation = CubeInterpolation::Nearest;
    // CASA's MVC cube uses usecubebriggs=False and reuses the continuum density.
    cube.perchanweightdensity = false;
    cube.restoration = false;
    cube.startmodel.clear();
    cube.continued_model = false;
    cube.write_pb = true;
    Ok(casa_imaging_runtime::Admitted {
        value: cube,
        reservation,
    })
}

/// CASA chooses roughly ten planes per octave, but at least `nterms + 1`
/// when its bandwidth-derived count is below five. A supplied count is used
/// as-is; every count must cover the requested Taylor terms.
fn resolve_cube_depth(
    requested: Option<std::num::NonZeroUsize>,
    [lower, upper]: [f64; 2],
    terms: usize,
) -> Result<std::num::NonZeroUsize, super::PrepareError> {
    let invalid = |reason| super::PrepareError::MvcCubeDepth { reason };
    if !lower.is_finite() || !upper.is_finite() || lower <= 0.0 || upper < lower {
        return Err(invalid("the selected frequency band is invalid"));
    }
    let depth = if let Some(count) = requested {
        count.get()
    } else {
        let estimate = ((upper - lower) / (0.1 * lower)).floor();
        if !estimate.is_finite() || estimate >= usize::MAX as f64 {
            return Err(invalid("the bandwidth-derived channel count is too large"));
        }
        let estimate = estimate as usize;
        if estimate < 5 {
            terms
                .checked_add(1)
                .ok_or_else(|| invalid("too many Taylor terms"))?
        } else {
            estimate
        }
    };
    if depth < terms {
        return Err(invalid("nchan must be at least nterms"));
    }
    std::num::NonZeroUsize::new(depth).ok_or_else(|| invalid("nchan must be positive"))
}

/// The Taylor problem and WCS formed from the same survey as the cube.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC conversion is being wired in #1069")
)]
pub(crate) struct TaylorView {
    pub(crate) specification: casa_imaging_model::ProblemSpecification,
    pub(crate) geometry: casa_imaging_model::GeometryInput,
    pub(crate) model_lifecycle: casa_imaging_model::ModelLifecycleRequirements,
    pub(crate) coordinates: Vec<(
        casa_imaging_model::ImageDomainRole,
        casa_coordinates::CoordinateSystem,
    )>,
    pub(crate) expansion_reference_hz: f64,
    pub(crate) start: Option<MvcStart>,
    /// Held for the lifetime of this additional view and its compiled form.
    pub(crate) reservation: casa_imaging_runtime::Reservation,
}

/// A compiled Taylor problem and its independently owned image coordinates.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC execution is being wired in #1069")
)]
pub(crate) struct CompiledTaylorView {
    pub(crate) problem: casa_imaging_model::CompiledProblem,
    pub(crate) coordinates: Vec<(
        casa_imaging_model::ImageDomainRole,
        casa_coordinates::CoordinateSystem,
    )>,
    pub(crate) expansion_reference_hz: f64,
    pub(crate) start: Option<MvcStart>,
    /// The prepared and compiled view stays charged while it is resident.
    pub(crate) reservation: casa_imaging_runtime::Reservation,
}

impl TaylorView {
    /// Compile this view against the already compiled cube observation. Its
    /// selection is shared; no MeasurementSet is opened or surveyed again.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "MVC execution is being wired in #1069")
    )]
    pub(crate) fn compile_from_cube(
        self,
        cube: &casa_imaging_model::CompiledProblem,
    ) -> Result<CompiledTaylorView, casa_imaging_model::CompileProblemError> {
        let problem = casa_imaging_model::compile(casa_imaging_model::ProblemInput::new(
            self.specification,
            self.geometry,
            cube.observation().clone(),
            self.model_lifecycle,
        ))?;
        Ok(CompiledTaylorView {
            problem,
            coordinates: self.coordinates,
            expansion_reference_hz: self.expansion_reference_hz,
            start: self.start,
            reservation: self.reservation,
        })
    }
}

/// Inputs already obtained while preparing the cube's observation.
pub(super) struct TaylorInputs<'a> {
    pub(super) parent: &'a ImagingRequest,
    pub(super) context: &'a crate::RunContext,
    pub(super) band: &'a super::stores::ContinuumBand,
    pub(super) surveyed: &'a super::Surveyed<'a>,
    pub(super) frame: &'a super::spectral::FrameContext<'a>,
    pub(super) centre: &'a super::direction::Centre,
    pub(super) observation_info: &'a casa_coordinates::ObsInfo,
    pub(super) object_name: &'a str,
    pub(super) instrument: Option<casa_imaging_model::InstrumentModel>,
    pub(super) has_primary_beam: bool,
}

/// Build the Taylor image view without opening or surveying the observation
/// again. The published Taylor axis spans the cube channels. The expansion
/// uses `reffreq` or the midpoint of the surveyed band.
pub(super) fn prepare_taylor(inputs: TaylorInputs<'_>) -> Result<TaylorView, super::PrepareError> {
    use casa_imaging_model::{ModelBounds, ModelLifecycleRequirements, NumericPrecision};
    let TaylorInputs {
        parent,
        context,
        band,
        surveyed,
        frame,
        centre,
        observation_info,
        object_name,
        instrument,
        has_primary_beam,
    } = inputs;
    // This initial MVC implementation owns exactly one fixed-centre image
    // domain and one Stokes-I, two-term problem. In
    // particular, an outlier file or facets would make the small metadata
    // envelope below scale with data it does not yet account for.
    check_view_scope(parent, surveyed.stores.len(), centre.ephemeris.is_some())?;
    let reservation = casa_imaging_runtime::admit(
        &context.host,
        &context.policy,
        &casa_imaging_runtime::Demand {
            phase: "MVC Taylor view",
            memory: view_demand(parent, observation_info, object_name)?,
        },
    )?;
    let expansion_reference_hz = parent.reffreq.unwrap_or_else(|| band.gridded.midpoint_hz());
    let request = parent;
    let mut spectral = super::spectral::prepare_axis(
        request,
        (surveyed.stores, surveyed.reference),
        Some(band),
        frame,
        None,
        None,
    )?;
    // CASA's `PyMtmfsViaCubeSynthesisImager::copy_image` gives the Taylor
    // plane the cube's end-channel midpoint, full width and rest label.
    let cube = surveyed.spectral;
    let first_hz = cube.image.reference_frequency_hz;
    let last_hz = first_hz + (cube.output_channels - 1) as f64 * cube.image.increment_hz;
    spectral.image.reference_frequency_hz = (first_hz + last_hz) / 2.0;
    spectral.image.increment_hz = cube.output_channels as f64 * cube.image.increment_hz;
    spectral.image.rest_frequency_hz = cube.image.rest_frequency_hz;
    let taylor_surveyed = super::Surveyed {
        stores: surveyed.stores,
        reference: surveyed.reference,
        catalogue: surveyed.catalogue,
        msv2: surveyed.msv2,
        engine: surveyed.engine,
        survey: surveyed.survey,
        spectral: &spectral,
    };
    let domains = super::domains::prepare_domains(
        request,
        centre,
        (&spectral.image, spectral.output_channels),
        (observation_info, object_name),
        super::domains::MaskPreparation::GeometryOnly,
    )?;
    let mut specification_inputs = super::specification_inputs(
        request,
        &taylor_surveyed,
        instrument,
        has_primary_beam,
        None,
    )?;
    specification_inputs.force_primary_beam = true;
    let specification =
        super::specification::specification(request, &spectral, specification_inputs)?;
    let geometry = super::geometry(request, &domains, centre, &spectral);
    let start = prepare_start(parent, &domains[0])?;
    let samples = super::domains::model_samples(&domains, request.nterms, &request.stokes)?;
    Ok(TaylorView {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(samples, samples, f64::MAX, f64::MAX)?,
            NumericPrecision::F64,
        ),
        coordinates: domains
            .into_iter()
            .map(|domain| (domain.role, domain.coordinates))
            .collect(),
        expansion_reference_hz,
        start,
        reservation,
    })
}

/// MVC forms a fresh cube and its two-term Taylor products. Cleaning uses
/// nsigma=0 and spatial-only user boxes. A standard-gridder continuation
/// may replace both Taylor terms using the saved primary beams (#1069).
pub(super) fn check_execution_scope(
    request: &ImagingRequest,
    selected_fields: usize,
) -> Result<(), super::PrepareError> {
    use casa_imaging_model::RestoringBeamPolicy;
    use casa_types::measures::frequency::FrequencyRef;
    let field_limit = if matches!(
        request.gridder,
        crate::Gridder::Awp2 { computepastep, .. } if computepastep < 180.0
    ) {
        2
    } else {
        1
    };
    if request.vis.len() != 1
        || !(1..=field_limit).contains(&selected_fields)
        || (request.niter != 0
            && (request.nsigma != 0.0
                || request.mask_box.iter().any(|region| region.range.is_some())))
        || !request.calcpsf
        || !request.calcres
        || ((request.tclean_restart || request.has_start_model()) && !starts_from_taylor(request))
        || request.savemodel != crate::SaveModel::None
        || request.outlierfile.is_some()
        || request.fitspw.is_some()
        || request.usemask != crate::UseMask::User
        || request.mask_image.is_some()
        || request.pbmask != 0.0
        || request.outframe != FrequencyRef::LSRK
        || !matches!(
            request.restoringbeam,
            RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::None
        )
    {
        return Err(super::PrepareError::MvcExecutionUnsupported);
    }
    Ok(())
}

fn starts_from_taylor(request: &ImagingRequest) -> bool {
    request.tclean_restart
        && request.niter > 0
        && request.startmodel.len() == 2
        && request.startmodel.iter().all(Option::is_some)
        && matches!(request.gridder, crate::Gridder::Standard { facets } if facets.get() == 1)
}

/// The two model terms and the saved primary beams CASA uses before the
/// initial resumed cube prediction.
pub(crate) struct MvcStart {
    pub(crate) model: super::StartModel,
    pub(crate) cube_pb: MvcPrimaryBeam,
    pub(crate) taylor_pb: MvcPrimaryBeam,
}

/// A saved Float PB with a bounded tile cache and its read workspace.
pub(crate) struct MvcPrimaryBeam {
    path: std::path::PathBuf,
    reading_bytes: u64,
}

impl MvcPrimaryBeam {
    const CACHE_BYTES: usize = 64 * 1024;

    fn prepare(path: std::path::PathBuf) -> Result<Self, super::PrepareError> {
        let image = casa_images::PagedImage::<f32>::open_with_cache(&path, Self::CACHE_BYTES)
            .map_err(|error| super::PrepareError::Restart {
                path: path.clone(),
                reason: error.to_string(),
            })?;
        let tile_bytes = image
            .tile_shape()
            .iter()
            .fold(4_u64, |n, &axis| n.saturating_mul(axis as u64));
        let reading_bytes = (Self::CACHE_BYTES as u64).saturating_add(tile_bytes.saturating_mul(2));
        Ok(Self {
            path,
            reading_bytes,
        })
    }

    pub(crate) fn open(&self) -> Result<casa_images::PagedImage<f32>, casa_images::ImageError> {
        casa_images::PagedImage::open_with_cache(&self.path, Self::CACHE_BYTES)
    }

    pub(crate) fn reading_bytes(&self) -> u64 {
        self.reading_bytes
    }
}

fn prepare_start(
    request: &ImagingRequest,
    domain: &super::domains::PreparedImageDomain,
) -> Result<Option<MvcStart>, super::PrepareError> {
    use crate::restart::{check_grid, mvc_cube_product, product_path, products, suffix};
    if !starts_from_taylor(request) {
        return Ok(None);
    }
    let [nx, ny] = domain.image_size;
    let template = casa_images::RegridTemplate {
        coordinates: domain.coordinates.clone(),
        shape: vec![nx, ny, 1, 1],
    };
    let cube_pb = product_path(&request.imagename, "pb");
    let taylor_pb = product_path(&request.imagename, "pb.tt0");
    for path in [
        &cube_pb,
        &taylor_pb,
        &product_path(&request.imagename, "psf"),
    ] {
        if !path.exists() {
            return Err(super::PrepareError::Restart {
                path: path.clone(),
                reason: "MVC startmodel requires the saved cube PSF and both primary beams (#1069)"
                    .into(),
            });
        }
    }
    for path in products(&request.imagename)? {
        let name = suffix(&request.imagename, &path);
        if !mvc_cube_product(&name) {
            check_grid(&path, &name, &template, None)?;
        }
    }
    let model = super::StartModel::prepare(&request.startmodel, domain, 1, (2, 2))?;
    Ok(Some(MvcStart {
        model,
        cube_pb: MvcPrimaryBeam::prepare(cube_pb)?,
        taylor_pb: MvcPrimaryBeam::prepare(taylor_pb)?,
    }))
}

/// The metadata envelope below applies only to this bounded view.
fn check_view_scope(
    parent: &ImagingRequest,
    store_count: usize,
    moving_centre: bool,
) -> Result<(), super::PrepareError> {
    if store_count != 1
        || parent.outlierfile.is_some()
        || moving_centre
        || !(matches!(parent.gridder, crate::Gridder::Standard { facets } if facets.get() == 1)
            || matches!(parent.gridder, crate::Gridder::Awp2 { .. }))
        || parent.stokes != [casa_imaging_model::PolarizationCoordinate::StokesI]
        || parent.nterms != 2
        || (parent.weighting != crate::Weighting::Natural
            && !matches!(parent.gridder, crate::Gridder::Awp2 { computepastep, .. }
                if computepastep < 180.0 && parent.weighting == crate::Weighting::Uniform))
    {
        return Err(super::PrepareError::MvcViewUnsupported);
    }
    Ok(())
}

/// Bound the additional view while it is prepared and compiled. The two
/// rest-frequency arrays overlap while the WCS is built; the scales remain
/// in the reconstruction contract. Metadata names are cloned for the
/// temporary domain and coordinate system. The fixed envelope covers the
/// one-domain, one-facet, two-term, one-Stokes problem's bounded coordinate,
/// product-list, transaction and compile bookkeeping, including overlap of
/// the prepared and compiled forms. Larger modes are refused above.
fn view_demand(
    request: &ImagingRequest,
    observation: &casa_coordinates::ObsInfo,
    object_name: &str,
) -> Result<u64, super::PrepareError> {
    const FIXED_VIEW_BYTES: usize = 64 * 1024;
    let overflow = || super::PrepareError::MvcCubeDepth {
        reason: "Taylor-view memory exceeds addressable size",
    };
    let frequencies = request
        .restfreq
        .len()
        .max(1)
        .checked_mul(2 * size_of::<f64>())
        .ok_or_else(overflow)?;
    let scales = request
        .scales
        .len()
        .max(1)
        .checked_mul(size_of::<f64>())
        .ok_or_else(overflow)?;
    let telescope = observation
        .telescope
        .len()
        .checked_mul(2)
        .ok_or_else(overflow)?;
    let bytes = [
        frequencies,
        scales,
        telescope,
        observation.observer.len(),
        object_name.len(),
        request.imagename.as_os_str().len(),
    ]
    .into_iter()
    .try_fold(FIXED_VIEW_BYTES, |sum, value| {
        sum.checked_add(value).ok_or_else(overflow)
    })?;
    u64::try_from(bytes).map_err(|_| overflow())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::tests::request;
    use serde_json::json;

    #[test]
    fn native_mvc_refuses_unimplemented_execution_modes() {
        use casa_imaging_model::RestoringBeamPolicy;
        use casa_types::measures::frequency::FrequencyRef;
        let parent = request(json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "weighting": "natural",
        }));
        assert!(check_execution_scope(&parent, 1).is_ok());
        assert!(check_execution_scope(&parent, 2).is_err());
        let mut other = parent.clone();
        other.vis.push(other.vis[0].clone());
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.niter = 1;
        other.pblimit = 0.8;
        assert!(check_execution_scope(&other, 1).is_ok());
        other.pblimit = -0.1;
        assert!(check_execution_scope(&other, 1).is_ok());
        other.nsigma = 1.0;
        assert!(check_execution_scope(&other, 1).is_err());
        other.nsigma = 0.0;
        other.mask_box = vec![crate::UserMaskBox {
            corners: [1, 1, 2, 2],
            range: Some(crate::SpectralMaskRange::Pixel([0.0, 1.0])),
        }];
        assert!(check_execution_scope(&other, 1).is_err());
        other.mask_box[0].range = None;
        assert!(check_execution_scope(&other, 1).is_ok());
        let mut other = parent.clone();
        other.calcpsf = false;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.calcres = false;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.tclean_restart = true;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.startmodel = vec![Some("startmodel".into())];
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.savemodel = crate::SaveModel::ModelColumn;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.outlierfile = Some("outliers".into());
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.fitspw = Some("0".into());
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.usemask = crate::UseMask::Pb;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.mask_image = Some("mask".into());
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.pbmask = 0.1;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent.clone();
        other.outframe = FrequencyRef::BARY;
        assert!(check_execution_scope(&other, 1).is_err());
        let mut other = parent;
        other.restoringbeam = RestoringBeamPolicy::Common;
        assert!(check_execution_scope(&other, 1).is_err());
    }

    #[test]
    fn view_admission_refuses_modes_outside_its_metadata_bound() {
        use casa_imaging_model::PolarizationCoordinate;
        use std::num::NonZeroUsize;

        let parent = request(json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "weighting": "natural",
        }));
        assert!(check_view_scope(&parent, 1, false).is_ok());
        assert!(check_view_scope(&parent, 0, false).is_err());
        assert!(check_view_scope(&parent, 2, false).is_err());
        assert!(check_view_scope(&parent, 1, true).is_err());
        let mut other = parent.clone();
        other.outlierfile = Some("outliers.txt".into());
        assert!(check_view_scope(&other, 1, false).is_err());
        let mut other = parent.clone();
        other.gridder = crate::Gridder::Standard {
            facets: NonZeroUsize::new(2).unwrap(),
        };
        assert!(check_view_scope(&other, 1, false).is_err());
        let mut other = parent.clone();
        other.stokes = vec![PolarizationCoordinate::StokesQ];
        assert!(check_view_scope(&other, 1, false).is_err());
        let mut other = parent.clone();
        other.nterms = 3;
        assert!(check_view_scope(&other, 1, false).is_err());
        let mut other = parent;
        other.weighting = crate::Weighting::Uniform;
        assert!(check_view_scope(&other, 1, false).is_err());
    }

    #[test]
    fn awp2_mvc_taylor_view_keeps_the_one_domain_envelope() {
        let parent = request(json!({
            "gridder": "awp2", "specmode": "mvc", "deconvolver": "mtmfs",
            "nterms": 2, "nchan": 3,
        }));
        assert!(check_view_scope(&parent, 1, false).is_ok());
        assert!(check_view_scope(&parent, 2, false).is_err());
        assert!(check_view_scope(&parent, 1, true).is_err());
    }

    #[test]
    fn casa_cube_depth_uses_bandwidth_and_requires_enough_terms() {
        use std::num::NonZeroUsize;
        assert_eq!(
            resolve_cube_depth(None, [100.0, 110.0], 2).unwrap().get(),
            3
        );
        assert_eq!(
            resolve_cube_depth(None, [100.0, 149.0], 2).unwrap().get(),
            3
        );
        assert_eq!(
            resolve_cube_depth(None, [100.0, 150.0], 2).unwrap().get(),
            5
        );
        assert_eq!(
            resolve_cube_depth(None, [100.0, 199.0], 2).unwrap().get(),
            9
        );
        assert_eq!(
            resolve_cube_depth(None, [100.0, 110.0], 4).unwrap().get(),
            5
        );
        assert_eq!(
            resolve_cube_depth(NonZeroUsize::new(4), [100.0, 199.0], 2)
                .unwrap()
                .get(),
            4
        );
        assert_eq!(
            resolve_cube_depth(NonZeroUsize::new(2), [100.0, 110.0], 2)
                .unwrap()
                .get(),
            2
        );
        assert!(resolve_cube_depth(NonZeroUsize::new(1), [100.0, 110.0], 2).is_err());
        assert!(resolve_cube_depth(None, [0.0, 100.0], 2).is_err());
        assert!(resolve_cube_depth(None, [100.0, 99.0], 2).is_err());
        assert!(resolve_cube_depth(None, [f64::NAN, 100.0], 2).is_err());
        assert!(resolve_cube_depth(None, [1.0, f64::MAX], 2).is_err());
    }

    #[test]
    fn cube_view_is_a_valid_hogbom_request_without_changing_the_parent() {
        let mut parent = request(json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "reffreq": "45GHz", "niter": 0,
        }));
        parent.mvc_output_channels = std::num::NonZeroUsize::new(3);
        let context = crate::RunContext {
            host: casa_imaging_runtime::HostResources::detect().unwrap(),
            policy: parent.resource_policy(),
            cancel: casa_imaging_runtime::pass::Cancel::default(),
            summary: None,
        };
        let cube = cube_request(&parent, [43.936e9, 44.704e9], &context).unwrap();
        cube.value
            .validate()
            .expect("the cube view is internally consistent");
        assert_eq!(parent.deconvolver, Deconvolver::Mtmfs);
        assert_eq!(parent.nterms, 2);
    }
}
