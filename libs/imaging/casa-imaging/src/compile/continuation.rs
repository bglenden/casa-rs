// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a run continuing on an earlier run's products reads of them,
//! checked when the request compiles (#944, #980): every product on the
//! requested grid, the model as the start model, a CLEAN mask to reuse,
//! and the PSF, sums of weights and residual a run reads rather than
//! forms ([`crate::restart`]). Compile reads headers and the mask; the
//! model and the normal-state images are read when the run uses them.

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_imageanalysis::RegridTemplate;
use casa_imaging_products::CasaImageWriter;
use casa_imaging_runtime::{Demand, Reservation, admit};

use super::domains::{DomainMask, PreparedImageDomain};
use super::{PrepareError, StartModel, Surveyed, psf_view};
use crate::request::execution::{OUTLIER_START_MODELS_SCOPE, supports_outlier_start_models};
use crate::restart::{self, ExistingProducts, Family};
use crate::{ImagingRequest, RunContext, UseMask};

/// What a run reads of the products at its `imagename`.
pub(crate) struct Continuation {
    /// The earlier model, on the image's own grid.
    pub(crate) start_model: Option<StartModel>,
    /// Saved or explicit outlier models in compiled domain order after the main image.
    pub(crate) outlier_models: Vec<Option<StartModel>>,
    pub(crate) outlier_psfs: Vec<Option<ExistingProducts>>,
    /// Existing mosaic PSF coordinates when CASA grids into that image.
    pub(crate) psf_coordinates: Option<CoordinateSystem>,
    /// Charges for reused mask and image metadata, held through publication.
    pub(crate) mask_resident: Reservation,
    /// Earlier products or their metadata, including images read in place of a pass.
    pub(crate) existing: Option<ExistingProducts>,
}

/// Bind the coordinates of a re-formed PSF and the model for the first pass.
pub(super) fn prepare(
    request: &ImagingRequest,
    domains: &mut [PreparedImageDomain],
    (channels, reconstruction_planes): (usize, usize),
    mvc_start: Option<&super::MvcStart>,
    (context, surveyed): (&RunContext, &Surveyed<'_>),
    writer: &mut CasaImageWriter,
) -> Result<Continuation, PrepareError> {
    let (main, outliers) = domains.split_first_mut().expect("main image domain");
    let field_seed = if outliers.is_empty() {
        None
    } else {
        outliers
            .iter()
            .find_map(|domain| domain.outlier_start_model.as_ref())
            .or_else(|| request.startmodel.iter().flatten().next())
    };
    if let Some(path) = field_seed {
        if request.tclean_restart {
            return Err(PrepareError::Restart {
                path: path.clone(),
                reason: "supplying a new outlier start model with saved products is not implemented (#944)"
                    .into(),
            });
        }
        if outliers.len() != 1 || !supports_outlier_start_models(request) {
            return Err(PrepareError::StartModel {
                path: path.clone(),
                reason: OUTLIER_START_MODELS_SCOPE.into(),
            });
        }
    }
    let mut continuation = read(request, main, channels, mvc_start, context, surveyed)?;
    if request.tclean_restart && !outliers.is_empty() {
        for outlier in outliers {
            let mut saved = read_domain(request, outlier, channels, None, context, None)?;
            continuation.mask_resident.join(saved.mask_resident);
            continuation.outlier_models.push(saved.start_model.take());
            continuation.outlier_psfs.push(saved.existing.take());
        }
    } else if field_seed.is_some() {
        continuation.outlier_models = outliers
            .iter()
            .map(|domain| {
                domain
                    .outlier_start_model
                    .as_ref()
                    .map(|_| {
                        StartModel::prepare(
                            std::slice::from_ref(&domain.outlier_start_model),
                            domain,
                            request.stokes.len(),
                            (reconstruction_planes, 1),
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    if let Some(coordinates) = continuation.psf_coordinates.take() {
        writer.set_main_psf_coordinates(coordinates);
    }
    if request.has_start_model() {
        let terms = match request.deconvolver {
            crate::Deconvolver::Mtmfs => request.nterms,
            _ => 1,
        };
        continuation.start_model = Some(StartModel::prepare(
            &request.startmodel,
            main,
            request.stokes.len(),
            (reconstruction_planes, terms),
        )?);
    }
    Ok(continuation)
}

/// Check every product at the main domain's prefix on its grid, and take
/// what the run continues on: the model, the CLEAN mask (into `main`'s
/// mask) when the run cleans with no new mask, and the PSF and residual
/// that `calcpsf=False` and `calcres=False` read. `channels` are the
/// image's output channels. A request that does not continue
/// (`tclean_restart` unset, lower-level execution) reads nothing.
fn read(
    request: &ImagingRequest,
    main: &mut PreparedImageDomain,
    channels: usize,
    mvc_start: Option<&super::MvcStart>,
    context: &RunContext,
    surveyed: &Surveyed<'_>,
) -> Result<Continuation, PrepareError> {
    if !request.tclean_restart {
        restart::check_inputs(request, false)?;
        return Ok(Continuation {
            start_model: None,
            outlier_models: Vec::new(),
            outlier_psfs: Vec::new(),
            psf_coordinates: None,
            mask_resident: Reservation::none(),
            existing: None,
        });
    }
    read_domain(request, main, channels, mvc_start, context, Some(surveyed))
}

fn check_restart_grid(
    path: &Path,
    suffix: &str,
    (template, taylor_psf): (&RegridTemplate, &mut Option<RegridTemplate>),
    request: &ImagingRequest,
    domain: &PreparedImageDomain,
    context: &RunContext,
    surveyed: Option<&Surveyed<'_>>,
) -> Result<Option<CoordinateSystem>, PrepareError> {
    if suffix.starts_with("psf.tt") && request.supports_separate_psf() {
        if let Some(grid) = taylor_psf {
            return restart::check_grid(path, suffix, grid, None);
        }
        let coordinates = restart::check_grid(path, suffix, template, Some(domain.direction))?;
        *taylor_psf = Some(RegridTemplate {
            coordinates: coordinates
                .as_ref()
                .unwrap_or(&template.coordinates)
                .clone(),
            shape: template.shape.clone(),
        });
        return Ok(coordinates);
    }
    if suffix == "psf" && request.supports_separate_psf() {
        if request.specmode == crate::SpecMode::Mfs {
            return restart::check_grid(path, suffix, template, Some(domain.direction));
        }
        if let Some(surveyed) = surveyed {
            let saved_cube_grid =
                psf_view::saved_cube_grid(path, request, domain, surveyed, context)?;
            return restart::check_grid(
                path,
                suffix,
                saved_cube_grid.as_ref().unwrap_or(template),
                None,
            );
        }
    }
    restart::check_grid(path, suffix, template, None)
}

fn read_domain(
    request: &ImagingRequest,
    domain: &mut PreparedImageDomain,
    channels: usize,
    mvc_start: Option<&super::MvcStart>,
    context: &RunContext,
    surveyed: Option<&Surveyed<'_>>,
) -> Result<Continuation, PrepareError> {
    let mut continuation = Continuation {
        start_model: None,
        outlier_models: Vec::new(),
        outlier_psfs: Vec::new(),
        psf_coordinates: None,
        mask_resident: Reservation::none(),
        existing: None,
    };
    let family = Family::of(request);
    let rebuilds_mosaic_psf = restart::rebuilds_mosaic_cube_psf(request);
    let [nx, ny] = domain.image_size;
    let polarizations = request.stokes.len();
    let planes = match family {
        Family::Single => channels,
        Family::Taylor { .. } => 1,
    };
    let template = RegridTemplate {
        coordinates: domain.coordinates.clone(),
        shape: vec![nx, ny, polarizations, planes],
    };
    let mut taylor_psf = None;
    for path in restart::continuation_products(request, &domain.output)? {
        let suffix = restart::suffix(&domain.output, &path);
        if mvc_start.is_some() && suffix != "mask" && !restart::mvc_cube_product(&suffix) {
            continue;
        }
        let psf_coordinates = if mvc_start.is_some() && suffix == "mask" {
            // The Taylor view checked the mask's one-channel grid.
            None
        } else {
            check_restart_grid(
                &path,
                &suffix,
                (&template, &mut taylor_psf),
                request,
                domain,
                context,
                surveyed,
            )?
        };
        if request.calcpsf
            && request.specmode == crate::SpecMode::Mfs
            && !request.uses_separate_psf()
            && let Some(coordinates) = psf_coordinates
        {
            // MFS forms new samples in the existing PSF image. A cube
            // forms a new PSF in the requested geometry and publishes
            // that geometry with it.
            let bytes = coordinates
                .to_record()
                .retained_heap_bytes()
                .and_then(|bytes| u64::try_from(bytes).ok())
                .unwrap_or(u64::MAX);
            continuation.mask_resident.join(admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "saved mosaic PSF coordinates",
                    memory: bytes,
                },
            )?);
            continuation.psf_coordinates = Some(coordinates);
        }
        // CASA fills a new user mask into its existing image store. Keep
        // this request scoped to reuse until that combination is covered.
        if suffix == "mask"
            && request.niter > 0
            && domain.role != casa_imaging_model::ImageDomainRole::Main
            && domain.mask != DomainMask::FullPlane
        {
            return Err(PrepareError::Restart {
                path,
                reason:
                    "merging a new mask into an existing CLEAN mask is not implemented yet (#944)"
                        .into(),
            });
        }
        let use_existing_mask =
            request.usemask == UseMask::User && domain.mask == DomainMask::FullPlane;
        let seed_automatic = request.usemask == UseMask::AutoMultithresh
            && restart::supports_saved_automask(request);
        if suffix == "mask"
            && (request.niter > 0 || mvc_start.is_some())
            && (use_existing_mask || seed_automatic)
        {
            let input = restart::MaskInput::open(&path)?;
            let mut charge = admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "restart CLEAN mask",
                    memory: input.reading_bytes(),
                },
            )?;
            let (planes, support) = input.read()?;
            charge.retain(restart::MaskInput::retained_bytes(planes, nx * ny));
            if let DomainMask::AutoMultithresh { previous, .. } = &mut domain.mask {
                *previous = Some((planes, support.into()));
            } else {
                domain.mask = DomainMask::PixelSupport {
                    planes,
                    support: support.into(),
                };
            }
            continuation.mask_resident.join(charge);
        }
    }
    let models = family
        .names("model")
        .into_iter()
        .map(|name| {
            let path = restart::product_path(&domain.output, &name);
            path.exists().then_some(path)
        })
        .collect::<Box<[Option<PathBuf>]>>();
    if !mvc_start.is_some_and(|start| start.projection_beams.is_some())
        && models.iter().any(Option::is_some)
    {
        continuation.start_model = Some(StartModel::restart(
            models,
            RegridTemplate {
                shape: vec![
                    nx,
                    ny,
                    polarizations,
                    match family {
                        Family::Single => channels,
                        Family::Taylor { terms } => terms,
                    },
                ],
                ..template
            },
        ));
    }
    let is_main = domain.role == casa_imaging_model::ImageDomainRole::Main;
    let residual = if is_main {
        restart::reads_residual(request)
    } else {
        !request.calcres
    };
    // CASA retains an existing residual's beam even when it forms new pixels.
    let residual_name = family
        .names("residual")
        .into_iter()
        .next()
        .expect("a residual name");
    let psf_name = family.names("psf").into_iter().next().expect("a PSF name");
    // CubeMajorCycleAlgorithm forms a temporary PSF without the old image
    // metadata, whereas MFS makes its PSF in the existing image store.
    let psf_beam = (!request.calcpsf || request.specmode == crate::SpecMode::Mfs)
        && !(request.calcpsf && request.uses_separate_psf())
        && restart::product_path(&domain.output, &psf_name).exists();
    let mask_beam = is_main
        && request.usemask == UseMask::AutoMultithresh
        && request.niter > 0
        && restart::product_path(&request.imagename, &residual_name).exists();
    if !request.calcpsf || residual || mask_beam || psf_beam {
        let weight = is_main && restart::reuses_direction_dependent_psf(request);
        let mut existing = ExistingProducts::new(
            &domain.output,
            family,
            ([nx, ny], polarizations, channels),
            (!request.calcpsf, residual),
            weight,
            weight || rebuilds_mosaic_psf || (residual && request.outlierfile.is_some()),
            request.specmode != crate::SpecMode::Mfs && matches!(family, Family::Single),
        )?;
        if request.niter > 0
            && let Some(path) = existing.masked_taylor_residual()
        {
            return Err(PrepareError::Restart {
                path: path.to_path_buf(),
                reason:
                    "AW CLEAN from a masked saved Taylor residual is not implemented yet (#944)"
                        .into(),
            });
        }
        if psf_beam {
            let mut charge = admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "restart PSF beam metadata",
                    memory: ExistingProducts::beam_reading_bytes(planes, polarizations),
                },
            )?;
            existing.read_psf_beams(channels, domain.direction.increment_rad()[0].abs())?;
            charge.retain(existing.beam_resident_bytes());
            continuation.mask_resident.join(charge);
        }
        if mask_beam {
            let mut charge = admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "restart residual beam metadata",
                    memory: ExistingProducts::beam_reading_bytes(planes, polarizations),
                },
            )?;
            let before = existing.beam_resident_bytes();
            existing.read_mask_beams(channels, domain.direction.increment_rad()[0].abs())?;
            charge.retain(existing.beam_resident_bytes() - before);
            continuation.mask_resident.join(charge);
        }
        continuation.existing = Some(existing);
    }
    Ok(continuation)
}
