// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a run continuing on an earlier run's products reads of them,
//! checked when the request compiles (#944, #980): every product on the
//! requested grid, the model as the start model, a CLEAN mask to reuse,
//! and the PSF, sums of weights and residual a run reads rather than
//! forms ([`crate::restart`]). Compile reads headers and the mask; the
//! model and the normal-state images are read when the run uses them.

use std::path::PathBuf;

use casa_coordinates::CoordinateSystem;
use casa_images::RegridTemplate;
use casa_imaging_products::CasaImageWriter;
use casa_imaging_runtime::{Demand, Reservation, admit};

use super::domains::{DomainMask, PreparedImageDomain};
use super::{PrepareError, StartModel};
use crate::restart::{self, ExistingProducts, Family};
use crate::{ImagingRequest, RunContext, UseMask};

/// What a run reads of the products at its `imagename`.
pub(crate) struct Continuation {
    /// The earlier model, on the image's own grid.
    pub(crate) start_model: Option<StartModel>,
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
    main: &mut PreparedImageDomain,
    (channels, reconstruction_planes): (usize, usize),
    mvc_start: Option<&super::MvcStart>,
    context: &RunContext,
    writer: &mut CasaImageWriter,
) -> Result<Continuation, PrepareError> {
    let mut continuation = read(request, main, channels, mvc_start, context)?;
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
) -> Result<Continuation, PrepareError> {
    let mut continuation = Continuation {
        start_model: None,
        psf_coordinates: None,
        mask_resident: Reservation::none(),
        existing: None,
    };
    if !request.tclean_restart {
        restart::check_inputs(request, false)?;
        return Ok(continuation);
    }
    let family = Family::of(request);
    let [nx, ny] = main.image_size;
    let polarizations = request.stokes.len();
    let planes = match family {
        Family::Single => channels,
        Family::Taylor { .. } => 1,
    };
    let template = RegridTemplate {
        coordinates: main.coordinates.clone(),
        shape: vec![nx, ny, polarizations, planes],
    };
    let saved_mosaic_psf = request.supports_separate_psf().then_some(main.direction);
    for path in restart::products(&request.imagename)? {
        let suffix = restart::suffix(&request.imagename, &path);
        if mvc_start.is_some() && suffix != "mask" && !restart::mvc_cube_product(&suffix) {
            continue;
        }
        let psf_coordinates = if mvc_start.is_some() && suffix == "mask" {
            // The Taylor view checked the mask's one-channel grid.
            None
        } else {
            restart::check_grid(&path, &suffix, &template, saved_mosaic_psf)?
        };
        if request.calcpsf
            && !request.uses_separate_psf()
            && let Some(coordinates) = psf_coordinates
        {
            // Without a separate PSF request CASA makes new samples in
            // the existing image store, keeping its coordinates and beam.
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
        // CASA's setupMask reuses an existing mask only when no mask is
        // given; `restart::check` refused the other cleaning cases.
        if suffix == "mask"
            && (request.niter > 0 || mvc_start.is_some())
            && request.usemask == UseMask::User
            && main.mask == DomainMask::FullPlane
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
            main.mask = DomainMask::PixelSupport {
                planes,
                support: support.into(),
            };
            continuation.mask_resident.join(charge);
        }
    }
    let models = family
        .names("model")
        .into_iter()
        .map(|name| {
            let path = restart::product_path(&request.imagename, &name);
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
    let residual = restart::reads_residual(request);
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
        && restart::product_path(&request.imagename, &psf_name).exists();
    let mask_beam = request.usemask == UseMask::AutoMultithresh
        && request.niter > 0
        && restart::product_path(&request.imagename, &residual_name).exists();
    if !request.calcpsf || residual || mask_beam || psf_beam {
        let mut existing = ExistingProducts::new(
            &request.imagename,
            family,
            ([nx, ny], polarizations, channels),
            (!request.calcpsf, residual),
            restart::reuses_direction_dependent_psf(request),
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
            existing.read_psf_beams(channels, main.direction.increment_rad()[0].abs())?;
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
            existing.read_mask_beams(channels, main.direction.increment_rad()[0].abs())?;
            charge.retain(existing.beam_resident_bytes() - before);
            continuation.mask_resident.join(charge);
        }
        continuation.existing = Some(existing);
    }
    Ok(continuation)
}
