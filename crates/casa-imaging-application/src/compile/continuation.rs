// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a run continuing on an earlier run's products reads of them,
//! checked when the request compiles (#944, #980): every product on the
//! requested grid, the model as the start model, a CLEAN mask to reuse,
//! and the PSF, sums of weights and residual a run reads rather than
//! forms ([`crate::restart`]). Compile reads headers and the mask; the
//! model and the normal-state images are read when the run uses them.

use std::path::PathBuf;

use casa_images::RegridTemplate;
use casa_imaging_runtime::{Demand, Reservation, admit};

use super::domains::{DomainMask, PreparedImageDomain};
use super::{PrepareError, StartModel};
use crate::restart::{self, ExistingProducts, Family};
use crate::{ImagingRequest, RunContext, UseMask};

/// What a run reads of the products at its `imagename`.
pub(crate) struct Continuation {
    /// The earlier model, on the image's own grid.
    pub(crate) start_model: Option<StartModel>,
    /// The charge of a reused CLEAN mask's support, held through publication.
    pub(crate) mask_resident: Reservation,
    /// The PSF, sums of weights and residual read in place of a pass.
    pub(crate) existing: Option<ExistingProducts>,
}

/// Check every product at the main domain's prefix on its grid, and take
/// what the run continues on: the model, the CLEAN mask (into `main`'s
/// mask) when the run cleans with no new mask, and the PSF and residual
/// that `calcpsf=False` and `calcres=False` read. `channels` are the
/// image's output channels. A request that does not continue
/// (`tclean_restart` unset, lower-level execution) reads nothing.
pub(super) fn prepare(
    request: &ImagingRequest,
    main: &mut PreparedImageDomain,
    channels: usize,
    context: &RunContext,
) -> Result<Continuation, PrepareError> {
    let mut continuation = Continuation {
        start_model: None,
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
    for path in restart::products(&request.imagename)? {
        let suffix = restart::suffix(&request.imagename, &path);
        restart::check_grid(&path, &suffix, &template)?;
        // CASA's setupMask reuses an existing mask only when no mask is
        // given; `restart::check` refused the other cleaning cases.
        if suffix == "mask"
            && request.niter > 0
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
    if models.iter().any(Option::is_some) {
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
    if !request.calcpsf || residual {
        continuation.existing = Some(ExistingProducts::new(
            &request.imagename,
            family,
            ([nx, ny], polarizations, channels),
            (!request.calcpsf, residual),
        )?);
    }
    Ok(continuation)
}
