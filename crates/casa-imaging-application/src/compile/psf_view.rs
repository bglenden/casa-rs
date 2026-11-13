// SPDX-License-Identifier: LGPL-3.0-or-later

//! The single-term mosaic's separately centred PSF (CASA task_tclean.py).

use casa_imaging_model::{
    CentreLaws, CompiledProblem, GeometryInput, ModelLifecycleRequirements, PhaseCentreLaw,
    ProblemInput, ProblemSpecification, UvwCoordinateLaw,
};
use casa_imaging_products::CasaImageWriter;
use casa_imaging_runtime::{Admitted, Demand, Reservation, admit};

use super::direction::{self, Centre};
use super::domains::PreparedImageDomain;
use super::spectral::PreparedSpectralAxis;
use super::{PrepareError, instrument};
use crate::{Gridder, ImagingRequest, RunContext};

pub(crate) struct PsfView {
    specification: ProblemSpecification,
    geometry: GeometryInput,
    reservation: Reservation,
}

impl PsfView {
    pub(crate) fn compile(
        self,
        main: &CompiledProblem,
        lifecycle: ModelLifecycleRequirements,
    ) -> Result<Admitted<CompiledProblem>, casa_imaging_model::CompileProblemError> {
        Ok(Admitted {
            value: casa_imaging_model::compile(ProblemInput::new(
                self.specification,
                self.geometry,
                main.observation().clone(),
                lifecycle,
            ))?,
            reservation: self.reservation,
        })
    }
}

/// CASA forms the alternate PSF as a new mosaic view of the same selection.
/// The alternate phase law participates in row packing and convolution, not
/// just the image header. The other products retain the main view.
pub(super) fn prepare(
    request: &ImagingRequest,
    main: &PreparedImageDomain,
    centre: &Centre,
    spectral: &PreparedSpectralAxis<'_>,
    context: &RunContext,
    specification: &ProblemSpecification,
    writer: &mut CasaImageWriter,
) -> Result<Option<PsfView>, PrepareError> {
    let Some(text) = request.psfphasecenter.as_deref() else {
        return Ok(None);
    };
    if !request.calcpsf || !matches!(request.gridder, Gridder::Mosaic { .. }) {
        return Ok(None);
    }
    if centre.ephemeris.is_some() {
        return Err(PrepareError::MovingMosaicPsf);
    }
    let phase = direction::parse_phase_center_direction(text)?;
    let (longitude, latitude) = (phase.longitude_rad(), phase.latitude_rad());
    direction::refuse_ncp_at_equator(request.projection, latitude)?;
    let direction = direction::direction_spec(
        main.image_size,
        request.cell,
        request.projection,
        phase.frame(),
        [longitude, latitude],
    );
    // Only a single domain and one spectral plane are admitted. Cover both
    // compiled and uncompiled metadata during conversion, including the
    // owned coordinate and scale arrays. The observation manifest is shared.
    let variable = (request.restfreq.len() as u64 + request.scales.len() as u64) * 32
        + (main.coordinates.obs_info().telescope.len() as u64
            + main.coordinates.obs_info().observer.len() as u64)
            * 2;
    let reservation = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "separate mosaic PSF view",
            memory: 64 * 1024 + variable,
        },
    )?;
    let coordinates = direction::image_coordinates(
        direction,
        main.image_size,
        &request.stokes,
        &spectral.image,
        main.coordinates.obs_info(),
    );
    let geometry = GeometryInput::new(
        vec![main.spec().with_direction(direction)],
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase),
            instrument::pointing_centre_law(&request.gridder),
        ),
        UvwCoordinateLaw::MosaicPhaseTrackingCentre,
        spectral.coordinate(),
    );
    writer.set_main_psf_coordinates(coordinates);
    Ok(Some(PsfView {
        specification: specification.clone(),
        geometry,
        reservation,
    }))
}
