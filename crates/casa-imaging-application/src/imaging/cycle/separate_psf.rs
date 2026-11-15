// SPDX-License-Identifier: LGPL-3.0-or-later

//! Grid only the separate mosaic PSF family with the ordinary measurement operator.

use std::{cell::OnceCell, mem};

use casa_imaging_deconvolution::{
    FinalNormalState, ImageDomainReconstructionMaskPlans, NormalBasis, PassImages,
    ReconstructionMaskPlan,
};
use casa_imaging_model::CompiledProblem;
use casa_imaging_runtime::pass::PassError;
use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};

use super::{BatchRoute, Forms, ImagingError, ImagingInputs, Run};
use crate::ApplicationRuntime;

pub(crate) enum SeparatePsf {
    Continuum(Admitted<Vec<f32>>),
    Cube(Admitted<FinalNormalState>),
}

impl SeparatePsf {
    /// Replace only the PSF of the current image window. The other normal
    /// fields must still come from the image centre's gridding pass.
    fn apply(&mut self, images: &mut PassImages) -> Result<(), ImagingError> {
        match self {
            Self::Continuum(psf) => images.psf = Some(mem::take(&mut psf.value)),
            Self::Cube(psf) => {
                let cells = images.shape[0] * images.shape[1];
                let target = images.psf.as_mut().expect("the initial pass forms a PSF");
                for (index, output) in target.chunks_exact_mut(cells).enumerate() {
                    let channel = images.channels.start + index / images.polarizations;
                    let pol = index % images.polarizations;
                    let plane = psf.value.read_plane(images.domain, channel, pol)?;
                    let pixels = plane
                        .read_psf_real()?
                        .expect("paged cube holds Float planes");
                    output.copy_from_slice(&pixels);
                }
            }
        }
        Ok(())
    }

    /// Keep the backing for subsequent windows. The last window holds it
    /// until the main normal state has copied the replacement PSF.
    pub(super) fn for_window(
        source: &mut Option<Self>,
        initial: bool,
        images: &mut PassImages,
        output_channels: usize,
    ) -> Result<Option<Self>, PassError> {
        let mut separate = initial.then(|| source.take()).flatten();
        let Some(psf) = &mut separate else {
            return Ok(None);
        };
        psf.apply(images)
            .map_err(|error| PassError::Images(Box::new(error)))?;
        if images.channels.end < output_channels {
            *source = separate.take();
        }
        Ok(separate)
    }
}

pub(crate) fn form(
    problem: &CompiledProblem,
    source: &BatchRoute,
    runtime: &ApplicationRuntime,
) -> Result<SeparatePsf, ImagingError> {
    let domain = &problem.geometry().domains()[0];
    let teams = OnceCell::new();
    let mut run = Run::open(
        ImagingInputs {
            problem,
            mvc: None,
            psf_cutoff: casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF,
            psf_beams: None,
            outlier_psf_beams: Vec::new(),
            mask_beams: None,
            source,
            separate_psf: None,
            masks: ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
                coordinate: domain.direction(),
            }])?,
            start_model: None,
            outlier_models: Vec::new(),
            outlier_products: Vec::new(),
            existing: None,
            forms: Forms {
                psf: true,
                residual: false,
            },
            image_response: None,
            primary_beam: None,
            visibility_write: None,
            products: (0, 0),
            host: runtime.host,
            policy: runtime.resource_policy,
            teams: &teams,
            cancel: runtime.cancel.clone(),
            spill_directory: &runtime.spill_directory,
            aw_catalog: None,
            awp2: None,
            backend: runtime.backend,
            grid_precision: runtime.grid_precision,
            ft_padding: runtime.ft_padding,
            diagnostics: runtime.diagnostics,
        },
        (&|_| 0, None),
        &mut None,
        true,
    )?;
    let major = run.psf_pass()?;
    let (normal, model) = major.completion.into_parts();
    drop(model);
    if normal.basis() == NormalBasis::ChannelLocal {
        let resident = normal.resident_bytes()?;
        run.retained.retain(resident);
        return Ok(SeparatePsf::Cube(Admitted {
            value: normal,
            reservation: run.retained,
        }));
    }
    let window = normal.read_window(0..1)?;
    let samples = window
        .normal_approximation()
        .complex()
        .ok_or(casa_imaging_deconvolution::NormalStateError::ProblemMismatch)?;
    // Continuum keeps one PSF, or every signed Taylor normal moment in
    // term-major and polarization-minor order.
    let mut psf = admit_samples(samples.len(), (&runtime.host, &runtime.resource_policy))?;
    psf.value
        .extend(samples.iter().map(|value| value.re as f32));
    Ok(SeparatePsf::Continuum(psf))
}

fn admit_samples(
    samples: usize,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<Vec<f32>>, ImagingError> {
    let bytes = (samples as u64).saturating_mul(size_of::<f32>() as u64);
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "separate mosaic PSF samples",
            memory: bytes,
        },
    )?;
    Ok(Admitted {
        value: Vec::with_capacity(samples),
        reservation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_retained_psf_reserves_every_float_sample() {
        let host = HostResources::detect().unwrap();
        let psf = admit_samples(2048, (&host, &ResourcePolicy::Exclusive)).unwrap();
        assert_eq!(psf.value.capacity(), 2048);
        assert_eq!(
            psf.reservation.memory(),
            (psf.value.capacity() * size_of::<f32>()) as u64
        );
    }
}
