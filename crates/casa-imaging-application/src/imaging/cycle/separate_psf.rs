// SPDX-License-Identifier: LGPL-3.0-or-later

//! Grid only the separate mosaic PSF with the ordinary measurement operator.

use std::cell::OnceCell;

use casa_imaging_deconvolution::{ImageDomainReconstructionMaskPlans, ReconstructionMaskPlan};
use casa_imaging_model::CompiledProblem;
use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};

use super::{BatchRoute, Forms, ImagingError, ImagingInputs, Run};
use crate::ApplicationRuntime;

pub(crate) fn form(
    problem: &CompiledProblem,
    source: &BatchRoute,
    runtime: &ApplicationRuntime,
) -> Result<Admitted<Vec<f32>>, ImagingError> {
    let domain = &problem.geometry().domains()[0];
    let teams = OnceCell::new();
    let mut run = Run::open(
        ImagingInputs {
            problem,
            mvc: None,
            psf_cutoff: casa_imaging_deconvolution::DEFAULT_PSF_FIT_CUTOFF,
            psf_beams: None,
            mask_beams: None,
            source,
            separate_psf: None,
            masks: ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
                coordinate: domain.direction(),
            }])?,
            start_model: None,
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
    let normal = major.completion.normal_state();
    let polarizations = problem.reconstruction().polarization().coordinates().len();
    let [nx, ny] = domain.shape().pixels();
    let samples = nx * ny * polarizations;
    let mut psf = admit_samples(samples, (&runtime.host, &runtime.resource_policy))?;
    for pol in 0..polarizations {
        let plane = normal.read_plane(0, 0, pol)?;
        psf.value
            .extend(plane.read_psf()?.iter().map(|value| value.re as f32));
    }
    Ok(psf)
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
