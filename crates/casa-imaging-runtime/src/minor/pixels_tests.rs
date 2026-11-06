// SPDX-License-Identifier: LGPL-3.0-or-later
//! The analytic beam fallback probe is shared among polarizations.

#[path = "../../../casa-imaging-deconvolution/tests/support/problems.rs"]
mod problems;
#[path = "../../../casa-imaging-deconvolution/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use super::*;
use crate::minor::{MinorCycleSetup, plane_keys};
use casa_imaging_deconvolution::{MajorCycle, NoiseEstimate};
use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
use casa_imaging_model::{
    HogbomIterationAccounting, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_products::{AnalyticPrimaryBeam, AnalyticPrimaryBeamModel};
use problems::{empty_final_model, model_lifecycle};
use synthetic_pass::{BLOCKS, SAMPLES, Scene};

/// An entirely excluded beam is probed once per channel, even if the
/// first polarization is blank. A surviving beam stops the domain scan.
#[test]
fn analytic_domain_probe_visits_only_the_first_valid_polarization() {
    let width = 16;
    let channels = 3;
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [width, width],
        channels,
        1,
        ReconstructionBasis::ChannelLocal { channels },
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(100, 0.1, 0.0),
        vec![StokesI, StokesQ, StokesU, StokesV],
    );
    let scene = Scene::new(&problem);
    let lifecycle = model_lifecycle(&problem);
    let mut cycle = MajorCycle::initial(
        &problem,
        empty_final_model(&lifecycle),
        scene.resident_storage(),
    )
    .unwrap();
    let (model, mut pass) = cycle.parts();
    let mut images = scene.pass_images(model, true);
    images.polarizations = 4;
    images.psf = images.psf.take().map(|psf| {
        psf.chunks_exact(width * width)
            .flat_map(|plane| plane.repeat(4))
            .collect()
    });
    images.residual = images
        .residual
        .chunks_exact(width * width)
        .flat_map(|plane| plane.repeat(4))
        .collect();
    images.sum_weights = vec![1.0; channels * 4];
    images.published_sum_weights = vec![1.0; channels * 4];
    for channel in 0..channels {
        images.sum_weights[channel * 4] = 0.0;
        images.published_sum_weights[channel * 4] = 0.0;
    }
    pass.append(images).unwrap();
    let completion = cycle.finish(SAMPLES, BLOCKS).unwrap();
    for (x, expected, calls) in [(-1.0e5, false, channels), (8.0, true, 1)] {
        let centres = std::collections::BTreeMap::from([(ImageDomainRole::Main, vec![[x, 8.0]])]);
        let beam = AnalyticPrimaryBeam::new(
            &problem,
            AnalyticPrimaryBeamModel::CasaEvlaCommon,
            Some(&centres),
        )
        .unwrap()
        .unwrap();
        let setup = MinorCycleSetup {
            psf_cutoff: 0.35,
            psf_beams: None,
            mask_beams: None,
            algorithm: ReconstructionAlgorithm::Hogbom,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            nsigma: 3.0,
            noise: NoiseEstimate::Fast,
            automask: true,
            primary_beam: Some(beam),
            residual_pixel_mask: true,
            pbmask: 0.2,
        };
        let normal = completion.normal_state();
        DOMAIN_BEAM_PROBES.with(|count| count.set(0));
        let result = domain_beams(
            normal,
            completion.final_model(),
            &setup,
            false,
            &plane_keys(normal, false),
        )
        .unwrap();
        assert_eq!(&*result, &[expected]);
        assert_eq!(DOMAIN_BEAM_PROBES.with(std::cell::Cell::get), calls);
        if expected {
            use crate::minor::{PsfCache, prepare_minor_cycle};
            use crate::pass::WorkerTeam;
            use casa_imaging_deconvolution::{
                AutoMultithreshControls, ImageDomainReconstructionMaskPlans, ReconstructionMaskPlan,
            };
            let controls = AutoMultithreshControls {
                sidelobe_factor: 3.0,
                noise_factor: 5.0,
                low_noise_factor: 1.5,
                negative_factor: 0.0,
                minimum_beam_fraction: 0.0,
                smooth_factor: 1.0,
                cut_threshold: 0.01,
                grow_iterations: 0,
                grow_prune: false,
                minimum_percent_change: -1.0,
                noise: NoiseEstimate::Fast,
                verbose: false,
            };
            let plans = ImageDomainReconstructionMaskPlans::new([
                ReconstructionMaskPlan::AutoMultithresh {
                    coordinate: problem.geometry().domains()[0].direction(),
                    controls,
                    completed_major_cycles: 0,
                    cycle_threshold_reached: false,
                    previous: None,
                    stopped_channels: Box::new([]),
                },
            ])
            .unwrap();
            let mut setup = setup.clone();
            setup.nsigma = 0.0;
            // The exact simultaneous buffers at Shared::masked: the
            // channel's two supports, a plane's saved validity and mask,
            // the automask's output, cleanable support, and the two
            // intersections measured with them. Count their real vector
            // capacities against the worker's beam charge.
            let key = PlaneKey {
                domain: 0,
                channel: 0,
                polarization: 1,
            };
            let channel = BeamPixels::analytic(
                &setup,
                (completion.final_model().shape().domain_roles(), key, false),
            )
            .unwrap();
            let valid = vec![true; width * width];
            let plane_mask = channel.for_valid(&valid).within();
            let made = vec![true; width * width];
            let cleanable = vec![true; width * width];
            let measured = channel.for_valid(&valid);
            let held = channel.support.capacity()
                + channel.within.as_ref().unwrap().capacity()
                + valid.capacity()
                + plane_mask.capacity()
                + made.capacity()
                + cleanable.capacity()
                + measured.support.capacity()
                + measured.within.as_ref().unwrap().capacity();
            let charged =
                crate::minor::layout::beam_bytes_per_cell(&setup, &plans) as usize * width * width;
            assert!(
                held <= charged,
                "channel buffers hold {held} bytes, charge {charged}"
            );
            CHANNEL_BEAM_FORMS.with(|count| count.set(0));
            prepare_minor_cycle(
                &completion,
                &plans,
                &setup,
                &mut PsfCache::default(),
                &WorkerTeam::new(1).unwrap(),
            )
            .unwrap();
            assert_eq!(CHANNEL_BEAM_FORMS.with(std::cell::Cell::get), channels);
        }
    }
}

/// Polarization-specific validity intersects both channel supports, without
/// changing the support reused by another polarization.
#[test]
fn channel_support_keeps_each_polarizations_own_valid_pixels() {
    let beam = BeamPixels {
        support: vec![true, true, false, true],
        within: Some(vec![true, false, false, true]),
    };
    let first = beam.for_valid(&[true, false, true, false]);
    let second = beam.for_valid(&[false, true, true, true]);
    assert_eq!(first.support, [true, false, false, false]);
    assert_eq!(first.within.unwrap(), [true, false, false, false]);
    assert_eq!(second.support, [false, true, false, true]);
    assert_eq!(second.within.unwrap(), [false, false, false, true]);
    assert_eq!(beam.support, [true, true, false, true]);
}
