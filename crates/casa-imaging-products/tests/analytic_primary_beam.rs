// SPDX-License-Identifier: LGPL-3.0-or-later

//! The analytic primary beam the minor cycle reads (#1078) is the one the
//! products publish: `AnalyticPrimaryBeam` gives `.pb`'s values where it
//! keeps them and `.pb`'s mask as its support, refuses the frequencies
//! planning refuses, and is no beam of a mosaic's, whose `.pb` is the
//! weight image's.

mod common;
use common::continuum::{
    SHAPE, axes, continuum_problem_with_domains_and_reconstruction,
    continuum_problem_with_reconstruction,
};
use common::synthetic_pass::{Scene, two_cycle_round};
use common::{GeneratedProducts, ImageOutput, full_window};

use casa_imaging_model::{
    FacetLayout, ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentResponse, ProductKind,
    ReconstructionAlgorithm, ReconstructionBasis, RestoringBeamPolicy,
};
use casa_imaging_products::{
    AnalyticPrimaryBeam, AnalyticPrimaryBeamModel, ContinuumProductControls,
    ContinuumProductInputs, PlannedContinuumGeneration, ProductsError, produce_continuum_members,
};

#[test]
fn the_minor_cycle_reads_the_published_analytic_beam() {
    let problem = continuum_problem_with_reconstruction(
        109,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::PrimaryBeam,
        ],
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    );
    let scene = Scene::new(&problem);
    let amplitudes = vec![1.0; scene.planes()];
    let scene = scene.with_point(0, [SHAPE[0] / 2, SHAPE[1] / 2], &amplitudes);
    let join = two_cycle_round(&problem, &scene, Vec::new());
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).expect("planned generation");
    let output = ImageOutput::for_problem(&problem);
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("product generation");
    let published = GeneratedProducts::from_output(&planned, &generated, &output);
    let pb = published
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");

    let analytic = |model| AnalyticPrimaryBeam::new(&problem, model, None).expect("L band");
    let beam = analytic(AnalyticPrimaryBeamModel::CasaEvlaCommon).expect("an analytic beam");
    let support = beam.support(&ImageDomainRole::Main, 0).expect("support");
    assert_eq!(support, pb.validity());
    let plane = beam.plane(&ImageDomainRole::Main, 0).expect("plane");
    for ((value, published), kept) in plane.iter().zip(pb.payload()).zip(&support) {
        assert_eq!(if *kept { *value } else { 0.0 }, *published);
    }
    assert!(
        analytic(AnalyticPrimaryBeamModel::MosaicSensitivity).is_none(),
        "a mosaic's beam is its weight image's"
    );
}

/// The minor cycle's beam refuses the cube channel planning refuses, past
/// the EVLA's C band.
#[test]
fn the_minor_cycles_beam_refuses_an_unsupported_frequency() {
    let products = [ProductKind::Psf, ProductKind::PrimaryBeam];
    let template = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let problem = continuum_problem_with_domains_and_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
        [7.880e9, 128.0e6],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            template.geometry().domains()[0].direction(),
            FacetLayout::Single,
            axes(),
        )],
    );
    let refused =
        AnalyticPrimaryBeam::new(&problem, AnalyticPrimaryBeamModel::CasaEvlaCommon, None)
            .expect_err("past the C band");
    assert!(
        matches!(
            refused,
            ProductsError::UnsupportedPrimaryBeamFrequency {
                model: AnalyticPrimaryBeamModel::CasaEvlaCommon,
                output_channel: 1,
                frequency_hz,
            } if frequency_hz == 8.008e9
        ),
        "{refused:?}"
    );
}
