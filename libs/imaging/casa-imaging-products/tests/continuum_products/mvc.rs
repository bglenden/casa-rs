// SPDX-License-Identifier: LGPL-3.0-or-later

//! Multi-family MVC product and staging laws.

use super::*;

/// A failure in a later family leaves the earlier staging intact until the
/// caller decides whether to publish or discard the whole run.
#[test]
fn a_failed_second_family_preserves_the_first_family_staging() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
    ];
    let problem = continuum_problem_with_policy(209, &products, RestoringBeamPolicy::None);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let output = ImageOutput::for_problem(&problem);
    let other = output.with_coordinates(vec![(
        ImageDomainRole::Main,
        casa_coordinates::CoordinateSystem::new(),
    )]);
    let storage = ProductStoragePlan::new(1, 1).unwrap();
    produce_continuum_members(&planned, &inputs, storage, &(), &output).unwrap();
    let before = output.entries();
    assert_eq!(before.len(), planned.members().len());
    let error = produce_continuum_members(&planned, &inputs, storage, &FailingCall::new(2), &other)
        .unwrap_err();
    assert!(matches!(error, ProductsError::GeneratedNonfinite));
    assert_eq!(
        output.entries(),
        before,
        "second family's partial staging was discarded"
    );
    let mut published = Vec::new();
    other
        .publish(|path| published.push(path.to_path_buf()))
        .unwrap();
    assert_eq!(published.len(), planned.members().len());
}

/// CASA MVC first adjusts each ordinary cube residual by the common Taylor
/// beam divided by that channel's published PB; the other cube products do
/// not change.
#[test]
fn a_cube_residual_uses_the_supplied_taylor_primary_beam() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        210,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let round = run_continuum_round(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let ordinary = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&ordinary, &controls);
    let baseline = generate_for(&planned, &ordinary);
    let beam: Vec<f32> = (0..SHAPE[0] * SHAPE[1])
        .map(|i| 0.3 + 0.001 * i as f32)
        .collect();
    let adjusted = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_cube_taylor_primary_beam(&beam)
        .unwrap();
    let actual = generate_for(&planned, &adjusted);
    let get = |products: &GeneratedProducts, name: &str| {
        products
            .members()
            .iter()
            .find(|m| m.name() == name)
            .unwrap()
            .payload()
            .to_vec()
    };
    for name in [".psf", ".sumwt", ".pb"] {
        assert_eq!(get(&actual, name), get(&baseline, name), "{name}");
    }
    let residual = get(&baseline, ".residual");
    let pb = get(&baseline, ".pb");
    let corrected = get(&actual, ".residual");
    for (index, (&value, &channel_pb)) in residual.iter().zip(&pb).enumerate() {
        assert!(channel_pb > 0.2, "fixture has a supported PB");
        let expected = beam[index / 3] * value / channel_pb;
        assert!(
            (corrected[index] - expected).abs() < 1e-6,
            "pixel/channel {index}"
        );
    }
    let published_psf = get(&baseline, ".psf");
    let published_sumwt = get(&baseline, ".sumwt");
    for (channel, &published_weight) in published_sumwt.iter().enumerate() {
        let streamed =
            casa_imaging_products::read_mvc_cube_plane(&planned, &adjusted, channel).unwrap();
        let raw = casa_imaging_products::read_mvc_cube_plane(&planned, &ordinary, channel).unwrap();
        let beam_only =
            casa_imaging_products::read_mvc_cube_primary_beam(&planned, &ordinary, channel)
                .unwrap();
        assert_eq!(beam_only.sum_weight, published_weight);
        assert_eq!(streamed.sum_weight, published_weight);
        assert_eq!(
            streamed.frequency_hz,
            problem
                .geometry()
                .spectral()
                .channel_centre_hz(channel)
                .unwrap()
        );
        for pixel in 0..beam.len() {
            let index = pixel * 3 + channel;
            assert_eq!(streamed.psf[pixel], published_psf[index], "PSF {index}");
            assert_eq!(
                raw.residual[pixel], residual[index],
                "ordinary residual {index}"
            );
            assert_eq!(
                streamed.residual[pixel], corrected[index],
                "residual {index}"
            );
            assert_eq!(streamed.primary_beam[pixel], pb[index], "PB {index}");
            assert_eq!(beam_only.pixels[pixel], pb[index], "PB-only {index}");
        }
    }
    assert!(casa_imaging_products::read_mvc_cube_plane(&planned, &adjusted, 3).is_err());
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .with_cube_taylor_primary_beam(&beam[..beam.len() - 1])
            .is_err()
    );
    let mut invalid = beam;
    invalid[0] = f32::NAN;
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .with_cube_taylor_primary_beam(&invalid)
            .is_err()
    );
    invalid[0] = -1.0;
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .with_cube_taylor_primary_beam(&invalid)
            .is_err()
    );
}

#[test]
fn cube_residual_beam_requires_a_cube_and_a_primary_beam_model() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
    ];
    let continuum = continuum_problem_with_policy(211, &products, RestoringBeamPolicy::None);
    let continuum_round = run_continuum_round(&continuum);
    let common = [0.5; SHAPE[0] * SHAPE[1]];
    assert!(
        ContinuumProductInputs::from_major_cycle(&continuum, &continuum_round.join)
            .with_cube_taylor_primary_beam(&common)
            .is_err()
    );

    let cube = continuum_problem_with_reconstruction(
        212,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let cube_round = run_continuum_round(&cube);
    let inputs = ContinuumProductInputs::from_major_cycle(&cube, &cube_round.join)
        .with_cube_taylor_primary_beam(&common)
        .unwrap();
    let plan = planned_for(&inputs, &ContinuumProductControls::default());
    let output = ImageOutput::for_problem(&cube);
    assert!(matches!(
        produce_continuum_members(&plan, &inputs, full_window(&plan), &(), &output),
        Err(ProductsError::MissingPrimaryBeamModel)
    ));
    assert!(output.entries().is_empty());
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_plane(&plan, &inputs, 0),
        Err(ProductsError::MissingPrimaryBeamModel)
    ));
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_primary_beam(&plan, &inputs, 0),
        Err(ProductsError::MissingPrimaryBeamModel)
    ));
    let with_model = planned_for(
        &inputs,
        &ContinuumProductControls::default()
            .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon),
    );
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_plane(&with_model, &inputs, 0),
        Err(ProductsError::UnsupportedProblem)
    ));
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_primary_beam(&with_model, &inputs, 0),
        Err(ProductsError::UnsupportedProblem)
    ));
    let continuum_inputs =
        ContinuumProductInputs::from_major_cycle(&continuum, &continuum_round.join);
    let continuum_plan = planned_for(
        &continuum_inputs,
        &ContinuumProductControls::default()
            .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon),
    );
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_plane(&continuum_plan, &continuum_inputs, 0),
        Err(ProductsError::UnsupportedProblem)
    ));
    assert!(matches!(
        casa_imaging_products::read_mvc_cube_primary_beam(&continuum_plan, &continuum_inputs, 0),
        Err(ProductsError::UnsupportedProblem)
    ));
}

#[test]
fn bounded_cube_reader_matches_published_values_across_the_pb_cutoff() {
    use casa_imaging_products::{AnalyticPrimaryBeam, PrimaryBeamCentres, read_mvc_cube_plane};
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        213,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let model = AnalyticPrimaryBeamModel::CasaEvlaCommon;
    let mut low = 0.0;
    let mut high = 100_000.0;
    for _ in 0..48 {
        let centre = (low + high) / 2.0;
        let points = PrimaryBeamCentres::from([(ImageDomainRole::Main, vec![[centre, 4.0]])]);
        let beam = AnalyticPrimaryBeam::new(&problem, model, Some(&points))
            .unwrap()
            .unwrap();
        let weighted: f32 = (0..3)
            .map(|channel| {
                let value = beam.plane(&ImageDomainRole::Main, channel).unwrap()[4 * SHAPE[1] + 4];
                if value > 0.2 {
                    value * (channel + 1) as f32
                } else {
                    0.0
                }
            })
            .sum::<f32>()
            / 6.0;
        if weighted > 0.2 {
            low = centre;
        } else {
            high = centre;
        }
    }
    let points =
        PrimaryBeamCentres::from([(ImageDomainRole::Main, vec![[(low + high) / 2.0, 4.0]])]);
    let round = run_continuum_round(&problem);
    let common = [0.5; SHAPE[0] * SHAPE[1]];
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_primary_beam_centres(&points)
        .with_cube_taylor_primary_beam(&common)
        .unwrap();
    let controls = ContinuumProductControls::default().with_primary_beam_model(model);
    let planned = planned_for(&inputs, &controls);
    let published = generate_for(&planned, &inputs);
    let values = |name: &str| {
        published
            .members()
            .iter()
            .find(|member| member.name() == name)
            .unwrap()
            .payload()
            .to_vec()
    };
    let pb = values(".pb");
    let residual = values(".residual");
    assert!(pb.contains(&0.0) && pb.iter().any(|value| *value > 0.2));
    for channel in 0..3 {
        let plane = read_mvc_cube_plane(&planned, &inputs, channel).unwrap();
        let beam_only =
            casa_imaging_products::read_mvc_cube_primary_beam(&planned, &inputs, channel).unwrap();
        for pixel in 0..common.len() {
            let index = pixel * 3 + channel;
            assert_eq!(plane.primary_beam[pixel], pb[index], "PB {index}");
            assert_eq!(beam_only.pixels[pixel], pb[index], "PB-only {index}");
            assert_eq!(plane.residual[pixel], residual[index], "residual {index}");
        }
    }
    let cells = common.len();
    let mut mean = vec![0.0; cells];
    let mut psf = vec![0.0; 3 * cells];
    let mut taylor_residual = vec![0.0; 2 * cells];
    let mut sumwt = [0.0; 3];
    casa_imaging_products::reduce_mvc_cube(
        &planned,
        &inputs,
        1.4e9,
        &mut mean,
        &mut psf,
        &mut taylor_residual,
        &mut sumwt,
    )
    .unwrap();
    let weights = values(".sumwt");
    let total: f64 = weights.iter().map(|value| f64::from(*value)).sum();
    let mut below = 0;
    for pixel in 0..cells {
        let expected: f64 = (0..3)
            .map(|channel| f64::from(pb[pixel * 3 + channel]) * f64::from(weights[channel]))
            .sum::<f64>()
            / total;
        assert!(
            (f64::from(mean[pixel]) - expected).abs() < 1e-6,
            "weighted PB {pixel}"
        );
        if mean[pixel] > 0.0 && mean[pixel] <= 0.2 {
            below += 1;
            for term in 0..3 {
                assert_eq!(psf[term * cells + pixel], 0.0);
            }
            for term in 0..2 {
                assert_eq!(taylor_residual[term * cells + pixel], 0.0);
            }
        }
    }
    assert!(below > 0, "the reduced mean PB must cross its cutoff");
    let good = mean.iter().filter(|value| **value > 0.2).count();
    assert!(
        good > 0,
        "the reduced mean PB must also have supported pixels"
    );
    let final_inputs = inputs.clone().with_cube_taylor_primary_beam(&mean).unwrap();
    let final_products = generate_for(&planned, &final_inputs);
    let final_residual = final_products
        .members()
        .iter()
        .find(|member| member.name() == ".residual")
        .unwrap()
        .payload();
    for pixel in 0..cells {
        if mean[pixel] > 0.2 {
            let expected: f64 = (0..3)
                .map(|channel| {
                    let index = pixel * 3 + channel;
                    f64::from(final_residual[index])
                        * f64::from(pb[index])
                        * f64::from(weights[channel])
                })
                .sum::<f64>()
                / total
                / f64::from(mean[pixel]);
            assert!(
                (f64::from(taylor_residual[pixel]) - expected).abs() < 1e-6,
                "supported residual {pixel}"
            );
        }
    }
}

/// The complete bounded reduction reads real normal-state planes and agrees
/// with independent sums of the cube products as they are published.
#[test]
fn mvc_reduces_real_cube_planes_to_weighted_taylor_images() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        215,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let round = run_continuum_round(&problem);
    let ordinary = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let plan = planned_for(&ordinary, &controls);
    let baseline = generate_for(&plan, &ordinary);
    let values = |products: &GeneratedProducts, name: &str| {
        products
            .members()
            .iter()
            .find(|member| member.name() == name)
            .unwrap()
            .payload()
            .to_vec()
    };
    let cube_pb = values(&baseline, ".pb");
    let cube_psf = values(&baseline, ".psf");
    let weights = values(&baseline, ".sumwt");
    let cells = SHAPE[0] * SHAPE[1];
    let mut mean = vec![0.0; cells];
    let mut psf = vec![0.0; 3 * cells];
    let mut residual = vec![0.0; 2 * cells];
    let mut sumwt = [0.0; 3];
    let reference = 1.402e9;
    casa_imaging_products::reduce_mvc_cube(
        &plan,
        &ordinary,
        reference,
        &mut mean,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )
    .unwrap();
    let adjusted = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_cube_taylor_primary_beam(&mean)
        .unwrap();
    let corrected = values(&generate_for(&plan, &adjusted), ".residual");
    let total: f64 = weights.iter().map(|&value| f64::from(value)).sum();
    let x: Vec<f64> = (0..3)
        .map(|channel| {
            (problem
                .geometry()
                .spectral()
                .channel_centre_hz(channel)
                .unwrap()
                - reference)
                / reference
        })
        .collect();
    for pixel in 0..cells {
        let pb: f64 = (0..3)
            .map(|c| f64::from(cube_pb[pixel * 3 + c]) * f64::from(weights[c]))
            .sum::<f64>()
            / total;
        assert!((f64::from(mean[pixel]) - pb).abs() < 1e-6, "PB {pixel}");
        for term in 0..3 {
            let moment: f64 = (0..3)
                .map(|c| {
                    f64::from(cube_psf[pixel * 3 + c])
                        * f64::from(cube_pb[pixel * 3 + c])
                        * f64::from(weights[c])
                        * x[c].powi(term as i32)
                })
                .sum::<f64>()
                / total
                / pb;
            assert!(
                (f64::from(psf[term * cells + pixel]) - moment).abs() < 1e-6,
                "PSF {term}/{pixel}"
            );
            if term < 2 {
                let moment: f64 = (0..3)
                    .map(|c| {
                        f64::from(corrected[pixel * 3 + c])
                            * f64::from(cube_pb[pixel * 3 + c])
                            * f64::from(weights[c])
                            * x[c].powi(term as i32)
                    })
                    .sum::<f64>()
                    / total
                    / pb;
                assert!(
                    (f64::from(residual[term * cells + pixel]) - moment).abs() < 1e-6,
                    "residual {term}/{pixel}"
                );
            }
        }
    }
    for (term, &actual) in sumwt.iter().enumerate() {
        let expected: f64 = (0..3)
            .map(|c| f64::from(weights[c]).powi(2) * x[c].powi(term as i32))
            .sum::<f64>()
            / total;
        assert!((f64::from(actual) - expected).abs() < 1e-6, "sumwt {term}");
    }
    for (mean_len, psf_len, residual_len, reference) in [
        (cells - 1, 3 * cells, 2 * cells, 1.4e9),
        (cells, 3 * cells - 1, 2 * cells, 1.4e9),
        (cells, 3 * cells, 2 * cells - 1, 1.4e9),
        (cells, 3 * cells, 2 * cells, 0.0),
        (cells, 3 * cells, 2 * cells, f64::NAN),
    ] {
        let mut mean = vec![9.0; mean_len];
        let mut psf = vec![9.0; psf_len];
        let mut residual = vec![9.0; residual_len];
        let mut sumwt = [9.0; 3];
        assert!(
            casa_imaging_products::reduce_mvc_cube(
                &plan,
                &ordinary,
                reference,
                &mut mean,
                &mut psf,
                &mut residual,
                &mut sumwt
            )
            .is_err()
        );
        assert!(
            mean.iter().all(|value| *value == 9.0),
            "preflight leaves PB untouched"
        );
        assert!(
            psf.iter().all(|value| *value == 9.0),
            "preflight leaves PSF untouched"
        );
        assert!(
            residual.iter().all(|value| *value == 9.0),
            "preflight leaves residual untouched"
        );
        assert_eq!(sumwt, [9.0; 3], "preflight leaves weights untouched");
    }
}
