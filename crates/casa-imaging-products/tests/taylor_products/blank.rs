// SPDX-License-Identifier: LGPL-3.0-or-later
//! The all-zero, unrestored MVC product boundary.
use super::*;
use casa_imaging_deconvolution::{MajorCycle, ModelLifecycle, ModelStoragePlan, NormalStoragePlan};
use casa_imaging_products::encode_mvc_blank_taylor_planes;

fn completion(problem: &casa_imaging_model::CompiledProblem, change: &str) -> MajorCycleCompletion {
    let cells = SHAPE[0] * SHAPE[1];
    let mut images = if change == "one-term" {
        casa_imaging_deconvolution::PassImages {
            domain: 0,
            shape: SHAPE,
            channels: 0..1,
            polarizations: 1,
            residual: vec![0.0; cells],
            psf: Some(vec![0.0; cells]),
            sum_weights: vec![1.0],
            published_sum_weights: vec![0.0],
            weight: None,
        }
    } else {
        encode_mvc_blank_taylor_planes(SHAPE, vec![0.0; 3 * cells], vec![0.0; 2 * cells], [0.0; 3])
            .unwrap()
    };
    match change {
        "psf" => images.psf.as_mut().unwrap()[0] = 1.0,
        "residual" => images.residual[0] = 1.0,
        "weight" => images.published_sum_weights[1] = 1.0,
        _ => {}
    }
    let lifecycle = ModelLifecycle::new(problem, ModelStoragePlan::resident(usize::MAX).unwrap());
    let terms = if change == "model" {
        vec![ModelDeltaTerm::new(
            ModelCell::new(0, 0, 0, CENTRE),
            ModelValue::new(1.0).unwrap(),
        )]
    } else {
        Vec::new()
    };
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), terms)
        .unwrap();
    let mut cycle =
        MajorCycle::initial(problem, model, NormalStoragePlan::resident(1).unwrap()).unwrap();
    cycle.parts().1.append(images).unwrap();
    cycle.finish_read().unwrap()
}

fn generate(
    problem: &casa_imaging_model::CompiledProblem,
    completion: &MajorCycleCompletion,
    marker: bool,
) -> Result<(), ProductsError> {
    let beam = vec![0.5; SHAPE[0] * SHAPE[1]];
    let inputs = ContinuumProductInputs::from_major_cycle(problem, completion)
        .with_taylor_residual_sum_weight(1.0)?;
    let inputs = if marker {
        inputs.with_taylor_primary_beam(&beam)?
    } else {
        inputs
    };
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let plan = PlannedContinuumGeneration::new(&inputs, &controls)?;
    let output = ImageOutput::for_problem(problem);
    produce_continuum_members(&plan, &inputs, full_window(&plan), &(), &output)?;
    Ok(())
}

#[test]
fn blank_mvc_is_scoped_and_never_hides_nonzero_planes() {
    let kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::TaylorTerms,
        ProductKind::PrimaryBeam,
    ];
    let problem = taylor_problem_with_beam(
        220,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let blank = completion(&problem, "blank");
    generate(&problem, &blank, true).unwrap();
    assert!(
        matches!(
            generate(&problem, &blank, false),
            Err(ProductsError::PrincipalSumWeight { .. })
        ),
        "ordinary Taylor retains its zero-weight refusal"
    );
    // A beam alone does not authorize MVC's unity-normalized handoff.
    let beam = vec![0.5; SHAPE[0] * SHAPE[1]];
    let ordinary = ContinuumProductInputs::from_major_cycle(&problem, &blank)
        .with_taylor_primary_beam(&beam)
        .unwrap();
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let plan = PlannedContinuumGeneration::new(&ordinary, &controls).unwrap();
    let output = ImageOutput::for_problem(&problem);
    assert!(matches!(
        produce_continuum_members(&plan, &ordinary, full_window(&plan), &(), &output),
        Err(ProductsError::PrincipalSumWeight { .. })
    ));
    for change in ["psf", "residual", "weight", "model"] {
        let corrupted = completion(&problem, change);
        assert!(
            generate(&problem, &corrupted, true).is_err(),
            "nonzero {change} must fail"
        );
    }
    let mut restored_kinds = kinds.to_vec();
    restored_kinds.extend([ProductKind::Model, ProductKind::RestoredImage]);
    let restored = taylor_problem_with_beam(
        221,
        &restored_kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::PerPlane,
    );
    let blank = completion(&restored, "blank");
    assert!(
        generate(&restored, &blank, true).is_err(),
        "a blank restored PSF cannot be fitted"
    );
    let one_term = taylor_problem_with_beam_terms(
        224,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
        1,
    );
    let blank = completion(&one_term, "one-term");
    assert!(
        generate(&one_term, &blank, true).is_err(),
        "the MVC path needs two terms"
    );
    let mut kinds_with_model = kinds.to_vec();
    kinds_with_model.push(ProductKind::Model);
    let model = taylor_problem_with_beam(
        222,
        &kinds_with_model,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let blank = completion(&model, "blank");
    assert!(
        generate(&model, &blank, true).is_err(),
        "a model is outside the dirty-only path"
    );
}

#[test]
fn blank_mvc_keeps_the_common_beam_and_its_cutoff_mask() {
    let kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::TaylorTerms,
        ProductKind::PrimaryBeam,
    ];
    let problem = taylor_problem_with_beam(
        223,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let completion = completion(&problem, "blank");
    let beam: Vec<f32> = (0..SHAPE[0] * SHAPE[1])
        .map(|i| [0.05, 0.2, 0.5][i % 3])
        .collect();
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &completion)
        .with_taylor_residual_sum_weight(1.0)
        .unwrap()
        .with_taylor_primary_beam(&beam)
        .unwrap();
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let plan = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    let output = ImageOutput::for_problem(&problem);
    let published =
        produce_continuum_members(&plan, &inputs, full_window(&plan), &(), &output).unwrap();
    let products = GeneratedProducts::from_output(&plan, &published, &output);
    let pb = member(&products, ".pb.tt0");
    assert_eq!(pb.payload(), beam);
    assert_eq!(
        pb.validity(),
        beam.iter().map(|value| *value > 0.2).collect::<Vec<_>>()
    );
}
