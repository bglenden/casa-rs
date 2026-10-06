// SPDX-License-Identifier: LGPL-3.0-or-later

use super::super::{
    SpectralOperatorSpecification, prepare_spectral_operator, spectral_operator_workload,
};
use super::*;

fn fixture() -> (
    casa_imaging_model::CompiledProblem,
    crate::ModelLifecycle,
    crate::ModelGeneration,
) {
    let cells = 512 * 512;
    let (problem, lifecycle, model, normal) = crate::major_cycle::native_minor_fixture::build(
        vec![Complex64::default(); 2 * cells].into_boxed_slice(),
        vec![Complex64::default(); 3 * cells].into_boxed_slice(),
        None,
    );
    drop(normal);
    (problem, lifecycle, model)
}

#[test]
fn t55_identity_chart_transfers_primitive_allocations_to_its_domain() {
    let (problem, _, model) = fixture();
    let specification = SpectralOperatorSpecification::new(&problem).unwrap();
    assert_eq!(specification.domains.len(), 1);
    assert_eq!(specification.charts.len(), 1);
    let cells = 512 * 512;
    let mut primitives = SpectralOperatorPrimitives::native_taylor_fixture(
        &problem,
        model.generation_id(),
        vec![Complex64::new(0.25, -0.5); 2 * cells].into_boxed_slice(),
        vec![Complex64::new(1.0, 0.0); 3 * cells].into_boxed_slice(),
        None,
    );
    primitives.invariant_dirty = Some(primitives.dirty.clone());
    let dirty = primitives.dirty.as_ptr();
    let invariant_dirty = primitives.invariant_dirty.as_ref().unwrap().as_ptr();
    let psf = primitives.psf.as_ptr();
    let sensitivity = primitives.sensitivity.as_ptr();
    let content = primitives.normal_state_content_identity();

    let domains = super::super::combine_initial_chart_primitives(
        &specification,
        [Ok(primitives)].into_iter(),
    )
    .unwrap();
    let retained = domains.primary();
    assert_eq!(retained.normal_state_content_identity(), content);
    assert_eq!(retained.dirty.as_ptr(), dirty);
    assert_eq!(
        retained.invariant_dirty.as_ref().unwrap().as_ptr(),
        invariant_dirty
    );
    assert_eq!(retained.psf.as_ptr(), psf);
    assert_eq!(retained.sensitivity.as_ptr(), sensitivity);
}

#[test]
fn t51_initial_empty_projection_omits_only_absent_residual_planes() {
    let (problem, _, _) = fixture();
    let specification = SpectralOperatorSpecification::new(&problem).unwrap();
    assert!(specification.is_initial_certified_zero(SpectralOperatorPass::InitialMajor));
    assert!(!specification.is_initial_certified_zero(SpectralOperatorPass::ResidualRefresh));
    let initial =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::InitialMajor).unwrap();
    let refresh =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::ResidualRefresh)
            .unwrap();
    let grid_cells = checked_cells(specification.grid_shape()).unwrap();
    let image_cells = 512 * 512;
    assert_eq!(initial.coefficient_terms(), 2);
    assert_eq!(initial.normal_moments(), 3);
    assert_eq!(initial.grid_complex_values(), grid_cells * (2 * 2 + 2 * 3));
    assert_eq!(
        initial.primitive_complex_values(),
        image_cells * 2 * (2 * 2 + 3)
    );
    assert_eq!(
        initial.fold_accumulator_complex_values(),
        image_cells * (2 * 2 + 3)
    );
    assert_eq!(refresh.grid_complex_values(), grid_cells * 2 * 2);
    assert_eq!(
        refresh.primitive_complex_values(),
        image_cells * (2 * 2 + 3 + 2)
    );
    let mut evaluated_specification = specification.clone();
    evaluated_specification.initial_model = super::super::InitialModelClassification::Evaluated;
    assert!(!evaluated_specification.is_initial_certified_zero(SpectralOperatorPass::InitialMajor));
    let evaluated = spectral_operator_workload(
        &evaluated_specification,
        3,
        SpectralOperatorPass::InitialMajor,
    )
    .unwrap();
    assert_eq!(
        evaluated.grid_complex_values(),
        grid_cells * (4 * 2 + 2 * 3)
    );
    assert_eq!(
        evaluated.primitive_complex_values(),
        image_cells * 2 * (3 * 2 + 3)
    );
    assert_eq!(
        evaluated.fold_accumulator_complex_values(),
        image_cells * (3 * 2 + 3)
    );
    assert_eq!(
        initial.forward_complex_values(),
        evaluated.forward_complex_values()
    );
    assert_eq!(
        initial.primitive_f64_values(),
        evaluated.primitive_f64_values()
    );
    assert_eq!(
        initial.primitive_validity_values(),
        evaluated.primitive_validity_values()
    );
    assert_eq!(
        refresh,
        spectral_operator_workload(
            &evaluated_specification,
            3,
            SpectralOperatorPass::ResidualRefresh
        )
        .unwrap()
    );
}

#[test]
fn t51_initial_empty_projection_rejects_delta_before_model_allocation() {
    use casa_imaging_model::{ModelCell, ModelDeltaTerm, ModelValue};
    let (problem, lifecycle, model) = fixture();
    let specification = SpectralOperatorSpecification::new(&problem).unwrap();
    let initial =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::InitialMajor).unwrap();
    let mut owner = prepare_spectral_operator(specification.clone(), initial)
        .unwrap()
        .begin_streaming(&problem)
        .unwrap();
    let original_grids = owner
        .operators
        .iter()
        .map(|operator| {
            operator
                .dirty_grids
                .iter()
                .flatten()
                .map(Array2::len)
                .sum::<usize>()
                + operator
                    .dirty_compensations
                    .iter()
                    .flatten()
                    .map(Array2::len)
                    .sum::<usize>()
                + operator
                    .psf_grids
                    .iter()
                    .flatten()
                    .map(Array2::len)
                    .sum::<usize>()
                + operator
                    .psf_compensations
                    .iter()
                    .flatten()
                    .map(Array2::len)
                    .sum::<usize>()
        })
        .sum::<usize>();
    assert!(
        owner
            .operators
            .iter()
            .all(|operator| operator.residual_grids.is_none())
    );
    owner.bind_major_cycle_model(&model, None).unwrap();
    assert!(
        owner
            .operators
            .iter()
            .all(|operator| operator.residual_grids.is_none()
                && operator.residual_compensations.is_none())
    );
    drop(owner);

    let delta = lifecycle
        .compile_delta(
            &model,
            [ModelDeltaTerm::new(
                ModelCell::new(0, 0, 0, [256, 256]),
                ModelValue::new(0.25).unwrap(),
            )],
        )
        .unwrap();
    let changed = lifecycle.apply_delta(model, delta).unwrap();
    let mut owner = prepare_spectral_operator(specification.clone(), initial)
        .unwrap()
        .begin_streaming(&problem)
        .unwrap();
    assert_eq!(
        owner.bind_major_cycle_model(&changed, None),
        Err(SpectralOperatorError::ModelMismatch)
    );
    assert_eq!(original_grids, initial.grid_complex_values());
    assert!(
        owner
            .operators
            .iter()
            .all(|operator| operator.residual_grids.is_none()
                && operator.residual_compensations.is_none())
    );
    drop(owner);

    let refresh =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::ResidualRefresh)
            .unwrap();
    let mut owner = prepare_spectral_operator(specification, refresh)
        .unwrap()
        .begin_streaming(&problem)
        .unwrap();
    owner.bind_major_cycle_model(&changed, None).unwrap();
    assert!(
        owner
            .operators
            .iter()
            .all(|operator| operator.residual_grids.is_some()
                && operator.residual_compensations.is_some())
    );
}
