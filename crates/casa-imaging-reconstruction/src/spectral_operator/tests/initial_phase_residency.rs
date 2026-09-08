// SPDX-License-Identifier: LGPL-3.0-or-later

use super::super::{
    InitialModelClassification, SpectralOperatorSpecification, combine_initial_chart_primitives,
    prepare_spectral_operator, spectral_operator_workload,
};
use super::*;
use casa_imaging_model::{AwProjectionContract, InstrumentModel};
use std::mem::size_of;

fn fixture() -> (
    casa_imaging_model::CompiledProblem,
    crate::ModelGeneration,
    crate::FinalNormalState,
) {
    let cells = 512 * 512;
    let (problem, _, model, normal) = crate::major_cycle::native_minor_fixture::build(
        vec![Complex64::default(); 2 * cells].into_boxed_slice(),
        vec![Complex64::default(); 3 * cells].into_boxed_slice(),
        None,
    );
    (problem, model, normal)
}

// The owner unit fixture varies private chart geometry independently of the
// compiler fixture. Runtime integration separately covers compiled AW inputs.
fn aw_specification(
    problem: &casa_imaging_model::CompiledProblem,
    shape: [usize; 2],
) -> SpectralOperatorSpecification {
    let mut specification = SpectralOperatorSpecification::new(problem).unwrap();
    let direction = specification.domains[0]
        .direction
        .with_reference_pixel([(shape[0] / 2) as f64, (shape[1] / 2) as f64]);
    let geometry =
        compile_operator_geometry(shape, direction, OperatorGridGeometry::AwProjection).unwrap();
    specification.image_shape = shape;
    specification.grid_shape = geometry.grid_shape;
    specification.image_blc = geometry.image_blc;
    specification.charts[0].geometry = geometry;
    specification.domains[0].image_shape = shape;
    specification.domains[0].direction = direction;
    specification.instrument_model = Some(InstrumentModel::CasaEvlaWidebandAwV1);
    specification.aw_projection = Some(
        AwProjectionContract::new(
            1.0,
            1.try_into().unwrap(),
            true,
            true,
            true,
            false,
            false,
            [0.0, 0.0],
            360.0,
            360.0,
        )
        .unwrap(),
    );
    specification
}

#[test]
fn t51_initial_phase_residency_is_geometry_derived_and_fail_closed() {
    let (problem, _, _) = fixture();
    let specification = aw_specification(&problem, [4096, 4096]);
    let workload =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::InitialMajor).unwrap();
    let phases = workload.initial_phase_residency().unwrap();
    assert_eq!(phases.accumulation_bytes(), 4_294_967_296);
    assert_eq!(phases.completion_bytes(), 7_381_975_089);
    assert_eq!(phases.retained_bytes(), 2_281_701_425);

    let small = aw_specification(&problem, [8, 6]);
    let projected = spectral_operator_workload(&small, 3, SpectralOperatorPass::InitialMajor)
        .unwrap()
        .initial_phase_residency()
        .unwrap();
    let complex = 8 * 6 * size_of::<Complex64>();
    let real = 8 * 6 * size_of::<f64>();
    let metadata = 6 * size_of::<f64>() + size_of::<SpectralChannelValidity>();
    assert_eq!(projected.accumulation_bytes(), 16 * complex);
    assert_eq!(
        projected.completion_bytes(),
        26 * complex + 3 * real + metadata
    );
    assert_eq!(
        projected.retained_bytes(),
        7 * complex + 3 * real + metadata
    );
    assert!(
        spectral_operator_workload(&small, 3, SpectralOperatorPass::ResidualRefresh)
            .unwrap()
            .initial_phase_residency()
            .is_none()
    );

    let mut exclusions = Vec::new();
    let mut excluded = small.clone();
    excluded.initial_model = InitialModelClassification::Evaluated;
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.aw_projection = None;
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.instrument_model = None;
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.mosaic = true;
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.basis =
        SpectralBasisPlan::TaylorViaChannelMajor(BlockNormalPlan::taylor(1.0e9, 2).unwrap());
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.basis = SpectralBasisPlan::ChannelLocal;
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.basis = SpectralBasisPlan::Joint {
        continuum: BlockNormalPlan::taylor(1.0e9, 2).unwrap(),
        line_terms: 1,
    };
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.charts = vec![small.charts[0].clone(); 2].into_boxed_slice();
    exclusions.push(excluded);
    let mut excluded = small.clone();
    excluded.domains = vec![small.domains[0].clone(); 2].into_boxed_slice();
    exclusions.push(excluded);
    let mut excluded = small;
    excluded.domains[0].image_shape[0] += 1;
    exclusions.push(excluded);
    for excluded in exclusions {
        assert!(
            super::super::project_initial_phase_residency(
                &excluded,
                SpectralOperatorPass::InitialMajor,
                workload.grid_complex_values(),
            )
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(
        super::super::project_initial_phase_residency(
            &specification,
            SpectralOperatorPass::InitialMajor,
            usize::MAX,
        ),
        Err(SpectralOperatorError::ResidencyOverflow)
    );
}

#[test]
fn t51_initial_phase_residency_bounds_actual_formation_and_parent_copy() {
    let (problem, model, _) = fixture();
    let specification = Arc::new(aw_specification(&problem, [8, 6]));
    let workload =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::InitialMajor).unwrap();
    let phases = workload.initial_phase_residency().unwrap();
    let layout = AwKernelLayout::new([0, 0], 1, [3, 3], [1, 1]).unwrap();
    let identity = PreparedArtifactScientificIdentity::convolution_function(
        PreparedArtifactCellSemantics::new(
            1.0e9,
            1.0,
            0,
            0,
            0.0,
            1.0e9,
            15,
            "EVLA",
            "L",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            false,
            "discrete-complex-sum",
        )
        .unwrap(),
    )
    .unwrap();
    let metadata =
        AwPreparedCellMetadata::new(identity, 1.0e9, 1.0, 1.0, 0, 0.0, layout, layout).unwrap();
    let mut values = vec![Complex64::default(); 9];
    values[4] = Complex64::new(1.0, 0.0);
    let kernel = AwConvolutionKernel::new(layout, values).unwrap();
    let cell = Arc::new(AwConvolutionCell::new(identity, kernel.clone(), kernel).unwrap());
    let provider_lifetime = Arc::downgrade(&cell);
    let prepared = crate::PreparedAwProjection::new(
        AwPreparedCatalog::new(vec![metadata]).unwrap(),
        ObserverAwProvider {
            cells: vec![(identity, cell)],
        },
        false,
        64 * 1024,
    )
    .unwrap();
    let source = SpectralSlabOperator::new_chart(
        specification.clone(),
        &specification.charts[0],
        workload,
        PreparedFft::new(
            workload.grid_shape(),
            workload.fft_resident_complex_values(),
        )
        .unwrap(),
        3,
        Some(prepared),
    )
    .unwrap();
    let grid_values: usize = [
        &source.dirty_grids,
        &source.dirty_compensations,
        &source.psf_grids,
        &source.psf_compensations,
        &source.aw_sensitivity_grids,
        &source.aw_sensitivity_compensations,
    ]
    .into_iter()
    .flat_map(|grids| grids.iter().flatten())
    .map(Array2::len)
    .sum();
    assert_eq!(
        grid_values * size_of::<Complex64>(),
        phases.accumulation_bytes()
    );
    assert!(source.residual_grids.is_none() && source.residual_compensations.is_none());
    assert!(
        source.common_residual_grids.is_none() && source.common_residual_compensations.is_none()
    );
    assert!(source.primary_beam.is_none() && source.mosaic_normal.is_none());
    assert!(provider_lifetime.upgrade().is_some());
    let mut received_local = false;
    let domains = combine_initial_chart_primitives(
        &specification,
        std::iter::once_with(|| {
            let local = source.finish_bound(Some(
                ReconstructionModelBinding::InitialCertifiedZero(model.generation_id()),
            ))?;
            // The actual chart owner (including its arrays) has been consumed before
            // the combiner can allocate its parent arrays.
            assert!(provider_lifetime.upgrade().is_none());
            received_local = true;
            assert!(local.common_residual.is_none() && local.invariant_common_dirty.is_none());
            assert!(
                local.primary_beam_weighted_sum.is_none() && local.major_cycle_residual.is_none()
            );
            let measured = local.measurements;
            let sensitivity_complex_bytes = local.sensitivity.len() * size_of::<Complex64>();
            assert!(
                measured.initial_sensitivity_capacity_bytes
                    >= measured.initial_sensitivity_box_bytes
            );
            assert!(measured.initial_sensitivity_capacity_bytes <= sensitivity_complex_bytes);
            assert_eq!(
                measured.initial_sensitivity_box_bytes,
                local.sensitivity.len() * size_of::<f64>()
            );
            let metadata = (local.sum_weights.len() + local.published_sum_weights.len())
                * size_of::<f64>()
                + local.validity.len() * size_of::<SpectralChannelValidity>();
            assert!(
                phases.accumulation_bytes()
                    + measured.initial_formation_buffer_bytes
                    + measured.initial_sensitivity_box_bytes
                    + metadata
                    <= phases.completion_bytes()
            );
            Ok(local)
        }),
    )
    .unwrap();
    assert!(received_local);
    let parent = &domains.domains[0].primitives;
    let retained =
        (parent.dirty.len() + parent.invariant_dirty.as_ref().unwrap().len() + parent.psf.len())
            * size_of::<Complex64>()
            + (parent.sensitivity.len()
                + parent.sum_weights.len()
                + parent.published_sum_weights.len())
                * size_of::<f64>()
            + parent.validity.len() * size_of::<SpectralChannelValidity>();
    assert_eq!(retained, phases.retained_bytes());
}

#[test]
fn t51_initial_phase_residency_rejects_prior_before_retaining_owner_state() {
    let (problem, model, normal) = fixture();
    let specification = SpectralOperatorSpecification::new(&problem).unwrap();
    let workload =
        spectral_operator_workload(&specification, 3, SpectralOperatorPass::InitialMajor).unwrap();
    let mut owner = prepare_spectral_operator(specification, workload)
        .unwrap()
        .begin_streaming(&problem)
        .unwrap();
    let certified = spectral_operator_workload(
        &aw_specification(&problem, [512, 512]),
        3,
        SpectralOperatorPass::InitialMajor,
    )
    .unwrap()
    .initial_phase_residency();
    // Isolate the early retention guard from cache/compiler fixture construction.
    owner.operators[0].workload.initial_phase_residency = certified;
    assert!(certified.is_some());
    assert_eq!(
        owner.bind_major_cycle_model(&model, Some(normal)),
        Err(SpectralOperatorError::ReusableNormalStateMismatch)
    );
    assert!(owner.reusable_domains.is_none() && owner.model_binding.is_none());
    assert!(
        owner
            .operators
            .iter()
            .all(|operator| operator.primary_beam_replay.is_none()
                && operator.reused_normal_state.is_none()
                && operator.residual_grids.is_none()
                && operator.residual_compensations.is_none())
    );
    owner.bind_major_cycle_model(&model, None).unwrap();
}
