// SPDX-License-Identifier: LGPL-3.0-or-later

//! MVC's admitted cube-to-Taylor normal-state handoff (#1069).

use casa_imaging_deconvolution::{
    MajorCycle, MajorCycleCompletion, ModelLifecycle, ModelStoragePlan, NormalStoragePlan,
    normal_state_window_residency_bytes, resident_bytes_per_cell,
};
use casa_imaging_model::{CompiledProblem, ModelSample, ReconstructionBasis};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, PlannedContinuumGeneration,
    PrimaryBeamCentres, ProductsError, encode_mvc_blank_taylor_planes, encode_mvc_taylor_planes,
    reduce_mvc_cube,
};
use casa_imaging_runtime::pass::Cancel;
use casa_imaging_runtime::{Admitted, Cancelled, Demand, HostResources, ResourcePolicy, admit};

use crate::native::NativeError;

/// Inputs to the Taylor conversion, all bound to this run's two views.
pub(crate) struct TaylorAssemblyInputs<'a> {
    pub(crate) cube_problem: &'a CompiledProblem,
    pub(crate) cube: &'a MajorCycleCompletion,
    pub(crate) taylor_problem: &'a CompiledProblem,
    pub(crate) expansion_reference_hz: f64,
    pub(crate) controls: ContinuumProductControls,
    pub(crate) centres: Option<&'a PrimaryBeamCentres>,
}

/// The Taylor state and common PB, kept under the returned admission.
#[derive(Debug)]
pub(crate) struct TaylorCompletion {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) primary_beam: Vec<f32>,
}

/// Complete one Taylor normal state from the cube. Admission covers the
/// reduction's six output planes and one channel of workspace, and the
/// overlap of those planes, the widened Taylor state and its initial model.
pub(crate) fn assemble_taylor_completion(
    inputs: TaylorAssemblyInputs<'_>,
    (host, policy, cancel): (&HostResources, &ResourcePolicy, &Cancel),
) -> Result<Admitted<TaylorCompletion>, NativeError> {
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let TaylorAssemblyInputs {
        cube_problem,
        cube,
        taylor_problem,
        expansion_reference_hz,
        controls,
        centres,
    } = inputs;
    if cube_problem.observation() != taylor_problem.observation() {
        return Err(NativeError::Other(
            std::io::Error::other("MVC cube and Taylor views must use the same observation").into(),
        ));
    }
    let shape = cube.normal_state().shape();
    let domains = taylor_problem.geometry().domains();
    if domains.len() != 1
        || domains[0].shape().pixels() != shape
        || taylor_problem.reconstruction().basis() != (ReconstructionBasis::Taylor { terms: 2 })
        || taylor_problem
            .reconstruction()
            .polarization()
            .coordinates()
            .len()
            != 1
    {
        return Err(NativeError::Other(
            std::io::Error::other("MVC needs matching single-Stokes two-term views").into(),
        ));
    }
    let cells = shape[0]
        .checked_mul(shape[1])
        .ok_or(ProductsError::ResourceDemandOverflow("MVC cells"))?;
    let samples = taylor_problem.model_lifecycle().target().sample_count();
    let memory = taylor_demand(shape, samples)?;
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "MVC Taylor conversion",
            memory,
        },
    )?;
    let mut mean = vec![0.0; cells];
    let mut psf = vec![
        0.0;
        cells
            .checked_mul(3)
            .ok_or(ProductsError::ResourceDemandOverflow("MVC PSF"))?
    ];
    let mut residual = vec![
        0.0;
        cells
            .checked_mul(2)
            .ok_or(ProductsError::ResourceDemandOverflow("MVC residual"))?
    ];
    let mut sumwt = [0.0; 3];
    let mut cube_inputs = ContinuumProductInputs::from_major_cycle(cube_problem, cube);
    if let Some(centres) = centres {
        cube_inputs = cube_inputs.with_primary_beam_centres(centres);
    }
    let plan = PlannedContinuumGeneration::new(&cube_inputs, &controls)?;
    reduce_mvc_cube(
        &plan,
        &cube_inputs,
        expansion_reference_hz,
        &mut mean,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )?;
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let images = if sumwt[0] == 0.0
        && taylor_problem.products().restoring_beam()
            == casa_imaging_model::RestoringBeamPolicy::None
    {
        encode_mvc_blank_taylor_planes(shape, psf, residual, sumwt)?
    } else {
        encode_mvc_taylor_planes(shape, psf, residual, sumwt)?
    };
    // Taylor product publication reads a whole coefficient plane at once.
    // Initialization itself remains chunked to at most 1 << 16 samples.
    let model_storage = ModelStoragePlan::resident(cells).map_err(other)?;
    let lifecycle = ModelLifecycle::new(taylor_problem, model_storage);
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().map_err(other)?, [])
        .map_err(other)?;
    let storage = NormalStoragePlan::resident(1).map_err(other)?;
    let mut cycle = MajorCycle::initial(taylor_problem, model, storage).map_err(other)?;
    cycle.parts().1.append(images).map_err(other)?;
    let scientific = cycle.finish_read().map_err(other)?;
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    Ok(Admitted {
        value: TaylorCompletion {
            scientific,
            primary_beam: mean,
        },
        reservation,
    })
}

/// A conservative bound for a two-term one-Stokes conversion's live heap.
fn taylor_demand(shape: [usize; 2], model_samples: usize) -> Result<u64, NativeError> {
    let overflow = || ProductsError::ResourceDemandOverflow("MVC Taylor conversion");
    let cells = u64::try_from(shape[0].checked_mul(shape[1]).ok_or_else(overflow)?)
        .map_err(|_| overflow())?;
    let bytes = |per_cell: u64| cells.checked_mul(per_cell).ok_or_else(overflow);
    let output = bytes(6 * size_of::<f32>() as u64)?;
    let cube_window = normal_state_window_residency_bytes(shape, 1, 1).map_err(other)?;
    let reduction = output
        .checked_add(bytes(3 * size_of::<f32>() as u64)?)
        .and_then(|n| n.checked_add(cube_window))
        .ok_or_else(overflow)?;
    let model = u64::try_from(model_samples)
        .map_err(|_| overflow())?
        .checked_mul(size_of::<ModelSample>() as u64)
        .ok_or_else(overflow)?;
    let initial_window = u64::try_from(model_samples.min(1 << 16))
        .map_err(|_| overflow())?
        .checked_mul(size_of::<ModelSample>() as u64)
        .ok_or_else(overflow)?;
    let state = bytes(resident_bytes_per_cell(2, 3, true))?;
    let assembly = output
        .checked_add(state)
        .and_then(|n| n.checked_add(model))
        .and_then(|n| n.checked_add(initial_window))
        .ok_or_else(overflow)?;
    Ok(reduction
        .max(assembly)
        .checked_add(64 * 1024)
        .ok_or_else(overflow)?)
}

fn other(error: impl std::error::Error + Send + Sync + 'static) -> NativeError {
    NativeError::Other(Box::new(error))
}
