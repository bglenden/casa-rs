// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dense MEM minor-cycle execution and its retained model reservation.

use casa_imaging_deconvolution::{
    CycleControls, DenseModelPlane, MinorCycleView, PlaneShape, PlaneStatistics, peak_magnitude,
};
use casa_imaging_model::{ModelSample, ReconstructionAlgorithm};

use super::{Inputs, MinorCycleRunError, PlaneData, PlaneKey, Solved, coefficient, plane_pixels};
use crate::{Demand, HostResources, ResourcePolicy, admit};

/// Allocate dense-result descriptors only for the dense solver; sparse
/// solvers retain no empty dense-results allocation.
pub(super) fn dense_result_slots(
    algorithm: &ReconstructionAlgorithm,
    planes: usize,
) -> Vec<(PlaneKey, DenseModelPlane)> {
    Vec::with_capacity(if matches!(algorithm, ReconstructionAlgorithm::Mem) {
        planes
    } else {
        0
    })
}

/// Bytes retained by one dense plane and the bounded model window applying it.
pub(super) fn dense_result_bytes(shape: PlaneShape) -> u64 {
    (shape.len() as u64)
        .saturating_mul((size_of::<f64>() + size_of::<ModelSample>()) as u64)
        .saturating_add(
            (size_of::<DenseModelPlane>() + size_of::<(PlaneKey, DenseModelPlane)>()) as u64,
        )
}

pub(super) fn solve_mem(
    inputs: &Inputs<'_>,
    (host, policy): (&HostResources, &ResourcePolicy),
    key: PlaneKey,
    plane: &PlaneData<'_>,
    view: &MinorCycleView<'_>,
    statistics: &PlaneStatistics,
    controls: &CycleControls,
) -> Result<Solved, MinorCycleRunError> {
    let charge = admit(
        host,
        policy,
        &Demand {
            phase: "minor-cycle dense results",
            memory: dense_result_bytes(plane.shape),
        },
    )?;
    let (mut outcome, mut model, mem_alpha) =
        casa_imaging_deconvolution::run_mem_plane(view, controls, statistics)?;
    let end_residual = std::mem::take(&mut outcome.end_residual);
    let peak_no_mask = match end_residual.first() {
        Some(residual) if inputs.setup.residual_pixel_mask => {
            let pixels = plane_pixels(inputs, key, plane)?;
            peak_magnitude(residual, pixels.residual_mask(true))
        }
        Some(residual) => peak_magnitude(residual, &plane.valid),
        None => statistics.peak_no_mask,
    };
    let dense = if model.is_empty() {
        None
    } else {
        let physical = plane.physical()?;
        for (index, value) in model.iter_mut().enumerate() {
            *value = physical.apply(*value, index)?;
        }
        Some(DenseModelPlane::new(
            key.domain,
            coefficient(inputs, key, 0),
            key.polarization,
            model,
        ))
    };
    Ok(Solved {
        outcome,
        peak_no_mask,
        terms: Vec::new(),
        dense,
        mem_alpha,
        trace: Vec::new(),
        charge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_mem_allocates_dense_result_slots() {
        assert_eq!(
            dense_result_slots(&ReconstructionAlgorithm::Hogbom, 7).capacity(),
            0
        );
        assert!(dense_result_slots(&ReconstructionAlgorithm::Mem, 7).capacity() >= 7);
    }
}
