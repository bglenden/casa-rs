// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded product-lane admission for the saved mosaic PB (#944).

mod common;

use casa_imaging_model::{
    InstrumentResponse, ProductKind, ReconstructionAlgorithm, ReconstructionBasis,
    RestoringBeamPolicy,
};
use casa_imaging_products::{
    ContinuumProductControls, PlannedContinuumGeneration, ProductStoragePlan,
};
use common::{ImageOutput, continuum::continuum_problem_with_reconstruction};

#[test]
fn a_saved_mosaic_beam_read_is_charged_per_product_lane() {
    let problem = continuum_problem_with_reconstruction(
        95,
        &[ProductKind::Residual],
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let output = ImageOutput::for_problem(&problem);
    let storage = ProductStoragePlan::new(1, 8).unwrap();
    let scratch = |read_bytes| {
        let controls =
            ContinuumProductControls::default().preserving_mosaic_products(read_bytes, false);
        let plan = PlannedContinuumGeneration::for_problem(&problem, &controls).unwrap();
        let demand = plan.planned_demand(&problem, storage, &output).unwrap();
        assert_eq!(demand.storage_plan().maximum_workers(), 2);
        demand.algorithm_scratch_bytes()
    };
    let per_plane = 701_000;
    assert_eq!(scratch(per_plane) - scratch(0), 2 * per_plane);
}

#[test]
fn taylor_restoration_charges_one_saved_residual_mask_at_publication() {
    let problem = continuum_problem_with_reconstruction(
        96,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::TaylorTerms,
        ],
        RestoringBeamPolicy::PerPlane,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        1,
    );
    let output = ImageOutput::for_problem(&problem);
    let storage = ProductStoragePlan::new(1, 8).unwrap();
    let demand = |read_bytes| {
        let controls = ContinuumProductControls::default().preserving_taylor_residuals(read_bytes);
        let plan = PlannedContinuumGeneration::for_problem(&problem, &controls).unwrap();
        plan.planned_demand(&problem, storage, &output).unwrap()
    };
    // Make mask publication dominate restoration, and check its exact
    // slope as the mask's backing store grows.
    let smaller = demand(128 * 1024 * 1024);
    let growth = 701_000;
    let larger = demand(128 * 1024 * 1024 + growth);
    assert_eq!(
        larger.peak_residency_bytes() - smaller.peak_residency_bytes(),
        growth
    );
    assert_eq!(
        larger.algorithm_scratch_bytes() - smaller.algorithm_scratch_bytes(),
        growth
    );
}
