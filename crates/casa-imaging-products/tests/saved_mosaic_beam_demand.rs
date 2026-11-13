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
