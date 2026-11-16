// SPDX-License-Identifier: LGPL-3.0-or-later
//! Alternate product coordinates contribute to the writer's memory demand.

mod common;

use casa_coordinates::{CoordinateSystem, SpectralCoordinate};
use casa_imaging_model::{ImageDomainRole, ProductKind, RestoringBeamPolicy};
use casa_imaging_products::{
    CasaImageWriter, ContinuumProductControls, PlannedContinuumGeneration, ProductStoragePlan,
};
use casa_types::measures::frequency::FrequencyRef;

#[test]
fn alternate_family_demand_accounts_for_its_own_coordinate_metadata() {
    let problem = common::continuum::continuum_problem_with_policy(
        211,
        &[ProductKind::Psf],
        RestoringBeamPolicy::None,
    );
    let plan =
        PlannedContinuumGeneration::for_problem(&problem, &ContinuumProductControls::default())
            .unwrap();
    let writer = CasaImageWriter::new("coordinate-demand".into(), CoordinateSystem::new());
    let mut alternate = CoordinateSystem::new();
    const FREQUENCIES: usize = 1 << 15;
    alternate.add_coordinate(
        SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1e9)
            .with_rest_frequencies(vec![1e9; FREQUENCIES]),
    );
    let alternate = writer.with_coordinates(vec![(ImageDomainRole::Main, alternate)]);
    let storage = ProductStoragePlan::new(1, 1).unwrap();
    let base = plan
        .planned_demand(&problem, storage, &writer)
        .unwrap()
        .peak_residency_bytes();
    let extra = plan
        .planned_demand(&problem, storage, &alternate)
        .unwrap()
        .peak_residency_bytes();
    assert!(
        extra >= base + (3 * FREQUENCIES * size_of::<f64>()) as u64,
        "alternate metadata must be charged: base={base}, alternate={extra}"
    );
}
