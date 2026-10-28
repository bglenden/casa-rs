// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image [`TotalPowerGrid::finish`] makes keeps the grids' share of the
//! admission ([`TotalPowerImage::bytes`]): while it lives the runtime admits
//! only what is left beside it, and once it drops nothing of the grid stays
//! reserved. The kernel's and the passes' bytes are released at `finish`.
//!
//! Reservations are counted per process, so this law has a test binary of
//! its own.

use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{Demand, HostResources, ResourcePolicy, admit, reserved_memory};
use casa_imaging_singledish::{
    FlagMerge, GridFunction, GridShape, KernelTable, SpectrumBlock, TotalPowerGrid, TotalPowerImage,
};
use ndarray::{Array2, Array3};

const BUDGET: u64 = 64 << 20;
const HOST: HostResources = HostResources {
    threads: 2,
    performance_cores: 2,
    available_memory: BUDGET,
    metal: false,
};
const POLICY: ResourcePolicy = ResourcePolicy::Explicit {
    workers: 2,
    memory: BUDGET,
};

fn admit_bytes(phase: &'static str, memory: u64) -> Result<(), casa_imaging_runtime::Admission> {
    admit(&HOST, &POLICY, &Demand { phase, memory }).map(drop)
}

#[test]
fn a_finished_image_keeps_its_grids_charge_until_it_drops() {
    let shape = GridShape {
        nx: 64,
        ny: 48,
        planes: 1,
        channels: 3,
    };
    let kernel = KernelTable::new(GridFunction::Spheroidal { support: 3 }).expect("kernel");
    let demand = Demand {
        phase: "total-power gridding",
        memory: TotalPowerGrid::bytes(shape, &kernel, shape.channels),
    };
    let reservation = admit(&HOST, &POLICY, &demand).expect("admitted");
    let mut grid =
        TotalPowerGrid::new(shape, kernel, FlagMerge::AnyCorrelation, reservation).expect("grid");
    let pixels = [Some([20.0, 30.0])];
    let values = Array3::from_elem((1, shape.channels, 2), 1.0_f32);
    let flags = Array3::from_elem((1, shape.channels, 2), false);
    let weights = Array2::from_elem((1, shape.channels), 1.0_f32);
    let channel_map: Vec<Option<usize>> = (0..shape.channels).map(Some).collect();
    let block = SpectrumBlock {
        pixels: &pixels,
        values: values.view(),
        flags: flags.view(),
        row_flags: &[false],
        weights: weights.view(),
        channel_map: &channel_map,
        correlation_map: &[Some(0), Some(0)],
    };
    grid.grid(&block, &WorkerTeam::new(2).expect("team"))
        .expect("gridded");

    let image = grid.finish();
    let kept = TotalPowerImage::bytes(shape);
    assert!(
        kept < demand.memory,
        "the kernel and the passes are released"
    );
    assert_eq!(image.reservation.memory(), kept);
    assert_eq!(reserved_memory(), kept);
    assert!(
        image.value.weight.iter().any(|&w| w > 0.0),
        "the spectrum gridded"
    );
    // Beside the image, only what it leaves is admitted.
    assert!(admit_bytes("the next image", BUDGET - kept + 1).is_err());
    admit_bytes("the next image", BUDGET - kept).expect("what the image leaves");

    drop(image);
    assert_eq!(reserved_memory(), 0);
    admit_bytes("the next image", BUDGET).expect("the whole budget");
}
