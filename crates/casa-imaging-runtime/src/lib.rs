// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Imaging execution (plan section 5.4): host resources and per-phase
//! admission, the process allocator that keeps the footprint to what is
//! admitted, the major-cycle pass on the bounded worker team, cooperative
//! cancellation, the paged cube state, the minor-cycle adapter and the run
//! summary.

mod allocator;
mod cube_state;
mod managed_cube_blocks;
mod managed_model;
mod managed_normal;
mod minor;
pub mod pass;
mod resources;
mod source_access;
mod summary;

pub use allocator::{RETURNED_BLOCK_BYTES, ReturningAllocator};
pub use cube_state::CubeState;
pub use minor::{
    MinorCycleOutcome, MinorCycleRunError, MinorCycleSetup, MinorCycleSummary, MinorLayout,
    PreparedMinorCycle, PsfCache, TracedComponent, mask_bytes, prepare_bytes, prepare_bytes_for,
    prepare_minor_cycle, result_bytes, run_bytes, run_bytes_for, run_minor_cycle,
};
pub use resources::{
    Admission, Admitted, CoreAllocation, Demand, HostError, HostResources, Reservation,
    ReservationWindow, ResourcePolicy, STREAM_PRODUCERS, admit, charge_process_caches, free_memory,
    process_cache_bytes, reserved_memory,
};
pub use source_access::{BOOTSTRAP_SOURCE_BYTES, FRAME_MARGIN, row_layout};
pub use summary::{
    Cancelled, Diagnostics, Phase, RunStatus, RunSummary, SCIENCE_TRACE, STAGE_TIMING,
    SummaryTarget, run_phase,
};
