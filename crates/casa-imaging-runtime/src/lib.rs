// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Imaging execution (plan section 5.4): host resources and per-phase
//! admission, the major-cycle pass on the bounded worker team, cooperative
//! cancellation, the paged cube state, the minor-cycle adapter and the run
//! summary.

mod cube_state;
mod managed_cube_blocks;
mod managed_model;
mod managed_normal;
mod minor;
pub mod pass;
mod resources;
mod source_access;
mod summary;

pub use cube_state::CubeState;
pub use minor::{
    MinorCycleOutcome, MinorCycleRunError, MinorCycleSetup, MinorCycleSummary, PreparedMinorCycle,
    PsfCache, TracedComponent, prepare_minor_cycle, run_minor_cycle,
};
pub use resources::{
    Admission, Demand, HostError, HostResources, Reservation, ResourcePolicy, admit, free_memory,
};
pub use source_access::{SourceAccessError, bootstrap_source_budget, finalize_source_access};
pub use summary::{Cancelled, Phase, RunSummary, SummaryTarget, run_phase};
