// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feature-gated calibration, flagging, transform, and single-dish task adapters.

mod appendantab;
pub mod casa_return;
pub mod cli;
mod flagdata;
mod fluxscale_history;
pub mod managed_output;
mod mstransform;
mod pccor;
pub mod plotbandpass_statistics;
pub mod plotms;
pub mod single_dish;
pub mod task_contract;

pub use casa_return::{fluxscale_record, solve_record};
pub use cli::{ImagerSetJyRequest, imager_setjy, task_front_end};
pub use flagdata::flagdata_front_end;
pub use managed_output::{
    ApplyMultiMsReport, ApplyPartitionReport, CalibrationTaskResult, CrossHandGainSolveReport,
    CrossHandGainType,
};
pub use mstransform::mstransform_front_end;
pub use task_contract::{
    CALIBRATION_SURFACES, CALIBRATION_TASK_PROTOCOL_NAME, CALIBRATION_TASK_PROTOCOL_VERSION,
    CalibrationTaskRequest, ExecuteApplyTaskRequest, ExportCorrectedDataTaskRequest,
    GencalTaskRequest, PlanApplyTaskRequest, SolveBandpassTaskRequest, SolveGainTaskRequest,
    StatsTaskRequest, SummaryTaskRequest, calibration_protocol_descriptor,
    calibration_task_schema_bundle,
};
