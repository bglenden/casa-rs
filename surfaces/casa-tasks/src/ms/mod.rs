// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feature-gated MeasurementSet task adapters, schemas, and front ends.

pub mod casa_return;
pub mod concat;
pub mod flagmanager;
pub mod front_end;
pub mod msconvert;
pub mod msexplore;
pub mod task_contract;

pub use casa_return::*;
pub use concat::{concat_front_end, concat_task_schema_bundle};
pub use front_end::{ms_task_success, task_flag_merge, task_selection_text};
pub use task_contract::{
    FlagDataTaskRequest, FlagManagerMutationResult, FlagManagerTaskRequest, FlagManagerTaskResult,
    MsTransformTaskRequest, flagdata_task_schema_bundle, flagmanager_task_schema_bundle,
    mstransform_task_schema_bundle,
};
