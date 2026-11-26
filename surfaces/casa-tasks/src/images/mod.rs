// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feature-gated image-analysis task adapters, contracts, and CASA return
//! projections shared between the CLI frontend and Python bindings.

mod casa_return;
pub mod exportfits;
pub mod feather;
pub mod front_end;
pub mod imexplore;
pub mod immath;
pub mod immoments;
pub mod impbcor;
pub mod importfits;
pub mod impv;
pub mod imregrid;
pub mod imsubimage;
pub mod task_contract;

pub use casa_return::{
    ImheadGetError, imhead_get_value, imhead_list_record, imhead_summary_record,
    imstat_planes_record, imstat_record,
};
pub use front_end::{image_task_front_end, image_task_returning, image_task_success};
pub use task_contract::{
    IMAGE_ANALYSIS_TASK_PROTOCOL_NAME, IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION,
    ImageAnalysisTaskRequest, ImageAnalysisTaskResult, ImpbcorTaskRequest, ImpvTaskRequest,
    ImregridTaskOutcome, ImregridTaskRequest, ImregridTaskTemplate, PbcorTaskBeam, PbcorTaskRegion,
    PvTaskAmount, image_analysis_protocol_descriptor, image_analysis_task_schema_bundle,
};
