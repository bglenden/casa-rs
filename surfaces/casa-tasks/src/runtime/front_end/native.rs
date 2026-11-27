// SPDX-License-Identifier: LGPL-3.0-or-later
//! In-process execution of the same adapters the standalone hosts use.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use casa_contracts::{ParameterValue, TaskReply, builtin_surface_bundle};

use super::{
    TaskFailure, TaskInvocation, TaskSuccess, first_error, observe_task, open_named_session,
    task_reply,
};
use crate::runtime::invocation::task_override_values;
use crate::{BaseSource, OpenSessionRequest, ResolutionPatch};

#[cfg(feature = "imaging")]
pub use casa_imaging::Cancel as TaskCancellation;

/// Writes a task-formatted diagnostic line to the caller's log stream.
pub type TaskLogLineSink = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

/// Caller-owned controls for a task run outside the standalone CLI.
#[derive(Default)]
pub struct NativeTaskOptions {
    /// Display CASA task messages that use the default text stream.
    pub default_logs: bool,
    /// Sink for CASA's default task diagnostics.
    pub log_line: Option<TaskLogLineSink>,
    /// Optional file for numeric arrays referenced by returned plots.
    pub plot_payload: Option<PathBuf>,
    /// Apply calibration without collecting a separate CLI report.
    pub applycal_value_only: bool,
    /// Return listcal pages with any error attached to the page that failed.
    pub listcal_paged_reply: bool,
    /// Plotbandpass pages the caller already rendered.
    pub bandpass_completed_pages: Option<usize>,
    /// Clearcal's reservations protecting the tables that it changes.
    #[cfg(all(unix, feature = "calibration"))]
    pub table_writes: Option<std::sync::Arc<[casa_tables::DelegatedTableWrite]>>,
    /// Per-run controls and a cancellation token for imaging tasks.
    #[cfg(feature = "imaging")]
    pub imaging: crate::imaging::ImagingExecutionOptions,
}

impl NativeTaskOptions {
    pub(crate) fn is_cancelled(&self) -> bool {
        #[cfg(feature = "imaging")]
        {
            self.imaging.cancel.is_cancelled()
        }
        #[cfg(not(feature = "imaging"))]
        {
            false
        }
    }
}

/// Run a built-in task with explicit overrides and no managed Last state.
///
/// Relative paths resolve against `workspace`, which must be absolute.
/// The caller selects its log subscriber. The returned lines are the
/// task's text output, separate from the typed reply and tracing events.
/// `originals` retains type or spelling where the task contract uses it.
pub fn run_task_in_process(
    surface: &str,
    values: BTreeMap<String, ParameterValue>,
    originals: &BTreeMap<String, ParameterValue>,
    argument_text: &BTreeMap<String, String>,
    workspace: &Path,
    options: &NativeTaskOptions,
) -> (TaskReply, Vec<String>) {
    let outcome = run(
        surface,
        values,
        originals,
        argument_text,
        workspace,
        options,
    );
    task_reply(surface, outcome)
}

fn run(
    surface: &str,
    values: BTreeMap<String, ParameterValue>,
    originals: &BTreeMap<String, ParameterValue>,
    argument_text: &BTreeMap<String, String>,
    workspace: &Path,
    options: &NativeTaskOptions,
) -> Result<TaskSuccess, TaskFailure> {
    if !workspace.is_absolute() {
        return Err(TaskFailure::validation("task workspace must be absolute"));
    }
    if options.is_cancelled() {
        return Err(TaskFailure::interrupted(
            "cancelled before the task started",
        ));
    }
    let bundle = builtin_surface_bundle(surface).map_err(TaskFailure::validation)?;
    let mut request = OpenSessionRequest {
        bundle,
        workspace: workspace.to_owned(),
        source: BaseSource::Defaults,
        profile_text: None,
        context_patch: ResolutionPatch::default(),
        override_patch: ResolutionPatch {
            values,
            ..ResolutionPatch::default()
        },
        managed_save: false,
    };
    let draft = open_named_session(request.clone()).map_err(session_error)?;
    request.override_patch.values = task_override_values(&draft, originals);
    let session = open_named_session(request).map_err(session_error)?;
    if let Some(error) = first_error(&session) {
        return Err(session_error((
            error.message.clone(),
            error.parameter.clone(),
        )));
    }
    let invocation = TaskInvocation::in_process(&session, workspace, argument_text, options);
    #[cfg(feature = "images")]
    return casa_imageanalysis::analysis::with_image_expression_workspace(workspace, || {
        observe_task(&invocation, execute)
    });
    #[cfg(not(feature = "images"))]
    observe_task(&invocation, execute)
}

fn session_error((message, parameter): (String, Option<String>)) -> TaskFailure {
    let mut failure = TaskFailure::validation(message);
    failure.parameter = parameter;
    failure
}

fn execute(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    match task.surface() {
        #[cfg(feature = "calibration")]
        "flagdata" => crate::calibration::run_flagdata(task),
        #[cfg(feature = "calibration")]
        "mstransform" | "split" => crate::calibration::run_mstransform(task),
        #[cfg(feature = "calibration")]
        id if crate::calibration::CALIBRATION_SURFACES.contains(&id) => {
            crate::calibration::cli::run_task(task)
        }
        #[cfg(feature = "imaging")]
        "tclean" => {
            crate::imaging::run_task_with_options(task, &task.native_options().unwrap().imaging)
        }
        #[cfg(feature = "imaging")]
        "tsdimaging" => crate::imaging::single_dish::run(
            task,
            task.native_options().unwrap().imaging.cancel.clone(),
        ),
        #[cfg(feature = "images")]
        "imhead" | "imstat" => crate::images::imexplore::run(task, task.default_stderr_logs()),
        #[cfg(feature = "images")]
        "exportfits" => crate::images::exportfits::run(task),
        #[cfg(feature = "images")]
        "feather" => crate::images::feather::run(task),
        #[cfg(feature = "images")]
        "immath" => crate::images::immath::run(task),
        #[cfg(feature = "images")]
        "immoments" => crate::images::immoments::run(task),
        #[cfg(feature = "images")]
        "impbcor" => crate::images::impbcor::run(task),
        #[cfg(feature = "images")]
        "importfits" => crate::images::importfits::run(task),
        #[cfg(feature = "images")]
        "impv" => crate::images::impv::run(task),
        #[cfg(feature = "images")]
        "imregrid" => crate::images::imregrid::run(task),
        #[cfg(feature = "images")]
        "imsubimage" => crate::images::imsubimage::run(task),
        #[cfg(feature = "ms")]
        "concat" => crate::ms::concat::run(task),
        #[cfg(feature = "ms")]
        "flagmanager" => crate::ms::flagmanager::run(task),
        #[cfg(feature = "ms")]
        "msconvert" => crate::ms::msconvert::run_task(task),
        #[cfg(feature = "ms")]
        "msexplore" | "plotms" => {
            let payload = task
                .native_options()
                .and_then(|options| options.plot_payload.as_deref());
            #[cfg(feature = "calibration")]
            return crate::ms::msexplore::cli::run_task(
                task,
                payload,
                Some(crate::calibration::plotms::load_calibration),
                Some(crate::calibration::plotms::load_models),
            );
            #[cfg(not(feature = "calibration"))]
            crate::ms::msexplore::cli::run_task(task, payload, None, None)
        }
        #[cfg(feature = "simulation")]
        "simobserve" => crate::simulation::run_task(task),
        #[cfg(feature = "simulation")]
        "simanalyze" => crate::simulation::run_simanalyze(task),
        #[cfg(feature = "simulation")]
        "simalma" => crate::simulation::run_simalma(task),
        #[cfg(feature = "simulation")]
        "predictcomp" => crate::simulation::run_predictcomp(task),
        #[cfg(feature = "vla")]
        "importvla" => crate::vla::run_task(task),
        id => Err(TaskFailure::validation(format!(
            "task {id:?} is unavailable in this build"
        ))),
    }
}
