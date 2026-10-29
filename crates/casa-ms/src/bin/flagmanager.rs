// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` - native CASA-style flag-version management, on the shared
//! task front end.

use std::process::ExitCode;

use casa_ms::{
    FlagManagerMutationResult, FlagManagerTaskRequest, FlagManagerTaskResult, MeasurementSet,
    delete_flag_version, flagmanager_list_record, flagmanager_task_schema_bundle,
    list_flag_versions, ms_task_success, rename_flag_version, restore_flag_version,
    save_flag_version, task_flag_merge,
};
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "flagmanager",
        flagmanager_task_schema_bundle(),
        &["flagmanager"],
        run,
    )
    .expect("the flagmanager contract carries its surface")
    .main(
        std::env::args_os().skip(1),
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.path("vis")?;
    let request = FlagManagerTaskRequest {
        vis: vis.clone(),
        mode: task.required_text("mode")?.to_ascii_lowercase(),
        versionname: task.text("versionname")?,
        oldname: task.text("oldname")?,
        comment: task.text("comment")?,
        merge: task_flag_merge(task, "merge")?,
    };
    let result = execute(request)?;
    // CASA's `list` returns the versions; the other modes return None.
    let value = match &result {
        FlagManagerTaskResult::Versions(versions) => flagmanager_list_record(versions, &vis),
        FlagManagerTaskResult::Mutation(_) => CasaValue::None,
    };
    ms_task_success(value, &result)
}

fn execute(request: FlagManagerTaskRequest) -> Result<FlagManagerTaskResult, TaskFailure> {
    let failed = |error: casa_ms::FlaggingError| TaskFailure::execution(error.to_string());
    let required = |name: &str, value: Option<String>| {
        value.ok_or_else(|| {
            TaskFailure::validation(format!(
                "flagmanager mode {:?} requires {name}",
                request.mode
            ))
            .at(name)
        })
    };
    let mut ms = MeasurementSet::open(&request.vis)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let mutation = |mode: &str, versionname, oldname| {
        Ok(FlagManagerTaskResult::Mutation(FlagManagerMutationResult {
            mode: mode.to_string(),
            versionname,
            oldname,
        }))
    };
    match request.mode.as_str() {
        "list" => Ok(FlagManagerTaskResult::Versions(
            list_flag_versions(&ms).map_err(failed)?,
        )),
        "save" => {
            let name = required("versionname", request.versionname.clone())?;
            let comment = request.comment.as_deref().unwrap_or("");
            save_flag_version(&ms, &name, comment, request.merge).map_err(failed)?;
            mutation("save", Some(name), None)
        }
        "restore" => {
            let name = required("versionname", request.versionname.clone())?;
            restore_flag_version(&mut ms, &name, request.merge).map_err(failed)?;
            ms.save_main_table_only()
                .map_err(|error| TaskFailure::execution(error.to_string()))?;
            mutation("restore", Some(name), None)
        }
        "delete" => {
            let name = required("versionname", request.versionname.clone())?;
            delete_flag_version(&ms, &name).map_err(failed)?;
            mutation("delete", Some(name), None)
        }
        "rename" => {
            let old = required("oldname", request.oldname.clone())?;
            let name = required("versionname", request.versionname.clone())?;
            let comment = request.comment.as_deref().unwrap_or("");
            rename_flag_version(&ms, &old, &name, comment).map_err(failed)?;
            mutation("rename", Some(name), Some(old))
        }
        other => Err(TaskFailure::validation(format!(
            "unsupported flagmanager mode {other:?}; expected list, save, restore, delete or rename"
        ))
        .at("mode")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_bundle_embeds_flagmanager_parameter_contract() {
        let bundle = flagmanager_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_flagmanager_task");
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "flagmanager");
        bundle.validate().expect("valid flagmanager provider");
    }
}
