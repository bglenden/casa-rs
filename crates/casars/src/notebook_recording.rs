// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, path::PathBuf};

use casa_notebook::{
    ApprovalRecord, ArtifactReference, AttemptHandle, ExecutionStatus, NotebookStore,
    ReceiptFinalization, RecordingPolicy, RecordingRequest, RunSafetyRecord, TaskCellIntent,
    Timestamp,
};
use casa_provider_contracts::ContextRole;
use casa_task_runtime::{ParameterSession, parse_profile};

#[derive(Debug)]
pub(crate) struct NotebookRecording {
    store: Option<NotebookStore>,
    handle: Option<AttemptHandle>,
    warning: Option<String>,
    expected_paths: Vec<PathBuf>,
    affected_paths_are_products: bool,
    casa_log: Option<PathBuf>,
}

impl NotebookRecording {
    pub(crate) fn begin(
        workspace: PathBuf,
        initiating_surface: &str,
        operation_id: &str,
        session: &ParameterSession,
        notebook: Option<&str>,
        bypass_once: bool,
        approved: bool,
    ) -> Self {
        let store = match absolute_workspace(workspace)
            .and_then(|path| NotebookStore::open(path).map_err(|error| error.to_string()))
        {
            Ok(store) => store,
            Err(error) => {
                return Self {
                    store: None,
                    handle: None,
                    warning: Some(error),
                    expected_paths: Vec::new(),
                    affected_paths_are_products: true,
                    casa_log: configured_casa_log(),
                };
            }
        };
        let notebook_id = match notebook
            .map(|selector| resolve_notebook_id(&store, selector))
            .transpose()
        {
            Ok(notebook_id) => notebook_id,
            Err(error) => {
                return Self {
                    store: Some(store),
                    handle: None,
                    warning: Some(error),
                    expected_paths: Vec::new(),
                    affected_paths_are_products: true,
                    casa_log: configured_casa_log(),
                };
            }
        };
        let expected_paths = expected_output_paths(session);
        let request = match recording_request(
            initiating_surface,
            operation_id,
            session,
            notebook_id,
            expected_paths.clone(),
            approved,
        ) {
            Ok(request) => request,
            Err(error) => {
                return Self {
                    store: Some(store),
                    handle: None,
                    warning: Some(error),
                    expected_paths: Vec::new(),
                    affected_paths_are_products: true,
                    casa_log: configured_casa_log(),
                };
            }
        };
        let policy = if bypass_once {
            RecordingPolicy::BypassOnce
        } else {
            RecordingPolicy::Record
        };
        let (handle, warning) = store.try_begin_attempt(policy, request);
        Self {
            store: Some(store),
            handle,
            warning,
            expected_paths,
            affected_paths_are_products: true,
            casa_log: configured_casa_log(),
        }
    }

    pub(crate) fn begin_operation(
        workspace: PathBuf,
        initiating_surface: &str,
        operation_id: &str,
        resolved_parameters: BTreeMap<String, serde_json::Value>,
        classification: &str,
        affected_paths: Vec<PathBuf>,
        bypass_once: bool,
    ) -> Self {
        let store = match absolute_workspace(workspace)
            .and_then(|path| NotebookStore::open(path).map_err(|error| error.to_string()))
        {
            Ok(store) => store,
            Err(error) => {
                return Self {
                    store: None,
                    handle: None,
                    warning: Some(error),
                    expected_paths: Vec::new(),
                    affected_paths_are_products: false,
                    casa_log: configured_casa_log(),
                };
            }
        };
        let request = RecordingRequest {
            initiating_surface: initiating_surface.to_owned(),
            operation_id: operation_id.to_owned(),
            notebook_id: None,
            cell_id: None,
            task_intent: None,
            provider_contract_version: 1,
            resolved_parameters,
            run_safety: RunSafetyRecord {
                classification: classification.to_owned(),
                affected_paths,
            },
            approvals: vec![ApprovalRecord {
                kind: "user_action".into(),
                actor: "user".into(),
                timestamp: Timestamp::now(),
                content_hash: None,
            }],
        };
        let policy = if bypass_once {
            RecordingPolicy::BypassOnce
        } else {
            RecordingPolicy::Record
        };
        let (handle, warning) = store.try_begin_attempt(policy, request);
        Self {
            store: Some(store),
            handle,
            warning,
            expected_paths: Vec::new(),
            affected_paths_are_products: false,
            casa_log: configured_casa_log(),
        }
    }

    pub(crate) fn take_warning(&mut self) -> Option<String> {
        self.warning.take()
    }

    pub(crate) fn finalize(
        &mut self,
        status: ExecutionStatus,
        stdout: String,
        stderr: String,
        affected_paths: Vec<PathBuf>,
        diagnostics: Vec<String>,
    ) -> Option<String> {
        let (Some(store), Some(handle)) = (&self.store, self.handle.take()) else {
            return None;
        };
        let mut affected_paths = affected_paths;
        affected_paths.extend(self.expected_paths.clone());
        affected_paths.sort();
        affected_paths.dedup();
        let products = if self.affected_paths_are_products {
            affected_paths
                .iter()
                .cloned()
                .map(|path| ArtifactReference {
                    role: "task_output".into(),
                    path,
                    media_type: None,
                })
                .collect()
        } else {
            Vec::new()
        };
        store.try_finalize_attempt(
            &handle,
            ReceiptFinalization {
                status,
                finished_at: Timestamp::now(),
                affected_paths: affected_paths.clone(),
                products,
                artifacts: Vec::new(),
                diagnostics,
                stdout: stdout.into_bytes(),
                stderr: stderr.into_bytes(),
                casa_log: self.casa_log.clone(),
            },
        )
    }
}

fn configured_casa_log() -> Option<PathBuf> {
    std::env::var_os("CASA_RS_LOG_TABLE").map(PathBuf::from)
}

fn expected_output_paths(session: &ParameterSession) -> Vec<PathBuf> {
    session
        .bundle()
        .surface
        .bindings()
        .iter()
        .filter(|binding| binding.context_role == Some(ContextRole::OutputProduct))
        .filter_map(|binding| session.states().get(&binding.name)?.value.as_ref())
        .filter_map(|value| serde_json::to_value(value).ok())
        .filter_map(|value| value.as_str().map(PathBuf::from))
        .collect()
}

fn recording_request(
    initiating_surface: &str,
    operation_id: &str,
    session: &ParameterSession,
    notebook_id: Option<casa_notebook::NotebookId>,
    affected_paths: Vec<PathBuf>,
    approved: bool,
) -> Result<RecordingRequest, String> {
    let sparse = session.render_sparse().map_err(|error| error.to_string())?;
    let profile = parse_profile(&sparse).map_err(|error| error.to_string())?;
    let parameters = profile
        .parameters
        .into_iter()
        .map(|(name, value)| {
            toml::Value::try_from(value)
                .map(|value| (name, value))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let resolved_parameters = session
        .values()
        .into_iter()
        .map(|(name, value)| {
            serde_json::to_value(value)
                .map(|value| (name, value))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let safety = session
        .required_run_safety()
        .map_err(|error| error.to_string())?;
    let classification = safety
        .classes()
        .iter()
        .map(|class| serde_json::to_value(class).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_owned))
        .collect::<Vec<_>>()
        .join(",");
    Ok(RecordingRequest {
        initiating_surface: initiating_surface.to_owned(),
        operation_id: operation_id.to_owned(),
        notebook_id,
        cell_id: None,
        task_intent: Some(TaskCellIntent {
            format: profile.header.format,
            surface: profile.header.surface,
            kind: "task".into(),
            contract: profile.header.contract,
            parameters,
        }),
        provider_contract_version: profile.header.contract,
        resolved_parameters,
        run_safety: RunSafetyRecord {
            classification: if classification.is_empty() {
                "read_only".into()
            } else {
                classification
            },
            affected_paths,
        },
        approvals: approved
            .then(|| ApprovalRecord {
                kind: "run_safety".into(),
                actor: "user".into(),
                timestamp: Timestamp::now(),
                content_hash: None,
            })
            .into_iter()
            .collect(),
    })
}

fn resolve_notebook_id(
    store: &NotebookStore,
    selector: &str,
) -> Result<casa_notebook::NotebookId, String> {
    let selector = selector.trim();
    let notebooks = store.list_notebooks().map_err(|error| error.to_string())?;
    notebooks
        .into_iter()
        .find(|entry| entry.filename == selector || entry.id.to_string() == selector)
        .map(|entry| entry.id)
        .ok_or_else(|| format!("notebook {selector:?} does not exist in the explicit workspace"))
}

fn absolute_workspace(path: PathBuf) -> Result<PathBuf, String> {
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|error| format!("resolve current workspace: {error}"))?
            .join(path)
    };
    path.canonicalize()
        .map_err(|error| format!("resolve explicit workspace {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
    use casa_task_runtime::ResolutionPatch;

    fn session() -> ParameterSession {
        let mut session =
            ParameterSession::defaults(builtin_surface_bundle("imstat").expect("imstat bundle"))
                .expect("default session");
        session
            .apply_override_patch(ResolutionPatch {
                values: BTreeMap::from([(
                    "imagename".into(),
                    ParameterValue::String("input.image".into()),
                )]),
                ..ResolutionPatch::default()
            })
            .expect("required image parameter");
        session
    }

    #[test]
    fn recording_helper_persists_surface_identity_status_and_streams() {
        let project = tempfile::tempdir().expect("project");
        let workspace = project.path().canonicalize().expect("canonical project");
        let session = session();
        let mut recording = NotebookRecording::begin(
            workspace.clone(),
            "tui",
            "imstat",
            &session,
            None,
            false,
            false,
        );
        let warning = recording.take_warning();
        assert!(warning.is_none(), "{warning:?}");
        assert!(
            recording
                .finalize(
                    ExecutionStatus::Failed,
                    "partial output".into(),
                    "provider error".into(),
                    Vec::new(),
                    vec!["failed fixture".into()],
                )
                .is_none()
        );
        let store = NotebookStore::open(workspace).expect("store");
        let notebook = store.open_notebook("default.md").expect("default notebook");
        let receipts = store
            .receipts_for_notebook(notebook.entry.id)
            .expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].initiating_surface, "tui");
        assert_eq!(receipts[0].status, ExecutionStatus::Failed);
        assert_eq!(receipts[0].operation_id, "imstat");
        assert_eq!(receipts[0].diagnostics, ["failed fixture"]);
        assert!(receipts[0].logs.stdout.is_some());
        assert!(receipts[0].logs.stderr.is_some());
    }

    #[test]
    fn recording_helper_bypass_applies_to_only_that_attempt() {
        let project = tempfile::tempdir().expect("project");
        let workspace = project.path().canonicalize().expect("canonical project");
        let session = session();
        let mut bypassed = NotebookRecording::begin(
            workspace.clone(),
            "cli",
            "imstat",
            &session,
            None,
            true,
            false,
        );
        let warning = bypassed.take_warning();
        assert!(warning.is_none(), "{warning:?}");
        assert!(
            bypassed
                .finalize(
                    ExecutionStatus::Succeeded,
                    String::new(),
                    String::new(),
                    Vec::new(),
                    Vec::new(),
                )
                .is_none()
        );
        assert!(!workspace.join("notebooks/default.md").exists());

        let mut recorded = NotebookRecording::begin(
            workspace.clone(),
            "cli",
            "imstat",
            &session,
            None,
            false,
            false,
        );
        let _ = recorded.finalize(
            ExecutionStatus::Succeeded,
            String::new(),
            String::new(),
            Vec::new(),
            Vec::new(),
        );
        assert!(workspace.join("notebooks/default.md").is_file());
    }

    #[test]
    fn operation_recording_has_no_task_intent_or_false_product_claim() {
        let project = tempfile::tempdir().expect("project");
        let workspace = project.path().canonicalize().expect("canonical project");
        let image = workspace.join("restored.image");
        let mut recording = NotebookRecording::begin_operation(
            workspace.clone(),
            "tui",
            "imexplore.write_region_mask",
            BTreeMap::from([
                ("dataset".into(), serde_json::json!(image)),
                ("set_default".into(), serde_json::json!(true)),
            ]),
            "input_mutation",
            vec![image.clone()],
            false,
        );
        assert!(recording.take_warning().is_none());
        assert!(
            recording
                .finalize(
                    ExecutionStatus::Succeeded,
                    String::new(),
                    String::new(),
                    vec![image.clone()],
                    Vec::new(),
                )
                .is_none()
        );

        let store = NotebookStore::open(workspace).expect("store");
        let notebook = store.open_notebook("default.md").expect("default notebook");
        let receipt = store
            .receipts_for_notebook(notebook.entry.id)
            .expect("receipts")
            .pop()
            .expect("operation receipt");
        assert_eq!(receipt.operation_id, "imexplore.write_region_mask");
        assert!(receipt.sparse_intent.is_none());
        assert_eq!(receipt.run_safety.classification, "input_mutation");
        assert_eq!(receipt.affected_paths, [image]);
        assert!(receipt.products.is_empty());
    }

    #[test]
    fn explicit_notebook_selector_routes_task_attempt_to_named_notebook() {
        let project = tempfile::tempdir().expect("project");
        let workspace = project.path().canonicalize().expect("canonical project");
        let store = NotebookStore::open(workspace.clone()).expect("store");
        let named = store
            .create_named("Analysis.md", "Analysis")
            .expect("named notebook");
        let session = session();
        let mut recording = NotebookRecording::begin(
            workspace.clone(),
            "python",
            "imstat",
            &session,
            Some("Analysis.md"),
            false,
            false,
        );
        assert!(recording.take_warning().is_none());
        let _ = recording.finalize(
            ExecutionStatus::Succeeded,
            String::new(),
            String::new(),
            Vec::new(),
            Vec::new(),
        );

        let receipts = store
            .receipts_for_notebook(named.entry.id)
            .expect("named receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].initiating_surface, "python");
        assert!(!workspace.join("notebooks/default.md").exists());
    }
}
