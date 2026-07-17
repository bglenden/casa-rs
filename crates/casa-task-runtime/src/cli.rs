// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::marker::PhantomData;
use std::path::Path;

use casa_provider_contracts::TaskProviderContract;
use serde::{Serialize, de::DeserializeOwned};

/// Machine-readable task action handled by the shared CLI host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCliAction {
    JsonSchema,
    ProtocolInfo,
    JsonRun(String),
}

/// Shared task CLI failure with stable user-facing wording.
#[derive(Debug, thiserror::Error)]
pub enum TaskCliError {
    #[error("missing value for --json-run")]
    MissingJsonRunSource,
    #[error("task CLI argument is not valid UTF-8")]
    NonUtf8Argument,
    #[error("failed to read JSON request from stdin: {0}")]
    ReadStdin(io::Error),
    #[error("failed to read JSON request from {path}: {source}")]
    ReadFile { path: String, source: io::Error },
    #[error("failed to parse task request: {0}")]
    ParseRequest(serde_json::Error),
    #[error("task execution failed: {0}")]
    Execute(String),
    #[error("invalid task provider contract: {0}")]
    InvalidContract(String),
    #[error("failed to serialize task output: {0}")]
    Serialize(serde_json::Error),
}

impl TaskCliError {
    /// Stable process classification shared by every hosted provider.
    pub fn exit_code(&self) -> i32 {
        1
    }
}

/// Reusable host for the common machine-readable shell around a typed task.
pub struct TaskCliHost<E, R, O, F> {
    bundle: TaskProviderContract<E>,
    execute: F,
    marker: PhantomData<fn(R) -> O>,
}

impl<E, R, O, F> TaskCliHost<E, R, O, F>
where
    E: Serialize,
    R: DeserializeOwned,
    O: Serialize,
    F: Fn(R) -> Result<O, String>,
{
    pub fn new(bundle: TaskProviderContract<E>, execute: F) -> Self {
        Self {
            bundle,
            execute,
            marker: PhantomData,
        }
    }

    /// Handle a common machine action, returning `None` for domain arguments.
    pub fn dispatch(&self, args: &[OsString]) -> Result<Option<String>, TaskCliError> {
        self.bundle.validate().map_err(|errors| {
            TaskCliError::InvalidContract(
                errors
                    .into_iter()
                    .map(|error| format!("{}: {}", error.code, error.message))
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;
        let Some(action) = parse_task_cli_action(args)? else {
            return Ok(None);
        };
        match action {
            TaskCliAction::JsonSchema => pretty_json(&self.bundle).map(Some),
            TaskCliAction::ProtocolInfo => pretty_json(&self.bundle.protocol).map(Some),
            TaskCliAction::JsonRun(source) => {
                let payload = read_task_request(&source)?;
                let request = serde_json::from_str(&payload).map_err(TaskCliError::ParseRequest)?;
                let result = (self.execute)(request).map_err(TaskCliError::Execute)?;
                pretty_json(&result).map(Some)
            }
        }
    }
}

pub fn parse_task_cli_action(args: &[OsString]) -> Result<Option<TaskCliAction>, TaskCliError> {
    let mut strings = Vec::with_capacity(args.len());
    for arg in args {
        strings.push(arg.to_str().ok_or(TaskCliError::NonUtf8Argument)?);
    }
    if strings.contains(&"--json-schema") {
        return Ok(Some(TaskCliAction::JsonSchema));
    }
    if strings.contains(&"--protocol-info") {
        return Ok(Some(TaskCliAction::ProtocolInfo));
    }
    if let Some(index) = strings.iter().position(|arg| *arg == "--json-run") {
        let source = strings
            .get(index + 1)
            .ok_or(TaskCliError::MissingJsonRunSource)?;
        return Ok(Some(TaskCliAction::JsonRun((*source).to_string())));
    }
    Ok(None)
}

pub fn read_task_request(source: &str) -> Result<String, TaskCliError> {
    if source == "-" {
        let mut payload = String::new();
        io::stdin()
            .read_to_string(&mut payload)
            .map_err(TaskCliError::ReadStdin)?;
        return Ok(payload);
    }
    fs::read_to_string(source).map_err(|error| TaskCliError::ReadFile {
        path: Path::new(source).display().to_string(),
        source: error,
    })
}

/// Render the common machine-action help block once for every hosted task.
pub fn task_cli_machine_help(request_type: &str) -> String {
    format!(
        "Machine-readable:\n  --json-schema            Emit the canonical task schema bundle\n  --protocol-info          Emit the task protocol descriptor\n  --json-run <SOURCE>      Execute one JSON {request_type} from SOURCE or - for stdin"
    )
}

fn pretty_json(value: &impl Serialize) -> Result<String, TaskCliError> {
    serde_json::to_string_pretty(value).map_err(TaskCliError::Serialize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Deserialize, schemars::JsonSchema)]
    struct Request {
        value: u32,
    }

    #[derive(Serialize, schemars::JsonSchema)]
    struct ResultValue {
        value: u32,
    }

    struct SerializationFailure;

    impl Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "deliberate serialization failure",
            ))
        }
    }

    fn contract() -> casa_provider_contracts::TaskProviderContract {
        let request_schema = schemars::schema_for!(Request);
        let result_schema = schemars::schema_for!(ResultValue);
        casa_provider_contracts::TaskProviderContract {
            protocol: casa_provider_contracts::ProviderProtocolDescriptor::new(
                "test",
                1,
                casa_provider_contracts::ProviderSurfaceKind::Task,
                "1",
            ),
            semantic: casa_provider_contracts::TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: vec![casa_provider_contracts::TaskOperationDescriptor {
                    name: "run".into(),
                    request_kind: "run".into(),
                    result_kind: Some("run".into()),
                }],
            },
            components: casa_provider_contracts::merged_components([
                &request_schema,
                &result_schema,
            ]),
            annotations: serde_json::json!({}),
            projections: casa_provider_contracts::ProviderProjectionMetadata {
                cli: Some(casa_provider_contracts::ProviderCliProjection {
                    machine_actions: casa_provider_contracts::ProviderCliMachineActions {
                        json_schema: Some("--json-schema".into()),
                        protocol_info: Some("--protocol-info".into()),
                        json_run: Some("--json-run <SOURCE>".into()),
                        session: None,
                    },
                }),
                python: None,
            },
            parameter_surfaces: vec![
                casa_provider_contracts::builtin_surface_bundle("imager").unwrap(),
            ],
            domain_schemas: casa_provider_contracts::TaskProviderSchemas {
                request_schema,
                result_schema,
                additional: casa_provider_contracts::NoAdditionalProviderSchemas {},
            },
        }
    }

    #[test]
    fn parses_common_actions_and_rejects_missing_source() {
        assert_eq!(
            parse_task_cli_action(&["--json-schema".into()]).unwrap(),
            Some(TaskCliAction::JsonSchema)
        );
        assert_eq!(
            parse_task_cli_action(&["--json-run".into(), "request.json".into()]).unwrap(),
            Some(TaskCliAction::JsonRun("request.json".into()))
        );
        assert!(matches!(
            parse_task_cli_action(&["--json-run".into()]),
            Err(TaskCliError::MissingJsonRunSource)
        ));
    }

    #[test]
    fn host_renders_discovery_and_executes_file_requests() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), r#"{"value": 4}"#).unwrap();
        let host = TaskCliHost::new(contract(), |request: Request| {
            Ok(ResultValue {
                value: request.value + 1,
            })
        });
        let output = host
            .dispatch(&["--json-run".into(), temp.path().as_os_str().to_owned()])
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&output).unwrap()["value"],
            5
        );
    }

    #[test]
    fn host_classifies_common_failure_corpus() {
        let malformed = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(malformed.path(), "not-json").unwrap();
        let host = TaskCliHost::new(contract(), |_request: Request| Ok(ResultValue { value: 1 }));
        assert!(matches!(
            host.dispatch(&["--json-run".into(), malformed.path().into()]),
            Err(TaskCliError::ParseRequest(_))
        ));
        assert!(matches!(
            host.dispatch(&[
                "--json-run".into(),
                "/definitely/missing/request.json".into()
            ]),
            Err(TaskCliError::ReadFile { .. })
        ));

        let request = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(request.path(), r#"{"value": 4}"#).unwrap();
        let execution_failure = TaskCliHost::new(contract(), |_request: Request| {
            Err::<ResultValue, _>("deliberate execution failure".into())
        });
        assert!(matches!(
            execution_failure.dispatch(&["--json-run".into(), request.path().into()]),
            Err(TaskCliError::Execute(_))
        ));

        let serialization_failure =
            TaskCliHost::new(contract(), |_request: Request| Ok(SerializationFailure));
        assert!(matches!(
            serialization_failure.dispatch(&["--json-run".into(), request.path().into()]),
            Err(TaskCliError::Serialize(_))
        ));

        let mut invalid = contract();
        invalid.protocol.protocol_name.clear();
        let invalid_host =
            TaskCliHost::new(invalid, |_request: Request| Ok(ResultValue { value: 1 }));
        assert!(matches!(
            invalid_host.dispatch(&["--protocol-info".into()]),
            Err(TaskCliError::InvalidContract(_))
        ));
    }

    #[test]
    fn exit_code_preserves_the_shared_provider_contract() {
        assert_eq!(TaskCliError::MissingJsonRunSource.exit_code(), 1);
        assert_eq!(TaskCliError::Execute("failed".into()).exit_code(), 1);
    }
}
