// SPDX-License-Identifier: LGPL-3.0-or-later
//! The shared task front end's wire types (#931): the `--json-run` request a
//! caller sends a task binary, and the one typed reply the binary writes to
//! stdout.
//!
//! The reply is the whole of stdout. Log lines go to stderr, and nothing is
//! ever inferred from them. The exit status agrees with the reply:
//! [`EXIT_SUCCEEDED`] for [`TaskReply::Succeeded`], and
//! [`TaskFailureKind::exit_code`] for [`TaskReply::Failed`]. A caller
//! classifies a finished process from its exit status and stdout alone; a
//! process killed by a signal, or one whose stdout is not a reply that agrees
//! with its exit status, is a different outcome from either failure kind.

use schemars::JsonSchema;
use schemars::schema::RootSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::CasaValue;

/// Exit status of a task whose reply is [`TaskReply::Succeeded`].
pub const EXIT_SUCCEEDED: i32 = 0;
/// Exit status of a task that started and failed ([`TaskFailureKind::Execution`]).
pub const EXIT_EXECUTION_FAILED: i32 = 1;
/// Exit status of a task whose parameters were refused before it changed
/// anything ([`TaskFailureKind::Validation`]).
pub const EXIT_VALIDATION_FAILED: i32 = 2;
/// Exit status of a task that stopped because it was interrupted
/// ([`TaskFailureKind::Interrupted`]): 128 + SIGINT, the shell's status for an
/// interrupted command.
pub const EXIT_INTERRUPTED: i32 = 130;

/// One `--json-run` request: the surface to run and the CASA-named values the
/// caller set.
///
/// Every parameter missing from `values` takes its current default. The task
/// binary resolves the values against the surface's contract itself, as it
/// resolves command-line flags, so a Python call and a command line share one
/// resolution. A `--json-run` call neither reads nor writes Last state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskRunRequest {
    /// Surface id, such as `"split"`; one the binary hosts.
    pub surface: String,
    /// The values the caller set, keyed by CASA parameter name, as plain JSON.
    #[serde(default)]
    pub values: Map<String, Value>,
}

/// Why a task did not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskFailureKind {
    /// The parameters were refused before the task changed anything. Python
    /// raises `ValueError`.
    Validation,
    /// The task started and failed. Python raises `RuntimeError`.
    Execution,
    /// The task caught an interrupt (SIGINT), stopped at its next safe point
    /// and published nothing. A task that handles no interrupt is ended by
    /// the signal instead, and writes no reply.
    Interrupted,
}

impl TaskFailureKind {
    /// The exit status that accompanies a reply of this kind.
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Validation => EXIT_VALIDATION_FAILED,
            Self::Execution => EXIT_EXECUTION_FAILED,
            Self::Interrupted => EXIT_INTERRUPTED,
        }
    }
}

/// The typed reply a task binary writes to stdout for `--json-run`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaskReply {
    /// The task succeeded.
    Succeeded {
        /// Surface that ran.
        surface: String,
        /// The value CASA's task returns, such as `imstat`'s statistics
        /// record, or [`CasaValue::None`] where CASA returns `None`. It is
        /// kept ahead of the generic product projection, which cannot
        /// recover it.
        value: CasaValue,
        /// The provider's full typed result, from which the surface's declared
        /// products are projected.
        result: Value,
    },
    /// The task failed.
    Failed {
        /// Surface that was asked to run.
        surface: String,
        /// Whether the parameters were refused or the run failed.
        kind: TaskFailureKind,
        /// The reason, as the task reports it.
        message: String,
        /// The parameter at fault, when the failure names one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parameter: Option<String>,
    },
}

impl TaskReply {
    /// The surface the reply is about.
    pub fn surface(&self) -> &str {
        match self {
            Self::Succeeded { surface, .. } | Self::Failed { surface, .. } => surface,
        }
    }

    /// The exit status that must accompany this reply.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Succeeded { .. } => EXIT_SUCCEEDED,
            Self::Failed { kind, .. } => kind.exit_code(),
        }
    }
}

/// The shared front end's `--json-run` request and reply schemas, carried by
/// a provider contract whose binary runs on the front end.
#[derive(Debug, Clone, Serialize)]
pub struct TaskFrontEndSchemas {
    /// Schema of [`TaskRunRequest`].
    pub run_request_schema: RootSchema,
    /// Schema of [`TaskReply`].
    pub reply_schema: RootSchema,
}

impl Default for TaskFrontEndSchemas {
    fn default() -> Self {
        Self {
            run_request_schema: schemars::schema_for!(TaskRunRequest),
            reply_schema: schemars::schema_for!(TaskReply),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_success_carries_casas_return_value_ahead_of_the_result() {
        let reply = TaskReply::Succeeded {
            surface: "imstat".into(),
            value: CasaValue::dict([("npts", CasaValue::float64_vector([12.0]))]),
            result: json!({"kind": "statistics"}),
        };
        let text = serde_json::to_string(&reply).unwrap();
        assert_eq!(
            text,
            r#"{"status":"succeeded","surface":"imstat","value":{"kind":"dict","entries":[{"key":"npts","value":{"kind":"float_array","dtype":"float64","shape":[1],"data":[12.0]}}]},"result":{"kind":"statistics"}}"#
        );
        assert_eq!(serde_json::from_str::<TaskReply>(&text).unwrap(), reply);
        assert_eq!(reply.exit_code(), EXIT_SUCCEEDED);
    }

    #[test]
    fn failure_kinds_keep_distinct_exit_statuses() {
        let refused: TaskReply = serde_json::from_value(json!({
            "status": "failed",
            "surface": "split",
            "kind": "validation",
            "message": "unknown parameter",
            "parameter": "widht",
        }))
        .unwrap();
        assert_eq!(refused.exit_code(), EXIT_VALIDATION_FAILED);
        assert_eq!(refused.surface(), "split");
        let failed = TaskReply::Failed {
            surface: "split".into(),
            kind: TaskFailureKind::Execution,
            message: "disk full".into(),
            parameter: None,
        };
        assert_eq!(failed.exit_code(), EXIT_EXECUTION_FAILED);
        assert!(
            !serde_json::to_string(&failed)
                .unwrap()
                .contains("parameter")
        );
        let interrupted: TaskReply = serde_json::from_value(json!({
            "status": "failed",
            "surface": "tclean",
            "kind": "interrupted",
            "message": "interrupted; nothing was published",
        }))
        .unwrap();
        assert_eq!(interrupted.exit_code(), EXIT_INTERRUPTED);
    }

    #[test]
    fn a_run_request_names_its_surface_and_only_the_values_set() {
        let request: TaskRunRequest =
            serde_json::from_value(json!({"surface": "split", "values": {"width": 2}})).unwrap();
        assert_eq!(request.values["width"], 2);
        let bare: TaskRunRequest = serde_json::from_value(json!({"surface": "split"})).unwrap();
        assert!(bare.values.is_empty());
        assert!(
            serde_json::from_value::<TaskRunRequest>(json!({"surface": "split", "vals": {}}))
                .is_err()
        );
    }
}
