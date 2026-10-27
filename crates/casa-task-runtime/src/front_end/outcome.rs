// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a caller classifies a finished task process: from its exit status and
//! its stdout reply, never from stderr.

use std::fmt;
use std::process::ExitStatus;

use casa_provider_contracts::{TaskFailureKind, TaskReply};
use serde_json::Value;

/// The exit status of a finished task process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskExit {
    /// The exit code, absent when a signal ended the process.
    pub code: Option<i32>,
    /// The signal that ended the process, on Unix.
    pub signal: Option<i32>,
}

impl From<ExitStatus> for TaskExit {
    fn from(status: ExitStatus) -> Self {
        #[cfg(unix)]
        let signal = std::os::unix::process::ExitStatusExt::signal(&status);
        #[cfg(not(unix))]
        let signal = None;
        Self {
            code: status.code(),
            signal,
        }
    }
}

/// How one task call ended, as its caller sees it.
///
/// The variants stay distinct: a refusal and an execution failure come from
/// the task's own reply, while a launch failure, a signal and a malformed
/// reply mean the task could not report at all. A signal is reported as the
/// signal, whatever sent it; it is not assumed to be the out-of-memory killer.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskOutcome {
    /// The task succeeded.
    Succeeded {
        /// Surface that ran.
        surface: String,
        /// CASA's return value.
        value: Value,
        /// The provider's full result.
        result: Value,
    },
    /// The task refused its parameters before it changed anything.
    Refused {
        /// Surface that was asked to run.
        surface: String,
        /// The task's reason.
        message: String,
        /// The parameter at fault, when the task names one.
        parameter: Option<String>,
    },
    /// The task started and failed.
    Failed {
        /// Surface that was asked to run.
        surface: String,
        /// The task's reason.
        message: String,
        /// The parameter at fault, when the task names one.
        parameter: Option<String>,
    },
    /// The task process could not be started.
    LaunchFailed {
        /// Why the launch failed.
        message: String,
    },
    /// A signal ended the task process.
    Signalled {
        /// The signal number.
        signal: i32,
    },
    /// The process exited without a reply that agrees with its exit status.
    MalformedReply {
        /// The process's exit code.
        exit_code: Option<i32>,
        /// What was wrong with the reply.
        reason: String,
    },
}

impl TaskOutcome {
    /// The outcome of a process that could not be started.
    pub fn launch_failed(error: impl fmt::Display) -> Self {
        Self::LaunchFailed {
            message: error.to_string(),
        }
    }
}

/// Classify a finished task process from its exit status and the reply it
/// wrote to stdout.
pub fn classify_task_exit(exit: TaskExit, stdout: &str) -> TaskOutcome {
    if let Some(signal) = exit.signal {
        return TaskOutcome::Signalled { signal };
    }
    let reply = match serde_json::from_str::<TaskReply>(stdout.trim()) {
        Ok(reply) => reply,
        Err(error) => {
            return TaskOutcome::MalformedReply {
                exit_code: exit.code,
                reason: if stdout.trim().is_empty() {
                    "the task wrote no reply".to_string()
                } else {
                    format!("the task's reply is not valid: {error}")
                },
            };
        }
    };
    if exit.code != Some(reply.exit_code()) {
        return TaskOutcome::MalformedReply {
            exit_code: exit.code,
            reason: format!(
                "the task exited with {} but its reply needs exit code {}",
                exit.code
                    .map_or_else(|| "no code".to_string(), |code| format!("code {code}")),
                reply.exit_code()
            ),
        };
    }
    match reply {
        TaskReply::Succeeded {
            surface,
            value,
            result,
        } => TaskOutcome::Succeeded {
            surface,
            value,
            result,
        },
        TaskReply::Failed {
            surface,
            kind: TaskFailureKind::Validation,
            message,
            parameter,
        } => TaskOutcome::Refused {
            surface,
            message,
            parameter,
        },
        TaskReply::Failed {
            surface,
            kind: TaskFailureKind::Execution,
            message,
            parameter,
        } => TaskOutcome::Failed {
            surface,
            message,
            parameter,
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn exited(code: i32) -> TaskExit {
        TaskExit {
            code: Some(code),
            signal: None,
        }
    }

    #[test]
    fn replies_classify_by_their_kind() {
        let succeeded = classify_task_exit(
            exited(0),
            r#"{"status":"succeeded","surface":"imstat","value":{"npts":[12.0]},"result":{}}"#,
        );
        assert_eq!(
            succeeded,
            TaskOutcome::Succeeded {
                surface: "imstat".into(),
                value: json!({"npts": [12.0]}),
                result: json!({}),
            }
        );
        let refused = classify_task_exit(
            exited(2),
            r#"{"status":"failed","surface":"split","kind":"validation","message":"bad width","parameter":"width"}"#,
        );
        assert!(matches!(
            refused,
            TaskOutcome::Refused { parameter: Some(ref name), .. } if name == "width"
        ));
        let failed = classify_task_exit(
            exited(1),
            r#"{"status":"failed","surface":"split","kind":"execution","message":"disk full"}"#,
        );
        assert!(matches!(failed, TaskOutcome::Failed { ref message, .. } if message == "disk full"));
    }

    #[test]
    fn a_signal_is_reported_as_the_signal_whatever_stdout_holds() {
        let killed = TaskExit {
            code: None,
            signal: Some(9),
        };
        assert_eq!(
            classify_task_exit(killed, r#"{"status":"succeeded"}"#),
            TaskOutcome::Signalled { signal: 9 }
        );
    }

    #[test]
    fn a_missing_garbled_or_contradicted_reply_is_malformed() {
        for (exit, stdout) in [
            (exited(1), ""),
            (exited(0), "imstat completed\n"),
            (
                exited(0),
                r#"{"status":"failed","surface":"x","kind":"execution","message":"m"}"#,
            ),
            (
                exited(1),
                r#"{"status":"succeeded","surface":"x","value":null,"result":{}}"#,
            ),
        ] {
            assert!(
                matches!(
                    classify_task_exit(exit, stdout),
                    TaskOutcome::MalformedReply { .. }
                ),
                "{exit:?} {stdout:?}"
            );
        }
    }

    #[test]
    fn a_launch_failure_keeps_its_reason() {
        assert_eq!(
            TaskOutcome::launch_failed("No such file or directory"),
            TaskOutcome::LaunchFailed {
                message: "No such file or directory".into()
            }
        );
    }
}
