// SPDX-License-Identifier: LGPL-3.0-or-later
//! Blocking execution of a task's binary.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};

use crate::registry::ResolvedCommand;

/// One task invocation: the resolved command, its arguments and stdin
/// request, and the workspace it runs in.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub command: ResolvedCommand,
    pub arguments: Vec<OsString>,
    pub stdin: Option<String>,
    pub working_directory: PathBuf,
}

/// How a task process ended: its status and the typed reply on its stdout.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionExit {
    pub status: ExitStatus,
    pub stdout: String,
}

/// Run the plan to completion. The child inherits this process's stderr, so
/// its log and progress reach the caller unbuffered; its stdout, which holds
/// the reply, is captured; a stdin payload is written and then closed.
pub(crate) fn run_process_blocking(plan: &ExecutionPlan) -> Result<ExecutionExit, String> {
    let mut child = plan
        .command
        .command()
        .args(&plan.arguments)
        .current_dir(&plan.working_directory)
        .stdin(if plan.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn subprocess: {error}"))?;
    if let Some(payload) = plan.stdin.as_deref() {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "subprocess stdin was not captured".to_string())?;
        stdin
            .write_all(payload.as_bytes())
            .map_err(|error| format!("write subprocess stdin: {error}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for subprocess: {error}"))?;
    Ok(ExecutionExit {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh(script: &str, stdin: Option<&str>, working_directory: PathBuf) -> ExecutionPlan {
        ExecutionPlan {
            command: ResolvedCommand::direct("sh"),
            arguments: vec!["-c".into(), script.into()],
            stdin: stdin.map(str::to_string),
            working_directory,
        }
    }

    #[test]
    fn run_reports_the_exit_code() {
        let exit = run_process_blocking(&sh("exit 3", None, PathBuf::from("."))).unwrap();
        assert_eq!(exit.status.code(), Some(3));
        assert!(!exit.status.success());
        assert_eq!(exit.stdout, "");
    }

    #[test]
    fn stdout_is_captured_not_passed_through() {
        let exit = run_process_blocking(&sh("printf reply", None, PathBuf::from("."))).unwrap();
        assert!(exit.status.success());
        assert_eq!(exit.stdout, "reply");
    }

    #[test]
    fn run_uses_the_workspace_and_the_projected_stdin() {
        let workspace = tempfile::tempdir().unwrap();
        let script = "pwd > seen.txt; IFS= read -r line; printf '%s\\n' \"$line\" >> seen.txt";
        let exit = run_process_blocking(&sh(
            script,
            Some("family-request\n"),
            workspace.path().to_path_buf(),
        ))
        .unwrap();
        assert!(exit.status.success());
        let seen = std::fs::read_to_string(workspace.path().join("seen.txt")).unwrap();
        let mut lines = seen.lines();
        assert_eq!(
            PathBuf::from(lines.next().unwrap()).canonicalize().unwrap(),
            workspace.path().canonicalize().unwrap()
        );
        assert_eq!(lines.next(), Some("family-request"));
    }
}
