// SPDX-License-Identifier: LGPL-3.0-or-later
//! Blocking execution of a task's provider process.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};

use crate::registry::ResolvedCommand;

/// One provider invocation: the resolved command, its projected arguments
/// and stdin payload, and the workspace it runs in.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub command: ResolvedCommand,
    pub arguments: Vec<OsString>,
    pub stdin: Option<String>,
    pub working_directory: PathBuf,
    /// Capture stdout, which holds a front-end binary's typed reply, instead
    /// of passing it through.
    pub capture_stdout: bool,
}

/// How a provider process ended.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionExit {
    pub status: ExitStatus,
    /// The captured stdout, when the plan asked for it.
    pub stdout: Option<String>,
}

impl ExecutionExit {
    pub fn success(&self) -> bool {
        self.status.success()
    }

    pub fn code(&self) -> Option<i32> {
        self.status.code()
    }
}

/// Run the plan to completion. The child inherits this process's stderr, and
/// its stdout unless the plan captures it, so its output and progress reach
/// the caller unbuffered; a stdin payload is written and then closed.
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
        .stdout(if plan.capture_stdout {
            Stdio::piped()
        } else {
            Stdio::inherit()
        })
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
        stdout: plan
            .capture_stdout
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned()),
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
            capture_stdout: false,
        }
    }

    #[test]
    fn run_reports_the_exit_code() {
        let exit = run_process_blocking(&sh("exit 3", None, PathBuf::from("."))).unwrap();
        assert_eq!(exit.code(), Some(3));
        assert!(!exit.success());
        assert_eq!(exit.stdout, None);
    }

    #[test]
    fn a_captured_stdout_is_returned_not_passed_through() {
        let mut plan = sh("printf reply", None, PathBuf::from("."));
        plan.capture_stdout = true;
        let exit = run_process_blocking(&plan).unwrap();
        assert!(exit.success());
        assert_eq!(exit.stdout.as_deref(), Some("reply"));
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
        assert!(exit.success());
        let seen = std::fs::read_to_string(workspace.path().join("seen.txt")).unwrap();
        let mut lines = seen.lines();
        assert_eq!(
            PathBuf::from(lines.next().unwrap()).canonicalize().unwrap(),
            workspace.path().canonicalize().unwrap()
        );
        assert_eq!(lines.next(), Some("family-request"));
    }
}
