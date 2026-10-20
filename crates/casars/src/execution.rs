// SPDX-License-Identifier: LGPL-3.0-or-later
//! Blocking execution of a task's provider process.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::Stdio;

use crate::registry::ResolvedCommand;

/// One provider invocation: the resolved command, its projected arguments
/// and stdin payload, and the workspace it runs in.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub command: ResolvedCommand,
    pub arguments: Vec<OsString>,
    pub stdin: Option<String>,
    pub working_directory: PathBuf,
}

/// How a provider process ended.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionExit {
    pub code: Option<i32>,
    pub success: bool,
}

/// Run the plan to completion. The child inherits this process's stdout and
/// stderr, so its output and progress reach the caller unbuffered; a stdin
/// payload is written and then closed.
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
    let status = child
        .wait()
        .map_err(|error| format!("wait for subprocess: {error}"))?;
    Ok(ExecutionExit {
        code: status.code(),
        success: status.success(),
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
        assert_eq!(exit.code, Some(3));
        assert!(!exit.success);
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
        assert!(exit.success);
        let seen = std::fs::read_to_string(workspace.path().join("seen.txt")).unwrap();
        let mut lines = seen.lines();
        assert_eq!(
            PathBuf::from(lines.next().unwrap()).canonicalize().unwrap(),
            workspace.path().canonicalize().unwrap()
        );
        assert_eq!(lines.next(), Some("family-request"));
    }
}
