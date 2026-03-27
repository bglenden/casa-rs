// SPDX-License-Identifier: LGPL-3.0-or-later
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::registry::ResolvedCommand;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub command: ResolvedCommand,
    pub arguments: Vec<OsString>,
    pub renderer: Option<String>,
    pub file_output_path: Option<String>,
}

#[derive(Debug)]
pub(crate) enum ExecutionEvent {
    Stdout(String),
    Stderr(String),
    Exited(ExecutionExit),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionExit {
    pub code: Option<i32>,
    pub success: bool,
}

#[derive(Debug)]
pub(crate) struct RunningProcess {
    receiver: Receiver<ExecutionEvent>,
    child: Arc<Mutex<Child>>,
}

impl RunningProcess {
    pub(crate) fn try_recv(&self) -> Result<ExecutionEvent, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }

    pub(crate) fn cancel(&self) -> Result<(), String> {
        self.child
            .lock()
            .map_err(|_| "failed to acquire child-process lock".to_string())?
            .kill()
            .map_err(|error| format!("terminate child process: {error}"))
    }
}

pub(crate) fn spawn_process(plan: &ExecutionPlan) -> Result<RunningProcess, String> {
    let mut command = plan.command.command();
    command
        .args(&plan.arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| format!("spawn subprocess: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "subprocess stdout was not captured".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "subprocess stderr was not captured".to_string())?;

    let child = Arc::new(Mutex::new(child));
    let (tx, rx) = mpsc::channel();

    let stdout_tx = tx.clone();
    let stdout_handle = thread::spawn(move || {
        read_stream(stdout, move |chunk| {
            let _ = stdout_tx.send(ExecutionEvent::Stdout(chunk));
        })
    });

    let stderr_tx = tx.clone();
    let stderr_handle = thread::spawn(move || {
        read_stream(stderr, move |chunk| {
            let _ = stderr_tx.send(ExecutionEvent::Stderr(chunk));
        })
    });

    let child_for_wait = Arc::clone(&child);
    thread::spawn(move || {
        let exit_status = wait_for_child_exit(&child_for_wait);
        let _ = stdout_handle.join();
        let _ = stderr_handle.join();
        let exit = match exit_status {
            Ok(status) => exit_from_status(status),
            Err(_error) => ExecutionExit {
                code: None,
                success: false,
            },
        };
        let _ = tx.send(ExecutionEvent::Exited(exit));
    });

    Ok(RunningProcess {
        receiver: rx,
        child,
    })
}

fn wait_for_child_exit(child: &Arc<Mutex<Child>>) -> Result<ExitStatus, String> {
    loop {
        let status = child
            .lock()
            .map_err(|_| "failed to acquire child-process lock".to_string())?
            .try_wait()
            .map_err(|error| format!("wait for child: {error}"))?;
        if let Some(status) = status {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn read_stream<R>(stream: R, mut send: impl FnMut(String))
where
    R: Read,
{
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => send(line.clone()),
            Err(_) => break,
        }
    }
}

fn exit_from_status(status: ExitStatus) -> ExecutionExit {
    ExecutionExit {
        code: status.code(),
        success: status.success(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ResolvedCommand;
    use std::time::Instant;

    #[test]
    fn spawn_process_reports_stdout_stderr_and_exit() {
        let plan = ExecutionPlan {
            command: ResolvedCommand::direct("sh"),
            arguments: vec![
                "-c".into(),
                "printf 'hello\\n'; printf 'oops\\n' >&2; exit 3".into(),
            ],
            renderer: None,
            file_output_path: None,
        };

        let process = spawn_process(&plan).expect("spawn process");
        let mut saw_stdout = false;
        let mut saw_stderr = false;
        let mut exit = None;
        for _ in 0..80 {
            match process.try_recv() {
                Ok(ExecutionEvent::Stdout(chunk)) => saw_stdout |= chunk.contains("hello"),
                Ok(ExecutionEvent::Stderr(chunk)) => saw_stderr |= chunk.contains("oops"),
                Ok(ExecutionEvent::Exited(status)) => {
                    exit = Some(status);
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => thread::sleep(Duration::from_millis(25)),
                Err(error) => panic!("unexpected channel error: {error}"),
            }
        }

        let exit = exit.expect("process exit event");
        assert!(saw_stdout);
        assert!(saw_stderr);
        assert_eq!(exit.code, Some(3));
        assert!(!exit.success);
    }

    #[test]
    fn cancel_stops_running_child() {
        let plan = ExecutionPlan {
            command: ResolvedCommand::direct("sh"),
            arguments: vec!["-c".into(), "sleep 5".into()],
            renderer: None,
            file_output_path: None,
        };

        let process = spawn_process(&plan).expect("spawn process");
        process.cancel().expect("cancel process");

        let mut exit = None;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match process.try_recv() {
                Ok(ExecutionEvent::Exited(status)) => {
                    exit = Some(status);
                    break;
                }
                Ok(_) | Err(mpsc::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("unexpected channel error: {error}"),
            }
        }

        assert!(!exit.expect("exit after cancel").success);
    }
}
