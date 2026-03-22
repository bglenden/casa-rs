// SPDX-License-Identifier: LGPL-3.0-or-later
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserRequestEnvelope, BrowserResponse, BrowserResponseEnvelope,
    BrowserSnapshot,
};

use crate::registry::ResolvedCommand;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug)]
pub(crate) struct BrowserClient {
    child: Arc<Mutex<Child>>,
    stdin: Arc<Mutex<ChildStdin>>,
    responses: Receiver<BrowserResponseEnvelope>,
    stderr: Arc<Mutex<String>>,
}

impl BrowserClient {
    pub(crate) fn spawn(command: &ResolvedCommand) -> Result<Self, String> {
        let mut process = command.command();
        process
            .arg("--session")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = process
            .spawn()
            .map_err(|error| format!("spawn tablebrowser session: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "tablebrowser session stdin was not captured".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "tablebrowser session stdout was not captured".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "tablebrowser session stderr was not captured".to_string())?;

        let child = Arc::new(Mutex::new(child));
        let stdin = Arc::new(Mutex::new(stdin));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line_result in reader.lines() {
                let Ok(line) = line_result else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let response = serde_json::from_str::<BrowserResponseEnvelope>(&line)
                    .unwrap_or_else(|error| {
                        BrowserResponseEnvelope::error(
                            "invalid_response",
                            format!("parse browser response: {error}"),
                        )
                    });
                if tx.send(response).is_err() {
                    break;
                }
            }
        });

        let stderr_target = Arc::clone(&stderr_buffer);
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line_result in reader.lines() {
                let Ok(line) = line_result else {
                    break;
                };
                if let Ok(mut stderr) = stderr_target.lock() {
                    stderr.push_str(&line);
                    stderr.push('\n');
                }
            }
        });

        Ok(Self {
            child,
            stdin,
            responses: rx,
            stderr: stderr_buffer,
        })
    }

    pub(crate) fn request(&self, command: BrowserCommand) -> Result<BrowserSnapshot, String> {
        self.request_with_timeout(command, request_timeout())
    }

    pub(crate) fn request_startup(
        &self,
        command: BrowserCommand,
    ) -> Result<BrowserSnapshot, String> {
        self.request_with_timeout(command, startup_timeout())
    }

    fn request_with_timeout(
        &self,
        command: BrowserCommand,
        timeout: Duration,
    ) -> Result<BrowserSnapshot, String> {
        let payload = serde_json::to_string(&BrowserRequestEnvelope::new(command))
            .map_err(|error| format!("serialize browser request: {error}"))?;
        {
            let mut stdin = self
                .stdin
                .lock()
                .map_err(|_| "failed to acquire browser stdin lock".to_string())?;
            stdin
                .write_all(payload.as_bytes())
                .and_then(|_| stdin.write_all(b"\n"))
                .and_then(|_| stdin.flush())
                .map_err(|error| format!("write browser request: {error}"))?;
        }

        let response = self
            .responses
            .recv_timeout(timeout)
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => {
                    let _ = self.terminate_and_wait();
                    format_browser_failure(
                        "timed out waiting for browser response",
                        self.stderr_text(),
                        None,
                    )
                }
                RecvTimeoutError::Disconnected => {
                    let status = self.reap_exit_status();
                    format_browser_failure(
                        "tablebrowser session exited",
                        self.stderr_text(),
                        status,
                    )
                }
            })?;

        match response.response {
            BrowserResponse::Snapshot(snapshot) => Ok(*snapshot),
            BrowserResponse::Error(error) => Err(format!("{}: {}", error.code, error.message)),
        }
    }

    pub(crate) fn cancel(&self) -> Result<(), String> {
        self.terminate_and_wait().map(|_| ())
    }

    pub(crate) fn stderr_text(&self) -> String {
        self.stderr
            .lock()
            .map(|stderr| stderr.clone())
            .unwrap_or_default()
    }

    fn reap_exit_status(&self) -> Option<ExitStatus> {
        let mut child = self.child.lock().ok()?;
        child.try_wait().ok().flatten()
    }

    fn terminate_and_wait(&self) -> Result<Option<ExitStatus>, String> {
        let mut child = self
            .child
            .lock()
            .map_err(|_| "failed to acquire browser child lock".to_string())?;
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("poll browser session: {error}"))?
        {
            return Ok(Some(status));
        }
        child
            .kill()
            .map_err(|error| format!("terminate browser session: {error}"))?;
        child
            .wait()
            .map(Some)
            .map_err(|error| format!("wait for browser session: {error}"))
    }
}

impl Drop for BrowserClient {
    fn drop(&mut self) {
        let _ = self.terminate_and_wait();
    }
}

fn request_timeout() -> Duration {
    duration_from_env(
        "CASARS_BROWSER_REQUEST_TIMEOUT_MS",
        DEFAULT_REQUEST_TIMEOUT_MS,
    )
}

fn startup_timeout() -> Duration {
    duration_from_env(
        "CASARS_BROWSER_STARTUP_TIMEOUT_MS",
        DEFAULT_STARTUP_TIMEOUT_MS,
    )
}

fn duration_from_env(name: &str, default_ms: u64) -> Duration {
    let millis = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_ms);
    Duration::from_millis(millis)
}

fn format_browser_failure(prefix: &str, stderr: String, status: Option<ExitStatus>) -> String {
    let mut message = match status {
        Some(status) => format!("{prefix} with {status}"),
        None => prefix.to_string(),
    };
    if !stderr.trim().is_empty() {
        message.push_str(": ");
        message.push_str(stderr.trim());
    }
    message
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::thread;

    use casacore_tablebrowser_protocol::{
        BrowserCommand, BrowserResponseEnvelope, BrowserSnapshot, BrowserViewport,
    };
    use tempfile::tempdir;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn timeout_terminates_session_before_a_late_reply_can_be_reused() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let temp = tempdir().expect("tempdir");
        let script = write_slow_browser_script(temp.path(), 200);
        unsafe {
            std::env::set_var("CASARS_BROWSER_REQUEST_TIMEOUT_MS", "50");
        }

        let client =
            BrowserClient::spawn(&ResolvedCommand::direct(script)).expect("spawn browser client");

        let error = client
            .request(BrowserCommand::GetSnapshot { viewport: None })
            .expect_err("request should time out");
        assert!(error.contains("timed out waiting for browser response"));

        thread::sleep(Duration::from_millis(250));
        assert!(
            client
                .request(BrowserCommand::GetSnapshot { viewport: None })
                .is_err(),
            "late reply should not be reused after timeout"
        );

        unsafe {
            std::env::remove_var("CASARS_BROWSER_REQUEST_TIMEOUT_MS");
        }
    }

    fn write_slow_browser_script(root: &Path, delay_ms: u64) -> PathBuf {
        let path = root.join("slow-browser.sh");
        let response = serde_json::to_string(&BrowserResponseEnvelope::snapshot(BrowserSnapshot {
            capabilities: casacore_tablebrowser_protocol::BrowserCapabilities { editable: false },
            view: casacore_tablebrowser_protocol::BrowserView::Overview,
            focus: casacore_tablebrowser_protocol::BrowserFocus::Main,
            table_path: "/tmp/fake.ms".to_string(),
            breadcrumb: Vec::new(),
            viewport: BrowserViewport::new(80, 24),
            status_line: "ok".to_string(),
            content_lines: vec!["Overview".to_string()],
            selected_address: None,
            inspector: None,
        }))
        .expect("serialize snapshot");
        let script = format!(
            "#!/bin/sh\nwhile IFS= read -r line; do\n  sleep {}\n  printf '%s\\n' '{}'\ndone\n",
            delay_ms as f64 / 1000.0,
            response
        );
        fs::write(&path, script).expect("write script");
        let mut permissions = fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("chmod");
        path
    }
}
